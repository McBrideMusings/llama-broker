package tenants

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mostlygeek/llama-swap/internal/config"
	"github.com/mostlygeek/llama-swap/internal/logmon"
	"github.com/mostlygeek/llama-swap/internal/process"
)

// modeServer serves the condition {"on": ...} in mode "on" or "off", drops the
// connection in mode "broken", and answers after the probe timeout in mode
// "slow".
func modeServer(t *testing.T, mode *atomic.Value) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch mode.Load().(string) {
		case "on":
			io.WriteString(w, `{"on": true}`)
		case "off":
			io.WriteString(w, `{"on": false}`)
		case "broken":
			panic(http.ErrAbortHandler)
		case "slow":
			select {
			case <-r.Context().Done():
			case <-time.After(2 * time.Second):
			}
			io.WriteString(w, `{"on": false}`)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// startFast starts m with the hi tenant polling every 20ms.
func startFast(t *testing.T, m *Manager) *fakeRouter {
	t.Helper()
	m.byName("hi").interval = 20 * time.Millisecond
	r := &fakeRouter{running: map[string]process.ProcessState{}}
	m.Start(t.Context(), r)
	return r
}

func hiCondition(m *Manager) ProbeStatus {
	return *m.Status(nil, nil).Tenants[0].Condition
}

func TestTenants_ConditionErrorKeepsClaim(t *testing.T) {
	var mode atomic.Value
	mode.Store("on")
	srv := modeServer(t, &mode)
	log := logmon.New()
	m := New(twoTenants(srv.URL, config.TenantOnBlockedHold), log)
	startFast(t, m)
	eventually(t, "hi to want the GPU", func() bool { return wantsGPU(m, "hi") })

	for _, failure := range []string{"broken", "slow"} {
		mode.Store(failure)
		start := hiCondition(m).Errors
		eventually(t, failure+" probes to fail", func() bool { return hiCondition(m).Errors >= start+3 })
		if reason, _ := m.Block("lo-model", nil); reason == nil {
			t.Fatalf("%s condition probe lifted the block on lo", failure)
		}
		cond := hiCondition(m)
		if cond.Result == nil || !*cond.Result || cond.LastError == "" || cond.LastErrorAt == nil {
			t.Fatalf("%s: condition status %+v, want result true with lastError set", failure, cond)
		}
	}
	m.mu.Lock()
	probe := m.byName("hi").conditionLocked()
	m.mu.Unlock()
	if !strings.Contains(probe, "condition of hi: true (HTTP 200, on=true); failed probes since: ") || !strings.Contains(probe, "context deadline exceeded") {
		t.Fatalf("decision probe %q does not show the kept reading and the failures", probe)
	}

	mode.Store("off")
	eventually(t, "lo to be released", func() bool { reason, _ := m.Block("lo-model", nil); return reason == nil })
	if cond := hiCondition(m); cond.Errors != 0 || cond.LastError == "" {
		t.Fatalf("after recovery: errors=%d lastError=%q, want 0 and the last failure kept", cond.Errors, cond.LastError)
	}

	history := string(log.GetHistory())
	// broken then slow is one run of failures: one warning.
	if n := strings.Count(history, "condition probe failed (1 in a row)"); n != 1 {
		t.Fatalf("logged %d first-failure warnings, want 1:\n%s", n, history)
	}
	if strings.Contains(history, "condition probe failed (2 in a row)") {
		t.Fatalf("a repeated failure logged above debug:\n%s", history)
	}
	if !strings.Contains(history, "keeping wants GPU: true from the reading at") ||
		!strings.Contains(history, "condition probe recovered after") {
		t.Fatalf("missing kept-reading or recovery line:\n%s", history)
	}
}

func TestTenants_ConditionChangeLogsReadingTime(t *testing.T) {
	var on atomic.Bool
	on.Store(true)
	srv := switchServer(t, &on)
	log := logmon.NewWriter(io.Discard)
	m := New(twoTenants(srv.URL, config.TenantOnBlockedHold), log)
	m.byName("hi").interval = time.Hour // one reading only
	m.Start(t.Context(), &fakeRouter{running: map[string]process.ProcessState{}})
	// The line is written after the reading is stored, so wait for the line.
	eventually(t, "the condition change line", func() bool {
		return strings.Contains(string(log.GetHistory()), "condition true")
	})

	want := "tenants: hi (priority 10) condition true (HTTP 200, on=true); wants GPU: true time=" +
		hiCondition(m).ProbedAt.Format(time.RFC3339Nano)
	if history := string(log.GetHistory()); !strings.Contains(history, want) {
		t.Fatalf("log lacks %q, the reading's probedAt:\n%s", want, history)
	}
}

func TestTenants_ConditionErrorBeforeFirstReading(t *testing.T) {
	var mode atomic.Value
	mode.Store("broken")
	srv := modeServer(t, &mode)
	log := logmon.New()
	m := New(twoTenants(srv.URL, config.TenantOnBlockedHold), log)
	startFast(t, m)
	eventually(t, "probes to fail", func() bool { return hiCondition(m).Errors >= 3 })

	if reason, _ := m.Block("lo-model", nil); reason != nil {
		t.Fatalf("a failing first probe claimed the GPU: %v", reason)
	}
	if cond := hiCondition(m); cond.Result != nil || cond.ProbedAt != nil {
		t.Fatalf("condition status %+v, want result and probedAt null before a reading", cond)
	}
	if history := string(log.GetHistory()); !strings.Contains(history, "no reading yet, wants GPU stays false") {
		t.Fatalf("missing the no-reading warning:\n%s", history)
	}

	mode.Store("on")
	eventually(t, "hi to want the GPU", func() bool { reason, _ := m.Block("lo-model", nil); return reason != nil })
}
