package tenants

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mostlygeek/llama-swap/internal/config"
	"github.com/mostlygeek/llama-swap/internal/logmon"
	"github.com/mostlygeek/llama-swap/internal/process"
	"github.com/mostlygeek/llama-swap/internal/swaputil"
)

// fakeRouter records StopModels and Wake calls.
type fakeRouter struct {
	mu      sync.Mutex
	running map[string]process.ProcessState
	stopped [][]string
	wakes   int
}

func (f *fakeRouter) RunningModels() map[string]process.ProcessState {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]process.ProcessState, len(f.running))
	for k, v := range f.running {
		out[k] = v
	}
	return out
}

func (f *fakeRouter) StopModels(ids ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stopped = append(f.stopped, ids)
	for _, id := range ids {
		delete(f.running, id)
	}
}

func (f *fakeRouter) Wake() {
	f.mu.Lock()
	f.wakes++
	f.mu.Unlock()
}

func (f *fakeRouter) snapshot() ([][]string, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]string(nil), f.stopped...), f.wakes
}

// switchServer serves 200 {"on": <state>} for a toggleable state.
func switchServer(t *testing.T, on *atomic.Bool) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if on.Load() {
			io.WriteString(w, `{"on": true}`)
		} else {
			io.WriteString(w, `{"on": false}`)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func twoTenants(conditionURL string, onBlocked string) map[string]config.TenantConfig {
	return map[string]config.TenantConfig{
		"hi": {
			Members:   []string{"hi-model"},
			Priority:  10,
			Condition: &config.TenantProbe{URL: conditionURL, Method: "GET", Status: 200, JSON: "on"},
			Interval:  1,
			OnBlocked: config.TenantOnBlockedHold,
		},
		"lo": {
			Members:   []string{"lo-model"},
			Priority:  1,
			Interval:  1,
			OnBlocked: onBlocked,
		},
	}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestTenants_LowerTenantHeldOrRefusedWhileHigherWantsGPU(t *testing.T) {
	for _, onBlocked := range []string{config.TenantOnBlockedHold, config.TenantOnBlockedRefuse} {
		t.Run(onBlocked, func(t *testing.T) {
			var on atomic.Bool
			srv := switchServer(t, &on)
			m := New(twoTenants(srv.URL, onBlocked), logmon.NewWriter(io.Discard))
			r := &fakeRouter{running: map[string]process.ProcessState{}}
			m.Start(t.Context(), r)

			if reason, _ := m.Block("lo-model", nil); reason != nil {
				t.Fatalf("lo blocked while hi does not want the GPU: %v", reason)
			}

			on.Store(true)
			eventually(t, "hi to want the GPU", func() bool { reason, _ := m.Block("lo-model", nil); return reason != nil })
			reason, refuse := m.Block("lo-model", nil)
			if refuse != (onBlocked == config.TenantOnBlockedRefuse) {
				t.Fatalf("refuse=%v for onBlocked=%s", refuse, onBlocked)
			}
			var httpErr swaputil.HTTPError
			if !errors.As(reason, &httpErr) || httpErr.StatusCode() != http.StatusServiceUnavailable {
				t.Fatalf("reason %v is not a 503 HTTPError", reason)
			}
			if !strings.Contains(reason.Error(), "tenant hi (priority 10)") {
				t.Fatalf("reason %q does not name the blocking tenant", reason)
			}
			if blocked, _ := m.Block("hi-model", nil); blocked != nil {
				t.Fatalf("hi blocked by its own condition: %v", blocked)
			}
			if blocked, _ := m.Block("untenanted", nil); blocked != nil {
				t.Fatalf("untenanted model blocked: %v", blocked)
			}

			on.Store(false)
			eventually(t, "lo to be released", func() bool { reason, _ := m.Block("lo-model", nil); return reason == nil })
			if _, wakes := r.snapshot(); wakes < 2 {
				t.Fatalf("wakes=%d want >= 2 (one per condition change)", wakes)
			}
		})
	}
}

func TestTenants_HigherWantsGPUStopsRunningLowerTenantFirst(t *testing.T) {
	var on atomic.Bool
	srv := switchServer(t, &on)
	m := New(twoTenants(srv.URL, config.TenantOnBlockedHold), logmon.NewWriter(io.Discard))
	r := &fakeRouter{running: map[string]process.ProcessState{"lo-model": process.StateReady}}
	m.Start(t.Context(), r)

	on.Store(true)
	eventually(t, "lo to be stopped", func() bool { s, _ := r.snapshot(); return len(s) == 1 })
	stopped, _ := r.snapshot()
	if len(stopped[0]) != 1 || stopped[0][0] != "lo-model" {
		t.Fatalf("stopped %v want [[lo-model]]", stopped)
	}
	if blocked, _ := m.Block("hi-model", nil); blocked != nil {
		t.Fatalf("hi still blocked after lo stopped: %v", blocked)
	}
}

func TestTenants_HigherTenantWaitsForLowerToStop(t *testing.T) {
	m := New(twoTenants("http://127.0.0.1:1", config.TenantOnBlockedHold), logmon.NewWriter(io.Discard))
	r := &fakeRouter{running: map[string]process.ProcessState{"lo-model": process.StateReady}}
	m.mu.Lock()
	m.router = r
	m.byModel["hi-model"].wants = true
	m.mu.Unlock()

	if blocked, refuse := m.Block("hi-model", nil); blocked == nil || refuse {
		t.Fatalf("hi-model Block = %v, refuse %v; want held while lo-model runs", blocked, refuse)
	}
	r.StopModels("lo-model")
	if blocked, _ := m.Block("hi-model", nil); blocked != nil {
		t.Fatalf("hi-model still blocked after lo-model stopped: %v", blocked)
	}
}

func TestTenants_DrainWaitsForBusyThenRunsAction(t *testing.T) {
	var mu sync.Mutex
	var events []string
	record := func(e string) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	}
	var busyPolls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/queue":
			n := busyPolls.Add(1)
			if n <= 2 {
				record("busy")
				io.WriteString(w, `{"queue_running": [["job"]]}`)
				return
			}
			record("idle")
			io.WriteString(w, `{"queue_running": []}`)
		case "/free":
			body, _ := io.ReadAll(r.Body)
			record(r.Method + " /free " + string(body))
		}
	}))
	t.Cleanup(srv.Close)

	m := New(map[string]config.TenantConfig{
		"lo": {
			Members:  []string{"lo-model"},
			Priority: 1,
			Busy:     &config.TenantProbe{URL: srv.URL + "/queue", Method: "GET", Status: 200, JSON: "queue_running"},
			Drain:    &config.TenantAction{URL: srv.URL + "/free", Method: "POST", Body: `{"free_memory":true}`},
			Interval: 1,
		},
	}, logmon.NewWriter(io.Discard))
	m.tenants[0].interval = 20 * time.Millisecond

	hook := m.StopHook("lo-model")
	if hook == nil {
		t.Fatal("no stop hook for a tenant with busy and drain")
	}
	if m.StopHook("untenanted") != nil {
		t.Fatal("stop hook for an untenanted model")
	}

	// Two concurrent stops share one drain.
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() { defer wg.Done(); hook(5 * time.Second) }()
	}
	wg.Wait()
	record("stop")

	want := []string{"busy", "busy", "idle", `POST /free {"free_memory":true}`, "stop"}
	if strings.Join(events, "|") != strings.Join(want, "|") {
		t.Fatalf("events %q\nwant   %q", events, want)
	}
}

func TestTenants_DrainBoundedByUnloadTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"busy": true}`)
	}))
	t.Cleanup(srv.Close)
	m := New(map[string]config.TenantConfig{
		"lo": {Members: []string{"lo-model"}, Priority: 1, Interval: 1,
			Busy: &config.TenantProbe{URL: srv.URL, Method: "GET", Status: 200, JSON: "busy"}},
	}, logmon.NewWriter(io.Discard))
	m.tenants[0].interval = 20 * time.Millisecond

	start := time.Now()
	m.StopHook("lo-model")(200 * time.Millisecond)
	if took := time.Since(start); took < 200*time.Millisecond || took > 2*time.Second {
		t.Fatalf("drain of a tenant that stays busy took %s, want about the 200ms unloadTimeout", took)
	}
}

func TestTenants_CommandProbe(t *testing.T) {
	ctx := context.Background()
	if res := runProbe(ctx, &config.TenantProbe{Cmd: "true"}); !res.ok {
		t.Fatalf("true probe = %s", res)
	}
	if res := runProbe(ctx, &config.TenantProbe{Cmd: "false"}); res.ok || res.raw != "exit 1" {
		t.Fatalf("false probe = %s, want false (exit 1)", res)
	}
}

func TestTenants_JSONTruthy(t *testing.T) {
	cases := []struct {
		body, path string
		want       bool
	}{
		{`{"a": {"b": true}}`, "a.b", true},
		{`{"a": {"b": false}}`, ".a.b", false},
		{`{"q": [1]}`, "q", true},
		{`{"q": []}`, "q", false},
		{`{"n": 0}`, "n", false},
		{`{"s": "x"}`, "s", true},
		{`{"l": [{"k": 2}]}`, "l.0.k", true},
		{`{}`, "missing.key", false},
	}
	for _, c := range cases {
		v, err := lookupJSON([]byte(c.body), c.path)
		if err != nil {
			t.Fatalf("%s %s: %v", c.body, c.path, err)
		}
		if truthy(v) != c.want {
			t.Errorf("%s at %s: truthy=%v want %v", c.body, c.path, truthy(v), c.want)
		}
	}
}
