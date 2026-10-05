package tenants

import (
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
)

// busyTenant is one tenant whose busy probe reads {"busy": ...} from url and
// whose drain action posts to url/free, probing every 20ms.
func busyTenant(url string, log *logmon.Monitor) *Manager {
	m := New(map[string]config.TenantConfig{
		"lo": {Members: []string{"lo-model"}, Priority: 1, Interval: 1,
			Busy:  &config.TenantProbe{URL: url + "/queue", Method: "GET", Status: 200, JSON: "busy"},
			Drain: &config.TenantAction{URL: url + "/free", Method: "POST"}},
	}, log)
	m.tenants[0].interval = 20 * time.Millisecond
	return m
}

func TestTenants_DrainBusyProbeErrorCountsAsBusy(t *testing.T) {
	var mu sync.Mutex
	var events []string
	record := func(e string) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	}
	var polls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/free" {
			record("free")
			return
		}
		switch polls.Add(1) {
		case 1:
			record("busy")
			io.WriteString(w, `{"busy": true}`)
		case 2:
			record("not json")
			io.WriteString(w, `<html>502</html>`)
		case 3:
			record("slow")
			select {
			case <-r.Context().Done():
			case <-time.After(2 * time.Second):
			}
		default:
			record("idle")
			io.WriteString(w, `{"busy": false}`)
		}
	}))
	t.Cleanup(srv.Close)
	log := logmon.New()
	m := busyTenant(srv.URL, log)

	m.StopHook("lo-model")(5 * time.Second)

	want := []string{"busy", "not json", "slow", "idle", "free"}
	if strings.Join(events, "|") != strings.Join(want, "|") {
		t.Fatalf("events %q\nwant   %q", events, want)
	}
	busy := *m.Status(nil, nil).Tenants[0].Busy
	if busy.Result == nil || *busy.Result || busy.Errors != 0 || !strings.Contains(busy.LastError, "context deadline exceeded") || busy.LastErrorAt == nil {
		t.Fatalf("busy status %+v, want result false, errors 0 and the slow probe as lastError", busy)
	}
	history := string(log.GetHistory())
	for _, line := range []string{"busy probe failed: ", "treating as busy, waiting (1 in a row)", "treating as busy, waiting (2 in a row)"} {
		if !strings.Contains(history, line) {
			t.Fatalf("log is missing %q:\n%s", line, history)
		}
	}
}

func TestTenants_DrainBusyProbeFailingBoundedByUnloadTimeout(t *testing.T) {
	var freed atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/free" {
			freed.Store(true)
			return
		}
		panic(http.ErrAbortHandler)
	}))
	t.Cleanup(srv.Close)
	log := logmon.New()
	m := busyTenant(srv.URL, log)

	start := time.Now()
	m.StopHook("lo-model")(200 * time.Millisecond)
	if took := time.Since(start); took < 200*time.Millisecond || took > 2*time.Second {
		t.Fatalf("drain with a failing busy probe took %s, want about the 200ms unloadTimeout", took)
	}
	if freed.Load() {
		t.Fatal("drain action ran after the busy wait spent unloadTimeout")
	}
	busy := *m.Status(nil, nil).Tenants[0].Busy
	if busy.Result != nil || busy.Errors < 2 || busy.LastError == "" {
		t.Fatalf("busy status %+v, want result null and the failures counted", busy)
	}
	history := string(log.GetHistory())
	for _, line := range []string{"busy probe still failing after 200ms", "drain action skipped: no time left of unloadTimeout 200ms"} {
		if !strings.Contains(history, line) {
			t.Fatalf("log is missing %q:\n%s", line, history)
		}
	}
}

func TestTenants_DrainBusyProbeCutByDeadlineIsNotAFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/free" {
			<-r.Context().Done()
		}
	}))
	t.Cleanup(srv.Close)
	log := logmon.New()
	m := busyTenant(srv.URL, log)
	m.tenants[0].interval = 60 * time.Millisecond

	// Probes at 0-60ms and, after a 60ms sleep, at 120-160ms: the second gets
	// 40ms and is cut off by the drain's 160ms deadline.
	m.StopHook("lo-model")(160 * time.Millisecond)

	if busy := *m.Status(nil, nil).Tenants[0].Busy; busy.Errors != 1 {
		t.Fatalf("busy status %+v, want 1 failure: the probe cut off by the drain deadline is not one", busy)
	}
	history := string(log.GetHistory())
	if !strings.Contains(history, "busy probe still failing after 160ms (1 in a row), stopping anyway") {
		t.Fatalf("log is missing the stopping-anyway warning for the one failure:\n%s", history)
	}
	if n := strings.Count(history, "treating as busy, waiting"); n != 1 {
		t.Fatalf("logged %d waiting lines, want 1 (none in the round that stops):\n%s", n, history)
	}
}
