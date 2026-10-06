package tenants

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mostlygeek/llama-swap/internal/config"
	"github.com/mostlygeek/llama-swap/internal/logmon"
)

func TestTenants_DrainOncePerStopEpisode(t *testing.T) {
	var probes, frees atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/free" {
			frees.Add(1)
			return
		}
		probes.Add(1)
		io.WriteString(w, `{"busy": true}`)
	}))
	t.Cleanup(srv.Close)
	log := logmon.New()
	m := New(map[string]config.TenantConfig{
		"lo": {Members: []string{"a", "b"}, Priority: 1, Interval: 1,
			Busy:  &config.TenantProbe{URL: srv.URL + "/queue", Method: "GET", Status: 200, JSON: "busy"},
			Drain: &config.TenantAction{URL: srv.URL + "/free", Method: "POST"}},
	}, log)
	m.tenants[0].interval = 20 * time.Millisecond

	end := m.BeginStops(map[string]time.Duration{"a": 100 * time.Millisecond, "b": 300 * time.Millisecond})
	start := time.Now()
	m.StopHook("a")(100 * time.Millisecond)
	if took := time.Since(start); took < 300*time.Millisecond || took > 2*time.Second {
		t.Fatalf("drain took %s, want about 300ms, the largest unloadTimeout in the episode", took)
	}
	n := probes.Load()
	// b's stop starts after a's drain finished but is in the same episode.
	m.StopHook("b")(300 * time.Millisecond)
	if probes.Load() != n || frees.Load() != 0 {
		t.Fatalf("b's stop probed %d more times and ran the action %d times, want neither", probes.Load()-n, frees.Load())
	}
	if drains := m.Status(nil, nil).Tenants[0].Drains; drains != 1 {
		t.Fatalf("drains=%d want 1 for one episode", drains)
	}
	if history := string(log.GetHistory()); !strings.Contains(history, "not draining again: the stop of a drained this tenant") {
		t.Fatalf("log is missing b's shared-drain step:\n%s", history)
	}
	end()

	m.StopHook("b")(50 * time.Millisecond)
	if drains := m.Status(nil, nil).Tenants[0].Drains; drains != 2 {
		t.Fatalf("drains=%d want 2: a stop after the episode closed drains again", drains)
	}
}

func TestTenants_StopOutsideOpenEpisodeDrains(t *testing.T) {
	var frees atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/free" {
			frees.Add(1)
			return
		}
		io.WriteString(w, `{"busy": false}`)
	}))
	t.Cleanup(srv.Close)
	m := New(map[string]config.TenantConfig{
		"lo": {Members: []string{"a", "b"}, Priority: 1, Interval: 1,
			Busy:  &config.TenantProbe{URL: srv.URL + "/queue", Method: "GET", Status: 200, JSON: "busy"},
			Drain: &config.TenantAction{URL: srv.URL + "/free", Method: "POST"}},
	}, logmon.NewWriter(io.Discard))

	// A batch stopping only a is still open when b's ttl unload arrives.
	end := m.BeginStops(map[string]time.Duration{"a": time.Second})
	defer end()
	m.StopHook("a")(time.Second)
	m.StopHook("b")(time.Second)
	if n := frees.Load(); n != 2 {
		t.Fatalf("drain action ran %d times, want 2: b is not in a's stop batch", n)
	}
}

func TestTenants_DrainActionSharesDeadlineWithBusyWait(t *testing.T) {
	var idle atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/free" {
			<-r.Context().Done()
			return
		}
		if idle.Load() {
			io.WriteString(w, `{"busy": false}`)
		} else {
			io.WriteString(w, `{"busy": true}`)
		}
	}))
	t.Cleanup(srv.Close)
	log := logmon.New()
	m := busyTenant(srv.URL, log)
	time.AfterFunc(150*time.Millisecond, func() { idle.Store(true) })

	start := time.Now()
	m.StopHook("lo-model")(300 * time.Millisecond)
	if took := time.Since(start); took < 300*time.Millisecond || took > 450*time.Millisecond {
		t.Fatalf("drain with a hanging action took %s, want about the 300ms unloadTimeout, not busy wait plus a fresh 300ms", took)
	}
	if history := string(log.GetHistory()); !strings.Contains(history, "drain action failed: ") {
		t.Fatalf("log is missing the action cut off by the deadline:\n%s", history)
	}
}

func TestTenants_CommandProbeKillsChildAtDeadline(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("uses sh")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	// The backgrounded sleep inherits the output pipe and outlives sh.
	res := runProbe(ctx, &config.TenantProbe{Cmd: `sh -c "sleep 5 & sleep 5"`})
	if took := time.Since(start); took > 500*time.Millisecond {
		t.Fatalf("probe took %s, want its child killed at the 100ms deadline", took)
	}
	if res.err == nil {
		t.Fatalf("probe %v cut off by its deadline has no error", res)
	}
}

// busyServer answers {"busy": <busy>} on /queue and counts posts to /free.
func busyServer(t *testing.T, busy *atomic.Bool, freed *atomic.Int32) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/free" {
			freed.Add(1)
			return
		}
		if busy.Load() {
			io.WriteString(w, `{"busy": true}`)
		} else {
			io.WriteString(w, `{"busy": false}`)
		}
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestTenants_StopJoiningARunningDrainMarksItsEpisodeDrained(t *testing.T) {
	var busy atomic.Bool
	var freed atomic.Int32
	busy.Store(true)
	m := busyTenant(busyServer(t, &busy, &freed), logmon.New())
	hook := m.StopHook("lo-model")

	// A stop outside any episode (a ttl unload) starts a drain.
	ttl := make(chan struct{})
	go func() { hook(5 * time.Second); close(ttl) }()
	for m.Status(nil, nil).Tenants[0].Drains != 1 {
		time.Sleep(5 * time.Millisecond)
	}
	// An unload opens an episode and its first stop joins that drain.
	end := m.BeginStops(map[string]time.Duration{"lo-model": 5 * time.Second})
	defer end()
	joined := make(chan struct{})
	go func() { hook(5 * time.Second); close(joined) }()
	time.Sleep(50 * time.Millisecond)
	busy.Store(false)
	<-ttl
	<-joined

	// The unload's next stop of the model finds the episode drained.
	hook(5 * time.Second)
	if drains, n := m.Status(nil, nil).Tenants[0].Drains, freed.Load(); drains != 1 || n != 1 {
		t.Fatalf("drains %d, drain actions %d; want 1 and 1: the joined drain covers the episode", drains, n)
	}
}

func TestTenants_ShutdownLimitBoundsAWaitOnARunningDrain(t *testing.T) {
	var busy atomic.Bool
	var freed atomic.Int32
	busy.Store(true)
	log := logmon.New()
	m := busyTenant(busyServer(t, &busy, &freed), log)
	hook := m.StopHook("lo-model")

	// An unload's drain runs with the model's 5s unloadTimeout.
	go hook(5 * time.Second)
	for m.Status(nil, nil).Tenants[0].Drains != 1 {
		time.Sleep(5 * time.Millisecond)
	}
	// Shutdown's stop of the same model waits on it for at most its 200ms limit.
	end := m.BeginStopsWithin(map[string]time.Duration{"lo-model": 5 * time.Second}, 200*time.Millisecond)
	defer end()
	start := time.Now()
	hook(5 * time.Second)
	if took := time.Since(start); took > time.Second {
		t.Fatalf("shutdown's stop waited %s on the running drain, want about the 200ms limit", took)
	}
	if !strings.Contains(string(log.GetHistory()), "the drain started by the stop of lo-model is still running after 200ms, stopping anyway") {
		t.Fatalf("log is missing the stopping-anyway line:\n%s", log.GetHistory())
	}
	busy.Store(false)
}

func TestTenants_ShutdownLimitCapsTheDrain(t *testing.T) {
	var busy atomic.Bool
	var freed atomic.Int32
	busy.Store(true)
	m := busyTenant(busyServer(t, &busy, &freed), logmon.New())

	end := m.BeginStopsWithin(map[string]time.Duration{"lo-model": 5 * time.Second}, 200*time.Millisecond)
	defer end()
	start := time.Now()
	m.StopHook("lo-model")(5 * time.Second)
	if took := time.Since(start); took > time.Second {
		t.Fatalf("drain took %s under a 200ms shutdown limit", took)
	}
}
