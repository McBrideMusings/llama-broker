package router

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mostlygeek/llama-swap/internal/config"
	"github.com/mostlygeek/llama-swap/internal/logmon"
)

// responderPath returns the built simple-responder binary, skipping the test
// when it is missing.
func responderPath(t *testing.T) string {
	t.Helper()
	responder := filepath.Join("..", "..", "build", fmt.Sprintf("simple-responder_%s_%s", runtime.GOOS, runtime.GOARCH))
	if runtime.GOOS == "windows" {
		responder = filepath.Join("..", "..", "build", "simple-responder.exe")
	}
	if _, err := os.Stat(responder); err != nil {
		t.Skipf("simple-responder not found at %s, run `make simple-responder`", responder)
	}
	responder, _ = filepath.Abs(responder)
	return responder
}

// TestTenants_EndToEndDrainStopLoadOrder runs two real upstreams (the
// simple-responder binary) as tenants. lo is loaded and reports busy for two
// seconds; when hi's condition turns true, lo must drain (wait out busy, run its
// drain action), stop, and only then may hi load. A lo request sent while hi
// wants the GPU is held, and runs once hi's condition clears.
func TestTenants_EndToEndDrainStopLoadOrder(t *testing.T) {
	responder := responderPath(t)

	var hiWants atomic.Bool
	var busyUntil atomic.Int64
	var drained atomic.Int32
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/condition":
			fmt.Fprintf(w, `{"on": %v}`, hiWants.Load())
		case "/queue":
			fmt.Fprintf(w, `{"busy": %v}`, time.Now().UnixNano() < busyUntil.Load())
		case "/free":
			drained.Add(1)
			io.WriteString(w, `{}`)
		}
	}))
	defer control.Close()

	cfg, err := config.LoadConfigFromReader(strings.NewReader(fmt.Sprintf(`
logLevel: info
unloadTimeout: 10
models:
  lo:
    cmd: %[1]s --port ${PORT} --silent --respond lo
  hi:
    cmd: %[1]s --port ${PORT} --silent --respond hi
groups:
  lo-group: {members: [lo], exclusive: false, swap: false}
  hi-group: {members: [hi], exclusive: false, swap: false}
tenants:
  hi:
    models: [hi]
    priority: 10
    interval: 1
    condition: {url: "%[2]s/condition", json: "on"}
  lo:
    models: [lo]
    priority: 1
    interval: 1
    busy: {url: "%[2]s/queue", json: "busy"}
    drain: {url: "%[2]s/free"}
`, responder, control.URL)))
	if err != nil {
		t.Fatalf("config: %v", err)
	}

	logs := logmon.NewGroup(io.Discard, false, false, false)
	rt, err := NewGroup(cfg, logs)
	if err != nil {
		t.Fatalf("NewGroup: %v", err)
	}
	defer rt.Shutdown(5 * time.Second)

	chat := func(model string) (int, string) {
		body := fmt.Sprintf(`{"model": %q, "messages": [{"role": "user", "content": "x"}]}`, model)
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		rt.ServeHTTP(rec, req)
		return rec.Code, rec.Body.String()
	}

	if code, body := chat("lo"); code != http.StatusOK {
		t.Fatalf("lo before hi wants the GPU: %d %s", code, body)
	}

	busyUntil.Store(time.Now().Add(2 * time.Second).UnixNano())
	hiWants.Store(true)
	flipped := time.Now()
	// The condition is polled, so hi wants the GPU only once a poll saw it.
	for !strings.Contains(string(logs.ProxyLogs.GetHistory()), "condition true") {
		if time.Since(flipped) > 5*time.Second {
			t.Fatal("poller never saw hi's condition turn true")
		}
		time.Sleep(20 * time.Millisecond)
	}

	type result struct {
		code int
		at   time.Time
	}
	send := func(model string) <-chan result {
		ch := make(chan result, 1)
		go func() {
			code, _ := chat(model)
			ch <- result{code, time.Now()}
		}()
		return ch
	}

	hiCh := send("hi")
	time.Sleep(500 * time.Millisecond)
	loCh := send("lo")

	var hi result
	select {
	case hi = <-hiCh:
	case <-time.After(15 * time.Second):
		t.Fatal("hi request never finished")
	}
	select {
	case lo := <-loCh:
		t.Fatalf("lo request finished (%d) while hi wants the GPU; want it held", lo.code)
	case <-time.After(time.Second):
	}
	// The run loop publishes the held count; the status reads it from there.
	for _, ts := range rt.TenantStatus().Tenants {
		switch {
		case ts.Name == "lo" && (ts.Held != 1 || len(ts.Loaded) != 0):
			t.Errorf("status lo held=%d loaded=%v, want 1 held and nothing loaded", ts.Held, ts.Loaded)
		case ts.Name == "hi" && (!ts.WantsGPU || len(ts.Loaded) != 1 || ts.Held != 0):
			t.Errorf("status hi wantsGPU=%v loaded=%v held=%d, want true, [hi], 0", ts.WantsGPU, ts.Loaded, ts.Held)
		}
	}
	cleared := time.Now()
	hiWants.Store(false)
	var lo result
	select {
	case lo = <-loCh:
	case <-time.After(15 * time.Second):
		t.Fatal("held lo request never finished after hi's condition cleared")
	}

	if hi.code != http.StatusOK || lo.code != http.StatusOK {
		t.Fatalf("hi=%d lo=%d, want 200 and 200", hi.code, lo.code)
	}
	if took := hi.at.Sub(flipped); took < 2*time.Second {
		t.Errorf("hi served %s after its condition turned true; lo was busy for 2s, so it loaded before lo drained", took)
	}
	if lo.at.Before(cleared) {
		t.Errorf("held lo request finished before hi's condition cleared")
	}
	if drained.Load() != 1 {
		t.Errorf("drain action ran %d times, want 1", drained.Load())
	}

	history := string(logs.ProxyLogs.GetHistory())
	order := []string{
		"tenants: hi (priority 10) condition true",
		`tenant=lo model= action=stop probe="condition of hi: true (HTTP 200, on=true)" reason="stopping [lo] to make room for tenant hi (priority 10)"`,
		`tenant=lo model=lo action=drain probe="" reason="draining before stopping lo: preempted by a higher-priority tenant (unloadTimeout 10s)"`,
		`tenant=lo model=lo action=drain probe="busy true (HTTP 200, busy=true)" reason="busy, waiting"`,
		`tenant=lo model=lo action=drain probe="busy false (HTTP 200, busy=false)" reason="idle"`,
		`tenant=lo model=lo action=drain probe="" reason="drain action done: HTTP 200 {}"`,
		"tenants: lo drained, stopping lo",
		"tenants: lo stopped [lo]",
		`tenant=hi model=hi action=load probe="condition of hi: true (HTTP 200, on=true)" reason="no higher-priority tenant wants the GPU"`,
		"<hi> Health check passed",
		"tenants: hi (priority 10) condition false",
		`tenant=lo model=lo action=load probe="condition of lo: none configured" reason="no higher-priority tenant wants the GPU"`,
		"<lo> Health check passed",
	}
	for _, held := range []string{
		`tenant=hi model=hi action=hold probe="condition of hi: true (HTTP 200, on=true)" reason="tenant hi waits for lower tenant lo to stop [lo]"`,
		`tenant=lo model=lo action=hold probe="condition of hi: true (HTTP 200, on=true)" reason="model lo (tenant lo, priority 1) is blocked: tenant hi (priority 10) wants the GPU"`,
	} {
		if !strings.Contains(history, held) {
			t.Errorf("log has no %q; log:\n%s", held, history)
		}
	}
	pos := 0
	for _, want := range order {
		i := strings.Index(history[pos:], want)
		if i < 0 {
			t.Fatalf("log line %q missing or out of order after offset %d; log:\n%s", want, pos, history)
		}
		pos += i + len(want)
	}
}

// busyTenantRouter starts a Group router with two real upstreams: job, whose
// tenant reads busy from busy and posts its drain to /free, and other, with no
// tenant. Both are loaded before it returns.
func busyTenantRouter(t *testing.T, busy *atomic.Bool, drained *atomic.Int32, unloadTimeout int) (*Group, *logmon.Group, func(string) int) {
	t.Helper()
	responder := responderPath(t)
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/queue":
			fmt.Fprintf(w, `{"busy": %v}`, busy.Load())
		case "/free":
			drained.Add(1)
		}
	}))
	t.Cleanup(control.Close)

	cfg, err := config.LoadConfigFromReader(strings.NewReader(fmt.Sprintf(`
logLevel: info
unloadTimeout: %[3]d
models:
  job:
    cmd: %[1]s --port ${PORT} --silent --respond job
  other:
    cmd: %[1]s --port ${PORT} --silent --respond other
groups:
  job-group: {members: [job], exclusive: false, swap: false}
  other-group: {members: [other], exclusive: false, swap: false}
tenants:
  job:
    models: [job]
    priority: 1
    interval: 1
    busy: {url: "%[2]s/queue", json: "busy"}
    drain: {url: "%[2]s/free"}
`, responder, control.URL, unloadTimeout)))
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	logs := logmon.NewGroup(io.Discard, false, false, false)
	rt, err := NewGroup(cfg, logs)
	if err != nil {
		t.Fatalf("NewGroup: %v", err)
	}
	chat := func(model string) int {
		body := fmt.Sprintf(`{"model": %q, "messages": [{"role": "user", "content": "x"}]}`, model)
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		rt.ServeHTTP(rec, req)
		return rec.Code
	}
	for _, model := range []string{"job", "other"} {
		if code := chat(model); code != http.StatusOK {
			t.Fatalf("loading %s: %d", model, code)
		}
	}
	return rt, logs, chat
}

// TestTenants_UnloadDrainDoesNotBlockOtherRequests unloads a busy tenant's
// model and sends a request for another model during the drain: it is served
// at once, not after the drain.
func TestTenants_UnloadDrainDoesNotBlockOtherRequests(t *testing.T) {
	var busy atomic.Bool
	var drained atomic.Int32
	rt, logs, chat := busyTenantRouter(t, &busy, &drained, 10)
	defer rt.Shutdown(5 * time.Second)

	busy.Store(true)
	unloaded := make(chan struct{})
	go func() {
		rt.Unload(0, "job")
		close(unloaded)
	}()
	for !strings.Contains(string(logs.ProxyLogs.GetHistory()), "busy, waiting") {
		time.Sleep(20 * time.Millisecond)
	}

	start := time.Now()
	if code := chat("other"); code != http.StatusOK {
		t.Fatalf("other during job's drain: %d", code)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("other took %s during job's drain, want it served without waiting for the drain", took)
	}
	select {
	case <-unloaded:
		t.Fatal("unload returned while job was still busy")
	default:
	}

	busy.Store(false)
	select {
	case <-unloaded:
	case <-time.After(5 * time.Second):
		t.Fatal("unload never returned after job went idle")
	}
	if n := drained.Load(); n != 1 {
		t.Errorf("drain action ran %d times, want 1", n)
	}
	if _, running := rt.RunningModels()["job"]; running {
		t.Error("job still running after the unload returned")
	}
	history := string(logs.ProxyLogs.GetHistory())
	if !strings.Contains(history, `tenant=job model=job action=drain probe="" reason="not draining again: the stop of job drained this tenant in the same stop batch"`) {
		t.Errorf("the run loop's stop drained again; log:\n%s", history)
	}
}

// TestTenants_ShutdownCapsDrainAtItsTimeout shuts down while a tenant with a
// 60s unloadTimeout stays busy: the drain gives up at shutdown's 1s timeout.
func TestTenants_ShutdownCapsDrainAtItsTimeout(t *testing.T) {
	var busy atomic.Bool
	var drained atomic.Int32
	rt, logs, _ := busyTenantRouter(t, &busy, &drained, 60)

	busy.Store(true)
	start := time.Now()
	rt.Shutdown(time.Second)
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("shutdown took %s with a 1s timeout, want the drain capped at 1s", took)
	}
	history := string(logs.ProxyLogs.GetHistory())
	for _, line := range []string{"(unloadTimeout 1s)", "still busy after 1s, stopping anyway"} {
		if !strings.Contains(history, line) {
			t.Errorf("log is missing %q; log:\n%s", line, history)
		}
	}
}
