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

// TestTenants_EndToEndDrainStopLoadOrder runs two real upstreams (the
// simple-responder binary) as tenants. lo is loaded and reports busy for two
// seconds; when hi's condition turns true, lo must drain (wait out busy, run its
// drain action), stop, and only then may hi load. A lo request sent while hi
// wants the GPU is held, and runs once hi's condition clears.
func TestTenants_EndToEndDrainStopLoadOrder(t *testing.T) {
	responder := filepath.Join("..", "..", "build", fmt.Sprintf("simple-responder_%s_%s", runtime.GOOS, runtime.GOARCH))
	if runtime.GOOS == "windows" {
		responder = filepath.Join("..", "..", "build", "simple-responder.exe")
	}
	if _, err := os.Stat(responder); err != nil {
		t.Skipf("simple-responder not found at %s, run `make simple-responder`", responder)
	}
	responder, _ = filepath.Abs(responder)

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
		"tenants: stopping lo (priority 1) [lo]",
		"tenants: draining lo before stopping lo",
		"tenants: lo busy",
		"tenants: lo idle",
		"tenants: lo drain action done",
		"tenants: lo drained, stopping lo",
		"tenants: lo stopped [lo]",
		"<hi> Health check passed",
		"tenants: hi (priority 10) condition false",
		"<lo> Health check passed",
	}
	for _, held := range []string{
		"holding request for model hi: tenant hi waits for lower tenant lo to stop",
		"holding request for model lo: model lo (tenant lo, priority 1) is blocked: tenant hi (priority 10) wants the GPU",
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
