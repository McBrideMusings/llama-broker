package tenants

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mostlygeek/llama-swap/internal/config"
	"github.com/mostlygeek/llama-swap/internal/logmon"
	"github.com/mostlygeek/llama-swap/internal/process"
)

func idleTenants(conditionURL string) map[string]config.TenantConfig {
	cfgs := twoTenants(conditionURL, config.TenantOnBlockedHold)
	lo := cfgs["lo"]
	lo.IdleLoad = "lo-model"
	cfgs["lo"] = lo
	return cfgs
}

func TestTenants_IdleLoadWhenNothingElseRuns(t *testing.T) {
	var on atomic.Bool
	srv := switchServer(t, &on)
	m := New(idleTenants(srv.URL), logmon.NewWriter(io.Discard))
	// embed is resident (a persistent group member): it runs throughout and
	// never keeps the GPU from being idle.
	r := &fakeRouter{running: map[string]process.ProcessState{"hi-model": process.StateReady, "embed": process.StateReady}}
	m.SetResident([]string{"embed"})
	m.Start(t.Context(), r)

	var loads atomic.Int32
	m.StartIdleLoad(t.Context(), func(_ context.Context, model string) bool {
		if model != "lo-model" {
			t.Errorf("idle load of %s, want lo-model", model)
		}
		loads.Add(1)
		r.mu.Lock()
		r.running[model] = process.StateReady
		r.mu.Unlock()
		return true
	})

	time.Sleep(2500 * time.Millisecond)
	if n := loads.Load(); n != 0 {
		t.Fatalf("%d idle loads while hi-model runs, want 0", n)
	}

	// A model outside every tenant, or a queued request, also keeps the GPU
	// busy.
	r.mu.Lock()
	delete(r.running, "hi-model")
	r.running["untenanted"] = process.StateStarting
	r.mu.Unlock()
	time.Sleep(2500 * time.Millisecond)
	r.mu.Lock()
	delete(r.running, "untenanted")
	r.queued = 1
	r.mu.Unlock()
	time.Sleep(2500 * time.Millisecond)
	if n := loads.Load(); n != 0 {
		t.Fatalf("%d idle loads while an untenanted model ran or a request was queued, want 0", n)
	}

	r.mu.Lock()
	r.queued = 0
	r.mu.Unlock()
	eventually(t, "lo-model to idle-load once hi-model stopped", func() bool { return loads.Load() == 1 })

	on.Store(true)
	eventually(t, "hi to stop lo-model", func() bool { s, _ := r.snapshot(); return len(s) == 1 })
	time.Sleep(2500 * time.Millisecond)
	if n := loads.Load(); n != 1 {
		t.Fatalf("%d idle loads while hi wants the GPU, want 1", n)
	}

	on.Store(false)
	eventually(t, "lo-model to idle-load again after hi released the GPU", func() bool { return loads.Load() == 2 })
}

func TestTenants_IdleLoadRefusedNotHeld(t *testing.T) {
	m := New(idleTenants("http://127.0.0.1:1"), logmon.NewWriter(io.Discard))
	m.mu.Lock()
	m.byName("hi").wants = true
	m.mu.Unlock()

	ctx, mark := withIdleLoad(t.Context())
	reason, refuse := m.Block(ctx, "lo-model", nil)
	if reason == nil || !refuse || !mark.refused.Load() {
		t.Fatalf("idle load Block = %v, refuse %v, marked %v; want refused under onBlocked: hold", reason, refuse, mark.refused.Load())
	}
	if _, refuse := m.Block(t.Context(), "lo-model", nil); refuse {
		t.Fatalf("an ordinary request was refused; onBlocked: hold should hold it")
	}
}

func TestTenants_IdleLoadModelGoneSoonBacksOff(t *testing.T) {
	m := New(idleTenants("http://127.0.0.1:1"), logmon.NewWriter(io.Discard))
	r := &fakeRouter{running: map[string]process.ProcessState{}}
	m.mu.Lock()
	m.router = r
	m.byName("hi").condErrAt = time.Now()
	m.mu.Unlock()

	// Served, but the model never shows as running: gone at once, with
	// nothing else having run, as after a crash.
	var loads atomic.Int32
	m.StartIdleLoad(t.Context(), func(context.Context, string) bool { loads.Add(1); return true })

	// Loads at about 1s, then the 2s check finds it gone and waits 2s more;
	// without the uptime rule it would load again at 2s and 3s.
	time.Sleep(3500 * time.Millisecond)
	if n := loads.Load(); n != 1 {
		t.Fatalf("%d idle loads in 3.5s, want 1", n)
	}
}

func TestTenants_IdleLoadBacksOffAfterFailure(t *testing.T) {
	m := New(idleTenants("http://127.0.0.1:1"), logmon.NewWriter(io.Discard))
	r := &fakeRouter{running: map[string]process.ProcessState{}}
	m.mu.Lock()
	m.router = r
	m.byName("hi").condErrAt = time.Now() // probed (and failed): hi does not want the GPU
	m.mu.Unlock()

	var loads atomic.Int32
	m.StartIdleLoad(t.Context(), func(context.Context, string) bool { loads.Add(1); return false })

	// Tries at about 1s and 3s (1s interval, then doubled to 2s); the next is
	// at about 7s.
	time.Sleep(4500 * time.Millisecond)
	if n := loads.Load(); n != 2 {
		t.Fatalf("%d idle loads in 4.5s, want 2 (backoff 1s then 2s then 4s)", n)
	}
}
