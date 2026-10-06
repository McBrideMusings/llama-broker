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
	"github.com/mostlygeek/llama-swap/internal/event"
	"github.com/mostlygeek/llama-swap/internal/logmon"
	"github.com/mostlygeek/llama-swap/internal/process"
	"github.com/mostlygeek/llama-swap/internal/swaputil"
)

func TestTenants_DecisionsLoggedAndEmitted(t *testing.T) {
	var mu sync.Mutex
	var got []Decision
	cancel := event.On(func(e TenantDecisionEvent) {
		mu.Lock()
		got = append(got, e.Decision)
		mu.Unlock()
	})
	defer cancel()
	decided := func(a Action) bool {
		mu.Lock()
		defer mu.Unlock()
		for _, d := range got {
			if d.Action == a {
				return true
			}
		}
		return false
	}

	var on atomic.Bool
	on.Store(true)
	srv := switchServer(t, &on)
	log := logmon.NewWriter(io.Discard)
	m := New(twoTenants(srv.URL, config.TenantOnBlockedHold), log)
	r := &fakeRouter{running: map[string]process.ProcessState{"lo-model": process.StateReady}}
	m.Start(t.Context(), r)

	eventually(t, "stop decision", func() bool { return decided(ActionStop) })
	reason, refuse := m.Block(t.Context(), "lo-model", nil)
	m.Record("lo-model", reason, refuse)
	eventually(t, "hi-model admitted", func() bool { r, _ := m.Block(t.Context(), "hi-model", nil); return r == nil })
	event.Emit(swaputil.ProcessStateChangeEvent{ProcessName: "hi-model", OldState: "stopped", NewState: "starting"})
	eventually(t, "hold and load decisions", func() bool { return decided(ActionHold) && decided(ActionLoad) })

	history := string(log.GetHistory())
	for _, want := range []string{
		`tenant=lo model= action=stop probe="condition of hi: true (HTTP 200, on=true)" reason="stopping [lo-model] to make room for tenant hi (priority 10)"`,
		`tenant=lo model=lo-model action=hold probe="condition of hi: true (HTTP 200, on=true)" reason="model lo-model (tenant lo, priority 1) is blocked: tenant hi (priority 10) wants the GPU"`,
		`tenant=hi model=hi-model action=load probe="condition of hi: true (HTTP 200, on=true)" reason="no higher-priority tenant wants the GPU"`,
	} {
		if !strings.Contains(history, want) {
			t.Errorf("log lacks %q\nlog:\n%s", want, history)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	for _, d := range got {
		if d.Time.IsZero() || d.Tenant == "" || d.Reason == "" {
			t.Errorf("event decision missing time, tenant or reason: %+v", d)
		}
	}
}

func TestTenants_LoadDecisionTakesReadingAtGatePass(t *testing.T) {
	var on atomic.Bool
	srv := switchServer(t, &on)
	log := logmon.NewWriter(io.Discard)
	m := New(twoTenants(srv.URL, config.TenantOnBlockedHold), log)
	m.Start(t.Context(), &fakeRouter{running: map[string]process.ProcessState{}})

	// Both models pass the gate while hi's condition reads false, then hi
	// turns true before their start events arrive. lo-model passes only once
	// hi's first probe has finished, so it goes first.
	for _, model := range []string{"lo-model", "hi-model"} {
		eventually(t, model+" admitted", func() bool { r, _ := m.Block(t.Context(), model, nil); return r == nil })
	}
	on.Store(true)
	eventually(t, "hi to want the GPU", func() bool { return wantsGPU(m, "hi") })
	for _, model := range []string{"hi-model", "lo-model"} {
		event.Emit(swaputil.ProcessStateChangeEvent{ProcessName: model, OldState: "stopped", NewState: "starting"})
	}
	logged := func(line string) func() bool {
		return func() bool { return strings.Contains(string(log.GetHistory()), line) }
	}
	eventually(t, "hi load at its gate-pass reading", logged(
		`tenant=hi model=hi-model action=load probe="condition of hi: false (HTTP 200, on=false)" reason="no higher-priority tenant wants the GPU"`))
	eventually(t, "lo load with the gate-pass reason", logged(
		`tenant=lo model=lo-model action=load probe="condition of lo: none configured" reason="no higher-priority tenant wants the GPU"`))

	// The pass is spent on the start it admitted.
	event.Emit(swaputil.ProcessStateChangeEvent{ProcessName: "lo-model", OldState: "stopped", NewState: "starting"})
	eventually(t, "start without a gate pass", logged(
		`tenant=lo model=lo-model action=load probe="" reason="started without passing the tenant gate"`))
}

func TestTenants_StatusReportsProbesLoadedAndHeld(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"on": true, "busy": false}`)
	}))
	t.Cleanup(srv.Close)
	cfgs := twoTenants(srv.URL, config.TenantOnBlockedRefuse)
	lo := cfgs["lo"]
	lo.Busy = &config.TenantProbe{URL: srv.URL, Method: "GET", Status: 200, JSON: "busy"}
	cfgs["lo"] = lo
	m := New(cfgs, logmon.NewWriter(io.Discard))
	m.Start(t.Context(), &fakeRouter{running: map[string]process.ProcessState{}})
	eventually(t, "hi to want the GPU", func() bool { return wantsGPU(m, "hi") })
	m.StopHook("lo-model")(time.Second)

	st := m.Status(map[string]process.ProcessState{"hi-model": process.StateReady}, map[string]int{"lo-model": 2})
	if len(st.Tenants) != 2 || st.Tenants[0].Name != "hi" || st.Tenants[1].Name != "lo" {
		t.Fatalf("tenants %+v, want hi then lo", st.Tenants)
	}
	hi, lo2 := st.Tenants[0], st.Tenants[1]
	if !hi.WantsGPU || hi.Condition == nil || hi.Condition.Result == nil || !*hi.Condition.Result ||
		hi.Condition.Raw != "HTTP 200, on=true" || hi.Condition.ProbedAt == nil {
		t.Errorf("hi condition = %+v, wantsGPU %v; want true, HTTP 200, on=true, with a probe time", hi.Condition, hi.WantsGPU)
	}
	if hi.Busy != nil || len(hi.Loaded) != 1 || hi.Loaded[0] != (LoadedModel{Model: "hi-model", State: "ready"}) || hi.Held != 0 {
		t.Errorf("hi busy %+v loaded %+v held %d; want no busy probe, hi-model ready, 0 held", hi.Busy, hi.Loaded, hi.Held)
	}
	if lo2.WantsGPU || lo2.Condition != nil || lo2.OnBlocked != config.TenantOnBlockedRefuse || lo2.Held != 2 || len(lo2.Loaded) != 0 {
		t.Errorf("lo = %+v; want no condition, refuse, 2 held, nothing loaded", lo2)
	}
	if lo2.Busy == nil || lo2.Busy.Result == nil || *lo2.Busy.Result || lo2.Busy.Raw != "HTTP 200, busy=false" {
		t.Errorf("lo busy = %+v; want the drain's reading false (HTTP 200, busy=false)", lo2.Busy)
	}
}
