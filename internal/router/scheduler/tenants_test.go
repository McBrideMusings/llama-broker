package scheduler

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mostlygeek/llama-swap/internal/process"
)

// gatedEffects is fakeEffects plus a programmable TenantGate.
type gatedEffects struct {
	*fakeEffects
	blocked   map[string]error
	refuse    map[string]bool
	records   []string                   // "hold lo", "refuse lo"
	alongside map[string][]string        // last alongside set per model
	ctx       map[string]context.Context // last request context per model
}

func (g *gatedEffects) TenantBlock(ctx context.Context, model string, alongside []string) (error, bool) {
	g.alongside[model] = alongside
	g.ctx[model] = ctx
	return g.blocked[model], g.refuse[model]
}

func (g *gatedEffects) TenantRecord(model string, reason error, refuse bool) {
	action := "hold"
	if refuse {
		action = "refuse"
	}
	g.records = append(g.records, action+" "+model)
}

func newGatedEffects() *gatedEffects {
	return &gatedEffects{fakeEffects: newFakeEffects(), blocked: map[string]error{}, refuse: map[string]bool{},
		alongside: map[string][]string{}, ctx: map[string]context.Context{}}
}

type ctxKey struct{}

func TestTenants_FIFOGateSeesWhatStaysLoaded(t *testing.T) {
	eff := newGatedEffects()
	eff.states["a"] = process.StateReady
	eff.states["b"] = process.StateReady
	eff.states["c"] = process.StateStopped
	s := newFIFO(&stubPlanner{evict: map[string][]string{"c": {"a"}}}, eff)

	r := reqCh("c")
	r.Ctx = context.WithValue(r.Ctx, ctxKey{}, "c's request")
	s.OnRequest(r)
	if got := eff.alongside["c"]; len(got) != 1 || got[0] != "b" {
		t.Fatalf("alongside c = %v, want [b]: a is evicted by c's swap", got)
	}
	if got := eff.ctx["c"]; got == nil || got.Value(ctxKey{}) != "c's request" {
		t.Fatalf("gate got ctx %v, want the request's own context", got)
	}
}

func TestTenants_FIFOHoldsBlockedModelUntilTenantsChange(t *testing.T) {
	eff := newGatedEffects()
	eff.states["lo"] = process.StateReady
	eff.blocked["lo"] = errors.New("tenant hi wants the GPU")
	s := newFIFO(&stubPlanner{}, eff)

	r := reqCh("lo")
	s.OnRequest(r)
	assertAdmitted(t, r)
	if got := eff.served("lo"); got != 0 {
		t.Fatalf("served lo %d times while blocked, want 0 (even on the fast path)", got)
	}
	if len(s.queued) != 1 {
		t.Fatalf("queued=%d want 1", len(s.queued))
	}

	// A wake while still blocked keeps the request held.
	s.OnTenantsChanged()
	if eff.served("lo") != 0 || len(s.queued) != 1 {
		t.Fatalf("after wake while blocked: served=%d queued=%d, want 0 and 1", eff.served("lo"), len(s.queued))
	}

	delete(eff.blocked, "lo")
	s.OnTenantsChanged()
	if got := eff.served("lo"); got != 1 {
		t.Fatalf("served lo %d times after the gate opened, want 1", got)
	}
	if len(s.queued) != 0 {
		t.Fatalf("queued=%d want 0", len(s.queued))
	}
}

func TestTenants_FIFORefusesWithReason(t *testing.T) {
	eff := newGatedEffects()
	eff.states["lo"] = process.StateStopped
	reason := errors.New("tenant hi wants the GPU")
	eff.blocked["lo"] = reason
	eff.refuse["lo"] = true
	s := newFIFO(&stubPlanner{}, eff)

	r := reqCh("lo")
	s.OnRequest(r)
	if err := admitErr(t, r); !errors.Is(err, reason) {
		t.Fatalf("admission err=%v want %v", err, reason)
	}
	if eff.startsFor("lo") != 0 || len(s.queued) != 0 {
		t.Fatalf("refused request started %d swaps and left %d queued, want 0 and 0", eff.startsFor("lo"), len(s.queued))
	}
	if len(s.reserved) != 0 {
		t.Fatalf("refused request kept a concurrency reservation: %v", s.reserved)
	}
}

func TestTenants_FIFOHeldModelDoesNotDelayLaterRequests(t *testing.T) {
	eff := newGatedEffects()
	eff.states["a"] = process.StateReady
	eff.states["lo"] = process.StateReady
	eff.states["hi"] = process.StateReady
	eff.blocked["lo"] = errors.New("tenant hi wants the GPU")
	s := newFIFO(&stubPlanner{}, eff)

	// The order startPreload sends hooks.on_startup.preload in.
	for _, model := range []string{"a", "lo", "hi"} {
		s.OnRequest(reqCh(model))
	}
	var served []string
	for _, g := range eff.grants {
		served = append(served, g.model)
	}
	if got, want := strings.Join(served, ","), "a,hi"; got != want {
		t.Fatalf("served %q want %q: hi must not wait on held lo", got, want)
	}
	if len(s.queued) != 1 || s.queued[0].Model != "lo" {
		t.Fatalf("queued=%v want [lo]", s.queued)
	}
}

func TestTenants_FIFORefusesQueuedRequestOnceBlocked(t *testing.T) {
	eff := newGatedEffects()
	eff.states["lo"] = process.StateStopped
	eff.states["other"] = process.StateReady
	s := newFIFO(&stubPlanner{evict: map[string][]string{"lo": {"other"}}}, eff)

	// "other" is serving, so a swap to lo that would evict it queues.
	busy := reqCh("other")
	s.OnRequest(busy)
	r := reqCh("lo")
	s.OnRequest(r)
	assertAdmitted(t, r)
	if len(s.queued) != 1 {
		t.Fatalf("queued=%d want 1", len(s.queued))
	}

	eff.blocked["lo"] = errors.New("tenant hi wants the GPU")
	eff.refuse["lo"] = true
	s.OnTenantsChanged()
	if got := eff.errored("lo"); got != 1 {
		t.Fatalf("lo error grants=%d want 1", got)
	}
	if len(s.queued) != 0 {
		t.Fatalf("queued=%d want 0", len(s.queued))
	}
}

func TestTenants_FIFORecordsDecisionsAndCountsHeld(t *testing.T) {
	eff := newGatedEffects()
	eff.states["lo"] = process.StateReady
	eff.states["refused"] = process.StateStopped
	eff.states["free"] = process.StateStopped
	eff.blocked["lo"] = errors.New("tenant hi wants the GPU")
	eff.blocked["refused"] = errors.New("tenant hi wants the GPU")
	eff.refuse["refused"] = true
	s := newFIFO(&stubPlanner{evict: map[string][]string{"free": {"other"}}}, eff)
	eff.states["other"] = process.StateReady
	s.OnRequest(reqCh("other"))

	for _, m := range []string{"lo", "lo", "refused", "free"} {
		s.OnRequest(reqCh(m))
	}
	// A wake while still blocked re-checks the queue without logging again.
	s.OnTenantsChanged()

	if got, want := strings.Join(eff.records, ","), "hold lo,hold lo,refuse refused"; got != want {
		t.Fatalf("records %q want %q", got, want)
	}
	held := s.HeldRequests()
	if len(held) != 1 || held["lo"] != 2 {
		t.Fatalf("held %v want map[lo:2] (free is queued behind a busy process, not held)", held)
	}
}
