package scheduler

import (
	"errors"
	"testing"

	"github.com/mostlygeek/llama-swap/internal/process"
)

// gatedEffects is fakeEffects plus a programmable TenantGate.
type gatedEffects struct {
	*fakeEffects
	blocked map[string]error
	refuse  map[string]bool
}

func (g *gatedEffects) TenantBlock(model string) (error, bool) {
	return g.blocked[model], g.refuse[model]
}

func newGatedEffects() *gatedEffects {
	return &gatedEffects{fakeEffects: newFakeEffects(), blocked: map[string]error{}, refuse: map[string]bool{}}
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
