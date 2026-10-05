package scheduler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/mostlygeek/llama-swap/internal/process"
	"github.com/mostlygeek/llama-swap/internal/tenants"
)

// gatedEffects is fakeEffects plus a programmable TenantGate.
type gatedEffects struct {
	*fakeEffects
	blocked   map[string]error
	refuse    map[string]bool
	records   []string            // "hold lo", "refuse lo"
	alongside map[string][]string // last alongside set per model
}

func (g *gatedEffects) TenantBlock(model string, alongside []string) (error, bool) {
	g.alongside[model] = alongside
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
		alongside: map[string][]string{}}
}

func TestTenants_FIFOGateSeesWhatStaysLoaded(t *testing.T) {
	eff := newGatedEffects()
	eff.states["a"] = process.StateReady
	eff.states["b"] = process.StateReady
	eff.states["c"] = process.StateStopped
	s := newFIFO(&stubPlanner{evict: map[string][]string{"c": {"a"}}}, eff)

	s.OnRequest(reqCh("c"))
	if got := eff.alongside["c"]; len(got) != 1 || got[0] != "b" {
		t.Fatalf("alongside c = %v, want [b]: a is evicted by c's swap", got)
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

func TestTenants_FIFOHoldsRefusedPreloadUntilTenantsChange(t *testing.T) {
	eff := newGatedEffects()
	eff.states["lo"] = process.StateReady
	reason := errors.New("tenant hi wants the GPU")
	eff.blocked["lo"] = reason
	eff.refuse["lo"] = true
	s := newFIFO(&stubPlanner{}, eff)

	r := reqCh("lo")
	r.Ctx = tenants.WithPreload(r.Ctx)
	s.OnRequest(r)
	assertAdmitted(t, r)
	if len(s.queued) != 1 {
		t.Fatalf("queued=%d want 1: a refused preload is held", len(s.queued))
	}

	// A wake while still refusing keeps the preload held, not failed.
	s.OnTenantsChanged()
	if eff.errored("lo") != 0 || len(s.queued) != 1 {
		t.Fatalf("after wake while blocked: errored=%d queued=%d, want 0 and 1", eff.errored("lo"), len(s.queued))
	}
	if got, want := strings.Join(eff.records, ","), "hold lo"; got != want {
		t.Fatalf("records %q want %q", got, want)
	}

	delete(eff.blocked, "lo")
	s.OnTenantsChanged()
	if got := eff.served("lo"); got != 1 {
		t.Fatalf("served lo %d times after the gate opened, want 1", got)
	}
}

func TestTenants_HeldPreloadDoesNotDelayLaterPreloads(t *testing.T) {
	for _, refuse := range []bool{false, true} {
		t.Run(fmt.Sprintf("refuse=%v", refuse), func(t *testing.T) {
			eff := newGatedEffects()
			eff.states["a"] = process.StateReady
			eff.states["lo"] = process.StateReady
			eff.states["hi"] = process.StateReady
			eff.blocked["lo"] = errors.New("tenant hi wants the GPU")
			eff.refuse["lo"] = refuse
			s := newFIFO(&stubPlanner{}, eff)

			// mu stands in for the run loop that serializes scheduler calls.
			var mu sync.Mutex
			stop, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Send [a, lo, hi] the way startPreload does: each send returns
			// once served, or waits forever while queued.
			for _, model := range []string{"a", "lo", "hi"} {
				tenants.Preload(stop, func(ctx context.Context) {
					mu.Lock()
					r := reqCh(model)
					r.Ctx = ctx
					s.OnRequest(r)
					served := eff.served(model) > 0
					mu.Unlock()
					if !served {
						<-stop.Done()
					}
				})
			}

			mu.Lock()
			defer mu.Unlock()
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
		})
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
