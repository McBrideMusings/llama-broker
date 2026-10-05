package router

import (
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/mostlygeek/llama-swap/internal/process"
	"github.com/mostlygeek/llama-swap/internal/tenants"
)

// start wires the tenant stop hooks into the processes, starts tenant polling
// and then the run loop. Concrete routers call it once their processes map is
// complete.
func (b *baseRouter) start() {
	for id, p := range b.processes {
		hookable, ok := p.(interface {
			SetStopHook(func(time.Duration))
		})
		if !ok {
			continue
		}
		if hook := b.tenants.StopHook(id); hook != nil {
			hookable.SetStopHook(hook)
		}
	}
	b.tenants.Start(b.shutdownCtx, b)
	go b.run()
}

// TenantBlock implements scheduler.TenantGate.
func (b *baseRouter) TenantBlock(model string, alongside []string) (error, bool) {
	return b.tenants.Block(model, alongside)
}

// TenantRecord implements scheduler.TenantGate.
func (b *baseRouter) TenantRecord(model string, reason error, refuse bool) {
	b.tenants.Record(model, reason, refuse)
}

// publishHeld stores the scheduler's held-request counts for TenantStatus. It
// runs on the run-loop goroutine, the only one that may read the queue.
func (b *baseRouter) publishHeld() {
	if b.tenants == nil {
		return
	}
	h, ok := b.schedule.(interface{ HeldRequests() map[string]int })
	if !ok {
		return
	}
	held := h.HeldRequests()
	b.held.Store(&held)
}

// VRAMSetter is how the server hands a local router the card total and the
// VRAM reserve. Every local router implements it.
type VRAMSetter interface {
	SetVRAM(totalMiB int64, reserveMiB int)
}

var (
	_ VRAMSetter = (*Matrix)(nil)
	_ VRAMSetter = (*Group)(nil)
)

// SetVRAM sets the card total and the reserve the tenant gate holds loads
// against, both in MiB.
func (b *baseRouter) SetVRAM(totalMiB int64, reserveMiB int) {
	b.tenants.SetVRAM(totalMiB, reserveMiB)
}

// TenantStatus reports every tenant's state for GET /api/tenants.
func (b *baseRouter) TenantStatus() tenants.Status {
	var held map[string]int
	if p := b.held.Load(); p != nil {
		held = *p
	}
	return b.tenants.Status(b.RunningModels(), held)
}

// Wake implements tenants.Router. The send never blocks: one pending wake
// already covers this one.
func (b *baseRouter) Wake() {
	select {
	case b.wakeCh <- struct{}{}:
	default:
	}
}

// beginStops opens a tenant stop episode for a batch of processes about to
// stop together, so each tenant among them drains once. Processes already
// stopped are left out: they run no drain and must not widen its deadline.
// Call the returned func after the batch has stopped.
func (b *baseRouter) beginStops(ids []string) (end func()) {
	timeouts := make(map[string]time.Duration, len(ids))
	for _, id := range ids {
		p, ok := b.processes[id]
		if !ok {
			continue
		}
		if st := p.State(); st == process.StateStopped || st == process.StateShutdown {
			continue
		}
		timeouts[id] = b.unloadTimeout(id)
	}
	return b.tenants.BeginStops(timeouts)
}

// beginStopsAll is beginStops for every process, as shutdown stops them.
func (b *baseRouter) beginStopsAll() (end func()) {
	return b.beginStops(slices.Collect(maps.Keys(b.processes)))
}

// StopModels implements tenants.Router. It stops the processes directly rather
// than through Unload, which would fail the requests the tenant gate is
// holding for these models; those wait in the queue until the gate opens.
func (b *baseRouter) StopModels(ids ...string) {
	defer b.beginStops(ids)()
	var wg sync.WaitGroup
	for _, id := range ids {
		p, ok := b.processes[id]
		if !ok {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.Stop(b.unloadTimeout(id)); err != nil {
				b.logger.Warnf("%s: stopping %s failed: %v", b.name, id, err)
			}
		}()
	}
	wg.Wait()
}
