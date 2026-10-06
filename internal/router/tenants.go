package router

import (
	"context"
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
func (b *baseRouter) TenantBlock(ctx context.Context, model string, alongside []string) (error, bool) {
	return b.tenants.Block(ctx, model, alongside)
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
	if q, ok := b.schedule.(interface{ QueueLen() int }); ok {
		b.queued.Store(int64(q.QueueLen()))
	}
}

// Queued implements tenants.Router: the requests queued at the run loop's
// latest turn, held or not.
func (b *baseRouter) Queued() int { return int(b.queued.Load()) }

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

// IdleLoadStarter is how the server hands a local router the function that
// loads a tenant's idleLoad model. Every local router implements it.
type IdleLoadStarter interface {
	StartIdleLoad(load tenants.IdleLoader)
}

var (
	_ IdleLoadStarter = (*Matrix)(nil)
	_ IdleLoadStarter = (*Group)(nil)
)

// StartIdleLoad starts the tenants' idle watcher until the router shuts down.
func (b *baseRouter) StartIdleLoad(load tenants.IdleLoader) {
	b.tenants.StartIdleLoad(b.shutdownCtx, load)
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
	return b.tenants.BeginStops(b.stopTimeouts(ids))
}

// stopTimeouts maps each process in ids that is not already stopped to its
// unloadTimeout.
func (b *baseRouter) stopTimeouts(ids []string) map[string]time.Duration {
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
	return timeouts
}

// beginStopsAll is beginStops for every process, as shutdown stops them. Each
// tenant's drain, and each wait on a drain already running, ends within limit,
// so a long unloadTimeout cannot hold shutdown past its own timeout.
func (b *baseRouter) beginStopsAll(limit time.Duration) (end func()) {
	return b.tenants.BeginStopsWithin(b.stopTimeouts(slices.Collect(maps.Keys(b.processes))), limit)
}

// beginUnload is beginStops for an unload, and also drains the targets' tenants
// here, on the caller's goroutine. The stops then run on the run loop, where
// the stop hook finds the episode drained and returns at once, so a busy
// tenant never holds up requests for other models.
func (b *baseRouter) beginUnload(ids []string) (end func()) {
	timeouts := b.stopTimeouts(ids)
	end = b.tenants.BeginStops(timeouts)
	var wg sync.WaitGroup
	for id, timeout := range timeouts {
		if hook := b.tenants.StopHook(id); hook != nil {
			wg.Go(func() { hook(timeout) })
		}
	}
	wg.Wait()
	return end
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
