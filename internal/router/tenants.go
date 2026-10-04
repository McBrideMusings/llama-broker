package router

import (
	"sync"
	"time"
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
func (b *baseRouter) TenantBlock(model string) (error, bool) {
	return b.tenants.Block(model)
}

// Wake implements tenants.Router. The send never blocks: one pending wake
// already covers this one.
func (b *baseRouter) Wake() {
	select {
	case b.wakeCh <- struct{}{}:
	default:
	}
}

// StopModels implements tenants.Router. It stops the processes directly rather
// than through Unload, which would fail the requests the tenant gate is
// holding for these models; those wait in the queue until the gate opens.
func (b *baseRouter) StopModels(ids ...string) {
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
