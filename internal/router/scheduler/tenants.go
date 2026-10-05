package scheduler

import (
	"slices"

	"github.com/mostlygeek/llama-swap/internal/tenants"
)

// TenantGate is the tenant admission check (internal/tenants). Effects
// implementations that also implement it have every request checked; ones that
// do not admit everything.
type TenantGate interface {
	// TenantBlock reports why model may not load now, or nil when it may.
	// refuse says to fail the request with the reason instead of holding it
	// in the queue. alongside is the models that stay loaded if model loads.
	TenantBlock(model string, alongside []string) (reason error, refuse bool)
	// TenantRecord logs that a request for model was held (refuse false)
	// or refused because of reason.
	TenantRecord(model string, reason error, refuse bool)
}

// tenantBlock asks the gate about req's model; without a gate nothing is
// blocked. The gate is told what stays loaded alongside the model: running
// processes and in-flight swap targets, less what its own swap would evict. A
// preload is held where the gate says refuse.
func (s *FIFO) tenantBlock(req HandlerReq) (error, bool) {
	if s.gate == nil {
		return nil, false
	}
	running := s.runningSet(req.Model)
	evict := s.planner.EvictionFor(req.Model, running)
	alongside := slices.DeleteFunc(running, func(id string) bool { return slices.Contains(evict, id) })
	reason, refuse := s.gate.TenantBlock(req.Model, alongside)
	return tenants.HoldPreload(req.Ctx, reason, refuse)
}

// OnTenantsChanged re-runs the queue after a tenant's condition changed or a
// lower tenant finished stopping, so held requests can proceed.
func (s *FIFO) OnTenantsChanged() {
	s.drainQueue()
}

// tenantRecord reports a hold or refuse to the gate.
func (s *FIFO) tenantRecord(model string, reason error, refuse bool) {
	if s.gate != nil {
		s.gate.TenantRecord(model, reason, refuse)
	}
}

// HeldRequests counts queued requests the tenant gate blocks, by model. It
// reads the queue, so only the run-loop goroutine may call it.
func (s *FIFO) HeldRequests() map[string]int {
	held := make(map[string]int)
	for _, req := range s.queued {
		if blocked, _ := s.tenantBlock(req); blocked != nil {
			held[req.Model]++
		}
	}
	return held
}
