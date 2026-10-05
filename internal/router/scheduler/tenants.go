package scheduler

import "slices"

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

// tenantBlock asks the gate about model; without a gate nothing is blocked.
// The gate is told what stays loaded alongside model: running processes and
// in-flight swap targets, less what model's own swap would evict.
func (s *FIFO) tenantBlock(model string) (error, bool) {
	if s.gate == nil {
		return nil, false
	}
	running := s.runningSet(model)
	evict := s.planner.EvictionFor(model, running)
	alongside := slices.DeleteFunc(running, func(id string) bool { return slices.Contains(evict, id) })
	return s.gate.TenantBlock(model, alongside)
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
		if blocked, _ := s.tenantBlock(req.Model); blocked != nil {
			held[req.Model]++
		}
	}
	return held
}
