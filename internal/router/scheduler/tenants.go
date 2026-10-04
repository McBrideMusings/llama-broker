package scheduler

// TenantGate is the tenant admission check (internal/tenants). Effects
// implementations that also implement it have every request checked; ones that
// do not admit everything.
type TenantGate interface {
	// TenantBlock reports why model may not load now, or nil when it may.
	// refuse says to fail the request with the reason instead of holding it
	// in the queue.
	TenantBlock(model string) (reason error, refuse bool)
}

// tenantBlock asks the gate about model; without a gate nothing is blocked.
func (s *FIFO) tenantBlock(model string) (error, bool) {
	if s.gate == nil {
		return nil, false
	}
	return s.gate.TenantBlock(model)
}

// OnTenantsChanged re-runs the queue after a tenant's condition changed or a
// lower tenant finished stopping, so held requests can proceed.
func (s *FIFO) OnTenantsChanged() {
	s.drainQueue()
}
