package tenants

import "time"

// busyRead stores a busy reading that did not error.
func (m *Manager) busyRead(t *tenant, res probeResult) {
	m.mu.Lock()
	t.busy, t.busyAt = res, time.Now().UTC()
	t.busyErrors = 0
	m.mu.Unlock()
}

// busyFailed records a busy probe that errored or timed out and returns how
// many have failed in a row. The last reading stays, so a failure never shows
// as idle.
func (m *Manager) busyFailed(t *tenant, res probeResult) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	t.busyErr, t.busyErrAt = res, time.Now().UTC()
	t.busyErrors++
	return t.busyErrors
}
