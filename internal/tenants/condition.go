package tenants

import (
	"fmt"
	"time"
)

// conditionRead stores a condition reading that did not error, and logs a
// change of the tenant's GPU claim and the end of a run of failed probes.
func (m *Manager) conditionRead(t *tenant, res probeResult) {
	m.mu.Lock()
	changed := t.wants != res.ok
	t.wants = res.ok
	t.cond, t.condAt = res, time.Now().UTC()
	failed := t.condErrors
	t.condErrors = 0
	m.mu.Unlock()

	if failed > 0 {
		m.log.Infof("tenants: %s (priority %d) condition probe recovered after %d failed polls", t.name, t.cfg.Priority, failed)
	}
	if changed {
		m.log.Infof("tenants: %s (priority %d) condition %s; wants GPU: %v", t.name, t.cfg.Priority, res, res.ok)
	} else {
		m.log.Debugf("tenants: %s condition %s", t.name, res)
	}
}

// conditionFailed records a condition probe that errored or timed out. The
// tenant keeps its last reading, so an unreachable or slow condition endpoint
// neither lifts a block nor claims the GPU. The first failure in a run logs at
// warn; the rest at debug.
func (m *Manager) conditionFailed(t *tenant, res probeResult) {
	m.mu.Lock()
	t.condErr, t.condErrAt = res, time.Now().UTC()
	t.condErrors++
	n := t.condErrors
	keeping := t.keptReadingLocked()
	m.mu.Unlock()

	logf := m.log.Debugf
	if n == 1 {
		logf = m.log.Warnf
	}
	logf("tenants: %s (priority %d) condition probe failed (%d in a row): %s; %s",
		t.name, t.cfg.Priority, n, res.failure(), keeping)
}

// keptReadingLocked says which reading a failed condition probe leaves in
// force.
func (t *tenant) keptReadingLocked() string {
	if t.condAt.IsZero() {
		return fmt.Sprintf("no reading yet, wants GPU stays %v", t.wants)
	}
	return fmt.Sprintf("keeping wants GPU: %v from the reading at %s", t.wants, t.condAt.Format(time.RFC3339))
}

// failure describes a probe that errored, with whatever it observed first.
func (r probeResult) failure() string {
	if r.raw == "" {
		return r.err.Error()
	}
	return fmt.Sprintf("%v (%s)", r.err, r.raw)
}
