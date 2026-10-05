package tenants

import (
	"context"
	"errors"
	"time"

	"github.com/mostlygeek/llama-swap/internal/config"
)

// runBusyProbe evaluates a busy probe. A url probe answered with a non-2xx
// status other than the configured one (a 502 from a proxy in front of the
// tenant) failed rather than read idle: reading idle by mistake runs the drain
// action on a job that may still be running.
func runBusyProbe(ctx context.Context, p *config.TenantProbe) probeResult {
	res := runProbe(ctx, p)
	if res.err == nil && res.status != 0 && res.status != p.Status && (res.status < 200 || res.status > 299) {
		res.err = errors.New("status is not 2xx")
	}
	return res
}

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
