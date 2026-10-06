package tenants

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

// ActionIdleLoad is a tenant's idleLoad model being loaded because the GPU is
// idle.
const ActionIdleLoad Action = "idle-load"

const (
	// maxIdleBackoff caps the wait between idle loads that keep failing.
	maxIdleBackoff = 10 * time.Minute
	// idleMinUptime is how long an idle-loaded model must stay before its
	// load counts as a success. One gone sooner, a crash after its first
	// answer for example, doubles the wait like a failed load.
	idleMinUptime = time.Minute
)

// IdleLoader loads model through the request path and reports whether it was
// served. It blocks until the load finishes or ctx ends.
type IdleLoader func(ctx context.Context, model string) bool

type idleLoadKey struct{}

// idleMark is the value withIdleLoad stores; refused is set when the tenant
// gate turns the idle load away.
type idleMark struct {
	refused atomic.Bool
	reason  atomic.Pointer[error]
}

func withIdleLoad(ctx context.Context) (context.Context, *idleMark) {
	m := &idleMark{}
	return context.WithValue(ctx, idleLoadKey{}, m), m
}

// refuseIdleLoad reports whether ctx is an idle load the gate blocks. An idle
// load is never held, whatever its tenant's onBlocked says: it has no client
// waiting, and holding it would load the model later without checking that
// the GPU is still idle. The next idle check tries again.
func refuseIdleLoad(ctx context.Context, reason error) bool {
	if reason == nil || ctx == nil {
		return false
	}
	m, _ := ctx.Value(idleLoadKey{}).(*idleMark)
	if m == nil {
		return false
	}
	m.reason.Store(&reason)
	m.refused.Store(true)
	return true
}

// SetResident names the models the idle check ignores: members of persistent
// routing groups, which stay loaded beside whatever else runs.
func (m *Manager) SetResident(ids []string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.resident = make(map[string]bool, len(ids))
	for _, id := range ids {
		m.resident[id] = true
	}
}

// StartIdleLoad checks for an idle GPU every interval of the tenant that sets
// idleLoad, and loads that model through load each time the GPU is idle. A
// load that fails, or a model gone within idleMinUptime of loading, doubles the
// wait before the next try, up to maxIdleBackoff. It does nothing when no
// tenant sets idleLoad.
func (m *Manager) StartIdleLoad(ctx context.Context, load IdleLoader) {
	if m == nil {
		return
	}
	for _, t := range m.tenants {
		if t.cfg.IdleLoad != "" {
			go m.idleLoop(ctx, t, load)
			return
		}
	}
}

func (m *Manager) idleLoop(ctx context.Context, t *tenant, load IdleLoader) {
	model := t.cfg.IdleLoad
	wait, backoff := t.interval, t.interval
	var loadedAt time.Time
	othersRan := false // since loadedAt, another model ran or was requested
	decide := func(logf func(string, ...any), probe, reason string) {
		m.recordAt(logf, Decision{Tenant: t.name, Model: model, Action: ActionIdleLoad, Probe: probe, Reason: reason})
	}
	retryLater := func(probe, why string) {
		backoff = min(backoff*2, maxIdleBackoff)
		wait = backoff
		decide(m.log.Warnf, probe, fmt.Sprintf("%s, next try in %s", why, wait))
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		wait = t.interval
		probe, idle, others := m.idle(model)
		othersRan = othersRan || others
		if !idle {
			continue
		}
		if !loadedAt.IsZero() {
			up := time.Since(loadedAt)
			loadedAt = time.Time{}
			// Evicted to make room for other work is not a fault.
			if up < idleMinUptime && !othersRan {
				retryLater(probe, fmt.Sprintf("idle model %s was gone within %s of loading", model, up.Round(time.Second)))
				continue
			}
		}
		decide(m.log.Infof, probe, fmt.Sprintf("GPU idle, loading idleLoad model %s of tenant %s", model, t.name))
		lctx, mark := withIdleLoad(ctx)
		ok := load(lctx, model)
		switch {
		case ctx.Err() != nil:
			return
		case mark.refused.Load():
			reason := *mark.reason.Load()
			var blocked *BlockedError
			if errors.As(reason, &blocked) {
				decide(m.log.Infof, probe, "idle load turned away, GPU no longer idle: "+reason.Error())
			} else {
				decide(m.log.Infof, probe, "idle load turned away: "+reason.Error())
			}
		case ok:
			backoff = t.interval
			loadedAt = time.Now()
			othersRan = false
		default:
			retryLater(probe, fmt.Sprintf("idle load of %s failed", model))
		}
	}
}

// idle reports whether the GPU is idle: no process outside the resident set is
// running or starting, no request is queued, and no tenant wants the GPU, has
// an unread condition or is being stopped. probe describes the reading. others
// reports work other than self, the idle model: another non-resident process, a
// queued request or a tenant that wants the GPU.
func (m *Manager) idle(self string) (probe string, idle, others bool) {
	m.mu.Lock()
	router, resident := m.router, m.resident
	m.mu.Unlock()
	if router == nil {
		return "", false, false
	}
	running := router.RunningModels()
	for id := range resident {
		delete(running, id)
	}
	_, selfRuns := running[self]
	others = len(running) > 1 || (len(running) == 1 && !selfRuns) || router.Queued() > 0

	m.mu.Lock()
	defer m.mu.Unlock()
	busy := false
	for _, t := range m.tenants {
		others = others || t.wants
		busy = busy || t.wants || t.unprobedLocked() || t.stopping
	}
	if busy || len(running) > 0 || router.Queued() > 0 {
		return "", false, others
	}
	return fmt.Sprintf("no process running, no request queued, no tenant wants the GPU (%d tenants)", len(m.tenants)), true, others
}
