package tenants

import (
	"fmt"
	"slices"
	"time"
)

// episode is one batch of stops of a tenant's models, opened by BeginStops.
// Once a drain started by one of its stops ends, every later stop of a model
// in the episode skips the drain.
type episode struct {
	timeouts  map[string]time.Duration // the episode's models and their unloadTimeouts
	limit     time.Duration            // cap on the drain's timeout; 0 for none
	drainedBy string                   // model whose stop drained the tenant; "" until then
}

// drainRun is a drain in progress. A stop that arrives while it runs waits for
// it and does not drain again.
type drainRun struct {
	first  string        // model whose stop runs the drain
	covers []*episode    // episodes of first that this drain counts for
	done   chan struct{} // closed when the drain ends
}

// BeginStops opens a stop episode for every tenant with a model in timeouts,
// which maps each model about to stop to its unloadTimeout. The caller stops
// the models and then calls end. Stops of one tenant inside the episode share
// a single drain, bounded by the largest of their unloadTimeouts.
func (m *Manager) BeginStops(timeouts map[string]time.Duration) (end func()) {
	return m.BeginStopsWithin(timeouts, 0)
}

// BeginStopsWithin is BeginStops with every drain in the episode, and every
// wait of one of its stops on a drain already running, cut off at limit.
// Shutdown passes its own timeout here.
func (m *Manager) BeginStopsWithin(timeouts map[string]time.Duration, limit time.Duration) (end func()) {
	if m == nil {
		return func() {}
	}
	opened := map[*tenant]*episode{}
	for id, timeout := range timeouts {
		t := m.byModel[id]
		if t == nil || (t.cfg.Busy == nil && t.cfg.Drain == nil) {
			continue
		}
		if opened[t] == nil {
			opened[t] = &episode{timeouts: map[string]time.Duration{}, limit: limit}
		}
		opened[t].timeouts[id] = timeout
	}
	for t, ep := range opened {
		t.drainMu.Lock()
		t.episodes = append(t.episodes, ep)
		t.drainMu.Unlock()
	}
	return func() {
		for t, ep := range opened {
			t.drainMu.Lock()
			t.episodes = slices.DeleteFunc(t.episodes, func(e *episode) bool { return e == ep })
			t.drainMu.Unlock()
		}
	}
}

// joinDrain runs inside model's stop hook. It reports whether this stop runs a
// drain, and the unloadTimeout that drain must finish within: the largest
// among model's open episodes and model's own, cut to the smallest limit among
// those episodes. A stop that does not run one returns once the drain covering
// it has ended, or once that timeout has passed, whichever is first.
func (m *Manager) joinDrain(t *tenant, model string, unloadTimeout time.Duration) (run *drainRun, timeout time.Duration, runs bool) {
	skip := func(first string) {
		m.record(Decision{Tenant: t.name, Model: model, Action: ActionDrain,
			Reason: fmt.Sprintf("not draining again: the stop of %s drained this tenant in the same stop batch", first)})
	}
	t.drainMu.Lock()
	var covers []*episode
	timeout = unloadTimeout
	var limit time.Duration
	for _, ep := range t.episodes {
		if _, ok := ep.timeouts[model]; !ok {
			continue
		}
		if ep.drainedBy != "" {
			t.drainMu.Unlock()
			skip(ep.drainedBy)
			return nil, 0, false
		}
		covers = append(covers, ep)
		for _, d := range ep.timeouts {
			timeout = max(timeout, d)
		}
		if ep.limit > 0 && (limit == 0 || ep.limit < limit) {
			limit = ep.limit
		}
	}
	if limit > 0 {
		timeout = min(timeout, limit)
	}
	if r := t.run; r != nil {
		t.drainMu.Unlock()
		wait := time.NewTimer(timeout)
		defer wait.Stop()
		select {
		case <-r.done:
			t.drainMu.Lock()
			for _, ep := range covers {
				if ep.drainedBy == "" {
					ep.drainedBy = r.first
				}
			}
			t.drainMu.Unlock()
			skip(r.first)
		case <-wait.C:
			m.recordAt(m.log.Warnf, Decision{Tenant: t.name, Model: model, Action: ActionDrain,
				Reason: fmt.Sprintf("the drain started by the stop of %s is still running after %s, stopping anyway", r.first, timeout)})
		}
		return nil, 0, false
	}
	run = &drainRun{first: model, covers: covers, done: make(chan struct{})}
	t.run = run
	t.drains++
	t.drainMu.Unlock()
	return run, timeout, true
}

// endDrain marks run's episodes drained and wakes every stop waiting on it.
func (t *tenant) endDrain(run *drainRun) {
	t.drainMu.Lock()
	for _, ep := range run.covers {
		ep.drainedBy = run.first
	}
	t.run = nil
	t.drainMu.Unlock()
	close(run.done)
}
