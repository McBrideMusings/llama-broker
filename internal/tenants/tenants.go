// Package tenants ranks GPU workloads. A tenant owns llama-swap models and has
// a priority; while its condition is true it wants the GPU, and every model of
// a lower-priority tenant is held or refused at admission, drained and then
// stopped. The scheduler asks Block before admitting a request, processes call
// the hook from StopHook before they stop, and the Manager's poll loop stops
// lower tenants through Router.
//
// Vocabulary (Tenant, Condition, Drain) is defined in docs/CONTEXT.md.
package tenants

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/mostlygeek/llama-swap/internal/config"
	"github.com/mostlygeek/llama-swap/internal/logmon"
	"github.com/mostlygeek/llama-swap/internal/process"
	"github.com/mostlygeek/llama-swap/internal/swaputil"
)

// Router is what the Manager needs from the local router.
type Router interface {
	// RunningModels returns every process that is not stopped.
	RunningModels() map[string]process.ProcessState
	// StopModels stops the named processes, each with its unloadTimeout, and
	// blocks until they have stopped. Queued requests stay queued.
	StopModels(ids ...string)
	// Wake makes the scheduler re-evaluate its queue.
	Wake()
}

type tenant struct {
	name     string
	cfg      config.TenantConfig
	interval time.Duration

	// wants, stopping and the last probe readings are guarded by Manager.mu.
	// cond holds the last condition reading that did not error; a failed probe
	// goes to condErr and leaves cond and wants as they were.
	wants      bool
	stopping   bool
	cond       probeResult
	condAt     time.Time
	condErr    probeResult // latest failed condition probe
	condErrAt  time.Time
	condErrors int         // condition probes failed in a row; 0 after a reading
	busy       probeResult // last busy reading that did not error; failures go to busyErr
	busyAt     time.Time
	busyErr    probeResult // latest failed busy probe
	busyErrAt  time.Time
	busyErrors int // busy probes failed in a row; 0 after a reading

	drainMu  sync.Mutex // guards episodes, run and drains; see episode.go
	episodes []*episode // open stop episodes
	run      *drainRun  // the drain in progress, if any
	drains   int        // drains run since start
}

// Manager holds the tenants and their polled state. A nil *Manager has no
// tenants: it blocks nothing and drains nothing.
type Manager struct {
	log     *logmon.Monitor
	tenants []*tenant // highest priority first
	byModel map[string]*tenant

	mu     sync.Mutex
	router Router
	vram   vram // set by SetVRAM; see reserve.go
	// admitted maps a model to its tenant's condition reading when the gate
	// last admitted it. The load decision logs that reading: a process starts
	// only in a swap, which the scheduler starts on the same run-loop turn as
	// a gate pass, but the start event reaches recordLoads later. Passes that
	// start nothing (fast path, held counts) only leave a reading that the
	// next swap's pass replaces. Guarded by mu.
	admitted map[string]string
}

// New builds a Manager from validated tenant configs. It returns nil when there
// are none.
func New(cfgs map[string]config.TenantConfig, log *logmon.Monitor) *Manager {
	if len(cfgs) == 0 {
		return nil
	}
	m := &Manager{log: log, byModel: make(map[string]*tenant), admitted: make(map[string]string)}
	for name, cfg := range cfgs {
		t := &tenant{name: name, cfg: cfg, interval: time.Duration(cfg.Interval) * time.Second}
		m.tenants = append(m.tenants, t)
		for _, id := range cfg.Members {
			m.byModel[id] = t
		}
	}
	sort.Slice(m.tenants, func(i, j int) bool {
		a, b := m.tenants[i], m.tenants[j]
		if a.cfg.Priority != b.cfg.Priority {
			return a.cfg.Priority > b.cfg.Priority
		}
		return a.name < b.name
	})
	return m
}

// Start begins polling every tenant condition until ctx is cancelled.
func (m *Manager) Start(ctx context.Context, r Router) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.router = r
	m.mu.Unlock()
	m.recordLoads(ctx)
	for _, t := range m.tenants {
		if t.cfg.Condition != nil {
			go m.poll(ctx, t)
		}
	}
}

// Block reports why model may not load now, or nil when it may. refuse says
// the caller should fail the request with the reason rather than wait.
//
// A model is blocked when a higher-priority tenant wants the GPU or has a
// condition whose first probe has not finished, and also when
// its own tenant wants the GPU but a lower tenant still has a process running:
// the higher tenant loads only after the lower one has drained and stopped.
//
// A pass also records the tenant's condition reading for model; the load
// decision for model's next start logs it.
// With a VRAM reserve it is also blocked while the needs of the tenants running
// alongside it, its own and the reserve exceed the card total. alongside is the
// models that stay loaded if model loads: running ones and in-flight swap
// targets, less those the swap evicts.
//
// ctx is the request's context. A preload (WithPreload) is never refused: a
// refusal becomes a hold, since a preload has no client to retry after a 503.
func (m *Manager) Block(ctx context.Context, model string, alongside []string) (reason error, refuse bool) {
	reason, refuse = m.block(model, alongside)
	return holdPreload(ctx, reason, refuse)
}

func (m *Manager) block(model string, alongside []string) (reason error, refuse bool) {
	if m == nil {
		return nil, false
	}
	t := m.byModel[model]
	if t == nil {
		return nil, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if h := m.blockerLocked(t); h != nil {
		err := &BlockedError{Model: model, Tenant: t.name, Priority: t.cfg.Priority,
			By: h.name, ByPriority: h.cfg.Priority, RetryAfter: h.cfg.Interval}
		return err, t.cfg.OnBlocked == config.TenantOnBlockedRefuse
	}
	if h := m.pendingLocked(t); h != nil {
		err := &PendingError{Model: model, Tenant: t.name, Priority: t.cfg.Priority,
			By: h.name, ByPriority: h.cfg.Priority, RetryAfter: h.cfg.Interval}
		return err, t.cfg.OnBlocked == config.TenantOnBlockedRefuse
	}
	if t.wants && m.router != nil {
		running := m.router.RunningModels()
		for _, l := range m.tenants {
			if l.cfg.Priority >= t.cfg.Priority {
				continue
			}
			if ids := runningMembers(l, running); len(ids) > 0 {
				return fmt.Errorf("tenant %s waits for lower tenant %s to stop %v", t.name, l.name, ids), false
			}
		}
	}
	if err := m.reserveBlockLocked(t, model, alongside); err != nil {
		return err, t.cfg.OnBlocked == config.TenantOnBlockedRefuse
	}
	m.admitted[model] = t.conditionLocked()
	return nil, false
}

// blockerLocked returns the highest-priority tenant above t that wants the GPU.
func (m *Manager) blockerLocked(t *tenant) *tenant {
	for _, h := range m.tenants {
		if h.cfg.Priority <= t.cfg.Priority {
			return nil
		}
		if h.wants {
			return h
		}
	}
	return nil
}

func (m *Manager) poll(ctx context.Context, t *tenant) {
	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()
	for {
		m.checkCondition(ctx, t)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// checkCondition reads t's condition once, then wakes the scheduler when the
// reading changed and stops any tenant that is now outranked. A probe that
// errors or times out changes nothing: the tenant keeps its last reading.
func (m *Manager) checkCondition(ctx context.Context, t *tenant) {
	pctx, cancel := context.WithTimeout(ctx, t.interval)
	res := runProbe(pctx, t.cfg.Condition)
	cancel()
	if ctx.Err() != nil {
		return
	}
	if res.err != nil {
		m.conditionFailed(t, res)
	} else {
		m.conditionRead(t, res)
	}
	m.mu.Lock()
	router := m.router
	m.mu.Unlock()
	// Wake on every poll, not only on a change: a held request may also be
	// waiting for a lower tenant's process that stopped some other way.
	router.Wake()
	m.preempt()
}

// preempt stops the running models of every tenant outranked by one that
// wants the GPU. Each stop drains its tenant first, through the stop hook.
func (m *Manager) preempt() {
	m.mu.Lock()
	router := m.router
	m.mu.Unlock()
	if router == nil {
		return
	}
	running := router.RunningModels()

	m.mu.Lock()
	defer m.mu.Unlock()
	for _, l := range m.tenants {
		h := m.blockerLocked(l)
		if h == nil || l.stopping {
			continue
		}
		ids := runningMembers(l, running)
		if len(ids) == 0 {
			continue
		}
		l.stopping = true
		reason := fmt.Sprintf("stopping %v to make room for tenant %s (priority %d)", ids, h.name, h.cfg.Priority)
		probe := h.conditionLocked()
		go func(l *tenant, ids []string) {
			m.record(Decision{Tenant: l.name, Action: ActionStop, Probe: probe, Reason: reason})
			router.StopModels(ids...)
			m.log.Infof("tenants: %s stopped %v", l.name, ids)
			m.mu.Lock()
			l.stopping = false
			m.mu.Unlock()
			router.Wake()
		}(l, ids)
	}
}

func runningMembers(t *tenant, running map[string]process.ProcessState) []string {
	var ids []string
	for _, id := range t.cfg.Members {
		if _, ok := running[id]; ok {
			ids = append(ids, id)
		}
	}
	return ids
}

// StopHook returns the function a process runs before it stops, or nil when
// model's tenant has neither a busy probe nor a drain action.
func (m *Manager) StopHook(model string) func(unloadTimeout time.Duration) {
	if m == nil {
		return nil
	}
	t := m.byModel[model]
	if t == nil || (t.cfg.Busy == nil && t.cfg.Drain == nil) {
		return nil
	}
	return func(unloadTimeout time.Duration) { m.drain(t, model, unloadTimeout) }
}

// drain waits until t's busy probe reads false, then runs its drain action,
// both before one deadline unloadTimeout from the start. A busy probe that
// errors or times out counts as busy, so an unreachable job endpoint never cuts
// off a running job before unloadTimeout. Each probe gets at most one interval.
// Stops of the same tenant in one stop episode share one drain, whose
// unloadTimeout is the largest among them.
func (m *Manager) drain(t *tenant, model string, unloadTimeout time.Duration) {
	run, unloadTimeout, runs := m.joinDrain(t, model, unloadTimeout)
	if !runs {
		return
	}
	defer t.endDrain(run)
	deadline := time.Now().Add(unloadTimeout)

	m.mu.Lock()
	why := "stop requested (unload, swap or shutdown)"
	if t.stopping {
		why = "preempted by a higher-priority tenant"
	}
	m.mu.Unlock()
	step := func(probe, reason string) {
		m.record(Decision{Tenant: t.name, Model: model, Action: ActionDrain, Probe: probe, Reason: reason})
	}
	warn := func(probe, reason string) {
		m.recordAt(m.log.Warnf, Decision{Tenant: t.name, Model: model, Action: ActionDrain, Probe: probe, Reason: reason})
	}
	step("", fmt.Sprintf("draining before stopping %s: %s (unloadTimeout %s)", model, why, unloadTimeout))
	if t.cfg.Busy != nil {
		probe := "busy not read"
		stopping := fmt.Sprintf("busy probe did not answer within %s, stopping anyway", unloadTimeout)
		for {
			budget := min(t.interval, time.Until(deadline))
			if budget <= 0 {
				warn(probe, stopping)
				break
			}
			ctx, cancel := context.WithTimeout(context.Background(), budget)
			res := runBusyProbe(ctx, t.cfg.Busy)
			// A probe given less than an interval and cut off ran into the
			// drain's deadline, not a fault of the endpoint.
			cutOff := budget < t.interval && ctx.Err() != nil
			cancel()
			if cutOff {
				warn(probe, stopping)
				break
			}
			failed := res.err != nil
			var waiting string
			if failed {
				n := m.busyFailed(t, res)
				probe = "busy probe failed: " + res.failure()
				stopping = fmt.Sprintf("busy probe still failing after %s (%d in a row), stopping anyway", unloadTimeout, n)
				waiting = fmt.Sprintf("treating as busy, waiting (%d in a row)", n)
			} else {
				m.busyRead(t, res)
				probe = "busy " + res.String()
				if !res.ok {
					step(probe, "idle")
					break
				}
				stopping = fmt.Sprintf("still busy after %s, stopping anyway", unloadTimeout)
				waiting = "busy, waiting"
			}
			if time.Until(deadline) <= 0 {
				warn(probe, stopping)
				break
			}
			if failed {
				warn(probe, waiting)
			} else {
				step(probe, waiting)
			}
			time.Sleep(min(t.interval, time.Until(deadline)))
		}
	}
	if t.cfg.Drain != nil && time.Until(deadline) <= 0 {
		warn("", fmt.Sprintf("drain action skipped: no time left of unloadTimeout %s", unloadTimeout))
	} else if t.cfg.Drain != nil {
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		out, err := runAction(ctx, t.cfg.Drain)
		cancel()
		if err != nil {
			warn("", fmt.Sprintf("drain action failed: %v %s", err, out))
		} else {
			step("", "drain action done: "+out)
		}
	}
	m.log.Infof("tenants: %s drained, stopping %s", t.name, model)
}

// BlockedError is the reason a request is held or refused. As an HTTP response
// it is a 503 naming both tenants.
type BlockedError struct {
	Model      string
	Tenant     string
	Priority   int
	By         string
	ByPriority int
	RetryAfter int // seconds
}

func (e *BlockedError) Error() string {
	return fmt.Sprintf("model %s (tenant %s, priority %d) is blocked: tenant %s (priority %d) wants the GPU",
		e.Model, e.Tenant, e.Priority, e.By, e.ByPriority)
}

func (e *BlockedError) StatusCode() int { return http.StatusServiceUnavailable }

func (e *BlockedError) Header() http.Header {
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	h.Set("Retry-After", strconv.Itoa(max(e.RetryAfter, 1)))
	return h
}

func (e *BlockedError) Body() []byte {
	return swaputil.NewErrorEnvelope(e.StatusCode(), e.Error(), "tenant_blocked").JSON()
}

var _ swaputil.HTTPError = (*BlockedError)(nil)
