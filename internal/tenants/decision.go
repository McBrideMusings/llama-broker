package tenants

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/mostlygeek/llama-swap/internal/event"
	"github.com/mostlygeek/llama-swap/internal/process"
	"github.com/mostlygeek/llama-swap/internal/swaputil"
)

// Action is what a tenant decision did.
type Action string

const (
	ActionHold   Action = "hold"   // a request waits in the queue
	ActionRefuse Action = "refuse" // a request fails with a 503
	ActionDrain  Action = "drain"  // a step of draining a tenant before its stop
	ActionStop   Action = "stop"   // a tenant's running models are stopped
	ActionLoad   Action = "load"   // a tenant's model starts loading
)

// TenantDecisionEventID is outside the range upstream's swaputil event IDs use.
const TenantDecisionEventID = 0x40

// Decision is one brokering decision: the log writes it as one line and the
// event stream carries it as a TenantDecisionEvent.
type Decision struct {
	Time   time.Time `json:"time"`
	Tenant string    `json:"tenant"`
	Model  string    `json:"model,omitempty"`
	Action Action    `json:"action"`
	// Probe is the probe reading the decision rests on, with its raw value,
	// e.g. "condition of hi: true (HTTP 200, on=true)".
	Probe  string `json:"probe,omitempty"`
	Reason string `json:"reason"`
}

// TenantDecisionEvent carries a Decision on the process-wide event bus.
type TenantDecisionEvent struct {
	Decision
}

func (e TenantDecisionEvent) Type() uint32 { return TenantDecisionEventID }

// record logs d as one info line and emits it as an event.
func (m *Manager) record(d Decision) { m.recordAt(m.log.Infof, d) }

// recordAt is record with the log level's printf, for decisions that log at
// warn.
func (m *Manager) recordAt(logf func(string, ...any), d Decision) {
	d.Time = time.Now().UTC()
	logf("tenants: decision time=%s tenant=%s model=%s action=%s probe=%q reason=%q",
		d.Time.Format(time.RFC3339Nano), d.Tenant, d.Model, d.Action, d.Probe, d.Reason)
	event.Emit(TenantDecisionEvent{d})
}

// Record logs the scheduler's hold or refuse of a request for model, blocked
// for reason.
func (m *Manager) Record(model string, reason error, refuse bool) {
	if m == nil || reason == nil {
		return
	}
	t := m.byModel[model]
	if t == nil {
		return
	}
	action := ActionHold
	if refuse {
		action = ActionRefuse
	}
	var reserve *ReserveError
	if errors.As(reason, &reserve) {
		m.record(Decision{Tenant: t.name, Model: model, Action: action, Probe: reserve.Probe(), Reason: reason.Error()})
		return
	}
	// The probe behind a block is the condition of the tenant that wants the
	// GPU or has not been probed yet: the blocker's, or t's own while it waits
	// for a lower tenant.
	by := t
	var blocked *BlockedError
	var pending *PendingError
	switch {
	case errors.As(reason, &blocked):
		by = m.byName(blocked.By)
	case errors.As(reason, &pending):
		by = m.byName(pending.By)
	}
	m.mu.Lock()
	probe := by.conditionLocked()
	m.mu.Unlock()
	m.record(Decision{Tenant: t.name, Model: model, Action: action, Probe: probe, Reason: reason.Error()})
}

func (m *Manager) byName(name string) *tenant {
	for _, t := range m.tenants {
		if t.name == name {
			return t
		}
	}
	return nil
}

// conditionLocked describes t's last condition reading for a decision, and the
// failed probes since it, if any.
func (t *tenant) conditionLocked() string {
	var s string
	switch {
	case t == nil:
		return ""
	case t.cfg.Condition == nil:
		return fmt.Sprintf("condition of %s: none configured", t.name)
	case t.condAt.IsZero():
		s = fmt.Sprintf("condition of %s: not read yet", t.name)
	default:
		s = fmt.Sprintf("condition of %s: %s", t.name, t.cond)
	}
	if t.condErrors > 0 {
		s += fmt.Sprintf("; failed probes since: %d, latest: %s", t.condErrors, t.condErr.failure())
	}
	return s
}

// recordLoads logs a load decision whenever a tenant's model starts, until ctx
// is cancelled. The decision carries the reading from the gate pass that
// admitted the load, not one taken when the start event arrives.
func (m *Manager) recordLoads(ctx context.Context) {
	cancel := event.On(func(e swaputil.ProcessStateChangeEvent) {
		if e.NewState == string(process.StateStopped) {
			// A stop frees VRAM: requests held by the reserve get a re-check.
			m.mu.Lock()
			router, enforced := m.router, m.vram.enforced()
			m.mu.Unlock()
			if enforced && router != nil {
				router.Wake()
			}
			return
		}
		if e.NewState != string(process.StateStarting) {
			return
		}
		t := m.byModel[e.ProcessName]
		if t == nil {
			return
		}
		m.mu.Lock()
		probe, ok := m.admitted[e.ProcessName]
		delete(m.admitted, e.ProcessName)
		m.mu.Unlock()
		d := Decision{Tenant: t.name, Model: e.ProcessName, Action: ActionLoad, Probe: probe}
		if !ok {
			d.Reason = "started without passing the tenant gate"
			m.recordAt(m.log.Warnf, d)
			return
		}
		d.Reason = "no higher-priority tenant wants the GPU"
		m.record(d)
	})
	go func() {
		<-ctx.Done()
		cancel()
	}()
}

// Status is the GET /api/tenants body.
type Status struct {
	VRAM    VRAMStatus     `json:"vram"`
	Tenants []TenantStatus `json:"tenants"`
}

// TenantStatus is one tenant's current state, highest priority first.
type TenantStatus struct {
	Name      string   `json:"name"`
	Priority  int      `json:"priority"`
	VRAMMiB   int      `json:"vramMiB"` // declared need; 0 when not declared
	OnBlocked string   `json:"onBlocked"`
	Models    []string `json:"models"`
	// WantsGPU is the last condition reading; always false without a condition.
	WantsGPU  bool         `json:"wantsGPU"`
	Condition *ProbeStatus `json:"condition"` // nil when not configured
	// Busy is the last busy probe reading. The busy probe runs only while the
	// tenant drains, so this is the reading from the latest drain.
	Busy     *ProbeStatus  `json:"busy"` // nil when not configured
	Draining bool          `json:"draining"`
	Drains   int           `json:"drains"` // drains run since start; one per stop episode
	Stopping bool          `json:"stopping"`
	Loaded   []LoadedModel `json:"loaded"`
	// Held counts requests for the tenant's models waiting at the tenant gate.
	Held int `json:"held"`
}

// ProbeStatus is a probe's last reading. Result and ProbedAt are null before
// the first reading.
type ProbeStatus struct {
	Result   *bool      `json:"result"`
	Raw      string     `json:"raw,omitempty"`
	ProbedAt *time.Time `json:"probedAt"`
	// A failed probe leaves Result as it was. Errors counts
	// failures since the last reading; LastError and LastErrorAt describe the
	// latest failure and stay after a reading succeeds.
	Errors      int        `json:"errors,omitempty"`
	LastError   string     `json:"lastError,omitempty"`
	LastErrorAt *time.Time `json:"lastErrorAt,omitempty"`
}

type LoadedModel struct {
	Model string `json:"model"`
	State string `json:"state"`
}

// Status reports every tenant. running is the router's processes that are not
// stopped; held counts held requests by model.
func (m *Manager) Status(running map[string]process.ProcessState, held map[string]int) Status {
	st := Status{VRAM: VRAMStatus{Running: []TenantNeed{}}, Tenants: []TenantStatus{}}
	if m == nil {
		return st
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	st.VRAM = m.vramStatusLocked(slices.Collect(maps.Keys(running)))
	for _, t := range m.tenants {
		ts := TenantStatus{
			Name:      t.name,
			Priority:  t.cfg.Priority,
			VRAMMiB:   t.cfg.VRAM,
			OnBlocked: t.cfg.OnBlocked,
			Models:    append([]string{}, t.cfg.Members...),
			WantsGPU:  t.wants,
			Stopping:  t.stopping,
			Loaded:    []LoadedModel{},
		}
		if t.cfg.Condition != nil {
			ts.Condition = probeStatus(t.cond, t.condAt, t.condErr, t.condErrAt, t.condErrors)
		}
		if t.cfg.Busy != nil {
			ts.Busy = probeStatus(t.busy, t.busyAt, t.busyErr, t.busyErrAt, t.busyErrors)
		}
		t.drainMu.Lock()
		ts.Draining = t.run != nil
		ts.Drains = t.drains
		t.drainMu.Unlock()
		for _, id := range t.cfg.Members {
			if state, ok := running[id]; ok {
				ts.Loaded = append(ts.Loaded, LoadedModel{Model: id, State: string(state)})
			}
			ts.Held += held[id]
		}
		st.Tenants = append(st.Tenants, ts)
	}
	return st
}

// probeStatus reports the last reading r taken at at, and the latest failure
// failed at failedAt with the count of failures since that reading.
func probeStatus(r probeResult, at time.Time, failed probeResult, failedAt time.Time, nFailed int) *ProbeStatus {
	ps := &ProbeStatus{}
	if !at.IsZero() {
		ok := r.ok
		ps.Result = &ok
		ps.Raw = r.raw
		ps.ProbedAt = &at
	}
	if !failedAt.IsZero() {
		ps.Errors = nFailed
		ps.LastError = failed.failure()
		ps.LastErrorAt = &failedAt
	}
	return ps
}
