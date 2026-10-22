package tenants

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/mostlygeek/llama-swap/internal/hw"
	"github.com/mostlygeek/llama-swap/internal/swaputil"
)

const (
	// unknownWarnMiB is how much GPU memory in use beyond what llama-broker
	// declared counts as an unknown user worth a warning.
	unknownWarnMiB = 256
	// measureMaxAge is how old a GPU memory reading may be when a load is
	// checked against it.
	measureMaxAge = 2 * time.Second
	// watchEvery is how often the manager re-reads GPU memory to warn about
	// unknown users and to wake held requests.
	watchEvery = 5 * time.Second
	// measureTimeout bounds one GPU memory reading.
	measureTimeout = 5 * time.Second
	// defaultRetryAfter is the Retry-After of a held model outside any tenant.
	defaultRetryAfter = 5
)

// MemoryReader returns the GPU memory in use now, in MiB.
type MemoryReader func(ctx context.Context) (int64, error)

// VRAMConfig is the VRAM settings the tenant gate checks loads against.
type VRAMConfig struct {
	ReserveMiB int
	// External is the always-on GPU users outside llama-broker, by name, in MiB.
	External map[string]int
	// Models is each model's vram, in MiB.
	Models map[string]int
	// Reader reads GPU memory in use; nil skips the measured check.
	Reader MemoryReader
}

// vram is the card total and the memory the gate keeps free or counts as
// taken. A tenant's need is its vram setting, counted while any of its models
// is running; a model outside such a tenant counts its own vram while running.
type vram struct {
	totalMiB   int64 // 0 when hardware detection found no accelerator memory
	reserveMiB int
	external   map[string]int
	models     map[string]int
	reader     MemoryReader
}

// externalMiB is the sum of the always-on external users.
func (v vram) externalMiB() int64 {
	var sum int64
	for _, mib := range v.external {
		sum += int64(mib)
	}
	return sum
}

// enforced reports whether loads are checked against the card total.
func (v vram) enforced() bool {
	return v.totalMiB > 0 && (v.reserveMiB > 0 || v.externalMiB() > 0)
}

// TotalVRAMMiB sums the memory of every accelerator that reports a capacity.
// It returns 0 for a nil snapshot or one with no capacity.
func TotalVRAMMiB(h *hw.HardwareSnapshot) int64 {
	if h == nil {
		return 0
	}
	var total uint64
	for _, a := range h.Accelerators {
		if a.Memory.CapacityBytes != nil {
			total += *a.Memory.CapacityBytes
		}
	}
	return int64(total / (1 << 20))
}

// SetVRAM sets the card total and the reserve, both in MiB; see ConfigureVRAM.
func (m *Manager) SetVRAM(totalMiB int64, reserveMiB int) {
	m.ConfigureVRAM(totalMiB, VRAMConfig{ReserveMiB: reserveMiB})
}

// ConfigureVRAM sets the card total and the VRAM settings, and wakes the
// scheduler so held requests are re-checked. A zero total leaves loads
// unchecked, which is logged as a warning.
func (m *Manager) ConfigureVRAM(totalMiB int64, c VRAMConfig) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.memMu.Lock()
	m.memAt = time.Time{}
	m.memMu.Unlock()
	m.vram = vram{totalMiB: totalMiB, reserveMiB: c.ReserveMiB, external: c.External, models: c.Models, reader: c.Reader}
	m.unknownLogged = 0
	v := m.vram
	router := m.router
	m.mu.Unlock()
	external := v.externalMiB()
	switch {
	case c.ReserveMiB == 0 && external == 0:
		m.log.Debugf("tenants: vram total=%d MiB, no vramReserve or vramExternal", totalMiB)
	case totalMiB == 0:
		m.log.Warnf("tenants: vramReserve=%d MiB vramExternal=%d MiB not enforced: hardware detection reported no accelerator memory", c.ReserveMiB, external)
	default:
		m.log.Infof("tenants: vram total=%d MiB reserve=%d MiB external=%d MiB; loads may use %d MiB",
			totalMiB, c.ReserveMiB, external, totalMiB-int64(c.ReserveMiB)-external)
		for _, t := range m.tenants {
			if over := int64(t.cfg.VRAM+c.ReserveMiB) + external - totalMiB; over > 0 {
				extra := ""
				if external > 0 {
					extra = fmt.Sprintf(" plus vramExternal %d MiB", external)
				}
				m.log.Warnf("tenants: %s can never load: vram %d MiB plus vramReserve %d MiB%s is %d MiB over the %d MiB total",
					t.name, t.cfg.VRAM, c.ReserveMiB, extra, over, totalMiB)
			}
		}
		for id, mib := range c.Models {
			if t := m.byModel[id]; t != nil && t.cfg.VRAM > 0 {
				continue
			}
			if over := int64(mib+c.ReserveMiB) + external - totalMiB; over > 0 {
				m.log.Warnf("tenants: model %s can never load: vram %d MiB plus vramReserve %d MiB plus vramExternal %d MiB is %d MiB over the %d MiB total",
					id, mib, c.ReserveMiB, external, over, totalMiB)
			}
		}
		if c.Reader == nil {
			m.log.Infof("tenants: GPU memory is not measured; loads are checked against declared vram only")
		} else if r := m.read(); !r.ok {
			m.log.Infof("tenants: GPU memory is not measured (%v); loads are checked against declared vram only", r.err)
		} else {
			m.log.Infof("tenants: measured GPU memory in use at startup: %d MiB", r.miB)
		}
	}
	if router != nil {
		router.Wake()
	}
}

// reading is a GPU memory reading; ok is false when nothing was read.
type reading struct {
	miB int64
	ok  bool
	err error
}

// read takes a GPU memory reading no older than memMaxAge. It must be called
// without mu held: a slow nvidia-smi then stalls only the callers that want a
// reading, never the scheduler's other work. It reads nothing while loads are
// not checked against the total or when there is no reader.
func (m *Manager) read() reading {
	m.mu.Lock()
	reader, enforced := m.vram.reader, m.vram.enforced()
	m.mu.Unlock()
	if reader == nil || !enforced {
		return reading{err: fmt.Errorf("no GPU memory reader")}
	}
	m.memMu.Lock()
	defer m.memMu.Unlock()
	if !m.memAt.IsZero() && time.Since(m.memAt) < m.memMaxAge {
		return reading{miB: m.memMiB, ok: m.memErr == nil, err: m.memErr}
	}
	ctx, cancel := context.WithTimeout(context.Background(), measureTimeout)
	defer cancel()
	used, err := reader(ctx)
	m.memAt, m.memMiB, m.memErr = time.Now(), used, err
	if err != nil {
		if !hw.IsNvidiaSMIMissing(err) && m.memFailLogged != err.Error() {
			m.log.Warnf("tenants: reading GPU memory failed: %v", err)
			m.memFailLogged = err.Error()
		}
		return reading{err: err}
	}
	m.memFailLogged = ""
	return reading{miB: used, ok: true}
}

// TenantNeed is a running tenant's declared VRAM need, or, with Model set, a
// running model's own vram outside any tenant that declares one.
type TenantNeed struct {
	Tenant  string `json:"tenant"`
	Model   string `json:"model,omitempty"`
	VRAMMiB int    `json:"vramMiB"`
}

// needsLocked returns the needs of every tenant with a member in models,
// highest priority first.
func (m *Manager) needsLocked(models []string) []TenantNeed {
	needs := []TenantNeed{}
	for _, t := range m.tenants {
		if slices.ContainsFunc(t.cfg.Members, func(id string) bool { return slices.Contains(models, id) }) {
			needs = append(needs, TenantNeed{Tenant: t.name, VRAMMiB: t.cfg.VRAM})
		}
	}
	return needs
}

// declaredLocked returns what models, all running, declare: their tenants'
// needs plus the vram of each model whose tenant declares none, and the sum.
func (m *Manager) declaredLocked(models []string) (needs []TenantNeed, sum int64) {
	needs = m.needsLocked(models)
	for _, id := range slices.Sorted(slices.Values(models)) {
		if t := m.byModel[id]; t != nil && t.cfg.VRAM > 0 {
			continue
		}
		if mib := m.vram.models[id]; mib > 0 {
			needs = append(needs, TenantNeed{Model: id, VRAMMiB: mib})
		}
	}
	for _, n := range needs {
		sum += int64(n.VRAMMiB)
	}
	return needs, sum
}

// declaredOfLocked is the memory declared for the single running model id: its
// tenant's vram, or its own.
func (m *Manager) declaredOfLocked(id string) int {
	if t := m.byModel[id]; t != nil && t.cfg.VRAM > 0 {
		return t.cfg.VRAM
	}
	return m.vram.models[id]
}

// needOfLocked is the memory loading model adds, or 0 when it is already
// counted: its tenant declares vram and already has a member alongside.
func (m *Manager) needOfLocked(t *tenant, model string, alongside []string) (need int, counted bool) {
	if t != nil && t.cfg.VRAM > 0 {
		for _, n := range m.needsLocked(alongside) {
			if n.Tenant == t.name {
				return 0, true
			}
		}
		return t.cfg.VRAM, false
	}
	return m.vram.models[model], false
}

// reserveBlockLocked returns why loading model would take memory the gate
// keeps free, or nil when it fits. t is model's tenant, nil for a model outside
// any. alongside is what stays loaded if model loads. A model already
// loaded, and one whose tenant is already counted, is never blocked here.
//
// The load fits when neither the declared figure nor the measured one is over
// the card total: declared is the external users, the running models' vram,
// model's vram and the reserve; measured is the GPU memory in use now (less
// what this swap evicts), model's vram and the reserve.
func (m *Manager) reserveBlockLocked(t *tenant, model string, alongside []string, meas reading) *ReserveError {
	if !m.vram.enforced() || slices.Contains(alongside, model) {
		return nil
	}
	need, counted := m.needOfLocked(t, model, alongside)
	if counted {
		return nil
	}
	needs, used := m.declaredLocked(alongside)
	external := m.vram.externalMiB()
	reserve := int64(m.vram.reserveMiB)
	total := m.vram.totalMiB
	e := &ReserveError{Model: model, NeedMiB: need, TotalMiB: total, ReserveMiB: m.vram.reserveMiB,
		Running: needs, UsedMiB: used, ExternalMiB: external, RetryAfter: defaultRetryAfter}
	if t != nil {
		e.Tenant, e.RetryAfter = t.name, t.cfg.Interval
	}
	if external+used+int64(need)+reserve > total {
		return e
	}
	if !meas.ok {
		return nil
	}
	evicted := int64(0)
	if m.router != nil {
		var running []string
		for id := range m.router.RunningModels() {
			running = append(running, id)
		}
		_, all := m.declaredLocked(running)
		evicted = max(all-used, 0)
		// A process this swap evicts with no declared vram still sits in the
		// reading and cannot be subtracted: skip the measured check rather
		// than refuse a load for memory the swap frees.
		for _, id := range running {
			if !slices.Contains(alongside, id) && m.declaredOfLocked(id) == 0 {
				return nil
			}
		}
	}
	inUse := max(meas.miB-evicted, 0)
	e.MeasuredMiB = &inUse
	if inUse+int64(need)+reserve > total {
		return e
	}
	return nil
}

// ReserveError is the reason a request is held or refused because loading its
// model would take memory the gate keeps free. As an HTTP response it is a 503.
type ReserveError struct {
	Model       string
	Tenant      string // empty for a model outside any tenant
	NeedMiB     int
	TotalMiB    int64
	ReserveMiB  int
	Running     []TenantNeed // tenants and models that stay loaded alongside Model
	UsedMiB     int64        // sum of Running
	ExternalMiB int64        // sum of vramExternal
	MeasuredMiB *int64       // GPU memory in use now less what the swap evicts; nil when not read
	RetryAfter  int          // seconds
}

func (e *ReserveError) Error() string {
	who := "model " + e.Model
	if e.Tenant != "" {
		who += " (tenant " + e.Tenant + ")"
	}
	declared := int64(e.NeedMiB) + e.UsedMiB + e.ExternalMiB + int64(e.ReserveMiB)
	if declared <= e.TotalMiB && e.MeasuredMiB != nil {
		measured := *e.MeasuredMiB + int64(e.NeedMiB) + int64(e.ReserveMiB)
		return fmt.Sprintf("%s is blocked: its vram %d MiB plus %d MiB of GPU memory in use plus vramReserve %d MiB is %d MiB, over the %d MiB total",
			who, e.NeedMiB, *e.MeasuredMiB, e.ReserveMiB, measured, e.TotalMiB)
	}
	external := ""
	if e.ExternalMiB > 0 {
		external = fmt.Sprintf(" plus %d MiB of vramExternal", e.ExternalMiB)
	}
	return fmt.Sprintf("%s is blocked: its vram %d MiB plus %d MiB of running tenants%s plus vramReserve %d MiB is %d MiB, over the %d MiB total",
		who, e.NeedMiB, e.UsedMiB, external, e.ReserveMiB, declared, e.TotalMiB)
}

// Probe is the raw numbers the decision rests on.
func (e *ReserveError) Probe() string {
	running := make([]string, len(e.Running))
	for i, n := range e.Running {
		name := n.Tenant
		if n.Model != "" {
			name = n.Model
		}
		running[i] = fmt.Sprintf("%s:%d", name, n.VRAMMiB)
	}
	name := e.Tenant
	if name == "" {
		name = e.Model
	}
	probe := fmt.Sprintf("vram total=%dMiB reserve=%dMiB running=[%s] need=%s:%d",
		e.TotalMiB, e.ReserveMiB, strings.Join(running, " "), name, e.NeedMiB)
	if e.ExternalMiB > 0 {
		probe += fmt.Sprintf(" external=%dMiB", e.ExternalMiB)
	}
	if e.MeasuredMiB != nil {
		probe += fmt.Sprintf(" measured=%dMiB", *e.MeasuredMiB)
	}
	return probe
}

func (e *ReserveError) StatusCode() int { return http.StatusServiceUnavailable }

func (e *ReserveError) Header() http.Header {
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	h.Set("Retry-After", strconv.Itoa(max(e.RetryAfter, 1)))
	return h
}

func (e *ReserveError) Body() []byte {
	return swaputil.NewErrorEnvelope(e.StatusCode(), e.Error(), "tenant_vram_reserve").JSON()
}

var _ swaputil.HTTPError = (*ReserveError)(nil)

// VRAMStatus is the VRAM part of GET /api/tenants. TotalMiB is null when
// hardware detection found no accelerator memory. MeasuredMiB and UnknownMiB
// are null when GPU memory cannot be read (no nvidia-smi).
type VRAMStatus struct {
	TotalMiB    *int64       `json:"totalMiB"`
	ReserveMiB  int          `json:"reserveMiB"`
	Enforced    bool         `json:"enforced"`
	Running     []TenantNeed `json:"running"`
	UsedMiB     int64        `json:"usedMiB"`     // declared need of everything running
	ExternalMiB int64        `json:"externalMiB"` // sum of vramExternal
	MeasuredMiB *int64       `json:"measuredMiB"` // GPU memory in use now
	UnknownMiB  *int64       `json:"unknownMiB"`  // measured less declared less external
}

// vramStatusLocked reports the total, the reserve, the external users and the
// need of every running tenant and model, with the measured GPU memory and the
// part of it nobody declared.
func (m *Manager) vramStatusLocked(running []string, meas reading) VRAMStatus {
	needs, used := m.declaredLocked(running)
	vs := VRAMStatus{ReserveMiB: m.vram.reserveMiB, Enforced: m.vram.enforced(), Running: needs,
		UsedMiB: used, ExternalMiB: m.vram.externalMiB()}
	if m.vram.totalMiB > 0 {
		total := m.vram.totalMiB
		vs.TotalMiB = &total
	}
	if measured := meas.miB; meas.ok {
		unknown := measured - used - vs.ExternalMiB
		vs.MeasuredMiB, vs.UnknownMiB = &measured, &unknown
		m.noteUnknownLocked(unknown)
	}
	return vs
}

// noteUnknownLocked warns about GPU memory in use that llama-broker neither
// runs nor declares. It logs when the figure first passes the threshold and
// again each time it moves by more than the threshold, so a steady figure is
// logged once.
func (m *Manager) noteUnknownLocked(unknown int64) {
	if unknown <= unknownWarnMiB {
		if m.unknownLogged != 0 {
			m.log.Infof("tenants: unknown VRAM user gone: %d MiB", max(unknown, 0))
			m.unknownLogged = 0
		}
		return
	}
	if m.unknownLogged == 0 || abs64(unknown-m.unknownLogged) > unknownWarnMiB {
		m.log.Warnf("tenants: unknown VRAM user: %d MiB", unknown)
		m.unknownLogged = unknown
	}
}

func abs64(n int64) int64 {
	if n < 0 {
		return -n
	}
	return n
}

// watchMemory re-reads GPU memory every watchEvery while loads are checked
// against it. It warns about unknown users with nobody asking, and wakes the
// scheduler when requests wait, since memory freed outside llama-broker raises
// no event.
func (m *Manager) watchMemory(ctx context.Context) {
	ticker := time.NewTicker(watchEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		meas := m.read()
		m.mu.Lock()
		router := m.router
		var running []string
		if meas.ok && router != nil {
			for id := range router.RunningModels() {
				running = append(running, id)
			}
		}
		if meas.ok {
			m.vramStatusLocked(running, meas)
		}
		m.mu.Unlock()
		if meas.ok && router != nil && router.Queued() > 0 {
			router.Wake()
		}
	}
}
