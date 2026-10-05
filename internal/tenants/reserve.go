package tenants

import (
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/mostlygeek/llama-swap/internal/hw"
	"github.com/mostlygeek/llama-swap/internal/swaputil"
)

// vram is the card total and the reserve kept free for GPU users llama-broker
// does not manage. A tenant's need is its vram setting, counted while any of
// its models is running.
type vram struct {
	totalMiB   int64 // 0 when hardware detection found no accelerator memory
	reserveMiB int
}

// enforced reports whether loads are checked against the reserve.
func (v vram) enforced() bool { return v.reserveMiB > 0 && v.totalMiB > 0 }

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

// SetVRAM sets the card total and the reserve, both in MiB, and wakes the
// scheduler so held requests are re-checked. A zero total leaves the reserve
// unenforced, which is logged as a warning.
func (m *Manager) SetVRAM(totalMiB int64, reserveMiB int) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.vram = vram{totalMiB: totalMiB, reserveMiB: reserveMiB}
	router := m.router
	m.mu.Unlock()
	switch {
	case reserveMiB == 0:
		m.log.Debugf("tenants: vram total=%d MiB, no vramReserve", totalMiB)
	case totalMiB == 0:
		m.log.Warnf("tenants: vramReserve=%d MiB not enforced: hardware detection reported no accelerator memory", reserveMiB)
	default:
		m.log.Infof("tenants: vram total=%d MiB reserve=%d MiB; tenants may use %d MiB", totalMiB, reserveMiB, totalMiB-int64(reserveMiB))
		for _, t := range m.tenants {
			if over := int64(t.cfg.VRAM+reserveMiB) - totalMiB; over > 0 {
				m.log.Warnf("tenants: %s can never load: vram %d MiB plus vramReserve %d MiB is %d MiB over the %d MiB total",
					t.name, t.cfg.VRAM, reserveMiB, over, totalMiB)
			}
		}
	}
	if router != nil {
		router.Wake()
	}
}

// TenantNeed is a running tenant's declared VRAM need.
type TenantNeed struct {
	Tenant  string `json:"tenant"`
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

// reserveBlockLocked returns why loading model would cut into the reserve, or
// nil when it fits. alongside is what stays loaded if model loads. A tenant
// already running alongside is already counted, so it is never blocked here.
func (m *Manager) reserveBlockLocked(t *tenant, model string, alongside []string) *ReserveError {
	if !m.vram.enforced() {
		return nil
	}
	needs := m.needsLocked(alongside)
	var used int64
	for _, n := range needs {
		if n.Tenant == t.name {
			return nil
		}
		used += int64(n.VRAMMiB)
	}
	if used+int64(t.cfg.VRAM)+int64(m.vram.reserveMiB) <= m.vram.totalMiB {
		return nil
	}
	return &ReserveError{Model: model, Tenant: t.name, NeedMiB: t.cfg.VRAM, TotalMiB: m.vram.totalMiB,
		ReserveMiB: m.vram.reserveMiB, Running: needs, UsedMiB: used, RetryAfter: t.cfg.Interval}
}

// ReserveError is the reason a request is held or refused because its tenant's
// VRAM need would cut into the reserve. As an HTTP response it is a 503.
type ReserveError struct {
	Model      string
	Tenant     string
	NeedMiB    int
	TotalMiB   int64
	ReserveMiB int
	Running    []TenantNeed // tenants that stay loaded alongside Model
	UsedMiB    int64        // sum of Running
	RetryAfter int          // seconds
}

func (e *ReserveError) Error() string {
	return fmt.Sprintf("model %s (tenant %s) is blocked: its vram %d MiB plus %d MiB of running tenants plus vramReserve %d MiB is %d MiB, over the %d MiB total",
		e.Model, e.Tenant, e.NeedMiB, e.UsedMiB, e.ReserveMiB, int64(e.NeedMiB)+e.UsedMiB+int64(e.ReserveMiB), e.TotalMiB)
}

// Probe is the raw numbers the decision rests on.
func (e *ReserveError) Probe() string {
	running := make([]string, len(e.Running))
	for i, n := range e.Running {
		running[i] = fmt.Sprintf("%s:%d", n.Tenant, n.VRAMMiB)
	}
	return fmt.Sprintf("vram total=%dMiB reserve=%dMiB running=[%s] need=%s:%d",
		e.TotalMiB, e.ReserveMiB, strings.Join(running, " "), e.Tenant, e.NeedMiB)
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
// hardware detection found no accelerator memory.
type VRAMStatus struct {
	TotalMiB   *int64       `json:"totalMiB"`
	ReserveMiB int          `json:"reserveMiB"`
	Enforced   bool         `json:"enforced"`
	Running    []TenantNeed `json:"running"`
	UsedMiB    int64        `json:"usedMiB"`
}

// vramStatusLocked reports the total, the reserve and the need of every
// tenant with a running model.
func (m *Manager) vramStatusLocked(running []string) VRAMStatus {
	vs := VRAMStatus{ReserveMiB: m.vram.reserveMiB, Enforced: m.vram.enforced(), Running: m.needsLocked(running)}
	if m.vram.totalMiB > 0 {
		total := m.vram.totalMiB
		vs.TotalMiB = &total
	}
	for _, n := range vs.Running {
		vs.UsedMiB += int64(n.VRAMMiB)
	}
	return vs
}
