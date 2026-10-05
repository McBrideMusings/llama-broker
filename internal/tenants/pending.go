package tenants

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/mostlygeek/llama-swap/internal/swaputil"
)

// unprobedLocked reports whether t has a condition that has not been probed
// yet. Until the first probe finishes, as a reading or a failure, nobody knows
// whether t wants the GPU.
func (t *tenant) unprobedLocked() bool {
	return t.cfg.Condition != nil && t.condAt.IsZero() && t.condErrAt.IsZero()
}

// pendingLocked returns the highest-priority tenant above t whose condition has
// not been probed yet.
func (m *Manager) pendingLocked(t *tenant) *tenant {
	for _, h := range m.tenants {
		if h.cfg.Priority <= t.cfg.Priority {
			return nil
		}
		if h.unprobedLocked() {
			return h
		}
	}
	return nil
}

// PendingError is the reason a request is held or refused while a higher
// tenant's first condition probe has not finished. As an HTTP response it is a
// 503 naming both tenants.
type PendingError struct {
	Model      string
	Tenant     string
	Priority   int
	By         string
	ByPriority int
	RetryAfter int // seconds
}

func (e *PendingError) Error() string {
	return fmt.Sprintf("model %s (tenant %s, priority %d) is blocked: the condition of tenant %s (priority %d) has not been probed yet",
		e.Model, e.Tenant, e.Priority, e.By, e.ByPriority)
}

func (e *PendingError) StatusCode() int { return http.StatusServiceUnavailable }

func (e *PendingError) Header() http.Header {
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	h.Set("Retry-After", strconv.Itoa(max(e.RetryAfter, 1)))
	return h
}

func (e *PendingError) Body() []byte {
	return swaputil.NewErrorEnvelope(e.StatusCode(), e.Error(), "tenant_condition_pending").JSON()
}

var _ swaputil.HTTPError = (*PendingError)(nil)
