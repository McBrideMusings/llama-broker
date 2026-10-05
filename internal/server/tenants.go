package server

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/mostlygeek/llama-swap/internal/event"
	"github.com/mostlygeek/llama-swap/internal/hw"
	"github.com/mostlygeek/llama-swap/internal/tenants"
)

// msgTypeTenantDecision carries one tenants.Decision on GET /api/events.
const msgTypeTenantDecision messageType = "tenantDecision"

// tenantStatus is implemented by local routers built from a config with
// tenants support.
type tenantStatus interface {
	TenantStatus() tenants.Status
}

// setTenantVRAM hands the tenant gate the card total from hardware detection
// and the configured reserve.
func setTenantVRAM(local any, hardware *hw.HardwareSnapshot, reserveMiB int) {
	if v, ok := local.(interface{ SetVRAM(int64, int) }); ok {
		v.SetVRAM(tenants.TotalVRAMMiB(hardware), reserveMiB)
	}
}

// handleAPITenants returns the VRAM total, reserve and running tenants' needs,
// and each tenant's priority, condition, busy state, loaded models and held
// request count. Without tenants the list is empty.
func (s *Server) handleAPITenants(w http.ResponseWriter, r *http.Request) {
	st := tenants.Status{VRAM: tenants.VRAMStatus{Running: []tenants.TenantNeed{}}, Tenants: []tenants.TenantStatus{}}
	if ts, ok := s.local.(tenantStatus); ok {
		st = ts.TenantStatus()
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(st)
}

// onTenantDecision forwards tenant decisions to an /api/events client.
func onTenantDecision(send func(messageEnvelope)) context.CancelFunc {
	return event.On(func(e tenants.TenantDecisionEvent) {
		if data, err := json.Marshal(e.Decision); err == nil {
			send(messageEnvelope{Type: msgTypeTenantDecision, Data: string(data)})
		}
	})
}
