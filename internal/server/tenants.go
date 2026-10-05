package server

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/mostlygeek/llama-swap/internal/event"
	"github.com/mostlygeek/llama-swap/internal/hw"
	"github.com/mostlygeek/llama-swap/internal/router"
	"github.com/mostlygeek/llama-swap/internal/swaputil"
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
// and the configured reserve. Both local routers implement router.VRAMSetter.
func setTenantVRAM(local router.LocalRouter, hardware *hw.HardwareSnapshot, reserveMiB int) {
	if v, ok := local.(router.VRAMSetter); ok {
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

// preload sends one hooks.on_startup.preload request for modelID and waits
// until it is served or the tenant gate holds it, so a held preload does not
// delay the models listed after it. A held request completes in the
// background once the gate opens.
func (s *Server) preload(modelID string) {
	tenants.Preload(s.shutdownCtx, func(ctx context.Context) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "/", nil)
		if err != nil {
			return
		}
		req = req.WithContext(swaputil.SetContext(req.Context(), swaputil.ReqContextData{Model: modelID, ModelID: modelID, Metadata: make(map[string]string)}))

		dw := &discardResponseWriter{status: http.StatusOK}
		s.local.ServeHTTP(dw, req)

		success := dw.status < http.StatusBadRequest
		if !success {
			s.logs.ProxyLogs.Errorf("failed to preload model %s: status %d", modelID, dw.status)
		}
		event.Emit(swaputil.ModelPreloadedEvent{ModelName: modelID, Success: success})
	})
}

// onTenantDecision forwards tenant decisions to an /api/events client.
func onTenantDecision(send func(messageEnvelope)) context.CancelFunc {
	return event.On(func(e tenants.TenantDecisionEvent) {
		if data, err := json.Marshal(e.Decision); err == nil {
			send(messageEnvelope{Type: msgTypeTenantDecision, Data: string(data)})
		}
	})
}
