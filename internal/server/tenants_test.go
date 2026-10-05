package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mostlygeek/llama-swap/internal/tenants"
)

// tenantRouter is a stubRouter that reports tenant state.
type tenantRouter struct {
	*stubRouter
	status tenants.Status
}

func (r *tenantRouter) TenantStatus() tenants.Status { return r.status }

func TestTenants_APITenantsEndpoint(t *testing.T) {
	get := func(s *Server) tenants.Status {
		t.Helper()
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/tenants", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("GET /api/tenants = %d %s", rec.Code, rec.Body)
		}
		var st tenants.Status
		if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil {
			t.Fatalf("decoding %s: %v", rec.Body, err)
		}
		return st
	}

	if st := get(newTestServer(newStubRouter(nil, ""), newStubRouter(nil, ""))); st.Tenants == nil || len(st.Tenants) != 0 {
		t.Fatalf("router without tenants: %+v, want an empty list", st)
	}

	want := tenants.Status{Tenants: []tenants.TenantStatus{{
		Name: "lo", Priority: 1, OnBlocked: "hold", Models: []string{"lo"}, Held: 3,
		Loaded: []tenants.LoadedModel{{Model: "lo", State: "ready"}},
	}}}
	st := get(newTestServer(&tenantRouter{stubRouter: newStubRouter(nil, ""), status: want}, newStubRouter(nil, "")))
	if len(st.Tenants) != 1 || st.Tenants[0].Held != 3 || st.Tenants[0].Loaded[0].Model != "lo" {
		t.Fatalf("GET /api/tenants = %+v, want %+v", st, want)
	}
}
