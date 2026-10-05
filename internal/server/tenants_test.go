package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mostlygeek/llama-swap/internal/swaputil"
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

func TestTenants_PreloadMarksRequestContext(t *testing.T) {
	local := newStubRouter([]string{"m1"}, "")
	var got context.Context
	local.serveHTTP = func(w http.ResponseWriter, r *http.Request) {
		got = r.Context()
		w.WriteHeader(http.StatusOK)
	}
	s := newTestServer(local, newStubRouter(nil, ""))
	defer s.shutdownFn()

	s.preload("m1")
	if got == nil {
		t.Fatal("preload did not reach the local router")
	}
	if data, ok := got.Value(swaputil.ReqContextKey).(swaputil.ReqContextData); !ok || data.ModelID != "m1" {
		t.Fatalf("request context data = %+v, want ModelID m1", data)
	}

	// The mark is unexported; HoldPreload only rewrites a refusal on a marked ctx.
	blocked := errors.New("tenant hi wants the GPU")
	reason, refuse := tenants.HoldPreload(got, blocked, true)
	if refuse {
		t.Fatal("preload request context is not marked: HoldPreload kept refuse=true")
	}
	if !strings.HasPrefix(reason.Error(), "preload held, onBlocked: refuse does not apply to preloads") || !errors.Is(reason, blocked) {
		t.Fatalf("HoldPreload reason = %v, want the wrapped preload-held reason", reason)
	}
}
