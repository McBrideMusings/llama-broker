package tenants

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mostlygeek/llama-swap/internal/config"
	"github.com/mostlygeek/llama-swap/internal/logmon"
	"github.com/mostlygeek/llama-swap/internal/process"
)

func TestTenants_LowerTenantHeldUntilHigherConditionProbed(t *testing.T) {
	answer := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-answer
		io.WriteString(w, `{"on": false}`)
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() {
		select {
		case <-answer:
		default:
			close(answer)
		}
	})
	log := logmon.New()
	m := New(twoTenants(srv.URL, config.TenantOnBlockedRefuse), log)
	r := &fakeRouter{running: map[string]process.ProcessState{}}
	m.Start(t.Context(), r)

	reason, refuse := m.Block("lo-model", nil)
	var pending *PendingError
	if !errors.As(reason, &pending) || !refuse {
		t.Fatalf("lo Block before hi's first probe = %v, refuse %v; want a PendingError, refused", reason, refuse)
	}
	if pending.StatusCode() != http.StatusServiceUnavailable || pending.Header().Get("Retry-After") != "1" {
		t.Fatalf("pending error is %d with Retry-After %q, want 503 and 1", pending.StatusCode(), pending.Header().Get("Retry-After"))
	}
	if blocked, _ := m.Block("hi-model", nil); blocked != nil {
		t.Fatalf("hi blocked by its own pending condition: %v", blocked)
	}
	m.Record("lo-model", reason, refuse)
	want := `tenant=lo model=lo-model action=refuse probe="condition of hi: not read yet" reason="model lo-model (tenant lo, priority 1) is blocked: the condition of tenant hi (priority 10) has not been probed yet"`
	if history := string(log.GetHistory()); !strings.Contains(history, want) {
		t.Fatalf("log lacks %q\nlog:\n%s", want, history)
	}

	close(answer)
	eventually(t, "hi's first reading to release lo", func() bool { reason, _ := m.Block("lo-model", nil); return reason == nil })
	if _, wakes := r.snapshot(); wakes < 1 {
		t.Fatalf("wakes=%d after the first reading, want >= 1", wakes)
	}
}

func TestTenants_FailedFirstProbeReleasesLowerTenant(t *testing.T) {
	answer := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-answer
		io.WriteString(w, `not json`)
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() {
		select {
		case <-answer:
		default:
			close(answer)
		}
	})
	m := New(twoTenants(srv.URL, config.TenantOnBlockedHold), logmon.NewWriter(io.Discard))
	r := &fakeRouter{running: map[string]process.ProcessState{}}
	m.Start(t.Context(), r)

	reason, refuse := m.Block("lo-model", nil)
	var pending *PendingError
	if !errors.As(reason, &pending) || refuse {
		t.Fatalf("lo Block before hi's first probe = %v, refuse %v; want a PendingError, held", reason, refuse)
	}

	close(answer)
	eventually(t, "hi's failed first probe to release lo", func() bool { reason, _ := m.Block("lo-model", nil); return reason == nil })
	if wantsGPU(m, "hi") {
		t.Fatal("a failed first probe claimed the GPU")
	}
	if _, wakes := r.snapshot(); wakes < 1 {
		t.Fatalf("wakes=%d after the failed probe, want >= 1", wakes)
	}
}
