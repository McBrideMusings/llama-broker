package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mostlygeek/llama-swap/internal/config"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

func newUpstreamGuardTestServer(t *testing.T, handler http.HandlerFunc) *Server {
	t.Helper()
	local := newStubRouter([]string{"qmd"}, "ok")
	local.serveHTTP = handler
	s := newTestServer(local, newStubRouter(nil, ""))
	guard := config.UpstreamGuardConfig{
		Paths: []string{`^/query$`, `^/health$`},
		Probe: config.UpstreamGuardProbeConfig{Path: "/query", Body: `{"probe":true}`, Expect: "results"},
	}
	cfg := config.UpstreamConfig{Guards: map[string]config.UpstreamGuardConfig{"qmd": guard}}
	// compile through the YAML path so the test exercises validation
	assert.NoError(t, cfg.UnmarshalYAML(mustYAMLNode(t, `guards:
  qmd:
    paths: ['^/query$', '^/health$']
    probe: {path: /query, body: '{"probe":true}', expect: results}
`)))
	s.cfg = config.Config{Models: map[string]config.ModelConfig{"qmd": {}}, Upstream: cfg}
	s.routes()
	return s
}

func guardResp(w http.ResponseWriter, r *http.Request, probeEmpty, userEmpty bool, probes *atomic.Int32) {
	body := `{"results":[{"id":1}]}`
	buf := new(strings.Builder)
	b := make([]byte, 64)
	n, _ := r.Body.Read(b)
	buf.Write(b[:n])
	if strings.Contains(buf.String(), "probe") {
		probes.Add(1)
		if probeEmpty {
			body = `{"results":[]}`
		}
	} else if r.URL.Path == "/health" || strings.HasSuffix(r.URL.Path, "/health") {
		body = `{"status":"ok"}`
	} else if userEmpty {
		body = `{"results":[]}`
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(body))
}

func TestUpstreamGuard_EmptyResultsWithFailingProbeIs503(t *testing.T) {
	var probes atomic.Int32
	s := newUpstreamGuardTestServer(t, func(w http.ResponseWriter, r *http.Request) { guardResp(w, r, true, true, &probes) })
	w := httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/upstream/qmd/query", strings.NewReader(`{"q":1}`)))
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Equal(t, int32(1), probes.Load())
}

func TestUpstreamGuard_EmptyResultsWithHealthyProbeIsForwarded(t *testing.T) {
	var probes atomic.Int32
	s := newUpstreamGuardTestServer(t, func(w http.ResponseWriter, r *http.Request) { guardResp(w, r, false, true, &probes) })
	w := httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/upstream/qmd/query", strings.NewReader(`{"q":1}`)))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.JSONEq(t, `{"results":[]}`, w.Body.String())
	assert.Equal(t, int32(1), probes.Load())
}

func TestUpstreamGuard_NonEmptyResultsSkipProbe(t *testing.T) {
	var probes atomic.Int32
	s := newUpstreamGuardTestServer(t, func(w http.ResponseWriter, r *http.Request) { guardResp(w, r, true, false, &probes) })
	w := httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/upstream/qmd/query", strings.NewReader(`{"q":1}`)))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, int32(0), probes.Load())
}

func TestUpstreamGuard_HealthRunsProbeAndFailsWhenProbeFails(t *testing.T) {
	var probes atomic.Int32
	s := newUpstreamGuardTestServer(t, func(w http.ResponseWriter, r *http.Request) { guardResp(w, r, true, false, &probes) })
	w := httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/upstream/qmd/health", nil))
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)

	// the failed probe is reused for cacheSeconds
	w = httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/upstream/qmd/health", nil))
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Equal(t, int32(1), probes.Load())
}

func mustYAMLNode(t *testing.T, src string) *yaml.Node {
	t.Helper()
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte(src), &doc); err != nil {
		t.Fatal(err)
	}
	return doc.Content[0]
}
