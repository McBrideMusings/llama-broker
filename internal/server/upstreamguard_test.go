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

const (
	mcpEmptyJSON = `{"jsonrpc":"2.0","id":7,"result":{"structuredContent":{"results":[]},"content":[{"type":"text","text":"{\"results\":[]}"}]}}`
	mcpEmptySSE  = "event: message\ndata: " + mcpEmptyJSON + "\n\n"
	mcpToolCall  = `{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"query","arguments":{"searches":[{"type":"vec","query":"x"}]}}}`
)

func newMCPGuardTestServer(t *testing.T, handler http.HandlerFunc) *Server {
	t.Helper()
	local := newStubRouter([]string{"qmd"}, "ok")
	local.serveHTTP = handler
	s := newTestServer(local, newStubRouter(nil, ""))
	var cfg config.UpstreamConfig
	assert.NoError(t, cfg.UnmarshalYAML(mustYAMLNode(t, `guards:
  qmd:
    paths: ['^/query$', '^/mcp$']
    mcpTools: [query]
    probe: {path: /query, body: '{"probe":true}', expect: results}
`)))
	s.cfg = config.Config{Models: map[string]config.ModelConfig{"qmd": {}}, Upstream: cfg}
	s.routes()
	return s
}

// mcpHandler answers /mcp with the recorded empty body (JSON or SSE) and the
// probe with hits or none.
func mcpHandler(sse, probeEmpty bool, probes *atomic.Int32) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		b := make([]byte, 128)
		n, _ := r.Body.Read(b)
		if strings.Contains(string(b[:n]), `"probe"`) {
			probes.Add(1)
			w.Header().Set("Content-Type", "application/json")
			if probeEmpty {
				w.Write([]byte(`{"results":[]}`))
			} else {
				w.Write([]byte(`{"results":[{"id":1}]}`))
			}
			return
		}
		if sse {
			w.Header().Set("Content-Type", "text/event-stream")
			w.Write([]byte(mcpEmptySSE))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(mcpEmptyJSON))
	}
}

func TestUpstreamGuard_MCPEmptyQueryWithFailingProbeIsToolErrorJSON(t *testing.T) {
	var probes atomic.Int32
	s := newMCPGuardTestServer(t, mcpHandler(false, true, &probes))
	w := httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/upstream/qmd/mcp", strings.NewReader(mcpToolCall)))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
	assert.JSONEq(t, `{"jsonrpc":"2.0","id":7,"result":{"isError":true,"content":[{"type":"text","text":"upstream qmd cannot search right now: probe POST /query returned no \"results\""}]}}`, w.Body.String())
	assert.Equal(t, int32(1), probes.Load())
}

func TestUpstreamGuard_MCPEmptyQueryWithFailingProbeIsToolErrorSSE(t *testing.T) {
	var probes atomic.Int32
	s := newMCPGuardTestServer(t, mcpHandler(true, true, &probes))
	w := httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/upstream/qmd/mcp", strings.NewReader(mcpToolCall)))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "text/event-stream", w.Header().Get("Content-Type"))
	data, ok := strings.CutPrefix(strings.SplitN(w.Body.String(), "\n", 3)[1], "data: ")
	assert.True(t, ok, w.Body.String())
	assert.Contains(t, w.Body.String(), "event: message\n")
	assert.JSONEq(t, `{"jsonrpc":"2.0","id":7,"result":{"isError":true,"content":[{"type":"text","text":"upstream qmd cannot search right now: probe POST /query returned no \"results\""}]}}`, data)
}

func TestUpstreamGuard_MCPEmptyQueryWithHealthyProbeIsForwarded(t *testing.T) {
	for _, sse := range []bool{false, true} {
		var probes atomic.Int32
		s := newMCPGuardTestServer(t, mcpHandler(sse, false, &probes))
		w := httptest.NewRecorder()
		s.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/upstream/qmd/mcp", strings.NewReader(mcpToolCall)))
		assert.Equal(t, http.StatusOK, w.Code)
		want := mcpEmptyJSON
		if sse {
			want = mcpEmptySSE
		}
		assert.Equal(t, want, w.Body.String())
		assert.Equal(t, int32(1), probes.Load())
	}
}

func TestUpstreamGuard_MCPOtherMethodsPassThroughUnprobed(t *testing.T) {
	for _, body := range []string{
		`{"jsonrpc":"2.0","id":1,"method":"ping"}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`,
		`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"get","arguments":{}}}`,
	} {
		var probes atomic.Int32
		s := newMCPGuardTestServer(t, mcpHandler(false, true, &probes))
		w := httptest.NewRecorder()
		s.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/upstream/qmd/mcp", strings.NewReader(body)))
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, mcpEmptyJSON, w.Body.String(), body)
		assert.Equal(t, int32(0), probes.Load(), body)
	}
}

func TestUpstreamGuard_MCPStreamBatchAndErrorsPassThrough(t *testing.T) {
	var probes atomic.Int32
	s := newMCPGuardTestServer(t, mcpHandler(false, true, &probes))

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/upstream/qmd/mcp", nil)
	req.Header.Set("Accept", "text/event-stream")
	s.ServeHTTP(w, req)
	assert.Equal(t, mcpEmptyJSON, w.Body.String())

	w = httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/upstream/qmd/mcp", strings.NewReader("["+mcpToolCall+"]")))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, mcpEmptyJSON, w.Body.String())
	assert.Equal(t, int32(0), probes.Load())
}
