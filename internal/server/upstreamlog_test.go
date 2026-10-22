package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mostlygeek/llama-swap/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newUpstreamLogTestServer(t *testing.T, maxMB int, handler http.HandlerFunc) (*Server, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "sub", "qmd.jsonl")
	rl := config.UpstreamRequestLogConfig{Path: path, Models: []string{"qmd"}, MaxSizeMB: maxMB, MaxBackups: 2}
	l, err := newUpstreamRequestLog(rl)
	require.NoError(t, err)
	t.Cleanup(func() { l.Close() })

	local := newStubRouter([]string{"qmd", "other"}, "ok")
	local.serveHTTP = handler
	s := newTestServer(local, newStubRouter(nil, ""))
	s.cfg = config.Config{Models: map[string]config.ModelConfig{"qmd": {}, "other": {}}}
	s.upstreamLog = l
	s.routes()
	return s, path
}

func readEntries(t *testing.T, s *Server) []upstreamLogEntry {
	t.Helper()
	w := httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/upstream/requests", nil))
	require.Equal(t, http.StatusOK, w.Code)
	var out struct {
		Requests []upstreamLogEntry `json:"requests"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	return out.Requests
}

func TestUpstreamLog_MCPToolCallOverSSE(t *testing.T) {
	s, _ := newUpstreamLogTestServer(t, 1, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		result := `{"jsonrpc":"2.0","id":1,"result":{"structuredContent":{"results":[{"file":"knowledge/fleet/a.md"},{"file":"b.md"}]},"content":[{"type":"text","text":"2 hits"}]}}`
		w.Write([]byte("event: message\ndata: " + result + "\n\n"))
	})
	body := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"query","arguments":{"searches":[{"type":"lex","query":"broker"}],"limit":8}}}`
	req := httptest.NewRequest(http.MethodPost, "/upstream/qmd/mcp", strings.NewReader(body))
	req.RemoteAddr = "100.1.2.3:5555"
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "knowledge/fleet/a.md", "client still receives the response")

	got := readEntries(t, s)
	require.Len(t, got, 1)
	e := got[0]
	assert.Equal(t, "100.1.2.3", e.Client)
	assert.Equal(t, "qmd", e.Model)
	assert.Equal(t, "query", e.Tool)
	assert.JSONEq(t, `{"searches":[{"type":"lex","query":"broker"}],"limit":8}`, string(e.Query))
	assert.Equal(t, 200, e.Status)
	require.NotNil(t, e.Results)
	assert.Equal(t, 2, *e.Results)
	assert.Equal(t, []string{"knowledge/fleet/a.md", "b.md"}, e.Top)
	assert.Nil(t, e.Error)
	_, err := time.Parse(time.RFC3339Nano, e.TS)
	assert.NoError(t, err)
}

func TestUpstreamLog_PlainQueryEmptyResult(t *testing.T) {
	s, _ := newUpstreamLogTestServer(t, 1, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"results":[]}`))
	})
	req := httptest.NewRequest(http.MethodPost, "/upstream/qmd/query", strings.NewReader(`{"searches":[{"type":"vec","query":"x"}],"limit":5}`))
	req.Header.Set("X-Client-Name", "hermes")
	s.ServeHTTP(httptest.NewRecorder(), req)

	e := readEntries(t, s)[0]
	assert.Equal(t, "hermes", e.Client)
	assert.Equal(t, "query", e.Tool)
	require.NotNil(t, e.Results)
	assert.Equal(t, 0, *e.Results)
	assert.Empty(t, e.Top)
}

func TestUpstreamLog_FailuresAreLogged(t *testing.T) {
	s, _ := newUpstreamLogTestServer(t, 1, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/boom") {
			http.Error(w, "upstream exploded", http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"bad params"}}`))
	})
	s.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/upstream/qmd/boom", strings.NewReader(`{"q":1}`)))
	s.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/upstream/qmd/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get","arguments":{}}}`)))
	s.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/upstream/qmd/health", nil))

	got := readEntries(t, s)
	require.Len(t, got, 3)
	assert.Equal(t, 502, got[0].Status)
	require.NotNil(t, got[0].Error)
	assert.Contains(t, *got[0].Error, "upstream exploded")
	assert.Equal(t, "get", got[1].Tool)
	require.NotNil(t, got[1].Error)
	assert.Equal(t, "bad params", *got[1].Error)
	assert.Equal(t, "health", got[2].Tool)
	assert.Equal(t, "null", string(got[2].Query))
	assert.Nil(t, got[2].Results)
}

func TestUpstreamLog_OnlyListedModels(t *testing.T) {
	s, _ := newUpstreamLogTestServer(t, 1, func(w http.ResponseWriter, r *http.Request) {})
	s.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/upstream/other/x", nil))
	assert.Empty(t, readEntries(t, s))
}

func TestUpstreamLog_SinceAndModelFilter(t *testing.T) {
	s, _ := newUpstreamLogTestServer(t, 1, func(w http.ResponseWriter, r *http.Request) {})
	s.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/upstream/qmd/a", nil))
	time.Sleep(5 * time.Millisecond)
	cut := time.Now().UTC().Format(time.RFC3339Nano)
	time.Sleep(5 * time.Millisecond)
	s.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/upstream/qmd/b", nil))

	w := httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/upstream/requests?since="+cut+"&model=qmd", nil))
	var out struct{ Requests []upstreamLogEntry }
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Len(t, out.Requests, 1)
	assert.Equal(t, "b", out.Requests[0].Tool)

	w = httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/upstream/requests?since=yesterday", nil))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestUpstreamLog_NotConfigured(t *testing.T) {
	s := newTestServer(newStubRouter(nil, ""), newStubRouter(nil, ""))
	w := httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/upstream/requests", nil))
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestUpstreamLog_RotatesBySize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "r.jsonl")
	l, err := newUpstreamRequestLog(config.UpstreamRequestLogConfig{Path: path, Models: []string{"qmd"}, MaxSizeMB: 1, MaxBackups: 2})
	require.NoError(t, err)
	defer l.Close()
	l.maxBytes = 300 // force rotation after a couple of lines

	for i := 0; i < 12; i++ {
		require.NoError(t, l.write(upstreamLogEntry{TS: time.Now().UTC().Format(time.RFC3339Nano), Model: "qmd", Tool: "t", Query: json.RawMessage("null")}))
	}
	got, err := l.read(time.Time{}, "", 100)
	require.NoError(t, err)
	assert.Less(t, len(got), 12, "oldest backup beyond maxBackups was dropped")
	assert.FileExists(t, path+".1")
	assert.FileExists(t, path+".2")
	assert.NoFileExists(t, path+".3")
}

func TestUpstreamLog_ConfigValidation(t *testing.T) {
	_, err := config.LoadConfigFromReader(strings.NewReader(`
models:
  m:
    cmd: x
    proxy: "http://localhost:1"
upstream:
  requestLog:
    path: /tmp/x.jsonl
`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "models must list")

	cfg, err := config.LoadConfigFromReader(strings.NewReader(`
models:
  m:
    cmd: x
    proxy: "http://localhost:1"
upstream:
  requestLog:
    path: /tmp/x.jsonl
    models: [m]
`))
	require.NoError(t, err)
	assert.Equal(t, 10, cfg.Upstream.RequestLog.MaxSizeMB)
	assert.Equal(t, 3, cfg.Upstream.RequestLog.MaxBackups)
}

func TestUpstreamLog_AbortedHandlerIsLogged(t *testing.T) {
	s, _ := newUpstreamLogTestServer(t, 1, func(w http.ResponseWriter, r *http.Request) {
		panic(http.ErrAbortHandler)
	})
	func() {
		defer func() { assert.Equal(t, http.ErrAbortHandler, recover(), "panic is re-raised") }()
		s.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/upstream/qmd/stream", nil))
	}()
	got := readEntries(t, s)
	require.Len(t, got, 1)
	assert.Equal(t, 502, got[0].Status)
	require.NotNil(t, got[0].Error)
	assert.Contains(t, *got[0].Error, "aborted")
}

func TestUpstreamLog_ReopensAfterFailedRotation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "r.jsonl")
	l, err := newUpstreamRequestLog(config.UpstreamRequestLogConfig{Path: path, Models: []string{"qmd"}, MaxSizeMB: 1, MaxBackups: 1})
	require.NoError(t, err)
	defer l.Close()
	entry := upstreamLogEntry{TS: time.Now().UTC().Format(time.RFC3339Nano), Model: "qmd", Query: json.RawMessage("null")}
	require.NoError(t, l.write(entry))

	l.file.Close() // simulate a handle lost to a failed rotation
	l.file = nil
	require.NoError(t, l.write(entry), "write reopens the file")
	got, err := l.read(time.Time{}, "", 10)
	require.NoError(t, err)
	assert.Len(t, got, 2)
}
