package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/mostlygeek/llama-swap/internal/chain"
	"github.com/mostlygeek/llama-swap/internal/config"
	"github.com/mostlygeek/llama-swap/internal/swaputil"
)

// bufferedResponse holds a handler's whole response so the guard can decide
// whether the client receives it.
type bufferedResponse struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func newBufferedResponse() *bufferedResponse {
	return &bufferedResponse{header: http.Header{}, status: http.StatusOK}
}

func (b *bufferedResponse) Header() http.Header         { return b.header }
func (b *bufferedResponse) WriteHeader(code int)        { b.status = code }
func (b *bufferedResponse) Write(p []byte) (int, error) { return b.body.Write(p) }
func (b *bufferedResponse) Flush()                      {}

func (b *bufferedResponse) ok() bool { return b.status >= 200 && b.status < 300 }

// hasResults reports whether the body is a JSON object whose field is a
// non-empty array.
func hasResults(body []byte, field string) bool {
	var obj map[string]json.RawMessage
	if json.Unmarshal(body, &obj) != nil {
		return false
	}
	var arr []json.RawMessage
	if json.Unmarshal(obj[field], &arr) != nil {
		return false
	}
	return len(arr) > 0
}

// guardMaxRequestBytes caps the request body the guard reads to find an MCP
// tool call; a larger body is passed through unchecked.
const guardMaxRequestBytes = 1 << 20

type upstreamGuard struct {
	cfg config.UpstreamGuardConfig

	mu        sync.Mutex
	checkedAt time.Time
	lastErr   error
}

// check runs the probe through next. A probe that answered with an empty
// array is reused for cacheSeconds; a probe that failed for any other reason
// (a 5xx during a swap, a timeout) is not, so a transient fault does not
// refuse legitimate answers for the whole window.
func (g *upstreamGuard) check(r *http.Request, searchName string, next http.Handler) error {
	ttl := time.Duration(g.cfg.Probe.CacheSeconds) * time.Second
	g.mu.Lock()
	if !g.checkedAt.IsZero() && time.Since(g.checkedAt) < ttl {
		err := g.lastErr
		g.mu.Unlock()
		return err
	}
	g.mu.Unlock()

	p := g.cfg.Probe
	path := "/" + strings.TrimPrefix(p.Path, "/")
	req, err := http.NewRequestWithContext(context.WithoutCancel(r.Context()), p.Method, "/upstream/"+searchName+path, strings.NewReader(p.Body))
	if err != nil {
		return err
	}
	req.SetPathValue("upstreamPath", searchName+path)
	if auth := r.Header.Get("Authorization"); auth != "" {
		req.Header.Set("Authorization", auth)
	}
	if p.Body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	req.RemoteAddr = r.RemoteAddr

	ctx, cancel := context.WithTimeout(req.Context(), 30*time.Second)
	defer cancel()
	req = req.WithContext(ctx)

	rec := newBufferedResponse()
	next.ServeHTTP(rec, req)
	var cacheable bool
	switch {
	case !rec.ok():
		err = fmt.Errorf("probe %s %s returned status %d", p.Method, path, rec.status)
	case !hasResults(rec.body.Bytes(), p.Expect):
		err = fmt.Errorf("probe %s %s returned no %q", p.Method, path, p.Expect)
		cacheable = true
	default:
		cacheable = true
	}
	if cacheable {
		g.mu.Lock()
		g.checkedAt = time.Now()
		g.lastErr = err
		g.mu.Unlock()
	}
	return err
}

// CreateUpstreamGuardMiddleware applies upstream.guards. A 2xx response on a
// guarded path whose probe array is empty or absent (including a /health
// body) triggers the probe; when the probe finds nothing too, the client gets
// 503 instead of a misleading success.
func CreateUpstreamGuardMiddleware(cfg config.Config, logf func(format string, args ...any)) chain.Middleware {
	guards := map[string]*upstreamGuard{}
	for name, g := range cfg.Upstream.Guards {
		guards[name] = &upstreamGuard{cfg: g}
	}

	return func(next http.Handler) http.Handler {
		if len(guards) == 0 {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			searchName, modelID, remaining, found := swaputil.FindModelInPath(cfg, strings.TrimPrefix(r.URL.Path, "/upstream"))
			if !found {
				next.ServeHTTP(w, r)
				return
			}
			g, ok := guards[searchName]
			if !ok {
				g, ok = guards[modelID]
			}
			if !ok {
				next.ServeHTTP(w, r)
				return
			}
			guarded := false
			for _, re := range g.cfg.PathRegexps {
				if re.MatchString(remaining) {
					guarded = true
					break
				}
			}
			if !guarded {
				next.ServeHTTP(w, r)
				return
			}

			var rpcID json.RawMessage
			isMCP := false
			if len(g.cfg.MCPTools) > 0 {
				// MCP's stream channel and session teardown are never buffered.
				if r.Method == http.MethodDelete || (r.Method == http.MethodGet && strings.Contains(r.Header.Get("Accept"), "text/event-stream")) {
					next.ServeHTTP(w, r)
					return
				}
				if r.Method == http.MethodPost && r.Body != nil {
					body, err := io.ReadAll(io.LimitReader(r.Body, guardMaxRequestBytes+1))
					if err != nil {
						swaputil.SendResponse(w, r, http.StatusBadRequest, "reading request body: "+err.Error())
						return
					}
					r.Body = struct {
						io.Reader
						io.Closer
					}{io.MultiReader(bytes.NewReader(body), r.Body), r.Body}
					trimmed := bytes.TrimSpace(body)
					if len(body) > guardMaxRequestBytes || bytes.HasPrefix(trimmed, []byte("[")) {
						next.ServeHTTP(w, r)
						return
					}
					var rpc struct {
						JSONRPC string          `json:"jsonrpc"`
						ID      json.RawMessage `json:"id"`
						Method  string          `json:"method"`
						Params  struct {
							Name string `json:"name"`
						} `json:"params"`
					}
					if json.Unmarshal(body, &rpc) == nil && rpc.JSONRPC != "" {
						if rpc.Method != "tools/call" || !slices.Contains(g.cfg.MCPTools, rpc.Params.Name) {
							next.ServeHTTP(w, r)
							return
						}
						isMCP, rpcID = true, rpc.ID
					}
				}
			}

			rec := newBufferedResponse()
			next.ServeHTTP(rec, r)
			if isMCP {
				if rec.ok() && mcpResultEmpty(rec) {
					if err := g.check(r, searchName, next); err != nil {
						logf("upstream %s: refused MCP %s %s: %v", modelID, r.Method, r.URL.Path, err)
						writeMCPSearchError(w, rec, rpcID, "upstream "+modelID+" cannot search right now: "+err.Error())
						return
					}
				}
			} else if rec.ok() && !hasResults(rec.body.Bytes(), g.cfg.Probe.Expect) {
				if err := g.check(r, searchName, next); err != nil {
					logf("upstream %s: refused %s %s with 503: %v", modelID, r.Method, r.URL.Path, err)
					swaputil.SendResponse(w, r, http.StatusServiceUnavailable, "upstream "+modelID+" cannot do its work: "+err.Error())
					return
				}
			}
			for k, v := range rec.header {
				w.Header()[k] = v
			}
			w.WriteHeader(rec.status)
			w.Write(rec.body.Bytes())
		})
	}
}

// mcpResultEmpty reports whether a tools/call answer holds a result list with
// zero entries. An answer in any other shape, a tool error included, is not
// empty: the guard only second-guesses a clean "found nothing".
func mcpResultEmpty(rec *bufferedResponse) bool {
	results, _, errMsg := parseUpstreamResponse(rec.body.Bytes(), rec.header.Get("Content-Type"), false)
	return errMsg == "" && results != nil && *results == 0
}

// writeMCPSearchError answers a tools/call with an MCP tool error for the
// request's id, framed as SSE when the upstream answered as SSE.
func writeMCPSearchError(w http.ResponseWriter, rec *bufferedResponse, id json.RawMessage, text string) {
	if len(id) == 0 {
		id = json.RawMessage("null")
	}
	payload, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"result": map[string]any{
			"isError": true,
			"content": []map[string]string{{"type": "text", "text": text}},
		},
	})
	ct := rec.header.Get("Content-Type")
	if strings.HasPrefix(ct, "text/event-stream") {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "event: message\ndata: %s\n\n", payload)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(payload)
}
