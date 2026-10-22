package server

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mostlygeek/llama-swap/internal/chain"
	"github.com/mostlygeek/llama-swap/internal/config"
	"github.com/mostlygeek/llama-swap/internal/swaputil"
)

const (
	upstreamLogMaxRequestBody  = 1 << 20
	upstreamLogMaxResponseBody = 4 << 20
	upstreamLogMaxQueryBytes   = 8 << 10
	upstreamLogMaxErrorBytes   = 200
	upstreamLogTopCount        = 5
	upstreamLogDefaultLimit    = 200
	upstreamLogMaxLimit        = 5000
)

// upstreamLogEntry is one JSON line of the upstream request log.
type upstreamLogEntry struct {
	TS     string          `json:"ts"`
	Client string          `json:"client"`
	Model  string          `json:"model"`
	Method string          `json:"method"`
	Path   string          `json:"path"`
	Tool   string          `json:"tool"`
	Query  json.RawMessage `json:"query"`
	Status int             `json:"status"`
	MS     int64           `json:"ms"`
	// Results is the number of results the upstream returned, null when the
	// response carried no recognisable result list.
	Results *int     `json:"results"`
	Top     []string `json:"top"`
	Error   *string  `json:"error"`
}

// upstreamRequestLog appends upstreamLogEntry lines to a size-rotated file.
type upstreamRequestLog struct {
	path       string
	models     []string
	maxBytes   int64
	maxBackups int

	mu     sync.Mutex
	file   *os.File
	size   int64
	closed bool
}

func newUpstreamRequestLog(cfg config.UpstreamRequestLogConfig) (*upstreamRequestLog, error) {
	if !cfg.Enabled() {
		return nil, nil
	}
	l := &upstreamRequestLog{
		path:       cfg.Path,
		models:     cfg.Models,
		maxBytes:   int64(cfg.MaxSizeMB) << 20,
		maxBackups: cfg.MaxBackups,
	}
	if err := os.MkdirAll(filepath.Dir(l.path), 0o700); err != nil {
		return nil, fmt.Errorf("upstream.requestLog: %w", err)
	}
	if err := l.open(); err != nil {
		return nil, fmt.Errorf("upstream.requestLog: %w", err)
	}
	return l, nil
}

func (l *upstreamRequestLog) open() error {
	f, err := os.OpenFile(l.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	l.file, l.size = f, st.Size()
	return nil
}

// Close releases the file. Safe on a nil log.
func (l *upstreamRequestLog) Close() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = true
	if l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	return err
}

func (l *upstreamRequestLog) logsModel(name, id string) bool {
	return slices.Contains(l.models, name) || slices.Contains(l.models, id)
}

// rotate shifts path -> path.1 -> ... -> path.N and drops the oldest. It always
// tries to reopen path, so a failed shift never leaves the log without a file.
// Callers hold l.mu.
func (l *upstreamRequestLog) rotate() error {
	closeErr := l.file.Close()
	l.file = nil
	os.Remove(fmt.Sprintf("%s.%d", l.path, l.maxBackups))
	for i := l.maxBackups - 1; i >= 1; i-- {
		os.Rename(fmt.Sprintf("%s.%d", l.path, i), fmt.Sprintf("%s.%d", l.path, i+1))
	}
	renameErr := os.Rename(l.path, l.path+".1")
	if err := l.open(); err != nil {
		return err
	}
	if renameErr != nil {
		return renameErr
	}
	return closeErr
}

func (l *upstreamRequestLog) write(e upstreamLogEntry) error {
	line, err := json.Marshal(e)
	if err != nil {
		return err
	}
	line = append(line, '\n')

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return fmt.Errorf("upstream request log is closed")
	}
	if l.file == nil {
		if err := l.open(); err != nil {
			return err
		}
	}
	if l.size > 0 && l.size+int64(len(line)) > l.maxBytes {
		if err := l.rotate(); err != nil {
			return err
		}
	}
	n, err := l.file.Write(line)
	l.size += int64(n)
	return err
}

// read returns entries with ts >= since for model ("" = any), oldest first,
// keeping only the newest limit.
func (l *upstreamRequestLog) read(since time.Time, model string, limit int) ([]upstreamLogEntry, error) {
	files := make([]string, 0, l.maxBackups+1)
	for i := l.maxBackups; i >= 1; i-- {
		files = append(files, fmt.Sprintf("%s.%d", l.path, i))
	}
	files = append(files, l.path)

	out := []upstreamLogEntry{}
	for _, name := range files {
		f, err := os.Open(name)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 64<<10), 4<<20)
		for sc.Scan() {
			var e upstreamLogEntry
			if json.Unmarshal(sc.Bytes(), &e) != nil {
				continue
			}
			if model != "" && e.Model != model {
				continue
			}
			if !since.IsZero() {
				ts, err := time.Parse(time.RFC3339Nano, e.TS)
				if err != nil || ts.Before(since) {
					continue
				}
			}
			out = append(out, e)
			if len(out) >= 2*limit {
				out = append(out[:0], out[len(out)-limit:]...)
			}
		}
		scanErr := sc.Err()
		f.Close()
		if scanErr != nil {
			return nil, scanErr
		}
	}
	if len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out, nil
}

// handleAPIUpstreamRequests serves GET /api/upstream/requests?since=<iso>&model=<id>&limit=<n>.
func (s *Server) handleAPIUpstreamRequests(w http.ResponseWriter, r *http.Request) {
	if s.upstreamLog == nil {
		swaputil.SendResponse(w, r, http.StatusNotFound, "upstream.requestLog is not configured")
		return
	}
	q := r.URL.Query()
	var since time.Time
	if v := q.Get("since"); v != "" {
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			swaputil.SendResponse(w, r, http.StatusBadRequest, "since must be an RFC 3339 time")
			return
		}
		since = t
	}
	limit := upstreamLogDefaultLimit
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			swaputil.SendResponse(w, r, http.StatusBadRequest, "limit must be a positive integer")
			return
		}
		limit = min(n, upstreamLogMaxLimit)
	}
	entries, err := s.upstreamLog.read(since, q.Get("model"), limit)
	if err != nil {
		swaputil.SendResponse(w, r, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"requests": entries})
}

// CreateUpstreamRequestLogMiddleware logs every /upstream/<model>/* request to
// a model the log is configured for, including failed and empty ones.
func CreateUpstreamRequestLogMiddleware(l *upstreamRequestLog, cfg config.Config, onError func(error)) chain.Middleware {
	return func(next http.Handler) http.Handler {
		if l == nil {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			name, id, remaining, found := swaputil.FindModelInPath(cfg, "/"+r.PathValue("upstreamPath"))
			if !found || !l.logsModel(name, id) {
				next.ServeHTTP(w, r)
				return
			}

			start := time.Now()
			method := r.Method
			// Read ahead (capped) so the query is logged even when the handler
			// rejects the request without reading its body; the full body is
			// replayed to the handler.
			var reqBody bytes.Buffer
			if r.Body != nil && r.Body != http.NoBody {
				io.Copy(&reqBody, io.LimitReader(r.Body, upstreamLogMaxRequestBody))
				r.Body = struct {
					io.Reader
					io.Closer
				}{io.MultiReader(bytes.NewReader(reqBody.Bytes()), r.Body), r.Body}
			}
			// Keep the response decompressible for parsing.
			if ae := r.Header.Get("Accept-Encoding"); ae != "" {
				r.Header.Set("Accept-Encoding", filterAcceptEncoding(ae))
			}

			rec := &upstreamLogWriter{ResponseWriter: w, status: http.StatusOK}
			// Log in a defer so a handler that aborts (the reverse proxy
			// panics with http.ErrAbortHandler when the upstream dies
			// mid-stream) is still recorded.
			defer func() {
				p := recover()
				if p != nil {
					rec.status = http.StatusBadGateway
				}
				l.logRequest(r, rec, start, method, id, remaining, &reqBody, onError, p)
				if p != nil {
					panic(p)
				}
			}()
			next.ServeHTTP(rec, r)
		})
	}
}

// logRequest builds and writes the entry for one finished or aborted request.
func (l *upstreamRequestLog) logRequest(r *http.Request, rec *upstreamLogWriter, start time.Time, method, id, remaining string, reqBody *bytes.Buffer, onError func(error), panicked any) {
	tool, query := describeUpstreamRequest(remaining, reqBody.Bytes())
	e := upstreamLogEntry{
		TS:     start.UTC().Format(time.RFC3339Nano),
		Client: upstreamClient(r),
		Model:  id,
		Method: method,
		Path:   remaining,
		Tool:   tool,
		Query:  query,
		Status: rec.status,
		MS:     time.Since(start).Milliseconds(),
	}
	body := rec.body.Bytes()
	if enc := rec.Header().Get("Content-Encoding"); enc != "" {
		if d, err := decompressBody(body, enc); err == nil {
			body = d
		} else {
			body = nil
		}
	}
	var errMsg string
	e.Results, e.Top, errMsg = parseUpstreamResponse(body, rec.Header().Get("Content-Type"), rec.truncated)
	switch {
	case errMsg != "":
	case panicked != nil:
		errMsg = fmt.Sprintf("response aborted: %v", panicked)
	case rec.status >= 400:
		errMsg = truncateString(strings.TrimSpace(string(body)), upstreamLogMaxErrorBytes)
		if errMsg == "" {
			errMsg = http.StatusText(rec.status)
		}
	case r.Context().Err() != nil:
		errMsg = "client canceled: " + r.Context().Err().Error()
	}
	if errMsg != "" {
		e.Error = &errMsg
	}
	if err := l.write(e); err != nil && onError != nil {
		onError(err)
	}
}

// upstreamClient names the caller: the X-Client-Name header when sent, else
// the source IP. No other header is recorded.
func upstreamClient(r *http.Request) string {
	if n := strings.TrimSpace(r.Header.Get("X-Client-Name")); n != "" {
		return truncateString(n, 100)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func truncateString(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return strings.ToValidUTF8(s[:n], "") + "…"
}

// describeUpstreamRequest names the tool and extracts the query from either an
// MCP JSON-RPC body (tools/call) or a plain JSON body such as POST /query.
func describeUpstreamRequest(path string, body []byte) (string, json.RawMessage) {
	last := path[strings.LastIndex(path, "/")+1:]
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		return last, json.RawMessage("null")
	}
	if !json.Valid(body) {
		return last, quoteQuery(string(body))
	}
	if len(body) > upstreamLogMaxQueryBytes {
		return last, quoteQuery(string(body))
	}

	var rpc struct {
		JSONRPC string `json:"jsonrpc"`
		Method  string `json:"method"`
		Params  struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		} `json:"params"`
	}
	if body[0] == '[' {
		return "batch", append(json.RawMessage(nil), body...)
	}
	if json.Unmarshal(body, &rpc) == nil && rpc.JSONRPC != "" {
		if rpc.Method == "tools/call" {
			args := rpc.Params.Arguments
			if len(args) == 0 {
				args = json.RawMessage("null")
			}
			return rpc.Params.Name, args
		}
		return rpc.Method, json.RawMessage("null")
	}
	return last, append(json.RawMessage(nil), body...)
}

func quoteQuery(s string) json.RawMessage {
	b, _ := json.Marshal(truncateString(s, upstreamLogMaxQueryBytes))
	return b
}

// parseUpstreamResponse extracts the result count and the first few result
// file names from a JSON or SSE response, and an error message when the
// response is a JSON-RPC error or an MCP tool error. All fields stay zero when
// the shape is not recognised.
func parseUpstreamResponse(body []byte, contentType string, truncated bool) (results *int, top []string, errMsg string) {
	if truncated || len(bytes.TrimSpace(body)) == 0 {
		return nil, nil, ""
	}
	var doc any
	if strings.HasPrefix(contentType, "text/event-stream") {
		for line := range strings.SplitSeq(string(body), "\n") {
			data, ok := strings.CutPrefix(strings.TrimRight(line, "\r"), "data:")
			if !ok {
				continue
			}
			var v any
			if json.Unmarshal([]byte(strings.TrimSpace(data)), &v) != nil {
				continue
			}
			if m, ok := v.(map[string]any); ok && (m["result"] != nil || m["error"] != nil) {
				doc = v
			}
		}
	} else if json.Unmarshal(body, &doc) != nil {
		return nil, nil, ""
	}
	m, ok := doc.(map[string]any)
	if !ok {
		return nil, nil, ""
	}

	if e, ok := m["error"].(map[string]any); ok {
		if msg, ok := e["message"].(string); ok {
			return nil, nil, truncateString(msg, upstreamLogMaxErrorBytes)
		}
	}
	root := m
	if r, ok := m["result"].(map[string]any); ok {
		root = r
		if isErr, _ := r["isError"].(bool); isErr {
			errMsg = truncateString(mcpText(r), upstreamLogMaxErrorBytes)
			if errMsg == "" {
				errMsg = "tool error"
			}
		}
	}
	list, found := findResultList(root)
	if !found {
		if txt := mcpText(root); txt != "" {
			var inner any
			if json.Unmarshal([]byte(txt), &inner) == nil {
				if im, ok := inner.(map[string]any); ok {
					list, found = findResultList(im)
				} else if arr, ok := inner.([]any); ok {
					list, found = arr, true
				}
			}
		}
	}
	if !found {
		return nil, nil, errMsg
	}
	n := len(list)
	results = &n
	top = []string{}
	for _, item := range list {
		if len(top) == upstreamLogTopCount {
			break
		}
		im, ok := item.(map[string]any)
		if !ok {
			continue
		}
		for _, k := range []string{"file", "path", "filepath", "displayPath", "docid", "id"} {
			if v, ok := im[k].(string); ok && v != "" {
				top = append(top, v)
				break
			}
		}
	}
	return results, top, errMsg
}

func findResultList(m map[string]any) ([]any, bool) {
	if arr, ok := m["results"].([]any); ok {
		return arr, true
	}
	if sc, ok := m["structuredContent"].(map[string]any); ok {
		if arr, ok := sc["results"].([]any); ok {
			return arr, true
		}
	}
	return nil, false
}

// mcpText joins the text parts of an MCP result's content array.
func mcpText(result map[string]any) string {
	content, _ := result["content"].([]any)
	var parts []string
	for _, c := range content {
		if cm, ok := c.(map[string]any); ok {
			if t, ok := cm["text"].(string); ok {
				parts = append(parts, t)
			}
		}
	}
	return strings.Join(parts, "\n")
}

// upstreamLogWriter tees the response into a capped buffer.
type upstreamLogWriter struct {
	http.ResponseWriter
	body        bytes.Buffer
	truncated   bool
	status      int
	wroteHeader bool
}

func (w *upstreamLogWriter) WriteHeader(code int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *upstreamLogWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	if room := upstreamLogMaxResponseBody - w.body.Len(); len(b) <= room {
		w.body.Write(b)
	} else {
		w.body.Write(b[:max(room, 0)])
		w.truncated = true
	}
	return w.ResponseWriter.Write(b)
}

func (w *upstreamLogWriter) MarkStatus(code int) {
	w.status = code
	if marker, ok := w.ResponseWriter.(swaputil.StatusMarker); ok {
		marker.MarkStatus(code)
	}
}

func (w *upstreamLogWriter) WroteHeader() bool { return w.wroteHeader }

func (w *upstreamLogWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		if !w.wroteHeader {
			w.WriteHeader(http.StatusOK)
		}
		f.Flush()
	}
}

func (w *upstreamLogWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hj, ok := w.ResponseWriter.(http.Hijacker); ok {
		return hj.Hijack()
	}
	return nil, nil, fmt.Errorf("underlying ResponseWriter does not support hijacking")
}
