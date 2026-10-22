package server

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/mostlygeek/llama-swap/internal/chain"
	"github.com/mostlygeek/llama-swap/internal/config"
	"github.com/mostlygeek/llama-swap/internal/swaputil"
)

// upstreamRetryAfterSeconds is the Retry-After value sent with a 503 for a
// full queue.
const upstreamRetryAfterSeconds = 5

var errUpstreamQueueFull = errors.New("upstream queue full")

// upstreamLimiter admits requests for one model: maxConcurrent run, up to
// maxQueue wait, the rest are refused. A waiting request whose client leaves
// or whose timeout passes drops out without reaching the upstream.
type upstreamLimiter struct {
	slots    chan struct{}
	maxQueue int64
	queued   atomic.Int64
}

func newUpstreamLimiter(l config.UpstreamLimitConfig) *upstreamLimiter {
	if l.MaxConcurrent == 0 {
		return nil
	}
	return &upstreamLimiter{slots: make(chan struct{}, l.MaxConcurrent), maxQueue: int64(l.MaxQueue)}
}

// acquire returns a release func, errUpstreamQueueFull, or the context error.
func (l *upstreamLimiter) acquire(ctx context.Context) (func(), error) {
	release := func() { <-l.slots }
	select {
	case l.slots <- struct{}{}:
		return release, nil
	default:
	}
	if l.queued.Add(1) > l.maxQueue {
		l.queued.Add(-1)
		return nil, errUpstreamQueueFull
	}
	defer l.queued.Add(-1)
	select {
	case l.slots <- struct{}{}:
		return release, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type upstreamLimitEntry struct {
	timeout time.Duration
	limiter *upstreamLimiter
}

// upstreamTimeoutWriter turns the proxy's 502, or the 500 a model load
// cancelled mid-start reports, into a 504 when the request
// ran out of its upstream.limits timeout, and forwards the status-marker
// calls the other middleware use.
type upstreamTimeoutWriter struct {
	http.ResponseWriter
	r           *http.Request
	wroteHeader bool
}

func (w *upstreamTimeoutWriter) WriteHeader(code int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	if (code == http.StatusBadGateway || code == http.StatusInternalServerError) && errors.Is(w.r.Context().Err(), context.DeadlineExceeded) {
		code = http.StatusGatewayTimeout
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *upstreamTimeoutWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}

func (w *upstreamTimeoutWriter) Flush() {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *upstreamTimeoutWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *upstreamTimeoutWriter) MarkStatus(code int) {
	if m, ok := w.ResponseWriter.(swaputil.StatusMarker); ok {
		m.MarkStatus(code)
	}
}

func (w *upstreamTimeoutWriter) WroteHeader() bool { return w.wroteHeader }

// CreateUpstreamLimitsMiddleware applies upstream.limits to
// /upstream/<model>/* requests: a per-request timeout that cancels the
// upstream call, a per-model concurrency cap with a bounded wait queue, and a
// 503 with Retry-After once the queue is full.
func CreateUpstreamLimitsMiddleware(cfg config.Config, logf func(format string, args ...any)) chain.Middleware {
	entries := map[string]*upstreamLimitEntry{}
	for name, l := range cfg.Upstream.Limits {
		entries[name] = &upstreamLimitEntry{timeout: time.Duration(l.Timeout) * time.Second, limiter: newUpstreamLimiter(l)}
	}

	return func(next http.Handler) http.Handler {
		if len(entries) == 0 {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			searchName, modelID, _, found := swaputil.FindModelInPath(cfg, strings.TrimPrefix(r.URL.Path, "/upstream"))
			if !found {
				next.ServeHTTP(w, r)
				return
			}
			e, ok := entries[searchName]
			if !ok {
				e, ok = entries[modelID]
			}
			if !ok {
				next.ServeHTTP(w, r)
				return
			}

			if e.timeout > 0 {
				ctx, cancel := context.WithTimeout(r.Context(), e.timeout)
				defer cancel()
				r = r.WithContext(ctx)
			}
			if e.limiter != nil {
				release, err := e.limiter.acquire(r.Context())
				switch {
				case errors.Is(err, errUpstreamQueueFull):
					logf("upstream %s: queue full, refused %s %s", modelID, r.Method, r.URL.Path)
					w.Header().Set("Retry-After", strconv.Itoa(upstreamRetryAfterSeconds))
					swaputil.SendResponse(w, r, http.StatusServiceUnavailable, "upstream queue for "+modelID+" is full; retry later")
					return
				case errors.Is(err, context.DeadlineExceeded):
					logf("upstream %s: timed out after %s waiting in queue: %s %s", modelID, e.timeout, r.Method, r.URL.Path)
					swaputil.SendResponse(w, r, http.StatusGatewayTimeout, "upstream request for "+modelID+" timed out in queue")
					return
				case err != nil:
					swaputil.MarkClientClosed(w, r)
					return
				}
				defer release()
			}

			next.ServeHTTP(&upstreamTimeoutWriter{ResponseWriter: w, r: r}, r)
			if errors.Is(r.Context().Err(), context.DeadlineExceeded) {
				logf("upstream %s: request timed out after %s: %s %s", modelID, e.timeout, r.Method, r.URL.Path)
			}
		})
	}
}
