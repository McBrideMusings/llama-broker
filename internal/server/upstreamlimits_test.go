package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mostlygeek/llama-swap/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newUpstreamLimitsTestServer(t *testing.T, limit config.UpstreamLimitConfig, handler http.HandlerFunc) *Server {
	t.Helper()
	local := newStubRouter([]string{"qmd", "other"}, "ok")
	local.serveHTTP = handler
	s := newTestServer(local, newStubRouter(nil, ""))
	s.cfg = config.Config{
		Models:   map[string]config.ModelConfig{"qmd": {}, "other": {}},
		Upstream: config.UpstreamConfig{Limits: map[string]config.UpstreamLimitConfig{"qmd": limit}},
	}
	s.routes()
	return s
}

func TestUpstreamLimits_QueueFullIs503WithRetryAfter(t *testing.T) {
	release := make(chan struct{})
	var ran atomic.Int32
	s := newUpstreamLimitsTestServer(t, config.UpstreamLimitConfig{MaxConcurrent: 1, MaxQueue: 1}, func(w http.ResponseWriter, r *http.Request) {
		ran.Add(1)
		<-release
	})
	var wg sync.WaitGroup
	start := func() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/upstream/qmd/query", nil))
		}()
	}
	start()
	require.Eventually(t, func() bool { return ran.Load() == 1 }, time.Second, time.Millisecond)
	start()
	time.Sleep(50 * time.Millisecond)

	w := httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/upstream/qmd/query", nil))
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Equal(t, "5", w.Header().Get("Retry-After"))

	close(release)
	wg.Wait()
	assert.Equal(t, int32(2), ran.Load())
}

func TestUpstreamLimits_TimeoutCancelsUpstream(t *testing.T) {
	cancelled := make(chan struct{})
	s := newUpstreamLimitsTestServer(t, config.UpstreamLimitConfig{Timeout: 1}, func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
		close(cancelled)
		w.WriteHeader(http.StatusInternalServerError)
	})
	w := httptest.NewRecorder()
	start := time.Now()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/upstream/qmd/query", nil))
	assert.Equal(t, http.StatusGatewayTimeout, w.Code)
	assert.Less(t, time.Since(start), 3*time.Second)
	select {
	case <-cancelled:
	default:
		t.Fatal("upstream request context was not cancelled")
	}
}

// 20 clients that give up while one slow request holds the model must not
// delay the 21st: the abandoned ones leave the queue without reaching the
// upstream.
func TestUpstreamLimits_AbandonedRequestsDoNotDelayNext(t *testing.T) {
	block := make(chan struct{})
	var reached atomic.Int32
	s := newUpstreamLimitsTestServer(t, config.UpstreamLimitConfig{MaxConcurrent: 1, MaxQueue: 30, Timeout: 1}, func(w http.ResponseWriter, r *http.Request) {
		n := reached.Add(1)
		if n == 1 {
			select {
			case <-block:
			case <-r.Context().Done():
			}
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	go s.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/upstream/qmd/query", nil))
	require.Eventually(t, func() bool { return reached.Load() == 1 }, time.Second, time.Millisecond)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		req := httptest.NewRequest(http.MethodPost, "/upstream/qmd/query", nil).WithContext(ctx)
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.ServeHTTP(httptest.NewRecorder(), req)
		}()
		time.AfterFunc(20*time.Millisecond, cancel)
	}
	wg.Wait()
	assert.Equal(t, int32(1), reached.Load(), "abandoned requests never reached the upstream")

	start := time.Now()
	w := httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/upstream/qmd/query", nil))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Less(t, time.Since(start), 2*time.Second, "21st request waits at most the timeout")
	close(block)
}
