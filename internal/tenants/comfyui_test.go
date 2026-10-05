package tenants

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mostlygeek/llama-swap/internal/config"
	"github.com/mostlygeek/llama-swap/internal/logmon"
)

const comfyGuide = "../../docs/kb/guides/upstreams/comfyui-tenant.md"

// comfyRecipe loads the first yaml block of the ComfyUI tenant guide, with
// ComfyUI's address replaced by comfyURL.
func comfyRecipe(t *testing.T, comfyURL string) config.Config {
	t.Helper()
	doc, err := os.ReadFile(comfyGuide)
	if err != nil {
		t.Fatal(err)
	}
	_, block, ok := strings.Cut(string(doc), "```yaml\n")
	if !ok {
		t.Fatalf("%s has no yaml block", comfyGuide)
	}
	block, _, _ = strings.Cut(block, "```")
	cfg, err := config.LoadConfigFromReader(strings.NewReader(strings.ReplaceAll(block, "http://127.0.0.1:8188", comfyURL)))
	if err != nil {
		t.Fatalf("the guide's config does not load: %v", err)
	}
	return cfg
}

// comfyStub serves ComfyUI's GET /prompt and POST /free. Each
// GET /prompt reads the next queue state from states; the last one repeats.
type comfyStub struct {
	mu     sync.Mutex
	states [][2]int // {running, pending}
	reads  int
	events []string
}

func (s *comfyStub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.states[min(s.reads, len(s.states)-1)]
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/prompt":
		s.reads++
		s.events = append(s.events, "remaining "+strconv.Itoa(state[0]+state[1]))
		io.WriteString(w, `{"exec_info": {"queue_remaining": `+strconv.Itoa(state[0]+state[1])+`}}`)
	case r.Method == http.MethodPost && r.URL.Path == "/free":
		body, _ := io.ReadAll(r.Body)
		s.events = append(s.events, "POST /free "+string(body))
	default:
		http.NotFound(w, r)
	}
}

func TestTenants_ComfyUIRecipeWaitsForWholeQueueThenFrees(t *testing.T) {
	// A render running with one pending, the moment between the two jobs
	// (nothing running, one pending), the second render, then empty.
	stub := &comfyStub{states: [][2]int{{1, 1}, {0, 1}, {1, 0}, {0, 0}}}
	srv := httptest.NewServer(stub)
	t.Cleanup(srv.Close)

	cfg := comfyRecipe(t, srv.URL)
	var logBuf syncBuffer
	m := New(cfg.Tenants, logmon.NewWriter(&logBuf))
	for _, tn := range m.tenants {
		tn.interval = 20 * time.Millisecond
	}
	hook := m.StopHook("comfyui_auto")
	if hook == nil {
		t.Fatal("the recipe gives comfyui_auto no stop hook")
	}
	// A short bound instead of the recipe's 900s, so a regression fails fast.
	hook(5 * time.Second)

	want := []string{"remaining 2", "remaining 1", "remaining 1", "remaining 0",
		`POST /free {"unload_models":true,"free_memory":true}`}
	stub.mu.Lock()
	got := append([]string(nil), stub.events...)
	stub.mu.Unlock()
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("ComfyUI saw %q\nwant          %q", got, want)
	}

	m.mu.Lock()
	busy := m.byModel["comfyui_auto"].busy
	m.mu.Unlock()
	if busy.ok || busy.raw != "HTTP 200, exec_info.queue_remaining=0" {
		t.Fatalf("last busy reading %s, want false (HTTP 200, exec_info.queue_remaining=0)", busy)
	}
	if !strings.Contains(logBuf.String(), `reason="drain action done: HTTP 200"`) {
		t.Fatalf("log has no drain action line ending at the status:\n%s", logBuf.String())
	}
}

// syncBuffer is a bytes.Buffer safe for the logger and the test to share.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestTenants_ComfyUIRecipeHoldsPromptWhileHigherTenantWantsGPU(t *testing.T) {
	cfg := comfyRecipe(t, "http://127.0.0.1:1")
	m := New(cfg.Tenants, logmon.NewWriter(io.Discard))
	m.mu.Lock()
	for _, tn := range m.tenants {
		tn.wants = tn.name == "game-streaming"
	}
	m.mu.Unlock()

	reason, refuse := m.Block("comfyui_auto", nil)
	if reason == nil || refuse {
		t.Fatalf("comfyui_auto Block = %v, refuse %v; want held", reason, refuse)
	}
	if !strings.Contains(reason.Error(), "tenant game-streaming (priority 100) wants the GPU") {
		t.Fatalf("reason %q does not name game-streaming", reason)
	}
}
