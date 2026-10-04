package process

import (
	"fmt"
	"testing"
	"time"

	"github.com/mostlygeek/llama-swap/internal/config"
)

// TestTenants_StopHookRunsBeforeStopOfLiveProcess checks the tenant drain hook:
// it runs with the model's unloadTimeout while the process is still serving,
// and not at all for a process that is already stopped.
func TestTenants_StopHookRunsBeforeStopOfLiveProcess(t *testing.T) {
	skipIfNoSimpleResponder(t)

	cmd, port := simpleResponderCmd(t, "-silent")
	p := newProcessCommand(t, config.ModelConfig{
		Cmd:                cmd,
		Proxy:              fmt.Sprintf("http://127.0.0.1:%d", port),
		CheckEndpoint:      "/health",
		HealthCheckTimeout: 10,
		UnloadTimeout:      7,
	})
	t.Cleanup(func() { p.Stop(testStopTimeout) })

	var calls []time.Duration
	var stateDuringHook ProcessState
	p.SetStopHook(func(unloadTimeout time.Duration) {
		calls = append(calls, unloadTimeout)
		stateDuringHook = p.State()
	})

	if err := p.Stop(testStopTimeout); err != nil {
		t.Fatalf("Stop of a stopped process: %v", err)
	}
	if len(calls) != 0 {
		t.Fatalf("hook ran %d times for a stopped process, want 0", len(calls))
	}

	runAsync(t, p)
	if err := p.Stop(testStopTimeout); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if len(calls) != 1 || calls[0] != 7*time.Second {
		t.Fatalf("hook calls %v, want one call with 7s", calls)
	}
	if stateDuringHook != StateReady {
		t.Fatalf("state during hook %s, want %s (hook must run before the stop)", stateDuringHook, StateReady)
	}
	if got := p.State(); got != StateStopped {
		t.Fatalf("state after Stop %s, want %s", got, StateStopped)
	}
}
