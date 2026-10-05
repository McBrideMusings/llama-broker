package tenants

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestTenants_PreloadWaitsForAdmittedSend(t *testing.T) {
	sent := false
	Preload(context.Background(), func(ctx context.Context) {
		if reason, _ := HoldPreload(ctx, nil, false); reason != nil {
			t.Errorf("HoldPreload with no reason returned %v", reason)
		}
		time.Sleep(50 * time.Millisecond)
		sent = true
	})
	if !sent {
		t.Fatal("Preload returned before an admitted send finished")
	}
}

func TestTenants_HoldPreload(t *testing.T) {
	blocked := errors.New("tenant hi wants the GPU")
	tests := []struct {
		name       string
		ctx        context.Context
		reason     error
		refuse     bool
		wantRefuse bool
		wantWrap   bool
		wantHeld   bool
	}{
		{name: "nil ctx", ctx: nil, reason: blocked, refuse: true, wantRefuse: true},
		{name: "unmarked ctx", ctx: context.Background(), reason: blocked, refuse: true, wantRefuse: true},
		{name: "nil reason", ctx: WithPreload(context.Background()), reason: nil, refuse: false},
		{name: "nil reason keeps refuse", ctx: WithPreload(context.Background()), reason: nil, refuse: true, wantRefuse: true},
		{name: "marked, hold passes through", ctx: WithPreload(context.Background()), reason: blocked, refuse: false, wantHeld: true},
		{name: "marked, refuse becomes hold", ctx: WithPreload(context.Background()), reason: blocked, refuse: true, wantWrap: true, wantHeld: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reason, refuse := HoldPreload(tt.ctx, tt.reason, tt.refuse)
			if refuse != tt.wantRefuse {
				t.Errorf("refuse = %v, want %v", refuse, tt.wantRefuse)
			}
			if tt.wantWrap {
				if !errors.Is(reason, blocked) {
					t.Errorf("reason %v does not wrap %v", reason, blocked)
				}
				if !strings.HasPrefix(reason.Error(), "preload held, onBlocked: refuse does not apply to preloads") {
					t.Errorf("reason = %q, want the preload-held prefix", reason)
				}
			} else if reason != tt.reason {
				t.Errorf("reason = %v, want %v unchanged", reason, tt.reason)
			}
			if m := preloadOf(tt.ctx); m != nil {
				select {
				case <-m.held:
					if !tt.wantHeld {
						t.Error("held closed, want open")
					}
				default:
					if tt.wantHeld {
						t.Error("held open, want closed")
					}
				}
			}
		})
	}
}

func TestTenants_PreloadReturnsWhenHeld(t *testing.T) {
	for _, refuse := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		exited := make(chan struct{})
		var gotRefuse bool
		returned := make(chan struct{})
		go func() {
			defer close(returned)
			Preload(ctx, func(ctx context.Context) {
				defer close(exited)
				_, gotRefuse = HoldPreload(ctx, errors.New("tenant hi wants the GPU"), refuse)
				// A second block of the same queued preload must not panic.
				HoldPreload(ctx, errors.New("tenant hi wants the GPU"), refuse)
				<-ctx.Done()
			})
		}()
		select {
		case <-returned:
		case <-time.After(time.Second):
			t.Fatalf("refuse=%v: Preload still waiting on a held send", refuse)
		}
		cancel()
		select {
		case <-exited:
		case <-time.After(time.Second):
			t.Fatalf("refuse=%v: held send did not exit after ctx was cancelled", refuse)
		}
		if gotRefuse {
			t.Fatalf("refuse=%v: HoldPreload refused a preload", refuse)
		}
	}
}
