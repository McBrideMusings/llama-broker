package tenants

import (
	"context"
	"errors"
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
