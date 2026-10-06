package tenants

import (
	"context"
	"fmt"
	"sync"
)

type preloadKey struct{}

// preloadMark is the value WithPreload stores. held closes the first time the
// gate blocks the preload.
type preloadMark struct {
	held chan struct{}
	once sync.Once
}

// WithPreload marks ctx as a hooks.on_startup.preload request. The tenant gate
// holds such a request whatever its tenant's onBlocked says: a preload is sent
// once and has no client to retry after a 503.
func WithPreload(ctx context.Context) context.Context {
	return context.WithValue(ctx, preloadKey{}, &preloadMark{held: make(chan struct{})})
}

func preloadOf(ctx context.Context) *preloadMark {
	if ctx == nil {
		return nil
	}
	m, _ := ctx.Value(preloadKey{}).(*preloadMark)
	return m
}

// holdPreload turns a refusal of a preload into a hold whose reason says why,
// and returns every other gate answer unchanged. Any block of a preload also
// releases its sender waiting in Preload.
func holdPreload(ctx context.Context, reason error, refuse bool) (error, bool) {
	m := preloadOf(ctx)
	if reason == nil || m == nil {
		return reason, refuse
	}
	m.once.Do(func() { close(m.held) })
	if !refuse {
		return reason, refuse
	}
	return fmt.Errorf("preload held, onBlocked: refuse does not apply to preloads: %w", reason), false
}

// Preload runs send with ctx marked by WithPreload and returns once send
// returns or the tenant gate blocks the preload, whichever comes first; a
// blocked send keeps running in the background until the gate opens. Sending a
// list of preloads in order this way keeps that order for every preload the
// gate admits, while a blocked one does not delay the models listed after it.
func Preload(ctx context.Context, send func(ctx context.Context)) {
	ctx = WithPreload(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		send(ctx)
	}()
	select {
	case <-done:
	case <-preloadOf(ctx).held:
	}
}
