package tenants

import (
	"context"
	"fmt"
)

type preloadKey struct{}

// WithPreload marks ctx as a hooks.on_startup.preload request. The tenant gate
// holds such a request whatever its tenant's onBlocked says: a preload is sent
// once and has no client to retry after a 503.
func WithPreload(ctx context.Context) context.Context {
	return context.WithValue(ctx, preloadKey{}, true)
}

// IsPreload reports whether ctx was marked by WithPreload.
func IsPreload(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v, _ := ctx.Value(preloadKey{}).(bool)
	return v
}

// HoldPreload turns a refusal of a preload into a hold whose reason says why,
// and returns every other gate answer unchanged.
func HoldPreload(ctx context.Context, reason error, refuse bool) (error, bool) {
	if reason == nil || !refuse || !IsPreload(ctx) {
		return reason, refuse
	}
	return fmt.Errorf("preload held, onBlocked: refuse does not apply to preloads: %w", reason), false
}
