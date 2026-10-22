package config

import "fmt"

// UpstreamLimitConfig bounds the /upstream/<model>/* requests one model
// handles, so a slow model cannot build an unbounded backlog.
type UpstreamLimitConfig struct {
	// MaxConcurrent is how many requests may run at once. Later requests wait
	// in a queue. 0 means no limit.
	MaxConcurrent int `yaml:"maxConcurrent,omitempty"`
	// MaxQueue is how many requests may wait for a free slot. A request that
	// arrives with the queue full is refused with 503 and a Retry-After
	// header. Requires maxConcurrent.
	MaxQueue int `yaml:"maxQueue,omitempty"`
	// Timeout is the seconds a request may take in total, queue wait included.
	// The upstream request is cancelled when it passes. 0 means no limit.
	Timeout int `yaml:"timeout,omitempty"`
}

func (c UpstreamLimitConfig) validate(model string) error {
	if c.MaxConcurrent < 0 || c.MaxQueue < 0 || c.Timeout < 0 {
		return fmt.Errorf("upstream.limits.%s: values must not be negative", model)
	}
	if c.MaxQueue > 0 && c.MaxConcurrent == 0 {
		return fmt.Errorf("upstream.limits.%s: maxQueue requires maxConcurrent", model)
	}
	return nil
}
