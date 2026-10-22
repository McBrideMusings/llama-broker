package config

import (
	"fmt"
	"net/http"
	"regexp"
)

// DefaultUpstreamGuardCacheSeconds is how long a probe result is reused.
const DefaultUpstreamGuardCacheSeconds = 10

// UpstreamGuardProbeConfig is the request the broker sends to a model to find
// out whether it can really do its work.
type UpstreamGuardProbeConfig struct {
	// Method defaults to POST when a body is set, else GET.
	Method string `yaml:"method,omitempty"`
	// Path is relative to the model, e.g. /query.
	Path string `yaml:"path"`
	// Body is the request body, sent as application/json.
	Body string `yaml:"body,omitempty"`
	// Expect names the top-level JSON array in the probe response that must
	// hold at least one element.
	Expect string `yaml:"expect"`
	// CacheSeconds is how long a probe result is reused (default 10).
	CacheSeconds int `yaml:"cacheSeconds,omitempty"`
}

// UpstreamGuardConfig turns a silent empty 200 from a model into an error.
// A 2xx response to a request on Paths whose Probe.Expect array is empty or
// absent makes the broker run the probe; if the probe's array is empty too,
// the client gets 503 instead of the response.
type UpstreamGuardConfig struct {
	// Paths are RE2 expressions matched against the path after the model ID.
	Paths []string                 `yaml:"paths"`
	Probe UpstreamGuardProbeConfig `yaml:"probe"`

	PathRegexps []*regexp.Regexp `yaml:"-"`
}

func (g *UpstreamGuardConfig) validate(model string) error {
	if len(g.Paths) == 0 {
		return fmt.Errorf("upstream.guards.%s: paths must list at least one expression", model)
	}
	g.PathRegexps = g.PathRegexps[:0]
	for _, p := range g.Paths {
		re, err := regexp.Compile(p)
		if err != nil {
			return fmt.Errorf("upstream.guards.%s: invalid regular expression %q: %w", model, p, err)
		}
		g.PathRegexps = append(g.PathRegexps, re)
	}
	if g.Probe.Path == "" || g.Probe.Expect == "" {
		return fmt.Errorf("upstream.guards.%s: probe.path and probe.expect are required", model)
	}
	if g.Probe.CacheSeconds < 0 {
		return fmt.Errorf("upstream.guards.%s: probe.cacheSeconds must not be negative", model)
	}
	if g.Probe.CacheSeconds == 0 {
		g.Probe.CacheSeconds = DefaultUpstreamGuardCacheSeconds
	}
	if g.Probe.Method == "" {
		g.Probe.Method = http.MethodGet
		if g.Probe.Body != "" {
			g.Probe.Method = http.MethodPost
		}
	}
	return nil
}
