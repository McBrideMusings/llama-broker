package config

import (
	"fmt"
	"net/url"
	"sort"
	"strings"
)

const (
	TenantOnBlockedHold   = "hold"
	TenantOnBlockedRefuse = "refuse"

	defaultTenantInterval = 5
)

// TenantConfig declares one GPU workload. A tenant owns llama-swap models,
// directly or through routing groups, and is ranked against other tenants by
// Priority. While its Condition is true the tenant wants the GPU: models of
// lower-priority tenants are held or refused and running ones are drained and
// stopped.
type TenantConfig struct {
	Models   []string `yaml:"models"`
	Groups   []string `yaml:"groups"`
	Priority int      `yaml:"priority"`

	// VRAM is the GPU memory, in MiB, the tenant uses while any of its models
	// is running. With a top-level vramReserve, a load is held while the
	// needs of running tenants plus the reserve would exceed the card total.
	VRAM int `yaml:"vram"`

	// Condition is polled every Interval seconds. Without one the tenant never
	// wants the GPU on its own; it is only ever made room for others.
	Condition *TenantProbe `yaml:"condition"`
	// Busy is polled during a drain; the stop waits while it reads true.
	Busy *TenantProbe `yaml:"busy"`
	// Drain runs once the busy probe reads false, just before the stop.
	Drain *TenantAction `yaml:"drain"`

	// Interval is the poll period, in seconds, for Condition and Busy.
	Interval int `yaml:"interval"`
	// OnBlocked is what a request for one of this tenant's models gets while a
	// higher tenant wants the GPU: "hold" (wait, the default) or "refuse"
	// (HTTP 503).
	OnBlocked string `yaml:"onBlocked"`

	// Members is Models plus every member of Groups, resolved to real model
	// IDs. Filled in by LoadConfigFromReader.
	Members []string `yaml:"-"`
}

// TenantProbe is an HTTP request or a command whose result is a boolean. An
// HTTP probe is true when the response status equals Status and, when JSON is
// set, the value at that dot path of the JSON body is truthy (true, a non-zero
// number, a non-empty string, array or object). A command probe is true when
// it exits 0.
type TenantProbe struct {
	URL    string `yaml:"url"`
	Method string `yaml:"method"`
	Status int    `yaml:"status"`
	JSON   string `yaml:"json"`
	Cmd    string `yaml:"cmd"`
}

// TenantAction is an HTTP request or a command run for its effect.
type TenantAction struct {
	URL    string `yaml:"url"`
	Method string `yaml:"method"`
	Body   string `yaml:"body"`
	Cmd    string `yaml:"cmd"`
}

// validateTenants checks the tenants section, applies its defaults and resolves
// every tenant's Members. It runs after routing groups are normalized.
func validateTenants(config *Config) error {
	owner := make(map[string]string)
	names := make([]string, 0, len(config.Tenants))
	for name := range config.Tenants {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		t := config.Tenants[name]
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("tenants: tenant names cannot be empty")
		}
		if len(t.Groups) > 0 && config.Routing.Router.Use != "group" {
			return fmt.Errorf("tenants.%s.groups needs the group router; list models instead", name)
		}

		var members []string
		for _, m := range t.Models {
			real, found := config.RealModelName(m)
			if !found {
				return fmt.Errorf("tenants.%s.models references unknown model %q", name, m)
			}
			members = append(members, real)
		}
		for _, g := range t.Groups {
			group, found := config.Routing.Router.Settings.Groups[g]
			if !found {
				return fmt.Errorf("tenants.%s.groups references unknown group %q", name, g)
			}
			members = append(members, group.Members...)
		}
		seen := make(map[string]bool)
		t.Members = nil
		for _, m := range members {
			if seen[m] {
				continue
			}
			seen[m] = true
			if other, taken := owner[m]; taken {
				return fmt.Errorf("model %s belongs to tenants %s and %s; a model can have one tenant", m, other, name)
			}
			owner[m] = name
			t.Members = append(t.Members, m)
		}
		sort.Strings(t.Members)

		switch t.OnBlocked {
		case "":
			t.OnBlocked = TenantOnBlockedHold
		case TenantOnBlockedHold, TenantOnBlockedRefuse:
		default:
			return fmt.Errorf("tenants.%s.onBlocked must be hold or refuse, got %q", name, t.OnBlocked)
		}

		if t.VRAM < 0 {
			return fmt.Errorf("tenants.%s.vram must be >= 0", name)
		}

		if t.Interval < 0 {
			return fmt.Errorf("tenants.%s.interval must be >= 0", name)
		}
		if t.Interval == 0 {
			t.Interval = defaultTenantInterval
		}

		if err := t.Condition.validate(); err != nil {
			return fmt.Errorf("tenants.%s.condition: %w", name, err)
		}
		if err := t.Busy.validate(); err != nil {
			return fmt.Errorf("tenants.%s.busy: %w", name, err)
		}
		if err := t.Drain.validate(); err != nil {
			return fmt.Errorf("tenants.%s.drain: %w", name, err)
		}

		config.Tenants[name] = t
	}
	return validateVRAMReserve(config, owner)
}

// validateVRAMReserve checks that, with a reserve set, every model belongs to a
// tenant that declares its vram need: an uncounted load could eat the reserve.
func validateVRAMReserve(config *Config, owner map[string]string) error {
	if config.VRAMReserve < 0 {
		return fmt.Errorf("vramReserve must be >= 0")
	}
	if config.VRAMReserve == 0 {
		return nil
	}
	var undeclared []string
	for id := range config.Models {
		if t, ok := owner[id]; !ok || config.Tenants[t].VRAM == 0 {
			undeclared = append(undeclared, id)
		}
	}
	if len(undeclared) > 0 {
		sort.Strings(undeclared)
		return fmt.Errorf("vramReserve needs every model in a tenant with vram set; no declared need for %v", undeclared)
	}
	return nil
}

func (p *TenantProbe) validate() error {
	if p == nil {
		return nil
	}
	if err := validateTarget(p.URL, p.Cmd); err != nil {
		return err
	}
	if p.Cmd != "" && (p.JSON != "" || p.Status != 0 || p.Method != "") {
		return fmt.Errorf("status, json and method apply to url probes only")
	}
	if p.URL != "" {
		if p.Method == "" {
			p.Method = "GET"
		}
		if p.Status == 0 {
			p.Status = 200
		}
	}
	return nil
}

func (a *TenantAction) validate() error {
	if a == nil {
		return nil
	}
	if err := validateTarget(a.URL, a.Cmd); err != nil {
		return err
	}
	if a.Cmd != "" && (a.Method != "" || a.Body != "") {
		return fmt.Errorf("method and body apply to url actions only")
	}
	if a.URL != "" && a.Method == "" {
		a.Method = "POST"
	}
	return nil
}

// validateTarget checks that exactly one of rawURL and cmd is set and that the
// one set parses.
func validateTarget(rawURL, cmd string) error {
	switch {
	case rawURL != "" && cmd != "":
		return fmt.Errorf("set url or cmd, not both")
	case rawURL != "":
		u, err := url.Parse(rawURL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return fmt.Errorf("url %q must be an absolute http or https URL", rawURL)
		}
	case cmd != "":
		if _, err := SanitizeCommand(cmd); err != nil {
			return fmt.Errorf("cmd: %w", err)
		}
	default:
		return fmt.Errorf("set url or cmd")
	}
	return nil
}
