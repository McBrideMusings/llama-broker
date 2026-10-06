package config

import (
	"strings"
	"testing"
)

const tenantModels = `
models:
  chat:
    cmd: path/to/cmd --port ${PORT}
    aliases: [chat-alias]
  coder:
    cmd: path/to/cmd --port ${PORT}
  comfy:
    cmd: path/to/cmd --port ${PORT}
groups:
  llms:
    members: [chat, coder]
`

func TestTenants_ConfigDefaultsAndMembers(t *testing.T) {
	cfg, err := LoadConfigFromReader(strings.NewReader(tenantModels + `
tenants:
  gaming:
    priority: 100
    condition:
      url: http://127.0.0.1:9999/status
      json: session.active
  comfy:
    priority: 10
    models: [comfy]
    busy:
      cmd: test -f /tmp/busy
    drain:
      url: http://127.0.0.1:8188/free
      body: '{"free_memory":true}'
    onBlocked: refuse
  llm:
    priority: 1
    groups: [llms]
    models: [chat-alias]
    idleLoad: chat-alias
`))
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	gaming := cfg.Tenants["gaming"]
	if gaming.Interval != 5 || gaming.OnBlocked != "hold" {
		t.Errorf("gaming interval=%d onBlocked=%q, want 5 and hold", gaming.Interval, gaming.OnBlocked)
	}
	if gaming.Condition.Method != "GET" || gaming.Condition.Status != 200 {
		t.Errorf("gaming condition method=%q status=%d, want GET and 200", gaming.Condition.Method, gaming.Condition.Status)
	}
	if len(gaming.Members) != 0 {
		t.Errorf("gaming members=%v want none", gaming.Members)
	}

	comfy := cfg.Tenants["comfy"]
	if comfy.Drain.Method != "POST" || comfy.OnBlocked != "refuse" {
		t.Errorf("comfy drain method=%q onBlocked=%q, want POST and refuse", comfy.Drain.Method, comfy.OnBlocked)
	}

	if got := strings.Join(cfg.Tenants["llm"].Members, ","); got != "chat,coder" {
		t.Errorf("llm members=%q want chat,coder (alias resolved, group expanded, deduplicated)", got)
	}
	if got := cfg.Tenants["llm"].IdleLoad; got != "chat" {
		t.Errorf("llm idleLoad=%q want chat (alias resolved)", got)
	}
}

func TestTenants_ConfigRejectsInvalid(t *testing.T) {
	cases := map[string]struct{ tenants, want string }{
		"unknown model": {`
  a: {models: [nope]}`, `tenants.a.models references unknown model "nope"`},
		"unknown group": {`
  a: {groups: [nope]}`, `tenants.a.groups references unknown group "nope"`},
		"model in two tenants": {`
  a: {models: [chat]}
  b: {groups: [llms]}`, "model chat belongs to tenants a and b"},
		"bad onBlocked": {`
  a: {onBlocked: drop}`, "tenants.a.onBlocked must be hold or refuse"},
		"url and cmd": {`
  a: {condition: {url: "http://x/", cmd: "true"}}`, "tenants.a.condition: set url or cmd, not both"},
		"neither url nor cmd": {`
  a: {busy: {json: x}}`, "tenants.a.busy: set url or cmd"},
		"relative url": {`
  a: {drain: {url: /free}}`, "tenants.a.drain: url \"/free\" must be an absolute http or https URL"},
		"json on cmd": {`
  a: {condition: {cmd: "true", json: x}}`, "status, json and method apply to url probes only"},
		"negative interval": {`
  a: {interval: -1}`, "tenants.a.interval must be >= 0"},
		"idleLoad outside tenant": {`
  a: {models: [chat], idleLoad: coder}`, `tenants.a.idleLoad "coder" is not one of the tenant's models [chat]`},
		"idleLoad on two tenants": {`
  a: {models: [chat], idleLoad: chat}
  b: {models: [coder], idleLoad: coder}`, "tenants.b.idleLoad: tenant a already sets idleLoad; only one tenant may"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := LoadConfigFromReader(strings.NewReader(tenantModels + "tenants:" + c.tenants + "\n"))
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err=%v want containing %q", err, c.want)
			}
		})
	}
}

func TestTenants_ConfigGroupsNeedGroupRouter(t *testing.T) {
	_, err := LoadConfigFromReader(strings.NewReader(`
models:
  a:
    cmd: path/to/cmd --port ${PORT}
matrix:
  sets:
    s: "a"
tenants:
  t: {groups: [x]}
`))
	if err == nil || !strings.Contains(err.Error(), "tenants.t.groups needs the group router") {
		t.Fatalf("err=%v", err)
	}
}

func TestTenants_ConfigVRAMReserveNeedsEveryModelDeclared(t *testing.T) {
	declared := `
tenants:
  llm: {groups: [llms], vram: 9000}
  comfy: {models: [comfy], vram: 12000}
`
	cfg, err := LoadConfigFromReader(strings.NewReader(tenantModels + "vramReserve: 4096\n" + declared))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.VRAMReserve != 4096 || cfg.Tenants["comfy"].VRAM != 12000 {
		t.Errorf("vramReserve=%d comfy vram=%d, want 4096 and 12000", cfg.VRAMReserve, cfg.Tenants["comfy"].VRAM)
	}

	_, err = LoadConfigFromReader(strings.NewReader(tenantModels + `vramReserve: 4096
tenants:
  llm: {groups: [llms], vram: 9000}
  comfy: {models: [comfy]}
`))
	if err == nil || !strings.Contains(err.Error(), "no declared need for [comfy]") {
		t.Fatalf("comfy without vram: err=%v", err)
	}

	_, err = LoadConfigFromReader(strings.NewReader(tenantModels + `vramReserve: 4096
tenants:
  llm: {models: [chat], vram: 9000}
`))
	if err == nil || !strings.Contains(err.Error(), "no declared need for [coder comfy]") {
		t.Fatalf("untenanted models: err=%v", err)
	}

	for yaml, want := range map[string]string{
		"vramReserve: -1\n":                             "vramReserve must be >= 0",
		"tenants:\n  llm: {models: [chat], vram: -1}\n": "tenants.llm.vram must be >= 0",
	} {
		if _, err := LoadConfigFromReader(strings.NewReader(tenantModels + yaml)); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%q: err=%v want containing %q", yaml, err, want)
		}
	}
}
