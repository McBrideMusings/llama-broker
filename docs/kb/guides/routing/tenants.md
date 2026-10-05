---
title: Sharing one GPU between workloads with tenants
summary: Rank GPU workloads by priority so a higher one holds or refuses lower models, and drains then stops a lower one to make room.
category: guides
tags: [tenants, priority, gpu, drain, busy, condition, preempt, hold, refuse, comfyui]
config_keys: [tenants, tenants.*.models, tenants.*.groups, tenants.*.priority, tenants.*.condition, tenants.*.busy, tenants.*.drain, tenants.*.interval, tenants.*.onBlocked, unloadTimeout]
updated: 2026-10-04
---

# Sharing one GPU between workloads with tenants

Groups and the matrix decide which models may run together. Tenants decide
which workload gets the GPU when several want it. A tenant owns llama-swap
models (directly or through routing groups) and has a `priority`. While its
`condition` reads true, the tenant wants the GPU, and three things happen:

1. A request for a model of a lower-priority tenant is held in the queue
   (`onBlocked: hold`, the default) or refused with HTTP 503
   (`onBlocked: refuse`). The 503 body names both tenants and sets
   `Retry-After` to the blocking tenant's `interval`.
2. Each running model of a lower tenant is drained, then stopped.
3. A request for the higher tenant's own models waits until those lower
   models have stopped, so it never loads next to them.

When the condition turns false, held requests continue in queue order.

```yaml
tenants:
  game-streaming:            # owns no models; it only claims the GPU
    priority: 100
    condition:
      url: http://127.0.0.1:9000/session
      json: active           # true while .active is truthy
    interval: 5
  comfy:
    priority: 10
    models: [comfy]
    busy:                    # stop waits while this reads true
      url: http://127.0.0.1:8188/queue
      json: queue_running
    drain:                   # runs once busy reads false, before the stop
      url: http://127.0.0.1:8188/free
      body: '{"unload_models":true,"free_memory":true}'
  chat:
    priority: 1
    groups: [llms]
    onBlocked: refuse
```

## Probes and actions

`condition` and `busy` are probes. The url form is true when the response
status equals `status` (default 200). When `json` is set, the value at that
dot path (`a.b`, `items.0.state`) must also be true, a non-zero number, or a
non-empty string, array or object. The cmd form is true when the command
exits 0. A probe that errors, times out or gets a different status reads false.

`drain` is an action: a url request (`method` defaults to POST, `body` is sent
as JSON) or a command.

## Drains

Every stop of a tenant's model drains the tenant first. That covers a stop to
make room for a higher tenant, a swap that evicts the model, a ttl unload, an
unload from the API, and shutdown. The drain polls `busy` every `interval`
seconds until it reads false, waiting at most the model's `unloadTimeout`.
Then it runs `drain` and the model stops. If the tenant is still busy when
`unloadTimeout` runs out, the drain logs a warning and the model stops anyway.
Stops of two models from one tenant share a single drain.

## What goes wrong

- **The higher tenant loads next to the lower one.** The condition is polled,
  so a higher tenant wants the GPU only after a poll has read it true. A
  request in the first `interval` seconds sees no conflict. Lower `interval`,
  or make the condition turn true before the workload starts.
- **A model outside every tenant still loads.** Tenants never hold, refuse or
  drain models that no tenant owns. Give each GPU model a tenant.
- **A tenant without a `condition` never preempts anything.** It can only be
  held, refused or drained by higher tenants.
- **Requests hang under `hold`.** They wait for as long as the higher tenant
  wants the GPU. Use `onBlocked: refuse` when clients would rather retry.
- **A render is cut off.** The drain waits at most `unloadTimeout`. Raise the
  model's `unloadTimeout` to cover your longest job.
- **Something happened and you want to know why.** Read the decision log
  lines or `GET /api/tenants`, below.

## Watching decisions and state

Every hold, refuse, drain step, stop and load of a tenant's model writes one
info line with the time, tenant, model, action, the probe reading it rests on
(raw value included) and the reason:

```text
tenants: decision time=2026-10-04T20:15:02.1Z tenant=chat model=qwen action=hold probe="condition of game-streaming: true (HTTP 200, active=true)" reason="model qwen (tenant chat, priority 1) is blocked: tenant game-streaming (priority 100) wants the GPU"
tenants: decision time=2026-10-04T20:15:03.4Z tenant=comfy model=comfy action=drain probe="busy true (HTTP 200, queue_running=[[\"job\"]])" reason="busy, waiting"
```

Condition changes log as `tenants: <name> (priority N) condition true (...)`.
The same decisions go out on `GET /api/events` as `tenantDecision` messages
whose data is `{time, tenant, model, action, probe, reason}`.

`GET /api/tenants` returns each tenant, highest priority first:

```json
{"tenants": [{"name": "comfy", "priority": 10, "onBlocked": "hold",
  "models": ["comfy"], "wantsGPU": false, "condition": null,
  "busy": {"result": false, "raw": "HTTP 200, queue_running=[]", "probedAt": "2026-10-04T20:15:09Z"},
  "draining": false, "stopping": false,
  "loaded": [{"model": "comfy", "state": "ready"}], "held": 0}]}
```

`condition` and `busy` are `null` when not configured, and their `result` is
`null` before the first reading. The busy probe only runs during a drain, so
`busy` is the latest drain's reading. `held` counts requests waiting at the
tenant gate, not requests queued behind a swap.
