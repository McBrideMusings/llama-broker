---
title: Sharing one GPU between workloads with tenants
summary: Rank GPU workloads by priority so a higher one holds or refuses lower models, and drains then stops a lower one to make room.
category: guides
tags: [tenants, priority, gpu, drain, busy, condition, preempt, hold, refuse, comfyui, vram, reserve, preload, startup]
config_keys: [tenants, tenants.*.models, tenants.*.groups, tenants.*.priority, tenants.*.vram, tenants.*.condition, tenants.*.busy, tenants.*.drain, tenants.*.interval, tenants.*.onBlocked, vramReserve, unloadTimeout, hooks.on_startup.preload]
updated: 2026-10-05
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
The full ComfyUI recipe, with why each setting is there, is
`guides/upstreams/comfyui-tenant`.

A blocked preload from `hooks.on_startup.preload` is held, never refused,
even under `onBlocked: refuse`: it is sent once and has nobody to retry
after a 503, so it waits in the queue and loads when the gate opens. Its
decision line reads `action=hold` with a reason starting `preload held,
onBlocked: refuse does not apply to preloads`. Preloads go out one at a
time in list order, each waiting for the one before it to load. A held
preload, under either `onBlocked` value, stops that wait: the models listed
after it still preload, and the held model loads when the gate opens, which
can be while a later model is still loading.

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
      url: http://127.0.0.1:8188/prompt
      json: exec_info.queue_remaining
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
exits 0. A different status, or a non-zero exit, reads false. `cmd` is split
into arguments and run directly, not through a shell, so `;`, `&&`, pipes and
redirects don't work: put them in a script and name the script, or use
`cmd: sh -c "<command>"`.

A probe that fails is not a reading. A probe fails when the request or
command can't run, when the body isn't JSON, or when it takes longer than
`interval` seconds. A failed `condition` probe changes nothing: the tenant
keeps its last reading, so a slow or briefly unreachable endpoint neither
releases held requests nor claims the GPU. Until a tenant's first condition
probe finishes, nobody knows whether it wants the GPU, so loads of every
lower tenant's models are held or refused. A first probe that fails releases them,
and the tenant doesn't want the GPU until a probe reads true. A failed `busy`
probe during a drain counts as busy: the drain keeps waiting, so a job whose
endpoint stops answering is not cut off before `unloadTimeout`.

A `busy` url probe also fails when the status is outside 200-299 and isn't
`status`, such as a 502 from a proxy in front of ComfyUI. Reading that as idle
would run `drain` on a job that may still be running. A 2xx status other than
`status` still reads false. If a busy endpoint answers 404 or 503 to mean idle,
every drain waits out `unloadTimeout`; set `status` to a 2xx code and read
`json` instead. A `condition` probe treats every status as a reading, so an
endpoint that answers 503 while its service is down releases the GPU.

`drain` is an action: a url request (`method` defaults to POST, `body` is sent
as JSON) or a command.

## Drains

Every stop of a tenant's model drains the tenant first. That covers a stop to
make room for a higher tenant, a swap that evicts the model, a ttl unload, an
unload from the API, and shutdown. The drain polls `busy` every `interval`
seconds until it reads false, then runs `drain`, and the model stops. The
busy wait and `drain` together finish within the model's `unloadTimeout`,
counted from the start of the drain; `drain` gets whatever time the busy wait
left. If the tenant is still busy when `unloadTimeout` runs out, the drain
logs a warning, skips `drain` with `drain action skipped: no time left of
unloadTimeout <t>`, and the model stops anyway. A probe or action
command still running at the deadline is killed together with every process
it started.
Each failed busy probe logs its own warning drain step,
`probe="busy probe failed: <error>" reason="treating as busy, waiting (N in a row)"`,
and when `unloadTimeout` runs out while the probe is still failing the drain
logs `busy probe still failing after <unloadTimeout> (N in a row), stopping
anyway`.

A tenant drains once per stop episode: one preempt, swap, API unload or
shutdown that stops several of its models. The first of those stops runs the
drain, bounded by the largest `unloadTimeout` among them; each other one logs
`not draining again: the stop of <model> drained this tenant in the same stop
batch` and stops without probing. An API unload of every model counts as one
episode even when the models have different `unloadTimeout`s. A stop outside
the episode, such as a ttl unload, drains again; only if it starts while
another drain of the tenant is running does it wait for that drain instead.

## Keeping VRAM free for other programs

`vramReserve` keeps that many MiB of GPU memory free for programs
llama-broker does not manage, such as a video transcoder. Each tenant declares
`vram`, the MiB it uses while any of its models runs. A load is held, or
refused under `onBlocked: refuse`, while the `vram` of the tenants running
alongside it, plus its own tenant's `vram`, plus the reserve, exceeds the card
total. The total is the sum of accelerator memory that hardware detection
reports (`GET /api/hardware`). Tenants that the load's own swap evicts don't
count, and a tenant that is already running is never held by the reserve.

```yaml
vramReserve: 4096            # MiB left for the transcoder
tenants:
  comfy: {priority: 10, models: [comfy], vram: 12000}
  chat:  {priority: 1, groups: [llms], vram: 9000}
```

On a 24576 MiB card, comfy (12000) and chat (9000) plus the 4096 reserve come
to 25096 MiB, so whichever loads second waits until the first stops. With a
reserve set, every model must belong to a tenant that sets `vram`, or the
config fails to load: a load nobody declared could use up the reserve without
the broker knowing. The reserve hold writes the raw numbers into the
decision line:

```text
tenants: decision time=2026-10-05T14:02:11Z tenant=chat model=qwen action=hold probe="vram total=24576MiB reserve=4096MiB running=[comfy:12000] need=chat:9000" reason="model qwen (tenant chat) is blocked: its vram 9000 MiB plus 12000 MiB of running tenants plus vramReserve 4096 MiB is 25096 MiB, over the 24576 MiB total"
```

## What goes wrong

- **The higher tenant loads next to the lower one.** The condition is polled,
  so a higher tenant wants the GPU only after a poll has read it true. A
  request between the workload starting and the next poll sees no conflict.
  Lower `interval`, or make the condition turn true before the workload
  starts.
- **Lower models wait right after startup.** Until a higher tenant's first
  condition probe finishes, its lower tenants' requests are held, and under
  `onBlocked: refuse` get a 503 of type `tenant_condition_pending`. The
  decision line's probe reads `condition of <name>: not read yet`. A slow
  probe holds them for up to `interval` seconds.
- **A model outside every tenant still loads.** Tenants never hold, refuse or
  drain models that no tenant owns. Give each GPU model a tenant.
- **Lower tenants stay blocked after the higher workload ended.** The
  condition endpoint stopped answering while it read true, and a failed
  probe keeps that reading. The first failure in a row logs a warning:
  `tenants: <name> (priority N) condition probe failed (1 in a row): <error>;
  keeping wants GPU: true from the reading at <time>`. `GET /api/tenants` shows
  the count under `condition.errors`. Bring the endpoint back, or have it
  answer false while the workload is down. A probe slower than `interval`
  fails every poll; raise `interval`.
- **A tenant without a `condition` never preempts anything.** It can only be
  held, refused or drained by higher tenants.
- **Requests hang under `hold`.** They wait for as long as the higher tenant
  wants the GPU. Use `onBlocked: refuse` when clients would rather retry.
- **A render is cut off.** The drain waits at most `unloadTimeout`. Raise the
  model's `unloadTimeout` to cover your longest job plus the time `drain`
  takes; a job that uses up the whole `unloadTimeout` leaves none for `drain`.
- **Every stop takes the full `unloadTimeout`.** The busy endpoint is
  unreachable, answers something other than JSON, or takes longer than
  `interval`, so every probe fails and counts as busy. The drain logs `busy
  probe failed: <error>` warnings and `GET /api/tenants` shows the count
  under `busy.errors`. Fix the URL or port, or raise `interval` past the
  endpoint's response time.
- **The reserve holds nothing.** When hardware detection reports no
  accelerator memory, the reserve is not enforced: startup logs `tenants:
  vramReserve=... not enforced` and `GET /api/tenants` shows `"totalMiB":
  null`. On unified-memory machines (Apple silicon, GB10) the total is system
  memory.
- **A model waits forever under the reserve.** A tenant whose `vram` plus the
  reserve exceeds the total can never load; startup logs `tenants: <name> can
  never load`. Lower one of the two.
- **Something happened and you want to know why.** Read the decision log
  lines or `GET /api/tenants`, below.

## Watching decisions and state

Every hold, refuse, drain step, stop and load of a tenant's model writes one
info line with the time, tenant, model, action, the probe reading it rests on
(raw value included) and the reason:

```text
tenants: decision time=2026-10-04T20:15:02.1Z tenant=chat model=qwen action=hold probe="condition of game-streaming: true (HTTP 200, active=true)" reason="model qwen (tenant chat, priority 1) is blocked: tenant game-streaming (priority 100) wants the GPU"
tenants: decision time=2026-10-04T20:15:03.4Z tenant=comfy model=comfy action=drain probe="busy true (HTTP 200, exec_info.queue_remaining=1)" reason="busy, waiting"
```

Condition changes log as `tenants: <name> (priority N) condition true (...)`.
The same decisions go out on `GET /api/events` as `tenantDecision` messages
whose data is `{time, tenant, model, action, probe, reason}`.

`GET /api/tenants` returns the VRAM accounting and each tenant, highest
priority first:

```json
{"vram": {"totalMiB": 24576, "reserveMiB": 4096, "enforced": true,
  "running": [{"tenant": "comfy", "vramMiB": 12000}], "usedMiB": 12000},
 "tenants": [{"name": "comfy", "priority": 10, "vramMiB": 12000, "onBlocked": "hold",
  "models": ["comfy"], "wantsGPU": false, "condition": null,
  "busy": {"result": false, "raw": "HTTP 200, exec_info.queue_remaining=0", "probedAt": "2026-10-04T20:15:09Z"},
  "draining": false, "drains": 3, "stopping": false,
  "loaded": [{"model": "comfy", "state": "ready"}], "held": 0}]}
```

`condition` and `busy` are `null` when not configured, and their `result` is
`null` before the first reading. A failed probe leaves `result` as it was and
adds `errors` (failures since the last reading, left out once one succeeds),
`lastError` and `lastErrorAt`. For `condition`, the decision line's probe also
ends with `; failed probes since: N, latest: <error>`. A tenant whose
condition endpoint has timed out twice since it last read true:

```json
"condition": {"result": true, "raw": "HTTP 200, active=true", "probedAt": "2026-10-04T20:15:02Z",
  "errors": 2, "lastError": "Get \"http://127.0.0.1:9000/session\": context deadline exceeded",
  "lastErrorAt": "2026-10-04T20:15:12Z"}
```

The busy probe only runs during a drain, so `busy` is the latest drain's
reading. `drains` counts the drains run since startup, one per stop
episode. `held` counts requests waiting at the
tenant gate, not requests queued behind a swap.
