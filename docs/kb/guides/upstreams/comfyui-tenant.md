---
title: ComfyUI as a tenant that finishes its renders before giving up the GPU
summary: Make ComfyUI a tenant whose busy probe reads its queue and whose drain calls /free, so a swap waits for renders instead of killing them.
category: guides
tags: [comfyui, tenants, drain, busy, queue, free, vram, render, swap, preempt]
config_keys: [tenants.*.models, tenants.*.priority, tenants.*.condition, tenants.*.busy, tenants.*.drain, tenants.*.interval, models.*.unloadTimeout, models.*.proxy, models.*.cmdStop]
updated: 2026-10-05
---

# ComfyUI as a tenant that finishes its renders before giving up the GPU

ComfyUI's `POST /prompt` returns as soon as the job is queued, and the
render runs on afterwards. To llama-swap the model looks idle, so a swap,
a ttl unload or a higher-priority workload can stop the container
mid-render. Making ComfyUI a tenant fixes that: every stop first waits for
its queue to empty, then asks ComfyUI to free its VRAM, then stops it.
Tenants in general are covered in `guides/routing/tenants`.

```yaml
models:
  comfyui_auto:
    # A fixed host port, so the busy probe and drain below can reach it
    proxy: http://127.0.0.1:8188
    checkEndpoint: /
    cmdStop: docker stop comfyui-auto
    cmd: >
      docker run --rm
      --name comfyui-auto --gpus all -p 8188:8188
      -v /path/to/comfyui/models:/root/ComfyUI/models
      yanwk/comfyui-boot:cu130-slim-v2
    unloadTimeout: 900   # the longest render a stop will wait for, in seconds
    ttl: 600
    unlisted: true

tenants:
  game-streaming:            # owns no models; it only claims the GPU
    priority: 100
    condition:
      url: http://127.0.0.1:9000/session
      json: active
    interval: 5
  comfy:
    priority: 10
    models: [comfyui_auto]
    interval: 2
    busy:                    # queued plus running prompts
      url: http://127.0.0.1:8188/prompt
      json: exec_info.queue_remaining
    drain:
      url: http://127.0.0.1:8188/free
      body: '{"unload_models":true,"free_memory":true}'
```

## What each part does

- **`busy`** reads `GET /prompt`, which answers
  `{"exec_info": {"queue_remaining": N}}`. ComfyUI counts both the running
  prompt and the pending ones in `N`, so the tenant reads busy until the last
  queued job has finished. `GET /queue` lists the same jobs as
  `queue_running` and `queue_pending`, and is the place to look at what is
  running; a probe on `queue_running` alone reads idle in the moment between
  two queued jobs, and the stop then kills the next one.
- **`drain`** posts to `/free` once the queue is empty. ComfyUI unloads its
  models and releases cached VRAM, so the memory is back before the container
  exits.
- **`unloadTimeout`** bounds the wait. When a render outlasts it, the drain
  logs `still busy after ..., stopping anyway` and the container stops. A
  busy probe that fails (ComfyUI too slow to answer within `interval`, or not
  reachable) counts as busy, so a render whose `/prompt` stalls still gets
  until `unloadTimeout` before `/free` runs.
- **`proxy` and `-p 8188:8188`** pin the port. Probe and action URLs are
  fixed strings; they cannot follow a `${PORT}` that changes on every start.

## A drain, step by step

When `game-streaming`'s condition reads true while a render runs:

1. A new `POST /prompt` (from the browser at `/comfyui/`, or through
   `/upstream/comfyui_auto/`) is held at the tenant gate: `action=hold`.
2. The comfy tenant is stopped to make room: `action=stop`, then
   `action=drain` with `reason="draining before stopping comfyui_auto: preempted ..."`.
3. Every `interval` seconds the busy probe logs
   `probe="busy true (HTTP 200, exec_info.queue_remaining=1)" reason="busy, waiting"`,
   until it reads `queue_remaining=0` and logs `reason="idle"`.
4. `/free` runs: `reason="drain action done: HTTP 200"`, then
   `tenants: comfy stopped [comfyui_auto]`.
5. When the condition reads false again, the held prompt continues: it starts
   ComfyUI and queues the job.

`GET /api/tenants` shows the same: `draining: true` during step 3, the busy
probe's raw reading, and `held: 1` for the waiting prompt.

## What goes wrong

- **The drain never waits.** The probe reads false at once, for example
  because the `json` path matches nothing in ComfyUI's answer. After a drain
  has run, check the `busy` reading's `raw` in `GET /api/tenants` (it is `null`
  until then). A tenant with neither `busy` nor `drain` never waits at all;
  its stops go straight through.
- **Every stop waits the full `unloadTimeout`.** The probe host, port or path
  is wrong, so every probe fails (connection refused, or a non-2xx status such
  as 404 or 502) and counts as busy. The drain logs
  `busy probe failed: ... connection refused` warnings, and `busy.errors` and
  `busy.lastError` in `GET /api/tenants` show the failures.
- **A render is cut off.** It ran longer than `unloadTimeout`. Raise it on
  `comfyui_auto`.
- **Jobs queued straight to ComfyUI are not held.** Only requests that pass
  through llama-swap reach the tenant gate. A client talking to port 8188
  directly still queues work, and the drain then waits for it.
- **A held prompt times out in the browser.** It waits as long as the higher
  tenant wants the GPU. Give `comfy` `onBlocked: refuse` to answer 503 at once
  instead.
- **The websocket and the static assets of an open tab** do not reload
  ComfyUI after the drain; `guides/upstreams/comfyui` explains why.

## Related

- `guides/routing/tenants` — priorities, conditions, hold and refuse
- `guides/upstreams/comfyui` — the `/comfyui/` endpoint and `comfyui_auto`
- `guides/model-runtime/ttl-and-unloading` — `unloadTimeout` and `cmdStop`
