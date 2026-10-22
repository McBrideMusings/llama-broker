---
title: Upstream API passthrough
summary: Skip swap-triggering paths, log upstream requests, limit a model's queue and request time, and turn an empty answer from a broken model into a 503.
category: guides
tags: [upstream, proxy, api]
config_keys: [upstream, upstream.ignorePaths, upstream.requestLog, upstream.limits, upstream.guards]
updated: 2026-10-22
---

# Upstream API passthrough

`upstream` controls the `/upstream/<model>/<path>` passthrough endpoint. Its
`ignorePaths` expressions prevent static or UI paths from triggering a model
swap; it does not configure a fallback OpenAI service for unmatched requests.

Use RE2-compatible expressions and keep the default static-asset patterns
unless a client needs different paths. Check logs to distinguish an ignored
path from a model-routing request.

## Log requests to a model

`upstream.requestLog` appends one JSON line per `/upstream/<model>/*` request
to a file, for the models you list. Use it when the model runs in a container
whose own log disappears when the broker stops it.

```yaml
upstream:
  requestLog:
    path: /appdata/llama-broker/qmd-requests.jsonl
    models: [qmd]
    maxSizeMB: 10
    maxBackups: 3
```

Each line holds `ts`, `client` (the `X-Client-Name` header, else the source
IP), `model`, `method`, `path`, `tool`, `query` (the request's JSON
arguments), `status`, `ms`, `results`, `top` (first result files) and `error`.
Failed, timed-out and empty requests get a line too. `results` and `top` are
`null` when the response is not a recognisable `results` list.

`GET /api/upstream/requests?since=<RFC 3339>&model=<id>&limit=<n>` returns the
newest matching lines, oldest first. It answers 404 until `path` is set. Point
`path` at persistent storage: a path inside the container is deleted with it.
Setting `path` without `models` fails config loading.

## Limit a slow model

`upstream.limits` protects a model that handles one request at a time, such
as a search server on a busy GPU. Without a limit, every request a client
abandons stays queued and the backlog grows by each request's duration.

```yaml
upstream:
  limits:
    qmd:
      maxConcurrent: 1
      maxQueue: 3
      timeout: 60
```

- `maxConcurrent` requests run at once. The broker holds the rest, so a
  client that disconnects while waiting leaves without reaching the model.
- With `maxQueue` requests already waiting, the next one gets `503` with
  `Retry-After: 5`. `maxQueue` needs `maxConcurrent`.
- A request that takes longer than `timeout` seconds, queue wait included,
  is cancelled and the client gets `504`.

Set `timeout` above the model's normal worst case, or healthy slow requests
fail. Model load time counts toward it.

## Refuse an empty answer from a model that cannot work

A search server can answer `200` with `{"results":[]}` while its embedder
failed to start, for example when the chat model leaves too little VRAM. Its
`/health` still says `ok`, so clients read "nothing found". `upstream.guards`
makes the broker check before it passes such an answer on.

```yaml
upstream:
  guards:
    qmd:
      paths: ['^/query$', '^/health$']
      probe:
        path: /query
        body: '{"searches":[{"type":"vec","query":"caddy tls certificate"}],"limit":1}'
        expect: results
```

- On a guarded path, a `2xx` response whose `results` array is empty or
  missing triggers the probe. A `/health` body has no `results`, so it always
  triggers it.
- If the probe returns a non-2xx status or an empty `results`, the client gets
  `503` and the broker logs the refusal. Otherwise the original response goes
  through, so a query that truly matches nothing still answers empty.
- The probe result is reused for `cacheSeconds` (default 10). A probe that
  fails with an error status or times out is not reused.
- The broker holds the whole response of a guarded path in memory before it
  answers, so guard only JSON endpoints, never a streaming one.

Choose a probe query the knowledge base always answers, or the guard refuses
healthy empty answers. Keep room for the embedder by listing it under
`vramExternal` and setting `vramReserve` (see the tenants guide), so a load
that would not leave its memory free waits. Loading the chat model with a
smaller context in its `cmd`, or putting both models in a group or matrix so
they do not load together, also works.
