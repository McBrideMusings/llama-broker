---
title: Upstream API passthrough
summary: Control which /upstream model paths are ignored instead of triggering a model swap, and log upstream requests to a file.
category: guides
tags: [upstream, proxy, api]
config_keys: [upstream, upstream.ignorePaths, upstream.requestLog]
updated: 2026-08-28
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
