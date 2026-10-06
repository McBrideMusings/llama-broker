# llama-broker

A fork of llama-swap that decides which workload gets one shared GPU, using llama-swap to load and unload them.

## Language

### Domain

**Tenant**:
A GPU workload the broker ranks: the llama-swap models or groups it owns, a priority, a condition, a busy probe, a drain action and a VRAM need.
_Avoid_: client, job

**Condition**:
A polled HTTP probe or command; while it is true, its tenant wants the GPU.

**Drain**:
Stopping a lower tenant safely: wait until its busy probe reads false (a failed probe counts as busy), run its drain action, then stop it, with the wait and the action together bounded by one `unloadTimeout`, or by shutdown's own timeout when that is shorter. Happens once per stop episode, the set of the tenant's models one preempt, swap, unload or shutdown stops together.

**VRAM reserve**:
VRAM the broker keeps free for GPU users it doesn't manage, such as video transcoders.

### Architecture

{Seeded on first run of `improve`. Don't seed up front.}

## Relationships

{Filled in as the model matures.}

## Flagged ambiguities

{When terms get used ambiguously and resolved, capture here.}
