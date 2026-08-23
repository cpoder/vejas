# Change safely: versions, time-travel, canary

The business surface is meant to be edited live — a threshold, a mapping
table, a routing rule (see [the rules view](rules-view.md)). The point of
this guide is the *safety around* that edit: you never change meaning
blind. You preview the change against **real traffic**, promote it
atomically, and can roll it back — all without a redeploy (ADR-0005,
ADR-0021).

## Preview before you promote

Shadow-replay reruns the flow's last real events through the *proposed*
change and shows you the before/after diff — nothing is published, nothing
is committed.

- Panel: edit a literal → **Apply** → the shadow strip shows what would
  change → **Promote** or **Discard**.
- API: `POST /surface/replay`; agent: `vejas_replay_literal`.

If there is no recent traffic to replay against, Apply promotes directly —
there is nothing to preview.

## Promote and roll back

A promote rewrites one literal in place, hot-reloads the flow, and records
an audit entry — no process restart.

- Promote: `POST /surface/set` (`vejas_set_literal`).
- Roll back: `POST /surface/rollback` (`vejas_rollback_literal`) — itself a
  forward-only, audited promote back to the value the literal held before
  its last change. History is never rewritten.

## Bigger changes: time-travel and canary

For a change that touches a whole *version* of a flow (not one literal),
two tools let you judge it against reality before it goes live:

| | Time-travel | Canary |
|---|---|---|
| Against | A **window of past** persisted traffic | **Live** traffic, as it arrives |
| What it does | Replays that window through the candidate version, diffs vs. current | Shadow-follows the flow, diffs each live event |
| Read | `vejas_time_travel` / `POST /surface/timetravel` | `vejas_canary_status`, `GET /surface/canary` |
| Start/stop | one-shot | `vejas_canary_start` / `_stop` |

Both obey the **shadow invariant**: the candidate runs in a shadow engine
and its emits are *never* published. You are comparing outcomes, not
double-sending them.

## In a cluster, a promote is a version

With `VEJAS_CLUSTER=1`, a local file write is refused — a promote instead
publishes a **version** into a shared JetStream KV overlay that every
instance converges on ([benchmarks](../reference/benchmarks.md): 60 ms
convergence, lossless mid-burst). If a later git deploy moves the baseline
the promote was made against, the overlay is **evicted loudly** (git wins;
the promoted content is kept in version history to re-promote) — visible at
`GET /evictions`. You never get a half-cluster running two meanings.

## The whole loop

Fix a [dead letter](dlq-replay.md): read the failure, draft the change,
**prove it** (time-travel yesterday's traffic, canary today's), promote —
and in [governed mode](governed-mode.md) that proof is the evidence a human
approves. The human owns one decision: the meaning.
