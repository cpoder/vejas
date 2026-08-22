# 0018 — Shadow-replay on persisted traffic (and the promote audit trail)

- Status: Accepted
- Date: 2026-08-22

## Context

The business-surface loop (ADR-0005) lets a domain user or an agent change a
literal — a mapping entry, a threshold — and the runtime hot-reloads it onto the
live flow. Before promoting, `vejas_replay_literal` / `POST /surface/replay`
**shadow-replays** the change: it reruns recent real events against the current
and the patched script and returns the before/after emit diff, writing nothing.
This is the safety rail that makes "let an agent correct the mapping" defensible —
you see the blast radius before it ships.

Until now that replay drew its events from the **in-memory trace ring**: the last
50 events per unit, held in `TRACES`, gone on restart. Two limits made the rail
weaker than the MANIFESTO's claim of "shadow-replay on real traffic":

1. **It evaporates on restart.** Replay right after a deploy (exactly when you are
   promoting a fix) saw an empty or near-empty ring — nothing to replay against.
2. **It is tiny and recency-clipped.** 50 events is not a representative sample of
   a busy flow; a change that only bites a rare shape would not be exercised.

Meanwhile the real traffic is *already persisted* — every event a flow consumes
lives in the JetStream stream (ADR-0002), for the stream's whole retention. The
replay was reading a lossy in-memory shadow of data the bus already holds durably.

## Decision

**Hydrate replay from JetStream (Accepted, implemented).** `replay_literal` now
pulls the flow's recent events for its `source` subject straight from the
persisted stream, and only falls back to the trace ring when there is no bus
source (an api/tool flow), the stream is empty, or NATS is unreachable. The
response carries a `source` tag (`jetstream` | `trace-ring`) so the operator
knows what the diff was computed against — a silent fallback would be a lie about
coverage.

The hydration is **strictly read-only** — "shadow" has to mean shadow:

- An **ephemeral** pull consumer (no durable name → a server-assigned name,
  wholly separate from the flow's own durable consumer). The flow's delivery and
  ack state are a different consumer entirely; replay cannot perturb them.
- **Never acked.** Messages are pulled with `AckPolicy::Explicit` and simply not
  acked; because the consumer is ephemeral and discarded (a 30s
  `inactive_threshold` reaps it), the un-acked pulls expire with it. Nothing is
  consumed off the stream, nothing redelivers to the flow.
- **Bounded.** It starts a window before the stream's last sequence
  (`DeliverPolicy::ByStartSeq`), not from the beginning, and caps the scan — an
  operator action must not walk an unbounded history.

Verified end to end: publish 30 events, **restart the runtime** (clearing the
ring), replay → `source: jetstream`, 30 events, the right subset changed; a second
replay still sees 30 (nothing consumed); a fresh event still flows through the
live durable; the ephemeral consumers are reaped (no leak).

**A promote audit trail + rollback (Accepted, implemented).** Hydration makes
*preview* honest; the other half of operator credibility is *history*: every
promote (`set_literal`) is recorded as an append-only, auditable fact — file,
name, key, before → after, when, and by whom — so "who changed this threshold and
to what" has an answer, and so a promote can be **rolled back**. The three policy
calls below were arbitrated (Cyril, 2026-08-22) and built accordingly.

1. **Where the trail lives — both git and a stream.** Repo-deployed changes are
   audited by git itself (the commit that changed the literal *is* the record).
   **Live** promotes (panel `POST /surface/set`, MCP `vejas_set_literal`) — which
   have no commit — are appended to a dedicated JetStream stream **`VEJAS_AUDIT`**
   on the sibling root `vxaudit.<unit>`, exactly mirroring the DLQ (ADR-0015): its
   own root so no `max_age` on the hot stream can touch it, Limits retention, a
   generous oldest-discard cap (`AUDIT_MAX_MSGS`) that is a safety bound, never a
   routine truncation. The record: `{ts, actor, unit, file, name, key, before,
   after}`. The live write is **best-effort** — a promote is not blocked when
   `VEJAS_AUDIT` is unreachable (git is the durable backstop for committed
   changes), but the failure is logged loudly, never swallowed.

2. **The actor is the channel.** The record's `actor` names where the promote came
   from — `panel`, `mcp`, `rollback:panel`, `rollback:mcp` — since MCP carries no
   human identity today. When a surface gains real user identity, it rides in the
   same field; the channel is the honest floor, not a placeholder.

3. **Rollback is a forward-only promote to the recorded previous value.** It reads
   the most recent `VEJAS_AUDIT` record for the literal, sets it back to that
   record's `before`, and writes a **new** audit record (`actor: rollback:*`,
   `before` = current, `after` = restored). It never rewrites the trail, reuses
   the same write + hot-reload + audit path with no special case, and can be
   previewed first with the same shadow-replay rail. Rolling back a rollback walks
   the history back one more step — verified. Surfaced as `vejas_rollback_literal`
   (MCP) and `POST /surface/rollback`.

## Consequences

- The MANIFESTO's "shadow-replay on real traffic" is now literally true and
  survives restarts; preview coverage is the stream's retention, not 50 entries.
- No new dependency and no hot-path cost: hydration runs only when an operator
  asks, over the bus that already holds the data.
- Replay cost scales with the scan window, not the whole stream; still, a very
  large or very sparse subject means "recent N" may scan many messages — bounded,
  logged if the cap is hit, but not free. Acceptable for an operator action.
- `source: trace-ring` in a response is a signal, not just metadata: it means the
  stream had nothing (fresh flow, or NATS down) and the diff is only as good as
  the ring — surface it, never hide it.
- Rollback composes with this rail for free: it is a promote to a recorded value,
  previewed the same way and audited the same way.
- The trail covers what git cannot see — promotes applied *live* between deploys.
  A live promote that never gets committed is still on the record in `VEJAS_AUDIT`;
  one that is later committed appears in both, which is a reconciliation surface,
  not a contradiction (same before → after, two witnesses).
