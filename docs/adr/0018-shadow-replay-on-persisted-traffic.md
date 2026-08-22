# 0018 — Shadow-replay on persisted traffic (and the promote audit trail)

- Status: Accepted (hydration); Proposed (audit trail + rollback)
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

**A promote audit trail (Proposed — open questions below).** Hydration makes
*preview* honest; the other half of operator credibility is *history*: every
promote (`set_literal`) recorded as an append-only, auditable fact — file, name,
key, before → after, when, and by whom — so "who changed this threshold and to
what" has an answer, and so a promote can be **rolled back** to its previous
value. The shape is deferred to the questions below rather than guessed.

## Open questions (for arbitration — audit trail + rollback)

1. **Where does the trail live?** Options: (a) a dedicated JetStream stream
   (`VEJAS_AUDIT`, sibling root like the DLQ's `vxdlq`, ADR-0015) — consistent
   with "NATS is the only dependency", replicated, survives restarts; (b) a file
   in the flows git repo — the change is already a git-tracked edit, so the trail
   could be git itself (the commit that changed the literal *is* the audit
   record). (b) is almost free and already exists if promotes flow through
   GitOps; (a) is needed when promotes are applied live (panel/MCP) without a
   commit. Likely **both**: git is the trail for repo-deployed changes, the stream
   captures live promotes that have not yet been committed.

2. **Who is the "actor" in an agentic system?** A promote can come from a human in
   the panel, an agent over MCP, or a GitOps deploy. The trail must name which,
   and carry the agent/human identity the surface actually has (MCP has no user
   identity today). This is a policy call, not a code detail.

3. **What does rollback *mean*?** Two clean semantics: (a) rollback = a new
   promote that sets the literal back to the recorded previous value (forward-only
   history, symmetric with promote, itself audited) — never rewrites the past; or
   (b) rollback = `git revert` when the trail is git. (a) generalises to live
   promotes; (b) is the honest answer when the source of truth is the repo.
   Recommendation: **(a)** — rollback is just a promote with a known target value,
   so it reuses the shadow-replay rail (preview the rollback before applying it)
   and the same audit record, with no special path.

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
- Once the audit trail lands, rollback (semantics (a)) composes with this rail for
  free: it is a promote to a recorded value, previewed the same way.
