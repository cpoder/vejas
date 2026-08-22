# 0015 — Persistent dead-letter queue with operator replay

- Status: Accepted
- Date: 2026-08-22

## Context

The bus is at-least-once (ADR-0002): a flow or sink that fails a message naks it,
and it redelivers. A message that fails *every* time (a poison message — bad
JSON, a genuine bug, a mapping that can't cope with a shape) must not redeliver
forever, so today the runtime **drops** it after `MAX_DELIVERIES` (acks it, writes
one line to the in-memory trace ring). This happens at three points:

1. a flow's source event is not JSON (`supervise_vjs`),
2. a flow's `vjs::run` errors past the delivery cap (`supervise_vjs`),
3. a sink handler errors past the cap (`run_sink`).

For an operator this is the wrong outcome twice over: the failed work is **lost**
(the trace ring holds 50 entries per unit and evaporates on restart), and there
is **no way to reprocess it** after fixing the cause. "It silently drops my
messages" disqualifies a runtime in a serious evaluation. This is the gap the
audit flagged (R2) — the operator-credibility half of the platform.

## Decision

Poison messages are **parked, not dropped**, in a persistent dead-letter queue,
and an operator **replays** them after correcting the cause.

**A dedicated JetStream stream.** Dead letters go to a **separate** stream
(`VEJAS_DLQ`) over a **sibling subject root** `vxdlq.<unit>`, *not* under `vx.>`.
Two reasons: JetStream forbids two streams with overlapping subjects, so a
dead-letter stream cannot live under the hot stream's `vx.>`; and the DLQ's
retention must be **independent** of the hot path (a `max_age` on the hot stream
must never evict dead letters). This mirrors the control plane's own root
(`vxc.`, ADR-0013): infrastructure that must not be captured by `vx.>` gets its
own root. `VEJAS_DLQ` uses Limits retention, no `max_age`, and a bounded
`max_msgs` safety cap (discard oldest when full, logged — a full DLQ is itself an
operator signal, never a silent truncation).

**A death envelope.** Each dead letter is published as
`{original_subject, unit, attempts, first_seen, last_error, dead_at, payload}` —
so the panel shows *why* it died and the replay knows *where* to re-inject it.
`payload` is the original message (parsed JSON when possible, else the raw
string). `first_seen` comes from the JetStream message timestamp when available;
`dead_at` from the runtime clock (the DLQ is Rust infrastructure — the no-clock
rule is about VejasScript, ADR-0001, not the runtime).

**Publish-before-ack.** At each of the three drop points, the runtime publishes
the envelope to `vxdlq.<unit>` and only **then** acks the original. If the DLQ
publish fails, the original is **not** acked — it redelivers, and we retry rather
than lose it. This keeps the at-least-once guarantee end to end (ADR-0002).

**Explicit operator replay — never auto-consume.** Nothing subscribes to
`vxdlq.>` to reprocess automatically: a message that is still poison would loop.
Replay is an operator action — surfaced as MCP tools (`vejas_dlq` to list,
`vejas_dlq_replay` to re-inject, `vejas_dlq_purge` to discard) and a panel "Dead
letters" card. Replaying a dead letter **re-publishes its `payload` to its
`original_subject`**, where the *current* (corrected) flow reprocesses it, then
removes it from the DLQ.

**The sibling of shadow-replay.** ADR-0005 shadow-replay previews a literal change
against the last *real* events (read-only, the ring); the DLQ replay recovers the
actually-dead work *after* the fix lands. Same doctrine — "correct the literal,
then rerun" — two corpora: the ring (preview) and the dead letters (recovery).
The operator loop is: a dead letter shows the failure → correct the literal
(`set_literal`, previewed by shadow-replay) → replay the dead letters.

## Consequences

- Poison messages are durable and inspectable, and recoverable after a fix — the
  operator story the audit demanded.
- **Cost:** a second JetStream stream; the DLQ must be bounded (the `max_msgs`
  cap) or a permanently-broken flow fills disk — a capped, logged DLQ is the
  safe default.
- **At-least-once, out of order:** replay re-injects to the original subject, so
  it inherits the bus's at-least-once semantics (a downstream must dedupe — the
  existing contract) and replays in DLQ order, not original order. Fine for
  idempotent flows; order-sensitive consumers must be aware. Documented, not
  hidden.
- The three drop points converge on one `to_dlq()` path, so the behaviour is
  uniform across flow-poison and sink-poison.
- A separate latent issue is now visible: a flow whose `emit` target is
  permanently unpublishable loops without a delivery cap. Out of scope here
  (it is a bus/config fault, not a poison payload), noted for a later pass.

## Alternatives considered

- **Keep dropping after the cap** — rejected: the failed work is lost and
  unrecoverable; the exact operator objection.
- **`vx.dlq.<unit>` inside the hot stream** — rejected: JetStream forbids
  overlapping stream subjects, and even a same-stream DLQ subject couples the
  dead-letter retention to the hot path's. A dedicated stream on a sibling root
  is the correct isolation.
- **Auto-replay from the DLQ** — rejected: a still-poison message would loop
  forever; recovery must be gated on a human having addressed the cause.
- **Drop straight to an external store (file/S3)** — rejected for v1: JetStream
  is already the one dependency (ADR-0002) and gives durability, consumers, and
  replay for free; an external sink is a future option, not a need.
