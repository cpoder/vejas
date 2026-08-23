# 0023 — IBM MQ: a first-class transactional connector, not an exec-bridge

- Status: Proposed
- Date: 2026-08-23

## Context

Kafka (ADR-0022) fit the exec-bridge: `kcat` streams records on stdout, the
driver publishes them, and resumption rides an offset in our KV because a Kafka
read is non-destructive. **IBM MQ breaks that model at the semantics, not the
dependency.** An MQ `MQGET` under syncpoint is **destructive** — the message
leaves the queue on `MQCMIT`. For bus-side at-least-once you must:

    MQGET (under MQGMO_SYNCPOINT) → publish to the bus (confirmed) → MQCMIT

If you `MQCMIT` before the bus publish is confirmed and then crash, the message
is gone from MQ and never reached the bus — **lost**. The exec-stream-source
model is one-way (child → stdout → driver publishes): the child has no way to
know the bus confirmed, so it cannot place its `MQCMIT` correctly. `kcat` works
because Kafka is not destructive; MQ is.

The bidirectional-exec fix (the child waits for an ack on stdin before `MQCMIT`)
is **rejected**: it reinvents — fragilely, over stdin framing and backpressure and
partial writes — exactly what a NATS client gives a process for free, and it moves
the transactional invariant into a home-made stdio protocol instead of the proven
bus.

## Decision

The MQ connector is a **dedicated binary that is a first-class citizen of the
bus** — precisely the external connector `docs/SUBJECTS.md` has described from the
start: *"a process that follows these rules on the same bus: publish JSON on
`vx.<domain>`; consume with a durable pull consumer; ack a message only after its
side effect succeeded."* The exec-bridges (kcat, curl-shaped) were the
convenience path; transactional MQ is the case that justifies the full citizen. It
owns both the MQ transaction and its own NATS client, so the commit ordering lives
where it belongs.

- **Source (MQ → bus):** `MQGET` under `MQGMO_SYNCPOINT` → NATS `publish` (await
  the JetStream pub-ack) → `MQCMIT`. A crash before `MQCMIT` leaves the message on
  the queue (`MQBACK` on any error), so it is re-got — at-least-once, no loss. No
  offset, no KV: **the broker holds the state** (contrast ADR-0022, where Kafka's
  non-destructive read needed our KV offset).
- **Sink (bus → MQ):** NATS **durable pull** → `MQPUT` under syncpoint →
  `MQCMIT` → **ack** the NATS message (side-effect-before-ack, SUBJECTS.md rule
  3). Honest at-least-once note: a crash between `MQCMIT` and the NATS ack
  redelivers the bus message → a **duplicate `MQPUT`**. That is the standard
  contract; the mitigation, documented in the recipe, is to carry an idempotency
  key in `MQMD.MsgId` so a downstream consumer dedups.

### The client, and the real verification point

The Rust path is `mqi` (v0.3) over `libmqm-sys` — a `-sys` crate that FFIs IBM's
**redistributable MQ C client**, needed at build and run time (like the SAP NW
RFC SDK, ADR-0014). It is not pure-Rust and does not pretend to be; the vendored C
lives with the connector binary, never in the core.

The verification that gates the crate choice is **not TLS** — MQ does TLS in the
channel/CCDT on the C-client side, the binding only passes it through — but
**syncpoint coverage**: `MQGMO_SYNCPOINT` on the get and `MQCMIT`/`MQBACK`
exposed cleanly. If `mqi` v0.3 does not expose them, the fallback is **not** a
different broker approach but a **hand-declared FFI on `libmqm-sys`** — the exact
move the SAP connector already made (~30 lines of `#[repr(C)]` FFI without
headers, ADR-0014). A young crate is therefore not a blocker; it is a precedent.

### Supervision

Reuse the existing exec mechanics — restart-with-backoff and the singleton lease
(ADR-0020) — with a "the child owns its NATS connection" flag rather than a new
supervisor kind, if it fits cleanly; the source is a singleton (one getter), the
sink is competing-safe by its own durable. Implementation detail, settled at
build time.

## Consequences

- The first-class external-connector citizen SUBJECTS.md always described gets its
  first real instance. The exec-source/sink stay the convenience for the
  non-transactional majority; this is the escape upward for the cases that need to
  own their transaction (MQ today, JMS/others later).
- No offset machinery for MQ — simpler than Kafka in that one respect, because the
  broker is the durable cursor. The trade is that recovery is MQ's transaction,
  not our KV; both are at-least-once, measured the same way on the bench.
- The IBM MQ C client is a build+runtime dependency of *this connector*, not the
  base image — installed where an MQ recipe runs, like the SAP child's SDK. A
  deployment without MQ ships nothing extra.
- CI certification likely needs the free **MQ Developer** container (the peer is
  evaluating it); if it is too heavy, a **declared exception** (ADR-0017,
  SAP-style) with what was verified against a real queue manager.

## Open questions (for review)

1. `mqi` v0.3 syncpoint coverage — confirm `MQGMO_SYNCPOINT` + `MQCMIT`/`MQBACK`
   before committing to the crate; hand-FFI fallback ready either way.
2. Whether the "child owns its NATS" supervision is a flag on the exec driver or a
   thin new kind — settle at build, keep it minimal.
3. The sink dedup key: is `MQMD.MsgId` always available/settable through `mqi`, or
   do we carry the key in a message property? Recipe-level, but flag it.

## Rejected

- **Exec-bridge (kcat-shaped) for MQ** — the one-way stdout model cannot place the
  `MQCMIT` after a confirmed bus publish, so a destructive get would lose messages.
- **Bidirectional exec (ack-on-stdin)** — reinvents the bus's guarantees over a
  fragile stdio protocol and relocates the transactional invariant off the proven
  bus.
- **rdkafka-style C-in-core** — not applicable (no such shared client), and against
  the footprint thesis regardless.
