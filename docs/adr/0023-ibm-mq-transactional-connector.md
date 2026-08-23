# 0023 — IBM MQ: a first-class transactional connector, not an exec-bridge

- Status: Accepted (built — `connectors/mq/`)
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
  key in an MQMD field (`CorrelId` by convention — `MsgId` is usually the queue
  manager's; and it is 24 binary bytes, so a key is hashed to fit) so a downstream
  consumer dedups. See open question 3.

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

### Source concurrency: singleton by default, BECAUSE order

A destructive `MQGET` is the *original* competing-consumers case: two getters
never receive the same message. So unlike Kafka or a poller, the MQ source could
be **competing-safe** — N getters, each its own syncpoint — scaling and failing
over with no lease at all. The one thing N concurrent getters lose is **order**:
messages interleave across getters, and the queue's FIFO does not survive onto the
bus — and our transport test T1 (ADR-0020 bench) asserts per-subject FIFO.

The decision, and the reason is written so a reviewer need not ask: **singleton by
default, because order.** The source takes the singleton lease (ADR-0020) so one
getter preserves the queue's order onto the bus. A `COMPETING = true` mode is
offered as an **explicit** option — N getters, no lease, throughput over order —
for the deployments that genuinely value drain rate over ordering and whose
downstream does not depend on it. Default is ordered; competing is a choice you
make on purpose, with the trade stated in the recipe.

### Supervision

Reuse the existing exec mechanics — restart-with-backoff and the singleton lease
(ADR-0020) — with a "the child owns its NATS connection" flag rather than a new
supervisor kind, if it fits cleanly. One classification caveat: the connector must
show up correctly in the topology/graph (as an external/first-class source, not a
mislabeled `exec-source`) — verify the panel rendering. The sink is competing-safe
by its own durable. Implementation detail, settled at build time.

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
3. The sink dedup key. `MQMD.MsgId` is **24 binary bytes**, so an idempotency key
   must be hashed/truncated to fit — and the MQ app-level convention is often
   `CorrelId` rather than `MsgId` (the queue manager usually owns `MsgId`). Spike
   which `mqi` exposes cleanly (likely `CorrelId`) and document the choice; carry
   the key in a message property if neither is settable.

## Build outcome (2026-08-23) — `connectors/mq/`

Built as a standalone crate (like `sap-rfc`), NOT in the runtime workspace, holding
its own `nats` client. The open questions, settled:

1. **MQI binding** — went straight to the **hand-declared FFI** fallback, not the
   `mqi`/`libmqm-sys` crate: `dlopen("libmqic_r.so")` at runtime (`VEJAS_MQ_LIB`
   overrides), the eight MQI calls (MQCONNX/MQOPEN/MQGET/MQPUT/MQCMIT/MQBACK/
   MQCLOSE/MQDISC) and the five v1 descriptors (MQCNO/MQOD/MQMD/MQGMO/MQPMO) as
   `#[repr(C)]`. No build-time MQ dependency — the crate builds with no MQ present.
   `MQGMO_SYNCPOINT` on the get and `MQPMO_SYNCPOINT` on the put; MQCMIT/MQBACK
   exposed cleanly, which was the verification the crate choice hinged on.
2. **Supervision** — not an exec-driver flag but a **standalone binary that owns
   its NATS client**, which is what the transactional ordering requires. It shows
   up as a first-class external source/sink (its own process), not a mislabeled
   exec-source.
3. **Dedup key** — **CorrelId**, per the app-level convention. An idempotency key
   named by `VEJAS_MQ_DEDUP_FIELD` (a top-level JSON field) is folded to the 24
   binary bytes by `correlid_from_key` (FNV-1a over three offset domains). MsgId is
   left to the queue manager (`MQPMO_NEW_MSG_ID`).

**Verified without a live queue manager:**
- The transactional invariants the ADR rests on, against an in-memory fake broker
  with fault injection: no-loss on crash-before-`MQCMIT` (the message is re-got),
  consume-only-after-commit, put-invisible-until-commit / discarded-on-backout,
  and commit-failure-leaves-the-bus-message-for-redelivery.
- The FFI layout: `size_of` each descriptor equals its `cmqc.h` `MQ*_LENGTH_1`
  (324 / 72 / 128 / 168 / 12) — a `#[repr(C)]` padding bug would corrupt memory
  against a real QM, so this is the layout proof the ADR promised.
- End-to-end source wiring on real NATS (fake broker → bus): messages land on the
  subject, in order, each committed only after its JetStream pub-ack.
- Graceful failure: missing config → exit 2; MQ client absent → a clear dlopen
  error, not a crash.

**Declared CI exception (ADR-0017, SAP-style):** the MQI *semantics* against a real
queue manager (does MQGET-under-syncpoint / MQCMIT behave as the fake models)
need the free **MQ Developer** container and are certified out-of-band, not in the
per-commit CI. What was verified above is what a build machine without MQ can prove.

**Singleton lease — done.** The source takes a JetStream KV lease (`lease.rs`, the
same create/CAS-renew/delete/TTL mechanism as the core runtime, reimplemented in
the binary since it owns its own NATS client): exactly one instance gets, so the
queue's order survives onto the bus. `VEJAS_MQ_COMPETING=1` skips the lease (N
getters, throughput over order — a destructive MQGET is competing-*safe*, only
order is traded). If the lease is fenced or ages out under a stall the renewal
thread flags it and the source stops getting. Verified against real NATS KV: mutual
exclusion (a second acquire is blocked until release) and CAS fencing (a stale
revision cannot renew over a newer holder).

## Rejected

- **Exec-bridge (kcat-shaped) for MQ** — the one-way stdout model cannot place the
  `MQCMIT` after a confirmed bus publish, so a destructive get would lose messages.
- **Bidirectional exec (ack-on-stdin)** — reinvents the bus's guarantees over a
  fragile stdio protocol and relocates the transactional invariant off the proven
  bus.
- **rdkafka-style C-in-core** — not applicable (no such shared client), and against
  the footprint thesis regardless.
