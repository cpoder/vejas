# The bus

NATS with JetStream is the **only** infrastructure (ADR-0002). Not "the
message broker among other services" — the entire platform substrate:

- **Transport & persistence.** Every event between units crosses a
  JetStream stream (`VEJAS`, subjects `vx.>`). A hop is acknowledged only
  after the next stage's write is confirmed — *publish-before-ack*, so a
  crash anywhere redelivers instead of losing (at-least-once, end to end).
- **State.** Offsets, versions, leases, proposals: JetStream KV buckets.
  There is no database.
- **Scaling.** Flows and sinks are competing consumers on durables — add an
  instance, throughput distributes; kill -9 one, nothing is lost (measured:
  20 000/20 000 through an instance kill under load). Sources that must be
  singletons take a KV lease ([clustering](../guides/clustering.md)).
- **Audit.** Mutations append to an audit stream; the DLQ is a subject
  hierarchy (`vxdlq.>`) with death envelopes.

## Subjects

Everything speaks `vx.<domain>.<name>` (root configurable via
`VEJAS_SUBJECT_ROOT`). A connector publishes *into* the taxonomy or
consumes *from* it; a flow declares `source "vx…"` and `emit`s to other
subjects. An **external connector in any language** is a first-class
citizen by following the same rules on the same bus — publish JSON on
`vx.<domain>.<name>`, consume with a durable, ack after your side effect.
The full contract: [Subjects reference](../reference/subjects.md).

## Delivery contract

At-least-once, per-subject FIFO, redelivery after `VEJAS_ACK_WAIT_SECS`
(default 30s; floor 1s), 5 attempts then the dead-letter queue with an
explicit envelope — never a silent drop
([DLQ & replay](../guides/dlq-replay.md)). Consumers must be idempotent or
carry a dedup key; the transport invariants are CI-tested on a live bus
(FIFO, redelivery→cap→DLQ, kill -9 no-loss, reconnection).
