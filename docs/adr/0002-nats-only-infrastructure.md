# 0002 — NATS JetStream as the only infrastructure dependency

- Status: Accepted
- Date: 2026-08-19

## Context

An integration platform needs a message bus (decoupling sources from sinks),
persistence (at-least-once delivery, replay), and some state (cursors, KV). The
easy path accretes components: a broker + Redis for cache + Postgres for state.
Each is another thing to deploy, secure, back up, and reason about — and it
contradicts the "ultra-light, two-process" promise.

## Decision

Use **NATS with JetStream as the single infrastructure dependency**. JetStream
provides the stream (`VEJAS`, bound to `vx.>`), durable consumers
(at-least-once, redelivery), and — going forward — KV and object storage. No
Redis, no Postgres. The deployment is two containers: `nats` and `vejas`.

## Consequences

- One `docker compose up`; the "ultra-light" claim is literally true (two
  processes).
- Redelivery is the retry mechanism: flows publish emits **before** ack, so a
  crash re-delivers rather than losing an emit. Side effects must therefore be
  idempotent or deduplicated — a rule we state to connector authors.
- Shadow-replay of recent real events (a planned validation feature) is
  cheap because JetStream already retains them.
- **Cost:** JetStream is now load-bearing; its limits (consumer semantics,
  storage config, clustering) are ours to understand. State that does not fit
  KV/object store would force revisiting this ADR.

## Alternatives considered

- **Kafka/Redpanda:** heavier to operate for the SME/self-host target; better
  as an *external source connector* than as the core bus.
- **Broker + Redis + Postgres:** the conventional stack; rejected as
  component-sprawl that breaks the deployment story.
- **An embedded queue (no broker):** loses the any-language external-connector
  story that the bus contract (ADR-0007) depends on.
