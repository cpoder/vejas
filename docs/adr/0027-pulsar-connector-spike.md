# 0027 — Apache Pulsar: spike, and a deliberate defer

- Status: Proposed (spike — recommend DEFER until real demand)
- Date: 2026-08-23

## Context

Pulsar was scoped as "a plus" (Cyril), after the brokers that enterprises run
day-to-day (IBM MQ, MQTT, AMQP — ADR-0023/0025/0026). This spike evaluated the
Rust path and asks one question: does a Pulsar connector fit the platform's grain —
all-Rust, sync (no async runtime), tiny footprint, no C where avoidable — the way
MQ (hand-FFI) and AMQP (amiquip, sync) did?

## What the spike found

The only real Rust client is the **`pulsar` crate (6.8)**. It breaks *three* thesis
axes at once, and two of them cannot be trimmed away:

1. **Async/tokio — unavoidable.** The crate is async-first; a runtime (`tokio` or
   `async-std`) is required, `default-features = false, features =
   ["tokio-runtime"]` still pulls tokio. Every other connector and the core are
   sync/blocking (MQTT and MQ hand-rolled, AMQP on sync amiquip, NATS blocking).
   Pulsar would be the *only* component dragging an async runtime — a real
   asymmetry, even isolated to its binary.
2. **`native-tls` → `openssl-sys` — a hard C dependency, NOT feature-gated.** pulsar
   6.8 depends on `native-tls` and `tokio-native-tls` unconditionally (`cargo tree
   -i openssl-sys` roots straight at pulsar). Unlike amiquip — where
   `default-features = false` dropped openssl-sys and a rustls adapter gives pure-
   Rust TLS (ADR-0026) — here there is no feature to turn it off. A C TLS stack is
   forced into the binary. (openssl-sys also failed to build in this environment
   without system OpenSSL headers, the same wall amiquip's default hit.)
3. **Weight.** 360 transitive crates at defaults; **233** even after dropping the
   compression codecs (lz4-sys, zstd-sys, snap — themselves C). For comparison: the
   MQ connector added ~0 over nats (hand-FFI), AMQP ~70.

## Decision (proposed): defer

**Do not build the Pulsar connector now.** It is a "plus", and the cost is the
highest of the wave while the demand is the least established. Building it would put
the platform's only async runtime and an un-removable C TLS stack into the tree for
a broker no user has asked for yet. The right call is to record the evaluation and
wait for real demand.

**If demand appears, the path is known:**

- Use the `pulsar` crate with `tokio-runtime`, **isolated to the Pulsar connector
  binary** (never the core, never the other connectors) — the same "vendored weight
  lives with the connector" rule the SAP/MQ connectors follow. Accept tokio there as
  a scoped exception, documented as such.
- Bridge async→sync at the connector's edges with a `Runtime::block_on` per
  operation, so the connector still presents the same blocking source/sink shape as
  MQ/AMQP: `consumer.next().await` → NATS publish (await pub-ack) → `ack`; durable
  pull → `producer.send().await` + await the send receipt → ack NATS. The
  transactional shape is identical to AMQP (receipt/ack is the commit, no syncpoint).
- Reuse `connectors/mq/src/lease.rs` for singleton-by-order, as with MQ and AMQP.
- The openssl-sys C dependency is accepted for Pulsar specifically (no rustls path in
  the crate), and called out in the recipe as a build-time requirement — like the
  SAP SDK, present only where a Pulsar recipe runs.

## Consequences

- The brokers wave lands as **three built/prototyped citizens** (MQ, MQTT, AMQP) plus
  **one recorded defer** (Pulsar) — an honest map of the space rather than a rushed
  fourth connector that would have been the heaviest and least-wanted.
- The decision is on record with the concrete blockers (async, un-gated openssl-sys,
  weight), so revisiting it later is a lookup, not a re-investigation.
- No `connectors/pulsar/` crate is committed: a half-built crate pulling tokio +
  openssl-sys would misrepresent Pulsar as supported and drag the deps into the repo.

## Rejected

- **Building it now on the `pulsar` crate** — drags the platform's only async runtime
  and an un-removable C TLS stack in for a broker with no established demand.
- **Hand-rolling the Pulsar binary protocol** (the MQTT move) — Pulsar's protobuf
  command protocol is larger than AMQP's; a large pure-Rust effort is not justified
  for a "plus". If it ever were, this is the only route back to no-C / no-tokio.
- **An async-std runtime instead of tokio** — swaps one async runtime for another;
  does not address the core mismatch (the platform is sync) or openssl-sys.
