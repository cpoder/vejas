# 0022 — Kafka connector: an exec-bridge over kcat, not a native client

- Status: Accepted
- Date: 2026-08-23

## Context

Kafka is the enterprise door-opener — "does it talk to Kafka?" gates a lot of
serious evaluations. The instinct is a native, compiled-in Kafka client so the
core binary speaks the protocol directly, the way the SAP connector went native
(ADR-0014) and the HTTP client became in-binary ureq (curl → ureq). The
footprint thesis — ~5MB binary, single-digit-MB RSS, "all-Rust, no C we can
avoid" — makes that the tempting default.

A spike settled it against the native path, on two findings:

1. **The pure-Rust client's auth matrix does not clear the enterprise bar.**
   `rskafka` (the credible pure-Rust option) supports TLS (rustls) and SASL
   **PLAIN**, but **not SCRAM** — its `SaslConfig` has only a `Plain` variant.
   Managed clusters (Confluent Cloud, Aiven) use PLAIN-over-TLS and would work;
   self-managed clusters commonly require SCRAM and would not. "PLAIN/SCRAM
   minimum" was the bar, and it isn't met.
2. **The pure-Rust client is async — it pulls tokio.** Adding tokio + rskafka +
   rustls to the *core* binary would grow it well past the 5MB the whole
   footprint story rests on — for a single connector. That is the wrong trade.

rdkafka (the C binding) is not the answer either: it drags librdkafka *into our
binary*, which is exactly the C-in-the-core we avoid.

## Decision

Ship Kafka as an **exec-bridge over `kcat`** (kafkacat) — the primary Kafka
connector, not a fallback. This is the ADR-0014 precedent applied one layer out:
vendored C (librdkafka) lives **in the child**, never in our binary — just as
`libsapnwrfc` lives behind the SAP child, not in the core.

- **`kafka-source`** is an `exec-stream-source` manifest whose `CMD` wraps
  `kcat -C -u … -J` (consume, unbuffered, JSON-per-message). Each line is one
  Kafka record; the driver publishes it to the bus. The **singleton lease**
  (ADR-0020) guarantees exactly one consumer across a cluster.
- **`kafka-sink`** is an `exec-sink` manifest whose `CMD` wraps `kcat -P …`
  (produce); each bus message is piped to the child's stdin and produced.
- **Offsets live in *our* JetStream KV, not Kafka's consumer groups.** A generic
  resume seam was added to `exec-stream-source`: set `OFFSET_KV` and the driver
  reads the last committed offset from the `VEJAS_OFFSETS` bucket at (re)start and
  hands it to the child as `$OFFSET` (`kcat -o $OFFSET`), then commits each
  record's `OFFSET_FIELD` (default `offset`) **after** publishing it —
  publish-before-commit, at-least-once. Recovery and failover stay in *our*
  measured model (the lease + the offset in our KV), not Kafka's opaque
  consumer-group rebalancing. rskafka's lack of consumer groups is therefore a
  non-issue: we already own the primitives they provide. To scale past one
  consumer, **partition** (the ADR-0020 doctrine): one manifest per partition
  range, each with its own lease and its own `OFFSET_KV` key.
- **The full auth matrix rides `kcat`'s librdkafka**: TLS, SASL PLAIN **and
  SCRAM** (SHA-256/512), Kerberos, OAUTHBEARER — everything an enterprise cluster
  asks for — configured with `kcat -X security.protocol=… -X sasl.mechanisms=…`.
  Credentials come through `ENV = {…: secret("…")}` (never argv, ADR-0008).

## The escape hatch (documented in the driver)

Need an exotic Kafka feature, a mechanism `kcat` exposes but the recipe doesn't,
or a different client entirely? The exec-bridge carries it **without touching the
core** — change the `CMD`, or swap the child binary. This is the ADR-0011
promise: the connector boundary is a subprocess and a subject convention, so the
core stays small and the connector stays replaceable.

## Consequences

- Core binary unchanged: no tokio, no librdkafka, no new heavy dependency. The
  5MB footprint holds; the generic offset-resume is a few dozen lines.
- `kcat` is a **documented dependency of the Kafka connector**, not of the base
  image — installed where a Kafka recipe runs (apt/brew package it everywhere),
  exactly like the SAP child needs the NW RFC SDK. A deployment without Kafka
  ships nothing extra.
- The offset-resume seam is generic: any exec stream source with a monotonic
  offset (not just Kafka) can now resume through our KV.
- `exec-sink` spawns the child per message, so very high produce volume wants a
  batching wrapper (kcat reads many stdin lines per invocation) — a recipe-level
  refinement, noted.
- Native `rskafka` remains a **future option behind the same `Driver` interface**
  if a PLAIN-only, zero-subprocess deployment ever justifies the tokio weight.

## Rejected

- **Native rskafka in the core** — fails the SCRAM bar and pulls tokio into the
  5MB binary. Deferred to a future option, not v1.
- **rdkafka (C binding) in the core** — librdkafka in our binary is the C-in-core
  we avoid; the whole point is to keep it in the child.
- **A bespoke Go (franz-go) child** — full matrix, but Go in an all-Rust repo
  (ADR-0009 in spirit) for no gain over kcat. No.
