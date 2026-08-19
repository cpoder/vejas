# 0007 — Connectors: native bundled + bus contract + SDK

- Status: Accepted (partial)
- Date: 2026-08-19

## Context

Connectors are where an integration platform meets the outside world, and where
an iPaaS's catalog value lives. Vejas needs connectors that fit the
"ultra-light, all-Rust" core, and an extension path that does not force every
integration into one language.

## Decision (built)

- **Bundled connectors are native Rust threads** in the runtime: `http-in`
  (`POST /ingest/<suffix>` → publish `vx.<suffix>`) and `slack-out` (durable
  pull consumer on `vx.slack.out` → webhook / DRY-RUN). No subprocess.
- **The subject convention is the whole connector interface** (`SUBJECTS.md`):
  publish JSON on `vx.<domain>.<name>`; consume with a durable pull consumer;
  ack only after the side effect succeeds, else `nak`. Any process that follows
  this — **in any language** — is a first-class external connector over the bus.

## Decision (planned, Phase 2)

- A typed **connector SDK**: a Rust `Connector` trait with two families,
  **Source** (pushes onto the bus) and **Sink** (consumes from it). The four
  input modes are patterns of one Source trait, modeled as trigger kinds:
  **webhook**, **poll** (tick + cursor), **queue/stream** (long-lived
  subscription: Kafka/AMQP/MQTT), **push/real-time**. A connector is a package
  (ADR-0003) with a manifest declaring its kind and its secret references
  (ADR-0008), hot-addable.
- **Connector-by-prompt**: the ADR-0006 generation loop, retargeted — a
  `vejas_new_connector` MCP tool whose grammar is the trait contract.

## Consequences

- The core stays Python-free and light; batteries are included, and the bus
  contract keeps the platform open to any-language connectors immediately.
- Modeling input modes as trigger kinds of one trait avoids reimplementing
  publish/ack logic per connector.
- Connector code is specification-shaped, so agents generate it well — the
  catalog can compound faster than hand-built catalogs did.
- **Cost / open question:** catalog breadth still takes real work; the SDK,
  manifest schema, and secret wiring are not built yet. Until the SDK lands,
  new bundled connectors mean editing `core/src/connectors.rs`.

## Alternatives considered

- **Container-per-connector (Airbyte):** strong isolation, heavy ops — rejected
  against ADR-0002.
- **WASM component connectors (wasmCloud/SmartModules):** attractive for pure
  transforms, but WASI networking/TLS friction is real for actual connectors;
  kept on the watchlist as a future compiled-connector runtime.
- **Python connector SDK (v0):** removed with ADR-0009.
