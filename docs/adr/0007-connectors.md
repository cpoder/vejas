# 0007 — Connectors: native bundled + bus contract + SDK

- Status: Accepted
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

## Decision (built, Phase 2)

- A typed **connector SDK**: a Rust `Driver` trait (`core/src/connectors.rs`)
  with two families, **Source** (pushes onto the bus) and **Sink** (consumes
  from it). Input modes are `kind`s of Source: `source:webhook`,
  `source:interval`, `source:poll` (queue/stream drivers for Kafka/AMQP/MQTT are
  future additions on the same trait). Shipped drivers: `http-in`, `timer`,
  `http-poll`, `slack-out`, `http-out`. The `Driver::kind()` string surfaces in
  the topology and graph.
- A connector is a **declarative instance manifest**: a `.vjs` file under
  `connectors/` (or `packages/<pkg>/connectors/`) with a `driver "name"`
  directive and UPPERCASE literal config. It is parsed and configured by the
  same machinery as flows — so its config is **editable in the panel /
  `set_literal`** and it is **hot-addable via reload**. The bundled `http-in`,
  `slack-out`, and a demo `timer` ship as such manifests.
- `vejas_drivers` (MCP) lists the driver catalog for writing manifests.

## Also built (Phase 2)

- **Secret references** in a manifest resolve via the Vault (ADR-0008):
  `WEBHOOK_URL = secret("slack/webhook")` yields the real value into the
  driver's config while the file holds only the reference.
- **Connector-by-prompt**: the `vejas_new_connector` MCP tool (and POST
  `/connectors/new`) — the ADR-0006 generation loop retargeted to the `Driver`
  trait contract; the agent picks a driver, writes config, and uses `secret()`
  for credentials.
- **External-process drivers** `exec-source`/`exec-sink` (ADR-0011): wrap a
  program in any language over stdio — the hot-add / native-vendor-SDK path.

## Still planned

- **Queue/stream Source drivers** (Kafka/AMQP/MQTT) on the same trait.
- **Resolved config as child-process env** for exec connectors (so a wrapped
  SAP jar receives `SAP_PASSWORD` without reading the Vault itself).

## Consequences

- The core stays Python-free and light; batteries are included, and the bus
  contract keeps the platform open to any-language connectors immediately.
- Modeling input modes as trigger kinds of one trait avoids reimplementing
  publish/ack logic per connector.
- Connector code is specification-shaped, so agents generate it well — the
  catalog can compound faster than hand-built catalogs did.
- **Cost / open question:** catalog breadth still takes real work. A new
  *bundled* driver still means editing `core/src/connectors.rs` and cutting a
  release; the out-of-process (`exec`) and agent-written paths absorb the long
  tail without a runtime change.

## Alternatives considered

- **Container-per-connector (Airbyte):** strong isolation, heavy ops — rejected
  against ADR-0002.
- **WASM component connectors (wasmCloud/SmartModules):** attractive for pure
  transforms, but WASI networking/TLS friction is real for actual connectors;
  kept on the watchlist as a future compiled-connector runtime.
- **Python connector SDK (v0):** removed with ADR-0009.
