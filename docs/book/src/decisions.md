# Architecture decisions

The platform's memory: every consequential decision is an ADR — context,
decision, consequences, and what was rejected. They are the honest answer
to "why is it built this way", and the moat a rewrite would have to re-earn.

- [ADR-0001 — 0001 — VejasScript as the native flow language](https://github.com/cpoder/vejas/blob/master/docs/adr/0001-vejascript-native-language.md)
- [ADR-0002 — 0002 — NATS JetStream as the only infrastructure dependency](https://github.com/cpoder/vejas/blob/master/docs/adr/0002-nats-only-infrastructure.md)
- [ADR-0003 — 0003 — The package model](https://github.com/cpoder/vejas/blob/master/docs/adr/0003-package-model.md)
- [ADR-0004 — 0004 — Service composition & EXPORTS visibility](https://github.com/cpoder/vejas/blob/master/docs/adr/0004-service-composition-exports.md)
- [ADR-0005 — 0005 — Business surface: literals, corrected in place](https://github.com/cpoder/vejas/blob/master/docs/adr/0005-business-surface.md)
- [ADR-0006 — 0006 — The runtime is its own MCP server; flow-as-tool](https://github.com/cpoder/vejas/blob/master/docs/adr/0006-runtime-is-mcp-server.md)
- [ADR-0007 — 0007 — Connectors: native bundled + bus contract + SDK](https://github.com/cpoder/vejas/blob/master/docs/adr/0007-connectors.md)
- [ADR-0008 — 0008 — Secrets via a Vault, never in literals](https://github.com/cpoder/vejas/blob/master/docs/adr/0008-secrets-vault.md)
- [ADR-0009 — 0009 — All-Rust runtime, no Python](https://github.com/cpoder/vejas/blob/master/docs/adr/0009-all-rust-no-python.md)
- [ADR-0010 — 0010 — Transformation doctrine: small registry + code-by-example](https://github.com/cpoder/vejas/blob/master/docs/adr/0010-transformation-doctrine.md)
- [ADR-0011 — 0011 — Connector extensibility: external process, not native libs; WASM later](https://github.com/cpoder/vejas/blob/master/docs/adr/0011-connector-extensibility.md)
- [ADR-0012 — 0012 — Deployment topologies: cells and outbound-only collectors](https://github.com/cpoder/vejas/blob/master/docs/adr/0012-deployment-topologies.md)
- [ADR-0013 — 0013 — Remote control plane over NATS leaf nodes](https://github.com/cpoder/vejas/blob/master/docs/adr/0013-control-plane-leafnodes.md)
- [ADR-0014 — 0014 — SAP connector: native Rust over the NW RFC SDK, no JVM](https://github.com/cpoder/vejas/blob/master/docs/adr/0014-sap-native-rust-nwrfc.md)
- [ADR-0015 — 0015 — Persistent dead-letter queue with operator replay](https://github.com/cpoder/vejas/blob/master/docs/adr/0015-dead-letter-queue.md)
- [ADR-0016 — 0016 — Observability: hand-rolled Prometheus `/metrics` and OTLP trace export](https://github.com/cpoder/vejas/blob/master/docs/adr/0016-observability-metrics-otlp.md)
- [ADR-0017 — 0017 — The connector admission test](https://github.com/cpoder/vejas/blob/master/docs/adr/0017-connector-admission-test.md)
- [ADR-0018 — 0018 — Shadow-replay on persisted traffic (and the promote audit trail)](https://github.com/cpoder/vejas/blob/master/docs/adr/0018-shadow-replay-on-persisted-traffic.md)
- [ADR-0019 — 0019 — Rules-view: a read-only projection of a flow's rules](https://github.com/cpoder/vejas/blob/master/docs/adr/0019-rules-view-read-only-projection.md)
- [ADR-0020 — 0020 — Clustering and zero-downtime, the NATS-native way](https://github.com/cpoder/vejas/blob/master/docs/adr/0020-clustering-zero-downtime.md)
- [ADR-0021 — 0021 — Versioning, canary, and time-travel](https://github.com/cpoder/vejas/blob/master/docs/adr/0021-versioning-canary-timetravel.md)
- [ADR-0022 — 0022 — Kafka connector: an exec-bridge over kcat, not a native client](https://github.com/cpoder/vejas/blob/master/docs/adr/0022-kafka-connector-exec-bridge.md)
- [ADR-0023 — 0023 — IBM MQ: a first-class transactional connector, not an exec-bridge](https://github.com/cpoder/vejas/blob/master/docs/adr/0023-ibm-mq-transactional-connector.md)
- [ADR-0024 — 0024 — The proposal queue: governed change, from agents and the fleet](https://github.com/cpoder/vejas/blob/master/docs/adr/0024-proposal-queue.md)
- [ADR-0025 — 0025 — MQTT: a hand-rolled sync client, not a dependency](https://github.com/cpoder/vejas/blob/master/docs/adr/0025-mqtt-hand-rolled-sync-client.md)
- [ADR-0026 — 0026 — AMQP 0-9-1 (RabbitMQ): a sync connector on amiquip](https://github.com/cpoder/vejas/blob/master/docs/adr/0026-amqp-connector-amiquip-spike.md)
- [ADR-0027 — 0027 — Apache Pulsar: spike, and a deliberate defer](https://github.com/cpoder/vejas/blob/master/docs/adr/0027-pulsar-connector-spike.md)
