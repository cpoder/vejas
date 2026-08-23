# Changelog

All notable changes to Vejas are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html). While the major
is `0`, minor versions may carry breaking changes — they are called out here.

## [Unreleased]

## [0.1.0] — 2026-08-23

First tagged release. The platform is public, measured, certified and in
production (NIS2 evidence collection across four EU countries).

### Runtime & language
- One Rust binary on NATS/JetStream — the only infrastructure dependency.
- VejasScript: a pure per-event language; the business surface (thresholds,
  transcoding tables, rules) is extracted from the code and edited by domain
  experts in the panel, no code touched (ADR-0001, ADR-0005, ADR-0019).
- The runtime **is** the MCP server: agents read the language, write flows,
  test them, and they land running (ADR-0006).

### Delivery & operations
- At-least-once, every hop persisted; dead-letter queue with explicit replay
  (ADR-0015). Observability: Prometheus `/metrics` + optional OTLP (ADR-0016).
- Clustering with no coordinator: competing consumers, singleton leases,
  cluster-wide version promote measured at 60 ms, lossless (ADR-0020/0021).
- Change safely: time-travel over real traffic, shadow canary, audited
  promote/rollback; governed mode where agents propose and a human approves
  with a distinct credential (ADR-0021, ADR-0024).

### Connectors
- Twenty-one certified recipes admitted by CI; brokers as first-class
  citizens — MQTT and AMQP/RabbitMQ certified against real brokers each run,
  Kafka and IBM MQ under stated exceptions with real-instance verification
  (ADR-0022/0023/0025/0026). SAP and Salesforce standalone binaries (ADR-0014).

### Container image
- The release image is published to `ghcr.io/cpoder/vejas-runtime`
  (`:0.1.0` and `:latest`).

### Measured (8-core dev machine; `bench/`)
- Cold start 11 ms · 6–8 MB RSS · end-to-end p50 2 ms uncongested ·
  2 285 rt/s through a real MQTT broker · cluster promote 60 ms, lossless.

### Security
- Secrets never literal (ADR-0008). Write surface behind an optional bearer
  token; governed mode adds a distinct approval token. Path traversal
  (including symlink escape) contained and regression-tested in CI.

[Unreleased]: https://github.com/cpoder/vejas/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/cpoder/vejas/releases/tag/v0.1.0
