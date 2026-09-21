# Changelog

All notable changes to Vejas are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html). While the major
is `0`, minor versions may carry breaking changes — they are called out here.

## [Unreleased]

### Added
- **Detect units** (ADR-0031): a VPL program under `detects/`, run by the
  Varpulis CEP engine embedded as a library — sequences, Kleene closures,
  negation, `.within()` in event time, windows, joins, forecast — with the
  contract a flow has: a durable consumer per `.from()` subject, emit before
  ack, poison to the DLQ. `vejas-runtime vpl-check <file>` and the MCP tool
  `vejas_vpl_check` give the engine's verdict; `/topology` lists the units
  under `detects`; CI checks every `.vpl` and runs `e2e/detect/run.sh`. The
  engine adds no async runtime to the binary (5.9 → 8.1 MB). Measured:
  18 643 evt/s on the merge evaluation's program (16 publishers,
  64 000 events; the flow unit runs it at 16 182). Not yet: snapshot-and-resume of a unit's state — a
  stateless detection loses nothing across a crash, a sequence can miss the
  matches that straddle one — the panel graph, and the business surface of
  a VPL program. See the book, *Detect units*.

### Changed
- The flow consumer loop no longer waits on itself: the pull request is
  flushed the moment it is made instead of sitting behind the sync client's
  5 ms flusher floor, messages are processed as they arrive instead of after
  the whole batch has landed, the next pull is kept in flight while a batch is
  processed, acks go out on a connection of their own, and the trace ring
  derives an event's preview on read instead of serialising every message a
  second time. Isolated flow hop 8 110 → 13 652 evt/s; the merge evaluation's
  flow 9 003 → 16 133 evt/s (16 publishers, 64 000 events, same box). The
  contract is unchanged — publish before ack, at-least-once (transport
  invariants T1–T5 pass; kill -9 mid-stream: 20 000/20 000 with 4
  duplicates, 0 lost). New on `/metrics`: `vejas_fetch_rounds_total`,
  `vejas_fetch_messages_total` and `vejas_round_seconds_sum{phase}` say where
  a unit's time goes. See `bench/README.md`, findings #6 and #7.

### Fixed
- `vejas-sap-rfc` (connectors/sap-rfc): a lost RFC conversation no longer
  poisons the connector for the rest of its life. The gateway (or a NAT in
  between) closes RFC connections left idle for a while; every later call then
  failed with `RFC_COMMUNICATION_FAILURE … no conversation found` until the
  process was restarted. The connector now pings a connection idle for more
  than a minute before using it, and when a call still finds the conversation
  gone (`RFC_COMMUNICATION_FAILURE`, `RFC_CLOSED`, `RFC_INVALID_HANDLE`) it
  reopens the connection and replays the call once. `SAP_RFC_TEST_HOOKS=1`
  enables `{"op":"_drop"}` to exercise that path.

## [0.2.0] — 2026-08-24

A security-hardening release. The write gate is now keyed on what a flow
actually *does* to the bus — not the HTTP verb — closing four HIGH findings from
an independent adversarial audit (R7). Adds the service-composition guide with an
executable CI guarantee, and corrects the published production numbers.

### Security — R7 audit (four HIGH, all fixed and frozen in CI)
- The write gate authorizes by **mutation, not HTTP verb**: it keys on
  `Program::writes_bus()`, an exhaustive AST walk over `emit` *and* `invoke`, so
  a bus write can no longer slip through a non-POST route (Finding A), a dynamic
  emit subject (Finding A′), or an invoke-mediated read (Finding A″). The
  fail-closed guarantees are frozen as end-to-end tests.
- `/connectors/new` is gated behind governed mode + the cluster guard, closing a
  connector-bypass path to command execution (Finding B).
- The three-way audit pass (A / A′ / A″ / B, plus the S4-1 secret-exposure note)
  is recorded in the security-audit scope.

### Language & docs
- **Composing services**: a new concept page for `invoke` (pipeline-merge
  composition) and cross-package `EXPORTS`, backed by an executable guarantee in
  CI — a service invoked from a flow merges its output into the caller, and the
  doc-contracts leg fails if that ever drifts.

### Corrected & honest
- Published numbers re-measured, not claimed (binary size, cold start, latency
  percentiles), each linked to its bench script.
- The Reglyze production line made accurate: young but real — two production
  customers, collecting NIS2 supplier-compliance evidence across eight EU
  countries today.
- Launch distribution kit added (`docs/launch/`): Show HN as a URL post + a
  seeded first comment.

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
  (`:v0.1.0` and `:latest`).

### Measured (8-core dev machine; `bench/`)
- Cold start 11 ms · 6–8 MB RSS · end-to-end p50 2 ms uncongested ·
  2 285 rt/s through a real MQTT broker · cluster promote 60 ms, lossless.

### Security
- Secrets never literal (ADR-0008). Write surface behind an optional bearer
  token; governed mode adds a distinct approval token. Path traversal
  (including symlink escape) contained and regression-tested in CI.

[Unreleased]: https://github.com/cpoder/vejas/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/cpoder/vejas/releases/tag/v0.1.0
