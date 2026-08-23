# Architecture Decision Records

Each ADR captures one decision: its context, the decision, its status, and its
consequences. Format after Michael Nygard. New decisions get the next number
and never rewrite history — a reversed decision is a new ADR that supersedes
the old one.

**Status legend:** `Accepted` (decided & implemented) · `Accepted (partial)`
(decided, partly built) · `Proposed` (decided direction, not yet built) ·
`Superseded by NNNN`.

| # | Title | Status |
|---|---|---|
| [0001](0001-vejascript-native-language.md) | VejasScript as the native flow language | Accepted |
| [0002](0002-nats-only-infrastructure.md) | NATS JetStream as the only infrastructure dependency | Accepted |
| [0003](0003-package-model.md) | The package model | Accepted |
| [0004](0004-service-composition-exports.md) | Service composition & EXPORTS visibility | Accepted |
| [0005](0005-business-surface.md) | Business surface: literals, corrected in place | Accepted |
| [0006](0006-runtime-is-mcp-server.md) | The runtime is its own MCP server; flow-as-tool | Accepted |
| [0007](0007-connectors.md) | Connectors: native drivers + declarative manifests + bus contract | Accepted |
| [0008](0008-secrets-vault.md) | Secrets via a Vault, never in literals | Accepted |
| [0009](0009-all-rust-no-python.md) | All-Rust runtime, no Python | Accepted |
| [0010](0010-transformation-doctrine.md) | Transformation doctrine: small registry + code-by-example | Accepted |
| [0011](0011-connector-extensibility.md) | Connector extensibility: external process, not native libs; WASM later | Accepted |
| [0012](0012-deployment-topologies.md) | Deployment topologies: cells and outbound-only collectors | Accepted |
| [0013](0013-control-plane-leafnodes.md) | Remote control plane over NATS leaf nodes | Accepted (partial) |
| [0014](0014-sap-native-rust-nwrfc.md) | SAP connector: native Rust over the NW RFC SDK, no JVM | Accepted |
| [0015](0015-dead-letter-queue.md) | Persistent dead-letter queue with operator replay | Accepted |
| [0016](0016-observability-metrics-otlp.md) | Observability: hand-rolled Prometheus `/metrics` and OTLP trace export | Accepted |
| [0017](0017-connector-admission-test.md) | The connector admission test | Accepted |
| [0018](0018-shadow-replay-on-persisted-traffic.md) | Shadow-replay on persisted traffic (+ promote audit trail) | Accepted |
| [0019](0019-rules-view-read-only-projection.md) | Rules-view: read-only rule projection (N1/N2/N3 doctrine) | Accepted |
| [0020](0020-clustering-zero-downtime.md) | Clustering & zero-downtime: competing durables + KV singleton leases | Accepted |
| [0021](0021-versioning-canary-timetravel.md) | Versioning, canary & time-travel (cluster-wide promotion) | Accepted |
| [0022](0022-kafka-connector-exec-bridge.md) | Kafka connector: exec-bridge over kcat (offsets in our KV) | Accepted |
| [0023](0023-ibm-mq-transactional-connector.md) | IBM MQ: first-class transactional connector (destructive get, syncpoint) | Proposed |
| [0024](0024-proposal-queue.md) | The proposal queue: governed change, from agents and the fleet | Proposed |

## Template

```markdown
# NNNN — Title

- Status: Proposed | Accepted | Accepted (partial) | Superseded by NNNN
- Date: YYYY-MM-DD

## Context
What forces are at play? What problem or tension prompts a decision?

## Decision
What we decided, stated plainly.

## Consequences
What becomes easier, harder, or constrained. Include the negative ones.

## Alternatives considered
What else was on the table and why it lost.
```
