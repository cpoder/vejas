# Environment variables

Everything below is read from the environment at process start. Nothing is
mandatory for a dev run: `vejas-runtime` with a local NATS and a `VEJAS_ROOT`
directory is a complete system.

## Core runtime

| Variable | Default | Purpose |
|---|---|---|
| `NATS_URL` | `nats://127.0.0.1:4222` | The bus. The only infrastructure dependency (ADR-0002). |
| `VEJAS_ROOT` | `.` | Root directory: `flows/`, `connectors/`, `tables/`, `tests/`. |
| `VEJAS_HTTP_ADDR` | `0.0.0.0:8686` | Panel + HTTP API + `/mcp` listen address. |
| `VEJAS_TOKEN` | *(unset)* | When set, the whole mutating surface (including `/mcp`) requires `Authorization: Bearer`. |
| `VEJAS_SUBJECT_ROOT` | `vx` | Bus subject prefix. |
| `VEJAS_STREAM` | `VEJAS` | JetStream stream name the runtime creates/uses. |
| `VEJAS_ACK_WAIT_SECS` | `30` | Redelivery window per consumer. Floor is 1s — JetStream silently rejects less. Cluster guidance: 3–5s. |
| `VEJAS_STATUS_SECS` | `10` | Status heartbeat cadence. |
| `VEJAS_TENANT` | *(unset)* | Tenant label for provisioned packages. |
| `VEJAS_BUNDLE` | *(unset)* | Bundle path for provisioning (`vejas_provision`). |
| `VEJAS_AGENT_CMD` | *(unset)* | Command the panel uses for its agent-assist box. |

## Secrets (ADR-0008)

| Variable | Purpose |
|---|---|
| `VEJAS_SECRETS_FILE` | File-backed secret store (dev). `VEJAS_SECRET_<PATH>` env entries also resolve. |
| `VAULT_ADDR` / `VAULT_TOKEN` / `VEJAS_VAULT_MOUNT` | HashiCorp Vault backend for `secret("path/key")`. |

## Clustering (ADR-0020)

| Variable | Default | Purpose |
|---|---|---|
| `VEJAS_CLUSTER` | *(off)* | Enables the cluster guard: local file mutations are refused (409) — changes go through versions or proposals. |
| `VEJAS_INSTANCE` | hostname | Instance identity for leases and audit. |
| `VEJAS_LEASE_TTL_SECS` | `10` | Singleton-lease TTL: crash-failover bound. |

## Governed mode (ADR-0024)

| Variable | Purpose |
|---|---|
| `VEJAS_REQUIRE_APPROVAL=1` | Every mutating path answers "submit a proposal instead". |
| `VEJAS_APPROVAL_TOKEN` | The **human** approval credential (`X-Approval-Token` header) — deliberately distinct from the agent's `VEJAS_TOKEN`. The runtime refuses to start governed mode without it. |

## Observability (ADR-0016)

| Variable | Purpose |
|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP/HTTP-JSON export (hand-rolled, no SDK tree). `/metrics` (Prometheus) is always on. |
| `OTEL_SERVICE_NAME` | Service name for exported spans/metrics. |

## API flows

| Variable | Purpose |
|---|---|
| `VEJAS_API_TITLE` / `VEJAS_API_VERSION` / `VEJAS_API_DESCRIPTION` | OpenAPI metadata for flows exposed as APIs. |

## Standalone connector binaries

Each first-class connector binary reads its own `VEJAS_<NAME>_*` family plus
`NATS_URL` and `VEJAS_STREAM`. The certified recipes under
[`docs/examples/connectors/`](https://github.com/cpoder/vejas/tree/master/docs/examples/connectors)
are the authoritative, linted reference for: **IBM MQ** (`VEJAS_MQ_*`, plus
`VEJAS_MQ_LIB` to point at the redistributable client and `VEJAS_MQ_USER` /
`VEJAS_MQ_PASSWORD` for MQCSP auth), **AMQP/RabbitMQ** (`VEJAS_AMQP_*`,
TLS via `VEJAS_AMQP_TLS_CA` / `VEJAS_AMQP_TLS_SERVER_NAME`), **SAP**
(`SAP_*`), and **Salesforce** (`SF_*`).
