# HTTP API

One listener (`VEJAS_HTTP_ADDR`, default `:8686`) serves the panel, the API
and MCP. With `VEJAS_TOKEN` set, every mutating route requires
`Authorization: Bearer <token>`. In governed mode (ADR-0024) mutating routes
answer with a didactic refusal pointing at the proposal queue.

## Request headers

| Header | On | Purpose |
|---|---|---|
| `Authorization: Bearer <token>` | every mutating route | Write protection, when `VEJAS_TOKEN` is set. |
| `X-Approval-Token: <token>` | `/proposals/{id}/approve\|reject` | The human approval credential, distinct from `VEJAS_TOKEN` (ADR-0024). |
| `X-Vejas-Actor: <id>` | any mutating route | *Optional.* Records **who** made the change in the audit trail; absent, the actor is `panel`. The enterprise auth proxy sets it after authenticating a user (ADR-0030) — useful open too, for a single operator naming themselves. Trimmed, capped at 128 chars. |

## Health & introspection

| Route | Purpose |
|---|---|
| `GET /healthz` | Liveness. |
| `GET /metrics` | Prometheus text format — hand-rolled, always on. |
| `GET /` · `GET /panel` | The panel (single embedded HTML). |
| `GET /topology` | Units and their wiring. |
| `GET /graph` | The flow graph (drives the panel's clickable diagram). |
| `GET /events` | Recent traced events (ring). |
| `GET /drivers` | Live driver catalog with config contracts. |
| `GET /rules` | The rules view (ADR-0019): N1 parameters, N2 read-only projection. |
| `GET /surface` | The business surface: editable literals, tables, spans. |
| `GET /evictions` | Version-overlay evictions (git-wins, loud — ADR-0021). |

## Files & surface

| Route | Purpose |
|---|---|
| `GET /file` · `POST /file/set` | Read / write a flow or connector file (guarded paths). |
| `POST /surface/set` | Edit one literal through the product write path (targeted restart). |
| `GET /fixture` · `POST /fixture/set` | Per-flow test fixtures. |
| `POST /preview` | Run a flow against a fixture without publishing. |
| `POST /flows/new` · `POST /connectors/new` | Create from the panel/agent. |
| `POST /connectors/test` | Connector probe (auth reachability, no writes). |
| `POST /reload` | Reload units after out-of-band file changes. |

## Failure handling

| Route | Purpose |
|---|---|
| `GET /dlq` | Dead letters with death envelopes (version-tagged). |
| `POST /dlq/replay` · `POST /dlq/purge` | Explicit replay / purge (ADR-0015). |

## Versions, time-travel, canary (ADR-0021)

| Route | Purpose |
|---|---|
| `POST /surface/timetravel` | Candidate vs live over a window of persisted real traffic. |
| `POST /surface/canary/start` · `/stop` · `GET /surface/canary` | Shadow canary on live traffic. |
| `POST /surface/replay` | Replay literal history (ADR-0018). |
| `POST /surface/rollback` | Forward-only rollback of a literal. |

## Governance (ADR-0024)

| Route | Purpose |
|---|---|
| `GET /proposals` | The proposal queue. |
| `POST /proposals/{id}/approve` · `/reject` | Human decision — requires `X-Approval-Token` (distinct credential). |

## Curation & provisioning

| Route | Purpose |
|---|---|
| `POST /events/golden` | Capture a ring event as a curated test case (golden traffic). |
| `POST /provision` | Instantiate a tenant package from a template. |
| `GET /secrets` · `POST /secrets/set` | Secret paths (values never returned). |

## Agents

| Route | Purpose |
|---|---|
| `POST /mcp` | JSON-RPC 2.0 — the runtime **is** the MCP server. See [MCP tools](mcp-tools.md). |
| `POST /ingest/<suffix>` *(http-in connector, own port)* | Webhook ingestion → `vx.<suffix>` on the bus, 202 after the JetStream pub-ack. |
