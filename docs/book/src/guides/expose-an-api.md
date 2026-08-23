# Expose an API — sync and async

Vejas gives you **both interaction semantics**, chosen per endpoint — not a
global mode:

| | Async ingestion | Sync API flow |
|---|---|---|
| Entry | `http-in` connector: `POST :8787/ingest/<subject>` | `api "VERB /path"` in the flow: `/api/...` on the panel port |
| Caller gets | `202` after the JetStream pub-ack | The flow's `respond <status>, {body}` |
| Durability | Persisted **before** processing; at-least-once through the whole pipeline | Computed in the request; a crash mid-request is the caller's retry (plain HTTP semantics) |
| Backpressure | The stream absorbs bursts; consumers drain at their pace | The caller waits |
| Use for | Webhooks, events, anything fire-and-forget | Lookups, validations, request/response REST |

## Async: a webhook that cannot lose

```
# connector: orders_webhook
driver "http-in"
PORT = 8787
```

`POST /ingest/shop.orders` publishes the JSON body on `vx.shop.orders` and
answers `202` only after JetStream confirmed the write — the caller's
success means *persisted*, not *processed*. Flows consume from there with
the full delivery contract (redelivery, DLQ).

**Lock the port to its subjects.** The `/ingest` port is unauthenticated by
design — it only publishes. Without a limit, any caller can publish to any
`vx.*` subject, a sink's subject included, triggering an outbound side
effect with no flow in between. Set `ALLOW` to the subject suffixes this
webhook is for:

```
driver "http-in"
PORT = 8787
ALLOW = ["shop"]          # shop, shop.orders, shop.refunds … — 403 for anything else
```

Match is by subject segment: `"shop"` allows `shop` and `shop.orders`, not
`shop_internal`. Absent, the port stays open to any `vx.*` (backward
compatible) — set it whenever the port is reachable by anyone but the
operator (ADR-0029). It is defence in depth, not authentication: put the
port behind your ingress trust boundary as you would any webhook.

## Sync: a flow that is an API

```
# flow: order_status
api "GET /orders/{id}"
API_RESPONSE = {id: "string", status: "string"}

respond 200, {id: id, status: "shipped"}
```

- One flow per verb: a REST resource is several small flows.
- `{path params}`, the JSON body and `query` all arrive as event variables.
- `respond` is the HTTP answer; `emit` still fires bus side-effects
  (best-effort — if the side effect must be guaranteed, emit to a subject
  and let an async flow own it).
- The whole API self-describes at `GET /api/openapi.json`
  (`API_REQUEST`/`API_RESPONSE` literals type it; `VEJAS_API_TITLE` and
  friends fill the metadata).

## Mixing them

A common shape: `POST /api/orders` (sync) validates and answers `201` with
an id, and emits `vx.orders.accepted` — everything downstream (ERP sync,
notifications) rides the async pipeline with its delivery guarantees. You
choose the boundary per flow, and can move it later without changing
infrastructure.
