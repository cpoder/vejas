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
