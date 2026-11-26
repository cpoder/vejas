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
the full delivery contract (redelivery, DLQ). The answer carries the event's
`trace_id`; send a `traceparent` header and the event continues your trace,
on this webhook as on an API flow (see [observability](observability.md)).

**Lock the port to its subjects.** The `/ingest` port is unauthenticated
unless the sender signs (see below), and it only publishes. Without a limit, any caller can publish to any
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

**Make the sender prove itself.** `ALLOW` limits where a request goes, not
who sent it. When the sender signs its webhooks, as GitHub, Stripe, Slack,
Shopify and the senders of the Standard Webhooks specification do, name its
scheme and give the port the secret you share with it:

```
# connector: stripe_webhook
driver "http-in"
PORT = 8787
ALLOW = ["stripe"]
SIGNATURE = "stripe"
SIGNING_SECRET = secret("stripe/webhook")
```

Each request is then checked before anything else is read from it. One whose
signature is missing, does not parse or does not match gets `401` with the
reason, `{"error":"signature: the signature does not match"}`, and nothing
reaches the bus. A caller without the secret gets `401` whatever the subject,
so it learns nothing of `ALLOW`. The secret stays in the secret store, and
neither the answer nor the log shows it.

| `SIGNATURE` | The sender sends | Signed |
|---|---|---|
| `github` | `X-Hub-Signature-256: sha256=<hex>` | the body |
| `stripe` | `Stripe-Signature: t=<ts>,v1=<hex>` | `<ts>.<body>`; any `v1` may match (two while a secret is rolled) |
| `slack` | `X-Slack-Signature: v0=<hex>` and `X-Slack-Request-Timestamp` | `v0:<ts>:<body>` |
| `shopify` | `X-Shopify-Hmac-Sha256: <base64>` | the body |
| `standard-webhooks` | `webhook-id`, `webhook-timestamp` and `webhook-signature: v1,<base64>` (or their `svix-` names); the secret is written `whsec_…` | `<id>.<ts>.<body>` |
| `gitlab` | `X-Gitlab-Token: <the secret>` | nothing: the token is the secret |
| `hmac-sha256` | `SIGNATURE_HEADER`, after an optional `SIGNATURE_PREFIX`, in `SIGNATURE_ENCODING` `hex` (default) or `base64` | the body |
| `token` | the secret itself, in `SIGNATURE_HEADER` | nothing |

Every HMAC is SHA-256. Where a timestamp is signed, a request more than
`SIGNATURE_TOLERANCE_SECS` (300) away from the runtime's clock is refused,
which bounds how long a captured request can be played again. GitHub and
Shopify sign no timestamp: a flow that must not act twice keys on what names
the event in its body, an order's `id` and `updated_at` for instance, since the
delivery id these senders put in a header does not reach a flow. The path is
not signed either: keep `ALLOW`, so that a signed request reaches only the
subjects it is for.

GitHub posts a form unless the webhook's content type is `application/json`:
choose it, since `http-in` takes JSON only. Slack checks a Request URL before
it sends events to it; once the signature passes and `ALLOW` takes the
subject, `http-in` answers that check with its challenge and publishes
nothing. Slack's Events API then posts JSON, and its slash commands post a
form, which `http-in` does not take.

Refusals are counted on `/metrics` as
`vejas_signature_failures_total{unit,reason}`, the reason being `missing`,
`malformed`, `stale` or `mismatch`, and the log counts them too. A manifest
that names a key of the check (`SIGNATURE`, `SIGNING_SECRET`, `SIGNATURE_…`)
and does not give the runtime a check it can run does not start: no scheme it
knows, no secret it can read, a `whsec_` key under 16 bytes. The port stays
closed rather than open without its check.

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
