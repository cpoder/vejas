# Example: a REST resource as VejasScript flows (with OpenAPI)

A flow can be exposed as a **synchronous HTTP endpoint** by declaring the verb and
path it serves; a **REST resource is a set of flows, one per verb**. The runtime
routes `(method, path)` to the flow, runs it on the body as sent with its
`{path params}` as variables, and returns its `respond`. The OpenAPI document
is generated automatically.

## The resource (`/orders`)

| File | Declaration | Endpoint |
|---|---|---|
| `orders_create.vjs` | `api "POST /orders"`       | create |
| `orders_get.vjs`    | `api "GET /orders/{id}"`   | read |
| `orders_update.vjs` | `api "PUT /orders/{id}"`   | update |
| `orders_delete.vjs` | `api "DELETE /orders/{id}"`| delete |

Each flow gets the request: its JSON **body** as the event (an object's fields
are variables), the `{path params}` as variables (e.g. `id`), and `request`
with the method, path, query string (`request.query.page`) and the headers
that say what the request is, plus those an `API_HEADERS` list names (see
[Expose an API](../../book/src/guides/expose-an-api.md)). It answers with:

```
respond <status>, { ...json body... }
```

`respond` is the synchronous HTTP response. A flow may still `emit "subject", {…}`
for asynchronous side effects on the bus — both happen.

## Served under `/api`

- `POST /api/orders`  · `GET /api/orders/{id}` · `PUT …` · `DELETE …`
- `GET /api/openapi.json` — the generated OpenAPI 3.0 spec (paths, verbs,
  path parameters, request bodies, operationIds from the flow names, and the
  per-operation summary from each flow's `tool` line).

Configure the spec's info via env: `VEJAS_API_TITLE`, `VEJAS_API_VERSION`,
`VEJAS_API_DESCRIPTION`. Writes honor `VEJAS_TOKEN` like the rest of the surface.

## Notes

These demo flows are stateless (they echo). A real resource would `invoke` a
service that persists — e.g. through a database connector — while keeping the
routing/shape here as the editable business surface.

## Richer OpenAPI (optional)

A flow may declare typed schemas as UPPERCASE literals — they feed the generated
spec (and stay editable business surface):

```
API_REQUEST  = {customer: "string", total: "number"}   # -> requestBody schema
API_RESPONSE = {id: "string", status: "string"}         # -> 200 response schema
```

Field types map to JSON Schema (`string`, `number`, `integer`, `boolean`,
`array`, `object`; unknown → `string`). Operations are tagged by their resource
(the first path segment), so tools group them per resource.
