# VejasScript

The language reference below is extracted from the runtime's own
`vejas_language` contract — the exact text every agent reads before writing
a flow. It is the source of truth; if this page and the tool ever disagree,
the tool wins.

```text
VejasScript in 20 lines:
  # comment
  source "vx.domain.name"            <- the flow's input subject, REQUIRED, line 1
  SEVERITY_CODES = {"critique": "P1", "haute": "P2"}   <- UPPERCASE literal dicts are transcoding tables the business expert edits
  ALERT_LEVELS = ["P1", "P2"]        <- UPPERCASE literal lists/scalars are editable constants
  x = priority                       <- the incoming event's top-level fields are variables; `event` is the whole document
  code = SEVERITY_CODES[priority] ?? "P3"
  email = lower(requester?.email)    <- builtins: upper lower trim len str num split join replace round abs; ?. is null-safe
  ids = orders[].id                  <- array projection
  big = orders[total > 100]          <- array filtering
  out = out + [{sku: l.sku}]         <- array concatenation builds lists inside a for
  fact = {source: "graph", in: 2}    <- doc keys and .field names may be ANY word, keywords included
  invoke format_alert(sev: code)     <- compose a service from services/<name>.vjs; its outputs MERGE into this pipeline
  d = invoke format_alert(sev: code) <- or capture its whole pipeline as a document
  invoke pkg:svc(k: v)               <- cross-package composition (the target package must list svc in its EXPORTS)
  key = secret("slack/webhook")      <- credentials resolve from the Vault at run time; NEVER a literal
  if code in ALERT_LEVELS:
      emit "vx.slack.out", {text: f"[{code}] {subject}"}
  end                                <- every if/for closes with `end`

Exposing a flow (instead of, or besides, `source`):
  tool "what calling this flow does" <- exposes the flow as an MCP tool
  api "POST /orders"                 <- expose the flow as a SYNCHRONOUS HTTP endpoint under /api (POST /api/orders)
  api "GET /orders/{id}"             <- a REST resource = several flows, ONE per verb; {path params} become event variables (here `id`)
  API_REQUEST = {customer: "string", total: "number"}   <- optional: typed request schema for the generated OpenAPI
  API_RESPONSE = {id: "string", status: "string"}       <- optional: typed 200 response schema
  respond 201, {id: id, status: "created"}   <- the SYNCHRONOUS HTTP response (status code + JSON body); `emit` still fires bus side-effects

Rules:
- Known sinks: vx.slack.out (payload {text: "..."}). All subjects start with "vx.".
- Put every business-meaningful value (thresholds, tables, queue names) in UPPERCASE literals.
- A flow file's first line is `# flow: <snake_case_name>`; it lives under flows/ (or packages/<pkg>/flows/).
- Its sample input lives at flows/fixtures/<flow>.json (or packages/<pkg>/fixtures/) — one JSON event.
- A flow is triggered by ONE of: `source "vx…"` (bus), `tool "…"` (MCP), or `api "VERB /path"` (HTTP). An `api` flow answers with `respond <status>, {body}`; the request's JSON body, {path params} and `query` are all in the event. The whole API is described at GET /api/openapi.json.
- A connector manifest's first line is `# connector: <name>`, then `driver "<name>"` (catalog: vejas_drivers) and UPPERCASE literal config; any credential uses secret("path/key"), never a literal.
```

## Design notes

- **Pure by construction** (ADR-0001): no I/O, clock, or network exists in
  the language. Effects happen only at the edges (`emit`, `respond`) —
  which is what makes replay, time-travel and canary structurally safe.
- **UPPERCASE literals are the contract with humans**: anything a domain
  expert may want to change belongs in one
  ([the business surface](../concepts/business-surface.md)).
- **Files are the deployment unit**: `flows/<name>.vjs`,
  `connectors/<name>.vjs`, fixtures next to them, golden tests under
  `tests/vjs/`. Git is the source of truth; the runtime supervises the
  directory.
