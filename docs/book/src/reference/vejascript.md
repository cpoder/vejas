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
  email = lower(requester?.email)    <- builtins: upper lower trim len str num split join replace round abs base64; ?. is null-safe
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
  end                                <- every if/for/try closes with `end`
  try:
      o = invoke get_quote(id: id)   <- a failure no later delivery may change runs the catch, not a failed run
  catch e:                           <- e = {error, service, status, attempts}; `catch:` when the flow does not read it
      emit "vx.quote.rejected", {id: id, status: e.status, why: e.error}
  end
  if seen(event.key):                <- true when a run of this flow remembered the key and its window has not passed
      emit "vx.quote.duplicate", {key: event.key}
  else:
      remember event.key for "7d"    <- marks the key once the run's emits are stored
  end

Exposing a flow (instead of, or besides, `source`):
  tool "what calling this flow does" <- exposes the flow as an MCP tool
  api "POST /orders"                 <- expose the flow as a SYNCHRONOUS HTTP endpoint under /api (POST /api/orders)
  api "GET /orders/{id}"             <- a REST resource = several flows, ONE per verb; {path params} become variables (here `id`)
  API_REQUEST = {customer: "string", total: "number"}   <- optional: typed request schema for the generated OpenAPI
  API_RESPONSE = {id: "string", status: "string"}       <- optional: typed 200 response schema
  respond 201, {id: id, status: "created"}   <- the SYNCHRONOUS HTTP response (status code + JSON body); `emit` still fires bus side-effects

Rules:
- Known sinks: vx.slack.out (payload {text: "..."}). All subjects start with "vx.".
- Put every business-meaningful value (thresholds, tables, queue names) in UPPERCASE literals.
- A flow file's first line is `# flow: <snake_case_name>`; it lives under flows/ (or packages/<pkg>/flows/).
- Its sample input lives at flows/fixtures/<flow>.json (or packages/<pkg>/fixtures/) — one JSON event.
- A flow is triggered by ONE of: `source "vx…"` (bus), `tool "…"` (MCP), or `api "VERB /path"` (HTTP). An `api` flow answers with `respond <status>, {body}`; `event` is the request's JSON body as sent (an object, an array, or null when there is none), {path params} are variables, and `request` holds {method, path, params, query, headers} (`headers`: accept, content-type, idempotency-key, user-agent, x-correlation-id, x-request-id, and those an `API_HEADERS` list names, in lower case, never one whose name reads as a credential, in any spelling; the trace record masks the value of each header the list names, where the request shows it). A path parameter named `event` or `request` is read as `request.params.<name>`, and a body field named `request` as `event.request`. A body that is not JSON answers 400. The whole API is described at GET /api/openapi.json.
- A message that is not JSON is a dead letter, unless the flow reads `unreadable`: its event is then {unreadable: {error, text, base64}}: `text` when the bytes are UTF-8 text, `base64` when they are not, the other null. An `api` flow that reads it takes such a body instead of answering 400. A JSON event with its own `unreadable` field sets the variable too. `unreadable.base64 ?? base64(unreadable.text)` is the message in base64 whatever its bytes are, since `base64()` gives a text's UTF-8 bytes. To forward the bytes, keep them under a key that ends in `base64` (`raw_base64`): the masks read base64 under such a key only.
- A string is in double quotes and reads JSON's escapes: `\n`, `\t`, `\r`, `\"`, `\\`, `\/`, `\b`, `\f`, and `\u` with four hex digits. In an f-string, `\{` and `\}` give a brace, as `{{` and `}}` do. Any other escape is an error.
- An `emit` subject must be a valid subject: no whitespace, no control character. A subject built from an event field is checked when the flow runs, and a value that breaks the rule fails the run, so the message reaches the DLQ after its deliveries. Map such a field through a table rather than trusting it.
- `secret()` never yields the value where a run is SHOWN instead of performed (preview, vejas_run_flow, flow-as-tool, shadow-replay, time-travel, canary, golden capture): it stays fail-closed and evaluates to the placeholder `<secret:path/key>`, so a branch comparing with a secret takes the not-equal path there.
- A service in `services/` is VejasScript, or the declaration of a system that answers: `driver "http-rpc"`, `URL = "{BASE_URL}/orders/{id}"` and its settings (see Composing services). `o = invoke get_order(id: x)` then gives `{status, body, headers}` with the arguments, and a call that gets no answer fails the run with the service's name and the cause. Every argument but `body` fills a `{name}` of the URL, and none takes the name of a setting.
- `try:` ... `catch e:` ... `end`: the catch takes a failure no later delivery may change: an error in the code, a call the service does not try again, or a call on the delivery that spends the service's `ATTEMPTS` (5 deliveries at most). Before that the run fails and the message comes back after the service's delay, as without `try`. A `secret()` or a `seen()` whose store does not answer is a failure a later delivery may change: the catch takes it on the fifth delivery. An `api` flow's request has no later delivery: its catch takes the failure at once. In `e`, `service` is null for an error in the code, `status` is null when the system gave no answer, and `attempts` is the delivery's number (1 for a request). What the `try` emitted, remembered or responded before the failure is dropped; the variables it set stay. A failure in the catch fails the run, or goes to the `try` around it. A run that is SHOWN makes no call, so its catch takes no call's failure.
- `seen(key)` is true when a run of this flow said `remember key for "<window>"` and the window has not passed. The mark is written once the run's emits are stored: a run that fails marks nothing, so a message that comes back is processed again, and a catch marks only what it remembers. The window is a number and s, m, h or d, up to 3650d, written in the statement or in an UPPERCASE constant. A key is any value but null; a number or a document is read as its JSON text. A copy of a message finds the mark of a run before it, also in the same batch, but two copies that run at the same instant, on two instances or as two requests, can both pass. Where a run is SHOWN, `seen` is false for every key. `remember` is a write: a read-method `api` flow that remembers needs the token, as one that emits. A flow whose memory does not answer fails its run, as a call that may answer later does (an `api` flow answers 503).
- A connector manifest's first line is `# connector: <name>`, then `driver "<name>"` (catalog: vejas_drivers) and UPPERCASE literal config; any credential uses secret("path/key"), never a literal.
```

## Design notes

- **Pure by construction** (ADR-0001): no I/O, clock, or network exists in
  the language. Effects happen only at the edges (`emit`, `respond`,
  `remember`), and what a run reads beyond its event (a system's answer
  through `invoke`, `seen`) is the runtime's: a run that is shown reads
  neither, which is what makes replay, time-travel and canary structurally
  safe.
- **UPPERCASE literals are the contract with humans**: anything a domain
  expert may want to change belongs in one
  ([the business surface](../concepts/business-surface.md)).
- **Files are the deployment unit**: `flows/<name>.vjs`,
  `connectors/<name>.vjs`, fixtures next to them, golden tests under
  `tests/vjs/`. Git is the source of truth; the runtime supervises the
  directory.
