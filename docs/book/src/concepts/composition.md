# Composing services

A flow does one job end to end. When it needs something it does not hold, it
calls a **service** with `invoke`: a piece of logic several flows share
(format an alert, classify a ticket, shape a payload), or a system that
answers (the current state of an order, from an API). The flow writes the
call the same way in both cases, and a service can move from one to the
other without touching the flows that call it.

## A service is a flow without a trigger

A service lives in `services/<name>.vjs` (or `packages/<pkg>/services/`). It
looks like a flow but declares no `source`, `api`, or `tool` — it never runs
on its own. It receives named arguments, and produces variables. Real
example (`services/format_alert.vjs`):

```
# service: format_alert — Inputs: sev, subj, email. Output: alert_text.
alert_text = f"[{sev}] {subj} - {lower(email)}"
```

A service written in VejasScript is pure, and everything true of a flow
holds: its literals are part of the [business surface](business-surface.md),
it is statically analyzable, and it has no I/O of its own.

## A service that answers from a system

A service can declare a connector instead of holding code (ADR-0032).
`services/get_order.vjs`:

```
driver "http-rpc"
BASE_URL = "https://orders.example"
URL = "{BASE_URL}/orders/{id}"
HEADERS = {Authorization: f"Bearer {secret("orders/token")}"}
TIMEOUT_MS = 5000
ATTEMPTS = 3
DELAY_MS = 500
```

A flow invokes it as it invokes any service, and reads what came back:

```
o = invoke get_order(id: data.orderId)
emit "vx.orders.ready", {id: data.orderId, state: o.body.status}
```

- `{name}` in `URL` takes the argument `name`, percent-encoded, or the
  setting `name` as written. Every argument must appear in the URL, except
  `body`, which is sent as the JSON body. `METHOD` is `GET` unless set.
- The answer is `{status, body, headers}`. `body` is parsed when it is JSON
  and is text otherwise; header names are in lower case.
- A status listed in `ANSWERS` is an answer; by default, any 2xx.
  `ANSWERS = [200, 404]` lets the flow decide what a 404 means.
- Any other status, a timeout (`TIMEOUT_MS`, 10 s by default) or a network
  error fails the run, with an error that names the service and the cause:
  `get_order: HTTP 503`. A failure that may change later (429, a 5xx, a
  timeout, the network; `RETRY_ON` lists other statuses) brings the message
  back after `DELAY_MS` (1 s), until it has been delivered `ATTEMPTS` times
  (3), and never past the five deliveries of any message. Any other failure,
  or the last attempt, is a [dead letter](../guides/dlq-replay.md) at once,
  replayed once the cause is fixed. A run's emits leave only when it
  succeeds, so a message tried again emits once.
- In an `api` flow, a failed call answers the caller 502, or 504 after a
  timeout.
- The call carries the message's trace (`traceparent`): a system that reads
  it continues the trace.
- A call is made at least once. When a later step of the run fails, the
  message's next delivery calls again. A service that changes something must
  be idempotent, with a key the flow passes, such as the event's id.
- A run that is shown (a preview, `vejas_run_flow`, a replay, time travel,
  the canary, a golden capture) calls no system: such an invoke fails there
  and says why. Reading the answers recorded with the event instead is the
  next step of ADR-0032.
- The credentials stay in the declaration, through `secret()`; the flow
  never sees them.

## `invoke` — merge, or capture

A flow calls a service with named arguments, two ways:

```
# merge: the service's outputs land in THIS flow's variables
invoke format_alert(sev: code, subj: subject, email: requester.email)
emit "vx.slack.out", {text: alert_text}          # alert_text came from the service

# capture: take the service's whole pipeline as one document
d = invoke format_alert(sev: code, subj: subject, email: requester.email)
emit "vx.slack.out", {text: d.alert_text}
```

Merge is the common case — the service extends the caller's pipeline.
Capture is for when you want the result as a self-contained document (to
nest, compare, or pass on).

## Across packages: `EXPORTS`

[Packages](../reference/vejascript.md) group flows and services. A service
is **package-private by default**. To call one from another package, the
owning package must export it in its `package.vjs`:

```
# packages/notifications/package.vjs
ENABLED = true
EXPORTS = ["notify_slack"]     # callable from other packages; the rest stays private
```

Then, from another package:

```
invoke notifications:notify_slack(channel: "#ops", text: alert_text)
```

## When to compose, and when to use the bus

- **`invoke`** when it's *synchronous shared logic* inside one processing
  step — formatting, classification, a lookup shape — or a question the step
  needs a system to answer: enrichment, a lookup. It runs in the caller's
  pipeline, same event, no persistence boundary.
- **The bus** (`emit` to a subject another flow `source`s) when it's a
  *separate stage* that deserves its own delivery guarantee, retry/replay, or
  independent scaling — the persisted, at-least-once path. Between packages,
  prefer the bus; `EXPORTS` is the deliberate exception for genuinely shared
  synchronous helpers.

A rule of thumb: if you'd want the step to survive a crash and replay on its
own, it's a bus hop, not an `invoke`.
