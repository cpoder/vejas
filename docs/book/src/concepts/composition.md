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
- The settings are the service's own. An argument may not have the name of
  one, so a flow cannot choose the host its credentials go to. An argument
  may not make a segment of the path empty, `.` or `..`, which would reach
  another resource; in the query, any value goes. A setting goes in as written, so
  a secret placed in the query must hold only characters a URL takes as
  they are.
- `vejas-runtime vjs-check` reports an argument the service does not take,
  when the declaration writes its `URL` as a literal. So does a preview.
- The answer is `{status, body, headers}`, with the arguments the flow
  passed, as a service of code gives them back. `body` is parsed when it is
  JSON and is text otherwise; header names are in lower case.
- A status listed in `ANSWERS` is an answer; by default, any 2xx.
  `ANSWERS = [200, 404]` lets the flow decide what a 404 means. A call
  follows no redirect, which would take the service's headers elsewhere: a
  3xx is an answer only if `ANSWERS` lists it, and the flow reads
  `location`.
- Any other status, a timeout (`TIMEOUT_MS`, 10 s by default) or a network
  error fails the run, with an error that names the service and the cause:
  `get_order: HTTP 503`. A failure that may change later (429, a 5xx, a
  timeout, the network; `RETRY_ON` lists other statuses) means the system is
  down: a flow on the bus **holds** its message until it answers (below). A
  failure no later try changes (another 4xx) is a
  [dead letter](../guides/dlq-replay.md) at once, replayed once the cause is
  fixed. A run's emits leave only when it succeeds, so a message tried again
  emits once.
- `WHEN_DOWN = "dead-letter"` keeps the path a call took before the hold:
  the message comes back after `DELAY_MS` (1 s, at most an hour), until it
  has been delivered `ATTEMPTS` times (3), and never past the five
  deliveries of any message; then it is a dead letter. A secret store that
  does not answer when the declaration is read takes that path whatever the
  service says.
- A flow that knows what such a failure means writes the call in a `try`:
  its `catch` runs instead of the dead letter (ADR-0039): at once when the
  service does not try again; for a service that says `"dead-letter"`, on
  the delivery that spends the attempts; for one that holds, when a person
  releases the held message. `e.status` holds the status the system
  answered, null when it gave none.
- The error names no URL and no header, and is masked as the trace record
  masks one, so a credential reaches no log, span, answer or dead letter.
- A flow that calls a system takes its messages one by one: it asks for the
  next once it has acked the last. Its messages then wait in no queue while
  calls run. Keep a run's calls, `TIMEOUT_MS` each, under the consumer's
  `ack_wait` (30 s): a message whose run outlives it is delivered again. Its
  own loop acks that copy without running it; another instance runs it. A
  flow that only computes takes them 64 at a time.
- A flow on the bus reads the declaration again at most once a second: an
  edit of it, or a rotated secret, is picked up within that time. An `api`
  flow reads it at each request.
- `TIMEOUT_MS` bounds the connection, TLS and the answer. The name lookup is
  outside it.
- In an `api` flow, a failed call answers the caller 502, or 504 after a
  timeout.
- The call carries the message's trace (`traceparent`): a system that reads
  it continues the trace.
- A call is made at least once. When a later step of the run fails, the
  message's next delivery calls again. A service that changes something must
  be idempotent, with a key the flow passes, such as the event's id.
- A run that is shown (a preview, `vejas_run_flow`, a replay, time travel,
  the canary, a golden capture) calls no system: such an invoke fails there
  and says why. A replay, time travel and the canary leave out an event whose
  two runs reach the same call, and count it as `not_recorded`. A candidate
  that calls with other arguments, calls another service or fails before
  its call has changed what the event does, and counts as changed. Reading the answers
  recorded with the event instead is the next step of ADR-0032.
- The credentials stay in the declaration, through `secret()`; the flow
  never sees them.


## While a system is down: the hold

A declared service holds by default (`WHEN_DOWN = "hold"`, ADR-0040). When a
run fails on a call a later try may change, the flow keeps the message in
hand instead of sending it back:

```
driver "http-rpc"
BASE_URL = "https://payments.example"
URL = "{BASE_URL}/payments/{id}"
HEALTH_URL = "{BASE_URL}/health"
ATTEMPTS = 3
DELAY_MS = 1000
PROBE_MAX_MS = 60000
```

- The run is tried again in hand, `DELAY_MS` apart, up to `ATTEMPTS` runs.
  Nothing is announced: a blip costs a delay.
- Then the flow is **held**. It probes after `DELAY_MS`, then twice as long
  each time, up to `PROBE_MAX_MS` (60 s by default). With `HEALTH_URL`, a
  probe is a GET that answers 2xx when the system is up, with the service's
  headers; without it, the probe is the held message's run itself, which
  calls again what the run called before the failing call.
- When the system answers, the held message runs first, then the ones
  behind it, in the order they came. No message behind it was called during
  the outage, and none became a dead letter.
- A failure no later try changes ends the hold for that message: its catch
  runs, or it is a dead letter, and the flow goes on.
- Probes are not deliveries: neither the five-delivery cap nor a catch's last
  delivery fires during a hold. The message is told to the bus as still in
  progress while it waits.
- A flow that calls a service that holds takes one message at a time across
  all instances (`max_ack_pending = 1` on its consumer): while one holds a
  message, the others wait. A flow that calls such a system for only some of
  its messages holds the others too: that is the price of order. Such a
  service says `WHEN_DOWN = "dead-letter"`.
- A stop gives the held message back to the bus at once, first in line for
  the next instance.
- What is seen: the flow's `status` is `held` and its `hold` in
  `GET /topology` and `vejas_topology` gives the service, since when, the
  last error, the runs, the probes, the next probe and the messages behind.
  `held`, `resumed`, `failed`, `released` and `stopped` are published on `vx.flow.events`, an
  ordinary subject, as dead letters are on `vx.dlq.events`. `/metrics` has
  `vejas_flow_held{unit,service}` and `vejas_hold_probes_total{unit}`. The
  trace record of the run that ends the hold carries `vejas.hold.ms`, its
  runs and its probes.
- A person can `POST /hold/release` with `{"flow": "flow:pay"}`: the held
  message runs once more, the held service answers it with the failure it
  gave and is not called, so its catch runs now, or it is a dead letter.
  Calls the flow makes before that one are made again: a service that
  changes something must take the same call twice. `POST /hold/probe` has
  the flow probe now. Both take the write token, are human acts in governed
  mode (the approval token, as a DLQ replay), and are written to the audit
  with who asked. They are answered once the flow took them, between two
  probes; a flow that holds nothing on this instance, or whose hold ended
  meanwhile, answers 409, and an act the audit could not record answers 500.
- A probe that ends in a failure no later try changes (a 404) ends the hold:
  `failed` on `vx.flow.events`, and its catch runs or it is a dead letter. A
  stop announces `stopped`. While held, the flow probes the service that
  failed last.
- The message stays in progress for the bus (`+WPI`, every third of the
  `ack_wait`) from its first failure until it is settled, through the runs
  and the probes. A flow that took its messages in rounds and holds for the
  first time settles the runs of the round before and gives the others
  back.
- An `api` flow is never held: it answers at once, 502 or 504, or its catch
  runs. A run that is shown makes no call.
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
