# 0032 — `invoke` calls any service: code, or a connector that answers

- Status: Accepted (partial). `http-rpc` is built: the call, its retries as deliveries, its dead letters, the `api` flow's 502 and 504, the trace carried on. A shown run refuses the call; recording answers and replaying them is not built.
- Date: 2026-11-26

## Context

**A flow cannot read a system.** A flow computes with builtins that never
leave the process, `invoke`s a VejasScript service that sees only its
arguments (`run_service` in `core/src/vjs.rs` sets the service's pipeline
to them), `emit`s to the bus and, in an `api` flow, `respond`s. Nothing in
it can ask an API or a database a question and use the answer while it
handles an event.

**Integration is mostly that question.** Receive, validate, enrich from an
API or a database, decide, publish. A bench on a realistic case stops at
the read: a webhook of business events, filtered, routed and validated,
then the object's current state read from an API, its status mapped
through a business table, and the result published or rejected. Of the
bench's 14 cases, 3 pass with Vejas alone. The read blocks 9, and a
service that answers would turn 7 of them green on its own. With an exec
script standing in for the read, 12 pass, at about 120 ms a message
against 1 to 2 ms for a flow hop.

**The connectors that reach systems do not answer a flow.** `http-out`
posts a message and drops the response. `exec-rpc` answers over NATS
request/reply, but only the SAP tools of the MCP server call it. ADR-0038
puts request/reply serving in the connector crate's host, with no caller
in a flow.

**The documentation reads `invoke` as composing pure logic** ("same
language, same purity", `concepts/composition.md`). The founder rejected
that reading on 2026-11-26: calling a service is the central act of an
integration flow, and `invoke` must call any service, whatever runs it. A
separate `call` statement, accepted in an earlier evaluation on
2026-10-22 and never built, is dropped for that reason.

**What purity bought must hold.** A preview on a sample, a shadow replay
(ADR-0018), time travel and the canary (ADR-0021) and a golden case run a
flow again and give the same answer. A shadow run never reaches a real
system and never holds a real secret (ADR-0034).

## Decision

1. **One verb.** `invoke name(k: v)` merges a service's outputs into the
   flow, `x = invoke name(k: v)` captures them, as today. A service is a
   file in a package's `services/`, resolved and exported as today
   (package scope, `EXPORTS`). It is either code, VejasScript as today, or
   the declaration of a connector that answers, written like a
   connector's manifest. The flow reads the same either way, and a service
   can move from one to the other without touching its callers.

   ```
   # services/get_order.vjs: the current state of an order
   driver "http-rpc"
   BASE_URL = "https://orders.example"
   METHOD = "GET"
   URL = "{BASE_URL}/orders/{id}"
   HEADERS = {Authorization: secret("orders/token")}
   TIMEOUT_MS = 5000
   ATTEMPTS = 3
   DELAY_MS = 500
   ```

   The flow then writes `o = invoke get_order(id: data.orderId)` and reads
   `o.status` and `o.body`. In `URL`, `{name}` takes the argument `name`,
   URL-encoded, or the declaration's constant `name`. An argument `body` is
   sent as the JSON body. An argument used nowhere is an error when the
   flow is checked. The declaration's constants are business literals
   (ADR-0016), so `BASE_URL` can change live like any other.

2. **What comes back.** For `http-rpc`, `{status, body, headers}`, with
   `body` parsed when the response is JSON and a text otherwise. A status
   listed in `ANSWERS` is an answer. The default is any 2xx; a service for
   which "not found" is an answer the flow decides on lists it,
   `ANSWERS = [200, 404]`.

3. **A failure is the message's, and its attempts are its deliveries.**
   An invoke tries once. If it fails, the run fails with an error that
   names the service and the cause (`get_order: HTTP 503`,
   `get_order: timeout after 5000 ms`, `get_order: network: …`), and says
   whether it can be retried. By default, 429, 5xx, a timeout and a
   network error can be retried; `RETRY_ON` changes the list. The message
   then takes the path of ADR-0015:
   - a failure that can be retried is redelivered after `DELAY_MS`, while
     the message has had fewer deliveries than the service's `ATTEMPTS`,
     and never past `MAX_DELIVERIES`;
   - another failure, or the last attempt, is a dead letter, whose
     `last_error` is the invoke's error, replayed by an operator once the
     cause is fixed.

   A run's emits leave only when it succeeds, all of them or none: an emit
   the bus refuses takes the others of its run back out of the batch. (The
   bench found that they left, at every delivery.) Retrying the message
   therefore retries the call, with no second retry loop inside the run,
   and no consumer thread waits out a delay. Each attempt is a delivery,
   so the trace and the panel's Activity already count and show it. A
   business reject stays the flow's own decision: it reads the answer and
   emits.

   An `api` flow has no message to redeliver. A failed invoke answers its
   caller 502 with the error, or 504 after a timeout, and the caller
   retries.

4. **Traced, recorded, then replayed.** The call carries the run's trace
   context (`traceparent`), so a system that reads it continues the trace,
   and the call is a span under the hop. Each answer and each failure of a
   connector-backed service is recorded with the run in the trace record
   (ADR-0034), masked as the record masks a payload and capped as a
   payload is.

   A shadow run never calls a system. This covers a preview on a sample,
   `vejas_run_flow`, a replay, time travel, the canary and a golden
   capture. In a shadow run, an invoke of a connector-backed service reads
   the answer recorded for the same service and the same arguments in the
   event's recorded run, in the order they were made. It reads it masked,
   as it reads the event (ADR-0034), and the result's `hidden` names the
   fields of the answer it could not see. A sample and a golden case carry
   their answers next to their input.

   When nothing is recorded, the invoke fails in the shadow run with `not
   recorded: get_order(id: …)`, and the result names it. That happens to a
   candidate that calls with other arguments, an answer past the cap, or a
   record turned off or rotated. ADR-0018 and ADR-0021 then hold as "the
   same answer from the same recordings", the nuance ADR-0031 gave detect
   units.

5. **Where it runs.** `http-rpc` runs in the runtime, on the HTTP client the
   binary already holds (ADR-0038's rule: a pure-Rust protocol driver of
   about 1 MB or less). A driver in its own process, SAP through
   `exec-rpc` or a client that links a C library, answers over NATS
   request/reply through the host of ADR-0038, with the same timeout,
   errors and recording. The order is:
   - `http-rpc` first;
   - then `exec-rpc` as a service backend, for SAP;
   - then SQL, with the database connector.

6. **Governance stays as it is.** The write gate already counts an
   `invoke` as a write, so a flow that gains a connector-backed service
   needs approval in governed mode, as one that gains an `emit` does. A
   declaration holds its credentials through `secret()`. The flow never
   sees them, and a shadow run never resolves them, since it never calls.

7. **Measured before it grows.** Before a second driver, the cost of an
   invoke of a local `http-rpc` service and of an `exec-rpc` one, with
   recording on, on the bench of ADR-0034.

## Consequences

- Enrichment, lookups and reads become steps of one flow. The business
  view sees one flow, not a request flow, a connector and a reply flow.
- A call is made at least once. When a later step of the run fails, the
  next delivery makes the call again. A read does not mind. A service that
  changes something must be idempotent, with a key the flow passes, such
  as the event's id.
- A message that waits out `DELAY_MS` lets later messages of its flow pass
  it.
- A flow handles its messages one at a time, so the latency of what it
  calls bounds its throughput: about 50 messages a second against a 20 ms
  API. Competing consumers (ADR-0020) spread a flow across instances.
- A flow's run time and failures now depend on the systems it calls. A
  slow API slows the flows that invoke it, within `TIMEOUT_MS`.
- A replay reruns a flow against recordings. A candidate that changes what
  it calls cannot be previewed in full, and its result says what was not
  recorded.
- The trace record grows by the answers it keeps.
- A sample needs recorded answers before a preview can run a flow that
  invokes a system. The panel says which answer is missing.
- `concepts/composition.md` and `reference/vejascript.md` present `invoke`
  as calling a service, pure logic being one case.

## Alternatives considered

- **A `call` statement next to `invoke`.** Accepted on 2026-10-22, never
  built. It gives two verbs for calling a service, and a flow changes when
  a service moves from code to a system. Rejected by the founder.
- **A choreography on the bus.** A flow emits a request, a connector
  answers on another subject, and a second flow continues. That is
  possible today with exec connectors. One business step then becomes two
  flows, a correlation and a timeout written by hand, and a replay cannot
  follow it.
- **Builtins that reach systems** (`http_get(…)`). Every flow would hold
  URLs and credentials, and the gate could not tell a pure builtin from one
  with an effect.
- **Retries inside the run.** A loop with delays in the invoke would
  multiply with the redeliveries of ADR-0015 (three attempts times five
  deliveries), block the consumer during the delays, and hide the attempts
  from the trace.
- **Calling the real system in a shadow run for safe methods (GET).** A
  replay would load production systems, its answer would change with them,
  and what it sends could carry a value from the event.

## Not decided here

- `fail` and `try`/`catch`, to tell a business reject from a technical
  failure inside a flow.
- An idempotent receiver: a duplicate check, marked only after the
  message's emits are published.
- Running several messages of one flow at once, in one instance.
- A message key and headers on `emit`.
