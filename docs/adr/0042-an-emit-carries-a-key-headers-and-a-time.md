# 0042 — An emit carries a key, headers and a time, and a flow reads those of its message

- Status: Accepted, not built yet. The founder chose the syntax of points 1 and 3 (2026-11-27).
- Date: 2026-11-27

## Context

`emit` sends a payload to a subject, and nothing else. The systems behind a
sink often need more than a body:

- A Kafka record has a key, which picks its partition, and so the order its
  consumers see and what compaction keeps. It has headers, such as an event
  type or a correlation id, and a timestamp.
- An AMQP message has application headers and a timestamp property.
- An IBM MQ message has properties.
- An HTTP call has request headers.

A flow sets none of these today. A connector's settings can fix values, such
as the `HEADERS` of `http-out` or the AMQP routing key, the same for every
message. So a flow cannot feed a topic compacted by key, nor a consumer that
routes on a header.

The reverse holds too. A bus flow cannot read the key, the headers or the
time of the message it runs. A correlation id that came in a header is lost
at the first flow, and with it the thread a business follows from one system
to the next.

The bus can carry them. A NATS message has headers, which the runtime already
uses for the trace (ADR-0034) and for replays (ADR-0015).

## Decision

### 1. An emit takes options

```
emit "vx.orders.priority", order, {key: customerId, headers: {eventType: "order.accepted", correlationId: corr}, time: receivedAt}
```

- The third part is optional. It is a doc with three optional fields:
  - `key`: a text. A number becomes its text. Null or absent, the message
    has no key.
  - `headers`: a doc of names and values. A value is a text; a number or a
    boolean becomes its text, as `str()` writes it; null sets no header.
  - `time`: when the event happened. Milliseconds since 1970-01-01 UTC, or
    an RFC 3339 text with its offset (`2026-11-27T10:15:00.123Z`), which the
    runtime turns into milliseconds.
- A header name is a token as HTTP defines it: letters, digits and a few
  marks such as `-`, `_` and `.`. Whatever their case, the names that start
  with `Nats-` or `Vejas-`, `traceparent` and `tracestate` are the bus's and
  the runtime's, and a flow cannot set them. A value holds no line break.
- `vjs-check` refuses a literal options doc with another field or a reserved
  name. When the options are computed, one the runtime cannot use fails the
  run, as any statement that cannot run does.
- The options are a doc like any other: they can be built first,
  `meta = {key: id}`, and then given, `emit "vx.out", body, meta`.
- An `api` flow's emits take them the same way.

### 2. On the bus

- They travel as NATS headers: the key as `Vejas-Key`, the time as
  `Vejas-Time`, in milliseconds written in decimal, and each header under its
  own name.
- The payload does not change. A flow or a sink that ignores them sees the
  message as before.
- They count toward the server's limit on a message, as the payload does. The
  trace headers give way first, as today (ADR-0034). A message still too
  large is refused, and the run fails as it does today for a payload too
  large.

### 3. A bus flow reads them in `envelope`

- A flow that reads `envelope` gets `{subject, key, headers, time}` of the
  message it runs. `key` and `time` are null when the message has none.
  `headers` holds the message's own, every header but the bus's and the
  runtime's, with their names as sent. A header sent twice is joined with
  `, `, as `request.headers` is (ADR-0039).
- It is found statically, as `unreadable` is (ADR-0039): a flow that does not
  read it pays nothing.
- `envelope` is the runtime's name. An event field of that name stays
  readable as `event.envelope`.
- An `api` flow has `request` (ADR-0039), and no `envelope`.
- Nothing is forwarded on its own. A flow passes on what it was given by
  naming it: `emit "vx.next", out, {key: envelope.key, headers: envelope.headers}`.

### 4. Connectors map them where their protocol has a place

- The connector crate (ADR-0038) gives every driver the same reading of a
  consumed message's key, headers and time, and one publish that takes them,
  so that every connector maps them alike.
- Sinks:
  - `http-out` sends the headers as request headers. A header its manifest
    sets wins, so that a message cannot replace a credential. The headers
    that frame a request (`Host`, `Content-Length`, `Transfer-Encoding`,
    `Connection`) are never taken from a message.
  - AMQP sends the headers as application headers, and the time as the
    `timestamp` property, in seconds as AMQP counts it. The key becomes the
    routing key only when the connector's settings ask for it, since the
    routing key decides where the message goes.
  - IBM MQ sends each header as a message property. Its message descriptor
    has no place for a key or an event time: a flow that needs them there
    sets them as headers.
  - Kafka has a place for all three. The `kcat` recipe (ADR-0022) cannot pass
    them per message. A connector that speaks Kafka itself would, and is a
    decision of its own.
  - `exec-sink`, `mqtt-out` (MQTT 3.1.1 has no headers) and `slack-out` send
    the body alone, as today.
- Sources do the reverse:
  - `http-in` puts on the bus the request headers an `api` flow would be
    given (ADR-0039): the six that say what the request is and those its
    settings name, never one whose name reads as a credential.
  - AMQP gives the application headers, and the `timestamp` property as the
    time.
  - IBM MQ gives the message properties.

### 5. Traced end to end

- The trace record shows each emit's key, headers and time beside its
  subject, and the envelope a run was given beside its event. They are masked
  as the payload is (ADR-0017).
- A dead letter keeps the key, the headers and the time of its message, and
  its replay sends them back: the second chance runs on the message as it
  came.
- A shown run (a preview, `vejas_run_flow`, time travel, the canary) lists
  each emit's options. A recorded event replays with its envelope.

### 6. Not decided here

- Order by key across instances. The key it would use is this one.
- An id on an emit, for the bus to drop a duplicate.
- A clock in the language.

## Building it

In this order, each step with its tests and an end-to-end suite:

1. The language, the bus, the trace record, the dead letters, the connector
   crate, `http-out` and `http-in`.
2. The AMQP and IBM MQ connectors, both ways.

## Consequences

- A flow can feed a topic compacted by key, a consumer that routes on a
  header, or an API that wants its own headers, without a connector written
  for that shape.
- A correlation id crosses flows and connectors in a header, beside the
  trace.
- The payload stays the message. Nothing is added to it, and nothing changes
  for what ignores the options.
- The language gains one name, `envelope`, and an optional third part to
  `emit`.

## Alternatives considered

- **Fields in the payload that a sink lifts out** (`_key`, `_headers`). The
  payload stops being the message: every other reader must strip them, and a
  business field of that name would be taken for an option.
- **A key field named in the sink's settings** (`KEY_FIELD = "customerId"`).
  Enough for a key read from the body, not for headers or a time the flow
  computes. And the choice leaves the flow, which knows why.
- **The bus's headers, raw, in the language.** Flows would be tied to NATS,
  and a flow could set the bus's own headers, such as the one that drops
  duplicates.
- **`message` as the name.** Events often have a field of that name, a log
  line or an alert: a flow that reads its event's `message` would have read
  the runtime's instead.
