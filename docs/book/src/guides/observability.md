# Observability

Four windows into a running runtime, all built in — no sidecar, no agent
to install (ADR-0016): a **metrics** endpoint to scrape, **traces** to a
collector, the **trace record** that keeps each event's path in NATS for the
panel and your agent, and a live **event ring** to see what each flow just did.

## Metrics — Prometheus

`GET /metrics` is always on, plain Prometheus exposition:

```
vejas_up 1
vejas_units{kind="flow"} 4
vejas_units{kind="connector"} 3
vejas_flow_restarts{unit="order_sync"} 0
```

Gauges (up, supervised units by kind, restarts per unit) come live from the
supervision registry; the flow hot path adds counters and latency
histograms. Point your Prometheus at the panel port and you have unit
health, throughput, and error rate with no configuration.

## Traces — OTLP

Every event the runtime lets in gets one trace, and every hop it crosses adds
a span to that trace: the source or webhook that received it, each flow that
processed it, each `invoke` a flow made, and the sink that delivered it. The
context is the W3C `traceparent`. It travels in a NATS header on every
message the runtime publishes, so the payload is untouched and a consumer
outside Vejas can ignore it. The headers count toward the server's
`max_payload`: a message so close to the limit that they would push it over
goes without them, and its next hop starts a new trace. A trace is never the
reason a message fails.

The context also crosses the edges of the cell:

- `http-in` and `api` flows honour a `traceparent` sent by the caller, and
  the event's trace continues the caller's. `http-in` answers with the trace
  id: `{"published":"vx.orders","trace_id":"4bf92f3577b34da6a3ce929d0e0e4736"}`.
- `http-out` and `slack-out` send `traceparent` with their HTTP calls, and an
  `exec` sink hands it to its program as `TRACEPARENT` in the environment. A
  downstream system that traces hangs its spans under the sink's.
- A dead letter keeps the trace it died in, and a [DLQ replay](dlq-replay.md)
  continues it. The `dead-lettered` event on `vx.dlq.events` carries it too,
  so the flow that pages someone appears in the failing event's trace.
- MQTT has no headers before version 5: the trace stops at an `mqtt-out`
  sink and starts again at an `mqtt-in` source.

Each entry of the event ring carries its `trace_id`, so a row of **Recent
events** leads to its trace: in the panel's **Trace** card (see
[the trace record](#the-trace-record--one-event-hop-by-hop) below), or in your
tracing backend.

### Exporting spans

Set one variable and spans go to any OpenTelemetry collector or backend that
accepts OTLP over HTTP with JSON:

```
OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4318   # spans go to /v1/traces
OTEL_SERVICE_NAME=vejas                              # optional, the default
```

To see a trace within minutes, Jaeger alone is enough:

```
docker run --rm -p 127.0.0.1:16686:16686 -p 127.0.0.1:4318:4318 jaegertracing/jaeger:2.23.0
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 vejas-runtime
```

then open `http://localhost:16686` and pick the service `vejas`.

The exporter reads the other variables the OpenTelemetry specification
defines for it:

| Variable | Effect |
|---|---|
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | The full URL for spans, used as given; it wins over the generic endpoint |
| `OTEL_EXPORTER_OTLP_HEADERS` | `key=value,key2=value2` (values percent-encoded), for a backend that wants a token; `OTEL_EXPORTER_OTLP_TRACES_HEADERS` wins over it |
| `OTEL_EXPORTER_OTLP_TIMEOUT` | Milliseconds per export request, 10000 by default; `OTEL_EXPORTER_OTLP_TRACES_TIMEOUT` wins over it |
| `OTEL_RESOURCE_ATTRIBUTES` | Extra resource attributes, such as `deployment.environment=prod` |
| `OTEL_TRACES_SAMPLER`, `OTEL_TRACES_SAMPLER_ARG` | Sampling, see below |
| `OTEL_BSP_SCHEDULE_DELAY` | Milliseconds a span may wait for its batch, 1000 by default (the specification says 5000; a second shows a test event at once) |
| `OTEL_BSP_MAX_EXPORT_BATCH_SIZE` | Spans per export request, 512 by default |
| `OTEL_BSP_MAX_QUEUE_SIZE` | Spans waiting for export, 4096 by default |

Header values are credentials, and the runtime never writes them to its log;
an entry that HTTP cannot carry is skipped at start, with a warning that gives
its position only. The endpoint is logged without its user info and query.
On a graceful stop the runtime gives the exporter up to 3 seconds to send
what is still queued, so the spans of the last events before a restart reach
the backend.

**Sampling.** The default sampler is `parentbased_always_on`: every trace is
exported. Under load, `OTEL_TRACES_SAMPLER=parentbased_traceidratio` with
`OTEL_TRACES_SAMPLER_ARG=0.1` keeps one trace in ten. The hop that starts a
trace decides, and the decision travels in the `sampled` flag, so every hop
of a trace agrees with it; a hop that is sampled out builds no span. A failed
hop or a dead letter is exported whatever the sampler says; its ancestors may
then be missing from the backend, which is the known limit of deciding at
the head. With a parent-based sampler, a caller that sends `traceparent`
decides for its own requests. `traceidratio`, `always_on` and `always_off`
ignore the caller's flag, which suits an edge exposed to callers you do not
control.

**What a span holds.** A hop's span is named after its unit
(`flow:order_sync`, `connector:erp_out`). Its kind says what the hop was:
`SERVER` for `http-in` and an `api` flow, `PRODUCER` for a source, `CONSUMER`
for a flow, `CLIENT` for a sink. It carries the OpenTelemetry messaging
attributes (`messaging.system=nats`, `messaging.destination.name`,
`messaging.message.id`, the JetStream sequence) and the Vejas ones:
`vejas.unit`, `vejas.flow.version` (the content hash of the version that ran,
see [change safely](change-safely.md)), `vejas.flow.baseline` when a
promoted version overrides git, `vejas.delivery.attempt`, `vejas.emits`, and
`vejas.dlq=true` on a dead-lettered run. An `invoke` span is named
`invoke package:service`. A failure sets the span status to error, with
`error.message`.

**Loss is counted.** The export queue is bounded and the hot path never waits
for the collector. Connection errors and the answers 429, 502, 503 and 504
are retried with backoff, honouring `Retry-After`. When the queue is full, or
an export still fails after its retries, the spans are dropped and counted
on `/metrics`, and the log says so at most once a minute:

```
vejas_spans_exported_total 18342
vejas_spans_dropped_total{reason="queue_full"} 0
vejas_spans_dropped_total{reason="export_failed"} 0
```

**Cost.** Without an exporter no span is built; the context still travels,
since it is what puts the trace id in the event ring and in downstream calls.
On the isolated flow hop (`bench/trace-cost.sh`: 20 000 events, median of
five runs, the runtime's own CPU time per event), the hop took 72.5 µs
before tracing. The context alone adds about 3 µs (1.5 to 4.5 over four
series of runs). Exporting every span adds 10.5 µs (+14 %), and exporting one
trace in ten adds 4 µs (+5.5 %). Throughput stays within 2.5 % of the runtime
from before tracing. Resident memory grows by 0.7 MB,
and by 2.6 MB with the exporter and its queue. The details are on the
[benchmarks](../reference/benchmarks.md) page.

## The trace record — one event, hop by hop

The runtime also keeps the traces itself, with no backend to run. Each hop the
sampler keeps, and every hop that fails, is written to the JetStream stream
`VEJAS_TRACES` under the subject `vxtrace.<trace id>`, so one read returns a
whole trace. NATS stays the only infrastructure (ADR-0002).

- Panel: the **Trace** card. Open it from a row of **Recent events**
  (`⇢ trace`), or type a trace id, or anything the event carried: an order
  number, a customer id.
- API: `GET /traces/{trace id}` returns the hops in the order they started.
  `GET /traces?search=4711` returns the latest traces with a hop that
  mentions the text, newest first; it looks through the last 50 000 hops.
- Agent: `vejas_trace`, with `trace_id` or `search`. Asked "what happened to
  order 4711", an agent answers from the record.

A hop's record is its span, the one the exporter sends, plus what a span has
no room for:

```json
{
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
  "span_id": "5e1c0c3a77b4e2d1",
  "parent_id": "00f067aa0ba902b7",
  "unit": "flow:order_sync",
  "kind": "consumer",
  "start_us": 1764118847112345,
  "duration_us": 412,
  "ok": true,
  "error": null,
  "subject": "vx.orders.in",
  "emits": ["vx.orders.validated"],
  "input": {"order": "4711", "card_token": "<masked>"},
  "answer": null,
  "attributes": {"vejas.flow.version": "d5768a2de6e8cbb0", "vejas.delivery.attempt": 1}
}
```

- `parent_id` is the hop that published what this one consumed, or the
  caller's span for the first hop. The panel indents a hop under its parent.
- `input` is the payload the hop received; for a source, what it let in. A
  payload over 16 KB keeps its size and the start of its text, and the
  runtime copies no more of it than that.
- `answer` is what a sink's downstream or an `api` flow answered. It is
  scrubbed whole, then cut to 2 KB, so a JSON answer keeps its shape.
- Each failed delivery is a hop of its own, with `vejas.delivery.attempt` in
  its attributes, and the one that went to the DLQ has `vejas.dlq: true`.

**What a record does not carry.** Every field is scrubbed before it is
written, on the record's own thread:

- The values of credential-named keys, with the panel's own pattern, become
  `<masked>`: in JSON at any depth, JSON held in a string included, and in
  text that names a key and gives its value. That covers a query, a form or a
  connection string (`api_key=…`, `Password=my pass;`, a percent-encoded key
  too), JSON that was cut or escaped in a string (`"password": "…`), a header
  or a YAML line or block (`Authorization: …`, `password: |`), a Python, Ruby
  or PHP dump (`'password': '…'`, `"password" => "…"`, `[password] => …`,
  var_dump's `["password"]=>`), HCL (`"password" = "…"`), a command line
  (`--password …`), an XML element up to its end tag
  (`<Password>…</Password>`, CDATA included) and the userinfo of a URL. A
  text's escapes are read before its keys are looked for: XML sent as
  entities inside SOAP, a query percent-encoded inside a query, a
  `password=\"…\"` inside a JSON log line.
- Every value a secret store handed out in this process becomes `<secret>`,
  wherever it appears: in a string, an object key or a number, as it is or
  once a text's escapes are read (JSON's and `\'`, a URL's `%XX`, XML's
  entities, one inside the other too). A flow that forwards a vault value
  does not leave it in the record, a value that is a JSON document (a
  service account's key file) included, and an error that quotes the start
  of one, where a connector cut a downstream's answer, loses that start. The
  runtime remembers the last 4 096 values, dropping first the one read least
  recently, and skips values shorter than four characters.
- A payload nested more than 100 levels deep is kept as scrubbed text, so
  every record reads as JSON. Past 64 credentials in one text, the rest of
  it is masked.

Masking works on key names and on exact values. A secret that a flow
transforms (encodes, splits, parses and rebuilds) before emitting it is not
recognised, and neither is a credential under a key the pattern does not
name: a camelCase `accessToken`, a `{"name": "DB_PASSWORD", "value": …}`
pair, a key worded for people (`Invalid API key: …`), a SQL statement
(`WITH PASSWORD '…'`), a multipart form field. Keep secrets in
credential-named fields.

**What is kept.** The exporter's sampler decides: by default every trace the
runtime starts is kept, and a caller that sends `traceparent` with the
sampled flag off is followed. Failures are kept whatever the sampler says.
The stream keeps the last 100 000 hops, 256 MB and seven days at most,
whichever limit comes first, oldest out first.

| Variable | Default | |
|---|---|---|
| `VEJAS_TRACES` | `on` | `off` records nothing and creates no stream. |
| `VEJAS_TRACES_PAYLOADS` | `on` | `off` keeps what each hop did without the data it did it to: no `input`, no `answer`. The `error` stays, scrubbed the same way, and an error can quote the data that caused it. |
| `VEJAS_TRACES_MAX_HOPS` | `100000` | Hops kept. |
| `VEJAS_TRACES_MAX_BYTES` | `268435456` | Bytes kept (256 MB). |
| `VEJAS_TRACES_MAX_AGE_SECS` | `604800` | Seconds a hop is kept (seven days). |

The limits are applied to the stream at each start, so a change holds from
the next restart; in a cluster, give every instance the same values. The
stream starts with one replica. Raise it with `nats stream edit`: the runtime
changes only the three limits and keeps everything else you set.

**What it costs NATS.** One subject per trace kept, which the server indexes
in memory. Measured on NATS 2.10 and 2.15 with 100 000 hops kept, that is
200 to 450 bytes per trace: 20 to 45 MB when every trace is a single hop, 7
to 15 MB when traces have three hops. Raising `VEJAS_TRACES_MAX_HOPS` raises
it in proportion.

**What it costs the runtime.** On the isolated flow hop of the benchmarks
page, recording every hop takes 17.5 to 20 µs of CPU per event over two
series, against 74 to 75.5 for the hop with the record off; 12 to 13.5 µs
with payloads kept out, about 4 µs when the sampler keeps one trace in ten.
About 13 µs of it runs on the record's own thread, which the flow does not
wait for. The NATS server spends a few µs more per event storing the hops.
`bench/trace-cost.sh` measures it.

**Who can read it.** The record holds payloads, masked but otherwise as they
came. `GET /traces` is a read, open like `GET /events` and `GET /dlq`: keep
the panel on loopback (the default) or behind your proxy's authentication.
With `VEJAS_TOKEN` set, `vejas_trace` needs it like every MCP call. When your
events carry personal data, so does the record: its limits are your
retention policy for it, and `VEJAS_TRACES_PAYLOADS=off` keeps the data out.

The record writes on a NATS connection of its own, so its traffic never
queues in front of a unit's emits and acks. A record is lost rather than
ever holding up a message. The queue holds 4 096 hops and 32 MB at most, and
a record the stream did not take is dropped and counted. The stream
acknowledges the last record of each batch, which confirms the ones before
it and that the stream is there. A stream removed by hand, or lost with a
NATS store, is created again on the next batch.

```
vejas_trace_records_total 18342
vejas_trace_records_dropped_total{reason="queue_full"} 0
vejas_trace_records_dropped_total{reason="publish_failed"} 0
```

Not built yet (ADR-0034, lot B): the statements a flow executed with its
variables at each `invoke`, `emit` and `respond`, and the provenance of each
flow version (who wrote it, who approved it, the replay diff before it was
promoted).

## The event ring — what just happened

For the everyday "did that message go through?" question there is a live
in-memory ring of recent events:

- Panel: the **Recent events** card (refreshes every 4 s).
- API: `GET /events`; agent: `vejas_events`.

Each entry carries the subject, ok/error, the emitted subjects, a payload
preview, the `trace_id`, and — for sinks — **what the downstream answered** (a rejected
fact, a skip), so you see not just that a message was sent but how it
landed.

## Health and failures

- `GET /healthz` — liveness for a load balancer or k8s probe.
- Anything that *failed* past its retries is not lost — it is parked with a
  verdict in the [DLQ](dlq-replay.md), which is its own inspectable surface.

Everything here is a read; none of it changes what runs. The
[benchmarks](../reference/benchmarks.md) are taken with metrics and the event
ring on, since neither can be switched off, so their numbers include that
cost. What tracing adds is measured on its own, above.
