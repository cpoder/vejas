# Observability

Three windows into a running runtime, all built in — no sidecar, no agent
to install (ADR-0016): a **metrics** endpoint to scrape, **traces** to a
collector, and a live **event ring** to see what each flow just did.

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
events** leads to the trace in your tracing backend.

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

The panel does not draw traces yet. The path of one event hop by hop, with
the flow version and its provenance, is the second lot of ADR-0034; until
then the trace is read in your tracing backend.

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
