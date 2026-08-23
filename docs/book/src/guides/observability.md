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

Set one variable and the runtime exports spans to any OpenTelemetry
collector:

```
OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4318
OTEL_SERVICE_NAME=vejas          # optional label
```

Unset, it is a genuine no-op — no exporter thread, no overhead. Each flow
execution becomes a span (subject, ok/error, emit count, timing), so an
end-to-end request shows up as a trace across the pipeline.

## The event ring — what just happened

For the everyday "did that message go through?" question there is a live
in-memory ring of recent events:

- Panel: the **Recent events** card (refreshes every 4 s).
- API: `GET /events`; agent: `vejas_events`.

Each entry carries the subject, ok/error, the emitted subjects, a payload
preview, and — for sinks — **what the downstream answered** (a rejected
fact, a skip), so you see not just that a message was sent but how it
landed.

## Health and failures

- `GET /healthz` — liveness for a load balancer or k8s probe.
- Anything that *failed* past its retries is not lost — it is parked with a
  verdict in the [DLQ](dlq-replay.md), which is its own inspectable surface.

Everything here is a read; none of it changes what runs. The performance
cost of all three, measured, is in the [benchmarks](../reference/benchmarks.md)
— the hot path stays a few-MB, few-thousand-per-second runtime with the
windows open.
