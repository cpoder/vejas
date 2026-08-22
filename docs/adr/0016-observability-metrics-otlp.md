# 0016 — Observability: hand-rolled Prometheus `/metrics` and OTLP trace export

- Status: Accepted
- Date: 2026-08-22

## Context

The MANIFESTO names observability as one of the four things that survive deleting
the designer ("the runtime, the transport, the connectors, and the
observability"). Until now the runtime shipped only the in-memory trace ring (the
last 50 events per unit, gone on restart) behind `GET /events` — enough for the
panel, not enough for an operator. A serious evaluation asks two questions the
ring cannot answer: *what is the throughput and error rate over time* (a metrics
question, scraped and graphed) and *where did this one message go across hops* (a
tracing question, correlated in a collector). "No Prometheus endpoint, no traces"
is a disqualifier for the operator-credibility half of the platform, the same
bar ADR-0015 (DLQ) was measured against.

The obvious answer is the `opentelemetry` + `opentelemetry-otlp` +
`prometheus` crate stack. It is also the wrong answer *here*. The perf arc we
just closed rests on a specific claim — single-digit-megabyte RSS, a binary ~70×
smaller than the category, 25× less RAM than the comparators — and that stack
pulls in tonic/prost/hyper/tokio and tens of megabytes of dependencies and binary
that would quietly undo it. The footprint *is* the product argument; an
observability story that costs the footprint is a net loss.

## Decision

Ship both, **hand-rolled**, in a single `metrics` module — no `opentelemetry`
crate tree, only what the runtime already links (`serde_json`, `ureq`).

**Metrics: pull-based Prometheus at `GET /metrics`.** Plain-text exposition
(format `0.0.4`). Counters and a latency histogram accumulate on the flow hot
path; gauges are read live from the supervision registry at scrape time:

- `vejas_events_processed_total{unit,result}` — `result` is `ok` / `error`.
- `vejas_emits_published_total{unit}` — messages a unit put on the bus.
- `vejas_dead_letters_total{unit}` — the ADR-0015 subset (deliveries exhausted).
- `vejas_event_duration_seconds{unit}` — a histogram of *in-process* per-event
  time (interpret + emit-buffer), the "pure" latency that excludes the batch
  flush wait. Buckets crowd the sub-millisecond end because a hop is ms-scale.
- `vejas_units{kind}`, `vejas_flow_restarts{unit}`, `vejas_up` — supervision
  gauges, pulled from the registry so they never drift from reality.

Label cardinality is bounded by the number of units (flows + connectors), which
is bounded by the repository — safe to label by `unit` without a cardinality
explosion.

**Traces: push-based OTLP/HTTP with a JSON payload.** OTLP defines a JSON
encoding over HTTP (`application/json` to `/v1/traces`), accepted by the
OTel Collector and every backend behind it — so "OTel-native" needs neither
protobuf nor the SDK, just a POST of the documented shape. Each processed event
becomes one span (`SPAN_KIND_CONSUMER`, `service.name`, `vejas.unit`,
`messaging.destination.name`, `vejas.emits`, `status` OK/ERROR, wall-clock
start/end in unix-nanos). A background thread owns a bounded queue (4096) and a
reused `ureq` agent; it blocks for one span then drains up to 512 into a single
POST, so a burst batches and a trickle still ships promptly.

Two properties are load-bearing:

- **Off by default, zero-cost when off.** The exporter thread is spawned only if
  `OTEL_EXPORTER_OTLP_ENDPOINT` is set. Unset, `span()` sees `None` and is a
  no-op; there is no thread, no queue, no allocation. Metrics are always on
  (accumulating counters is free); only the network push is opt-in.
- **The hot path never blocks on the collector.** Spans are `try_send` onto the
  bounded queue: a stalled or slow collector drops spans rather than back-pressure
  the flow loop. Observability degrades to lossy under stress; the data plane does
  not. (Metrics, being pull-based, are immune to this by construction — a dead
  scraper costs nothing.)

Span/trace IDs are generated from a counter mixed with wall-clock nanos — unique,
which is all a collector requires for correlation; they are not, and need not be,
cryptographically random.

## Consequences

- The four-pillar MANIFESTO claim is now literally true: the runtime exports
  traces and exposes metrics, and the honest "on the roadmap, not in the binary
  yet" disclaimer is retired.
- The footprint story holds: no new heavy dependencies, RSS unchanged.
- Metrics count the flow data plane (interpreter invocations) and the DLQ. Sink
  drivers and the synchronous `/api` path are not yet histogrammed — a follow-up,
  called out here rather than left for someone to discover by its absence.
- `vejas_event_duration_seconds` measures in-process time only. It corroborates
  the bench finding that end-to-end latency is dominated by the persisted bus
  round-trip, not the interpreter (sub-millisecond in-process vs. single-digit-ms
  e2e) — but it is not the e2e number and must not be quoted as one.
- If a full OTLP/protobuf or a richer trace context (parent spans across a
  multi-hop flow) is ever required, this module is the seam to extend; the wire
  format changes there and nowhere else.
