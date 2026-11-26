# 0034 — Observability as the trust surface

- Status: Accepted (partial): lot A is built and T6 gates it in CI (points 1
  to 6, and 12 with them); lot B (points 7 to 11) is not
- Date: 2026-10-24; measured state re-read on v0.6.0 on 2026-11-26; lot A
  built 2026-11-26

## Context

Flows are written by agents and promoted under governance (ADR-0001, ADR-0024).
The person who operates a cell did not write the flow and often will not read
it. Their distrust is specific. For one event they want two answers on one
screen: what did the flow do with it, and who changed that flow, in which
version, with what evidence before the change went live. The runtime already
holds the data for both answers. It shows neither as one picture.

A per-execution view is the entry ticket of the category: n8n shows every
node's input and output for a run, Temporal keeps an event history per
workflow, Kestra draws a timeline per execution. Exporting traces to an
OpenTelemetry collector is expected of any runtime that calls itself operable.
This ADR treats both as table stakes. What no tool in the category shows is the
link between what a run did and who changed the code that ran, because none of
them holds versioning, approval and replay evidence in the same place. Vejas
holds all three (ADR-0018, ADR-0021, ADR-0024). The decision below makes that
link visible on a real event.

### The measured state (re-read 2026-11-26 on v0.6.0, master at `ef48a1f`; first measured on v0.3.6)

| Element | State | Where |
|---|---|---|
| OTLP spans | one span per processed event, `SPAN_KIND_CONSUMER`, a fresh random `traceId` and `spanId` on every span, no `parentSpanId` | `core/src/metrics.rs`, `otlp_payload` |
| W3C trace context | `traceparent` / `tracestate`: 0 occurrences in `core/src` and `connectors/` | `grep -rn traceparent core/src connectors` |
| NATS headers on emit | none; since v0.5.0 every publish goes through `Bus::publish` or a confirmed `Batch`, in the one module that talks to async-nats (ADR-0035); neither takes headers, although async-nats offers `publish_with_headers` and `Message.headers` | `core/src/bus.rs` |
| Span attributes | `service.name`, `vejas.unit`, `messaging.destination.name`, `vejas.emits`, `error.message` | `metrics.rs`, `otlp_payload` |
| Flow version in traces | absent; `flow_version` is computed per unit and only tags dead letters | `main.rs`, the consumer loop: `flow_version` goes to `connectors::to_dlq` only |
| Statement-level detail | none; `exec_block` has no observer hook | `core/src/vjs.rs`, `exec_block` |
| Event ring | last 50 per unit, in memory, lost on restart; fields `seq, ts, flow, subject, ok, error, emits, preview, response, event` | `main.rs`, the per-flow ring |
| Exporter loss | `try_send` drops spans when the queue of 4 096 is full; no counter says how many | `metrics.rs`, the exporter thread: `sync_channel(4096)`, `try_send` |
| OTLP wire | HTTP JSON only; no auth headers, no retry, no sampling | `metrics.rs`, the exporter thread |
| OTLP metrics and logs | none; metrics are Prometheus pull only | ADR-0016 |
| Panel | no trace view; a Recent events card refreshed every 4 s | `core/src/panel.html` |
| CI | no test runs a collector; the transport suite boots nats and the runtime only | `e2e/transport/run.sh` |
| Docs | the guide said "an end-to-end request shows up as a trace across the pipeline" | `docs/book/src/guides/observability.md:35`, corrected with this ADR |

ADR-0016 named the seam honestly: "a richer trace context (parent spans across
a multi-hop flow)" is the extension point of the `metrics` module. The guide
went further than the code. That broke the cross-cutting rule of the ROADMAP
(no promise the code cannot keep) and guardrail R7 of ADR-0029 (trust is
pre-earned). The sentence is corrected in the same change as this ADR. The
capability is decided below and will be claimed in the docs only once its CI
test is green.

### What the view can link, as it exists today

- `VEJAS_AUDIT` (ADR-0018) records a promote as `{ts, actor, unit, file, name,
  key, before, after}` and a proposal decision as `{ts, actor, kind, proposal,
  applied}`.
- `VEJAS_VERSIONS` (ADR-0021) holds the content snapshot and its FNV-1a hash
  per flow, with the git baseline it forked from.
- `VEJAS_PROPOSALS` (ADR-0024) holds `{id, kind, payload, author, baseline,
  evidence, status, decided_by, decided_at}`.
- Shadow replay (ADR-0018) produces a before/after diff on persisted events.
- Git holds the deployed versions.

Today these are four endpoints and three panel cards. None of them starts from
an event.

## Decision

Two lots, twelve points. Each point names its test; a point is "built" when
that test is green. The constraints of ADR-0002 (NATS is the only
infrastructure), ADR-0016 (hand-rolled, no `opentelemetry` crate tree) and the
MANIFESTO (the monitoring screen draws nothing) hold throughout.

### Lot A: one trace per business event

1. **Trace context travels in NATS headers.** A message entering at a source
   gets a trace id; every hop that follows carries it. The context is a W3C
   `traceparent` header (`tracestate` is passed through unchanged) set on every
   publish the runtime makes (flow emits, tick emits, confirmed batches) and
   read on every consumed message. Since ADR-0035 all of them go through
   `core/src/bus.rs`, so the header is written and read in that one module.
   An `api` flow reads `traceparent` from the HTTP request; the HTTP
   connectors inject it on outgoing requests. A message
   that arrives without the header starts a new trace, so foreign producers
   keep working unchanged, and a foreign consumer sees a header it can ignore.
   The payload is untouched, so the at-least-once contract of ADR-0002 is not
   affected. NATS headers need server 2.2 or later, which JetStream already
   requires.

2. **Spans have parents.** Per hop: one span for the flow run (kind
   `CONSUMER`, parent = the producer's span id from the header), one child
   span per `invoke` (kind `INTERNAL`), per emit (kind `PRODUCER`) and per
   connector call (kind `CLIENT`), and one span for the sink hop. Assignments
   do not get spans: they are sub-microsecond and would multiply the volume for
   nothing. Their detail belongs to the local hop record of point 8.

3. **Attributes follow the OpenTelemetry semantic conventions** where one
   exists, at the version current when built: `messaging.system = nats`,
   `messaging.destination.name`, `messaging.operation.type`,
   `messaging.message.id` (the JetStream sequence), `code.file.path` and
   `code.line.number` on invoke and emit spans, `error.type`, `error.message`.
   Vejas-specific attributes: `vejas.unit`, `vejas.flow.version` (the content
   hash of ADR-0021), `vejas.flow.baseline` (the git baseline when the version
   is an overlay), `vejas.delivery.attempt`, `vejas.dlq = true` on a
   dead-lettered run. The version hash is the join key to provenance; the
   exporter does not look provenance up, the view does (point 10).

4. **The exporter is hardened, still hand-rolled.** `OTEL_EXPORTER_OTLP_HEADERS`
   for the hosted backends that need a token; `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`
   with the precedence the specification gives it; HTTPS through the `ureq`
   TLS already linked; bounded retry with backoff on connection errors, 429 and
   5xx; sampling via `OTEL_TRACES_SAMPLER=parentbased_traceidratio` and
   `OTEL_TRACES_SAMPLER_ARG`, the decision taken at the entry hop and carried in
   the `sampled` flag so every hop agrees; errors and dead letters are always
   exported whatever the ratio. A counter `vejas_spans_dropped_total{reason}`
   (`queue_full`, `export_failed`) on `/metrics` makes loss visible. The wire
   stays OTLP/HTTP JSON; the Collector accepts it and protobuf adds nothing
   here.

5. **The proof is a transport-level test.** `e2e/transport/run.sh` gains
   scenario T6: a three-hop chain (source flow, middle flow, sink) under a stub
   collector that is part of the test (a small HTTP listener writing what it
   receives to a file). It asserts that every span produced for one input event
   carries the same trace id, that parent ids chain across the three hops, that
   an incoming `traceparent` on an `api` request is honoured, and that
   `vejas_spans_dropped_total` is 0 at the end. It runs in CI with T1–T5. A
   Collector configuration and a Grafana dashboard (traces waterfall plus the
   `/metrics` series) ship in the repo so an operator sees something within
   minutes of pointing a collector at a cell.

6. **The cost is measured, not claimed.** `bench/flow-only.sh` runs before and
   after, with the exporter unset (must stay at zero cost, as ADR-0016
   promises) and set against a local collector. The numbers go to the
   benchmarks page. The hot path keeps the ADR-0016 property: it never blocks
   on the collector.

### Lot B: the end-to-end view in the default panel

7. **Traces persist in NATS, nothing else.** A dedicated JetStream stream,
   `VEJAS_TRACES`, bounded by age, message count and bytes like the DLQ and
   audit streams, holds one message per hop. The subject carries the trace id
   as a token, so one filtered consumer returns every hop of a trace without
   any index outside NATS. The memory cost of that subject index on the server
   is measured in the bench before the default limits are set; if it measures
   too high, the fallback is a KV bucket mapping trace id to stream sequences.
   Under load the same sampling decision as the exporter applies, errors and
   dead letters always kept. The in-memory ring and `GET /events` stay as the
   zero-dependency fallback and keep their shape.

8. **The hop record is richer than the span.** Per hop it records the unit,
   the flow version and its baseline, the delivery attempt, the statements
   executed with a snapshot of the pipeline (the flow's variables) at each
   `invoke`, `emit` and `respond` boundary, the emits, the downstream answer
   for a sink, and the DLQ verdict when there is one. The snapshot passes
   through the credential mask that already guards the panel
   (`secrets::SECRET_KEY_PATTERN`, one definition shared with the admission
   lint), and a value resolved from the vault is never written to a record.
   That second rule gets its own test. The boundary snapshot is one JSON walk
   per boundary; its cost is part of the measurement of point 6.

9. **The screen is the monitoring screen the MANIFESTO allows.** Entry by
   trace id, by subject, or from a row of Recent events; then the path hop by
   hop; for each hop the unit, the flow version, the statements executed with
   the pipeline at each boundary, the emits, the downstream answer, the DLQ
   verdict and the attempt count; and the existing replay actions (DLQ replay,
   shadow replay), nothing new to write. The statement-level rendering is the
   read-only projection doctrine of ADR-0019 applied to the whole flow body,
   fed by the hop record; that projection gets its own ADR. The screen edits
   nothing. The same data is an MCP tool, `vejas_trace`, so an agent asked
   "what happened to order 4711" answers with the record rather than with a
   guess (ADR-0006).

10. **Provenance sits on the same screen.** For each hop, the flow version
    links to the `VEJAS_VERSIONS` snapshot or the git baseline that produced
    it; to the `VEJAS_AUDIT` promote record (actor, before, after); to the
    proposal (author, evidence, decided by, decided at) when the change went
    through the queue; and to the shadow-replay diff recorded with the
    promotion. The join key is the version hash. Today the promote record and
    the proposal decision do not carry the resulting content hash; this point
    adds it to both records. The operator reads, on one event: this version,
    written by this author, approved by this person, replayed on these events
    with this diff before promotion.

11. **Open-core placement.** Everything above is in the free runtime: the
    headers, the spans, the exporter, the stream, the screen and the MCP tool.
    ADR-0028 puts the line at fleet, identity and export, and this is
    monitoring of one cell. The enterprise seams of ADR-0030 attach where they
    already do: the SIEM export reads `VEJAS_TRACES` next to `VEJAS_AUDIT`, the
    fleet console shows traces across cells.

12. **Documentation discipline.** The guide sentence is corrected with this
    ADR: today every span is its own trace. The guide claims end-to-end traces
    only when T6 is green, and the ROADMAP points here from Phase 5.

## What is built (2026-11-26): lot A

Points 1 to 6 are built, point 12 with them, and T6 gates them in CI. Where
the build departs from the text above, the reason is stated here.

- **No span per emit.** An emit is a buffered publish, confirmed once per
  batch (ADR-0002): its own span would last a few microseconds, measure
  nothing, and double the volume of a one-emit flow. The emit carries the
  context of the hop that made it, and the downstream span names its subject
  and hangs under that hop. Fan-out stays readable: one child per subject.
- **No CLIENT span inside a sink hop.** The sink hop's span is itself of kind
  `CLIENT`, since the runtime is the client of the system it delivers to, and
  its id is the one the HTTP call carries. A second span covering the same
  call would repeat the first.
- **Spans are named after their unit** (`flow:order_sync`), as before, rather
  than `process <subject>`: the unit is what an operator knows, and a
  backend's per-name rate and latency metrics then come per unit. The
  semantic-convention attributes carry the subject.
- **`code.line.number` is not set.** The AST has no line numbers yet. An
  invoke span carries `code.function.name` and `code.file.path`.
- **A success leaves the status unset**, as the semantic conventions ask of
  instrumentation; a failure sets it to error with `error.type=_OTHER` and
  `error.message`. int64 attributes travel as strings (OTLP JSON mapping).
- **Retries follow the OTLP specification:** connection errors, 429, 502, 503
  and 504, with `Retry-After` honoured up to 30 s. Any other 5xx is a
  permanent failure there, so it is not retried.
- **The exporter batches as the OpenTelemetry SDKs do**, with their variables
  (`OTEL_BSP_SCHEDULE_DELAY`, `OTEL_BSP_MAX_EXPORT_BATCH_SIZE`,
  `OTEL_BSP_MAX_QUEUE_SIZE`; the delay defaults to 1 s rather than 5 s), and
  reads its queue on a 25 ms tick while spans are pending instead of waiting
  on it. The first measurement showed why: export cost 63 µs of CPU per event.
  A request per handful of spans and a JSON tree per span explained about 35
  of it, and waking the export thread for every span about 20. A graceful
  stop flushes the queue (3 s at most), and T6 checks that it does. The six
  samplers of the specification are read, and a hop that is sampled out
  builds no span.
- **The context reaches more places than the text named:** the `http-in`
  answer returns the trace id, a dead letter stores its `traceparent` and a
  DLQ replay continues it, the `vx.dlq.events` announcement carries the trace
  of its last letter, an `exec` sink gets `TRACEPARENT` and `TRACESTATE` in
  its environment (the OpenTelemetry carrier for a child process), detect
  units (ADR-0031) take part as flows do, and each entry of the event ring
  records its trace and span ids.
- **Propagation stays on without an exporter, and is not free.** Point 6
  asked for zero cost with the exporter unset. The context has to travel even
  then: a downstream system that traces needs it on the sink's call, another
  instance or cell may export, and the event ring shows the trace id.
  Measured, it costs about 3 µs of CPU per flow hop (1.5
  to 4.5 over four series, against 72.5 µs for the hop). No switch turns it off; one can be added if a deployment needs it.
- **Not built in lot A:** the Collector configuration and the Grafana
  dashboard of point 5 (the guide gives a one-line Jaeger instead, checked
  against Jaeger 2.23.0); `traceparent` on the GETs of `http-poll` and
  `oauth-poll`; MQTT, which has no headers before version 5; `exec-rpc`
  request/reply. Each is an addition, not a change of what is built.
- **Cost (point 6):** `bench/trace-cost.sh` against the runtime from before
  tracing (master at `491ba69`), isolated flow hop, 20 000 events, median of
  five runs, the runtime's own CPU time per event: 72.5 µs before tracing,
  74.0 with the context and no exporter (about 3 µs more over four series),
  83.0 exporting every span, 76.5 exporting one trace in ten. Throughput stays within 2.5 %, since the four
  publishers of the bench bound it. An event that arrives with a
  `traceparent` costs about 5 µs more to continue. The hot path never waits
  for the collector: the 200 000 spans of the exporting runs all arrived,
  none dropped. The numbers are in `bench/README.md` and on the benchmarks
  page.

## Consequences

- The guide is honest today. The "future extension" of ADR-0016 has a design
  and a test instead of a sentence.
- The operator's two questions get one answer on one screen, and an agent gets
  the same answer over MCP. This is the first place where the governance
  machinery of ADR-0018, ADR-0021 and ADR-0024 is visible on a real event,
  which is the argument the MANIFESTO makes to a buyer who distrusts
  agent-written code.
- A new bounded stream per cell, and a subject index whose server-side memory
  is a function of retention. Defaults are set from the measurement of point 7,
  not guessed; a cell on a small box keeps them small.
- Hop records carry business payloads. They inherit the retention concerns of
  the DLQ and the audit stream; the key-pattern mask is a filter on names, not
  a classifier of values, so a secret stored under an unremarkable name would
  pass it. The never-record rule for vault values closes the known hole; the
  mask's limits stay documented.
- A sampled-out trace that fails late is kept locally in full (errors are
  always kept) but its ancestors are missing in the collector. Head sampling
  has this limit; it is stated rather than hidden.
- The interpreter gains a per-statement branch for the boundary snapshot. If
  the measurement of point 6 shows a cost above noise with the exporter unset,
  the snapshot becomes opt-in per flow rather than silently accepted.
- Rolling upgrades are safe: a runtime from before this ADR ignores the header
  and keeps processing; a runtime after it starts new traces for messages
  without one.
- Volume at the exporter grows from one span per hop to one plus the number of
  invokes, emits and connector calls. The queue and batch sizes of ADR-0016
  are re-checked under the bench load; the drop counter makes the limit a
  number rather than a silence.
- Not decided here: OTLP logs (stderr stays the log), OTLP metrics (Prometheus
  pull stays; a Collector scrapes it), a protobuf wire, and any editing of a
  flow from the trace screen.

Rough sizing, in working sessions, to be corrected by reality:

| Lot | Points | Sessions |
|---|---|---|
| A: context propagation, parent spans, T6 | 1, 2, 3, 5 | 2 |
| A: exporter hardening and the cost measurement | 4, 6 | 1 |
| B: stream, hop record, screen, MCP tool | 7, 8, 9 | 3 to 4 |
| B: provenance links and the hash on audit records | 10 | 1 |

## Alternatives considered

- **The `opentelemetry` crate stack.** Rejected for the reasons of ADR-0016,
  which have not changed: the OTLP exporter in that stack drags an async
  runtime and tens of megabytes behind it, and what is missing here is a W3C
  header, a parent id and a handful of environment variables.
- **Trace context inside the payload** (an envelope field). Rejected: it would
  change the event every flow sees and every foreign consumer receives. A NATS
  header is invisible to anyone who does not ask for it.
- **The collector's own UI as the only view** (Jaeger, Tempo, a hosted
  backend). Rejected as the only option, kept as the complement: the view must
  work with NATS alone (ADR-0002), and no collector can show provenance since
  it does not hold the audit stream, the proposals or the replay diffs. The
  collector remains the right place for long retention and cross-service
  search.
- **Per-assignment spans.** Rejected on volume. Statement detail lives in the
  hop record at boundaries.
- **Growing the in-memory ring** instead of a stream. Rejected: ADR-0018
  already moved replay hydration to JetStream because the ring is small,
  recency-clipped and lost on restart. The same reasons apply to traces.
- **A separate observability product or sidecar.** Rejected: one binary, one
  bus, one screen is the point of the platform (MANIFESTO), and a sidecar
  would reopen the footprint question ADR-0016 closed.
