# Changelog

All notable changes to Vejas are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html). While the major
is `0`, minor versions may carry breaking changes — they are called out here.

## [Unreleased]

### Added
- **`invoke` calls a service that answers from a system** (ADR-0032). A file in
  `services/` may declare `driver "http-rpc"` instead of holding code: `URL`
  with `{arg}` placeholders, `METHOD`, `HEADERS` (with `secret()`),
  `TIMEOUT_MS`, `ATTEMPTS`, `DELAY_MS`, `ANSWERS`, `RETRY_ON`. A flow writes
  `o = invoke get_order(id: x)` and reads `o.status`, `o.body`, `o.headers`.
  A call that gets no answer fails the run with the service's name and the
  cause; when a later try may change it, the message comes back after the
  service's delay, up to its attempts (a delayed nak, not `ack_wait`), and
  otherwise it is a dead letter at once. An `api` flow answers 502, or 504
  after a timeout. The call carries the message's `traceparent`. A run that
  is shown (preview, replay, time travel, canary, golden) calls no system and
  says so; replay, time travel and the canary count an event whose two runs
  reach the same call as `not_recorded` instead of comparing it, and the
  panel says which events it could not check. A candidate that calls with
  other arguments, calls another service or fails before its call counts as
  changed. Replaying recorded answers comes next. An argument fills a
  `{name}` of the URL, may not take a setting's name, and may not make a
  segment of the path empty, `.` or `..`; `vjs-check` and a preview report
  one the service does not take. A call follows no redirect. Its error names no URL and no
  header and is masked as the record masks one. A flow that calls a system
  takes its messages one by one, so none waits in the client past
  `ack_wait` and runs twice. A message whose run outlived `ack_wait` and
  comes back to its loop is acked without running again. A declaration holds settings only (an `invoke`
  there is refused), is read again at most once a second, and a secret store
  that does not answer while reading it is a failure a later delivery may
  get past. No new crate: the call uses the runtime's HTTP client. e2e
  `invoke-http.sh` checks it in CI.

- **Signed webhooks on `http-in`** (ADR-0037, the first driver it calls for).
  A manifest names its sender's scheme in `SIGNATURE` (`github`, `stripe`,
  `slack`, `shopify`, `standard-webhooks`, `gitlab`, or `hmac-sha256` and
  `token` for any other sender) and gives `SIGNING_SECRET = secret("…")`.
  Each request must then carry a valid signature, checked in constant time
  over the raw body before anything else is read, or it gets 401 with the
  reason and is not published. Where a timestamp is signed, one more than
  `SIGNATURE_TOLERANCE_SECS` (300) from the runtime's clock is refused. Slack's
  check of a Request URL gets its challenge back and is not published.
  Refusals are counted on `/metrics` (`vejas_signature_failures_total{unit,reason}`).
  A manifest that names a key of the check without a check the runtime can run
  (an unknown scheme, no secret it can read, `SIGNING_SECRET` without
  `SIGNATURE`) does not start. No new crate in the build: `ring` and `base64`
  were in it already. e2e `http-in-signed.sh` checks it in CI.

- **`GET /shape?file=<path>`**: a flow's or a service's shape for the panel's
  flow page. The event fields it reads, each value it computes with the
  business literals it uses, the services it calls and what they give back,
  its checks (the rules projection), what it sends and where, and the comment
  written above each literal. `/surface` entries carry that comment as `doc`.
- **`GET /events?flow=<unit>&paths=1`**: one unit's events, each with the other
  hops of its path (same trace id). Activity for one flow reads it, and the
  flow and connector pages read `?flow=<unit>`, so a unit that sends often (a
  timer) no longer pushes a quiet flow's events out of its page, as it did out
  of the newest hundred of all units. The unit's own events are all there; the
  other hops are capped at the newest 200.
- An event in the ring names the JetStream message its hop consumed and which
  delivery it was (`stream_seq`, `attempt`; null for a source's publish). A
  retry shares its message; the other messages a flow's loop sends in the same
  trace do not, so one of them that failed is no longer hidden by the others
  that went through.
- A dead letter's replay names the message that died (ADR-0015 Amendment B):
  `original_seq` in the envelope, a `Vejas-Replay-Of` header on the replay,
  `replay_of` in the ring and `vejas.replay_of` on its span. An event whose
  replay went through no longer shows as failed.

### Changed
- **The panel is redesigned around two people.** A menu on the left; a page
  per flow for the domain expert, read top to bottom (receives, computes,
  calls, checks, sends), with the business values in yellow where the flow
  uses them, each checked on recent real events before it goes live; Activity
  (one row per event, end to end) and To review (dead letters, proposals) for
  the operator; a map of what runs. Still one HTML file with no dependency.
  Every function of the previous panel is kept. e2e `demo.mjs` and
  `bridge-film.mjs` follow the new screens.
- **`/graph` gives a connector's subjects as NATS patterns.** A webhook
  without `SUBJECT` publishes on `vx.>` (it was `vx.*`, which missed any
  subject of three tokens), and within its `ALLOW` list when it has one,
  under `VEJAS_SUBJECT_ROOT` when it is set, as the webhook itself does.
- `POST /flows/new` and `/connectors/new` answer with the name the file got.
  When the name the agent chose was taken, the file is `<name>_2.vjs` and the
  answer said `<name>`, the existing unit.
- **Connectors are written against one crate, `vejas-connector`** (`sdk/`,
  ADR-0038): the bus, the trace context, `Config`, `Ctx`, the `Driver` trait,
  and the services every driver shares (a source's publish as the first hop of
  its trace, the sink loop with its retries and dead-letter queue, offsets).
  Each hop a driver makes is reported to a `Host`, which in the runtime writes
  the events ring, the trace record and the metrics, as before. The runtime,
  the IBM MQ connector and the AMQP connector build on it; MQ and AMQP no
  longer compile the runtime's `bus.rs` through a path. No change for a
  manifest or a recipe. One detail of the trace record: a source's publish
  that fails no longer lists its subject under `emits` (nothing was
  published; the subject is still on the span).
- **The credential mask reads camelCase as snake_case.** `accessToken`,
  `refreshToken`, `idToken` and their kind were shown in the clear in trace
  records; they are masked as `ACCESS_TOKEN` is, in the record, the panel
  and the admission lint alike. On 43 labelled camelCase names, credentials
  missed go from 18 to 3, against 9 false masks, pagination tokens such as
  `nextPageToken` among them, as `next_page_token` already was (ADR-0017,
  decided by the founder). The record also masks the value of a pair whose
  name names a credential: `{"name":"Authorization","value":"…"}` in a
  header list, an environment list or tags.
- `http-in` takes a request's body as long as its `Content-Length` says;
  bytes read after it are no longer part of the body. A request without
  `Content-Length` is taken as before, its body what came with its head. A
  request over 1 MiB, head and body, gets 413, a `Content-Length` that is not
  one number 400, a chunked body 411, and the connection closes. A request over
  1 MiB used to be cut at 1 MiB and refused as JSON that does not parse, and a
  chunked body was refused the same way.

### Fixed
- **An emit a stream does not store fails its run.** Before acking a batch's
  messages, the runtime checked that the server had read their emits, not that
  a stream had stored them. A stream that was full and discarded new messages,
  a full disk or a cluster with no leader for the stream dropped the emit, and
  the message was acked and recorded as a success: the event was lost without
  a trace. An emit to a subject a stream stores, `VEJAS` or any other, now
  waits for that stream's answer. One the stream refuses fails its run: the
  message comes back after `ack_wait`, then is a dead letter that names the
  refusal (`publish vx.orders.ready: maximum messages exceeded`). A refusal no
  delivery changes, a message over the stream's own size limit, is a dead
  letter at the first delivery, and an `api` flow answers 500 for it, where it
  answers 503 for any other refusal. A sealed stream is one too; a full
  stream, account or disk is not. The batch's other runs stand, and the
  refused run's other emits may be stored already, so they leave again with
  it. A flow's success is recorded in `/events` once its emits are confirmed.
  The runtime reads the account's streams when a unit starts and every 30 s
  after, in the background, which takes the permission to list them
  (`$JS.API.STREAM.LIST`); without it, the subjects under the root are taken
  for stored. A stream that stores without answering (`no_ack`) keeps the
  check of before. A runtime that runs only `api` flows makes `VEJAS` before their
  first emit, and again within 30 s when it is deleted: before, those emits
  were lost when no unit on the bus had made it. Waiting for the stream costs
  a flow 4 % of its rate when it drains a backlog, and nothing that shows with
  publishers in the loop (ADR-0035, amendment).
- **A detect unit no longer feeds its engine a message twice.** A batch the
  bus did not confirm was delivered again to the same engine, which had taken
  every event of it in: a window counted them twice. Such a batch now starts
  the unit again with a new engine, from its snapshot when it keeps one, whose
  replay sends the alerts again. A message the server delivers again, its ack
  lost or late, is acked without being fed when the program keeps state: the
  unit's consumer is made again at each start, after what its engine holds. A
  stateless program alerts again for it, at least once. An alert a stream
  refuses is a dead letter of its own, marked `"emit": true`, and its message
  is acked; its replay publishes the alert where it was going, for every
  reader. An event the engine fails on is a dead letter at once.
  When an alert is too large for the server, none of the run's alerts leave
  and each is a dead letter. Alerts that time closes, with no message behind
  them, are confirmed the same way: before, nothing checked that they left.
- **A dead letter never fails for its size.** A payload that would take its
  letter past the server's limit is cut to fit, and the letter says so
  (`"cut": true`). A replay refuses it, since it holds only the start.
- **An `api` flow answers 503 when the bus does not confirm its emits.** It
  answered 200, or what the flow said, without knowing whether its emits had
  left: with the bus down, the caller was told the event was taken and it was
  lost. The emits now leave before the answer, and when the bus does not
  confirm them the caller gets 503, with the reason and "send the request
  again"; the hop is recorded as failed. `http-in` answers the same 503 where it
  answered 502. An emit the server refuses, over its limit, answers 500
  instead: none of the request's events left, and sending it again changes
  nothing. A failed request no longer counts its emits in the metrics.
- **A run's emits leave all together or not at all.** When the bus refused one
  emit of a run (a payload over the server's limit), the emits held before it
  still left, at every delivery of the message, until it was dead-lettered:
  five copies of the first emit and none of the others. They now go back out
  of the batch with the run. A detect unit did worse: the event was never
  dead-lettered and its other emits left at every delivery, without end. It
  now fails as an event the engine refuses does, dead-lettered after its
  deliveries.
- **Strings read JSON's escapes.** A string read only `\n` and `\t`, and kept
  the letter of any other escape: a business value set live to a CR came back
  as an `r`, since `/surface/set` writes JSON. `\r`, `\b`, `\f`, `\/` and
  `\u` with four hex digits (a pair for a character above U+FFFF) now read as
  JSON reads them, and `\{` `\}` give a brace in an f-string. Any other escape
  is an error that says how a backslash is written: a flow that relied on one
  must write `\\` instead (breaking). `vejas_language`, the text agents
  read, says so. An error in an f-string's `{…}` says the line it is on (it
  said line 1), and a table set live gains a key whatever its text holds: an
  escape JSON does not read, keys without quotes, a comment after it.
- **Docs**: a dead letter lands on `vxdlq.<unit>`, not on its original subject;
  the runtime reads no `tables/` folder; `rpc:exec` serves the MCP tools, and
  no flow can read its answer yet (ADR-0032). The detect page says what the
  engine keeps of an event: 256 KiB of a string, 10 000 elements of an array,
  1 024 fields. It cuts the rest and the runtime is not told.
- **A dead letter's replay runs again only in the unit whose letter it is**
  (ADR-0015 Amendment C). It went back on the subject the letter came from,
  where every consumer of that subject ran it again: a flow that had handled
  the event the first time sent it on a second time. The replay now carries
  its unit (`Vejas-Replay-For`), and every other consumer acks it unread: the
  runtime's flows and sinks, the MQ and AMQP bridges, and `rpc:exec`. A letter
  that no unit would take stays, with the reason, instead of being replayed to
  no one: its unit is no longer declared, or no longer reads the letter's
  subject (the envelope now keeps the unit's `consumer`). The panel shows that
  reason instead of "Replayed.".

### Security
- **A request of `http-out`, `http-poll` or `oauth-poll` that does not get
  through names its host in its error, not its URL.** The HTTP client's text
  starts with the whole URL, its query and user info included, and quotes a
  header it cannot send, value included. A credential placed in either
  reached the unit's last error and the log.
- **The runtime masks what it shows of the bus, as the trace record does**
  (ADR-0034). The events ring (`/events`, `vejas_events`: a hop's preview, a
  sink's answer, an error), dead letters (`/dlq`, `vejas_dlq`: the payload and
  the last error), each unit's last error in `/topology`, the events a shadow
  replay, a time travel or a canary replays, the evidence of proposals and the
  detail of a connector test were given as they came. The panel masked what
  it shows; an agent reading the MCP tools saw it all. Credential-named keys
  and pairs are now masked, and the values the secret stores handed out taken
  out, before the answer leaves. Units, sequences, times and trace ids stay
  as they are. The control channel's status and probe are masked the same
  way. A dead letter's payload past 16 KB is shown as its start, and
  `vejas_dlq` gives 200 letters at most.
- **A shadow run never reads a credential of the events it replays.** Time
  travel and the canary run an agent's candidate on real events: one that
  copied `event.password` under another name got the value back past any
  mask on names. The shadow replay, time travel, the canary and golden
  capture now read the events masked, as they already read `secret()`. A
  golden case no longer writes an event's credentials into the repository.
  Flags and nulls stay as they are, so a branch on them goes as live; a
  number stays as the record shows it. A result names in `hidden` the fields
  its code may read that the run did not see, and the panel says so under
  its verdict.
- A cut JSON text is read as JSON as far as it goes, so a pair
  (`{"name": "Authorization", "value": "Bearer …"}`) is masked in an event's
  preview, and in the record's preview of a payload past 16 KB, as it is in
  whole JSON. A text that came cut, such as a payload a producer truncated,
  is read the same way and keeps its end, in the record too.

## [0.7.0] — 2026-11-26

### Added
- **One trace per business event** (ADR-0034, lot A). The runtime carries a W3C
  `traceparent` in a NATS header on every message it publishes, so the spans of
  one event, from the source or webhook through each flow and `invoke` to the
  sink, form one trace with parent links. `http-in` and `api` flows continue a
  caller's trace, and `http-in` answers with the trace id. `http-out` and
  `slack-out` send `traceparent` downstream, and an `exec` sink gets it as
  `TRACEPARENT`. A dead letter keeps its trace and a replay continues it. The
  event ring and the `dead-lettered` event show the trace id. Transport test T6
  checks the chain in CI against a stub collector, a graceful stop included.
- **The OTLP exporter reads the standard variables:** the traces endpoint,
  headers (never logged) and timeout, `OTEL_RESOURCE_ATTRIBUTES`, the six
  samplers of `OTEL_TRACES_SAMPLER`, and the batch variables `OTEL_BSP_*`. It
  retries connection errors, 429, 502, 503 and 504 with backoff, flushes its
  queue on a graceful stop, and counts on `/metrics` what it sends and what it
  loses (`vejas_spans_exported_total`, `vejas_spans_dropped_total{reason}`).
- **The trace record** (ADR-0034, lot B, first part). Each hop the sampler
  keeps, and every hop that fails, is written to a new JetStream stream,
  `VEJAS_TRACES`, under `vxtrace.<trace id>`: the span plus the subjects the
  hop emitted, the payload it received and what a sink or an `api` flow
  answered. Before it is written, each record loses the values of
  credential-named keys (the panel's pattern), in JSON at any depth, JSON held
  in a string included, and in text that names a key and gives its value (a
  query, a form or a connection string, a header or a YAML line or block,
  XML, a URL's userinfo, a Python, Ruby or PHP dump, HCL, a command line),
  escaped or not, and every value a secret store handed out, in any string,
  key or number, however a text escapes it (JSON, `%XX`, XML entities, one
  inside the other). A
  record is written on a NATS connection of its own, from a queue bounded in
  hops and in bytes, and the stream acknowledges each batch; a stream removed
  by hand comes back on the next batch. The panel's new **Trace** card
  shows one event hop by hop, opened from **Recent events** or found by id or
  by what the event carried; `GET /traces/{id}`, `GET /traces?search=` and the
  MCP tool `vejas_trace` read the same record. On by default; `VEJAS_TRACES=off`
  turns it off, `VEJAS_TRACES_PAYLOADS=off` keeps payloads out, and
  `VEJAS_TRACES_MAX_HOPS`, `_MAX_BYTES` and `_MAX_AGE_SECS` bound it (100 000
  hops, 256 MB, seven days). Counted on `/metrics`
  (`vejas_trace_records_total`, `vejas_trace_records_dropped_total{reason}`).
  Recording every hop costs 17.5 to 20 µs of CPU per flow hop over two
  benchmark series, about 13 of them on the record's own thread, and about
  4 µs at one trace in ten (see the benchmarks page). e2e `trace-record.sh` checks it in CI.
- **A flow hop's record says which statements it ran** (ADR-0034, point 8,
  first part): each line in order, the service a line is in, the subject an
  `emit` sent to, the status a flow's `respond` set, and the version of an
  invoked service where it starts; the record also lists the services the
  run invoked with their versions. A run keeps its first 192 steps and its
  last 64, with the count of those between. A failed run ends on the
  statement that failed, marked `"failed": true` and said again after the
  steps of a service it invoked before failing. Steps carry no payload, so
  they stay with payloads off.
  Reading a trace (`GET /traces/{id}`, `vejas_trace`, the panel's Trace card)
  gives each step its line of code when the file, or the version store for a
  flow promoted in a cluster, still has the version that ran. A read drops
  any code a record carries and stops at 64 files, 257 steps a hop and 1 MB
  of code. Spans carry `code.file.path`. On the benchmark's flow, eleven
  statements per event, the steps add 3.5 to 5.5 µs of CPU per hop, about 2
  of them on the record's own thread (two series of eight alternating pairs,
  `bench/pairs.sh`, new); with the record off, the interpreter does no more
  work than before.

### Changed
- Spans have a new shape. The name is still the unit and the old attributes
  stay, but the kind now says what the hop was (`SERVER`, `PRODUCER`,
  `CONSUMER`, `CLIENT`, `INTERNAL` instead of `CONSUMER` everywhere), new
  attributes follow the OpenTelemetry semantic conventions (`messaging.*`,
  `error.type`) and carry the flow version, and a success leaves the status
  unset instead of `OK`. A query or dashboard that filters on kind or status
  may need updating.
- The trace context travels even without an exporter, so a downstream system
  and the event ring get it. It costs about 3 µs of CPU per flow hop; exporting
  every span about 10 µs (see the benchmarks page).

### Fixed
- **The file guard took a folder's name as a prefix.** `flows_old/a.vjs` or a
  `services.json` at the project root passed the check that confines
  `GET /file` and the file writes to `flows/`, `services/`, `connectors/` and
  `packages/`. A folder must now be the path's whole first component.
- **A character of several bytes outside a string crashed the parser.** An
  em dash or a byte-order mark in a flow's code panicked the lexer, and the
  unit restarted in a loop. A byte-order mark at the start of a file is now
  skipped, and any other such character is a parse error that names it.
- The MCP tools reference showed twelve dashes as broken characters and left
  out `vejas_vpl_check` and the four SAP tools.
- **A read-only cursor stopped after 1 000 messages.** The ephemeral consumers
  behind the shadow replay's history, the canary and the trace search asked for
  acks and never gave any, so the server stopped delivering at its default of
  1 000 pending messages. A shadow replay asked for more than about 125 events
  read the oldest 1 000 messages of its window instead of the newest, and a
  canary stopped comparing after its first 1 000 live events, without a word.
  These cursors now take no acks.

### Corrected
- Up to 0.6.0, the observability guide said an end-to-end request shows up as a
  trace across the pipeline. It did not: every span carried a fresh trace id and
  no parent, so a request that crossed three flows made three unrelated traces in
  the collector. This release builds that trace (see Added), and the guide now
  describes what a trace holds and what it costs.

### Security
- **url 2.5.8 → 2.5.9** in the runtime and in the `mq` and `amqp` connectors, for
  RUSTSEC-2026-0592 (stack overflow in `Url::origin()` on nested `blob:` URLs).
  The `amqp` connector still carries url 1.7.2 through amiquip, which only parses
  the broker URL the operator configures and never calls `origin()`; that one is
  ignored in `.cargo/audit.toml` with this reason, and leaves with amiquip.

## [0.6.0] — 2026-11-07

If you reach a raw binary from another machine, set `VEJAS_HTTP_ADDR` before you
upgrade, and `VEJAS_TOKEN` with it: see Security. Containers do not change.

### Security
- **The binary listens on `127.0.0.1:8686` by default.** It was `0.0.0.0:8686`, with
  no token, on a port that writes flows and runs commands. To reach a raw binary from
  elsewhere, set `VEJAS_HTTP_ADDR`, and `VEJAS_TOKEN` with it. The image sets
  `0.0.0.0:8686` itself, so containers do not change. Past loopback with no token, the
  runtime now warns at boot.

### Changed
- Published numbers corrected against v0.5.1: cluster promote 20 ms, graceful handoff
  at most 2 s, crash failover 4–5 s, image 40 MB to pull (100 MB unpacked), and against
  Redpanda Connect ~20× less memory and a ~30× smaller binary.

## [0.5.1] — 2026-11-07

If an `exec` recipe read a runtime credential by inheritance, it now has to declare
it: see Security.

### Security
- **Exec children no longer inherit the runtime's credentials.** `sh -c` children of
  the `exec-*` drivers and the agent CLI got `VEJAS_APPROVAL_TOKEN`, `VEJAS_TOKEN`,
  `VAULT_TOKEN` and every `VEJAS_SECRET_*`. They now get what the manifest declares
  in `ENV`, so a recipe that read one of these by inheritance has to declare it. An
  exec connector stays trusted code: this is hygiene, not a sandbox.

### Fixed
- **The panel can write when `VEJAS_TOKEN` is set.** Only Approve and Reject worked
  there: every other write got a 401, with no way to give the token. The panel now
  asks once and keeps it in the page's memory.
- `http-in` writes its response in one segment instead of one per piece of the
  format string.
- `bench/flow-only.sh` and `bench/detect.sh` no longer leave the runtime and NATS
  running: under `set -e`, the status of the first killed process ended the trap.

### Changed
- `scripts/deploy-docs.sh` publishes the book from a commit, and `RELEASING.md` gains
  the step: the book at vejas.dev/docs had not moved since 2026-08-24. A release now
  pushes master first and tags once CI is green. Site and book backups stay outside
  the served directory, where a stale page was reachable.

## [0.5.0] — 2026-11-07

The runtime leaves the deprecated sync NATS client. If your NATS user has a publish
allow-list, read the upgrade note under Changed first.

### Added
- **Dead letters announce themselves** on `vx.dlq.events`, an ordinary subject,
  so that a flow or a sink can wake a person or an agent (ADR-0015 Amendment A).
  The event carries metadata only, never the payload or the error text, and
  `VEJAS_DLQ_EVENT_SECS` (default 60) groups a burst into one event per unit,
  with `count` for the letters it stands for. `docs/examples/dlq_webhook.vjs.example`
  posts it to a URL.

### Security
- **Seven advisories leave `.cargo/audit.toml`**: RUSTSEC-2026-0049, -0098, -0099,
  -0104, -0346, -0349 and -0382, all held by the sync `nats` client's rustls 0.22
  and rustls-webpki 0.102. Every lockfile is now on rustls 0.23.46 and
  rustls-webpki 0.103.16, `connectors/amqp` included.

### Fixed
- **A burst of connections during boot could end the version watch for good.** Its
  first connection panicked when the OS had no thread to give: nothing showed it,
  and a promoted version no longer reached that instance until a restart. It
  needs a thread limit and a burst at that moment. The watch and the control loop
  now carry a name and start again after a panic.
- **A consumer deleted under a running flow no longer leaves a zombie.** The flow
  stayed `running` for good, consuming nothing, and two empty dead letters from an
  `_INBOX` subject landed in the queue. It now restarts and recreates the consumer.
- **An emit the server cannot take no longer redelivers forever.** A publish
  refused before it leaves, for a payload over the server's limit, left its source
  message un-acked with no cap on deliveries. It is now a failed run, dead-lettered
  after `MAX_DELIVERIES` like any other. Detect units keep the old behaviour here:
  their emits are small and their engine state makes a redelivery a different case.

### Changed
- **The NATS client is now `async-nats`, behind one module (ADR-0035).** `nats` 0.25
  is deprecated. `core/src/bus.rs` runs `async-nats` on one tokio worker and gives
  the rest of the runtime a blocking API: no other module sees tokio or a NATS type,
  and flows, connectors and supervisors stay threads. A process now holds one
  connection, where it held two per flow. At 200 flows that is 210 threads against
  1 011, 1 connection against 401, and 26 MB against 140. A flow is running 11 ms
  after spawn, against 1.7 s. The flow hop holds its rate, ten flows run about 10 %
  faster, and the 5 ms flusher floor is gone, with `publish_confirmed`,
  `fetch_iter_flushed` and the second connection for acks. The costs: the binary
  grows from 9.9 to 11.2 MB, RSS at one flow from 9.2 to 10.2 MB, and the MQTT
  loopback falls from about 2 400 to 2 070 round-trips/s. Existing streams,
  consumers, buckets and snapshots work as they are, and v0.3.6 reads back what this
  version writes. **To upgrade:** a NATS user with a publish allow-list needs
  `_INBOX.>` on it. Without it nothing is ever acked and everything redelivers; the
  log names the permissions violation.
- **`connectors/mq` and `connectors/amqp` compile the same module**, and
  `connectors/amqp` moves to rustls 0.23.

## [0.4.0] — 2026-11-06

A security release: upgrade from any 0.3.x, and rotate any secret a shared or
agent-facing deployment could have exposed. It is a minor release because of one
breaking change to the HTTP API in governed mode, under Changed.

### Security
- **An event field in an `emit` subject could inject NATS protocol.** The subject
  reached the wire as it was, so a line break in a field of an event turned the
  rest of that field into protocol commands, within the permissions of the
  runtime's NATS user. Through `http-in` this needed no credential: a flow that
  builds a subject from an event field let an anonymous POST publish anywhere or
  purge a stream. No shipped flow or template does this; check the flows of your
  own that do. `emit` now refuses a subject with whitespace or a control
  character: the run fails and the message reaches the DLQ. `http-in` answers 400
  on such a suffix.
- **Secret values left through every path that shows a run.** In v0.3.6
  preview, `vejas_run_flow`, flow-as-tool, shadow-replay, time-travel, canary
  and golden capture evaluated flows with the real secret store and returned
  the result. `GET /preview`, which needs no token, returned any variable
  holding a secret. In governed mode, `vejas_time_travel` handed the value to an
  agent holding only `VEJAS_TOKEN`, through a candidate that emits it. These
  paths now evaluate `secret()` to the placeholder `<secret:path/key>`,
  fail-closed as before, and a real run still gets the value. Rotate any secret
  that a shared or agent-facing deployment could have exposed this way. A branch
  comparing with a secret now takes the not-equal path in shadow.
- **rustls-webpki 0.103.15 → 0.103.16 and webpki-roots 1.0.9 → 1.0.11**, for
  RUSTSEC-2026-0382 and -0381 (certificate paths through distrusted emSign
  intermediates). The TLS stack of the runtime's HTTP client and of MQTT over
  TLS rejects them again. The sync `nats` client keeps rustls-webpki 0.102 and
  its OS trust store, so -0382 stays ignored there, with the reason in
  `.cargo/audit.toml`, until the move to async-nats.

### Fixed
- **A burst of connections could silently kill `http-in`.** When the OS refused
  a thread (a pids limit, memory), `thread::spawn` panicked in the accept loop:
  the connector stopped for good, while `/healthz` said 200 and the topology said
  `running`, `restarts: 0`, `last_error: null`. Reproduced on the v0.3.6 image
  with `--pids-limit=64` and 300 idle connections. `http-in` now holds at most
  `MAX_CONNECTIONS` (default 512) and answers 503 past it, gives a request 30 s
  to arrive, and drops one connection when no thread is available. The panel
  port has the same bound (`VEJAS_HTTP_MAX_INFLIGHT`, default 256). Refusals
  count in `vejas_http_rejected_total`.
- **A panic in a unit no longer ends its supervision.** It takes the path of
  any other failure: `restarting`, the message in `last_error`, backoff, another
  attempt. Supervisor threads carry the unit's name, so a panic in the log says
  which unit it was.
- **The panel could not approve when `VEJAS_TOKEN` was set.** Approve and Reject
  got a 401 from the write gate, because the panel holds the approval token and
  never the agent's. A valid `X-Approval-Token` now answers the write gate on
  the routes that belong to the human.

### Changed
- **Breaking, governed mode only: replaying and discarding dead letters are human
  acts.**
  `vejas_dlq_replay` and `vejas_dlq_purge` are refused, and `POST /dlq/replay`
  and `/dlq/purge` answer 409 without `X-Approval-Token`. They used to accept
  `VEJAS_TOKEN`, which the agent holds. The last step of the self-healing loop
  moves to the panel in governed mode (ADR-0024 Amendment A). Outside governed
  mode nothing changes.
- Replays and purges are now recorded in `VEJAS_AUDIT`, as the guide already said.

### Corrected
- The production line (README, vejas.dev, launch kit) claimed two production
  customers collecting NIS2 supplier-compliance evidence across eight EU
  countries. Checked in Reglyze's production on 2026-10-15: the collection cell
  has run there since August, with Microsoft 365 and EDR collectors mapped to
  four national laws (IT, PT, HR, PL), and no customer has connected a source
  yet. The line now says that.

## [0.3.6] — 2026-10-14

### Added
- **`vejas-runtime --version`** (or `-V`) prints the version and exits. It
  used to start a runtime, like any argument the binary does not know, so
  `docker run IMAGE vejas-runtime --version` stayed up instead of answering.
  `e2e/detect` D3 checks it.

### Security
- **rustls 0.23.43 → 0.23.46** in the runtime, for RUSTSEC-2026-0285 (TLS 1.3
  handshake messages accepted across encryption levels), -0346, -0348 and
  -0349. rustls is the TLS stack of the runtime's HTTP client (http-poll,
  oauth-poll, webhooks, Vault) and of MQTT over TLS.
- **Dependency advisories are checked in CI.** `scripts/audit.sh` runs
  cargo-audit on every `Cargo.lock` in the repo, the runtime and each
  connector, on every push and once a day. Six advisories against rustls 0.22
  and rustls-webpki 0.102 remain: they come with the sync `nats` 0.25 client,
  which has no patched release, and moving to async-nats removes them.
  `.cargo/audit.toml` lists them with the reason none is reachable here, as
  well as idna 0.1.5 in the AMQP connector (via amiquip).

## [0.3.5] — 2026-09-24

### Changed — no cliff past ten thousand open correlations (varpulis #289)
- **A correlation with many sequences open at once no longer falls off a
  cliff.** The engine swept every partition's open runs on each event, to
  expire them and confirm absences; since varpulis #289 it keeps their
  deadlines in order and visits only the partitions with one due.
  `bench/detect.sh` (`PROGRAM=sequence`), against v0.3.4 in the same
  session: 30 000 open went from 2 080 to 14 500 events/s (and 145 to about
  100 MB), 10 000 from 11 900 to 22 000, 1 000 from 19 600 to 26 000; a
  hundred thousand open now runs at 11 000 events/s in 450 MB. The sizing in
  `bench/README.md` and `concepts/detects.md` is re-measured: what bounds a
  unit is memory, about 4 KB an open sequence.

### Fixed — a cancellation on a named pattern (varpulis #288)
- **`.not(C)` on a stream that reads a named pattern cancels its runs.** It
  was accepted and silently did nothing, so `Unacked.not(Cancel)` still
  raised the cancelled orders. It now cancels them as it does on an inline
  sequence, per partition when the pattern has one.

## [0.3.4] — 2026-09-24

### Fixed — an absence is raised when its deadline passes (varpulis #287)
- **"An order not acknowledged within 4h" (`-> NOT Ack ... within 4h`) is
  raised when the four hours are over**, by the first event that takes the
  event time of the pattern's types past them or, when nothing more comes at
  all, about a grace (`VEJAS_IDLE_CLOSE_SECS`) after them. It used to wait
  for the next event that reached the pattern itself: on subjects that went
  quiet after the order, never. `e2e/detect` D8 covers it: an order, an
  acknowledged order, then silence; the previous runtime raises nothing.
- **An absence the unit was waiting out when it stopped is still raised
  after the restart.** The snapshot kept the open sequence but not what its
  negated step waited for, so after a restore the acknowledgement no longer
  cancelled it and the deadline dropped it without an alert. `e2e/detect`
  D9 covers it: an order, kill -9 with the order in the snapshot, restart,
  silence; the previous runtime never raises it.

## [0.3.3] — 2026-09-24

### Fixed — a window on a quiet source closes
- **A window fed by a source that goes quiet closes**: a detect unit sets an
  idle grace, `VEJAS_IDLE_CLOSE_SECS` (60 seconds by default, 0 turns it
  off). Once a subject's type has sent nothing that long, its event time
  moves on with the wall clock, less the grace, and the unit checks while it
  has nothing to read. A brute force on a sparse source (VPN logons, one
  application's log) was raised only when that source spoke again; it is now
  raised about a grace after its window ends. The types' clocks are in the
  unit's snapshot, so a restarted unit closes the windows it restored on time
  too (varpulis #286). `e2e/detect` D7 covers it: three VPN failures, then
  silence; the previous runtime never raises it.

## [0.3.2] — 2026-09-23

### Fixed — detect units, from the Varpulis engine (varpulis #284, #285)
- **A time window closes when event time passes its end**, on the next event
  of the types feeding it, not only when a later event reaches that same
  window. A window over a filtered stream used to wait for the next event
  passing the filter, a partitioned one for the next event of the same
  partition: a brute force counted per address that ended in a successful
  logon never raised its count in a running unit. A window fed by several
  types waits for the slowest of them; a type that goes quiet holds back only
  the windows it feeds (with `.watermark()` it used to hold back every
  window). Windows close upstream first, so a count's alert reaches the
  windows and correlations above it in time.
- **A sequence step that names a derived stream reads what that stream
  outputs**: over an aggregate it matched the raw events instead of the
  results, a second `.where()` of the stream was lost, and a first step's own
  filter replaced the stream's instead of adding to it.

## [0.3.1] — 2026-09-23

### Fixed — detect units, from the Varpulis engine (varpulis #277, #279, #281)
- **A sequence with a closure followed by a step** (`A -> all B -> C`) no
  longer matches before C. Under the default `.each()` it emitted a complete
  match at every B, with or without a C; the brute-force example alerted on
  failed logins alone. Matches now come when C arrives, one per closure event.
- **`.stnm()` now keeps an event from opening a second run** when a run
  already took it, so `.stnm().longest()` makes one alert of a brute force of
  any length.
- **A condition on a field the event does not carry is false** in `.where()`,
  as it was in a sequence step: `a == "x" or ends_with(b, "y")` fired only
  when `b` was present, and `selection and not filter` dropped events lacking
  the filter's field.
- **A program with a lambda (`arr.filter(x => ...)`), `a?.b` or a timestamp
  literal no longer aborts the process** with a stack overflow, which took
  every unit of the runtime down with it.
- **`vejas-runtime vpl-check` and the MCP tool `vejas_vpl_check` run the
  engine's semantic validator**: an unknown function (E050), a misspelled
  event type once types are declared (E033), a regular expression that cannot
  compile (E052) and an unbounded closure (W003, a warning) are reported with
  their line. They checked parse and load only.

### Added — in VPL
- Single-quoted raw strings (`'\AppData\Local\Temp\'`, `''` for a quote),
  `regex_match(s, pattern)`, and backticked field names (`` `cs-uri-query` ``)
  for web and proxy logs. With them, rules converted from Sigma by
  `sigma convert -t varpulis`
  ([pySigma-backend-varpulis](https://github.com/varpulis/pySigma-backend-varpulis))
  run as detect units.

## [0.3.0] — 2026-09-22

### Added
- **The detect demo** (`e2e/detect-demo/`): a signature and a behavioural
  sequence on the same Sysmon bus, four asserted beats — both fire on PsExec
  under its own name, the signature alone goes quiet when the binary is
  renamed, and a `kill -9` between the two halves of a sequence does not cost
  the alert. Runs in CI.
- **Detect units** (ADR-0031): a VPL program under `detects/`, run by the
  Varpulis CEP engine embedded as a library — sequences, Kleene closures,
  negation, `.within()` in event time, windows, joins, forecast — with the
  contract a flow has: one durable consumer over its `.from()` subjects
  (stream order, so a sequence across subjects survives a backlog; several
  subjects need NATS 2.10), emit before ack, poison to the DLQ. `vejas-runtime vpl-check <file>` and the MCP tool
  `vejas_vpl_check` give the engine's verdict; `/topology` lists the units
  under `detects`; CI checks every `.vpl` and runs `e2e/detect/run.sh`. The
  engine adds no async runtime to the binary (5.9 → 8.1 MB). Measured:
  18 643 evt/s on the merge evaluation's program (16 publishers,
  64 000 events; the flow unit runs it at 16 182). A unit with state (a
  sequence, a window, a join) snapshots it to the object store
  `VEJAS_DETECT_STATE` with the stream sequence it stands for
  (`VEJAS_SNAPSHOT_SECS`, `VEJAS_SNAPSHOT_ACKS`) and, on restart, restores it
  and resumes its consumer after that sequence: what was acked since replays,
  at least once. Not yet: the panel graph and the business surface of a VPL
  program. See the book, *Detect units*.

### Changed
- The flow consumer loop no longer waits on itself: the pull request is
  flushed the moment it is made instead of sitting behind the sync client's
  5 ms flusher floor, messages are processed as they arrive instead of after
  the whole batch has landed, the next pull is kept in flight while a batch is
  processed, acks go out on a connection of their own, and the trace ring
  derives an event's preview on read instead of serialising every message a
  second time. Isolated flow hop 8 110 → 13 652 evt/s; the merge evaluation's
  flow 9 003 → 16 133 evt/s (16 publishers, 64 000 events, same box). The
  contract is unchanged — publish before ack, at-least-once (transport
  invariants T1–T5 pass; kill -9 mid-stream: 20 000/20 000 with 4
  duplicates, 0 lost). New on `/metrics`: `vejas_fetch_rounds_total`,
  `vejas_fetch_messages_total` and `vejas_round_seconds_sum{phase}` say where
  a unit's time goes. See `bench/README.md`, findings #6 and #7.

### Fixed
- `vejas-sap-rfc` (connectors/sap-rfc): a lost RFC conversation no longer
  poisons the connector for the rest of its life. The gateway (or a NAT in
  between) closes RFC connections left idle for a while; every later call then
  failed with `RFC_COMMUNICATION_FAILURE … no conversation found` until the
  process was restarted. The connector now pings a connection idle for more
  than a minute before using it, and when a call still finds the conversation
  gone (`RFC_COMMUNICATION_FAILURE`, `RFC_CLOSED`, `RFC_INVALID_HANDLE`) it
  reopens the connection and replays the call once. `SAP_RFC_TEST_HOOKS=1`
  enables `{"op":"_drop"}` to exercise that path.

## [0.2.0] — 2026-08-24

A security-hardening release. The write gate is now keyed on what a flow
actually *does* to the bus — not the HTTP verb — closing four HIGH findings from
an independent adversarial audit (R7). Adds the service-composition guide with an
executable CI guarantee, and corrects the published production numbers.

### Security — R7 audit (four HIGH, all fixed and frozen in CI)
- The write gate authorizes by **mutation, not HTTP verb**: it keys on
  `Program::writes_bus()`, an exhaustive AST walk over `emit` *and* `invoke`, so
  a bus write can no longer slip through a non-POST route (Finding A), a dynamic
  emit subject (Finding A′), or an invoke-mediated read (Finding A″). The
  fail-closed guarantees are frozen as end-to-end tests.
- `/connectors/new` is gated behind governed mode + the cluster guard, closing a
  connector-bypass path to command execution (Finding B).
- The three-way audit pass (A / A′ / A″ / B, plus the S4-1 secret-exposure note)
  is recorded in the security-audit scope.

### Language & docs
- **Composing services**: a new concept page for `invoke` (pipeline-merge
  composition) and cross-package `EXPORTS`, backed by an executable guarantee in
  CI — a service invoked from a flow merges its output into the caller, and the
  doc-contracts leg fails if that ever drifts.

### Corrected & honest
- Published numbers re-measured, not claimed (binary size, cold start, latency
  percentiles), each linked to its bench script.
- The Reglyze production line made accurate: young but real — two production
  customers, collecting NIS2 supplier-compliance evidence across eight EU
  countries today.
- Launch distribution kit added (`docs/launch/`): Show HN as a URL post + a
  seeded first comment.

## [0.1.0] — 2026-08-23

First tagged release. The platform is public, measured, certified and in
production (NIS2 evidence collection across four EU countries).

### Runtime & language
- One Rust binary on NATS/JetStream — the only infrastructure dependency.
- VejasScript: a pure per-event language; the business surface (thresholds,
  transcoding tables, rules) is extracted from the code and edited by domain
  experts in the panel, no code touched (ADR-0001, ADR-0005, ADR-0019).
- The runtime **is** the MCP server: agents read the language, write flows,
  test them, and they land running (ADR-0006).

### Delivery & operations
- At-least-once, every hop persisted; dead-letter queue with explicit replay
  (ADR-0015). Observability: Prometheus `/metrics` + optional OTLP (ADR-0016).
- Clustering with no coordinator: competing consumers, singleton leases,
  cluster-wide version promote measured at 60 ms, lossless (ADR-0020/0021).
- Change safely: time-travel over real traffic, shadow canary, audited
  promote/rollback; governed mode where agents propose and a human approves
  with a distinct credential (ADR-0021, ADR-0024).

### Connectors
- Twenty-one certified recipes admitted by CI; brokers as first-class
  citizens — MQTT and AMQP/RabbitMQ certified against real brokers each run,
  Kafka and IBM MQ under stated exceptions with real-instance verification
  (ADR-0022/0023/0025/0026). SAP and Salesforce standalone binaries (ADR-0014).

### Container image
- The release image is published to `ghcr.io/cpoder/vejas-runtime`
  (`:v0.1.0` and `:latest`).

### Measured (8-core dev machine; `bench/`)
- Cold start 11 ms · 6–8 MB RSS · end-to-end p50 2 ms uncongested ·
  2 285 rt/s through a real MQTT broker · cluster promote 60 ms, lossless.

### Security
- Secrets never literal (ADR-0008). Write surface behind an optional bearer
  token; governed mode adds a distinct approval token. Path traversal
  (including symlink escape) contained and regression-tested in CI.

[Unreleased]: https://github.com/cpoder/vejas/compare/v0.7.0...HEAD
[0.7.0]: https://github.com/cpoder/vejas/releases/tag/v0.7.0
[0.6.0]: https://github.com/cpoder/vejas/releases/tag/v0.6.0
[0.5.1]: https://github.com/cpoder/vejas/releases/tag/v0.5.1
[0.5.0]: https://github.com/cpoder/vejas/releases/tag/v0.5.0
[0.4.0]: https://github.com/cpoder/vejas/releases/tag/v0.4.0
[0.3.6]: https://github.com/cpoder/vejas/releases/tag/v0.3.6
[0.3.5]: https://github.com/cpoder/vejas/releases/tag/v0.3.5
[0.3.4]: https://github.com/cpoder/vejas/releases/tag/v0.3.4
[0.3.3]: https://github.com/cpoder/vejas/releases/tag/v0.3.3
[0.3.2]: https://github.com/cpoder/vejas/releases/tag/v0.3.2
[0.3.1]: https://github.com/cpoder/vejas/releases/tag/v0.3.1
[0.3.0]: https://github.com/cpoder/vejas/releases/tag/v0.3.0
[0.1.0]: https://github.com/cpoder/vejas/releases/tag/v0.1.0
