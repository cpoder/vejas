# Changelog

All notable changes to Vejas are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html). While the major
is `0`, minor versions may carry breaking changes — they are called out here.

## [Unreleased]

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

[Unreleased]: https://github.com/cpoder/vejas/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/cpoder/vejas/releases/tag/v0.4.0
[0.3.6]: https://github.com/cpoder/vejas/releases/tag/v0.3.6
[0.3.5]: https://github.com/cpoder/vejas/releases/tag/v0.3.5
[0.3.4]: https://github.com/cpoder/vejas/releases/tag/v0.3.4
[0.3.3]: https://github.com/cpoder/vejas/releases/tag/v0.3.3
[0.3.2]: https://github.com/cpoder/vejas/releases/tag/v0.3.2
[0.3.1]: https://github.com/cpoder/vejas/releases/tag/v0.3.1
[0.3.0]: https://github.com/cpoder/vejas/releases/tag/v0.3.0
[0.1.0]: https://github.com/cpoder/vejas/releases/tag/v0.1.0
