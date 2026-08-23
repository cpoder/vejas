# The subject convention (this is the whole connector interface)

Everything on the bus lives under one subject root: `vx.` (configurable via
`VEJAS_SUBJECT_ROOT`). One JetStream stream named `VEJAS` binds `vx.>`.

Bundled connectors are **native Rust drivers** run from a declarative manifest
(`connectors/<name>.vjs`: `driver "..."` + literal config, editable in the panel,
hot-addable). No subprocess, no Python. Drivers today:

- **http-in** (source:webhook) — `POST /ingest/<suffix>` → `vx.<suffix>`. Config: PORT.
- **timer** (source:interval) — emits PAYLOAD on SUBJECT every INTERVAL_SECS
  (an object payload gains a `ts` field, ISO 8601 UTC, when absent).
- **http-poll** (source:poll) — GETs URL every INTERVAL_SECS → SUBJECT. Optional HEADERS; optional `ENVELOPE = true` publishes `{endpoint, fetched_at, body}` (like oauth-poll) so a stateless flow gets a `collected_at`.
- **oauth-poll** (source:poll) — OAuth2 client-credentials REST poller: token from
  TOKEN_URL (CLIENT_SECRET via `secret()`), GETs each of ENDPOINTS with the Bearer,
  pagination via NEXT_LINK_FIELD (default `@odata.nextLink`, absolute links followed
  as-is) capped by MAX_PAGES, publishes one `{endpoint, fetched_at, body}` message
  per page on SUBJECT. SCOPE is optional (omitted from the token form when empty —
  e.g. CrowdStrike). `EXPAND = [{name, list, detail, key, as, list_field?}]` adds a
  client-side $expand — every item of the list array (`list_field`, default
  `value`; a bare-string item becomes `{key: id}`, so CrowdStrike's
  `resources: [ids]` → per-id detail works) enriched with its detail call, the
  page shipped as one envelope — for list APIs without a server-side expand.
  One generic OAuth+REST driver stands in for most of a connector catalog.
- **slack-out** (sink) — consumes vx.slack.out → Slack webhook / DRY-RUN.
- **http-out** (sink) — consumes SUBJECT → POST to URL. Optional HEADERS doc for
  authenticated pushes, values via `secret()`:
  `HEADERS = {"Authorization": f"Bearer {secret("acme/api_token")}"}`.
- **mqtt-in** (source) / **mqtt-out** (sink) — a hand-rolled synchronous MQTT
  3.1.1 client, in-binary, zero dependency (ADR-0025). QoS 1 maps our
  at-least-once natively: the source holds the broker's PUBACK until the bus
  publish is confirmed; the sink acks the bus only after the broker's PUBACK.
  TLS / QoS 2 / MQTT 5 → the mosquitto exec-bridge escape hatch.
- **exec-source** / **exec-sink** — bridge an external program in ANY language over stdio (source prints JSON on stdout; sink reads JSON on stdin). The hot-add path for new connector types without recompiling the core or loading native libs (ADR-0011).

An **external connector** in any language is still a first-class citizen: it is
just a process that follows these rules on the same bus. Language, host and
supervisor are irrelevant.

1. Publish JSON, UTF-8, on `vx.<domain>.<name>`.
2. Consume with a **durable pull consumer** whose durable name identifies you.
3. Ack a message only after its side effect succeeded; on failure, `nak` with a
   delay. Redelivery is the retry mechanism; make side effects idempotent.
4. Ensure the `VEJAS` stream exists before first use (idempotent create).
5. Expose nothing else. No registry, no manifest, no RPC handshake.

Flows (VejasScript) follow the same contract; the runtime does the boring parts
and guarantees every `emit` is published before the incoming message is acked,
so a crash means redelivery, never a lost emit.

One sibling root is reserved: `vxc.<tenant>.>` — the remote-collector
control channel (leaf-node uplink, closed command allowlist, local approval
for content changes). It lives OUTSIDE `vx.>` on purpose: control traffic is
transient and must never be captured by a stream (a stream on the command
subject would hijack request/reply with its pub-ack). Specification:
[CONTROL.md](CONTROL.md), decision: ADR-0013.
