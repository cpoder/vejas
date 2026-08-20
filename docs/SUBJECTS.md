# The subject convention (this is the whole connector interface)

Everything on the bus lives under one subject root: `vx.` (configurable via
`VEJAS_SUBJECT_ROOT`). One JetStream stream named `VEJAS` binds `vx.>`.

Bundled connectors are **native Rust drivers** run from a declarative manifest
(`connectors/<name>.vjs`: `driver "..."` + literal config, editable in the panel,
hot-addable). No subprocess, no Python. Drivers today:

- **http-in** (source:webhook) — `POST /ingest/<suffix>` → `vx.<suffix>`. Config: PORT.
- **timer** (source:interval) — emits PAYLOAD on SUBJECT every INTERVAL_SECS.
- **http-poll** (source:poll) — GETs URL every INTERVAL_SECS → SUBJECT. Optional HEADERS.
- **oauth-poll** (source:poll) — OAuth2 client-credentials REST poller: token from
  TOKEN_URL (CLIENT_SECRET via `secret()`), GETs each of ENDPOINTS with the Bearer,
  pagination via NEXT_LINK_FIELD (default `@odata.nextLink`, absolute links followed
  as-is) capped by MAX_PAGES, publishes one `{endpoint, fetched_at, body}` message
  per page on SUBJECT. One generic OAuth+REST driver stands in for most of a
  connector catalog.
- **slack-out** (sink) — consumes vx.slack.out → Slack webhook / DRY-RUN.
- **http-out** (sink) — consumes SUBJECT → POST to URL. Optional HEADERS doc for
  authenticated pushes, values via `secret()`:
  `HEADERS = {"Authorization": f"Bearer {secret("acme/api_token")}"}`.
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
