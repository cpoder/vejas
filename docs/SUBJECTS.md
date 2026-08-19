# The subject convention (this is the whole connector interface)

Everything on the bus lives under one subject root: `vx.` (configurable via
`VEJAS_SUBJECT_ROOT`). One JetStream stream named `VEJAS` binds `vx.>`.

The bundled connectors run as **native Rust threads inside the runtime** — no
subprocess, no Python:

- **http-in** — `POST /ingest/<suffix>` publishes the JSON body on `vx.<suffix>`
  (e.g. `POST /ingest/stripe.events` → `vx.stripe.events`). Port `HTTP_IN_PORT`
  (default 8787).
- **slack-out** — durable pull consumer on `vx.slack.out`, posts `{text: ...}`
  to `SLACK_WEBHOOK_URL` (or logs a DRY-RUN line when unset).

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
