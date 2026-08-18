# The subject convention (this is the whole plugin interface)

Everything on the bus lives under one subject root: `vx.` (configurable via
`VEJAS_SUBJECT_ROOT`). One JetStream stream named `VEJAS` binds `vx.>`.

A **connector** is any process that follows these rules. Language, host and
supervisor are irrelevant; the bundled Python connectors are a convenience,
not a requirement.

1. Publish JSON, UTF-8 encoded, on `vx.<domain>.<name>` (e.g. `vx.stripe.events`).
2. Consume with a **durable pull consumer** whose durable name identifies you
   (e.g. `slack_out`). One consumer identity per logical connector.
3. Ack a message only after its side effect succeeded. On failure, `nak` with
   a delay. Redelivery is the retry mechanism; design side effects to be
   idempotent or deduplicate on your own key.
4. Ensure the `VEJAS` stream exists before first use (idempotent create).
5. Expose nothing else. No registry, no manifest, no RPC handshake. If you
   can do these four things, you are a Vejas connector.

Flows follow the same contract; the SDK (`sdk/python/vejas`) just does the
boring parts for you and adds one guarantee: everything `emit()`ed is
published before the incoming message is acked, so a crash means redelivery,
never a lost emit.
