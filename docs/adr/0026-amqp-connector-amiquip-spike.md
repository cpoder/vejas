# 0026 — AMQP 0-9-1 (RabbitMQ): a sync connector on amiquip

- Status: Proposed (spike — prototype in `connectors/amqp/`)
- Date: 2026-08-23

## Context

AMQP 0-9-1 (RabbitMQ is the dominant server) is squarely in the "brokers wave"
enterprises actually run. Unlike MQTT (ADR-0025), AMQP 0-9-1 is a big protocol —
connection/channel negotiation, exchange/queue/binding management, the basic
class, frame multiplexing, heartbeats. Hand-rolling it the way we hand-rolled the
~3.1.1-line MQTT client would be a large, error-prone effort for little gain: a
mature sync client already exists.

The client this spike evaluated is **amiquip** (0.4.2) — Cyril named it: sync,
pure-Rust, no async runtime. The transactional shape is also *simpler* than IBM MQ
(ADR-0023): AMQP has no two-phase syncpoint. `basic.ack` **is** the commit, so the
same side-effect-before-ack rule (SUBJECTS.md rule 3) covers both directions with
one ack, not a get/commit pair.

## What the spike found

**amiquip is genuinely sync — no tokio, no async-std, no futures.** It runs its own
I/O thread over `mio` (0.6-era) + `crossbeam-channel`; the public API is blocking
(`Connection::open`, `Channel::basic_consume`, `Delivery::ack`, `Channel::
basic_publish` + publisher confirms). It builds and its consume/ack/publish
surface fits the connector shape directly. Good fit on the core axis.

**The catch is TLS, and it is the footprint axis.** amiquip's `default` feature is
`native-tls` → `openssl-sys` — a **C dependency** that also failed to build here
without system OpenSSL headers. Three ways out, in order of preference:

1. **Plaintext, `default-features = false`** — drops openssl-sys entirely, pure-Rust
   build (verified: 23s clean). Correct for a broker on a trusted network / a
   sidecar on the same host. This is what the prototype uses today.
2. **rustls over a user stream (pure-Rust TLS, recommended for production).** amiquip
   exposes `insecure_open_stream<S: IoStream>(stream, …)` where `IoStream: Read +
   Write + mio::Evented + Send`. A ~30-line adapter — rustls `StreamOwned` over a
   `mio::net::TcpStream`, delegating `Evented` register/reregister/deregister to the
   inner TcpStream fd — satisfies it, giving amqps:// with **no C**. This is exactly
   the transport-wrapping the MQTT connector already does (ADR-0025), plus the mio
   delegation. Deferred to the build, not the spike.
3. **native-tls (openssl-sys)** — rejected as the default: a C TLS stack pulled into
   a connector is against the footprint thesis when a pure-Rust path exists.

**Weight.** amiquip drags ~70 transitive crates (amq-protocol codec + codegen) and,
through amq-protocol, an aging `nom 4.2.3` (a future-incompat warning today). This
is the price of not hand-rolling AMQP; it is contained to *this connector's* binary
(never the core), like every external connector. Judged acceptable for a protocol
this size — but noted, because a future amiquip that unmaintains would strand us on
that nom.

## Decision (proposed)

Build the AMQP connector as a **standalone first-class bus citizen** owning its own
NATS client — the exact ADR-0023 shape, because the ack ordering is the same
concern:

- **source (AMQP → bus):** `basic_consume` (manual ack, `no_ack = false`) → NATS
  publish (await pub-ack) → `Delivery::ack`. A crash before the ack redelivers the
  AMQP message (at-least-once, no loss). RabbitMQ is the durable cursor — no offset
  KV, like MQ.
- **sink (bus → AMQP):** NATS durable pull → `basic_publish` with **publisher
  confirms** (`confirm_select` + wait for the broker ack) → ack the NATS message.
  A crash between the confirm and the NATS ack redelivers → a duplicate publish;
  carry an idempotency key in a message property (`message_id`/`correlation_id`) for
  downstream dedup, as with MQ's CorrelId.
- **concurrency:** unlike MQ's destructive get, an AMQP queue with multiple
  consumers *is* the competing-consumers pattern and RabbitMQ round-robins — so N
  source consumers are duplication-safe. Order is still lost across N, so the same
  rule stands: **singleton lease by default (order), `COMPETING=1` to opt out** —
  reuse `connectors/mq/src/lease.rs`'s mechanism.

TLS: ship plaintext (`default-features = false`) now; add the rustls-over-`IoStream`
adapter for amqps:// before certifying a production recipe.

## Consequences

- A second first-class external citizen after MQ, on a mature sync client instead
  of hand-FFI — the two shapes (vendor-C-via-dlopen vs pure-Rust-crate) now both
  have a worked example.
- The connector builds pure-Rust in plaintext; the only C temptation (native-tls) is
  designed out via the rustls adapter path.
- Real-broker certification (against RabbitMQ) is the declared CI exception
  (ADR-0017), like MQ — the prototype verifies the loop logic against a fake channel
  and the graceful-failure paths; the amiquip-vs-live-RabbitMQ behaviour is
  certified out-of-band with a RabbitMQ container.

## Open questions (for review / the build)

1. The rustls-`IoStream` adapter — confirm the `Evented` delegation drives amiquip's
   mio loop correctly through a TLS handshake (WouldBlock during handshake is the
   fiddly bit). Prototype it against a TLS RabbitMQ before the production recipe.
2. Publisher-confirm batching — confirm-per-message is simplest and matches the
   ack-per-message pull; if throughput needs it, confirm in batches and ack the bus
   batch only after the batch confirm. Measure on the bench first.
3. amq-protocol's `nom 4.2.3` future-incompat — track amiquip maintenance; if it
   stalls, the fallback is a heavier lift (another crate, or hand-rolling), so this
   is the one strategic risk to watch.

## Rejected

- **Hand-rolling AMQP 0-9-1** (the MQTT move) — the protocol is an order of magnitude
  larger than MQTT 3.1.1; a mature sync client is the right call here.
- **native-tls as the TLS story** — C dependency in a connector when rustls over the
  user stream is available (see decision).
- **An async client (lapin) + a tokio runtime** — drags an async runtime into a
  connector whose every other I/O (NATS, the bus) is blocking; amiquip's sync model
  matches the rest of the platform.
