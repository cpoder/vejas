# 0025 — MQTT: a hand-rolled sync client, not a dependency

- Status: Proposed
- Date: 2026-08-23

## Context

MQTT is the edge protocol — telemetry, IoT, Cumulocity-style device fleets. The
spike inverts the Kafka finding (ADR-0022): where Kafka's native path was
impossible (no SCRAM in pure-Rust, and rskafka pulls tokio), MQTT's native path is
*easy*, because MQTT 3.1.1 is a small binary protocol — a dozen packet types, a
varint length prefix, a keepalive.

The candidates:

- **rumqttc** — pure-Rust but **tokio**: the same footprint hit that ruled out
  rskafka. Out.
- **paho-mqtt** — a wrapper over the Paho **C** client: C-in-core, the thing we
  keep out of the binary. Out.
- **symqtt** (v0.1.1) — a genuine synchronous, no-async-runtime pure-Rust client
  (3.1.1/5.0, TLS via a transport crate). Promising, but v0.1.1, single
  maintainer, unproven.
- **Hand-rolled sync** — ~300–500 lines: CONNECT/CONNACK, PUBLISH/PUBACK,
  SUBSCRIBE/SUBACK, PINGREQ/PINGRESP, DISCONNECT, the remaining-length varint,
  keepalive. Blocking TCP, the same house style as the sync `http-in`/`http-out`
  code.

## Decision

**Hand-roll a synchronous MQTT 3.1.1 client, QoS 0/1** — a compiled-in driver, no
external dependency. Depending on a v0.1.1 single-maintainer crate for a protocol
this small is the wrong side of the risk when we have the precedent of hand-rolled
Prometheus/OTLP (ADR-0016), the DLQ, and the sync HTTP paths. Zero deps, all-Rust,
and it integrates our metrics/OTLP natively — an external `mosquitto` process
could not.

Four conditions make the hand-roll safe and sellable:

### 1. Conformance against a REAL mosquitto in CI

A hand-rolled wire protocol without a real-broker test is hubris. This connector
is where the **real-CI certification** starts: a tiny `mosquitto` container, the
admission runner extended with an optional per-recipe `broker.sh` (peer's lane,
landing alongside this driver). The client is tested against a real broker's
CONNACK/SUBACK/PUBACK/retransmit behaviour, not just its own encoder.

### 2. Publish-before-ack maps NATIVELY onto QoS 1 — the elegant part

Our end-to-end at-least-once invariant *is* the QoS 1 handshake, with no KV:

- **Source (MQTT → bus):** a QoS 1 PUBLISH arrives → publish to the bus (await
  the JetStream pub-ack) → **then** send the MQTT PUBACK to the broker. Until the
  PUBACK, the broker holds the message inflight and **retransmits** it, so a crash
  before our bus-publish re-delivers from the broker — at-least-once, no KV, the
  broker is the durable cursor (a lighter form of MQ's syncpoint, ADR-0023). This
  requires **`cleanSession = false`** so the subscription and its inflight queue
  survive a disconnect — decided, and documented in the recipe.
- **Sink (bus → MQTT):** durable pull → QoS 1 PUBLISH → await the broker's PUBACK
  → **then** ack the NATS message (side-effect-before-ack, SUBJECTS.md rule 3). A
  crash between PUBACK and the NATS ack redelivers → a duplicate PUBLISH; standard
  at-least-once, the downstream dedups on the app key if it must.

QoS 0 is fire-and-forget (at-most-once) — offered for pure telemetry where loss is
acceptable and rate is everything; QoS 1 is the default.

### 3. Disciplined scope

**3.1.1, QoS 0/1 only. No MQTT 5, no QoS 2.** The escape hatch for those is a
`mosquitto_pub`/`mosquitto_sub` exec-bridge (the kcat pattern, ADR-0022),
documented in the driver — an exotic need does not bloat the core client. Scope
discipline is what keeps a hand-rolled protocol maintainable.

## Consequences

- **The edge angle writes its own marketing.** "An MQTT client in a 5MB binary
  with zero dependencies" is a claim neither a tokio async stack nor a JVM broker
  client can make. MQTT lives at the edge, where footprint and cold-start are the
  whole game — this is exactly the Cumulocity-adjacent ground, and the in-binary,
  no-dependency story is a genuine differentiator, not a checkbox.
- No new dependency; the client is a few hundred lines of sync Rust, house-style,
  metrics/OTLP-native.
- We own the protocol — which the real-mosquitto conformance test (condition 1)
  is precisely there to make safe. A wire bug is caught by CI against a real
  broker, not in the field.
- QoS 2 / MQTT 5 users fall to the documented mosquitto exec-bridge; the vast
  majority (telemetry, IoT at QoS 0/1) run in-binary.

## Open questions (for review)

1. TLS: reuse the rustls path the runtime already links (ureq/rustls) for the MQTT
   TCP+TLS transport, or a thin direct rustls stream? Lean the latter (a raw TLS
   stream, no HTTP), keeping the client transport-agnostic over a `Read+Write`.
2. `cleanSession = false` requires a stable client id per source (so the broker
   ties the session to us) — derive it from the connector name; document that two
   instances must not share a client id (the singleton lease already guarantees
   one source, so this is consistent).
3. Keepalive/backoff tuning defaults — reuse the connector restart/backoff.

## Rejected

- **rumqttc / any tokio client** — the async runtime is the footprint the whole
  thesis avoids, for a protocol we can do synchronously.
- **paho-mqtt** — the Paho C client in our binary; C-in-core, rejected on the same
  grounds as rdkafka.
- **symqtt (v0.1.1)** — a real sync pure-Rust option, but taking a v0.1.1
  single-maintainer dependency for a protocol we can own cleanly is the wrong risk;
  revisit only if the hand-roll ever proves more burden than a proven crate.
- **mosquitto exec-bridge as the primary** — kept as the QoS 2 / MQTT 5 escape
  hatch, not the default: it adds an external dependency and can't integrate our
  metrics/OTLP the way an in-binary client does.
