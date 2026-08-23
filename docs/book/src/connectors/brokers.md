# Brokers: Kafka, MQTT, AMQP, IBM MQ

Each broker got the integration its protocol deserves — a deliberate
spike-then-decide per broker (ADRs 0022, 0023, 0025, 0026, 0027):

| Broker | Shape | Why |
|---|---|---|
| **MQTT** | Hand-rolled 3.1.1 client, in-binary | The protocol is small; QoS 1 maps our at-least-once natively (source holds PUBACK until the bus confirms; sink acks the bus after PUBACK). Measured: 2 285 rt/s through a real mosquitto, QoS 1 both ways. |
| **Kafka** | Exec-bridge over `kcat` | The auth matrix (TLS, SASL, Kerberos) rides librdkafka *in the child*, never in our binary. Offsets in our KV: publish-before-commit, cadenced (100 ms), kill -9-proof in CI. |
| **AMQP / RabbitMQ** | First-class binary, pure Rust | amiquip (sync, no tokio) + TLS via rustls over its mio loop — no OpenSSL anywhere. Consume→pub-ack→ack; publish→confirm→ack. Certified against a real RabbitMQ in CI. |
| **IBM MQ** | First-class transactional binary | The no-loss guarantee is an *ordering*: MQGET under syncpoint → bus pub-ack → MQCMIT, in one process. Hand-declared MQI FFI, `dlopen` at runtime — builds with no MQ installed. MQCSP user/password auth. Verified live against a real queue manager: ordered drain, sink, and a bus-outage backout with zero loss. |
| **Pulsar** | Deferred, documented | The client crate imposes tokio + OpenSSL as hard deps and ~233 crates — the highest cost of the wave for the least established demand. The build path is written down for when a real user asks (ADR-0027). |

Operational notes that matter:

- **Singleton because order** (MQ, AMQP, Kafka sources): instances contend
  on a KV lease; exactly one consumes. `*_COMPETING=1` opts into competing
  consumers when throughput outranks global order — destructive reads make
  competing duplicate-safe.
- **IBM MQ packaging**: the redistributable client needs its full directory
  structure; `lib64` alone segfaults inside `libmqic`. Point `VEJAS_MQ_LIB`
  at `<extract>/lib64/libmqic_r.so`.
- Recipes: [`mqtt_source`… `mq_sink`](https://github.com/cpoder/vejas/tree/master/docs/examples/connectors) — env families in each `.env.example`.
