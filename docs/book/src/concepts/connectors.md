# Connectors

A connector puts external systems on the bus. There are three shapes, by
escalating need (ADR-0007/0011):

1. **A manifest on a built-in driver** — most SaaS is this: a `.vjs` file
   with `driver "http-poll"` (or `oauth-poll`, `http-out`, `http-in`,
   `mqtt-in/out`, `timer`, `slack-out`…) and UPPERCASE config. No code.
   The driver catalog with each config contract: `GET /drivers` or the
   `vejas_drivers` MCP tool.
2. **An exec bridge** — `exec-source` / `exec-sink` / `exec-stream-source`
   / `rpc:exec` run a child program in any language over stdio. Kafka rides
   this (kcat carries the full librdkafka auth matrix); the generic
   offset-resume (`OFFSET_KV`) gives publish-before-commit resume in our
   KV, kill -9-proof (CI-tested).
3. **A first-class standalone binary** — when the ordering that guarantees
   no-loss must live in one process with its own bus client: IBM MQ
   (MQGET-under-syncpoint → bus pub-ack → MQCMIT) and AMQP/RabbitMQ
   (consume→pub-ack→ack; publish→confirm→ack). Configured by env, not by
   manifest. See [Brokers](../connectors/brokers.md).

Whatever the shape, the same rules hold: subjects under `vx.`, at-least-once
with the ack *after* the side effect, credentials through `secret()` or the
deployment's secret machinery — never a literal (a CI lint enforces it on
every certified recipe), and an admission test that proves data actually
flows ([the certified catalog](../connectors/catalog.md)).
