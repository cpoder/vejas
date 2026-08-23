# The certified catalog

**Twenty recipes**, every one *admitted* by CI — not "documented", proven:
credential lint (no literal secrets, single-sourced pattern), parse, then a
real data-flow check. The recipes live in
[`docs/examples/connectors/`](https://github.com/cpoder/vejas/tree/master/docs/examples/connectors)
— copy the manifest, fill your instance URL, set the secret it references,
wire a flow.

| Family | Recipes | Certified against |
|---|---|---|
| SaaS (http-poll / oauth-poll / http-out) | ServiceNow ×2, Jira ×2, Slack, Workday RaaS, Stripe, SendGrid, GitHub, PagerDuty, Discord | A mock through the product's own write path — data must flow |
| Webhook in | Shopify orders | Its own ingest: POST the fixture, see it on the bus |
| MQTT | source + sink | **A real mosquitto on every CI run** (incl. a 50-message QoS-1 burst) |
| AMQP/RabbitMQ | source + sink | **A real RabbitMQ on every CI run** (through the broker and back) |
| Kafka | source + sink (kcat bridge) | Stated exception + the offset-resume kill -9 test in CI |
| IBM MQ | source + sink (transactional binary) | Stated exception + a real-queue-manager verification transcript (MQCSP auth, live backout) |

Three certification regimes, honestly labeled:

- **Mock-certified**: the admission boots a throwaway runtime + the
  recipe's mock; the probe must pass and a real message must flow.
- **Real-broker-certified**: the recipe ships `broker.sh` (a throwaway
  container) and `dataflow.sh`; CI sends messages through the actual
  broker.
- **Stated exception**: when a meaningful mock would *be* the system
  (a Kafka broker, a queue manager), the exception file says exactly what
  CI proves instead and what to verify against a dev instance — the first
  line is printed in the CI log, never silent.
