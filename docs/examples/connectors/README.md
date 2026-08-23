# Connector manifests on the socle: Slack, ServiceNow, Jira, Workday

The point Cyril made after SAP: SAP is the *hard* case (a native protocol, a C
SDK, IDocs). Most enterprise connectors are OAuth2 / REST — so they are **just
manifests on the existing drivers**, no new binary:

- **read** → `http-poll` (GET a URL every INTERVAL, publish the JSON) or
  `oauth-poll` (OAuth2 client-credentials).
- **write** → `http-out` (consume a subject, POST each message to a URL).

Credentials always ride in `HEADERS` via `secret()` — never a literal (ADR-0008).
For Basic-auth APIs, store the pre-encoded `base64("user:password")` as the
secret and use `f"Basic {secret(\"…\")}"`.

| File | Driver | What it does |
|---|---|---|
| `slack_post.vjs.example`                | `http-out`  | post messages (Web API `chat.postMessage`, bot token) |
| `servicenow_incidents_poll.vjs.example` | `http-poll` | read incidents (Table API) |
| `servicenow_create_incident.vjs.example`| `http-out`  | create incidents (Table API) |
| `jira_issues_poll.vjs.example`          | `http-poll` | read issues (Cloud REST v3, JQL) |
| `jira_create_issue.vjs.example`         | `http-out`  | create an issue (Cloud REST v3) |
| `workday_raas_poll.vjs.example`         | `http-poll` | read a Workday RaaS report (JSON) |

## Using one

1. Copy the `.vjs.example` into your Vejas root as `connectors/<name>.vjs`.
2. Fill in the instance/site URL and set the secret it references
   (`vejas_set_secret`, e.g. `jira/basic` = `base64("me@corp.com:api_token")`).
3. Wire a flow: read connectors publish to their `SUBJECT` (a flow `source`s it);
   write connectors consume a `SUBJECT` (a flow `emit`s to it). The mapping in
   between is the editable business surface.

## Notes / what's next

- **Slack** also works with an incoming-webhook (`slack-out`) for the simplest
  "post to one channel" case; the Web API (`http-out` above) is for bot-token
  posting to any channel and richer payloads.
- **Workday**: the RaaS (Report-as-a-Service) read path is covered here. Workday
  *transactional* operations use the SOAP Web Services (WWS) — that needs a
  native/exec connector (like SAP), a candidate for a future increment.
- These example endpoints and payloads are accurate to each vendor's API but are
  **not** validated against a live tenant here (no accounts) — set your
  credentials and they run on the socle that Reglyze's Microsoft Graph / EDR
  connectors already use in production.

## Certified recipes (ADR-0017)

Each directory here is a **certified** recipe: the manifest, a golden
`fixture.json` (what it publishes or consumes), a `mock.mjs` stand-in for
the remote, and `overrides.json` (which literals the admission job points at
the mock, which dummy secrets it seeds). CI admits every recipe on every
push: lint (credentials must be `secret()` — the pattern is single-sourced
from the runtime), parse, boot against the mock, green probe, and the data
actually flowing. Run locally: `e2e/admission/run.sh [name]`.

A flat `.vjs.example` outside a directory is a draft, not a connector.

**Twenty-one certified recipes**: ServiceNow (poll + create), Jira (poll +
create), Slack, Workday RaaS, Stripe events, Shopify order webhook,
SendGrid email, GitHub issues, PagerDuty events, Discord webhook — every
one admitted by CI against its mock, credentials vaulted, data flow
proven — plus **MQTT source and sink** (the hand-rolled in-binary client,
ADR-0025) certified against a **real mosquitto** in CI, and **AMQP/RabbitMQ
source and sink** (the first-class pure-Rust binary, ADR-0026) certified
against a **real RabbitMQ** in CI (each recipe ships its own `broker.sh`
and `dataflow.sh`: through the broker and back) — and **Kafka source and
sink** (exec-bridge over kcat, ADR-0022) and **IBM MQ source and sink**
(the first-class transactional binary, ADR-0023; an `.env.example` instead
of a manifest — same credential lint, enforced on env keys),
both admitted under a **stated exception**: a meaningful broker mock would
be a broker, so CI enforces the lint (+ the offset-resume kill -9 test for
Kafka; the crate's fault-injection invariant tests for MQ — plus a
real-queue-manager verification transcript against the MQ Developer
container), and the exception file says exactly what to verify against a
dev broker. The webhook recipes (Shopify, and `webhook_scoped_ingest` which locks the
unauthenticated port to its subjects with `ALLOW` — out-of-list → 403) are
tested end to end through their own ingest: POST the fixture, see it on the
bus.
