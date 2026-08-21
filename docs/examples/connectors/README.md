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
