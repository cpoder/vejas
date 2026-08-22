# The cookbook — say it, and it lands running

You don't write VejasScript to use Vejas. You connect the agent you already
use to the runtime over MCP, and you say what you want. The agent reads the
language reference (`vejas_language`), writes the flow, tests it on a fixture
(`vejas_run_flow`), and deploys it (`vejas_write_flow`) — it lands running.

```bash
docker compose up
claude mcp add --transport http vejas http://localhost:8686/mcp   # or any MCP client
```

Each recipe below is a **prompt you say to your agent**, the **file that
lands**, and how to check it. The flows shown are real files from this repo.
Prompts marked ✓ were exercised against a live runtime by a real agent;
the others follow the same contract.

---

## 1 · Your first flow — ticket alerts ✓

> Watch helpdesk tickets on the bus at `vx.helpdesk.tickets`. Tickets carry a
> French `priority` label (critique, haute, normale, basse) — map it to a
> severity code P1–P4, and post the P1s and P2s to Slack with the subject and
> the requester's email.

What lands — `flows/helpdesk_ticket_alerts.vjs`:

```
# flow: helpdesk_ticket_alerts
source "vx.helpdesk.tickets"

# French priority label -> severity code. The business expert edits this table.
SEVERITY_CODES = {"critique": "P1", "haute": "P2", "normale": "P3", "basse": "P4"}
ALERT_SEVERITIES = ["P1", "P2"]

severity = SEVERITY_CODES[priority] ?? "P3"

if severity in ALERT_SEVERITIES:
    invoke format_alert(sev: severity, subj: subject, email: requester?.email)
    emit "vx.slack.out", {text: alert_text}
end
```

Note what the agent did without being told: the mapping table and the alert
list are **UPPERCASE literals** — so they appear in the panel where a
support lead can add a priority or stop paging on P2s, without touching code.

Check it:

```bash
curl -X POST localhost:8787/ingest/helpdesk.tickets \
  -d '{"priority":"critique","subject":"Panne totale","requester":{"email":"a@b.co"}}'
# then watch the event and its emit in the panel, or: curl localhost:8686/events
```

## 2 · Shop orders into the ERP

> New shop orders arrive on `vx.shop.orders`. Map country names to ISO codes,
> lowercase the customer email, turn `line_items` into ERP positions (sku,
> integer qty, unit price in euros from cents), and push the result to the
> ERP queue.

What lands — `flows/order_sync.vjs`: a transcoding table (`COUNTRY_CODES`),
`num`/`round` conversions, a projection loop, one `emit`. The table and the
target queue are the business surface.

## 3 · A connector from one sentence ✓

> Poll the GitHub status API every 60 seconds and publish it on the bus.

What lands — a manifest, not code (`connectors/github_status_poll.vjs`):

```
driver "http-poll"
URL = "https://www.githubstatus.com/api/v2/status.json"
SUBJECT = "vx.github.status"
INTERVAL_SECS = 60
```

The agent picked the driver from the live catalog (`vejas_drivers`). The
interval is a literal: change it in the panel, the connector restarts itself.

## 4 · A credential that never becomes a literal ✓

> Add a Slack sink for `vx.secure.out`. The webhook URL is confidential —
> take it from the vault under `slack/webhook`.

```
driver "slack-out"
SUBJECT = "vx.secure.out"
WEBHOOK_URL = secret("slack/webhook")
```

`secret()` resolves at run time, fail-closed. The value never appears in the
file, the panel, or `git diff` — agents are held to that contract by the
generation loop itself (ADR-0008).

## 5 · Bridge two SaaS — ServiceNow incidents into Jira

> Poll our ServiceNow for active incidents every 5 minutes (basic auth from
> the vault under `servicenow/basic`). For every P1 incident, create a Jira
> issue in project OPS with the incident number and short description
> (Jira credentials under `jira/basic`).

What lands: **two manifests and one flow** — `servicenow_incidents_poll`
(http-poll + `ENVELOPE`), a flow that filters `body.result[]` on priority
and shapes the Jira `fields` document, and `jira_create_issue` (http-out).
Full recipes: [`docs/examples/connectors/`](examples/connectors/).

## 6 · A flow that is an API

> Give me a REST endpoint POST /orders that validates the body, computes the
> total in euros, and answers 201 with the enriched order — and document it.

What lands — a flow with an `api` directive instead of a `source`:

```
api "POST /orders"
...
respond 201, {order: order_id, total_eur: total_eur, positions: positions}
```

It is served at `/api/orders`, and `GET /api/openapi.json` documents it —
generated from the flows themselves. One file per verb composes a resource
([`docs/examples/rest-api/`](examples/rest-api/)).

## 7 · Talk to SAP

> List the BAPIs matching BAPI_COMPANY* on our SAP, describe the first one,
> and call it.

No flow needed — with a SAP connector running (`exec-rpc`, ADR-0014), the
agent has `sap_list`, `sap_describe`, `sap_call` and `sap_send_idoc` as MCP
tools. Flows then productionize what the agent explored by hand
([`docs/examples/sap_rpc.vjs.example`](examples/sap_rpc.vjs.example)).

---

## Correcting meaning, in plain words

The other half of the thesis: corrections go through the same conversation.

> In the order flow, amounts are arriving in cents but we treat them as
> euros. Fix the meaning, show me the impact on real traffic before it lands.

The agent edits nothing blindly: it calls `vejas_replay_literal` — the
change is replayed on the flow's **last real events**, and comes back as a
before/after diff of what would have been emitted. You approve; it promotes
with `vejas_set_literal`. The bus was never touched during the rehearsal
(ADR-0005).

> The severity table is missing "bloquante" — it should page like a P1.

Same loop, one table entry: replay, diff, promote. A domain expert can do
this one **without any agent** — it is the panel's Apply → shadow-replay →
Promote button path. The prompt and the panel are two doors to the same
governed change.

---

*The generation contract behind every recipe: the agent must read
`vejas_language` first, literals for anything a human may want to correct,
`secret()` for anything confidential, and a fixture + `vejas_run_flow` test
before it ships. That contract is enforced by the tool descriptions the
agent sees — not by hope.*
