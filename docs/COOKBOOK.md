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

**Every prompt marked ✓ was replayed on 2026-08-22 by a real agent
(`claude -p`, MCP only, no repo access) against a fresh local runtime — and
validated on the outcome**: the file landed, parsed, and behaved (fixtures
run, API answered, tables corrected). Agents routinely landed *more* than
the recipe shows: normalized inputs, DRY-RUN sinks with the `secret()` line
ready, staleness envelopes — each choice argued in their report.

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

## 2 · Shop orders into the ERP ✓

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

## 5 · Bridge two SaaS — ServiceNow incidents into Jira ✓

> Poll our ServiceNow for active incidents every 5 minutes (basic auth from
> the vault under `servicenow/basic`). For every P1 incident, create a Jira
> issue in project OPS with the incident number and short description
> (Jira credentials under `jira/basic`).

What lands: **two manifests and one flow** — `servicenow_incidents_poll`
(http-poll + `ENVELOPE`), a flow that filters `body.result[]` on priority
and shapes the Jira `fields` document, and `jira_create_issue` (http-out).
Full recipes: [`docs/examples/connectors/`](examples/connectors/).

## 6 · A flow that is an API ✓

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

## 7 · Talk to SAP *(needs a live SAP — exercised in the recorded demo, not in the local pass)*

> List the BAPIs matching BAPI_COMPANY* on our SAP, describe the first one,
> and call it.

No flow needed — with a SAP connector running (`exec-rpc`, ADR-0014), the
agent has `sap_list`, `sap_describe`, `sap_call` and `sap_send_idoc` as MCP
tools. Flows then productionize what the agent explored by hand
([`docs/examples/sap_rpc.vjs.example`](examples/sap_rpc.vjs.example)).

---

## 8 · IBM MQ, transactionally *(mechanics CI-proven against a fake broker; needs a live queue manager for the last mile — ADR-0023)*

> Consume DEV.QUEUE.1 on queue manager QM1 and put every message on the bus
> as `vx.mq.orders.in`. Nothing may ever be lost, even if we crash mid-way.

What lands is not a manifest — IBM MQ is a **first-class standalone binary**
(`connectors/mq`), because the no-loss guarantee is an *ordering* of calls
that must live in one process: MQGET under syncpoint → bus publish (await
the JetStream pub-ack) → MQCMIT. Crash anywhere before the commit and the
queue manager re-delivers: at-least-once, the qmgr is the durable cursor.
The agent fills the env recipe instead
([`docs/examples/connectors/mq_source/`](examples/connectors/mq_source/)):

```
MQSERVER="DEV.APP.SVRCONN/TCP/mq.example.com(1414)"
VEJAS_MQ_MODE=source
VEJAS_MQ_QMGR=QM1
VEJAS_MQ_QUEUE=DEV.QUEUE.1
VEJAS_MQ_SUBJECT=vx.mq.orders.in
```

The mirror (`mq_sink`) delivers a bus subject into a queue, with an optional
JSON field riding MQMD CorrelId for consumer-side dedup. Singleton by
default *because order* (a lease on the bus, ADR-0020); set
`VEJAS_MQ_COMPETING=1` when throughput outranks global order — MQGET is
destructive, so competing instances never duplicate.

## 9 · RabbitMQ, certified against the real thing ✓

> Consume the `orders` queue on our RabbitMQ (TLS, credentials from the
> vault) and put every message on the bus as `vx.amqp.orders.in`.

Like IBM MQ, a **first-class standalone binary** (`connectors/amqp`) — pure
Rust, synchronous, no tokio, and TLS through rustls (no OpenSSL anywhere).
The at-least-once contract is the platform's, spoken in AMQP: the source
acks RabbitMQ only after the bus pub-ack; the sink acks the bus only after
the publisher confirm. The agent fills the env recipe
([`docs/examples/connectors/amqp_source/`](examples/connectors/amqp_source/)):

```
VEJAS_AMQP_URL="${AMQP_URL:?from your secret store}"   # amqps://… for TLS
VEJAS_AMQP_MODE=source
VEJAS_AMQP_QUEUE=orders
VEJAS_AMQP_SUBJECT=vx.amqp.orders.in
```

Unlike the brokers CI cannot host, these recipes are certified **against a
real RabbitMQ container on every CI run** — the message goes through the
actual broker and back.

## Governed mode — the agent proposes, the human approves ✓

> From here on, no agent lands a change directly. Everything goes through
> me.

Two env vars make governance a product step instead of a convention
(ADR-0024): `VEJAS_REQUIRE_APPROVAL=1`, and `VEJAS_APPROVAL_TOKEN` — a
credential **distinct from the agent's** `VEJAS_TOKEN`, so holding the MCP
key never implies holding the approve key. Then, validated live:

1. Any direct write — agent tool or `POST /surface/set` — answers with the
   didactic refusal: *"approval required: submit a proposal (vejas_propose,
   or the panel) — a human approves it"*.
2. The agent calls `vejas_propose` (`kind: set_literal` or `kind: version`),
   attaching the evidence it gathered — time-travel results, canary stats.
   The proposal lands `pending`, pinned to the current baseline hash; if a
   deploy or promote moves the baseline first, it **expires loudly** rather
   than landing on a base the evidence never saw.
3. The queue is one list for everyone — `GET /proposals`, the panel card, or
   the agent's read-only `vejas_proposals`. The panel shows the evidence
   next to Approve, and flags **"no evidence"** loudly when absent.
4. A human approves in the panel or with the token —
   `POST /proposals/{id}/approve` (or `/reject`) with `X-Approval-Token`.
   Without the header: 401. On approve, the existing paths execute — a
   `version` proposal promotes cluster-wide through ADR-0021 — and the audit
   record carries the proposal id.

Every transition emits on `vx.proposals.events` — so "notify on-call when a
proposal is pending" is just a flow, routed to Slack or PagerDuty with the
connectors above. No notification subsystem; the platform routes its own
governance.

## Correcting meaning, in plain words

The other half of the thesis: corrections go through the same conversation.

> In the order flow, amounts are arriving in cents but we treat them as
> euros. Fix the meaning, show me the impact on real traffic before it lands.

The agent edits nothing blindly: it calls `vejas_replay_literal` — the
change is replayed on the flow's **last real events**, and comes back as a
before/after diff of what would have been emitted. You approve; it promotes
with `vejas_set_literal`. The bus was never touched during the rehearsal
(ADR-0005).

> The severity table is missing "bloquante" — it should page like a P1. ✓

Same loop, one table entry: replay, diff, promote. A domain expert can do
this one **without any agent** — it is the panel's Apply → shadow-replay →
Promote button path. The prompt and the panel are two doors to the same
governed change.

And the expert *reads* the rules before touching anything: every flow card
carries a **Rules** section — the flow's `if/elif/else`, projected straight
from the AST as readable sentences (« WHEN total ≥ MIN_TOTAL_EUR THEN
→ vx.erp.orders »), the thresholds and tables in them editable inline
through the same replay-promote loop. A condition too complex to render
*exactly* is shown as its verbatim source, marked *advanced* — never a
lossy paraphrase — and changing a rule's *structure* goes through the
agent, in plain words (ADR-0019). Reading is free; meaning is governed. *(The validation pass ran exactly this prompt on real
traffic: two silent `bloquante` tickets, then the correction, then the next
`bloquante` alerted — and caught a promote-no-op bug on the way (sub-second mtime),
fixed and re-validated live: two consecutive promotes, each visible in the
next event within seconds. Validation that finds bugs is validation working.)*

## Change a whole version, safely ✓

> Here is the fix for the helpdesk flow: escalate the new "bloquante"
> priority and add an `escalated` field to the alert. Test it against
> yesterday's real traffic first, then watch it against live traffic —
> nothing ships until I say so.

The agent's path, every step governed:

1. It writes the **candidate** version (a full flow, never deployed).
2. `vejas_time_travel` — the candidate replays a window of **historical
   real traffic** side by side with the live version; the diff comes back
   joined per event (`seq`, before/after emits). Nothing is published — the
   shadow invariant is structural, not a setting.
3. `vejas_canary_start` — the candidate now shadow-follows **live**
   traffic; `vejas_canary_status` shows the accumulating diff
   (`{events, changed}`). Still zero real emits. A promote or deploy that
   changes the baseline under the canary stops it with a reason — never a
   silently stale diff.
4. On your word: **promote**. In a cluster, that publishes a version every
   instance converges on — measured at 60 ms, lossless mid-burst — with an
   audit record (actor, from-hash → to-hash). Rollback is a promote to any
   previous version, forward-only, previewable the same way.

And the loop closes underneath: a dead letter's envelope carries the
**version** that killed the message — replay it after the fix and the
transition is on record (ADR-0021).

---

*The generation contract behind every recipe: the agent must read
`vejas_language` first, literals for anything a human may want to correct,
`secret()` for anything confidential, and a fixture + `vejas_run_flow` test
before it ships. That contract is enforced by the tool descriptions the
agent sees — not by hope.*
