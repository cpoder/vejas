# Your first flow

Say it to your agent:

> Watch helpdesk tickets on `vx.helpdesk.tickets`. Priority arrives in
> French — map critique→P1, haute→P2, otherwise P3 — and post P1/P2 alerts
> to Slack with the requester's email, lowercased.

What lands (`flows/helpdesk_ticket_alerts.vjs`):

```
# flow: helpdesk_ticket_alerts
source "vx.helpdesk.tickets"

# French priority label -> severity code. The business expert edits this table.
SEVERITY_CODES = {"critique": "P1", "haute": "P2"}
ALERT_LEVELS = ["P1", "P2"]

code = SEVERITY_CODES[priority] ?? "P3"
email = lower(requester?.email)

if code in ALERT_LEVELS:
    emit "vx.slack.out", {text: f"[{code}] {subject} — {email}"}
end
```

Three things to notice:

- **The business lives in UPPERCASE literals.** `SEVERITY_CODES` and
  `ALERT_LEVELS` appear in the panel as editable tables — a domain expert
  corrects `"haute"` to `"P1"` without reading a line of code, and the flow
  restarts itself (ADR-0005). Adding a *new* row — `"bloquante" → "P1"` — is
  the same gesture: the panel inserts it span-exact, no agent needed
  (changing the flow's *logic* is the one that's a sentence to the agent).
- **The language is pure.** No I/O, no clock, no network in a flow — input
  event in, emits out. That is what makes replay, time-travel and canary
  structurally safe later.
- **Delivery is at-least-once, persisted.** The event was on JetStream
  before the flow saw it; the emit is acknowledged before the input is.

Feed it an event:

```bash
curl -X POST localhost:8787/ingest/helpdesk.tickets \
  -d '{"priority":"critique","subject":"SAP down","requester":{"email":"Jane@ACME.com"}}'
```

then watch it in the panel — the event, its emit, and the editable surface,
side by side. More recipes, each validated live: the
[cookbook](https://github.com/cpoder/vejas/blob/master/docs/COOKBOOK.md).
