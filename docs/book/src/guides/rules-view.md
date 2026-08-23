# The rules view

The reason Vejas keeps flows as one screen of readable VejasScript is so the
*meaning* — the thresholds, the mappings, the routing conditions — is
visible and correctable by the person who owns it, not buried in code
(ADR-0019). The rules view is where a domain expert reads and fixes that
meaning without touching logic.

## The business surface

Every flow's editable meaning — its constants, transcoding tables, and
mappings — is the **business surface**. It self-describes at `GET /surface`
(`vejas_surface`):

```json
{ "name": "SEVERITY_CODES", "kind": "table",
  "value": { "critique": "P1", "haute": "P2", "normale": "P3", "basse": "P4" } }
```

The panel renders each entry as an editable card; the agent reads the same
surface to know what it may safely change.

## Rules as sentences

Beyond the raw literals, the rules view *projects* a flow's decision
branches into plain conditions — `GET /rules?file=<flow>`:

```json
{ "rules": [
    { "kind": "if", "when": "severity in ALERT_SEVERITIES",
      "then": ["→ vx.slack.out"], "literals": ["ALERT_SEVERITIES"],
      "projectable": true } ]}
```

You read *"when severity is in ALERT_SEVERITIES, alert Slack"* — and the
`literals` tell you exactly which value to edit to change who gets alerted.
A branch that is too dynamic to project cleanly is marked
`projectable: false` and shown as its raw source rather than a misleading
sentence.

## Correcting a value

Editing one entry is a promote:

- Panel: change the card → **Apply**.
- API: `POST /surface/set`; agent: `vejas_set_literal`.

It hot-reloads that flow — the one unit picks up the new value, no process
restart and no deploy. And you don't do it blind:
Apply previews the change against real traffic first (shadow-replay), so you
see *what would have differed* before you commit. That whole safety story —
preview, promote, roll back, canary — is [change safely](change-safely.md).

## Why this is the differentiator

Anyone can generate glue that moves data. The thing an integration platform
rarely gives you is a place where the person who *knows the business* can
correct what a rule means — live, previewed, and audited — while the person
who knows the plumbing keeps owning the plumbing. The rules view is that
place.
