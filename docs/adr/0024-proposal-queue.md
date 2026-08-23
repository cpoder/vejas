# 0024 — The proposal queue: governed change, from agents and the fleet

- Status: Proposed
- Date: 2026-08-23

## Context

Every ingredient of a governed self-healing loop now exists and is measured:
version-tagged dead letters (ADR-0015/0021), time-travel and shadow canary
(ADR-0021), audited cluster-wide promote and rollback (ADR-0018/0021). What
does not exist is the seam the thesis demands between *how* and *what it
means*: today an MCP-connected agent with write access **lands** changes
directly — governance is a conversational convention, not a product step.
The human's approval must be first-class.

The fleet side already specified this shape: CONTROL.md v2 defines tier-3
**proposals** for remote collectors (propose → local panel approves), with
approval deliberately *not* a remote command. That spec predates clustering
and stores proposals on disk; this ADR unifies local agent proposals and
fleet proposals into one mechanism, on the bus.

## Decision (proposed)

### One queue, on the bus

A **proposal** is `{id, kind, payload, author, created_at, baseline,
evidence, status}` in a `VEJAS_PROPOSALS` JetStream KV bucket (bounded
history) — cluster-visible by construction, like versions and leases. Kinds
v1: `set_literal`, `version` (a candidate flow version — the flagship),
`package_update` (fleet). `author` is the **channel** (`mcp`, `ctl:<hub>`,
`panel`), never a claimed identity (the CONTROL.md rule: the human behind a
channel is logged at that channel's edge, not asserted on the wire).

This supersedes CONTROL.md's on-disk `proposals/` detail (a per-instance
decision from before clustering); fleet proposals arriving over `vxc.` land
in the same bucket and the same panel queue. One list, or the operator
misses things.

### Approval is a human act

`vejas_propose` and `vejas_proposals` join the MCP tools. **Approve and
reject do not** — they are panel actions (`POST /proposals/{id}/approve|reject`),
by design: an agent that can propose *and* approve is not governed. The
existing `VEJAS_AUTO_APPROVE` (local-only, zero-touch fleets) is honored
and stays local-only. On approve, the **existing** paths execute — a
`version` proposal promotes through ADR-0021 (cluster-wide, 60 ms) — and
the audit record carries the proposal id; a reject is audited the same way.

### Evidence rides the proposal

A `version` proposal carries what the agent proved: time-travel results
(`{events, changed}`) and canary stats. The panel shows the evidence next
to the Approve button — and shows **"no evidence"** loudly when absent. The
generation contract (tool descriptions) makes attaching evidence the norm.

### The governance knob

`VEJAS_REQUIRE_APPROVAL=1`: mutating MCP tools answer "submit a proposal
instead" (didactic, like the cluster guard); the panel — already human —
is unaffected. Default off: dev deployments keep direct-write freedom. The
ADR-0020 cluster guard message gains "or submit a proposal".

### Staleness reuses the baseline rule

A proposal records the `baseline` (content hash) it was made against —
the same rule as overlay eviction and canary auto-stop (ADR-0021): if the
baseline moves (deploy, another promote) before approval, the proposal
**auto-expires, loudly** (status `expired`, audited, re-proposable). Never
an approval that lands on a base the evidence never saw.

### Notifications are flows

Every proposal transition emits an event on `vx.proposals.events`. How does
the on-call human hear about a pending proposal? The platform's own answer:
a flow — route it to Slack, PagerDuty, email with the connectors that
already exist. No notification subsystem; dogfood.

## The loop this enables (the acte-6 recipe)

A message dies (DLQ, version-tagged) → an agent reads `vejas_dlq`, drafts a
candidate, proves it (`vejas_time_travel` over the window that killed it,
`vejas_canary` on live traffic) → `vejas_propose` with the evidence → the
human sees diff + evidence in the panel, **approves** → promote fans out in
60 ms → DLQ replay: the dead messages pass under the new version, the
transition recorded in their envelopes. Self-healing, with the human owning
exactly one step: the meaning.

## Consequences

- Governance becomes a product surface, not a convention — the missing half
  of "the agent owns how, the human owns what it means."
- CONTROL.md v2 is discharged by unification (transport differs, queue and
  panel are one); the fleet console lists the same queue.
- New bus object: `VEJAS_PROPOSALS` KV, bounded like DLQ/audit; no new
  dependency.
- The panel gains its most consequential card: the approval queue.

## Rejected

- **Agent-approvable proposals** — defeats the purpose; approval is the
  human step.
- **Disk-based proposals** (CONTROL.md v2's original detail) — per-instance
  state in a clustered world; the bus is the shared truth.
- **A separate fleet queue** — two lists means missed approvals; the
  channel is a field, not a silo.
- **A notification subsystem** — the platform routes events; proposals are
  events.

## Open questions (for review)

1. Expiry semantics on *literal* proposals whose literal still exists after
   an unrelated baseline change — auto-rebase (the ADR-0021 follow-up) or
   expire-always for v1? Leaning expire-always: simple, safe, loud.
2. Panel approval and multi-user: v1 has one operator identity (the panel).
   Real RBAC/SSO is acte-5 territory — this ADR must not block on it, only
   leave the `approved_by` field ready.
3. Bounds: KV history depth, expired-proposal retention.
