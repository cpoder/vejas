# 0019 — Rules-view: a read-only projection of a flow's rules

- Status: Accepted
- Date: 2026-08-22

## Context

The manifesto's business-surface bet is that a domain expert corrects the
*business* of a flow while the agent owns the *code*. ADR-0005 delivered that for
**parameters** — thresholds, tables, constants — extracted from the AST and
edited in place. But the recurring question (Cyril, 22/08) is sharper: *can the
expert edit the business RULES themselves — the logic, not just its numbers?*

The wrong answer is a graphical rule editor. A drag-a-branch, add-a-condition UI
is the flow builder we deleted, wearing a different hat — it recreates the
proprietary canvas, the lock-in, the "nothing to export" problem the whole
project argues against. So the question needs a doctrine, not a feature.

## Decision

Three levels, each a distinct answer to "can the expert edit the rules?":

- **N1 — rule parameters.** The thresholds/tables/constants a rule reads. Already
  editable: this is the business surface (ADR-0005). Done.
- **N2 — reading the rules.** A **read-only** projection of the flow's control
  flow into sentences the expert can *read*, with the surface literals a rule
  references editable **inline** (reusing N1's exact Apply → shadow-replay →
  Promote loop — no new write path). The structure is shown, never editable here.
  **This ADR builds N2.**
- **N3 — rule structure.** Adding a condition or a branch — changing the logic.
  This is **agent territory**: the expert describes the change in natural
  language, the agent rewrites the VejasScript, it is shadow-replayed on real
  traffic (ADR-0018) and promoted. Never a graphical editor.

**Amendment A (normative):** there is no graphical rule editor at any level. N3
goes through the agent loop precisely because a rule-structure GUI *is* the
builder reintroduced.

### The N2 rendering rule — faithful or raw, never a paraphrase

Only forms that render **exactly** become sentences. The granularity is
**per-arm and binary**:

- A guard that is a single comparison of simple operands (`event.total >=
  MIN_TOTAL_EUR`, `event.currency == "EUR"`) renders as a sentence — *when … then
  → subject* — and the surface literals it names (`MIN_TOTAL_EUR`) become inline
  editors wired to the existing promote loop.
- A **composed** guard (`and`/`or`, optional `?.`, projections, filters, calls)
  is shown as its **verbatim source** — sliced from the script by the arm's
  condition span, not re-rendered — flagged *advanced*, with a line pointing the
  expert at the agent loop (N3).

No partial substitution: an arm is a sentence **or** a raw block, never a
half-French/half-code hybrid with a verbatim fragment dropped mid-sentence — that
grey zone is exactly what erodes trust in the projection. If one arm projects and
its neighbour does not, each gets its own treatment; that is fine. An arm whose
*action* is not a simple set of `emit`/`respond` (nested control flow, etc.) is
raw for the same reason.

### Placement and mechanism

The **Rules** section sits on the flow card, next to the business surface
(constants/tables) — same screen, same loop. Backend: `vjs::flow_rules` walks the
top-level `if/elif/else` arms (each arm now carries its condition source span in
the AST) and emits, per arm, either `{when, then, literals}` or `{raw}`; served
at `GET /rules?file=`. The panel renders sentences with inline literal chips and
raw "advanced" blocks. Zero new write path: every edit goes through
`/surface/set`, the same one ADR-0005/0018 already guard.

## Consequences

- The differentiator deepens without reopening the builder: the expert can now
  *read* the rules, edit their parameters in context, and knows exactly when a
  change is structural (advanced → agent). The boundary is visible, not implicit.
- The projection is honest about its own limits — a composed guard is never
  dressed up as a tidy sentence; the expert sees the real code and is routed to
  the right tool. "Faithful or raw" is a trust property, not a rendering
  convenience.
- v1 scope is top-level `if` statements; a nested `if` inside an arm makes that
  arm advanced (raw). Deeper projection (nested arms, boolean decomposition) can
  come later, but only if it stays exact — the doctrine forbids the lossy middle.
- The AST's `If` arm gained a condition source span. It is view-only; the
  interpreter ignores it. Any future tooling that needs verbatim rule source now
  has it.
