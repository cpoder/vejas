# 0005 — Business surface: literals, corrected in place

- Status: Accepted
- Date: 2026-08-19

## Context

The keystone of the vision (VISION.md): a **domain expert must validate and
correct the business meaning** of a flow without a developer and without
reading code. In every classic iPaaS the one screen business users touched was
the mapping table. Deleting the builder must not delete them.

But there are two symmetric traps. Grow a transform DSL rich enough to express
everything and you have rebuilt low-code inside the language. Push everything to
code and the expert's screen becomes a toy.

## Decision

A flow's **business surface** is the set of **literals** it declares:
`MAPPING*` dict literals (field mappings), other UPPERCASE dict literals
(transcoding tables), and UPPERCASE scalar/list literals (thresholds, queue
names, flags). These are:

- **Extracted statically** from the AST (`/surface`), rendered in the panel as
  the two-column tables and constant fields a business user knows.
- **Corrected in place**: an edit rewrites exactly that one literal
  (`set_literal`, using parser byte-spans) and hot-reloads the one flow; the
  code around it is untouched, and the file must still parse or the write is
  refused.
- **Validated by behavior**: a sample run executes the *real* flow on its
  fixture and shows the emitted payloads, so the expert approves what the flow
  *does*, not a diff.

Anything beyond literals is deliberately code (ADR-0010), written by the agent.
The business user approves behavior; the artifact stays plain code.

## Consequences

- The non-developer has a real, safe editing surface without a builder and
  without a proprietary format.
- The line between "business surface" (literals) and "algorithm" (code) is
  crisp and enforced by the extractor — no creeping DSL.
- Corrections are ordinary git diffs (one literal changed), reviewable and
  revertible.
- **Cost:** literals only. Richer business logic must be expressed as code and
  validated by example — which is a feature (ADR-0010), but means the panel is
  not a universal editor.
- **Built since:** shadow-replay before promotion — a proposed literal change
  is applied in memory and rerun against the flow's last real events (the
  runtime's trace ring); the before/after emit diff is shown and the expert
  promotes or discards (`/surface/replay`, `vejas_replay_literal`, panel
  Apply → Promote/Discard). **Still planned:** replay hydrated from JetStream
  (survives restarts, deeper history), approval audit trail, one-click
  rollback (today: the one-line git diff).

## Alternatives considered

- **A transform DSL in the mapping cells:** rebuilds low-code; rejected.
- **Everything in code, no surface:** loses the keystone differentiator.
- **A separate config store for business values:** splits the artifact,
  breaks "it's all in one versionable file," and desyncs from the code.
