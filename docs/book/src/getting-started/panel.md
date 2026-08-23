# The panel

One embedded HTML page at `http://localhost:8686` — no build step, no SPA
stack. It is the **human** side of the platform: everything an expert or an
operator needs to *read* the system and *own* the meaning, nothing that
turns them into a flow developer.

## What you can do there

- **See the pipeline.** The diagram is a filter: click a node and the event
  cards, flows and connectors narrow to that path; deep-link with
  `#focus=f:<flow>` (multi-selection supported).
- **Edit the business surface.** Every UPPERCASE literal — thresholds,
  transcoding tables, queue names — is editable in place. Saving goes
  through the same audited write path agents use (`/surface/set`), and the
  unit restarts itself. Credential-shaped values are masked (single-sourced
  pattern, [ADR-0017](../decisions.md)).
- **Read the rules.** The rules view projects each flow's decision logic as
  faithful sentences — whole-sentence or verbatim code, never a half
  translation ([the rules view](../guides/rules-view.md)).
- **Watch events.** A live ring of traced events with payloads; any event
  can be replayed against a **candidate** version (the replay strip), or
  starred ★ into a curated golden test
  ([golden traffic](../guides/golden-traffic.md)).
- **Approve.** In governed mode the approval queue lists agent proposals
  with their evidence next to the Approve button
  ([governed mode](../guides/governed-mode.md)).
- **Dead letters.** The DLQ card shows death envelopes (version-tagged) and
  offers explicit replay ([DLQ & replay](../guides/dlq-replay.md)).
