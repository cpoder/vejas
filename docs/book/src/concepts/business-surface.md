# The business surface

The core bet of the platform (ADR-0005): **the agent owns *how*, the human
owns *what it means*.** In practice, "what it means" lives in the flow's
UPPERCASE literals — thresholds, transcoding tables, routing keys, feature
lists. The runtime extracts them from the AST with their exact source spans
and serves them as *the surface*:

- the panel shows them as editable values and tables next to live sample
  events;
- editing writes back **literally** (span-exact, no reformatting), audits
  the change, and restarts just that unit;
- credential-shaped keys are masked and must be `secret()` references — the
  same single-sourced pattern gates CI, the panel, and generation.

Three levels of "editable" (ADR-0019):

- **N1 — parameters**: the literals above; already editable.
- **N2 — the rules view**: the flow's decision logic *read* as faithful
  sentences ([guide](../guides/rules-view.md)) with its inline literals
  editable; the structure itself is read-only.
- **N3 — structure**: changing the logic is an agent conversation — never a
  form-based rules editor. That line is deliberate: half-editable code is
  how platforms rot.
