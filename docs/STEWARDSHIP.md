# Stewardship — who handles what (ADR-0029 R5)

Written before launch, on purpose: the project must survive a pause, and
the split between what runs autonomously and what waits for the founder's
voice must be legible — to contributors, to users, and to whoever resumes
work from this repo alone.

## Handled autonomously (agent lane, continuous)

- **Issue triage**: first response within the R1 tripwire (7 days — the
  target is much less), reproduction attempts, labeling, linking to docs
  or ADRs; closing only trivial duplicates.
- **Recipe contributions**: review against the admission bar (lint, parse,
  data-flow or stated exception — ADR-0017); a recipe PR that passes
  admission and matches the catalog conventions can merge autonomously.
- **Docs**: corrections, regeneration of the source-derived reference
  pages, keeping the documented-contracts CI leg (`e2e/doc-examples.sh`)
  honest — a divergence is fixed with the change that caused it.
- **Benches**: re-runs after relevant changes; published numbers updated
  only from fresh runs, never edited in place.
- **Dependency watch**: the quarterly R3/R6 review sweep (licence,
  governance, regulatory window), summarized for the founder.

## Founder's voice (never autonomous)

- Anything **public-facing and new**: launch posts, positioning changes,
  the tone of announcements.
- **Taste**: visual identity, naming, what "feels like Vejas".
- **Boundary calls**: anything touching ADR-0028's line, licensing,
  commercial terms, partnerships.
- **Security disposition**: agents draft the 48-hour response (R7); the
  founder signs it.
- **The four graveyard-review gates** (ADR-0029): the review runs
  agent-side; the verdict and consequences are the founder's.

## If this repo goes quiet

Thirty days of inactivity with open third-party issues triggers a public
status note (R5) — silence is the failure, not the pause. Everything
needed to resume lives here: the ADRs are the decisions, the benches are
the claims, the CI is the proof, and this file is the operating manual.
