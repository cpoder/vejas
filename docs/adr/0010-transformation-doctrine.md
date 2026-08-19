# 0010 — Transformation doctrine: small registry + code-by-example

- Status: Accepted
- Date: 2026-08-19

## Context

Real mappings need transformations without end: cents→euros, name→code lookups,
splitting and parsing strings, arrays of structures projected onto other arrays,
date reformatting, conditional enrichment. The instinct is to enumerate them —
grow a rich transform library. That instinct is the grave of every iPaaS: the
library becomes a second, worse programming language, and it is never complete.

## Decision

Never try to enumerate the universe of transformations. Instead:

1. **A deliberately small builtin registry**, gated by an admission test: a
   transform is admitted only if it is parameterizable by a literal,
   deterministic, and verifiable in the preview. Today: `upper lower trim len
   str num split join replace round abs`, plus `lookup:<TABLE>` and the `each`
   projection at the mapping level.
2. **The open-ended tail is code**, written by the agent in VejasScript, and
   validated **by example** — golden tests (`tests/vjs`, `vjs-test`) assert
   exact emits for real inputs. The examples are the contract.
3. **Recurring helpers get promoted** into the registry afterwards. The registry
   is a cache of proven patterns, not a designed catalog.

## Consequences

- The mapping surface (ADR-0005) stays small and safe; complex logic lives in
  code, not in an ever-growing DSL.
- A domain expert validates the hard parts by behavior (sample run / golden
  case), never by reading code — which is the whole point.
- Coverage is a test discipline: 19 golden cases today (transco hit/miss,
  negatives, null-safe, UTF-8 both ways, rounding, projections, composition,
  EXPORTS denial). `vjs-test` is the intended CI gate.
- **Cost:** saying no to "just add one more transform" requires discipline; the
  admission test is the objective bar that makes the no defensible.

## Alternatives considered

- **A rich transform DSL / function library:** rebuilds low-code; unbounded;
  never complete. Rejected.
- **Only builtins, no code tail:** cannot express real integrations.
- **Only code, no registry:** loses the common, verifiable transforms that make
  the mapping surface legible to a business user.
