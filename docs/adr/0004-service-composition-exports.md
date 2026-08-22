# 0004 — Service composition & EXPORTS visibility

- Status: Accepted
- Date: 2026-08-19

## Context

Composition is essential: real integrations reuse logic (formatting,
enrichment, notification). Classic ESBs made this their core — `invoke` a service,
its pipeline flows into yours. But they also allowed any service to invoke
any other across the whole namespace, which produced the classic
spaghetti-IS coupling where nothing can be changed safely.

We want the composition ergonomics without the coupling.

## Decision

- `invoke fmt(args)` runs `services/fmt.vjs` **in the caller's package** and
  **merges** its final pipeline into the caller. `x = invoke fmt(args)`
  captures the whole pipeline as a document instead.
- `invoke pkg:fmt(args)` crosses packages, and is allowed **only if** `pkg`'s
  manifest lists `fmt` in `EXPORTS`. **Private by default**: no manifest, or not
  listed, is denied with a message that points to `EXPORTS` or the bus.
- While an invoked service runs, its own invokes resolve within **its** package
  (the run swaps the caller-package context and restores it after).

## Consequences

- Package-internal composition is frictionless (no ceremony within a package).
- Cross-package coupling is explicit and auditable: a package's public API is
  exactly its `EXPORTS` list; everything else is private. Between packages, the
  first-class option is the bus.
- The pipeline graph shows `invoke` edges and composed services, so coupling is
  visible, not hidden.
- **Cost:** a small resolution layer (qualified names, per-package export
  cache, context swap) and a depth guard against invoke recursion.

## Alternatives considered

- **Global namespace, any-to-any invoke (the classic ESB way):** best
  ergonomics, worst coupling. Rejected as the failure mode we are avoiding.
- **No cross-package invoke, bus only:** maximal decoupling but loses the
  synchronous-compose ergonomics that make services worth having. We keep both,
  with EXPORTS as the deliberate, narrow exception.
