# 0001 — VejasScript as the native flow language

- Status: Accepted
- Date: 2026-08-19

## Context

Vejas deletes the visual builder (VISION.md). Something has to express a flow.
The options: a proprietary data format (the thing we are rejecting), a general
host language (Python/JS with an SDK), or a small purpose-built language.

Two forces dominate. First, the artifact must be **safely editable from the
dashboard** and **exactly analyzable** (the business surface and the pipeline
graph must be derivable, not guessed). A general language with imports, I/O and
arbitrary side effects makes both hard. Second, the artifact is mostly written
by **agents**, and a small constrained grammar is a better generation target
than a general language — the agent cannot produce a whole class of errors it
otherwise would.

Cyril already built WmScript (github.com/cpoder/wmscript), a readable scripting
language for webMethods Integration Server (ANTLR grammar, compiles to JVM
bytecode). It is the right shape, wrong host.

## Decision

Reimplement a practical subset of WmScript in Rust as **VejasScript**,
interpreted **in-process** by the runtime. Files are `.vjs`, plain text, in the
user's git repo. The only side effects are `emit` and `invoke`: no imports, no
filesystem, no network, no clock. The webMethods pipeline model is kept: an
event's top-level fields are the variable space.

## Consequences

- The business surface, the pipeline graph, and cross-package `EXPORTS` are all
  derived from the AST — exact, never a registry to keep in sync.
- Scripts are safe to edit from the panel: a bounded language with no I/O
  cannot exfiltrate data or wedge the host.
- In-process interpretation means instant hot-reload and no subprocess per flow.
- Agents generate `.vjs` well because the grammar is small (verified: a
  correct transcoding flow generated in ~14s, first try).
- **Cost:** we own a language — lexer, parser, interpreter, editing, tests, and
  the docs for agents. And its expressiveness is deliberately capped; anything
  beyond it is either a composed service or (ADR-0010) code validated by
  example, not a growing pile of operators.

## Alternatives considered

- **Python/JS + SDK** (what v0 shipped, then removed): general and familiar,
  but unbounded side effects break dashboard-editing safety and AST analysis,
  and it forces a subprocess per flow. Superseded by this ADR and ADR-0009.
- **A proprietary JSON/YAML flow format**: exactly the lock-in Vejas exists to
  reject.
- **Embedding an existing scripting engine (Rhai, Lua):** general-purpose,
  larger surface, and not the webMethods pipeline model the composition story
  depends on.
