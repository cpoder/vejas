# Vejas — Roadmap

Phases are increments, each validated live and end-to-end before the next. The
governing discipline (VISION.md): **distribution and a real, deployable demo
come before the next feature** — the failure mode to avoid is building the whole
platform before a single user touches it.

## Phase 0 — Language & runtime · DONE

- VejasScript: lexer, parser, in-process interpreter (ADR-0001).
- NATS JetStream, at-least-once, hot-reload (ADR-0002).
- Business surface: literals extracted by AST, corrected in place, sample runs
  (ADR-0005).
- Panel: pipeline graph, prompt→flow, script & input editing.
- All-Rust, Python removed; one binary (ADR-0009).

## Phase 1 — Composition, packages, MCP · DONE

- Service composition, wM-style merge; `pkg:service` + EXPORTS private-by-default
  (ADR-0003, ADR-0004).
- The runtime is its own MCP server; flow-as-tool (ADR-0006).
- Golden-test runner + 19 cases; 15 language unit tests (ADR-0010).

## Phase 2 — Connectors & secrets · NEXT

The increment that makes the demo **actually deployable** (a real webhook, a
real secret, a real sink). Do this before anything else.

1. **Connector SDK** (ADR-0007): `Connector` trait, `Source`/`Sink`; source
   trigger kinds webhook / poll / queue / push; connector = package with a
   manifest. Port `http-in`/`slack-out` onto the trait as the first two.
2. **Secrets/Vault** (ADR-0008): `SecretStore` trait (Vault default, dev
   backend), `secret("path")` builtin, panel shows references not values.
3. **Connector-by-prompt** (built): `vejas_new_connector` MCP tool + POST
   /connectors/new — the ADR-0006 generation loop retargeted to the driver
   catalog; the agent picks a driver, writes config, and uses secret() for
   credentials.

Exit criterion: a Stripe webhook → a flow → a Slack post, with the signing key
in Vault, deployed from `docker compose` — recordable for a Show HN.

## Phase 3 — Surfaces & administration

- **Flow-as-API**: `GET/POST /api/<name>` backed by the same `tool "…"`
  declaration (ADR-0006).
- **Panel administration**: connectors (with secret references, never values),
  the live MCP tool list, and a real-time monitor — assembled on existing
  endpoints.
- **Shadow-replay & approval** (ADR-0005) — built (lite): propose a correction
  → replay the last real events from the trace ring → before/after diff →
  promote or discard (panel + MCP). Later: JetStream-hydrated history,
  audit trail, one-click rollback.

## Phase 4 — Distribution & durability

- Package/connector distribution as git repos; the seam toward a marketplace
  (ADR-0003) — a candidate monetization surface (the paid panel: collaboration,
  approvals, audit, SSO).
- `vjs-test` as the CI gate; transport-level tests (redelivery, ordering,
  reconnection) beyond the language golden cases.

## Cross-cutting, ongoing

- Keep README/MANIFESTO aligned with VejasScript (no stale Python).
- Purge Python-era JetStream durable consumers.
- Register `vejas.io` / `vejas.dev`; create the GitHub repo; publish the
  manifesto only when the Phase 2 demo is real.

## How to pick up this project (for another agent)

Read `VISION.md`, then `ARCHITECTURE.md`, then the ADRs in order. The code is
`core/src/{vjs,main,connectors}.rs` (~3k lines). Run `cargo test
--manifest-path core/Cargo.toml` and `vejas-runtime vjs-test tests/vjs` to see
the contract. Everything the platform can do is reachable over `POST /mcp`
(`docs/MCP.md`). Do not add a transform without the ADR-0010 admission test;
do not put a secret in a literal (ADR-0008); do not reintroduce Python
(ADR-0009).
