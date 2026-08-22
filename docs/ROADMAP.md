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

- Service composition, pipeline merge; `pkg:service` + EXPORTS private-by-default
  (ADR-0003, ADR-0004).
- The runtime is its own MCP server; flow-as-tool (ADR-0006).
- Golden-test runner + the language unit-test suite (ADR-0010); both have kept growing since.

## Phase 2 — Connectors & secrets · DONE

1. **Connector SDK** (ADR-0007): the `Driver` trait, Source/Sink families,
   declarative `.vjs` instance manifests, supervision like flows. Bundled
   drivers from `http-in` to the generic `oauth-poll`, plus the exec bridges
   (any-language over stdio, ADR-0011) — including `exec-stream-source` and
   `exec-rpc`, which carry the **SAP connector** (native Rust over
   `libsapnwrfc`, ADR-0014) and the **Salesforce connector** (Bulk API 2.0).
2. **Secrets/Vault** (ADR-0008): `SecretStore` trait (Vault / file / env),
   `secret("path")` builtin, fail-closed, write-only from the panel; the panel
   shows references and resolve status, never values.
3. **Connector-by-prompt**: `vejas_new_connector` + `POST /connectors/new`.

Exit criterion met: a real webhook → flow → sink with the credential in a
vault, from `docker compose`.

## Phase 3 — Surfaces & administration · DONE (first pass)

- **Flow-as-API** — built: `api "VERB /path"` + `respond`, served under
  `/api`, OpenAPI generated at `/api/openapi.json` (ADR-0006).
- **Panel administration** — built: connector cards (status, last error, test
  probe), secrets card (references + resolve status, write-only set), trace
  feed with sink responses.
- **Shadow-replay & approval** (ADR-0005) — built (lite): propose a correction
  → replay the last real events from the trace ring → before/after diff →
  promote or discard (panel + MCP). Later: JetStream-hydrated history,
  audit trail, one-click rollback.

## Phase 4 — Distribution & durability · IN PROGRESS

- **Remote control plane** for outbound-only collectors (ADR-0013,
  `CONTROL.md`) — **v1 built**: NATS leaf-node uplink, closed command
  allowlist, status push, dual audit. v2 (content changes as locally-approved
  proposals — the ADR-0005 loop applied to fleet management) is specified,
  not built.
- Package/connector distribution as git repos; the seam toward a marketplace
  (ADR-0003) — a candidate monetization surface (the paid panel: collaboration,
  approvals, audit, SSO).
- `vjs-test` as the CI gate — built (`.github/workflows/ci.yml`: unit +
  golden + parse-check of every script and example + image build).
  Transport-level tests (ordering, redelivery + poison→DLQ cap, no-loss under
  `kill -9`, reconnection, anti-zombie shutdown) — **built**
  (`e2e/transport/run.sh`, in CI), asserting the at-least-once / publish-before-
  ack / anti-zombie invariants (ADR-0002) against a live nats+runtime.

## Phase 5 — Operator credibility · NEXT

What turns "nice project" into "I can run this in production":

1. **Persistent dead letters**: a poisoned message stops being dropped — it
   lands on a dedicated DLQ stream with a death envelope (subject, unit,
   attempts, last error, payload), visible in the panel, re-injected
   explicitly after the fix. The sister loop of ADR-0005.
2. **Real observability**: OpenTelemetry traces + a Prometheus `/metrics`
   endpoint. (The manifesto stops claiming this until it ships.)
3. **Full shadow-replay** (ADR-0005): JetStream-hydrated history, audit trail,
   one-click rollback.
4. **Published, reproducible benchmarks**: throughput, memory, image size,
   cold start — against the incumbents, same scenario.

## Cross-cutting, ongoing

- Keep README/MANIFESTO aligned with what is actually built — no promise the
  code cannot keep.
- Purge Python-era JetStream durable consumers.
- Publish: `vejas.dev` (canonical) / `vejas.io` are registered and the repo
  exists; the public flip ships with the recorded demo, not before.

## How to pick up this project (for another agent)

Read `VISION.md`, then `ARCHITECTURE.md`, then the ADRs in order. The code is
`core/src/{vjs,main,connectors,secrets,control}.rs` (~7k lines, plus the SAP and Salesforce connector crates under `connectors/`). Run `cargo test
--manifest-path core/Cargo.toml` and `vejas-runtime vjs-test tests/vjs` to see
the contract. Everything the platform can do is reachable over `POST /mcp`
(`docs/MCP.md`). Do not add a transform without the ADR-0010 admission test;
do not put a secret in a literal (ADR-0008); do not reintroduce Python
(ADR-0009).
