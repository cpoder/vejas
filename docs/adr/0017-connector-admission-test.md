# 0017 — The connector admission test

- Status: Proposed
- Date: 2026-08-22

## Context

The manifesto's counter-bet on catalogs: a small catalog + a good SDK +
agents may compound faster than hand-built catalogs ever did. For that to be
credible, "we have a connector for X" must mean something verifiable — not a
`.vjs.example` that parses and nothing more. ADR-0010 solved the same
problem for language transforms with an admission test; this is the
connector equivalent. The cookbook validation pass (2026-08-22) showed
agents produce good recipes fast; what is missing is the bar they must
clear to be *counted*.

## Decision (proposed)

A connector recipe is **certified** when it ships as a directory —
`docs/examples/connectors/<name>/` — holding four things, all enforced by a
CI admission job:

1. **The manifest** (`<name>.vjs.example`) — parses (`vjs-check`, already in
   CI), declares a `driver` from the live catalog, and its doc header states:
   purpose, subjects in/out, every secret path it expects, and the remote's
   pagination/rate notes when relevant.
2. **The credential rule, linted** — any literal whose key matches the
   credential pattern (`pass(wd|word)|secret|token|api[_-]?key`) must be a
   `secret()` reference; the lint fails the admission job on a literal
   credential. **Single-source requirement**: today that regex lives only in
   the panel's JS (`isSecretKey`, panel.html) — duplicating it in a Rust
   lint would create two definitions whose divergence is a security hole.
   The decided mechanism: a `pub const SECRET_KEY_PATTERN` in `secrets.rs`
   becomes the source of truth; the lint consumes it, and the runtime
   injects it into the served panel (placeholder substitution in the
   `include_str!`'d page). One definition, two consumers — the claim is
   true only once that extraction lands, and the extraction is part of
   implementing this ADR.

   **Stated limit of the heuristic**: it matches key *names*, not values. A
   credential under an unmatched key (`AUTH = "Bearer …"`, a raw
   `Authorization` header value) slips the lint and the panel mask alike —
   consistently, but silently. Widening the pattern with `auth` would catch
   it at the cost of false positives (`author`, `oauth_url`); the current
   decision is to keep the pattern as is and document the limit here.
3. **A golden fixture** (`fixture.json`) — one real-shaped sample of what
   the connector publishes (source) or consumes (sink), envelope included
   when the driver adds one. This is what downstream flows are golden-tested
   against, and what the cookbook shows.
4. **A probe mock** (`mock.mjs`, where the remote can be mocked) — a
   minimal local stand-in for the remote API (the `e2e/mock-*.mjs` pattern).
   The admission job boots a throwaway runtime, starts the connector against
   the mock with dummy secrets, and requires a green `vejas_test_connector`
   probe plus at least one published message matching the fixture's shape.
   Where the remote cannot be mocked meaningfully (SAP's RFC gateway), the
   recipe is certified **with a stated exception** — the README says what
   was verified instead (e.g. the recorded live demo) — never silently.

The public claim follows the test: the connector counter (vejas.dev, README)
counts **certified recipes only**, and links each to its directory. A recipe
that fails admission is a draft, not a connector.

## Consequences

- "Agent writes a connector in hours" becomes "agent writes a **certified**
  connector in hours" — the admission job is the reviewer that never tires.
  The generation loop can even run it: an agent iterates until admission
  passes, unattended.
- The six existing flat recipes (Slack, ServiceNow ×2, Jira ×2, Workday)
  migrate to directories and pass admission first; the flat `.vjs.example`
  form stays valid for *drafts* only.
- CI grows one job (`admission`) — per recipe: a dedicated `nats-server`
  (**with `-js`** — without it the probe dies on "no responders"), a
  throwaway runtime on recipe-disjoint ports (parallelizable), dummy
  secrets via the existing `EnvStore` (`secret("a/b")` ← env
  `VEJAS_SECRET_A_B` — nothing to build), readiness by polling `/healthz`
  (never a fixed sleep), teardown by captured PID or `pgrep -x` (never
  `pkill -f` with a pattern the script itself contains). Measured ~3 s per
  recipe, bounded, no real credentials in CI ever. The probe is the
  existing `vejas_test_connector` (`{file} → {ok, detail}`).
- The catalog's growth rate becomes measurable and honest — the number on
  the site is the number of directories that pass, nothing else.

## Rejected

- **Certification by review** (a human reads the recipe): does not scale
  with agent-speed production, and the reviewer's bar drifts. The test is
  the bar.
- **Live-API certification in CI** (real credentials against real SaaS):
  flaky, secret-hungry, rate-limited, and it would make third-party
  contributions impossible to verify. Mocks + the probe contract cover the
  seam; the `vejas_test_connector` probe against the *real* remote remains a
  deploy-time act (panel "Test" button), where the real credentials live.
