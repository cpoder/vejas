# Vejas — Architecture

This describes the system **as built** (Phase 0–1). Target-state components not
yet implemented are marked _(planned)_ and specified in the ADRs and ROADMAP.

## Repository layout

```
core/                     the runtime (Rust, one binary)
  Cargo.toml
  src/main.rs             supervision, HTTP surface, panel, MCP server, CLI
  src/vjs.rs              VejasScript: lexer, parser, interpreter, editing, tests
  src/connectors.rs       native bundled connectors (http-in, slack-out)
  src/panel.html          the dashboard (embedded at compile time)
flows/*.vjs               the "default" package's flows
flows/fixtures/*.json     one sample input per flow (name-matched)
services/*.vjs            composable services (default package)
packages/<pkg>/           a package: package.vjs + flows/ + services/ + fixtures/
tests/vjs/*.json          golden end-to-end cases (vejas-runtime vjs-test)
docs/                     VISION, ARCHITECTURE, ROADMAP, MCP, SUBJECTS, adr/
Dockerfile                one binary on debian-slim (curl for webhooks)
docker-compose.yml        two containers: nats + vejas
```

## Runtime processes and threads

The runtime is a single OS process. Inside it:

- **one supervisor thread per flow** — connects to NATS, creates a durable pull
  consumer on the flow's `source`, and loops: fetch a batch, run each event
  through the interpreter, publish the emits **before** acking (at-least-once;
  a crash means redelivery, never a lost emit). Restarts with backoff on error;
  restarts on file change (mtime).
- **native connector threads** — `http-in` (a threaded HTTP server; `POST
  /ingest/<suffix>` → publish `vx.<suffix>`) and `slack-out` (durable pull
  consumer on `vx.slack.out` → webhook or DRY-RUN).
- **the HTTP server thread** — serves the panel, the JSON introspection
  endpoints, the editing endpoints, and the MCP endpoint. One request = one
  thread (so a slow `/flows/new` never blocks monitoring).

There is no database. NATS JetStream holds the stream `VEJAS` bound to `vx.>`,
the durable consumers, and (later) KV/object state.

## VejasScript (`core/src/vjs.rs`)

A Rust reimplementation of a practical subset of WmScript
(github.com/cpoder/wmscript), interpreted in-process. See ADR-0001.

- **Pipeline model (webMethods):** the incoming event's top-level fields are the
  variable space; `event` also holds the whole document.
- **Statements:** `source`, `tool`, `NAME = <literal>`, assignment (`a.b = …`),
  `if/elif/else/end`, `for … in … end`, `emit subject, expr`, `invoke svc(args)`.
- **Expressions:** literals, `{doc: …}`, `[arrays]`, f-strings, dotted paths,
  `?.` null-safe, `??` coalesce, comparisons + `in`, `and/or/not`, `+ - * /`
  (with array concatenation), indexing, `xs[].field` projection, `xs[cond]`
  filtering, builtins (`upper lower trim len str num split join replace round
  abs`).
- **Side effects are only `emit` and `invoke`.** No imports, no I/O, no clock —
  which is what makes a script safe to edit from the dashboard and exactly
  analyzable (business surface & graph are derived from the AST, never a
  registry).
- **Editing:** `set_literal` rewrites exactly one literal in place (byte spans
  from the parser), then re-parses; it refuses to write a file that no longer
  parses.

## Composition & packages (ADR-0003, ADR-0004)

- `invoke fmt(args)` runs `services/fmt.vjs` in the **caller's package** and
  merges its final pipeline into the caller; `x = invoke fmt(args)` captures it
  as a document.
- `invoke pkg:fmt(args)` crosses packages, allowed **only if** `pkg`'s
  `package.vjs` lists `fmt` in `EXPORTS` (private by default). While a service
  runs, invokes resolve within **its** package.
- A package is a directory `packages/<pkg>/` with `package.vjs` (`ENABLED`,
  `EXPORTS = […]`), `flows/`, `services/`. Hot-addable via `POST /reload`.

## Business surface (ADR-0005)

The literals of a flow are its business surface: `MAPPING*` dict literals
(field mappings), other UPPERCASE dict literals (transcoding tables), and
UPPERCASE scalar/list literals (constants). `surface_json` extracts them by
AST; the panel renders them; corrections go through `set_literal` +
targeted reload. Sample runs execute the real flow on the fixture and show the
emitted payloads — how a non-developer validates behavior without reading code.

## HTTP surface

| Method / path | Purpose |
|---|---|
| `GET /` `/panel` | the dashboard |
| `GET /healthz` | liveness |
| `GET /topology` | flows (status) + connectors |
| `GET /graph` | pipeline graph (sources, flows, services, destinations) |
| `GET /surface` | business surface of every flow |
| `GET /preview?file=` | run a flow on its fixture → emits + pipeline |
| `GET /file?path=` · `POST /file/set` | read / write a script (parse-validated) |
| `GET /fixture?file=` · `POST /fixture/set` | read / write a sample input |
| `POST /surface/set` | rewrite one literal in place |
| `POST /flows/new` | agent writes a new flow from a prompt |
| `POST /reload` | rescan; start new, stop removed, restart changed |
| `POST /mcp` | JSON-RPC 2.0 MCP server (see `MCP.md`) |

## MCP server (ADR-0006)

`POST /mcp` speaks JSON-RPC 2.0 (`initialize`, `tools/list`, `tools/call`,
`ping`, batches). Ten platform tools drive the whole system; any flow/service
declaring `tool "…"` becomes a first-class MCP tool whose call runs the flow on
the arguments and returns its emits. Details and the tool table: `MCP.md`.

## Connectors (ADR-0007)

A connector is a native Rust **driver** + a declarative **instance manifest**.
Drivers implement `connectors::Driver` (`kind()`, `about()`, `run(ctx)`) in two
families — Source (pushes onto the bus) and Sink (consumes it); Source kinds are
`webhook` / `interval` / `poll` (queue/stream drivers are future). Shipped:
`http-in`, `timer`, `http-poll`, `slack-out`, `http-out`.

An instance is a `.vjs` manifest under `connectors/` (or
`packages/<pkg>/connectors/`): a `driver "name"` directive plus UPPERCASE literal
config. It is scanned, supervised (restart/backoff, restart on mtime change),
and reported in `/topology` and `/graph` like a flow — and its config is
editable via `set_literal` / the panel, hot-addable via `/reload`. Config maps
straight from the manifest's surface literals into the driver's `Config`.

The **subject convention** (`SUBJECTS.md`) remains the whole interface, so an
**external connector in any language** is a first-class citizen over the bus.
Secret references in manifests (ADR-0008) and connector-by-prompt are _(planned,
this phase)_.

## Secrets _(planned, ADR-0008)_

A `SecretStore` trait (HashiCorp Vault by default) resolved at run time via a
`secret("path")` builtin. Secrets never appear in a VejasScript literal or the
panel — that keeps "the whole script is editable and versionable" true without
leaking credentials into git.

## Build, run, test

```
docker compose up                         # nats + vejas, nothing else
cargo test --manifest-path core/Cargo.toml # 15 language unit tests
vejas-runtime vjs-test tests/vjs           # 19 golden end-to-end cases
vejas-runtime vjs-check <file.vjs>         # parse-check one script
vejas-runtime vjs-run <file.vjs> <in.json> # run one script on an input
```
