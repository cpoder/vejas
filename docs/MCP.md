# Vejas over MCP

The runtime is its own MCP server: JSON-RPC 2.0 over `POST /mcp`. Point any
agent at it (`http://<host>:8686/mcp`) and the whole platform is drivable —
inspect, edit, generate, run — with no side channel.

## Platform tools

| Tool | Does |
|---|---|
| `vejas_topology` | running flows + connectors and their status |
| `vejas_graph` | the pipeline graph (sources, flows, services, destinations) |
| `vejas_surface` | every flow's business surface (mappings, tables, constants) |
| `vejas_language` | the VejasScript reference — read it before writing any `.vjs` |
| `vejas_read` | read a `.vjs` script or a `.json` fixture |
| `vejas_write_flow` | create/overwrite a `.vjs` (parse-validated, hot-reloaded) or a `.json` fixture |
| `vejas_set_literal` | rewrite one business-surface literal in place |
| `vejas_replay_literal` | shadow-replay a proposed literal change on the last real events — diff only, no write |
| `vejas_preview` | run a flow on its fixture, return emits + final pipeline |
| `vejas_run_flow` | run any flow on a supplied input, return emits (bus untouched) |
| `vejas_events` | the last events the flows processed (subject, ok/error, emits, preview) |
| `vejas_new_flow` | agent writes a new VejasScript flow from a prompt; it lands running |
| `vejas_reload` | rescan flows/packages |

`vejas_new_flow` and `vejas_new_connector` shell out to an agent CLI
(`VEJAS_AGENT_CMD`, default `claude`) and are advertised **only where one
exists** — the stock container ships none. The normal path for an external
agent is to write the file itself: `vejas_language` for the syntax, then
`vejas_write_flow`.

When the runtime is started with `VEJAS_TOKEN` set, every write (POST,
`/mcp` included) requires `Authorization: Bearer <token>` — e.g.
`claude mcp add --transport http vejas <url> --header "Authorization: Bearer <token>"`.

## Flow-as-tool

A flow or service that declares a description is exposed as a first-class MCP
tool, callable by name:

```
# service: classify_ticket
tool "Classify a support ticket: French priority -> severity code, page or not."
SEVERITY_CODES = {"critique": "P1", "haute": "P2"}
severity = SEVERITY_CODES[priority] ?? "P3"
emit "vx.classify.result", {severity: severity}
```

`tools/list` now includes `flow_classify_ticket`; calling it runs the flow on
the arguments and returns its emits. The MCP surface grows as you write flows —
no server code to touch. The same declaration is what a future `/api/<name>`
HTTP endpoint will expose.

## Connectors over MCP

`vejas_drivers` lists the available connector drivers (name, kind, description).
A connector is a manifest file under `connectors/` (or `packages/<pkg>/connectors/`):

```
# connector: stripe_in
driver "http-in"
PORT = 8787
```

Write one with `vejas_write_flow` (any `.vjs` path), tune its config with
`vejas_set_literal`, and it hot-starts on reload — same tools as flows. Drivers
today: `http-in` (webhook), `timer` (interval), `http-poll` (poll), `slack-out`
and `http-out` (sinks). See ADR-0007.

Or describe it: `vejas_new_connector` asks the agent to pick a driver, write the
config, and use `secret("…")` for any credential — it lands running (like
`vejas_new_flow` for flows). Drivers today: `http-in` (webhook), `timer`
(interval), `http-poll` (poll), `slack-out` / `http-out` (sinks), `exec-source` /
`exec-sink` (any-language over stdio). See ADR-0007, ADR-0011.

## Secrets over MCP

`vejas_secrets` lists every `NAME = secret("path/key")` reference declared by
flows and connectors — **references only, never values**. Secrets resolve at run
time via the `secret()` builtin against the configured `SecretStore` (Vault by
default, env for dev); a missing secret fails the run closed. A secret is never a
literal, so it never appears in `/surface`, the file, or the panel. See ADR-0008.
