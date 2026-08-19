# 0006 — The runtime is its own MCP server; flow-as-tool

- Status: Accepted
- Date: 2026-08-19

## Context

The vision is "entirely drivable via MCP": an agent should inspect, edit,
generate, and run the whole platform with no side channel. Two design questions:
where does the MCP server live, and how do platform capabilities become tools
without hand-writing a tool per capability forever?

## Decision

- **The runtime is the MCP server.** `POST /mcp` speaks JSON-RPC 2.0
  (`initialize`, `tools/list`, `tools/call`, `ping`, batches), served by the
  same binary — no separate Python/TS sidecar. It reuses the exact functions
  that back the HTTP/panel surface.
- **Ten platform tools** cover inspect/edit/generate/run: `vejas_topology`,
  `vejas_graph`, `vejas_surface`, `vejas_read`, `vejas_write_flow`,
  `vejas_set_literal`, `vejas_preview`, `vejas_run_flow`, `vejas_new_flow`,
  `vejas_reload`.
- **Flow-as-tool:** a flow or service that declares `tool "description"`
  (a top-level directive, like `source`) is exposed as a first-class MCP tool
  `flow_<name>`. Calling it runs the flow on the arguments and returns its
  emits. The MCP surface therefore **grows by writing flows** — no server code
  to touch. The same declaration will back a future HTTP `/api/<name>`.

## Consequences

- The whole platform is agent-drivable out of the box; verified live, including
  an agent writing and starting a new flow through one `vejas_new_flow` call.
- Extending the MCP surface is authoring, not coding: `tool "…"` in a `.vjs`
  file is the extension mechanism the vision asks for.
- One declaration, multiple surfaces (MCP tool now, HTTP API next) keeps flows
  the single source of truth.
- **Cost:** we implement enough of MCP by hand (no SDK) to stay compatible with
  clients; the transport is HTTP JSON-RPC (not stdio), which suits a
  long-running runtime but means clients must support HTTP MCP.

## Alternatives considered

- **A separate MCP sidecar (Python/TS SDK):** reintroduces a second language
  and process (contra ADR-0009), and desyncs from the runtime's own functions.
- **A fixed, hand-maintained tool list:** every new capability needs server
  code; flow-as-tool removes that ceiling.
- **stdio MCP transport:** natural for one-shot tools, awkward for a persistent
  multi-client runtime; HTTP fits better.
