# Agents & MCP

There is no builder UI to click together — the flow-writing interface *is*
an agent, and the runtime *is* the MCP server (`POST /mcp`, ADR-0006). No
sidecar, no plugin: the platform's own write path, test path and telemetry
are the [29 MCP tools](../reference/mcp-tools.md).

The loop an agent follows (enforced by the tool descriptions themselves —
the generation contract):

1. `vejas_language` — read the language and its rules first.
2. `vejas_drivers` / `vejas_surface` / `vejas_topology` — see what exists.
3. Write: `vejas_write_flow` / `vejas_new_connector` — literals for
   anything a human may want to change, `secret()` for anything
   confidential.
4. Prove: fixture + `vejas_run_flow` (pure, no side effects), probe
   connectors with `vejas_test_connector`.
5. Ship — or in governed mode, **propose**: `vejas_propose` carries the
   change *plus the evidence* (time-travel result, canary stats), and a
   human approves in the panel
   ([governed mode](../guides/governed-mode.md)).

Flows themselves can face agents: `tool "description"` exposes a flow as an
MCP tool — the platform grows its own tool surface as flows are written.
And the self-healing loop closes the circle: a dead letter → the agent
reads `vejas_dlq`, drafts a candidate, proves it on real traffic
(`vejas_time_travel`, `vejas_canary_start`), proposes with evidence → human
approves → cluster-wide promote in 60 ms → `vejas_dlq_replay`.
