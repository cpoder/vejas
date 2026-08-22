# Vejas — Vision

> An integration platform with no builder UI. Flows are readable code, written
> by agents, corrected by domain experts, run natively by a single Rust binary
> on NATS, and driven end to end over MCP.

## The thesis

Visual integration designers (MuleSoft, Boomi, n8n, Zapier)
exist because developers were expensive and integration logic is 80% repetitive
glue. The trade was always the same: give up code, get a canvas — and with it,
proprietary formats you cannot diff, review, unit-test or leave.

In 2026 the constraint that justified the canvas is gone: an agent writes
correct glue for cents. So the canvas becomes the obstacle, not the product.
Delete the visual designer and what remains of an integration platform is the
runtime, the transport, the connectors, and the observability. That reduced
platform is Vejas.

Two consequences drive every decision here:

1. **The artifact is code again.** Plain, versionable, reviewable VejasScript —
   written and rewritten by agents, read by humans when they want to.
2. **The human keeps what agents cannot know.** Agents are better at the
   algorithmic side (parsing, retries, pagination, idempotency). They have no
   idea whether amounts are cents or euros, or what threshold is worth paging
   on. That business meaning lives in a person's head, so the platform gives a
   domain expert a way to see and correct it without reading code.

The differentiator is **not** "agent-driven integration" (a commodity — every
iPaaS demos it). It is that **any domain expert can validate and correct the
business meaning**, safely, without a developer.

## Non-negotiables

- **No builder UI.** You never draw a flow. You state intent; an agent writes
  code; the platform runs it and shows you what it does.
- **No proprietary format.** VejasScript is plain text in your git repo. There
  is nothing to export because nothing was captured. Everything is Apache-2.0.
- **One infra dependency.** NATS JetStream (persistence, KV, object store).
  Two containers, one `docker compose up`.
- **All Rust.** One binary. No Python, no interpreter subprocess, no sidecar.
- **Driven over MCP.** The runtime is its own MCP server; the whole platform is
  inspectable, editable, generatable and runnable by any agent.

## The shape of the platform

```
             agent (Claude Code / any MCP client)
                     │  MCP: inspect · edit · generate · run
                     ▼
   ┌───────────────────────────────────────────────────────┐
   │  Vejas runtime (one Rust binary)                       │
   │   • VejasScript interpreter (flows + composed services)│
   │   • bundled connectors (http-in, slack-out) as threads │
   │   • business surface (literals) + panel + MCP server   │
   └───────────────┬───────────────────────────────────────┘
                   │  NATS JetStream (vx.>)
     ┌─────────────┴──────────────┐
   sources (in)               sinks (out)
   webhook · poll · queue     Slack · ERP · any bus consumer
   (external connectors in any language, over the bus)
```

- A **flow** is a NATS pull-consumer + the interpreter: `source "vx…"` in,
  `emit "vx…"` out. It runs inside the runtime, hot-reloads on edit.
- A **service** is composable (`invoke name(args)`), pipeline-merge style: its
  pipeline merges into the caller's.
- The **business surface** is the literals in a flow (transcoding tables,
  thresholds, mappings). Extracted by AST, rendered in the panel, corrected in
  place — the one screen a non-developer needs.
- A **package** groups flows and services with a manifest; hot-addable;
  cross-package calls go through `EXPORTS` (private by default) or the bus.
- The **MCP server** is the runtime itself. A flow that declares `tool "…"` is
  exposed as an MCP tool (and, later, an HTTP API).

## Who does what

| Actor | Owns |
|---|---|
| Agent | writes and rewrites the algorithmic body (VejasScript) |
| Domain expert | validates & corrects the business surface (tables, thresholds) via the panel |
| Operator | watches the pipeline & traces; approves promotions |
| Platform (Vejas) | runs it well, keeps it honest, exposes it over MCP |

## What Vejas is not

- Not a durable-execution engine (Temporal/Restate solve orthogonal state
  problems; Vejas runs beside them).
- Not Windmill (excellent, but still a builder-UI-first, code-second tool).
- Not low-code. It runs the opposite way: all code, no builder.

## The roadmap in one line

Language + runtime + composition + packages + business surface + MCP + the
connector SDK (with SAP and Salesforce riding the exec bridges) + secrets +
flow-as-API + the admin panel + a v1 remote control plane are built (see
`ROADMAP.md`, Phases 0–4). Next: operator credibility — persistent dead
letters, OpenTelemetry/metrics, full shadow-replay, published benchmarks —
each an increment, validated live, never a big-bang. The discipline:
distribution and a real deployable demo before the next feature.
