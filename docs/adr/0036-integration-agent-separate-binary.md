# 0036 — The integration agent is a separate binary, a client of `/mcp`

- Status: Accepted, not built
- Date: 2026-11-07

## Context

Vejas says "an agent writes the flow", and on a stock install no agent exists.
`ask_agent` is one call to a CLI, `claude -p`, with no tool, no fixture and no proof.
The stock image ships no CLI, and governed mode refuses `/flows/new`. The rules view
sends a composed condition "to the agent", which for a domain expert on a stock or
governed install is a dead end, and that expert is the differentiator VISION names.
Since ADR-0015 Amendment A a dead letter announces itself, and nothing answers.

Bringing your own agent stays right for a platform team, which already has one. It
does not serve the expert in the panel or the event at night.

## Decision

Vejas ships an optional agent, `vejas-agent`, and it is a **client of `/mcp`, in its
own binary**.

- **Its own process, its own credentials.** It holds `VEJAS_TOKEN` and the model's
  key, never `VEJAS_APPROVAL_TOKEN`. It goes through the door every other agent uses,
  so the gates apply and "bring your own agent" stays true.
- **Propose only.** Tools are an allow-list of read, prove and propose. It refuses to
  start outside governed mode, because a single token cannot express less: outside
  it, the same bearer lands changes directly.
- **Untrusted input is the threat model.** It reads dead letters, which anyone who
  can send an event controls. It is woken by `vx.dlq.events`, through `http-out`,
  and the event carries no payload. What it reads cannot reach a secret value
  (ADR-0008 Amendment A) and cannot purge what it is judged on (ADR-0024 Amendment
  A). The worst a hostile letter can do is produce a proposal a human reads.
- **Any model with tool calling.** Rig, pinned, behind one module, with an
  OpenAI-compatible endpoint and native Anthropic first. A local model is a base URL.
  Which models produce working VejasScript is measured on the golden corpus and
  published: a model that fails the proof loop wastes tokens and reaches nothing.
- **First scope:** the panel prompt becomes a proposal with its evidence. The
  dead-letter trigger comes second. Every model call is recorded in `VEJAS_AUDIT`,
  and payloads leave only for the endpoint the operator configured.

## This overrides a non-negotiable

VISION says "One binary. No Python, no interpreter subprocess, no sidecar." and
ADR-0009 argues for it. This decision departs from it on purpose, for the agent only.
That line protects the runtime: one process to operate, one thing to analyze, no
second language. An agent inside the runtime would keep the sentence and lose what it
is for, since it would share an address space and an environment with the approval
token and every secret while reading attacker-controlled text. ADR-0024 requires a
credential the agent does not hold, and in that one process nothing separates them.
Rig also brings 70 crates and a heavier tree, against a runtime that counts them.

So the line stays exact for the runtime and VISION now says so: the runtime is one
binary, it needs nothing else to run, and the agent is optional beside it, like the
standalone connectors.

## Consequences

- `/flows/new` and `VEJAS_AGENT_CMD` become the path for a local CLI, and the panel
  prompt talks to the agent where one is configured.
- A second binary to release, and a dependency that moves fast: `rig-core` had 22
  semver-breaking lines in twelve months, hence the pin and the single module.
- A scope for `VEJAS_TOKEN` (read, prove, propose) would let it run outside governed
  mode. That is later work, and refusing to start is the safe answer until then.

## Rejected

- **In the runtime process:** see above.
- **A `llm()` builtin in VejasScript:** a side effect the write gate cannot see
  (`Program::writes_bus()` treats every call as pure), and a model in the hot path.
  A flow that needs a model calls one through a connector.
- **`langchain-rust`:** chains, memory and retrievers answer nothing here, since the
  tools are the MCP server. 213 crates, and a hundredth of Rig's downloads.
- **Hand-rolling the provider layer,** as for MCP, MQTT and OTLP: the promise of any
  model is precisely not to follow each provider's variations ourselves.
- **Building it now:** this decision records the design. Its build comes after
  adoption has had its chance: the measured brake is upstream, and no real model
  has been run against this design yet.
