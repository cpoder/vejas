# 0041 — The playground: the language in the browser, and a live runtime shown read-only

- Status: Proposed
- Date: 2026-11-27

## Context

vejas.dev serves the book. A developer who wants to try the language today
installs the runtime and a bus first. The founder asked for a playground in
two parts:

- developers try the language, at once, in the browser;
- anyone can watch a real runtime run real flows, through its console, read
  only, with the MCP server out of reach.

Two facts shape the answer:

- The engine (`core/src/vjs.rs`, about 5,000 lines) parses and runs flows
  but sits inside the runtime. It reaches the runtime's secrets masking, the
  answers of declared services, the trace, the versions and the metrics.
- One HTTP server serves everything: the panel and its reads, the `api`
  flows, the write routes and `/mcp`. Reads are open and writes take a token.
  A runtime that faces the public should not serve the write routes at all.

## Decision

### 1. The language in the browser

- The engine moves to a crate of its own, `vejas-lang`, that parses, checks
  and runs a flow and performs no I/O. What a run needs from outside goes
  through one trait, `Host`: a declared service's answer, a secret, `seen`,
  and the masking of what a run shows. The runtime implements it for its
  live and shown runs. The playground implements it with answers the page
  simulates.
- `vejas-lang` compiles to WebAssembly. A static page, vejas.dev/play, puts
  the flow and its event side by side, and the result below: the emits, the
  answer of an `api` flow, the lines the event went through, a failure and
  where it happened.
- The UPPERCASE constants can be edited there, as in the panel. Each declared
  service is answered by a simulation the page sets: a status, a body, a
  delay. A 503 can be set to watch a `try` take it.
- The whole state lives in the link, so a flow can be shared by sending it.
- The page runs nothing on a server, and nothing leaves the browser.
- It is built from a tag, like the book, and published with the founder's go.

### 2. A live runtime, shown read-only

**The runtime has a public mode**, `VEJAS_PUBLIC=1`. The mode lives in the
runtime itself, so that a proxy's configuration is not the only barrier:

- It serves an allowlist of reads, those the console needs: the panel, the
  topology and the graph, the events, the dead letters, the traces, the
  business surface, and the files of flows, services and connectors.
- It answers 404 to everything else: every write whatever the credential,
  `/mcp`, the `api` flows, the secrets, the previews and shown runs, the
  proposals and the metrics.
- It starts no agent.
- It caps what a read may ask for, such as the number of events or traces.
- The panel learns the mode, shows no action, and says it is a read-only
  demo.

**The demo is public, in the repository** (`deploy/demo/`). It is a handful
of flows that show what Vejas does: routing, a call to a system that
sometimes fails, a business reject, duplicates dropped, a dead letter now
and then, a detection.

- A generator feeds them with invented data. It runs as a process of its
  own, publishes on the bus and plays the system the flows call.
- No secret is configured.
- The history is bounded: the stream keeps one hour. The runtime gets a
  setting for its stream's retention, which the unbounded default needed
  anyway.

**In front**, a reverse proxy with TLS passes only GET on the same allowlist
and limits each client's rate. The container runs as a non-root user, on a
read-only file system, with CPU and memory limits, on a host apart from any
production.

**Before it faces the public**, an independent security review tries every
route of the public mode. Putting it online needs the founder's go.

### 3. What the playground is not

- It is not a sandbox where the public writes flows on the live runtime. The
  browser page does that, with nothing on a server to protect.
- It is not a hosted trial.

## Consequences

- The language gets a boundary it only had by convention: no I/O, by
  construction, and its tests need no runtime. The extraction is the larger
  part of the work, because the engine reaches masking, answers, traces and
  versions today.
- The size of the WebAssembly module is not known yet. It is measured once
  the crate exists.
- The public mode is one more mode to keep honest. Its allowlist sits in one
  place, and an end-to-end suite tries every route the runtime serves
  against it, so a new route is refused until it is listed.
- The live demo costs a small host and someone to watch it.

## Alternatives considered

- **Only a proxy in front of an ordinary runtime.** One mistake in its
  configuration exposes `/mcp` or a write. With the mode, the runtime refuses
  them itself, and the proxy is a second barrier.
- **A playground that runs the public's flows on a server.** It would run
  strangers' code on a host of ours. The browser runs it with nothing at
  stake.
- **A recorded demo.** Nothing runs: no one sees the traces, the dead letters
  or the retries happen.
