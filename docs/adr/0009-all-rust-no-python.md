# 0009 — All-Rust runtime, no Python

- Status: Accepted
- Date: 2026-08-19

## Context

v0 shipped with a Python SDK: flows were Python files run as supervised
subprocesses, the bundled connectors were Python, and several runtime endpoints
(surface/graph/preview/set) shelled out to `python3 -m vejas`. Once VejasScript
(ADR-0001) could express flows natively and run in-process, Python became a
second language, a second runtime, and a subprocess-per-flow tax — for no
remaining benefit.

## Decision

Make the runtime **all Rust, Python-free**. Port the remaining Python flows to
VejasScript; make the bundled connectors native Rust threads (ADR-0007); remove
the Python SDK and every `python3` shell-out (surface/graph/preview/set are pure
Rust over `vjs::parse`). Ship **one binary** on `debian-slim` (with `curl` for
the Slack webhook); `docker compose` is two containers.

## Consequences

- One language, one process, no subprocess per flow; smaller image, simpler
  ops, faster reload.
- The whole system is analyzable and testable in one place: 15 Rust unit tests
  on the language + 19 golden end-to-end cases (`vjs-test`).
- Porting order_sync required one language addition — **array concatenation**
  (`positions = positions + [{…}]` in a `for`) — to build lists element by
  element.
- **Cost:** contributors write Rust for the core (an external connector can
  still be any language over the bus, ADR-0007). Old JetStream durable consumers
  from the Python era should be purged to avoid replaying stale-format messages.

## Alternatives considered

- **Keep Python as an escape hatch inside the core:** a permanent second
  language and subprocess model for a capability VejasScript now covers.
  Rejected; the escape hatch lives at the bus boundary instead (external
  connectors), not inside the runtime.
