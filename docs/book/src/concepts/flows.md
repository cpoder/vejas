# Flows & VejasScript

A flow is one file under `flows/`: a **pure, per-event program**. One
trigger (`source` subject, `api` route, or MCP `tool`), transformations,
`emit`s and/or a `respond`. The language has no I/O, no clock and no
network. What a run reads beyond its event, a system's answer through
`invoke` or what an earlier run remembered (`seen`), comes from the
runtime, and a run that is shown (a preview, `vejas_run_flow`, a replay,
the canary) reads neither. That purity is load-bearing:

- the **business surface** (UPPERCASE literals) can be extracted, shown and
  edited safely — a change is a literal swap plus a unit restart;
- any persisted event can be **replayed** through any version of the flow
  with zero side effects — which is what makes
  [time-travel and canary](../guides/change-safely.md) structural rather
  than bolted on;
- a fixture plus `vejas_run_flow` is a complete test of a flow that reads
  only its event. That run makes no call: an `invoke` of a service that
  answers from a system fails there and says why
  ([composing services](composition.md)), and `seen` is false for every
  key.

The language fits in 20 lines — see the
[VejasScript reference](../reference/vejascript.md). Highlights: null-safe
access (`requester?.email`), array projection/filtering
(`orders[total > 100]`), f-strings, transcoding via literal dicts,
`invoke` to compose services (`services/<name>.vjs`, cross-package with
EXPORTS), `secret("path/key")` for credentials — never a literal.

## Lifecycle

The supervisor watches `VEJAS_ROOT`: a new or changed file becomes a
running unit; a broken one fails loudly without taking the rest down. Each
flow gets a durable consumer on its `source` subject, so **stopped is not
losing**: events accumulate on the stream and drain on restart. Tests live
next to the code (`tests/vjs/` golden cases, per-flow fixtures) and run in
CI with `vejas-runtime vjs-test`.
