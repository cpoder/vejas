# 0011 — Connector extensibility: external process, not native libs; WASM later

- Status: Accepted
- Date: 2026-08-19

## Context

Built-in drivers (ADR-0007) require recompiling the core. We need to add **new
connector types at run time**, without recompiling and without forcing every
integration into Rust. The obvious first idea is a native dynamic library
(`.so`/`.dll`) loaded with `dlopen`/`libloading`. The question is whether that,
or WASM, or something else, is the right hot-add mechanism.

## Decision

Three levels of "how a driver exists", all behind the **same** stable interface
(the `Driver` trait + the declarative `.vjs` manifest), so the choice per
connector is a `driver "…"` name, not a different platform:

1. **Built-in drivers** — compiled into the binary (`http-in`, `timer`,
   `http-poll`, `slack-out`, `http-out`). For universal, trusted connectors.
2. **External process connectors** — the hot-add path (built): a manifest
   `driver "exec-source"` / `driver "exec-sink"` runs an external program in
   **any language**; the runtime bridges it to the bus over **stdio** (a source
   prints JSON lines on stdout; a sink reads a JSON body on stdin), so the
   program needs **no NATS client**. Isolation is by process (containerizable,
   separable privileges).
3. **WASM component drivers** — _(planned; see triggers below)_ for in-process,
   sandboxed, portable connectors when that is what's wanted.

**We explicitly reject native dynamic libraries (`.so`/`.dll`).** And we do
**not** build WASM yet.

## Why not `.so`/`.dll`

- **No stable Rust ABI.** Passing Rust types across a separately-compiled
  library boundary is UB across compiler versions/flags; it would force a C-ABI
  rewrite (`#[repr(C)]`, raw pointers, `unsafe`).
- **No isolation.** A loaded library runs in the runtime's memory and privileges
  — a buggy or hostile connector crashes or corrupts everything. For
  prompt-generated / third-party connectors this is the wrong default.
- **Portability lost.** A `.so` must be built per OS × arch × libc; clean unload
  (`dlclose` with threads) is a trap.

## Why not WASM *yet* (and when we will)

WASM and external processes are **not substitutes** — they solve different
problems, and will coexist:

- Real connectors that matter (Kafka, AMQP, databases, SAP, Salesforce) need
  full TCP/TLS/SASL and native client libraries. WASI networking is still young
  (wasi-http is usable; raw sockets / heavy clients are not), so those need
  external-process or built-in drivers **regardless of WASM**. Adding WASM now
  does not remove `exec`; it adds to it.
- Building WASM now is a large effort (wasmtime dependency, WIT host/guest
  interface, component model, wasi-http, publish/consume bindings) — premature
  against the "distribution before feature" discipline with zero users yet
  (the Varpulis-mirror risk).
- The security need WASM addresses (isolating untrusted connectors) is already
  met by `exec` (separate process, containerizable). No gap forces WASM today.

**Not debt, because the interface is stable.** A WASM driver will be
`driver "wasm"` + `MODULE = "…"`; the manifest, supervision, graph, and live
config editing are identical. Adding WASM later throws nothing away — it is one
more entry in `driver_for()`.

**Triggers to build WASM drivers:** a real need for *pure* connectors/transforms
(logic, no heavy system I/O) that must run in-process, be portable, and be
distributed through a third-party connector marketplace where the sandbox is
required — and once `wasi-http` (or the needed WASI capability) is sufficient.

## Consequences

- New connector types are hot-addable **today**, in any language, with process
  isolation and no ABI/portability hazard — proven with a pure-shell source
  connector bridged over stdout.
- The `Driver` trait + manifest is the durable abstraction; the three levels
  are additive, so the WASM decision stays open without cost.
- **Cost:** external-process connectors are separate processes (that is the
  isolation, but also more moving parts than in-process); the runtime supervises
  them (restart/backoff, mtime reload) like any connector. Streaming (vs
  periodic-batch) exec sources and per-connector resource limits are refinements
  for later.

## Alternatives considered

- **Native `.so`/`.dll`:** rejected (ABI, isolation, portability) — above.
- **WASM now:** rejected as premature and non-substitutive — above; kept as a
  planned additive level with explicit triggers.
- **Bus-only, no runtime supervision (bring-your-own-process):** already
  supported (SUBJECTS.md) and still valid; the `exec` drivers add supervision
  and a declarative manifest so an external connector is a first-class,
  hot-addable citizen rather than an out-of-band process.
