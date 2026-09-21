# 0031 — Pattern detection as a unit type: the Varpulis engine embedded

- Status: Proposed (decided direction; the engine extraction on the Varpulis
  side is under way, the unit is not built)
- Date: 2026-09-21

## Context

A flow is a pure, per-event program (ADR-0001): no state, no clock, no window.
That purity is load-bearing — it is what makes replay, time-travel and canary
structurally safe (ADR-0018, ADR-0021) — and it is also why Vejas cannot say
"three failed logins then a success from the same host within two minutes",
"an SMB connection followed by a remote service start within 120 s", or "an
order that was never shipped". Every one of those is a pattern over *several*
events and *time*, and every one is the shape of the detections a security or
operations team actually wants from an integration platform that already sees
the events.

That capability exists, in the same house, in Rust: Varpulis is a complex
event processing engine built around SASE+ pattern matching — sequences,
Kleene closures, negation, `within` bounds evaluated in event time, tumbling
and sliding windows, joins, trend aggregates and a probabilistic-suffix-tree
forecast — with published, reproducible comparisons against Arroyo, Proton and
Apama and a validated APT29 detection set.

On 2026-09-21 the two products were evaluated for a merge, on adoption alone
(best stack, simplicity, robustness, performance; neither has users). The
evaluation's verdict is the decision below. The facts that carried it:

- **Surface.** Vejas: 1 crate, 11 526 lines, 175 packages, 7 direct
  dependencies, no async runtime, 5.9 MB, clean release build 106 s. Varpulis:
  36 crates, 158 691 lines, 947 packages, tokio, 82 features, 23.8 MB, 485 s.
- **The engine is portable and the platform is not.** The engine crates
  (`varpulis-core`, `-parser`, `-sase`, `-hamlet`, `-pst`, `-zdd`, `-simd`,
  28 800 lines) have no tokio dependency, and `varpulis-runtime` builds with
  its default features off — `cargo tree` then shows 91 packages and zero
  `tokio` — which is how its WASM build already works.
- **Bus semantics.** Same machine, same events, same workload: after a
  `kill -9` mid-stream and a restart, Vejas emitted 20 000 of 20 000 (durable
  consumer, ack after emit); the Varpulis NATS path, core NATS with no ack,
  emitted 10 754 and lost 9 246. Bus-to-bus throughput was 8 100–9 400 evt/s
  for Vejas, bound by its one-ack-per-message consumer path, and above 20 000
  evt/s for Varpulis on a path that promises less. Cold start 11 ms vs 13 ms.

So the fast part of Varpulis is a library, and the slow, duplicated and
fragile part is everything Vejas already is.

## Decision (proposed)

Add a third unit type next to flows and connectors: **`detect`**. A detect
unit is a file `detects/<name>.vpl` — a VPL program, with its `event`
declarations and its streams — run by the Varpulis engine embedded in the
runtime as a library, `varpulis-engine`, taken as a git dependency. The
platform around that engine (its cluster, its server, its CLI, its
connectors, its SaaS layer) is not imported; that is the Varpulis
repository's own retirement decision and is out of scope here.

The contract, in eight points:

1. **Source.** The program's `.from()` bindings name the subjects. The unit
   gets one durable JetStream consumer per unit, exactly like a flow, so
   *stopped is not losing*.
2. **Execution.** One supervisor thread per unit, the engine driven
   synchronously (`Engine::new_sync`, `process_batch_sync_collect`) in
   **event time**: an event's `timestamp` field is its time, arrival order is
   not. No tokio enters the binary.
3. **Emission.** The program's `.emit()` and `.to()` publish on the bus
   **before** the acknowledgement, the same contract as `emit` in a flow
   (ADR-0002): a crash re-delivers, it never loses an alert.
4. **State.** The engine's state is snapshotted to the JetStream object
   store every N acknowledgements, together with the consumer's stream
   sequence at that point.
5. **Restart.** Restore the last snapshot, then resume the consumer at the
   recorded sequence. Events between the snapshot and the crash are replayed;
   because the engine is deterministic in event time, that replay produces
   the same alerts it produced before, and the only visible effect is a
   duplicate alert the downstream already has to dedupe (ADR-0002, ADR-0015).
6. **Partition.** A program's `partition_by` key is the slice key of
   ADR-0020: one unit per slice, one lease per unit. "Partition, don't
   un-singleton" applies unchanged.
7. **Business surface.** The numeric and string literals of a VPL program
   (thresholds, `within` bounds, names) are the unit's business surface, the
   way UPPERCASE constants are a flow's (ADR-0005): visible in the panel,
   corrected in place, replayed against persisted traffic before promotion.
8. **Tooling.** `vejas-runtime vpl-check <file>` beside `vjs-check`; an MCP
   tool `vejas_vpl_check` and a `vejas_vpl_language` contract beside
   `vejas_language`, so the agent that writes flows writes detections the
   same way. The Varpulis LSP and VS Code grammar keep working on `.vpl`
   files as they are.

The first engineering step is not the unit: it is the consumer path.
Vejas acknowledges one message at a time and tops out around 9 000 evt/s per
unit; the engine it is about to host processes hundreds of thousands. Batch
fetch and batch acknowledgement come first, measured with the same harness
the evaluation used, so the fast engine never waits on the slow bus.

## Consequences

**Easier.** The platform gains the one capability its language deliberately
cannot express, from a tested engine, without a second runtime, a second
broker or a second process model. A detection is a file in the user's git
repository, written by an agent from a contract, tested on a fixture,
replayed against real traffic — everything a flow already is.

**Harder.** Two languages in one binary: VejasScript for transformation, VPL
for correlation. This is accepted rather than avoided: folding SASE+ into a
language that is pure by construction would break ADR-0001, and rewriting a
benchmarked engine to save a syntax would be the "Varpulis-mirror risk" of
ADR-0011 in reverse.

**Constrained.** A detect unit is stateful, so the guarantees of ADR-0018 and
ADR-0021 change shape for it: replay is *deterministic in event time from a
sequence*, not *pure per event*. Snapshot and resume (points 4 and 5) are the
design work of this ADR; they are weeks, not months, and they gate the unit.

**Costs.** The binary grows by the engine (its dependency tree is 91
packages; the size will be measured, not claimed), and the release build gets
longer. The engine carries its own open items from the September 2026 audit
of Varpulis (a Kleene precondition, a synchronous path with few unit tests);
they come with it and are tracked here from now on.

## Alternatives considered

- **Express patterns in VejasScript.** Rejected: contradicts ADR-0001 and
  re-implements an engine that already exists and is measured.
- **Keep Varpulis as a separate product beside Vejas.** Rejected by the
  evaluation: two overlapping tools, two languages, two launches at zero users
  each, and a doubled verification surface — the failure mode the
  "distribution before build" guardrail exists to prevent.
- **Fold Vejas into Varpulis instead.** Rejected: keeps 158 000 lines, tokio
  and 947 packages as the base, and a bus path that is at-most-once until
  someone writes the JetStream consumer Vejas already has.
- **Run Varpulis as an exec-bridge process.** Considered, since that is how
  SAP and Salesforce attach (ADR-0011). Rejected for this capability: the
  engine is a library with no I/O of its own, and a bridge would keep the
  whole Varpulis platform alive as a second runtime with its own delivery
  semantics. Bridges are for vendor SDKs, not for our own code.

## Interactions

ADR-0001 (purity stays the flow contract; detect units are the one stateful
kind, by design), ADR-0002 (emit before ack), ADR-0005 (literals as the
business surface), ADR-0006 (MCP: `vejas_vpl_check`), ADR-0015 (a poison
event for a detect unit is parked the same way), ADR-0018 / ADR-0021 (replay
and time-travel semantics for stateful units), ADR-0020 (partition by slice).
