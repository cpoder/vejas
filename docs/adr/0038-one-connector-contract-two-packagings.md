# 0038 — One connector contract, two packagings

- Status: Accepted, not built (the founder chose this on 2026-11-26)
- Date: 2026-11-26

## Context

**Four shapes, four contracts.** A connector is one of these today:

1. **A driver in the runtime.** Twelve, all in `core/src/connectors.rs`
   (2,865 lines). The same file holds services the whole runtime uses: the
   traced publish that opens a trace, the sink loop (ack, retries, DLQ, one
   hop per delivery), offsets in KV, the audit stream, state snapshots.
2. **An exec child**, in any language, that speaks JSON lines on its stdin
   and stdout to one of four exec drivers (`exec-source`, `exec-sink`,
   `exec-stream-source`, `exec-rpc`), each with its own line protocol:
   Salesforce, SAP RFC (ADR-0014), Kafka through `kcat` (ADR-0022).
3. **A standalone Rust binary on NATS**: IBM MQ (ADR-0023) and AMQP. Each
   compiles the runtime's `bus.rs` into itself through a `#[path]`
   attribute, and is configured by environment variables. These two are
   second-class:
   - they publish without a trace context, so a message from a queue starts
     no trace and its first hop is missing from the record (ADR-0034);
   - they have no probe, so admission needs a stated exception for them;
   - the topology does not show them, since the runtime does not run them;
   - neither the release archive nor the image carries them.
4. **A recipe**: a manifest on a driver (ADR-0037). This record does not
   change recipes.

**The question.** The founder asked whether every connector should rather be
an independent Rust binary built on a framework. Measured on this
repository: a binary that speaks to the bus carries its own tokio, rustls and
NATS client, and `vejas-mq` is 3.0 MB, `vejas-amqp` 4.7 MB, against 11.6 MB
for the runtime with all twelve drivers. Each binary is one more process to
supervise. Every cross-cutting feature (the trace record, secrets, the probe,
the topology, hot reload) would cross a process boundary, with a protocol to
version between the runtime and each binary. And the catalog grows by recipes
on protocol drivers, without compiling anything (ADR-0037), which a binary
per connector does not change.

The framework is the right part of the question: there is no single contract
a connector is written against.

## Decision

1. **One contract, in a crate.** `vejas-connector` (at `sdk/`) holds what a
   driver is written against:
   - the `Driver` trait and its `Config`;
   - the bus module (ADR-0035), which the runtime and the standalone
     binaries then share as a dependency, not as an included file;
   - the services a driver uses, behind a `Host`: the traced publish that
     opens a trace or continues the caller's, the consume loop (ack,
     retries, DLQ, the hop of each delivery), offsets in KV, request/reply
     serving, the probe;
   - the event a hop produces (its span, its input, its answer, its
     outcome), which is what the trace record is written from.
2. **Two packagings of the same driver code.**
   - **In the runtime.** The runtime's host records each hop itself: trace
     record, metrics, events ring, as today.
   - **Standalone.** The binary's `main` is `vejas_connector::serve(driver)`.
     The runtime starts it for a manifest that names its driver (`driver
     "ibm-mq"` starts `vejas-connector-ibm-mq`), restarts it as it restarts
     any unit, and stops it on reload. The binary is found next to the
     runtime's executable or in `VEJAS_CONNECTORS_DIR`, never on `PATH`: a
     manifest names a driver, not a program.
   - **The protocol between the two.** The first stdin line carries the
     configuration with its secrets resolved: never argv, the environment
     or a file at rest. The rest of stdin carries control requests (probe,
     stop); end of file means the runtime is gone, and the binary exits.
     Data goes from the binary straight to NATS, with the trace context in
     the message headers. Each hop, probe answer and health report comes
     back as one JSON line on stdout. The runtime records it through the
     same path as an in-process hop, so the record, its masking and its
     vault scrub stay in one place. Logs go to stderr. The protocol is
     versioned: the configuration line says its version, and a binary that
     does not know it exits with that reason, which the unit shows as its
     last error.
3. **The rule for choosing.**
   - In the runtime: a protocol driver in pure Rust whose dependencies add
     about 1 MB or less (HTTP, webhooks, MQTT, SQL with a pure-Rust client).
   - A standalone binary: a driver that links a C or proprietary SDK (IBM
     MQ, SAP NW RFC, Oracle, DB2: licences, crash isolation), one whose
     dependencies are heavy, or third-party code.
   - An exec child: when the best client exists in another language. The
     four exec drivers converge on one line protocol when they are next
     touched.
4. **One way to configure and certify.** Every connector, standalone ones
   included, is configured by a manifest, and admission has one path: the
   environment files of standalone recipes go. Certifying MQ and AMQP
   against a real broker (`dataflow.sh`) stays.

## Order

Each step is its own pull request and leaves `master` green.

1. Split `connectors.rs`: one module per driver, the services apart. No
   behaviour changes.
2. The crate: bus, `Driver`, `Config`, `Host`, the hop event. The runtime
   builds on it, and the Docker build copies `sdk/` with `core/`.
3. The standalone host and the runtime's supervision of standalone drivers.
   IBM MQ and AMQP move onto it: their messages carry the trace, they have
   a probe and show in the topology, and the release archive and the image
   carry them.
4. Postgres (ADR-0037, driver 2), the first driver written on the crate, in
   the runtime.

SAP RFC and Salesforce move when they are next worked on.

## Consequences

- One contract to write a connector against. A third party builds a
  standalone driver on the public crate without touching the core.
- The trace runs end to end through IBM MQ and AMQP.
- The runtime's size does not change; the standalone binaries keep theirs,
  and the release archive grows by them.
- The runtime and each standalone binary must agree on the protocol version;
  a mismatch stops the unit with a stated reason instead of failing quietly.
- A standalone connector now runs under a runtime. One deployed today as its
  own service, configured by environment, moves to a manifest.

## Relation to ADR-0030 and ADR-0035

ADR-0030 (proposed) keeps the core free of any plugin trait or in-process
extension point, and attaches enterprise capabilities as standalone binaries
on public seams, naming MQ and AMQP as the precedent. This record keeps that
line. The `Driver` trait is compiled in and loads no outside code; the
standalone protocol is a public, documented and versioned seam between
processes; and the core runs the same with no standalone connector at all.
ADR-0035's island stays an island: the bus module moves into the crate as it
is, and NATS types stay inside it.

## Rejected

- **Every connector a standalone binary.** One shape for all, at the cost
  measured above: several MB and one process per connector, every
  cross-cutting feature behind a process boundary, and the single binary on
  NATS, an argument for adoption at the edge, gone. The protocol drivers
  gain nothing from the isolation.
- **Dynamic libraries loaded by the runtime.** Rust has no stable ABI, so a
  library built by another compiler version breaks the runtime, and a crash
  in it is a crash of the runtime. ADR-0030 rejects in-process extension.
- **WebAssembly components.** They isolate without a process, but add a
  WebAssembly runtime of several MB, and the C SDKs that call for a separate
  packaging (IBM MQ, SAP) cannot run inside one.
- **Keeping the `#[path]` inclusion.** It shares code without a contract,
  and the trace gap stays.
