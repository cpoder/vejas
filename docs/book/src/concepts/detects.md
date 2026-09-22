# Detect units

A detect unit is one file under `detects/`, with a `.vpl` extension: a
program in **VPL**, the pattern language of the Varpulis engine, which Vejas
embeds as a library ([ADR-0031](../../../adr/0031-pattern-detection-unit-varpulis-engine.md)).
Where a flow is a pure, per-event transformation, a detect unit is the one
kind of unit that **remembers**: a sequence of events, a window, a count over
time, an absence.

```vpl
connector Bus = nats (
    url: "ignored: the bus is NATS_URL"
)

event SmbConnect:
    host: str
    target: str

event ServiceStart:
    host: str
    image: str

stream Net = SmbConnect
    .from(Bus, topic: "vx.sysmon.net")

stream Proc = ServiceStart
    .from(Bus, topic: "vx.sysmon.proc")

stream LateralMovement = SmbConnect as smb
    -> ServiceStart where host == smb.target as svc
    .within(2m)
    .emit(rule: "lateral_movement", from: smb.host, to: svc.host, image: svc.image)
    .to(Bus, topic: "vx.alerts.lateral")
```

An SMB connection followed, within two minutes, by a service starting on the
machine it connected to: one alert, on `vx.alerts.lateral`.

## The contract

- **Source.** Every `.from(<connector>, topic: "...")` names a bus subject
  under the subject root. The unit gets one durable consumer over all of
  them, like a flow gets one for its `source`: *stopped is not losing*. One
  consumer, not one per subject, so the engine sees the bus in stream
  order: a sequence across two subjects is judged in the order the events
  were published, also when a backlog is replayed after a restart. Several
  subjects need NATS 2.10 or newer.
- **Time is event time.** A payload's `@timestamp` (RFC 3339), else its
  `ts` or `timestamp` (epoch milliseconds), is the event's time. Every
  `.within()` and window is judged against it, never against the clock, so
  a replay of the same events gives the same alerts. A payload without any
  of them is stamped on arrival.
- **Types.** A string `event_type` in the payload names the event type;
  without it, the type is the one the `.from()` binding declares for that
  subject. The engine's own decoder does this, the same one every Varpulis
  connector uses.
- **Emission before acknowledgement.** An `.emit()` goes to the subject its
  stream's `.to(...)` names, or to `<root>.detect.<unit>.<stream>` without
  one, and it is published before the source messages are acked
  ([ADR-0002](../../../adr/0002-nats-only-infrastructure.md)): a crash
  re-delivers, it never loses an alert.
- **Poison.** A payload that is not JSON, or an event the engine refuses
  after `MAX_DELIVERIES` attempts, is parked in the DLQ
  ([ADR-0015](../../../adr/0015-dead-letter-queue.md)) like a flow's would be.
- **The connector declaration.** VPL binds sources and sinks to a named
  connector; Vejas ignores its `url` — the bus is `NATS_URL`, there is one.

## Snapshot and resume

The engine's state — open sequences, window contents, join buffers — lives
in memory, and a unit that has any (a `->`, a window, a join) keeps a
snapshot of it in the JetStream object store `VEJAS_DETECT_STATE`, one
object per unit: the state as the engine serialises it, under a header
naming the program it belongs to and the **stream sequence it stands
for**. It is taken at a batch boundary every `VEJAS_SNAPSHOT_SECS` (5) or
`VEJAS_SNAPSHOT_ACKS` (10 000), whichever comes first, never while a
message is waiting for redelivery, once more on a clean stop, and once at
start, so that a crash before the first cadence still has a sequence to
resume from.

On restart the unit restores its snapshot and re-creates its consumer at
the sequence after it. Everything acked since the snapshot replays from
the bus into the restored state: the engine is deterministic in event
time, so a sequence that was open at the snapshot closes on the event that
was to close it, and one that closed inside the replay window emits again —
at least once, like every unit
([ADR-0002](../../../adr/0002-nats-only-infrastructure.md)). A snapshot of
another version of the program is ignored (the state of one program is not
the state of another) and the unit starts empty at its ack floor. A
**stateless** detection (a threshold, a filter) keeps no snapshot and
resumes at its ack floor, as a flow does.

Not in a snapshot: trend aggregates and forecasts, which start empty after
a restart. `vejas_snapshots_total`, `vejas_snapshot_seq`,
`vejas_snapshot_bytes`, `vejas_restores_total` and `vejas_restore_seq` on
`/metrics` say what a unit is doing about it.

## What is not there yet

The panel lists detect units in `/topology` under `detects` and their events
in the trace; it does not yet draw them in the pipeline graph.

## Tools

- `vejas-runtime vpl-check <file>` — the engine's verdict, `ok` or the
  refusal, exit code to match; the same check every `.vpl` in the repo gets
  in CI.
- `vejas_vpl_check` over MCP — the same verdict for an agent writing a
  detect unit.
- The Varpulis LSP and VS Code grammar work on `.vpl` files as they are.
