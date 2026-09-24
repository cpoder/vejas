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
- **A window closes when the event time of the types feeding it passes its
  end**: on any event of those types, not only one of the window's own. A
  brute force counted per address on Security events is raised by the next
  Security event of any kind, even when the attacker got in and stopped. A
  window fed by several types waits for the slowest. A type that sends
  nothing for `VEJAS_IDLE_CLOSE_SECS` (60 by default; 0 turns it off) has
  its event time move on with the wall clock, less that grace, so a brute
  force on a sparse source (VPN logons) is raised about a minute after its
  window ends even if the source never speaks again; the unit also checks
  while it has nothing to read. A stream whose events arrive late declares
  `.watermark(out_of_order: 30s)` to hold its windows that much longer.
- **An absence is raised when its deadline passes**, on the same clocks:
  "an order not acknowledged within 4h" (`-> NOT Ack ... within 4h`) is
  raised by the first event that takes the time of the pattern's types past
  the four hours or, when nothing more comes at all, about a grace after
  them. An absence the unit was waiting out when it stopped is in its
  snapshot, and is still raised after the restart.
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

What this costs, said plainly: after a crash a unit replays everything
acked since its last snapshot — up to `VEJAS_SNAPSHOT_SECS` of traffic — so
an alert raised inside that window is raised again. Alerts are at least
once, like everything else on this bus; downstream must tolerate a repeat,
by its own key or by the alert's fields. Lower the cadence to shrink the
window; the snapshot costs a write to the object store at a batch boundary.

Not in a snapshot: trend aggregates and forecasts, which start empty after
a restart. `vejas_snapshots_total`, `vejas_snapshot_seq`,
`vejas_snapshot_bytes`, `vejas_restores_total` and `vejas_restore_seq` on
`/metrics` say what a unit is doing about it.

## What it costs

A stateless rule runs at about 30 000 events/s in 10 MB. A correlation is
sized by how many of its sequences are **open at once** — first steps still
waiting for their second:

```
open at once  =  first steps per second  x  how long a first step waits
```

Under a hundred open, a unit keeps 25 000 events/s; at a thousand, 17 000;
at ten thousand, 11 000 and about 40 MB. Past that it falls away sharply, so
ten thousand open sequences is the number to stay under on one unit.
`.partition_by` on the field a pair shares is worth three to four times the
throughput as soon as anything is open — partition every correlation.

One process saturates near 44 000 events/s whatever the unit count; beyond
it, add instances. The measurements and the method are in
[`bench/README.md`](https://github.com/cpoder/vejas/blob/master/bench/README.md).

## See it

`e2e/detect-demo/run.sh` is four beats on one bus, each asserted: two units
boot; a lazy attacker runs PsExec under its own name and both a
signature-style rule and a behavioural sequence fire; the attacker renames
the binary and the signature goes quiet while the sequence still fires; and
the runtime is killed with `-9` between the two halves of a sequence, after
which the alert still arrives out of the snapshot. It runs in about forty
seconds, brings up its own bus, and `PAUSE=manual` walks it beat by beat.

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
