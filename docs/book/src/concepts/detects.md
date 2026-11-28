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
  of them is stamped on arrival. A `timer()` is the one thing that runs on
  the wall clock (see [Timers](#timers)).
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
  re-delivers, it never loses an alert. The emits of one event leave together
  or not at all: one the server refuses, over its limit, fails the event, and
  the others go back out with it.
- **Poison.** A payload that is not JSON, or an event the engine refuses
  after `MAX_DELIVERIES` attempts, is parked in the DLQ
  ([ADR-0015](../../../adr/0015-dead-letter-queue.md)) like a flow's would be.
  So is an event whose emit the server refuses.
- **What the engine keeps of an event.** Its decoder keeps the first 256 KiB
  of a string, the first 10 000 elements of an array and the first 1 024
  fields. It cuts the rest, and the runtime is not told: the trace shows the
  event as it came, and an emit that copies a cut field carries the cut value.
- **The connector declaration.** VPL binds sources and sinks to a named
  connector; Vejas ignores its `url` — the bus is `NATS_URL`, there is one.

## Both, either: `and` and `or`

Since engine 0d1c7b0, the steps of a `pattern` declaration can be joined by
`and` (both events, in either order, within the pattern's `within`) and `or`
(either one). Between steps `and` binds tightest, then `or`, then `->`;
parentheses group.

```vpl
pattern ReadyToClose =
    Order as o
    -> (Shipment where id == o.id as s
        and Invoice where id == o.id as i)
    within 1h
    partition by id

pattern Paid =
    Order as o
    -> (CardPayment where id == o.id as p
        or Transfer where id == o.id as p)
    within 1h
    partition by id
```

An order shipped then invoiced, or invoiced then shipped, closes; one paid by
card or by transfer is paid, and `p` names whichever came. A run gives at
most one match through an `or`; when one event completes several
alternatives, the leftmost wins. Two traps:

- **`->` is looser than `or`.** `A -> B or C` is `A -> (B or C)`, not
  `(A -> B) or C`. Write the parentheses you mean.
- **A `where` takes a following `and`.** In `A where x > 1 and B`, the
  filter is `x > 1 and B` and there is no B step. End the step with its
  alias, `A where x > 1 as a and B`, or put it in parentheses. The engine
  warns when `B` is a declared event type (W038).

A pattern that one event completes (`pattern Failed = CardDeclined as f or
TransferBounced as f`) emits at once. Before 0d1c7b0 it emitted when the
unit's next event arrived. A named pattern the engine cannot compile is now
refused at load; it used to be dropped with a log line.
`e2e/detect/run.sh` (D10) runs these patterns on the bus.

## Timers

A `timer(period)` source fires on the runtime's wall clock: Vejas owns the
time and asks the engine for its next deadline. The unit waits for its next
message or that deadline, whichever comes first, and a unit without a timer
waits as it always did. What a timer emits goes out like any other emit: to
its stream's `.to()` subject or `<root>.detect.<unit>.<stream>`, with a
trace of its own, since no message stands behind it. A unit made of timers
alone has no `.from()` and no consumer; a unit that drops its last
`.from()` deletes the consumer it had.

**Beats follow the wall clock, not the data.** The unit fires a timer
after the batch it has read, but when its consumer lags, the beats are
still ahead of the events it has yet to process: an event published before
a beat can reach the engine after it. The exception is the backlog the
consumer holds when the unit starts: the timers wait until it is replayed,
so an event acked after the last snapshot comes back before the beats that
came due while the unit was down.

```vpl
stream Beat = timer(1s)
    .emit(kind: "beat", at: timestamp)
    .to(Bus, topic: "vx.ops.beat")

pattern Waited =
    Order as o
    -> Beat
    -> Beat
    within 1m

stream Stale = Waited
    .emit(rule: "two_beats", order: o.id)
    .to(Bus, topic: "vx.alerts.stale")
```

The fired event carries its deadline as `timestamp` (epoch milliseconds),
and a pattern takes the timer's stream as a step. The first deadline is one
period after the unit starts, plus `initial_delay` when the timer has one
(`timer(1m, initial_delay: 10s)`).

**Late.** When the unit reaches a deadline late by more than a period (a
pause, a stopped runtime), the timer fires once, at the latest deadline
passed, and the periods in between are counted as `timer_periods_skipped`
(see below). A unit back from an hour away emits one beat, not 3 600. The
schedule does not drift: the next deadline stays on the grid of the first.

**Restart.** A unit with a timer keeps a snapshot, and the timers' schedule
is in it. After a restart the schedule goes on where it was, not from the
restart: what came due while the unit was down fires once, and the periods
missed are counted, those since the last snapshot included. A new version
of the program starts a new schedule.

**Clock stepped back.** A timer never fires before its deadline on the
timers' own clock, which only moves forward. When the wall clock steps
back (a manual set, a VM restored), no timer fires until the wall clock
has caught up again, by the size of the step. The unit says it once per
start, on the log with the step in milliseconds and as a `clock_back`
event on `<root>.flow.events` (at most one a minute).

`vpl-check` and `vejas_vpl_check` accept a `timer()` as the unit runs it;
the engine alone, with no clock to drive it, refuses it (E102).
`e2e/detect/run.sh` runs the example above and the restart (D11), and the
replay of an event ahead of the catch-up beat (D12).

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
resume from. A unit with a timer also takes one after its timers fire, on the
same cadence, so that a quiet unit keeps its schedule current.

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

## What the engine loses, counted

A pattern keeps at most 10 000 sequences open at once (per partition for a
partitioned one). Past that, a new first step opens no sequence: the match
it would have found is lost. `VEJAS_DETECT_MAX_RUNS` sets the cap for every
detect unit of the runtime; size it from the formula below, and partition
before you raise it.

Four counters say what was lost without an error, per unit, on `/metrics`
as `vejas_detect_losses_total{unit, kind}` and under `losses` in
`/topology`:

- `runs_dropped`: sequences never opened because the cap was reached;
- `runs_evicted`: open sequences closed early to make room (only under an
  eviction strategy, not the default);
- `filters_without_value`: events a `.where()` could not judge, a field
  missing or of another type, and took as not matching. On a source that
  mixes event shapes this is the normal course; it says how often;
- `timer_periods_skipped`: timer periods that came due while the unit was
  late or down, and did not fire on their own (see [Timers](#timers)).

The totals count since the runtime started and do not go back when a unit
restarts or reloads. A loss of runs or of timer periods is also said: the
first, then at most one a minute (a loss held back is said once the minute
has passed), as a line on the log and a `losses` event on
`<root>.flow.events` with the four totals. A skipped period means the unit
was late, which is worth a warning. `filters_without_value` is counted, not
said.

The engine refuses at load what it would not run: `import`, `context`,
`.enrich()` and `.alert(webhook: ...)` are errors (E100, E101, E103, E104),
not warnings, and so is a `timer()` whose period or `initial_delay` is not a
duration literal, or whose period is zero (E102).

## What it costs

A pattern with 8 sequences waiting or more looks up the sequences an event
can advance instead of visiting each one: equality, order and range
conditions between the event and what a sequence captured are indexed.
Without `.partition_by` the index covers the pattern's sequences (since
engine 6d40466). With it, each partition gets its own index (since engine
b09ff7e): a partition builds one once it holds 8 sequences, drops it under
4, and a partition of one or two sequences, the usual `partition_by id`
shape, never builds one. The index stays off under a negation that spans
the pattern, and covers those conditions only; other sequences are visited
as before. The engine itself measures about 1.4 to 1.6 KB a waiting
sequence.

**Order of matches.** The matches one event completes come out in the
order their sequences opened, within a partition as without one, and so do
those a deadline confirms (a negated step). Before b09ff7e a partition
emitted them in the order of its run list, which removals reshuffled. When
one event completes several sequences, a `.limit()`, `.distinct()` or count
window after the pattern can keep different matches than it did then.

A stateless rule runs at about 30 000 events/s in 10 MB (measured on
engine 1cb201b). A correlation is sized by how many of its sequences are
**open at once**, first steps still waiting for their second:

```
open at once  =  first steps per second  x  how long a first step waits
```

Measured on 2026-11-28 with `PROGRAM=sequence bench/detect.sh` (a two-step
correlation over 500 hosts, partitioned on the host), engines 0d1c7b0 and
b09ff7e alternating on the same box, two runs each:

| Open at once | 0d1c7b0 | b09ff7e | RSS |
|---:|---:|---:|---:|
| up to 100 | 20 300 – 21 200/s | 20 300 – 21 200/s | 22 – 23 MB |
| 1 000 | 20 300 – 20 400/s | 19 600/s | 24 – 25 MB |
| 10 000 | 18 900 – 19 100/s | 18 800 – 18 900/s | 44 – 62 MB |
| 30 000 | 15 700 – 16 200/s | 15 000 – 15 100/s | 87 – 107 MB |
| 100 000 | 11 500 – 11 800/s | 11 800 – 13 300/s | 307 – 424 MB |
| 1 000, unpartitioned | 20 200 – 20 500/s | 20 300 – 20 900/s | 23 – 24 MB |

The two engines are within the noise here. The bench's pairs share the
partition key, so every sequence of a partition passes the condition and
the index has nothing to narrow. It pays when a partition holds many
sequences an event cannot advance (a key of low cardinality, or many orders
waiting under one key). For that shape the engine's own bench gives one
event completing a sequence plus one reopening it at 179 µs with 1 000
sequences in one partition before b09ff7e, and about 2 µs after. What
bounds a unit is memory, about 3 to 4 KB an open sequence.

Before any index (engine 1cb201b, an earlier session on the same box, when
a unit kept 28 600/s with one sequence open), `.partition_by` was worth
about six times the throughput at 1 000 open: 26 000/s against 4 300. The
table shows that gap is gone. Partition a correlation on the field its
steps share all the same: the cap of 10 000 sequences counts per
partition, and the sequences the index cannot file stay within their own
key.

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
