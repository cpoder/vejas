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
  under the subject root. The unit gets one durable consumer per subject,
  like a flow gets one for its `source`: *stopped is not losing*.
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

## What is not there yet

The engine's state — open sequences, window contents — lives in memory. A
**stateless** detection (a threshold, a filter) loses nothing across a
crash: redelivery replays what was un-acked. A **stateful** one can miss the
matches that straddle a crash, because the redelivered events replay into an
empty engine. The snapshot-and-resume of ADR-0031 (points 4–5) is the next
increment; until it lands, that is the honest limit of a detect unit.

The panel lists detect units in `/topology` under `detects` and their events
in the trace; it does not yet draw them in the pipeline graph.

## Tools

- `vejas-runtime vpl-check <file>` — the engine's verdict, `ok` or the
  refusal, exit code to match; the same check every `.vpl` in the repo gets
  in CI.
- `vejas_vpl_check` over MCP — the same verdict for an agent writing a
  detect unit.
- The Varpulis LSP and VS Code grammar work on `.vpl` files as they are.
