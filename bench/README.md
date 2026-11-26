# The benchmark harness

Reproducible, end-to-end, honest. One command:

```
cargo build --release --manifest-path core/Cargo.toml
bench/run.sh [seconds] [concurrency]      # defaults: 30s, 32 connections
```

## Scenario

The full production path, nothing synthetic skipped:

```
loadgen (HTTP POST, keep-alive) → http-in (webhook) → NATS JetStream
      → flow bench_orders (lookup table, conversions, projection loop, branch)
      → NATS JetStream → http-out (sink driver) → counting sink (HTTP)
```

Every event carries a `t` stamp from the load generator; the sink measures
true end-to-end latency on arrival. The run reports cold start (spawn →
`/healthz` 200), sustained ingest and delivered rates, latency percentiles,
runtime RSS and binary size — as JSON, from `bench/run.sh`. A dedicated
`nats-server` on its own port and throwaway store keeps the measure clean.

## Current numbers (dev machine, 8 cores, WSL2 — pre-optimization)

All five ceilings fell (#4+#3 `5cffb7b`, #2 `b34d9f3`, #1 `75fad83`,
#5 `d5c45d7`…`5bf3a7c`):

| Metric | v0 | all five fixes in |
|---|---|---|
| Cold start | 13–15 ms | **11–13 ms** |
| Runtime RSS under load | 6–8 MB | **10 MB** |
| Binary size | 3.9 MB | 11.2 MB (rustls, async-nats) |
| e2e sustained, paced ~1 900/s (`bench/paced.sh 2000 15`) | — | **p50 14 ms, p99 36 ms**, 20 000/20 000 |
| e2e saturated (32 conns) | 65/s delivered | ~4 900/s ingest, **~2 650/s delivered** (sink-bound, queue absorbs the rest) |
| e2e latency, uncongested (`bench/paced.sh 20 15`) | p50 859 ms | **p50 2 ms, p99 3 ms** |
| MQTT broker loopback, QoS 1 both ways (`bench/broker-mqtt.sh 5000`) | — | **2 070 rt/s**, 5 000/5 000 |
| Isolated flow hop | 171/s | **8 110/s** |

The old "sustained (12 conns) 1 701/s, p50 18 ms" row died with ceiling #5:
that operating point was the flusher floor throttling each connection to
~140 req/s — with the floor gone, `bench/run.sh` saturates the pipeline at
any concurrency, so steady-state latency is now measured **paced**
(`bench/paced.sh`, fixed event rate below the sink bound).

Every hop persisted in JetStream throughout — the guarantee never moved.

### Since ADR-0035

The sync `nats` client left for `async-nats` behind `core/src/bus.rs`, so findings
#5 and #6 below describe a client that is gone, along with `publish_confirmed`,
`fetch_iter_flushed` and the second connection of #7. Against the v0.3.6 binary,
alternating in one session on this box:

| | v0.3.6 | now |
|---|---:|---:|
| Threads at 1 / 10 / 50 / 200 idle flows | 16 / 61 / 261 / 1 011 | 11 / 20 / 60 / 210 |
| NATS connections, same | 3 / 21 / 101 / 401 | 1 |
| RSS, same, MB | 8.9 / 16.7 / 46.9 / 139.8 | 9.6 / 10.5 / 14.2 / 26.4 |
| Flow hop (`PUBS=16`, 64 000 events) | 13 100 – 13 700/s | 13 200 – 13 500/s |
| Flow hop, `bench/pub.py` as publisher | 20 100 – 20 500/s | 19 400 – 21 400/s |
| One detect unit | 30 800 – 35 400/s, 11 MB | 34 700 – 41 100/s, 12.7 MB |
| MQTT loopback | 2 370 – 2 430 rt/s | 2 050 – 2 090 rt/s |
| Spawn to a flow running | 1 714 ms | 10.9 ms |
| Paced 2 000/s, p50 / p99 | 3–4 / 14–17 ms | 3 / 14–15 ms |
| Binary | 9.9 MB | 11.2 MB |

The paced row is why the p50 14 ms above stands as published: both binaries are far
below it today, so this change did not move it. The detect tables further down
predate this: add about 1.7 MB to their RSS. The method and the MQTT cost are in
ADR-0035.

The footprint numbers are the thesis, measured. The throughput and latency
numbers are **known ceilings of the v0 I/O paths, not of the interpreter** —
each one matches its cause exactly:

1. **http-in accept poll + no keep-alive** — *fixed* (`75fad83`):
   keep-alive loop + TCP_NODELAY (Nagle was the hidden half) + 5 ms accept
   poll. Ingest 322/s → 4 879/s.
2. **curl-per-message** — *fixed* (`b34d9f3`): a pooled pure-Rust HTTP
   client (ureq + rustls) shared by http-out/http-poll/oauth-poll;
   ~30× on the sink leg, secrets move from temp files to memory.
3. **Batch-fill wait** — *fixed with #4* (`no_wait` pulls, immediate
   delivery, anti-zombie invariant preserved).
5. **The nats-crate 5 ms flusher floor** — *fixed*
   (`d5c45d7`…`5bf3a7c`): the sync `nats` client rate-limits writer
   flushes to one per 5 ms (`MIN_FLUSH_BETWEEN`, hard-coded), so every
   *sequential* JetStream pub-ack — a request/reply — was capped at
   ~200/s. Surfaced by the MQTT loopback bench (js.publish p50 5.1 ms,
   dead on the tick, isolated by per-step instrumentation).
   `publish_confirmed()` re-does the request with a direct caller-thread
   flush — same at-least-once contract, no new dependency — and is wired
   at the four sequential publishers: mqtt-in, MQ source, http-in ingest,
   exec-stream (kcat). Ingest request p50 5.2 → 0.8 ms; MQTT loopback
   8 → 2 285 rt/s; uncongested e2e p50 6 → 2 ms.

Numbers here are updated by re-running the harness after each fix — never
quoted without the scenario and the machine.

## The isolated flow hop (`bench/flow-only.sh`)

Publish straight onto the bus, run only the flow, count its emits with a
plain subscription — no HTTP anywhere:

| Metric | v0 | after `5cffb7b` | `PUBS=4` | after the consumer-loop rework (`PUBS=16`, 64 000 events) |
|---|---|---|---|---|
| Flow-hop rate | 171/s | ≥ 2 786/s (publisher-bound) | 8 110/s | **13 652/s** |
| Runtime RSS | 5.6 MB | 5.5 MB | 6.2 MB | 7.5 MB |

Finding **#4 (the structural one — fixed)**: ~5.8 ms per message was the
per-message synchronous JetStream round-trips in the consumer loop. The
rework buffers emits and does **one flush per batch before acking that
batch** (publish-before-ack preserved, at-least-once intact — 5 000-burst
loss test: 0 lost, DLQ clean), with `no_wait` pulls killing the batch-fill
wait (#3) at the same time. 16× on the isolated hop; the true ceiling needs
a faster publisher to measure.

Findings **#6 and #7 (the consumer loop — fixed, 2026-09-21)**, found with
the flow hop instrumented per phase (`vejas_round_seconds_sum{phase}` and
`vejas_fetch_rounds_total` / `vejas_fetch_messages_total` on `/metrics`):

- **#6 The pull request sat behind the flusher floor.** A pull request is a
  publish, and the sync `nats` client only buffers a publish — its flusher
  thread waits at least 5 ms between writes (the same floor as #5). Every
  round of 64 messages paid it: 7 ms a round, 9 003 evt/s on the merge
  evaluation's workload, with the interpreter at 10 µs per event. A direct
  flush of the request (`fetch_iter_flushed`) made it one round-trip.
- **#7 The loop collected a batch, then processed it.** The reader thread
  decoded 64 messages while the flow thread waited, then the flow thread ran
  64 while the reader idled. Now the loop consumes messages as they arrive,
  keeps the next pull request in flight while it works, and sends its acks on
  a connection of their own so no PING waits behind them; the trace ring
  keeps the event and derives the preview on read instead of serialising
  every message twice.

Together: **9 003 → 16 133 evt/s** on the evaluation's flow (`h2h`, 16
publishers), **8 110 → 13 652/s** on the bench flow above. What remains,
measured: ~33 µs of loop time per message (12 µs of it the interpreter, the
rest JSON in and out, the trace ring, metrics) and two round-trips per
64-message batch. For scale, `nats bench js fetch --batch 64` (the Go client,
explicit acks, same server, same box) pulls 72 644 msgs/s: the server is not
the ceiling, the sync client's receive path and the per-message bookkeeping
are. Past ~16 k/s per unit, partition (ADR-0020: one unit per slice, one
lease per unit) rather than tune further.

**The detect unit** (ADR-0031, the Varpulis engine embedded) on the same
harness and the same program: **18 643 evt/s** at 16 publishers (17 265 at
32), 10.3 MB RSS, against 16 182 for the flow unit — the engine's share is
about 20 µs per event under load and under 10 µs in isolation (its own
`throughput_probe`). Same consumer path, same publish-before-ack barrier.

## What tracing costs (ADR-0034, `bench/trace-cost.sh`)

`bench/trace-cost.sh` runs the isolated flow hop above in each tracing
configuration, interleaved, five times each (20 000 events, four
publishers), and reads the runtime's own CPU time from `/proc`. With four
publishers the rate is theirs; the CPU time is the runtime's. Spans go to a
collector that only counts them (`bench/otlp-discard.py`). 2026-11-26, same
machine; the runtime from before tracing is master at `491ba69`, passed as
`BASE_BIN`:

| Configuration | Flow-hop rate | Runtime CPU per event | RSS |
|---|---|---|---|
| Before tracing | 9 390/s | 72.5 µs | 9.7 MB |
| Context propagated, no exporter | 9 403/s | 74.0 µs | 10.4 MB |
| Every span exported | 9 166/s | 83.0 µs | 12.3 MB |
| One trace in ten exported | 9 204/s | 76.5 µs | 12.0 MB |
| Before tracing, events carrying `traceparent` | 8 718/s | 71.0 µs | 9.5 MB |
| No exporter, events carrying `traceparent` | 8 632/s | 78.5 µs | 10.5 MB |
| Every span exported, events carrying `traceparent` | 8 669/s | 88.0 µs | 12.6 MB |

The 200 000 spans of the "every span" runs all reached the collector, and
10 023 of the 100 000 events sampled at one in ten. Over four series the
context alone measured between 1.5 and 4.5 µs; over three, export held at 10
to 10.5 µs over the runtime from before tracing. Events that carry a header slow the
publishers (the `nats` CLI sends it) as much before tracing as after.

Two findings on the way, both fixed before the numbers above:

- **#8 One HTTP request per handful of spans.** The first exporter posted
  whatever its queue held and built a JSON tree per span: export cost 63 µs
  of CPU per event over the same runtime without an exporter. Batching as
  the OpenTelemetry SDKs do (512 spans or 1 s, `OTEL_BSP_*`) and writing the
  JSON directly brought it to 28 µs.
- **#9 Waking the exporter for every span.** An exporter that waits on its
  queue has every producer wake it, a futex call and a context switch per
  span. Reading the queue on a 25 ms tick while spans are pending brought
  export to between 5.5 and 9 µs.

### The trace record (lot B)

The same script runs the record's configurations, interleaved with lot A's
(the record is on by default, so lot A's rows set `VEJAS_TRACES=off`).
2026-11-26, same machine, a first series on the first complete scrub:

| Configuration | Flow-hop rate | Runtime CPU per event | NATS CPU per event | RSS |
|---|---|---|---|---|
| Record off, no exporter | 9 302/s | 75.5 µs | 168.5 µs | 10.2 MB |
| Record | 9 238/s | 93.0 µs | 177.0 µs | 13.5 MB |
| Record, payloads off | 9 112/s | 87.5 µs | 180.0 µs | 13.1 MB |
| Record, one trace in ten | 9 191/s | 79.5 µs | 167.5 µs | 12.1 MB |
| Record and every span exported | 9 162/s | 99.0 µs | 176.0 µs | 15.2 MB |

A second series, on the scrub as it shipped after its second review (the
event above goes through the same steps of it):

| Configuration | Flow-hop rate | Runtime CPU per event | NATS CPU per event | RSS |
|---|---|---|---|---|
| Record off, no exporter | 9 066/s | 74.0 µs | 167.5 µs | 10.1 MB |
| Record | 8 450/s | 94.0 µs | 170.0 µs | 13.2 MB |
| Record, payloads off | 8 865/s | 87.5 µs | 166.5 µs | 12.6 MB |
| Record, one trace in ten | 9 033/s | 77.5 µs | 158.5 µs | 11.8 MB |
| Record and every span exported | 8 933/s | 102.5 µs | 166.0 µs | 15.0 MB |

Each full run recorded its 20 000 events, and the one-in-ten runs about
2 000. Recording every hop costs the runtime 17.5 to 20 µs of CPU per event
over the two series, 12 to 13.5 without payloads, 3.5 to 4 at one trace in
ten. The NATS server's CPU per event moves by several µs from one series to
the next: storing the hops costs it 2.5 to 8.5 µs. A profile of a
100 000-event run puts about 13 µs of the record's cost on its own thread,
which the flow hands its hop to and does not wait for.

- **#10 A JSON tree per record.** The first record built a
  `serde_json::Value` for each hop, payload included, and masked it in
  place: 25.5 µs per event, and a profile put 22 % of the runtime's CPU on the
  record's thread, most of it allocating and freeing the tree. Writing the
  record straight to JSON text, the payload transcoded through serde without
  a tree, brought it to 17.5 µs, although the scrub does more (text forms,
  every vault value in one pass).
- **The subject index.** One subject per trace lets one filtered read return a
  trace, and the server indexes every subject in memory.
  `bench/trace-subjects.py` publishes hop records of about 400 bytes into a
  file stream, one subject per trace or one subject for all, and reads the
  server's memory at peak and after a restart. On NATS 2.10.24 and 2.15.3 a
  subject per trace costs 200 to 450 bytes per trace: 20 to 45 MB for
  100 000 one-hop traces, 7 to 15 MB for 100 000 hops in traces of three. A KV
  bucket mapping trace id to sequences, the fallback ADR-0034 named, keeps a
  subject per key, so it would cost the same.

### The steps (point 8)

A flow hop's record also lists the statements its run went through; the
benchmark's flow runs eleven per event. With `BASE_BIN` set to the runtime
from before the steps (master at `a8f0a6a`, the record on by default),
`bench/trace-cost.sh` runs it in its "before tracing" rows. Three series on
2026-11-26, runtime CPU per event:

| Series | Record without steps (`a8f0a6a`) | Record with steps | Difference | Record off |
|---|---|---|---|---|
| 1 | 95.5 µs | 100.0 µs | +4.5 µs | 74.5 µs |
| 2 | 97.0 µs | 102.0 µs | +5.0 µs | 75.5 µs |
| 3 | 95.5 µs | 103.0 µs | +7.5 µs | 75.5 µs |

The first two ran the steps before their review, the third after it and
after #11 below. The difference moves more from one series to the next than
anything that changed between them, so `bench/pairs.sh` runs the two
runtimes in eight alternating pairs (A then B, B then A) and reads each
thread's CPU time from `/proc`. A is `a8f0a6a`, B the steps:

```
µs of CPU per event        A       B                                          B - A, each pair   median
runtime                 99.2   104.5            +9.0 +12.5  +3.0  +3.5 +20.5  +5.0  +6.0  +5.0     +5.5
flow:bench_orde         72.0    75.5            +5.5  +8.0  +2.0  +1.5 +14.0  +3.5  +4.0  +3.0     +3.8
vejas-record            15.0    17.0            +2.0  +3.0  +1.5  +2.5  +5.0  +2.0  +2.5  +2.0     +2.2
vejas-bus               10.5    11.2            +1.0  +2.0  +0.0  +0.0  +2.0  +0.5  -0.5  +0.0     +0.2
(medians of 8 pairs; hop records kept, A: 20000 to 20000, B: 20000 to 20000)
```

A first series of eight pairs, run the same way before the script existed,
gave a median of +3.5 µs, +1.5 on the record's thread and +2.5 on the
flow's. The steps cost 3.5 to 5.5 µs of CPU per event, about 2 of them on
the record's thread, which the flow does not wait for.

CPU time moves with whatever else the machine does; the instructions a
thread runs hardly do. `perf stat -e instructions:u --per-thread` on the
runtime over the same run, three runs each (thousands of instructions per
event, the same to 0.1 k from run to run):

| | Record off | Record on | Flow's thread, on | Record's thread |
|---|---|---|---|---|
| `a8f0a6a` | 198.9 k | 262.4 k | 197.1 k | 54.2 k |
| The steps | 197.5 k | 266.2 k | 197.5 k | 57.5 k |

With the record off, the interpreter runs slightly fewer instructions than
before. With it on, the steps are 3.8 k instructions per event, 3.3 k of
them on the record's thread; on the flow's thread they are 0.5 k, a push
per statement, but cost about 2 µs of its user time (its system time does
not move). Its cycles grow with its cache misses, 10 to 25 % more
(`cache-misses:u`, two runs each): memory the flow's thread now shares with
the record's thread. With the record off, the same pairs swing both ways,
from -6.0 to +9.5 µs, where the instructions say the work is the same:

```
µs of CPU per event        A       B                                          B - A, each pair   median
runtime                 79.0    80.5            +5.0  +3.5  -5.5  +2.5  -3.5  +6.0  -6.0  +9.5     +3.0
flow:bench_orde         67.2    68.2            +4.0  +3.0  -3.5  +2.0  -0.5  +4.5  -3.5  +6.0     +2.5
vejas-bus               11.0    11.5            +0.0  +1.0  -1.0  +0.5  -3.0  +1.0  -2.5  +3.0     +0.2
(medians of 8 pairs; hop records kept, A: 0 to 0, B: 0 to 0)
```

- **#11 Handing the steps over.** Recording a statement is a push into a
  vector. In three interleaved runs each, a build that recorded the steps and
  dropped them on the flow's thread cost what master did, and the first
  build, which handed them to the record's thread, cost 6.5 µs more. That
  build gave each run a new vector, which the record's thread freed once
  written: glibc's malloc then takes the lock of the arena the flow's thread
  allocates from, and a profile showed the record's thread waiting on it
  (`__lll_lock_wait_private`). A run now takes its vector from a small pool
  that the record's thread gives it back to, a line number is written
  without `fmt`, a repeated subject is copied from the record already
  written, and the attributes the runtime computes are shared instead of
  copied for every span. The flow's thread still pays more than the pushes
  do. A profile of it puts about 40 % of its time in glibc's malloc and
  free, most of it for the interpreter's own values, so an allocator built
  for threads that free each other's memory is the next thing to measure.

## The true hop ceiling and multi-flow scaling

With parallel publishers, the single-flow hop tops out around **14–16 k/s**
(`PUBS=16`; four publishers push ~10.6 k/s and the flow drains as fast as
they publish). Scaling the number of flows (`bench/multi-flow.sh`):

| Flows | Aggregate rate | Runtime RSS |
|---|---|---|
| 1 | 7–8 k/s | 10 MB |
| 10 | **13 700 – 14 700/s** | 14.5 MB |
| 50 | 11 100 – 12 100/s | 38 MB |

Throughput *rises* with flow count (consumers parallelize; the bus, not the
interpreter, is the bound) and memory stays around half a megabyte per running
flow under load — fifty live, persisted flows in under 40 MB.

## Detect units (`bench/detect.sh`)

A detect unit is a VPL program run by the embedded Varpulis engine. Two
programs are measured, both in `bench/root/detects/`:

- **`bench_orders.vpl`** — the twin of `flows/bench_orders.vjs`, so the two
  unit kinds meet on one workload: a lookup with a fallback, two conversions,
  a threshold, one emit per event.
- **`bench_sequence.vpl`** — what a flow cannot express: an SMB connection
  followed, within two minutes of **event** time, by a process whose parent is
  `services.exe`, keyed on the host. Every pair matches, so the run measures
  carrying state rather than leaking it.

Events are published by `bench/pub.py`, a socket and a buffer — the NATS
protocol is a line and a payload. It does 2.1 M messages/s onto a subject
nobody reads, so the publisher is never what is being measured. (The `nats`
CLI tops out near 16 k/s across sixteen processes, which is what earlier
numbers in this file were actually reporting.)

```
bench/detect.sh 64000 1                       # a stateless rule, one unit
PROGRAM=sequence LAG=1000 bench/detect.sh      # a correlation, 1000 runs open
```

### A stateless rule

| Units on one instance | Aggregate | Per unit | RSS |
|---|---:|---:|---:|
| 1 | 33 000 – 35 400/s | same | 10.2 MB |
| 2 | 40 900 – 44 700/s | ~21 000/s | 11.0 MB |
| 4 | 44 300 – 44 600/s | ~11 000/s | 13.3 MB |

One unit does the work; more units share one process and the box saturates
near **44 000/s**. Past that, add instances, not units.

### A correlation, and what it costs

The number that matters is not the key count and not the event rate: it is
how many correlations are **open at once** — first steps still waiting for
their second. `LAG` sets it directly.

| Open at once | Rate | RSS | Unpartitioned |
|---:|---:|---:|---:|
| 1 | 28 600/s | 12.5 MB | — |
| 100 | 28 200 – 28 600/s | 13.3 MB | — |
| 1 000 | 25 900 – 26 200/s | 16.2 – 16.5 MB | 4 250 – 4 260/s |
| 10 000 | 21 600 – 22 600/s | 32 – 34 MB | — |
| 30 000 | 14 200 – 14 900/s | 88 – 109 MB | — |
| 100 000 (400 000 events) | 11 100 – 11 200/s | 448 MB | — |

Three things fall out of that table.

**`.partition_by` is worth about six times** as soon as anything is open:
without it the engine has to consider every open run for every event, and at
1 000 open that is 26 000/s against 4 300. Partition every correlation, on
the field the pair shares.

**What bounds a unit is memory, not a cliff.** An open correlation costs
about 2 KB at ten thousand and 4.5 KB at a hundred thousand; the rate falls
gently with the runs each partition carries (here 500 hosts, so 200 runs a
partition at a hundred thousand open).

**The cliff this table used to show is gone.** Up to v0.3.4 the engine swept
every partition's open runs on each event, to expire them and to confirm
absences: 30 000 open fell to 2 000/s and 145 MB. Since varpulis #289 it
keeps their deadlines in order and visits only the partitions with one due.
Measured against the v0.3.4 runtime in the same session, same box:

| Open at once | v0.3.4 | now |
|---:|---:|---:|
| 1 | 28 400 – 28 500/s | 28 600/s |
| 100 | 27 100 – 27 200/s | 28 200 – 28 600/s |
| 1 000 | 19 500 – 19 600/s | 25 900 – 26 200/s |
| 10 000 | 11 900/s | 21 600 – 22 600/s |
| 30 000 | 2 075 – 2 085/s (145 MB) | 14 200 – 14 900/s (88 – 109 MB) |
| 1 000, unpartitioned | 4 280 – 4 290/s | 4 250 – 4 260/s |

Correlations spread across units the way flows do — 1 000 open in each:

| Units | Aggregate | Per unit | RSS |
|---|---:|---:|---:|
| 1 | 27 100 – 27 200/s | 27 100/s | 16.4 MB |
| 2 | 37 200 – 39 400/s | 18 600 – 19 700/s | 21.1 – 21.5 MB |
| 4 | 48 300 – 48 900/s | 12 100 – 12 200/s | 28.4 – 29.1 MB |

Four correlation units go past the stateless ceiling above because they emit
one alert per pair, where the stateless rule emits one per event: publishing
is part of what saturates a process.

### Sizing, then

Open runs are what you size for, and you can compute them before deploying:

```
open at once  =  first steps per second  x  how long a first step waits
```

A rule whose first step fires 200 times a second and whose second step
typically follows within ten seconds carries 2 000 open runs. From the table,
one unit handles that at around 25 000 events/s in 18 MB.

- **A stateless rule:** budget 30 000 events/s and 10 MB per unit.
- **A correlation under 100 open:** 28 000 events/s, 13 MB.
- **At 1 000 open:** 26 000 events/s, 16 MB. **At 10 000:** 22 000 events/s,
  33 MB. **At 100 000:** 11 000 events/s, 450 MB: plan on memory, about 4 KB
  an open run.
- **Always partition a correlation** — about six times the throughput.
- **One instance saturates near 44 000 events/s** whatever the unit count.
  Beyond it, add instances: they share the durable consumers and the work
  (see *Clustering*).
- **Artefacts:** the binary is 10.7 MiB, the image 37.6 MiB, and a unit that is
  idle costs nothing measurable.

Measured on the dev box in this file's header — 8 cores, WSL2 — two runs of
everything, serially, on a quiet machine. Timing-sensitive figures move by a
fifth under load; if your numbers disagree, check `uptime` first.

## Clustering (ADR-0020, measured)

The table below is v0.5.1 on `async-nats`, re-measured on 2026-11-07. Leave a few
seconds between runs: each script frees its port on exit, and a run chained straight
after another printed no result.

Two instances, one NATS, `bench/cluster.sh` + `bench/cluster-gaps.sh` — and
the same probe at **three** (`bench/cluster.sh 3 20000`): 20 000/20 000
delivered, the singleton timer ticked 8 times in 8 seconds rather than 24,
and after a `kill -9` at 1.5 s the two survivors shared the rest between
them (8 243 and 7 611).

| Invariant | Result |
|---|---|
| Flows under kill -9 (1.5s into load) | **20 000/20 000 exactly-all**; what the dead instance held comes back after the ack wait, 31.5 s in all with the default 30 s, on v0.3.6 and v0.5.1 alike |
| Singleton duplication (timer, 2 instances) | **eliminated** — 8 ticks/8 s (was 16 pre-lease) |
| Graceful handoff (SIGTERM leader) | **at most 2 s** ≈ tick interval + standby retry (four runs: three under 1.5 s, one at 2.0 s) |
| Crash failover (kill -9 leader, TTL 3 s) | **4–5 s** ≈ TTL + retry + tick (four runs: 5.0, 5.0, 4.0, 4.0 s) |
| Split-brain guard | clustered instance answers 409 on local-file mutation, file untouched |
| Live promote across the cluster (ADR-0021) | **20 ms convergence**, first new-version emit at 20–30 ms, zero interleave, 40 000/40 000 delivered mid-burst |

## Not measured yet

Comparative runs beyond Redpanda Connect (n8n, Windmill done/in table) —
Windmill pending. Detect units against an incumbent correlation engine
(Esper, Siddhi, Flink CEP) on one scenario. What happens past 30 000 open
correlations, and why the cliff is there.
