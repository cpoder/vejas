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
| Runtime RSS under load | 6–8 MB | **6–8 MB** |
| Binary size | 3.9 MB | 6.2 MB (rustls) |
| e2e sustained, paced ~1 900/s (`bench/paced.sh 2000 15`) | — | **p50 14 ms, p99 36 ms**, 20 000/20 000 |
| e2e saturated (32 conns) | 65/s delivered | ~4 900/s ingest, **~2 650/s delivered** (sink-bound, queue absorbs the rest) |
| e2e latency, uncongested (`bench/paced.sh 20 15`) | p50 859 ms | **p50 2 ms, p99 3 ms** |
| MQTT broker loopback, QoS 1 both ways (`bench/broker-mqtt.sh 5000`) | — | **2 285 rt/s**, 5 000/5 000 |
| Isolated flow hop | 171/s | **8 110/s** |

The old "sustained (12 conns) 1 701/s, p50 18 ms" row died with ceiling #5:
that operating point was the flusher floor throttling each connection to
~140 req/s — with the floor gone, `bench/run.sh` saturates the pipeline at
any concurrency, so steady-state latency is now measured **paced**
(`bench/paced.sh`, fixed event rate below the sink bound).

Every hop persisted in JetStream throughout — the guarantee never moved.

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

## The true hop ceiling and multi-flow scaling

With parallel publishers, the single-flow hop tops out around **14–16 k/s**
(`PUBS=16`; four publishers push ~10.6 k/s and the flow drains as fast as
they publish). Scaling the number of flows (`bench/multi-flow.sh`):

| Flows | Aggregate rate | Runtime RSS |
|---|---|---|
| 1 | 7–8 k/s | 6 MB |
| 10 | **13 746/s** | 16.6 MB |
| 50 | 8 410/s | 49.3 MB |

Throughput *rises* with flow count (consumers parallelize; the bus, not the
interpreter, is the bound) and memory stays ~1 MB per running flow — fifty
live, persisted flows in under 50 MB.

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
- **Artefacts:** the binary is 8.2 MiB, the image 37.6 MiB, and a unit that is
  idle costs nothing measurable.

Measured on the dev box in this file's header — 8 cores, WSL2 — two runs of
everything, serially, on a quiet machine. Timing-sensitive figures move by a
fifth under load; if your numbers disagree, check `uptime` first.

## Clustering (ADR-0020, measured)

Two instances, one NATS, `bench/cluster.sh` + `bench/cluster-gaps.sh` — and
the same probe at **three** (`bench/cluster.sh 3 20000`): 20 000/20 000
delivered, the singleton timer ticked 8 times in 8 seconds rather than 24,
and after a `kill -9` at 1.5 s the two survivors shared the rest between
them (8 243 and 7 611).

| Invariant | Result |
|---|---|
| Flows under kill -9 (1.5s into load) | **20 000/20 000 exactly-all**, ~8 k/s aggregate |
| Singleton duplication (timer, 2 instances) | **eliminated** — 8 ticks/8 s (was 16 pre-lease) |
| Graceful handoff (SIGTERM leader) | **2.6 s** ≈ tick interval + 1 s standby retry |
| Crash failover (kill -9 leader, TTL 3 s) | **5.9 s** ≈ TTL + retry + tick (worst case) |
| Split-brain guard | clustered instance answers 409 on local-file mutation, file untouched |
| Live promote across the cluster (ADR-0021) | **60 ms convergence**, first new-version emit at 680 ms, zero interleave, 40 000/40 000 delivered mid-burst |

## Not measured yet

Comparative runs beyond Redpanda Connect (n8n, Windmill done/in table) —
Windmill pending. Detect units against an incumbent correlation engine
(Esper, Siddhi, Flink CEP) on one scenario. What happens past 30 000 open
correlations, and why the cliff is there.
