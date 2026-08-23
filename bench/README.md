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

| Metric | v0 | after `5cffb7b` | with parallel publishers (`PUBS=4`) |
|---|---|---|---|
| Flow-hop rate | 171/s | ≥ 2 786/s (publisher-bound) | **8 110/s** |
| Runtime RSS | 5.6 MB | 5.5 MB | 6.2 MB |

Finding **#4 (the structural one — fixed)**: ~5.8 ms per message was the
per-message synchronous JetStream round-trips in the consumer loop. The
rework buffers emits and does **one flush per batch before acking that
batch** (publish-before-ack preserved, at-least-once intact — 5 000-burst
loss test: 0 lost, DLQ clean), with `no_wait` pulls killing the batch-fill
wait (#3) at the same time. 16× on the isolated hop; the true ceiling needs
a faster publisher to measure.

## The true hop ceiling and multi-flow scaling

With parallel publishers (`PUBS=4/8`), the single-flow hop tops out around
**7–8 k/s** (the publisher pushes 9.7 k/s, the flow drains just behind).
Scaling the number of flows (`bench/multi-flow.sh`):

| Flows | Aggregate rate | Runtime RSS |
|---|---|---|
| 1 | 7–8 k/s | 6 MB |
| 10 | **9 948/s** | 12.9 MB |
| 50 | 8 410/s | 49.3 MB |

Throughput *rises* with flow count (consumers parallelize; the bus, not the
interpreter, is the bound) and memory stays ~1 MB per running flow — fifty
live, persisted flows in under 50 MB.

## Clustering (ADR-0020, measured)

Two instances, one NATS, `bench/cluster.sh` + `bench/cluster-gaps.sh`:

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
Windmill pending. Cluster scaling beyond 2 instances.
