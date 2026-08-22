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

| Metric | Value |
|---|---|
| Cold start | **13–15 ms** |
| Runtime RSS under load | **6–8 MB** |
| Binary size | **3.9 MB** |
| Ingest rate (32 conns) | 321/s |
| Delivered rate | 65/s |
| End-to-end p50 (uncongested) | 859 ms |

The footprint numbers are the thesis, measured. The throughput and latency
numbers are **known ceilings of the v0 I/O paths, not of the interpreter** —
each one matches its cause exactly:

1. **Ingest ≈ concurrency × 10/s** — `http-in` polls its non-blocking
   listener with a 100 ms sleep and closes after every request (no
   keep-alive). 32 × 10 = 320/s, measured 321-322/s. *Open.*
2. **Delivered ≈ 65-73/s** — `http-out` spawns one curl process per message
   (~15 ms each). *Open — now THE e2e ceiling since #4/#3 fell.*
3. **Batch-fill wait** — *fixed with #4* (`no_wait` pulls, immediate
   delivery, anti-zombie invariant preserved).

Numbers here are updated by re-running the harness after each fix — never
quoted without the scenario and the machine.

## The isolated flow hop (`bench/flow-only.sh`)

Publish straight onto the bus, run only the flow, count its emits with a
plain subscription — no HTTP anywhere:

| Metric | v0 | after the loop rework (`5cffb7b`) |
|---|---|---|
| Flow-hop rate | 171/s | **≥ 2 786/s** — now publisher-bound: the flow keeps pace with the sequential publisher, so this is a floor, not the ceiling |
| Runtime RSS | 5.6 MB | 5.5 MB |

Finding **#4 (the structural one — fixed)**: ~5.8 ms per message was the
per-message synchronous JetStream round-trips in the consumer loop. The
rework buffers emits and does **one flush per batch before acking that
batch** (publish-before-ack preserved, at-least-once intact — 5 000-burst
loss test: 0 lost, DLQ clean), with `no_wait` pulls killing the batch-fill
wait (#3) at the same time. 16× on the isolated hop; the true ceiling needs
a faster publisher to measure.

## Not measured yet

Multi-flow scaling, comparative runs (same scenario on n8n / Windmill /
Benthos) — after the ceilings above fall.
