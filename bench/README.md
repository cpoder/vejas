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
   keep-alive). 32 × 10 = 320/s, measured 321/s.
2. **Delivered ≈ 65/s** — `http-out` spawns one curl process per message
   (~15 ms each). The code's own comment plans a Rust HTTP client.
3. **p50 ≈ 860 ms at low rate** — the pull loop's batch-fill wait (~700 ms
   server expiry) before delivering an underfilled batch, paid twice (flow
   hop + sink hop).

These are the first three tickets the harness produced. Numbers here are
updated by re-running `bench/run.sh` after each fix — never quoted without
the scenario and the machine.

## Not measured yet

Interpreter-only throughput (masked by the sink ceiling), multi-flow scaling,
comparative runs (same scenario on n8n / Windmill / Benthos) — after the
ceilings above fall.
