# Benchmarks

Every number quoted in these docs is reproducible from
[`bench/`](https://github.com/cpoder/vejas/tree/master/bench) — scripts, not
claims. Machine of record: an 8-core dev machine under WSL2; run them on
yours.

## Current numbers

| Metric | Value | Reproduce with |
|---|---|---|
| Cold start (spawn → healthz) | 11–13 ms | `bench/run.sh` |
| Runtime RSS under load | 6–8 MB (49 MB with 50 live flows) | `bench/run.sh`, `bench/multi-flow.sh` |
| Binary / image | 6.2 MB / 201 MB | — |
| e2e latency, uncongested (webhook→flow→sink) | **p50 2 ms, p99 3 ms** | `bench/paced.sh 20 15` |
| e2e paced sustained ~1 900/s | p50 14 ms, p99 36 ms, 20 000/20 000 | `bench/paced.sh 2000 15` |
| e2e saturated (32 conns) | ~4 900/s ingest, ~2 650/s delivered (sink-bound) | `bench/run.sh 15 32` |
| Isolated flow hop | 8 110/s (9 948/s over 10 flows) | `bench/flow-only.sh`, `bench/multi-flow.sh` |
| MQTT loopback, QoS 1 both ways, real mosquitto | 2 285 rt/s, 5 000/5 000 | `bench/broker-mqtt.sh 5000` |
| Cluster: instance kill -9 under load | 20 000/20 000, zero loss | `bench/cluster.sh` |
| Cluster: singleton failover | ~2.6 s graceful / ~5.9 s crash (TTL-bound) | `bench/cluster-gaps.sh` |
| Cluster-wide version promote | 60 ms convergence, lossless mid-burst | `bench/cluster-promote.sh` |

Every hop persisted in JetStream throughout — the guarantee never moved
while these numbers were earned. Methodology, the five ceilings that fell
(and their causes), and the honest comparison table against Redpanda
Connect and n8n:
[`bench/README.md`](https://github.com/cpoder/vejas/blob/master/bench/README.md)
and [`bench/compare/`](https://github.com/cpoder/vejas/tree/master/bench/compare).
