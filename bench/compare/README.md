# The comparison leg

Same load generator, same counting sink, same mapping work (lookup with
fallback, lowercase, numeric conversions, a per-line projection). One
scenario, several engines. Run:

```
bench/run.sh 20 32                                   # Vejas
RPCONNECT=/path/to/redpanda-connect \
  bench/compare/run-benthos.sh 20 32                 # Redpanda Connect (ex-Benthos)
bench/compare/run-n8n.sh 20 32                       # n8n (official image, tuned)
```

## Same machine, same day (dev machine, 8 cores, WSL2)

| | Vejas v0 | Vejas, all five fixes | Redpanda Connect 4.106 | n8n 1.x (tuned) |
|---|---|---|---|---|
| Delivered, saturated | 65/s | **~2 650/s** | 3 433/s | 12–15/s |
| Delivered, paced sustained | — | **~1 900/s** (p50 14 ms, p99 36 ms) | — | — |
| e2e latency p50 | 859 ms | 14 ms sustained / **2 ms** uncongested | 1 ms | 2 350 ms |
| Flow-hop rate (no HTTP) | 171/s | **8 110/s** (9 948/s over 10 flows) | — | — |
| Cold start | 15 ms | **11–13 ms** | 391 ms | 7–17 s (container) |
| RSS under load | **6–8 MB** | **6–8 MB** (49 MB at 50 flows) | 202 MB | 1.2–1.3 GB |
| Distribution size | 3.9 MB binary | **6.2 MB binary / 201 MB image** | 338 MB binary | 372 MB image |
| Persistence | every hop (JetStream) | **every hop (JetStream)** | in-flight only | per-execution DB (disabled for this run) |

**n8n fairness notes.** Single instance from the official image, tuned per
their production docs (execution persistence off — the untuned default did
7/s). n8n's scaling answer is queue mode with a worker fleet plus Postgres
and Redis; this table is single-node, one process per engine, which is the
deployment Vejas targets with `docker compose up`. n8n's webhook responds
before executing (like our 202-then-bus), so its latency is queue wait under
32-connection pressure — at low rate its per-execution latency is ~150-400 ms.
Cold start measured through its container, as officially distributed.

**Read both columns honestly.** After the five fixes, Vejas delivers in the
same order of magnitude as the category (~2.7k vs 3.4k/s saturated, both
sink-bound) while **persisting every hop**, starting ~30× faster, holding
~25× less memory, in a ~70× smaller binary. The remaining latency gap
(2 ms vs 1 ms) is the price of the stronger guarantee — two persisted
JetStream hops sit in the path.

**The guarantee is not the same.** This Redpanda Connect pipeline holds
messages in flight only (input → processor → output, ack-chained): a crash
loses nothing acked, but nothing is persisted. Vejas persists **every hop**
in JetStream — durable consumers, publish-before-ack, replayable dead
letters. A parity variant (Redpanda Connect with a JetStream buffer between
stages) would be the stricter comparison; future work.

Note also Redpanda Connect's own sink ceiling here: it ingested 6 440/s but
delivered 3 433/s to the HTTP sink — at sustained load its output leg is the
limiter too. Latency stays flat because its buffer is bounded memory, not a
persisted queue.

## Target

After the loop rework (#4+#3) and the I/O fixes (#2+#1), the goal for this
table is a delivered rate in the same order of magnitude as the column on
the right, while keeping the left column's footprint and the stronger
guarantee. That claim gets made **only** by re-running these two commands.
