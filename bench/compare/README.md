# The comparison leg

Same load generator, same counting sink, same mapping work (lookup with
fallback, lowercase, numeric conversions, a per-line projection). One
scenario, several engines. Run:

```
bench/run.sh 20 32                                   # Vejas
RPCONNECT=/path/to/redpanda-connect \
  bench/compare/run-benthos.sh 20 32                 # Redpanda Connect (ex-Benthos)
```

## Same machine, same day (dev machine, 8 cores, WSL2)

| | Vejas v0 | Vejas after #4+#3+#2 | Redpanda Connect 4.106 |
|---|---|---|---|
| Delivered rate | 65/s | 325/s *(ingest-bound: #1 open)* | 3 433/s |
| Flow-hop rate (no HTTP) | 171/s | **8 110/s** | — |
| e2e latency p50 | 859 ms | 109 ms *(the accept sleep)* | **1 ms** |
| Cold start | 15 ms | **11 ms** | 391 ms |
| RSS under load | **6–8 MB** | **6–8 MB** | 202 MB |
| Binary | 3.9 MB | **4.9 MB** | 338 MB |

**Read both columns honestly.** Vejas's throughput numbers are the four
diagnosed v0 ceilings (`bench/README.md`), not a property of the design —
the loop rework targets this table. The footprint numbers are structural:
26× faster to start, ~30× less memory, ~87× smaller.

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
