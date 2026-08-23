# Cluster & zero-downtime

Run N instances of the runtime against the same bus. There is no
coordinator to install and no quorum to configure — **the bus is the
coordination** (ADR-0020). Turn it on with one variable:

```
VEJAS_CLUSTER=1
```

## What scales, and what stays single

| Unit | In a cluster | Why |
|---|---|---|
| Flows, sinks | **All N run** — a shared JetStream durable load-balances | Competing consumers: each message goes to exactly one instance |
| `http-in` | **All N run** behind your load balancer | Each has its own listener; ingestion is stateless |
| Singleton sources (`interval`, `poll`, `exec`, `stream`, `mqtt`) | **Exactly one runs** | N timers/getters would produce N× the events |

A singleton source takes a **lease** in a JetStream KV bucket
(`VEJAS_LEASES`) before it runs:

- **acquire** = atomic create-if-absent — exactly one instance wins.
- **renew** = compare-and-set — a paused leader that wakes with a stale
  revision stands down (fencing: two instances never *keep* running the
  same unit).
- **release** = delete on graceful shutdown — instant handoff.
- **failover** = the bucket's TTL (`VEJAS_LEASE_TTL_SECS`, default 10 s)
  ages a crashed leader's lease out; a stand-by acquires.

## Rolling a deploy with no loss

Because every publish is confirmed by JetStream **before** the source acks
its input, a killed instance loses nothing — the message redelivers to a
survivor.

- Instance `kill -9` under load: **20 000/20 000, zero loss**.
- Singleton failover: **~2.6 s graceful**, ~5.9 s crash (TTL-bound).

([benchmarks](../reference/benchmarks.md).) Graceful shutdown rides
`SIGTERM` (a k8s rolling restart): the lease hands off, in-flight work
drains, the instance exits.

## Changing meaning across the cluster

In cluster mode a local file write is **refused** (`409`) — a split where
one instance fixed a rule and the others did not is the worst failure for a
business surface. Change flows through GitOps, or through a **version** that
publishes cluster-wide and every instance converges on in **60 ms,
lossless** ([change safely](change-safely.md)).

## Scaling past one getter

A singleton source is single by *correctness*, not capacity. When one
getter is a bottleneck, **partition**: one manifest per key range (e.g. one
Kafka consumer per partition set, each its own offset key). Ordered by
default; throughput is a choice you make on purpose.
