# 0020 — Clustering and zero-downtime, the NATS-native way

- Status: Proposed
- Date: 2026-08-23

## Context

The runtime is single-instance today: one process supervises every flow and
every connector. That is fine for a demo and for a small deployment, but "I can
run this in production" means *more than one* — for throughput, for rolling
deploys without a gap, for surviving a lost node. This is exactly the kind of
capability that is expensive to copy, so it is where we should invest.

The instinct from most stacks is a coordination service — a sticky leader, a
Zookeeper/etcd quorum, consistent-hash routing. Varpulis, for instance, runs
three sticky coordinators. We should not import that machinery, for the same
reason we run no Redis and no Postgres: **NATS is the only infrastructure
dependency (ADR-0002)**, and JetStream already *is* a replicated, consistent
log with a Key-Value store on top. The coordination we need is mostly already
done by the bus; the design is about naming the small part that isn't.

The key observation: **most of the runtime is already horizontally scalable and
we have not been using it.** A flow consumes through a *durable pull consumer*
keyed by the flow's name. JetStream delivers each message on a durable to exactly
one of its pull subscribers. So N runtime instances binding the *same* durable is
a competing-consumer group for free — no code, no router, no stickiness. The same
is true of every sink (also a durable pull consumer). What is *not* automatically
safe is a **singleton source**: a timer, a poller, an inbound registration. Run
that on all N instances and you get N× the polls, N× the publishes.

So clustering reduces to a taxonomy and one small mechanism.

## Decision (proposed)

Classify every supervised unit into one of three regimes, and add leader-election
only for the third.

### 1. Competing-safe — free horizontal scale (no new code)

Flows (`supervise_vjs`) and sinks (`run_sink`) are durable pull consumers. N
instances with the same `VEJAS_ROOT` bind the same durables and JetStream
load-balances messages across them. Throughput scales with instances; a lost
instance's in-flight (un-acked) messages redeliver to the survivors
(at-least-once, ADR-0002). This already works — the task is to *prove* it (the
multi-instance bench) and document it, not to build it.

### 2. LB-safe — run everywhere behind a load balancer

`http-in` (webhook), the synchronous `/api` path, and the panel/HTTP surface are
stateless per request. Every instance runs its own listener; an external load
balancer fronts them. `http-in` binds its own port on its own node, so there is
no bind conflict in a real (multi-node) cluster. No lease needed.

### 3. Singleton — exactly-one via a JetStream KV lease

A source whose trigger is **not** the bus must run on exactly one instance:
`timer` (`source:interval`), `http-poll` (`source:poll`), `oauth-poll`, the SAP
IDoc inbound registration (`idoc-server`), and `exec-source` /
`exec-stream-source`. These acquire a **lease** before running:

- A KV bucket `VEJAS_LEASES` (its own sibling root, like the DLQ and audit
  streams — ADR-0015/0018) with `max_age` = the lease TTL (e.g. 10s).
- **Acquire** with `Store::create(connector_name, instance_id)` — an atomic
  create-if-absent. Exactly one instance wins; it runs the connector. Losers
  stand by and retry.
- **Renew** with `Store::update(key, instance_id, revision)` — a compare-and-set
  on the revision, every T < TTL. Renewal keeps the value younger than `max_age`,
  so the key stays present while the leader lives.
- **Fencing** falls out of the CAS: if a paused leader (GC, slow disk) wakes and
  tries to renew with its stale revision, the `update` fails because a new leader
  already wrote a newer revision — the stale leader learns it lost and stops. Two
  instances never *keep* running the same singleton.
- **Failover on crash** (`kill -9`, no chance to release): the leader stops
  renewing, the value ages out after `max_age`, a stand-by's `create` succeeds,
  and it takes over. Bounded by the TTL.
- **Handoff on graceful shutdown** (rolling restart): the leaving leader
  `Store::delete`s its lease on the way out, so a stand-by acquires *immediately*
  — no TTL wait. This is what makes a rolling restart gap-free for singletons.

Each instance gets a unique `instance_id` at boot (hostname + pid, or a random
token) — `VEJAS_INSTANCE` overridable for tests.

### Zero-downtime rolling restart

Draining one instance at a time: its flows/sinks stop pulling (survivors already
hold the durables, so consumption never pauses), its singleton leases are
released and handed off, and any message it had un-acked redelivers elsewhere.
Bring the new version up, it rejoins the competing groups and stands by for
leases. No gap, no loss — the bus is the only shared state.

## Consequences

- Horizontal scale is real and nearly free; the only genuinely new code is the
  KV lease around singleton sources — a small, self-contained module.
- **Per-instance local state.** The trace ring (`/events`) and the metrics
  counters live in each instance's memory. Prometheus already scrapes per-target,
  so `/metrics` aggregates correctly across instances; the panel's `/events`
  ring, however, shows only the instance that served the page. Aggregating the
  ring (or a sticky panel) is a follow-up, called out rather than hidden.
- **Live promotes vs. the cluster.** The business-surface live edit
  (`/surface/set`, ADR-0005/0018) writes one instance's local file; other
  instances would not see it. In a clustered deployment, promotes must flow
  through **GitOps** (the repo is the shared source of truth) — which is already
  the intended production path, and already the git half of the ADR-0018 audit
  trail. Live panel promotes remain a single-instance / development affordance.
  Propagating a live promote cluster-wide (e.g. instances watching `VEJAS_AUDIT`)
  is possible but deferred; it also interacts with versioning (the next ADR), so
  it should be designed there, not bolted on here.
- **Duplicates are bounded, not impossible.** During a crash failover a singleton
  can double-publish for at most the TTL window. The pollers were already
  designed for this — `fetched_at` and the idempotency keys derived from it
  (oauth-poll, http-poll ENVELOPE) make a replayed fact byte-identical, so a
  downstream idempotent sink absorbs the duplicate. At-least-once was always the
  contract; clustering does not weaken it.
- No new dependency: the lease is JetStream KV, already in the client we link.

## Interactions

- **ADR-0013 (control plane over leaf nodes):** orthogonal. A controlled tenant
  may now be N instances; the control channel targets the tenant, and the
  singleton among the control handlers (if any) takes a lease like the rest.
- **ADR-0018 (audit trail / promote):** the git path is the cluster-safe promote
  channel; the `VEJAS_AUDIT` stream is the seam for future live-promote fan-out.
- **Versioning / canary (next ADR):** a candidate version is itself a
  consumer-group question (a second durable on a sampled subject, or a replay of
  a JetStream window through the candidate). It builds directly on this taxonomy,
  which is why this ADR lands first.

## Rejected

- **A sticky coordinator / external quorum (Zookeeper, etcd, consistent-hash
  routing).** It reintroduces an infrastructure dependency the platform exists to
  avoid, and the bus already provides the replicated log and the KV the design
  needs. Stickiness is the thing to delete, not to add.
- **Leader-election by a bespoke stream + expected-last-sequence CAS.** Workable,
  but it is re-implementing KV by hand; the client already ships KV built on
  exactly that primitive. Use it.
- **A shared external cache for the trace ring / promotes.** That is a Redis by
  another name. Metrics go to Prometheus per-instance; ring aggregation, if it is
  ever needed, rides the bus.
