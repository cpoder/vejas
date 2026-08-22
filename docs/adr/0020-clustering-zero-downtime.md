# 0020 — Clustering and zero-downtime, the NATS-native way

- Status: Accepted
- Date: 2026-08-23

## Validated on the bench (before the build)

`bench/cluster.sh` measured the taxonomy directly, and it holds:

- **Competing-safe is free and lossless.** Two instances on the same durables,
  `kill -9` one at 1.5s under load → **20000/20000 processed, zero duplicates,
  zero loss**; the survivor takes over the whole stream.
- **The flow unavailability window is `ack_wait`, not the crash.** In-flight
  un-acked messages of a killed instance only redeliver after `ack_wait`: the
  30s default drains in ~31.5s, `VEJAS_ACK_WAIT_SECS=1` drains the same scenario
  in ~2.1–2.5s at 8–9.6k/s aggregated. This is why the deployment note below
  recommends a low ack-wait in a cluster.
- **Singleton duplication is real and measured.** Two instances running a 1s
  timer produced ~16 ticks in 8s vs the ~8 expected — the concrete case the KV
  lease closes.

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

One unit in this regime needs a **one-line** change: the RPC responder
(`exec-rpc`, `rpc:exec`) today subscribes with a plain core subscription
(`nc.subscribe`), so N instances would each answer every request — duplicate
replies. It must join a NATS **queue group** (`nc.queue_subscribe(subject,
"vejas")`) so exactly one instance answers each request, load-shared across the
group. That is competing-safe like the durables, not a singleton — no lease, each
instance keeps its own child process (e.g. its own SAP logon).

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
- **Per-instance local state — but not the replay.** The trace ring (`/events`)
  and the metrics counters live in each instance's memory. Prometheus already
  scrapes per-target, so `/metrics` aggregates correctly across instances. Only
  the panel's `/events` *view* is local — it shows the instance that served the
  page. Crucially, **shadow-replay is not affected**: it hydrates from JetStream
  (ADR-0018), which is cluster-global, so previewing a change replays the real
  cluster-wide traffic regardless of which instance answers. Aggregating the ring
  view (or a sticky panel) is a follow-up, called out rather than hidden.
- **Live promotes vs. the cluster — fail loud, never split-brain.** The
  business-surface live edit (`/surface/set`, ADR-0005/0018) writes *one*
  instance's local file; the others would not see it. Silently mutating one
  instance is the **worst** failure mode for the whole thesis: the expert
  believes they corrected the meaning while half the traffic still applies the
  old rule. So in cluster mode — declared explicitly by `VEJAS_CLUSTER=1` — every
  endpoint that mutates a **local file** REFUSES with a didactic error
  (`clustered: promote via git — see ADR-0020`): `/surface/set`, the
  `vejas_set_literal` MCP tool, `/file/set`, `/fixture/set`, provisioning, and
  `/secrets/set` when it is backed by the local `FileStore`. What stays allowed:
  `/reload` (per-instance, and *needed* after a GitOps pull), DLQ replay
  (bus-side, cluster-safe), and secret writes to a **shared** Vault backend.
  Promotes in a cluster flow through **GitOps** (the repo is the shared source of
  truth — already the git half of the ADR-0018 audit trail); live panel promotes
  remain a single-instance / development affordance.

  This GitOps-only state is an **intermediate**, not the destination: an
  enterprise panel that cannot promote live loses its reason to exist. The
  **versioning ADR (next) MUST solve cluster-wide live promotion** — a promote
  that fans out to every instance atomically (the `VEJAS_AUDIT` stream is the
  likely seam, and versioning needs the same fan-out for a candidate rollout).
  It is deferred there deliberately, because doing it here without the version
  machinery would bolt on a half-answer.
- **Duplicates are bounded, not impossible.** During a crash failover a singleton
  can double-publish for at most the TTL window. The pollers were already
  designed for this — `fetched_at` and the idempotency keys derived from it
  (oauth-poll, http-poll ENVELOPE) make a replayed fact byte-identical, so a
  downstream idempotent sink absorbs the duplicate. At-least-once was always the
  contract; clustering does not weaken it.
- No new dependency: the lease is JetStream KV, already in the client we link.
- **LB-safe means between machines.** Each instance binds its own `http-in` /
  `/api` / panel ports, which is conflict-free across nodes. Two instances on the
  *same* host collide on the port — a clustered same-host deploy must give each
  instance its own `VEJAS_HTTP_ADDR` / connector `PORT` (env override). Real
  clusters run one instance per node/pod and never hit this.

## Deployment notes

- **`VEJAS_ACK_WAIT_SECS=3–5` in a cluster.** The bench shows the drain window
  after a lost instance is the ack-wait, so a low value makes failover fast. The
  **30s default stays** for single-instance runs (safe for slow sinks that need
  the time) — a stated tradeoff, tuned per deployment, not changed globally.
- **Lease TTL ~10s** matches the real pollers (`INTERVAL_SECS ≥ 60`), so a normal
  failover loses at most one poll cycle's slack. The two handoff windows, both
  measured on the bench (TTL=3s, a 1s timer):
  - **graceful (SIGTERM / rolling restart) ≈ interval + retry** — the leaving
    leader releases the lease, a stand-by's next `create` (retry 1s) picks it up,
    plus the in-flight tick. Measured **2.6s**; there is *no* TTL term, the lease
    is deleted not aged out.
  - **crash (`kill -9`) ≈ TTL + retry + interval** — no release, so the value
    must age out (TTL) before a stand-by acquires. Measured **5.9s** worst case.
  The crash window is TTL-dominated, so the stand-by retry stays at 1s (no knob:
  lowering it would not move the crash number and would only add config surface
  for a sub-second handoff nobody has asked for; a real need would ship with its
  use case). For a fast timer the crash gap is the TTL — bounded and documented,
  so nobody is surprised by a ~10s pause of a 1s timer after a hard kill.

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
