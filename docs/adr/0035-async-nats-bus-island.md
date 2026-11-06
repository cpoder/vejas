# 0035 — The NATS client moves to async-nats, behind one synchronous module

- Status: Proposed
- Date: 2026-11-06

## Context

The runtime is synchronous and thread-based, and it stays that way: a flow, a
connector and a supervisor are OS threads, and no async runtime was in the tree.
That was also true of the NATS client, `nats` 0.25, which is the problem. The
crate is deprecated. `nats` 0.26 does not help: it keeps rustls 0.22.4 and the
same advisories, and seven entries of `.cargo/audit.toml` existed only for it.

It also cost more than its size. The client runs two threads per connection, and
the runtime opened a connection per use, 25 sites in all. Measured on v0.3.6: 11
threads idle, then 5 threads and 2 connections per flow. Two hundred flows were
1 011 threads, 401 connections and 140 MB.

And a hard-coded 5 ms floor in its flusher thread shaped the hot path. Findings
#5 and #6 of `bench/README.md` come from it, and three workarounds followed:
`publish_confirmed`, `fetch_iter_flushed`, and a second connection per flow for
the acks (#7).

## Decision

One new module, `core/src/bus.rs`, hosts `async-nats` on one tokio runtime and
exposes a synchronous API. Nothing else in the runtime imports tokio or a NATS
type. Flows, the interpreter, connectors and supervisors do not change shape.
`Engine::live` and `Engine::shadow` are untouched and the interpreter has no bus
access, so `Program::writes_bus()` means what it meant.

This is not an async rewrite. The rewrite was never on the table: threads are
what make a flow simple to read, and the cost above was the client's, not the
threads'.

The eight points that make the module.

### 1. One connection per process

`bus::connect` returns the one `Bus` of a URL, whoever asks. Pull rounds,
publishes, requests, subscriptions and acks all share it.

Whether data and acks need a connection each was measured, because finding #7
said they did. They do not, at any flow count, and that decides it:

| Events/s, drained with `bench/pub.py` | v0.3.6 | 2 conns, 2 workers | 1 conn, 2 workers | 2 conns, 1 worker | 1 conn, 1 worker |
|---|---:|---:|---:|---:|---:|
| 1 flow | 20 100 – 20 500 | 19 500 – 19 800 | 20 700 | 20 000 – 20 400 | 19 400 – 21 400 |
| 10 flows | 35 000 – 35 200 | 36 500 – 37 600 | 37 900 – 38 400 | 36 600 | 35 200 – 36 000 |
| 50 flows | 22 100 | 26 900 | 27 000 | 26 900 | 27 400 |
| 1 detect unit | 33 200 – 35 400 | 37 700 – 40 500 | 35 500 – 37 300 | not run | 40 700 – 41 100 |

### 2. One worker

One connection is one task, and the runtime runs nothing that blocks. In the table
above a second worker is about 5 % ahead at ten flows, about 10 % behind on a detect
unit and level elsewhere, for one more thread. The simpler one stays.

`wait` parks the calling OS thread on a future. It is never called from inside
the runtime: tokio panics rather than deadlocks if it ever is, and a test pins
that. The event callback and the echo task are the only code of ours that runs
there.

### 3. The barrier is rebuilt, because `flush` does not mean what it meant

The at-least-once rule of a batch is: emits leave, the server confirms it has read
them, then the source messages are acked. The sync client's `flush` was a PING and
a PONG. `async_nats::Client::flush` only writes the socket. Using it would have
compiled, passed every test and acked messages the server never saw.

There is no public PING, so the barrier publishes to a subject only this
connection subscribes to and waits to hear it back. A connection's messages are
handled in order, so the echo cannot overtake them. Two alternatives were
rejected. A request to a subject nobody listens to, relying on the 503, stalls
every flow as soon as someone runs `nats sub '>'`. Draining a subscription does
write a PING, but the client does not wait for the PONG.

### 4. A reconnect fails the barrier

On a reconnect `async-nats` replaces the connection and its write buffer with it:
what was not yet written is dropped, in silence, and what comes next leaves on the
new connection. A round-trip alone would then confirm a batch that lost its first
half. `Statistics::connects` moves on every connection, before any later command
is handled, so the barrier reads it before the batch leaves and again after the
echo. If it moved, the batch fails and redelivers, whether or not anything was
lost. The sync client could complete a pending PONG on reconnect, so this is
stricter than what it replaces.

### 5. Emits are held per batch

A first version handed each emit to the connection as it was made. It ran the flow
hop at 12 600 evt/s against 20 300 for v0.3.6, and the round phases on `/metrics`
put all of it in `process`: 33 µs more per message, CPU tripled. Each emit cost a
cross-thread wake and a write of its own. The 5 ms floor had been coalescing those
all along, as a side effect. So `Batch` holds a round's emits and hands them over
with the barrier: one wake and one write per round, and throughput came back.

A batch is whatever has arrived, so a quiet flow's emit still leaves at once: the
server ends a round as soon as the consumer has caught up. Subject and size are
checked when an emit is held, so one bad emit fails its own message and not the
batch.

### 6. Pull rounds keep their wire format

`async-nats`'s pull streams manage their own requests and thresholds, which is not
what "one request in flight ahead of the round being processed" asks for. So
`Puller` owns an inbox on the shared connection and sends the same request as
before:
`{"batch":64,"expires":500000000,"no_wait":true}`. `Round` is the same iterator,
with one change: a 503, or a 409 saying the consumer was deleted, now fails the
next round so the supervisor recreates it. The old iterator handed those statuses
to the flow as empty messages. Deleting a consumer under a running flow on v0.3.6
put two dead letters from an `_INBOX` subject in the queue and left the flow
`running`, consuming nothing, for good. Here it restarts once and carries on.

While the connection is down no request is sent. The round times out empty and the
loop keeps its pace and its stop flag, then picks up where it was.

### 7. Administration keeps its wire format too

Streams, buckets and consumers are created through raw JetStream API requests,
with the bodies the sync client sent. The typed configs of `async-nats` carry
defaults of their own: a bucket would have come out with `allow_direct` on. KV and
object stores then open through `async-nats` on top of those streams.

The result was checked three ways. `nats stream info` and `nats consumer info` on
a fresh store are identical to v0.3.6 for all eight streams and every durable; only
the ordered consumers behind a KV watch differ, and they hold no state. An upgrade
test ran v0.3.6 against a real store, then this build on it, then v0.3.6 again:
durable positions, the version overlay, a pending proposal, a dead letter, offsets
and the lease survive both ways, and a sequence opened under one binary closed
under the other, from the other client's detect snapshot.

### 8. Two things the client does not do

`async-nats` parses `nats://user:pass@host` and `nats://token@host` and connects
without them, where the sync client used them. `bus.rs` hands them over. No test
in the repo connects with credentials, so this would have failed in production.

And `connect` refuses while the connection is down, as `nats::connect` did: a unit
that starts during an outage backs off and comes back, and an HTTP route says so.
Threads already running stay in place and resume on reconnect.

## Measured

v0.3.6 and this build alternating, one session on the 8-core WSL2 box of
`bench/README.md`, each run started under a load of 6.

| | v0.3.6 | this build |
|---|---:|---:|
| Threads at 0 / 1 / 10 / 50 / 200 flows | 11 / 16 / 61 / 261 / 1 011 | 10 / 11 / 20 / 60 / 210 |
| NATS connections, same | 1 / 3 / 21 / 101 / 401 | 1 / 1 / 1 / 1 / 1 |
| Idle RSS, same, MB | 7.1 / 8.9 / 16.7 / 46.9 / 139.8 | 9.1 / 9.6 / 10.5 / 14.2 / 26.4 |
| Flow hop, `bench/flow-only.sh`, 16 publishers | 13 100 – 13 700/s | 13 200 – 13 500/s |
| Flow hop, drained with `bench/pub.py` | 20 100 – 20 500/s | 19 400 – 21 400/s |
| RSS under load, one flow | 9.2 MB | 10.1 – 10.3 MB |
| `bench/multi-flow.sh`, 10 flows | 12 300 – 13 000/s, 18.6 MB | 13 700 – 14 700/s, 14.5 MB |
| `bench/multi-flow.sh`, 50 flows | 11 300/s, 58 MB | 11 100 – 12 100/s, 38 MB |
| One detect unit, `bench/detect.sh` | 30 800 – 35 400/s, 11 MB | 34 700 – 41 100/s, 12.7 MB |
| Paced 2 000/s, p50 / p99 | 3–4 / 14–17 ms | 3 / 14–15 ms |
| Paced 20/s, p50 / p99 | 2 / 3–4 ms | 2 / 3 ms |
| `bench/run.sh 15 32`, ingest / delivered per s | 4 930 – 4 960 / 2 850 – 2 890 | 4 640 – 5 110 / 2 570 – 2 910 |
| MQTT loopback, `bench/broker-mqtt.sh 5000` | 2 370 – 2 430 rt/s | 2 050 – 2 090 rt/s |
| Spawn to `/healthz` 200 | 4.0 ms | 3.9 ms |
| Spawn to a flow running | 1 714 ms | 10.9 ms |
| Binary | 9.90 MB | 11.24 MB |

`bench/flow-only.sh` is bound by its `nats` publishers on both sides, which is why
the second line is there. `bench/cluster.sh` delivers 20 000/20 000 through a
`kill -9` with the timer staying single, `bench/cluster-promote.sh` converges in
10 ms with 40 000/40 000 and no interleave, and all the e2e suites pass, the
connector admission job included.

The MQTT loopback is the one number that fell, by 14 %. Both its legs work one
message at a time, a pub-ack on `mqtt-in` and an ack on `mqtt-out`, and each now
crosses to the worker and back where the sync client wrote from the calling thread.
That is the likely cost, not an isolated one. A batch has no such hop to lose, so
the flow hop did not feel it.

## Connectors

`connectors/mq` and `connectors/amqp` hold their own NATS client (ADR-0023,
ADR-0026) and move too, by compiling the same file: `#[path]` on
`core/src/bus.rs`, which depends on nothing else in core. Their `publish_confirmed`
and `fetch_round` go away. `connectors/amqp` also moves from rustls 0.22 to 0.23,
with no source change to its TLS adapter. Both are sync programs; "no tokio" is no
longer true of them, in the same contained sense as the runtime.

## Consequences

- `.cargo/audit.toml` loses seven entries, and every lockfile is on rustls 0.23.46
  and rustls-webpki 0.103.16. `core/Cargo.lock` goes from 251 packages to 201.
- An emit subject with whitespace or a line break is refused. The sync client
  wrote it into the `PUB` line, so event data could inject protocol: reproduced on
  the v0.3.6 binary, see the changelog. The refused message stays un-acked and
  redelivers. It does not reach the dead-letter queue, because a failed emit was
  always treated as transient; a payload over the server's limit behaves the same.
- A runtime whose NATS user has a publish allow-list needs `_INBOX.>` on it, for
  the barrier. It already needed to subscribe there, and to answer `exec-rpc` and
  control requests. Without it the emits leave, no batch is ever confirmed, nothing
  is acked, and everything redelivers; the log names the permissions violation. No
  deployment in this repo sets such a list, so this is an upgrade note.
- tokio is in the tree. It stays inside one file, and the runtime has one more
  thing it depends on behaving. What `bus.rs` rebuilt (points 3, 4 and 8) is what a
  client upgrade could break. The `bus::live` tests hold it against a real server;
  like every test that needs one they are ignored in CI, so run them on an upgrade:
  `cargo test bus::live -- --ignored`.
- One connection is one fate: when it drops, every flow waits at once, and a server
  that closes it as a slow consumer stops the whole process. A pull bounds what a
  flow can have in flight to 128 messages, which is what keeps that far away.
- The binary grows by 1.3 MB and RSS at one flow by about 1 MB. Past a few flows
  the runtime is smaller than it was.

## Not measured

TLS performance (`tls://` was checked for behaviour: a private CA in the trust
store, an upgrade from `nats://`, an unknown CA refused). Failover between the
servers of a NATS cluster, or more than one server in `NATS_URL`: the runs here
restart one server in place.
