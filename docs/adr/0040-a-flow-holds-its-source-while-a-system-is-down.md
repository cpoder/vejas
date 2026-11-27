# 0040 — A flow holds its source while a system it calls is down

- Status: Accepted (partial). Point 6: the founder chose hold as the default,
  2026-11-27. Built: `WHEN_DOWN`, `HEALTH_URL` and `PROBE_MAX_MS` on a
  declared service; the runs in hand with the in-progress ack; the hold with
  doubling probes; the held message first on resume; release and probe-now
  (`POST /hold/release`, `POST /hold/probe`, write-gated, human acts in
  governed mode, audited); one message at a time on the consumer of a flow
  that holds; the state in `/topology` and `vejas_topology`; `held`,
  `resumed` and `released` on `vx.flow.events`; the trace record and the
  metrics; e2e/flow-holds.sh. Not built: the panel's badge and buttons, MCP
  tools for the two acts.
- Date: 2026-11-27

## Context

A flow that calls a system takes its messages one by one (ADR-0032). When a
call fails in a way a later try may change (429, a 5xx, a timeout, the
network), the run fails and the message comes back after the service's
`DELAY_MS`, up to `ATTEMPTS` deliveries, five at most. Then it is a dead
letter, or its `catch` runs (ADR-0039).

That is right for one bad message. It is wrong when the system itself is
down. A private bench of integration scenarios put a payment system down for
90 seconds while three payments came in. It measured three faults:

- **Every message behind the first one is called during the outage.** The
  first message waits for its next delivery, so the flow takes the next one,
  which fails too. Each of the three was called three times, and each spent
  its attempts.
- **Nothing resumes.** After their attempts the three were dead letters. When
  the system came back, nothing ran until a person replayed them. The replay
  then ran them in reverse, a defect fixed apart (#66).
- **Order is lost.** The second payment passed before the first while the
  first waited. With two instances, two messages run at once even when the
  system is up.

Integration servers usually answer this by suspending the consumer on such a
failure and probing the resource until it answers. Their common limits: one
probe interval for the whole server, no backoff, and a suspended consumer
that looks like one a person disabled.

## Decision

### 1. A service says what its system's outage does

A declared service (`driver "http-rpc"`, ADR-0032) gets one setting:

- `WHEN_DOWN = "hold"`: a flow that reads a source holds it while this
  system is down (point 2).
- `WHEN_DOWN = "dead-letter"`: today's behaviour. The message comes back
  after `DELAY_MS` until `ATTEMPTS`, then it is a dead letter or its catch
  runs.

Two optional settings shape the hold:

- `HEALTH_URL = "{BASE_URL}/health"`: a GET that answers 2xx when the system
  is up. Without it, the probe is the held message's run itself.
- `PROBE_MAX_MS` (60000 by default): the longest wait between two probes.

### 2. Holding

When a run fails on a call to a holding service, with a failure a later try
may change:

1. The message stays in hand. It is not sent back and it is not a dead
   letter. The flow takes no other message. While it waits, the runtime
   tells the bus the message is still in progress, often enough that its
   `ack_wait` never runs out.
2. The run is tried again in hand, `DELAY_MS` apart, up to `ATTEMPTS` runs.
   Nothing is announced yet: a blip costs a delay and nothing else.
3. After that the flow is **held**. It says so (point 4) and probes, first
   after `DELAY_MS`, then twice as long each time, up to `PROBE_MAX_MS`.
4. When the probe answers, the held message runs, and the flow settles it as
   any other: emits confirmed, then acked. It says it resumed, and takes the
   next message.
5. A failure no later try may change ends the hold for that message: a 4xx
   outside `ANSWERS`, an error in the code. Its catch runs or it becomes a
   dead letter, as today, and the flow goes on.

Probes are not deliveries: neither the five-delivery cap nor a catch's last
delivery fires during a hold. As a delivery does today, a probe that reruns
the run calls again what the run called before the failing call;
`HEALTH_URL` avoids that.

A stop during a hold gives the message back to the bus at once, without a
delay, so it is the first one the next holder takes. A crash gives it back
after `ack_wait`.

### 3. Order across instances

A flow that calls a holding service takes one message at a time across all
instances: its durable consumer allows one message waiting for its ack
(`max_ack_pending = 1`). While one instance holds a message, the others wait.
Without this, two instances would each hold a message during an outage, and
the first to probe after it would run first.

### 4. What is seen

A hold is a state that people and agents see, never a silent wait:

- **The flow's state.** It carries the service, since when, the last error,
  the number of probes, the next probe and the number of messages behind.
  `GET /topology` and `vejas_topology` give it. The panel says it on the
  flow: held, which system, since when, when it tries next.
- **Events.** `held`, `resumed` and `released` transitions are published on
  `vx.flow.events`, an ordinary subject like `vx.dlq.events`. A flow or a
  sink can page someone.
- **The trace.** The held message's hop keeps its tries, each probe and its
  outcome. The run that finally passes continues the same trace, and the
  trace record says how long the message was held.
- **Metrics.** A gauge per held flow and service, and a count of probes.

### 5. What a person can do

On a held flow a person can:

- **release** the message: its catch runs, or it becomes a dead letter, now.
  The flow goes on, and holds again if the next message fails the same way;
- **probe now**.

Both are recorded in `VEJAS_AUDIT` with the actor. In governed mode
(ADR-0024) they are human acts, like a DLQ replay.

### 6. The default: hold (decided 2026-11-27)

Two possible defaults for a declared service that does not say:

- **`hold`**. A short outage no longer turns messages into dead letters that
  a person must replay. A dead letter then means "this message cannot pass",
  never "the system was down". The cost: a flow that calls this system for
  only some of its messages holds the others too, during the outage. Such a
  service says `WHEN_DOWN = "dead-letter"`.
- **`dead-letter`**, which keeps today's behaviour, and `hold` is opted in.

The recommendation was `hold`, and the founder chose hold as the default,
2026-11-27. A service that wants today's path says `WHEN_DOWN = "dead-letter"`.

### What does not change

- An `api` flow answers at once: 503 for a failure a later try may change, or
  its catch runs. A request cannot be held.
- A shown run (preview, replay, canary) makes no call.
- A failure no later try may change goes to the catch or the DLQ at once.
- A `secret()` or a `seen()` whose store does not answer keeps the
  delivery's pace (ADR-0039).

## Consequences

- An outage no longer fills the DLQ, and the flow resumes alone, in order.
  The DLQ keeps the messages that cannot pass.
- A held flow blocks every message behind it, including those that would not
  have called the system. That is the price of order.
- A flow that calls a holding service runs one message at a time across
  instances. Order per key (partitions) would lift that; it is not decided.
- A hold that lasts shows where people look: the panel, `vx.flow.events`,
  the metrics.
- To build: an in-progress answer on a message, `max_ack_pending` on a
  durable, the three settings, the hold in the flow's loop, the state and
  its events, the panel's badge, and the two actions. An end-to-end suite
  checks it: a system down for a few seconds, three messages, a single call
  during the outage, no dead letter, all three in order after, the
  transitions announced, a stop during a hold, and two instances in order.

## Alternatives considered

- **Send the message back with a delay, or pause the consumer on the
  server.** The message leaves the flow's hand, so the next one is delivered
  while it waits: order is lost. That is the behaviour the bench measured.
- **One probe interval for the whole runtime.** Each system has its own pace.
  The setting lives on the service.
- **A wait in the language.** A run that sleeps holds its message while the
  runtime cannot tell why: nothing could show it or release it. ADR-0001
  keeps the clock out of the language.
- **One state per service, shared by every flow that calls it.** It would
  probe less, at the price of a state shared across flows, with failure
  modes of its own. Each flow holds on its own message for now.
