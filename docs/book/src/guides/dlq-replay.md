# Handle failures: DLQ & replay

The delivery contract ends in one of two places: the message is processed,
or it is **parked with a verdict** — never silently dropped (ADR-0015).

## How a message dies

A flow error → redelivery (after `VEJAS_ACK_WAIT_SECS`) → up to 5 attempts
→ the dead-letter queue. Unparseable input (bad JSON) is direct poison —
no pointless retries. The dead letter lands on `vxdlq.<original subject>`
wrapped in a **death envelope**: original payload, error, attempt count,
timestamps, and the **flow version** that killed it (ADR-0021) — so a
post-mortem knows *which* logic failed, even after a promote.

## Seeing and replaying

- Panel: the DLQ card; API: `GET /dlq`; agent: `vejas_dlq`.
- Replay is **explicit** — `POST /dlq/replay` (`vejas_dlq_replay`), never
  automatic: if the logic was wrong, replaying before fixing just kills the
  message again. Purge (`/dlq/purge`) is equally explicit. Both are recorded
  in `VEJAS_AUDIT` with the actor and the count.
- In [governed mode](governed-mode.md) both are human acts: `vejas_dlq_replay`
  and `vejas_dlq_purge` are refused, and the HTTP routes want the
  `X-Approval-Token` header, which the panel sends.

## Being told

Each dead letter is announced on `vx.dlq.events`, an ordinary subject, so what
happens next is a flow or a sink like any other:

```json
{"transition": "dead-lettered", "ts": 1793980000, "unit": "flow:orders",
 "subject": "vx.orders.in", "attempts": 5, "version": "d5768a2de6e8cbb0",
 "count": 1}
```

- **Metadata only.** The payload and the error text stay in the DLQ: a dead
  letter is untrusted content, and this event is what wakes a person or an
  agent. Read the letter with `vejas_dlq`.
- **One event per unit per window** (`VEJAS_DLQ_EVENT_SECS`, 60 s). The first
  letter is news at once; `count` says how many letters the next event stands
  for, so a burst wakes a reader once and loses nothing. Each instance of a
  cluster keeps its own window.
- **It cannot feed itself.** A reader of these events that keeps failing
  dead-letters them, and those letters are not announced.

[`dlq_webhook.vjs.example`](https://github.com/cpoder/vejas/blob/master/docs/examples/dlq_webhook.vjs.example)
posts the event to a URL: a chat channel, or a runner that starts your agent
with the agent's token alone. Do not start an agent from an `exec-*` connector:
the runtime's credentials are out of a child's environment, but it runs as the
runtime's user and reaches the same bus, far more than a proposal needs.

## The loop that makes it self-healing

1. A message dies; the envelope carries version `v3`, and `vx.dlq.events` says
   so.
2. An agent reads `vejas_dlq`, drafts a candidate flow, proves it: replay
   yesterday's real traffic through it
   ([time-travel](change-safely.md)), watch it shadow live traffic
   (canary).
3. In governed mode it *proposes* with that evidence; you approve — the
   promote fans out cluster-wide in 20 ms.
4. Replay from the panel, or `vejas_dlq_replay` outside governed mode — the
   dead messages pass under the new version, and their envelopes record the
   transition.

The human owns exactly one step: the meaning.
