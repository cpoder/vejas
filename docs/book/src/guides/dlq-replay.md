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
  message again. Purge (`/dlq/purge`) is equally explicit and audited.

## The loop that makes it self-healing

1. A message dies; the envelope carries version `v3`.
2. An agent reads `vejas_dlq`, drafts a candidate flow, proves it: replay
   yesterday's real traffic through it
   ([time-travel](change-safely.md)), watch it shadow live traffic
   (canary).
3. In governed mode it *proposes* with that evidence; you approve — the
   promote fans out cluster-wide in 60 ms.
4. `vejas_dlq_replay` — the dead messages pass under the new version, and
   their envelopes record the transition.

The human owns exactly one step: the meaning.
