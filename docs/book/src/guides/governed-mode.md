# Governed mode: proposals & approvals

An agent is good at *how* — reading a dead letter, drafting the fix,
proving it against real traffic. It should not be the one who decides *what
a rule means* in production. Governed mode draws that line in the runtime:
agents **propose**, a human **approves** (ADR-0024).

## Turn it on

```
VEJAS_REQUIRE_APPROVAL=1
VEJAS_APPROVAL_TOKEN=<a secret distinct from VEJAS_TOKEN>
```

The approval token **must differ** from the agent's `VEJAS_TOKEN` — the
agent holds `VEJAS_TOKEN` to reach `/mcp`, so a shared secret would let it
approve its own change. The runtime **refuses to start** if
`VEJAS_REQUIRE_APPROVAL` is set without a distinct `VEJAS_APPROVAL_TOKEN`: a
governance mode with a shared key is governance in name only.

## What changes

With it on, **every** mutation door — the mutating MCP tools *and* the raw
HTTP endpoints (`/surface/set`, `/flows/new`, `/secrets/set`, …) — stops
executing and answers:

```
409  approval required: submit a proposal (vejas_propose, or the panel)
```

Reads stay open. A direct write is never silently accepted; it is turned
into a request.

## The flow

1. **Agent proposes** — `vejas_propose` with the change and its *evidence*
   (a shadow-replay diff, a canary result). It cannot approve. `vejas_proposals`
   lists the queue.
2. **Human decides** — in the panel's **Approval queue** card, or
   `POST /proposals/{id}/approve|reject` with the `X-Approval-Token`
   header. The card shows the evidence; a proposal with none is flagged
   **"⚠ No evidence"** loud — approving blind is a deliberate act.
3. **On approve** — the change executes exactly as a normal promote would
   (hot-reload, or a [cluster-wide version](change-safely.md) in 60 ms) and
   is recorded.

## Two safeguards

- **Audit outlives the queue.** The live queue is a bounded JetStream KV
  (`VEJAS_PROPOSALS`); the durable proof of who approved what is a separate
  audit stream (`VEJAS_AUDIT`) — a proposal aging out of the queue never
  takes its approval record with it.
- **No stale approvals.** The baseline is re-checked at approve time: if the
  surface moved since the proposal was made, it **auto-expires** —
  re-propose against the current state.

## Where it fits

Governed mode is the seam of the whole [change-safely](change-safely.md)
loop: the agent does the work and the proving, the human owns the meaning,
and every step is on the record. Leave it off for a single-operator dev box;
turn it on where a wrong meaning is a real incident.
