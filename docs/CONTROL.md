# The control channel (remote collectors) — specification

Status: specified with ADR-0013 (Proposed), not built. This document is the
wire-level contract; it is deliberately small and closed.

A **collector** (ADR-0012) may connect its local NATS to an operator's hub as
a **leaf node** — an outbound TLS connection with per-tenant credentials. Over
that link, the operator pilots the collector through ordinary NATS messages on
a fixed subject convention, within a closed command allowlist. No leafnode
configuration, no control plane: the collector then behaves exactly as an
unmanaged deployment.

## Enabling

Two pieces of configuration, both local to the collector, neither settable
remotely:

- `VEJAS_TENANT=<tenant>` in the runtime's environment — names the subject
  space below.
- A leafnode remote in the collector's NATS config:

```
# nats-server.conf (collector)
jetstream {}
leafnodes {
  remotes = [
    { url: "tls://hub.<operator>:7422", credentials: "/creds/<tenant>.creds" }
  ]
}
```

Hub-side, the tenant's account authorization pins imports and exports to
`vx.<tenant>.>` — subject pinning IS the isolation between tenants. Revoking
the tenant's credentials severs the hand; the collector keeps collecting.

Optional: `VEJAS_AUTO_APPROVE=1` (local only) applies tier-3 proposals
without local approval — for fleets that want zero-touch. Default: off.

## Subjects

| Subject | Direction | What |
|---|---|---|
| `vx.<tenant>.ctl.cmd` | hub → collector | command requests (core NATS request/reply) |
| `vx.<tenant>.ctl.status` | collector → hub | periodic state push (`STATUS_SECS`, default 60) |
| `vx.<tenant>.ctl.audit` | collector → hub | one message per executed command / proposal decision |

Commands are **interactive**: core NATS request/reply, no queueing. An offline
collector fails a command visibly at the console (timeout), it does not apply
it later. (Queued delivery via JetStream domains is the designed v3 upgrade;
it changes nothing in the payloads below.)

## Wire format

Request (hub → `vx.<tenant>.ctl.cmd`):

```json
{ "id": "<hub-generated id>", "cmd": "<name>", "args": { } }
```

Reply (same NATS request/reply exchange):

```json
{ "id": "<same id>", "ok": true,  "result": { } }
{ "id": "<same id>", "ok": false, "error": "<plain words>" }
```

Every request produces exactly one reply and one `ctl.audit` echo:

```json
{ "ts": "<ISO 8601>", "id": "…", "cmd": "…", "ok": true, "summary": "<one line>" }
```

The acting *human* behind a command is recorded hub-side by the console,
keyed on the request `id`. The collector cannot verify an identity claim, so
none travels on the wire — do not add actor fields here and mistake them for
authentication.

## Commands (v1 allowlist — closed; unknown `cmd` is an error)

| Tier | `cmd` | `args` | `result` |
|---|---|---|---|
| 1 telemetry | `ping` | — | `{pong: true, ts, runtime_version, bundle}` |
| 1 telemetry | `status` | — | topology (units, statuses, restarts, last_error), versions, secret references with resolve-status (**statuses only, never values**), pending proposal count |
| 1 telemetry | `events` | `{unit?, n?}` | trace-ring excerpt (same shape as `GET /events`) |
| 1 telemetry | `probe` | `{file}` | connector test verdict (same as `POST /connectors/test`) |
| 2 operations | `reload` | — | `{total, started, stopped}` |
| 2 operations | `restart` | `{unit}` | `{ok}` |
| 2 operations | `rotate_requested` | `{ref}` | `{ok}` — flags the reference in the panel's Secrets card ("rotation requested by the operator"); the new value is typed locally, never transmitted |
| 3 content | `propose` | see below | `{proposal_id, state: "pending" \| "applied"}` |
| 3 content | `proposals` | — | pending list `[{id, kind, summary, received_at}]` |

`approve` and `reject` are **not remote commands**. They are local panel
actions, by design: the approval queue is the client's hand.

## Proposals (tier 3)

```json
{ "cmd": "propose", "args": {
    "kind": "set_literal",
    "file": "packages/<t>/flows/x.vjs", "name": "THRESHOLD", "key": "-", "value": 200 } }

{ "cmd": "propose", "args": {
    "kind": "package_update",
    "package": "transpositions",
    "files": { "services/map_misure_acn.vjs": "<full content>", … } } }
```

- Proposals persist on disk under `proposals/<id>/` in `VEJAS_ROOT` (they
  survive restarts) and appear in the panel's approval queue.
- Approving a `set_literal` runs the existing apply path — shadow-replay on
  the flow's recent real events first (ADR-0005), then the surgical write.
- Approving a `package_update` parse-checks every file, stages them into
  place, and reloads. Any parse failure rejects the whole proposal.
- With `VEJAS_AUTO_APPROVE=1`, `propose` applies immediately and returns
  `state: "applied"`; the audit trail is identical.
- A `package_update` may create or modify **`exec-*` manifests only through
  this queue** — never via any direct command — so arbitrary command execution
  always crosses the client's approval (or their explicit standing auto-approve).

## Status push

Every `STATUS_SECS` on `vx.<tenant>.ctl.status`:

```json
{ "ts": "…", "tenant": "…", "bundle": "<REVISION>", "runtime_version": "…",
  "units": [{"name": "…", "status": "…", "restarts": 0, "last_error": null}],
  "pending_proposals": 0 }
```

`last_error` is nullable, truncated to ~200 characters — it is what a fleet
page displays continuously ("collector X: unit y failing") without issuing an
interactive `status` per tenant per refresh.

This is the operator console's live view; the heartbeat *fact* (ADR-0012)
remains the data-channel liveness signal and needs no control plane.

## Security invariants (normative)

1. No secret **value** ever appears on any `ctl.*` subject, in any direction.
   `rotate_requested` (tier 2, in the table above) may flag a reference in
   the panel; the value is typed locally.
2. The allowlist is closed. Unknown commands error. There is no remote shell,
   no remote `exec`, no remote write outside the proposal queue.
3. The control plane cannot reconfigure itself: `VEJAS_TENANT`,
   `VEJAS_AUTO_APPROVE`, leafnode credentials and this allowlist are local
   configuration only.
4. Everything is audited twice: locally (trace ring, panel) and upstream
   (`ctl.audit`).
5. Hub-side subject pinning per tenant is mandatory; a hub must never grant a
   leaf credentials broader than `vx.<tenant>.>`.

## Rollout

- **v1:** leafnode uplink; `ping status events probe reload restart`; audit;
  status push. (Interactive fleet console becomes possible.)
- **v2:** proposals (`set_literal`, `package_update`) + panel approval queue +
  `VEJAS_AUTO_APPROVE`. (Remote rollout with client consent.)
- **v3:** queued commands over JetStream domains for offline collectors;
  same payloads.
