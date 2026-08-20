# 0013 — Remote control plane over NATS leaf nodes

- Status: Proposed
- Date: 2026-08-20

## Context

Collectors (ADR-0012) are outbound-only. The operator can *observe* them
(heartbeat facts carry liveness and bundle revision) but cannot *operate* them:
every update, config correction or diagnostic goes through the client's local
admin. Fleet operations need a remote channel.

The standard answer is publish/subscribe over a connection the edge initiates:
the collector dials out and subscribes to a command topic, and the operator
pilots it through that link. The firewall posture is unchanged — nothing
listens, the collector still only dials out. What changes is the **trust
contract**: the operator gains a hand inside the client's network, so what that
hand may do must be explicit, bounded, auditable and — where it touches
business behavior — locally approvable.

Vejas already runs NATS on both sides (ADR-0002), and NATS has this exact
pattern built in: **leaf nodes**.

## Decision

- **Uplink:** the collector's NATS connects as a *leaf node* to the operator's
  hub — outbound TLS, per-tenant credentials, and hub-side authorization that
  pins the tenant to its own subjects (`vx.<tenant>.>`). Enabled by
  configuration in the bundle (`VEJAS_TENANT` + a leafnode remote in the NATS
  config); absent configuration means no control plane, and the collector
  behaves exactly as today.
- **Control consumer:** the runtime subscribes to `vx.<tenant>.ctl.cmd` and
  executes a **closed allowlist** of commands (wire format and command set:
  `docs/CONTROL.md`), replying per request and pushing periodic status
  upstream. Unknown commands are errors, not extensions.
- **Three tiers of authority:**
  1. *Telemetry* — `ping`, `status`, `events`, `probe`: read-only, always
     allowed. No payload ever contains a secret value.
  2. *Operations* — `reload`, `restart`: re-execute what is already on disk;
     always allowed.
  3. *Content changes* — literal edits and package updates arrive as
     **proposals** in a local approval queue: the client's admin approves in
     the panel (literals with shadow-replay, ADR-0005 — the propose→approve
     loop applied to fleet management). A client-side, locally-configured
     `VEJAS_AUTO_APPROVE` switch turns tier 3 into direct-apply for
     zero-touch fleets; it is not settable remotely.
- **Never remote:** reading or writing secret values (`rotate_requested` may
  flag a rotation in the panel, nothing more), anything that executes
  arbitrary commands (no remote path may create or modify `exec-*` manifests
  outside the approval queue), and the control plane's own configuration
  including the auto-approve switch.
- **Audit:** every command, reply and proposal decision is recorded in the
  local trace ring (panel-visible) and echoed upstream on
  `vx.<tenant>.ctl.audit` for the operator's console.

## Consequences

- The sales pitch strengthens rather than weakens: "pilotable from the cloud,
  and everything the cloud is allowed to do is visible, bounded, auditable in
  your panel — and your secrets are not readable, even by us."
- The operator's console (per-tenant fleet page) becomes real: live topology,
  remote Test button, one-click package rollout with per-client approval
  state.
- Revocation is a credentials kill: cut a tenant's leaf creds and the hand is
  gone; the collector keeps collecting.
- **Provisioning is coupled:** the future "provision tenant" operation must
  also mint the leaf credentials and the hub account pinned to
  `vx.<tenant>.>`, and offboarding must revoke them — otherwise fleets grow
  uplink-less tenants or, worse, orphan credentials. The tenant-to-operator
  id mapping (e.g. a tenant slug to an organization id) lives on the
  operator's side; Vejas carries no operator ids.
- **Costs:** the allowlist is now a security boundary to maintain and review;
  hub authorization must be tested per tenant (subject pinning is the
  isolation); v1 commands are interactive request/reply over core NATS — an
  offline collector fails commands visibly rather than queueing them
  (JetStream-domain queued delivery is the designed upgrade path, kept out of
  v1 to avoid cross-leafnode JetStream complexity).

## Alternatives considered

- **HTTPS long-poll / WebSocket command channel:** a second protocol and a
  second server surface while NATS is already deployed on both ends.
- **MQTT:** a second broker for a pattern NATS leaf nodes cover natively.
- **VPN / reverse SSH:** all-or-nothing network access; opposite of a closed
  allowlist.
- **Staying observation-only:** the previous state; makes every fleet
  operation a manual client-side task, which does not scale past a handful of
  collectors.
