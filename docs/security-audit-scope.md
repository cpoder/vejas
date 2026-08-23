# Vejas — external security audit scope

This is the brief for an external review of Vejas's write-capable and
network-exposed surfaces, and — because we publish it — the project's threat
model in the open. It is deliberately public: the surfaces are visible in the
source already, and stating the claimed invariants plus *how to falsify them*
is a stronger signal than silence (ADR-0029 R7: trust is pre-earned).

Scope is the **runtime's attack surface**, not business or personnel matters.
Every claim below is meant to be *tested*, not trusted — where a claimed
invariant is wrong, that is the finding we want.

## Actors and trust boundaries

| Actor | Reaches | Should be able to |
|---|---|---|
| **Operator** | The write API (panel, `/mcp`, HTTP mutations) with `VEJAS_TOKEN` | Everything: write flows, promote, set secrets — this is the admin |
| **Agent** | `/mcp` with `VEJAS_TOKEN` | Propose and (outside governed mode) mutate; in governed mode, *propose only* — never approve |
| **Approver (human)** | `/proposals/{id}/approve` with `VEJAS_APPROVAL_TOKEN` | Approve/reject in governed mode; nothing else |
| **Event source** | `http-in` on its own port, **no auth** | Publish a JSON body to one subject — nothing else |
| **Bus peer** | NATS/JetStream | Governed by NATS's own auth (deployment-owned) |

Default posture: the write API binds `0.0.0.0:8686` in the raw binary but
**`127.0.0.1` in the shipped bundle**; `http-in` binds `0.0.0.0` on its own
port by design. The invariants below assume the write API is not exposed
unauthenticated to a hostile network — an assumption the audit should
pressure-test.

## Surfaces

### S1 — The write API (panel, `/mcp`, HTTP mutation endpoints)

- **Threat:** an unauthenticated or lower-privileged caller mutates flows,
  secrets, or the surface.
- **Claimed invariant:** when `VEJAS_TOKEN` is set, *every* POST (the whole
  mutating surface, `/mcp` included) requires `Authorization: Bearer <token>`;
  reads stay open and expose no secret values. With no token set, the write
  API is open — for a `127.0.0.1`/trusted-network deployment only.
- **How to test:** enumerate every `POST` route and every `/mcp` mutating
  tool; confirm each returns `401` without the bearer and only executes with
  it. Probe for routes that bypass the gate (path normalization, casing,
  trailing slashes, the dynamic `/proposals/…` prefix). Confirm no GET leaks a
  secret value or a token.

### S2 — Governed mode (the approval gate)

- **Threat:** the proposing agent approves its own change; a stale or
  tampered proposal executes.
- **Claimed invariant:** with `VEJAS_REQUIRE_APPROVAL=1`, every mutation is
  refused (`409`, "submit a proposal") and approval requires
  `VEJAS_APPROVAL_TOKEN` — a credential **distinct** from `VEJAS_TOKEN` (the
  agent holds the latter for `/mcp`). The runtime **refuses to start** if the
  distinct token is absent. At approve time the baseline is re-checked
  (TOCTOU): a proposal whose surface moved since submission auto-expires.
- **How to test:** with both tokens set, attempt approve using `VEJAS_TOKEN`
  (must fail), a missing token (must fail), the distinct token (must succeed).
  Race a promote against an approve to exercise the baseline re-check. Confirm
  the audit record (`VEJAS_AUDIT`) survives the proposal aging out of the KV
  queue. Confirm no mutation door (MCP or raw HTTP) skips the gate.

### S3 — Exec drivers (`exec-source`, `exec-stream-source`, `exec-sink`, `exec-rpc`)

- **Threat:** arbitrary code execution. A connector manifest's `CMD` is run
  via `sh -c` — this is RCE **by design** (the escape hatch for any protocol).
- **Claimed invariant:** the boundary is not "can a manifest run code" (it
  can) but **"who can write a manifest."** A manifest is a repo file under
  the flows/connectors root; creating or editing one goes through S1's write
  gate or filesystem access. The runtime never executes a `CMD` that did not
  arrive as an authored, on-disk manifest. Child processes receive `ENV`
  secrets in their **environment, never on argv**.
- **How to test:** confirm there is no path that turns runtime *input* (an
  event body, an HTTP param, a proposal payload) into a `CMD` string or a
  new manifest without passing the write gate. Inspect `guard_path` for
  traversal (`..`, absolute paths, symlinks). Confirm `secret()` values never
  appear in `/proc/<pid>/cmdline` for exec children.

### S4 — Secrets

- **Threat:** a credential leaks via argv, logs, a panel field, or an API
  read.
- **Claimed invariant:** `secret("path/key")` values are **write-only** — no
  surface returns them; the panel masks credential-shaped keys (a single
  `SECRET_KEY_PATTERN`, shared by the CI lint, the panel mask, and generation);
  in-process HTTP (ureq) carries them in headers, never a subprocess argv; the
  standalone connectors that shell out use a `0600` config file, never argv
  (ADR-0008). A literal is never accepted where a `secret()` is required.
- **How to test:** grep every response body and log line for a set secret;
  set a secret and confirm `GET /secrets` shows status only, never the value;
  inspect argv and env of every spawned process; attempt to store a credential
  as a plain literal and confirm refusal.

### S5 — `http-in` (unauthenticated ingestion)

- **Threat:** the one deliberately unauthenticated surface is used to do more
  than publish an event.
- **Claimed invariant:** `http-in` **only** publishes a JSON body to
  `vx.<suffix>` on its own port and answers `202` after the JetStream pub-ack.
  It cannot mutate config, read secrets, or reach the write API. Non-JSON is
  rejected; the subject is derived from the path, not from privileged input.
- **How to test:** confirm no `http-in` request can reach a mutation route or
  a secret; fuzz the subject suffix for injection into subject space
  (`.`, `>`, `*`, traversal) that could publish outside the intended prefix;
  confirm body-size / malformed-JSON handling; confirm it shares no port with
  the write API.

### S6 — The control plane (`vxc.*`, provision / CONTROL)

- **Threat:** a tenant drives another tenant's runtime, or a control command
  exceeds its allowlist, or a secret rides the control channel.
- **Claimed invariant:** control lives under a dedicated `vxc.` subject root
  (never the data `vx.` root); v1 is a **closed allowlist** of commands;
  content changes route through the approval queue, not raw execution; **no
  secret value ever appears on `ctl.*`**; per-tenant subject pinning at the
  hub is the isolation (ADR-0013). Provisioning validates the template in
  memory before writing, guards the slug and every parameter against `.vjs`/
  exec injection, and refuses an unfilled `${placeholder}`.
- **How to test:** attempt a command outside the allowlist; attempt to
  subscribe/publish across a tenant's `vxc.<other>` pin; scan control traffic
  for secret material; fuzz provision slugs/params for injection and path
  traversal; confirm the leaf-node creds are the revocation mechanism.

### S7 — The bus (NATS / JetStream)

- **Threat:** an unauthenticated bus peer reads or injects messages.
- **Claimed invariant:** NATS auth/TLS is **deployment-owned** — Vejas assumes
  the bus is either loopback or an authenticated/TLS NATS. The runtime adds no
  bus-level auth of its own.
- **How to test:** confirm the deployment guides state this clearly; confirm
  no code path assumes an *unauthenticated* bus is safe on a shared network.

## Deployment assumptions the invariants rest on

1. The write API is on `127.0.0.1` or an authenticated network; `VEJAS_TOKEN`
   is set whenever it is reachable by anyone but the operator.
2. `http-in` is behind the load balancer / ingress the operator intends, on a
   port distinct from the write API.
3. The NATS bus is loopback or authenticated + TLS.
4. In multi-tenant / multi-site mode, `VEJAS_APPROVAL_TOKEN` is set and the
   hub pins each tenant's `vxc.` subjects.

An audit finding that any invariant above **also holds when an assumption is
violated** is a strengthening we want; a finding that an invariant **fails
even when the assumptions hold** is a bug to fix before distribution.

## Explicitly accepted (by-design) risks

- Exec drivers are RCE for whoever can author a manifest — that actor is the
  operator, by definition. The audit's job is to prove *runtime input* cannot
  become an authored manifest (S3), not to remove the escape hatch.
- `http-in` is unauthenticated — its blast radius is bounded to "publish one
  event" (S5); the audit's job is to prove that bound is tight.

## A passing audit

Every surface's claimed invariant survives an adversarial attempt to break it
under the stated assumptions; every by-design risk is bounded as claimed; and
any gap is either fixed or explicitly accepted in writing before Vejas is
distributed (ADR-0029 R7).
