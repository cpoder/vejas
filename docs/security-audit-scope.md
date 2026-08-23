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
- **Reads are public by design.** Even with `VEJAS_TOKEN` set, GET routes
  (`/file`, `/surface`, `/rules`, `/graph`, `/topology`, `/events`,
  `/metrics`, `/dlq`) stay open: they expose the integration **logic** — flow
  source, the business surface, the pipeline — but **never a secret value**
  (`/secrets` returns status only, verified; and since the symlink fix in S3,
  `/file` cannot read outside the root). If that logic is itself sensitive,
  the write-API port belongs on a trusted network or behind the operator's own
  auth. Gating reads as well — which requires the panel to carry the token — is
  a deferred product decision, not a runtime bug.

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
  new manifest without passing the write gate. `guard_path` **contains
  traversal** — `..`, absolute escapes, wrong extensions, **and symlinks**: the
  resolved path is canonicalized and required to stay under
  `root.canonicalize()`, so a `*.vjs` symlink to `/etc/passwd` or the secrets
  file is refused (regression-tested in `e2e/security-traversal.sh`). Residual
  and out of scope for a static-attacker model: a TOCTOU race that swaps a
  symlink *between* the check and the read needs an active racing process, not
  a statically planted link (e.g. from a merged recipe PR) — the practical hole
  is closed. Confirm `secret()` values never appear in `/proc/<pid>/cmdline`
  for exec children.

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
- **Honest bound (found this pass — S4-1).** `secret()` protects the **store**:
  no endpoint returns a stored secret, and secrets are references, never
  literals. But a flow *author* can copy a resolved secret onto the **data
  path** — `k = secret("x"); emit "vx.out", {t: k}` (or `respond`) — and the
  raw-payload read surfaces then display it: the `/events` ring, DLQ envelopes,
  `/topology` `last_error`, and the control-plane `status`/`events` forwarded to
  the hub. This is author misuse, not a store leak, and the key-shaped panel
  mask does not catch it (the emitted key isn't credential-shaped). Rule:
  **`secret()` is for the auth path — never emit, respond, or log a resolved
  secret.** Mitigation: this rule documented now; a load-time lint that warns
  when a `secret()` value flows into an `emit`/`respond` is planned.

### S5 — `http-in` (unauthenticated ingestion)

- **Threat:** the one deliberately unauthenticated surface is used to do more
  than publish an event.
- **Claimed invariant:** `http-in` cannot mutate config, read secrets, or reach
  the write API (a separate port); it publishes a JSON body to `vx.<suffix>`
  and answers `202` after the pub-ack; non-JSON is rejected. **Honest bound:**
  the suffix is caller-controlled, so it can publish to *any* `vx.*` subject —
  **including one a sink consumes** (a Slack post, an MQPUT). The blast radius
  is therefore "inject any bus event," not "publish to a source only." The
  optional `ALLOW` list restricts the surface to declared suffixes/prefixes
  (recommended, defence in depth); without it the port is any unauthenticated
  webhook — it belongs behind the operator's ingress / trust boundary.
- **How to test:** confirm `http-in` reaches no mutation route or secret;
  with `ALLOW` set, confirm a suffix outside it is refused (`403`) and a
  permitted one still publishes (`e2e` covers this); without `ALLOW`, confirm
  the honest bound above is documented, not surprising; fuzz the suffix for
  injection into subject space (`.`, `>`, `*`, traversal) beyond the intended
  prefix; confirm it shares no port with the write API.

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
- **Re-export note.** The control channel adds no secret of its own — `status`
  is refs + resolve-status, `audit` is metadata — but it re-exports data-plane
  fields (`last_error`, event previews) which are raw payloads, so a secret a
  flow wrongly puts on the data path (S4-1) surfaces here too, not only on
  `/events`.

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
- `http-in` is unauthenticated — its blast radius is "inject any bus event,
  including one a sink consumes" (S5, honest bound). `ALLOW` tightens it to the
  operator's declared subjects; the port otherwise belongs behind the
  operator's ingress. The audit's job is to prove nothing worse than bus
  injection is reachable, and that `ALLOW` holds when set.

## A passing audit

Every surface's claimed invariant survives an adversarial attempt to break it
under the stated assumptions; every by-design risk is bounded as claimed; and
any gap is either fixed or explicitly accepted in writing before Vejas is
distributed (ADR-0029 R7).

## Already found and fixed this session (auditor orientation)

Two internal adversarial passes ran before this external audit (the second a
three-way pass that re-attacked the first's fixes). Do not spend time
re-deriving these — they are fixed and regression-tested; instead, try to
*break the fix* and look for siblings the passes missed:

- **F1 — path traversal via symlink (CRITICAL, fixed).** `guard_path`
  blocked `..` but not symlinks; a link under `flows/` to `/etc/passwd` or
  the secrets file was read in the clear by `GET /file` (unauthenticated).
  Fixed by canonicalizing the resolved path under `root.canonicalize()`;
  regression test `e2e/security-traversal.sh` runs in CI. **Residual:** a
  TOCTOU symlink swap during the read is out of scope for a static attacker
  but worth a look.
- **F3 — non-constant-time token compare (fixed).** `ct_eq` now used for
  `VEJAS_TOKEN` and `X-Approval-Token`.
- **F2 — http-in published to any `vx.*` (fixed, defence in depth).**
  Optional `ALLOW` on the `http-in` connector restricts the subject
  suffixes; out-of-list → 403. Absent = any suffix (compat), documented.
- **A — the write gate keyed on the POST verb, not mutation (HIGH, fixed).**
  The bearer gate covered `POST` only; `PUT`/`PATCH`/`DELETE` and a read-method
  flow that emits wrote the bus unauthenticated. Fixed: the gate keys on
  *mutation* — every non-read method is gated, and a read-method flow that
  writes the bus is gated too. `e2e/api-write-gate.sh` in CI (exotic verb →
  `401` fail-closed; `HEAD` on an emitting flow → `404`).
- **A' — a dynamic emit subject escaped the read-method gate (HIGH, fixed).**
  The check used `emit_subjects`, which lists only statically resolvable
  subjects; a flow emitting to `f"vx.{x}"` or a lowercase local had an empty
  list and gated open. Fixed: gate on *any* emit, not the resolved-subject list.
- **A'' — an emit reached via `invoke` escaped the gate (HIGH, fixed).** A
  read-method flow whose only bus write is a service `invoke` (the service
  emits, and its emits are appended to the caller's at runtime) still gated
  open — the emit check did not follow invokes, and `invoke` nests in any
  subexpression. Fixed: `Program::writes_bus()` is a full-AST visitor — true on
  any `emit` or any `invoke`, wherever it sits — with **no `_` arm**, so a new
  `Stmt`/`Expr` variant fails to compile rather than silently escaping the
  gate. **Break it:** is there a bus-writing or side-effecting construct other
  than `emit`/`invoke`? The fix assumes every builtin is pure.
- **B — governed-mode bypass on `/connectors/new` (HIGH, fixed).** With
  `VEJAS_REQUIRE_APPROVAL=1`, `/connectors/new` lacked the approval + cluster
  guards its twin `/flows/new` had — an agent could create and hot-start a
  connector (an exec driver = arbitrary `CMD`) with no human approval:
  governance bypass **and** RCE. Fixed to mirror `/flows/new`;
  `e2e/governed-gate.sh` in CI. Sibling-checked: the HTTP twin, all mutating
  MCP tools, and `/provision` are gated; `dlq/replay`, `dlq/purge` and
  `connectors/test` are operational (write-token, not approval) by design.

## Known minor points to confirm or harden (not blockers)

- **M1 — poller URL in logs.** `http-poll`/`oauth-poll` log the full poll
  URL. Convention is that credentials ride in `HEADERS` via `secret()`, not
  the URL, so the URL carries no secret — but a user who puts a token in the
  URL would see it logged. Consider masking query params in the log.
- **M2 — the secret-shaped mask is client-side only.** The panel masks
  credential-shaped values using `SECRET_KEY_PATTERN`, but `GET /surface`
  returns raw literal values; the real protection is the model itself
  (secrets are `secret()` references, never literal values — enforced by the
  admission lint). **Confirmed this pass (S4-1):** no endpoint returns a
  *stored* secret, but a flow that **emits/responds** a resolved secret routes
  it onto the data path, where the raw-payload read surfaces display it (see the
  S4 honest bound) — author-routed, not a store leak. The mask is cosmetic, not
  the boundary; the fix is the S4 rule plus the planned lint.
- **M3 — `guard_path` dir check is a prefix, not a path segment.**
  `flowsevil/x.vjs` satisfies `starts_with("flows")`; the file stays under
  `root` (and unscanned, so inert), so this is not a traversal — a path-segment
  match would read cleaner. Hardening, not a bug.

## Method note

The internal pass used **static data-flow tracing** (no heavy live
processes) after a bug in `rand_hex` (unbounded `/dev/urandom` read → OOM)
took down sessions running live flows. Prefer static analysis over standing
up the full stack repeatedly.
