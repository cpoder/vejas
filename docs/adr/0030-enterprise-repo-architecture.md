# 0030 — How the enterprise tier attaches: seams, not plugins

- Status: Proposed (cross-review; the architecture accept is ours, the go to
  build act 5 is the founder's — already given, ADR-0028)
- Date: 2026-08-23

## Context

ADR-0028 drew the open-core boundary and named the mechanism: a private
`vejas-enterprise` repo, "separate crates and binaries that attach at the
seams the architecture already has — everything speaks the bus, heavy
connectors are standalone binaries, the fleet console is specified as a bus
consumer (CONTROL.md). Zero forks, zero closed code in the public repo." The
founder has prioritized building act 5 (SSO, RBAC, N-approver policies,
audit/SIEM export, fleet console). This ADR decides the *how* concretely, so
every enterprise feature has one obvious way in and the public core never
grows a closed appendage.

## Decision (proposed)

### The principle: attach by consuming seams, never by plugging into the core

The public runtime gains **no plugin trait, no extension point, no auth
hook**. Enterprise features attach only to seams that are *already public and
stable*: the HTTP API, the CONTROL channel (`vxc.*`, CONTROL.md), the audit /
DLQ / version streams (SUBJECTS.md), and the token gate. Each enterprise
capability is a **standalone binary** (the MQ/AMQP precedent) that consumes
those seams or sits in front of them.

The decisive property: **the public core runs identically with or without the
enterprise tier.** That is what keeps the boundary honest (ADR-0028: nothing
closed in the public repo), and it is the same property R5 already demands —
the core must be complete and survivable alone. A plugin interface would blur
the boundary and couple the two release trains; seams do neither.

### The four attachments

| Enterprise capability | Attaches as | To which seam |
|---|---|---|
| **SSO + RBAC** (`vejas-authproxy`) | A reverse proxy **in front of** the write-API port | The token gate + the HTTP API |
| **Fleet console** (`vejas-console`) | A standalone binary + web UI | The CONTROL channel `vxc.*` (CONTROL.md) |
| **Audit / SIEM export** (`vejas-siem`) | A bus consumer | `VEJAS_AUDIT` + `vxc.<tenant>.audit` streams |
| **Provisioning at scale** (`vejas-provisioner`) | A standalone binary | The provision API + control creds |

### Auth is a proxy, not a core hook

SSO/RBAC lives in `vejas-authproxy`, a reverse proxy before the write API. The
core's single `VEJAS_TOKEN` becomes the proxy→core credential (held only by the
proxy); **users** authenticate to the proxy (OIDC/SAML), which enforces RBAC
per role by inspecting the documented request (who may edit which surface, who
may hit `/proposals/approve`) and forwards only what passes, stamped with the
core token. This is the standard enterprise-auth layering and needs **zero core
change** — the core keeps its one-token primitive.

**N-approver policy** rides the same seam: the core's *single approve* is the
primitive; the proxy/console holds the policy — a proposal is forwarded to the
core's approve endpoint only once N distinct authenticated approver identities
have signed off. Governed mode (ADR-0024) stays the mechanism; the enterprise
layer is the workflow on top.

### Candidate public-safe seam additions (added when a feature needs them)

Some enterprise features want a *little* more from a seam. Anything added lands
in the **public** core, stays useful open, and is added only when the feature
needs it — never a closed appendage:

- An optional **actor identity** on the approve endpoint, so the audit records
  *who* approved (the proxy supplies it after authenticating). Useful to an
  open single operator naming themselves; required by N-approver.

If a future need cannot be met by a public-safe seam, that is the signal to
re-examine the boundary in an ADR — not to add closed code.

### Sequencing (R4: early and sellable)

Build order follows the graveyard rule "the enterprise tier must exist early
and sellable, not late and polished": **`vejas-authproxy` (SSO + basic RBAC)
first** — SSO is the first thing an enterprise buyer asks for and is sellable
on its own. Then the fleet console, N-approver, and SIEM export, each an
increment that a design partner can pull.

## Consequences

- The public repo stays whole: same CI, catalog, docs; the core is a complete
  product alone (R5), and the enterprise tier is additive.
- Two release trains stay decoupled — the enterprise binaries pin a version of
  the public seams, exactly as an external integrator would.
- The seams become a *contract*: the HTTP API, CONTROL.md and SUBJECTS.md are
  now load-bearing for a paying tier, so their stability is a commitment, not
  just a convenience.
- The **Vejas trademark** (ADR-0028) is the commercial protection; the private
  repo and the seams carry no license novelty.

## Rejected

- **A plugin trait / auth hook in the public core** — couples the release
  trains, blurs the ADR-0028 boundary, and adds a maintenance surface the core
  does not need. Seams already give every attachment a way in.
- **Forking the core into the enterprise repo** — the thing ADR-0028's "zero
  forks" explicitly forbids; the core stays single-sourced and public.
- **A closed panel tier of existing read/write features** — breaks ADR-0028
  rule 1 (nothing shipped moves back). Enterprise is new organize-and-control
  capability, never a paywall over what shipped.
- **Building auth into the core behind a feature flag** — a flag that gates a
  closed code path is closed code in the public repo by another name.
