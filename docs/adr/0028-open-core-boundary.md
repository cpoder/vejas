# 0028 — The open-core boundary

- Status: Proposed (the final accept is the founder's — this records the
  reasoning and the line)
- Date: 2026-08-23

## Context

The platform is public, Apache-2.0, measured, certified, documented — and
undistributed pending an unrelated gate. Before act 5 (multi-user panel,
SSO, RBAC) can be built, one question decides what goes where: what is
open, what is commercial, and by what rule. The founder has no prior
open-core experience; the boundary must therefore be a *principle* that
answers future cases by itself, not a list renegotiated feature by feature.

Reference points considered: GitLab's buyer-based open core (the model that
aged best), pure open-source + hosted cloud (Supabase), source-available
"fair-code" (n8n — our named comparison point), dual licensing (AGPL +
commercial), support-only (does not scale for a solo founder).

## Decision (proposed)

### The principle: the buyer draws the line

**Free: everything an individual developer or a single team needs to run
Vejas in production. Commercial: capabilities whose buyer is a manager, a
CISO, or a platform team** — the features bought to *organize and control*
usage across people and sites, not to *use* the product.

The line is predictable in both directions: anyone can tell which side a
future feature lands on before it is built, and so can we. The line is
always drawn by the *nature of control* a capability grants, never by a
quantity ("at scale", "more than N") — a threshold is a renegotiation in
disguise.

**Precedence with rule 1:** for anything *already shipped* that the buyer
line would classify as enterprise (the `vejas_provision` primitive, the
CONTROL v1 fleet channel), rule 1 wins — it stays open. The buyer line
governs what gets *built next*, never the already-delivered. Concretely:
the CONTROL channel remains open; the industrialized fleet *console* on
top of it is the enterprise product.

### The boundary, applied

| Core — Apache-2.0, forever | Enterprise (act 5, to be built) |
|---|---|
| Runtime, language, panel, MCP surface | SSO (OIDC / SAML) |
| **All connectors, present and future** | RBAC: who edits which surface, who approves what |
| Clustering, versioning / time-travel / canary | Approval policies: N approvers, role separation, workflows |
| Governed mode, single approver | Compliance & audit export (SIEM feeds, reports) |
| DLQ, replay, golden traffic, observability | Fleet console: multi-site CONTROL, industrialized |
| Everything on master today (incl. the `vejas_provision` primitive and the CONTROL v1 channel) | Managed multi-tenant control: mass lifecycle, per-tenant isolation, quotas, per-tenant RBAC · support / SLA |

### Three rules that make the line credible

1. **Nothing shipped ever moves back.** Everything public today — clustering
   and versioning included, though others sell their equivalents — stays
   open. They are also the published, reproducible proof behind "measured,
   not claimed"; a claim whose bench is paywalled is a claim.
2. **Never a paid connector.** Connectors are the adoption surface and the
   certified catalog is the moat; a paywalled connector kills both.
3. **The core license never changes.** Apache-2.0, in perpetuity, stated in
   the README as a commitment — the standing differentiator against the
   fair-code relicensing pattern. The **no-CLA policy is the mechanism**
   that makes this structural rather than promised: without a CLA, even we
   cannot relicense contributed code — "forever" holds because it is out of
   our own hands. Contributions ride a DCO. (Apache-2.0 already lets
   enterprise code incorporate core code with attribution — no CLA is
   needed for that, and copyleft is what would break it, which is one more
   reason the core is not AGPL.)

A governance corollary of the line: a community PR that implements an
enterprise-side capability *in the free core* (e.g. contributed RBAC) is
declined politely, citing this ADR — the boundary must hold from below as
well as from above. The reverse never happens: nothing is removed from
core to make room for a paid version of it (rule 1).

### Mechanics

- A private `vejas-enterprise` repository: separate crates and binaries
  that attach at the seams the architecture already has — everything
  speaks the bus, heavy connectors are standalone binaries, the fleet
  console is specified as a bus consumer (CONTROL.md). Zero forks, zero
  closed code in the public repo.
- The **Vejas trademark** is the commercial protection; the license is not.
- **Hosted console (SaaS)**: deliberately deferred — the same posture as
  ADR-0027. The fleet console's code splits naturally into a hosted control
  plane later (runtimes stay on-prem); we record the option and wait for a
  concrete demand signal instead of building on a hypothesis.

### Sequencing

Deciding this boundary and building act 5 behind it can proceed now.
**Selling** — entity, billing, contracts — is gated on the founder's
employment-conflict clarity, the same gate as public distribution. Pricing
is deliberately not designed here: it needs real users asking for SSO
first.

## Consequences

- Act 5 unblocks with a line that answers feature placement without
  renegotiation.
- The public repo's promise gets *stronger* (explicit perpetual-license
  commitment), not weaker — open-core done as positioning, not retreat.
- Enterprise work happens in a separate repo; the public CI, catalog and
  docs remain whole.

## Rejected

- **Fair-code / source-available relicensing** — betrays the README's own
  argument and burns the differentiation against n8n.
- **Paid connectors or a paid panel tier of existing features** — cannibalizes
  adoption, breaks rule 1.
- **Dual licensing (AGPL)** — legal machinery serving an embedding scenario
  we do not have.
- **Support-only monetization** — does not scale for a solo founder with
  agent leverage; support rides the enterprise tier instead.
- **Building the hosted console now** — no demand signal yet (deferred, not
  refused).
