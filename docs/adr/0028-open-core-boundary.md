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
future feature lands on before it is built, and so can we.

### The boundary, applied

| Core — Apache-2.0, forever | Enterprise (act 5, to be built) |
|---|---|
| Runtime, language, panel, MCP surface | SSO (OIDC / SAML) |
| **All connectors, present and future** | RBAC: who edits which surface, who approves what |
| Clustering, versioning / time-travel / canary | Approval policies: N approvers, role separation, workflows |
| Governed mode, single approver | Compliance & audit export (SIEM feeds, reports) |
| DLQ, replay, golden traffic, observability | Fleet console: multi-site CONTROL, industrialized |
| Everything on master today | Tenant provisioning at scale · support / SLA |

### Three rules that make the line credible

1. **Nothing shipped ever moves back.** Everything public today — clustering
   and versioning included, though others sell their equivalents — stays
   open. They are also the published, reproducible proof behind "measured,
   not claimed"; a claim whose bench is paywalled is a claim.
2. **Never a paid connector.** Connectors are the adoption surface and the
   certified catalog is the moat; a paywalled connector kills both.
3. **The core license never changes.** Apache-2.0, in perpetuity, stated in
   the README as a commitment — the standing differentiator against the
   fair-code relicensing pattern. Consequence: no CLA (inbound = outbound
   Apache-2.0 suffices); contributions ride a DCO.

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
