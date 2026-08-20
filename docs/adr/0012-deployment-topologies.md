# 0012 — Deployment topologies: cells and outbound-only collectors

- Status: Accepted
- Date: 2026-08-20

## Context

Vejas must run in two very different places with one artifact. Operated
centrally, it is a multi-tenant integration layer (many tenants' connectors and
flows on shared infrastructure). Deployed at a customer's site, it must satisfy
an IT department that refuses inbound exposure and wants credentials to never
leave the premises. Building two products would fork the runtime; building a
"phone home to be configured" agent would fork the packaging model.

## Decision

One artifact — the same binary, the same package layout, the same manifests —
in **two topologies**:

1. **Cells (central, multi-tenant).** One NATS + one runtime per cell of N
   tenants, each cell its own `VEJAS_ROOT` with `packages/<tenant>/` per tenant
   and shared packages (e.g. per-country mapping services) alongside. Scaling
   or isolating a tenant = another cell, which is a compose change, not code.
2. **Collectors (remote, single-tenant).** A generated per-client bundle:
   compose with **no inbound listener** (internal NATS, panel bound to
   localhost/LAN only), a single outbound HTTPS egress to the operator's
   ingestion API, the client's package plus shared packages, a writable local
   secret store (`VEJAS_SECRETS_FILE`, filled from the panel, values never
   leave the machine), and a heartbeat connector whose facts double as
   liveness + deployed-revision reporting upstream.

Bundles are **generated** (packages copied, sinks and heartbeat templated,
every `.vjs` parse-checked, required secret references derived from the files
themselves) so a client install is: start, paste secrets in the panel, test.

## Consequences

- One codebase to harden; every runtime improvement serves both topologies.
- The collector's security story is structural: nothing listens, one egress
  rule, secrets stay local. Remote management is deliberately a separate
  decision (ADR-0013) so this posture is a baseline, not an accident.
- Fleet observability rides the data channel (heartbeat facts), so the
  operator needs no side channel to know a collector is alive and which
  bundle revision it runs.
- **Cost:** updates are pull-shaped (ship a new bundle, client reloads) until
  a control plane exists; per-tenant secret scoping inside a shared cell is
  still open (ADR-0008) and gates third-party-authored packages.

## Alternatives considered

- **Hosted-only (no on-prem):** excludes exactly the customers whose systems
  hold the evidence; contradicts the collector use case.
- **Agent-per-host (Datadog-style daemon):** wrong granularity — Vejas
  integrates systems over APIs, it does not instrument hosts.
- **VPN / reverse tunnel into the client:** all-or-nothing network access,
  heavy to operate, and destroys the "nothing enters" pitch.
