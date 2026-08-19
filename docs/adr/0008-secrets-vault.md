# 0008 — Secrets via a Vault, never in literals

- Status: Proposed
- Date: 2026-08-19

## Context

Connectors and flows need credentials (webhook signing keys, API tokens, DB
passwords). Two hard constraints collide with the platform's design: the
business surface makes literals editable in the dashboard and versionable in
git (ADR-0005) — so a secret must **never** be a literal — and the runtime is
all-Rust, single-process (ADR-0009), so the secret path must be in-process, not
a sidecar.

## Decision (proposed)

- A **`SecretStore` trait**, **HashiCorp Vault by default**, with alternate
  backends (encrypted-file for dev, cloud KMS later). The backend is
  configuration, not a code dependency.
- Secrets are referenced, never inlined: a `secret("path/to/key")` builtin in
  VejasScript resolves at run time; the value is used by the flow and **never
  written to the file or shown in the panel**. A connector manifest declares its
  secret references the same way.
- The panel shows *which* secrets a flow/connector references and whether they
  resolve — never their values.

## Consequences

- "The whole script is editable and versionable" stays true without leaking
  credentials into git or the dashboard.
- Rotation is a Vault concern, transparent to flows.
- **Cost:** a resolution + caching layer with careful failure semantics (a
  missing secret must fail closed, loudly); Vault becomes an operational
  dependency for deployments that use it (the dev backend avoids that locally).
- **Open questions:** caching/TTL and revocation; per-package scoping of secret
  paths (a package should not read another's secrets — mirrors ADR-0004's
  private-by-default stance); audit of secret access.

## Alternatives considered

- **Env vars only** (today's stopgap): fine for one Slack webhook, doesn't
  scale to many connectors, no rotation, leaks into process listings.
- **Secrets as encrypted literals in the file:** keeps one-file simplicity but
  puts ciphertext in git and couples key management to the repo — rejected.
- **A secrets sidecar/agent:** reintroduces a second process against ADR-0009.
