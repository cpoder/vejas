# 0008 — Secrets via a Vault, never in literals

- Status: Accepted
- Date: 2026-08-19

## Context

Connectors and flows need credentials (webhook signing keys, API tokens, DB
passwords). Two hard constraints collide with the platform's design: the
business surface makes literals editable in the dashboard and versionable in
git (ADR-0005) — so a secret must **never** be a literal — and the runtime is
all-Rust, single-process (ADR-0009), so the secret path must be in-process, not
a sidecar.

## Decision (built)

- A **`SecretStore` trait** (`core/src/secrets.rs`): **VaultStore** (HashiCorp
  Vault KV v2, when `VAULT_ADDR` is set) and **EnvStore** (dev default:
  `secret("a/b")` → env `VEJAS_SECRET_A_B`). The backend is chosen at startup,
  not a code dependency; cloud KMS is a future backend.
- Secrets are referenced, never inlined: the `secret("path/key")` builtin
  resolves at run time (fail-closed: a missing secret aborts the run). The
  value is used by the flow/connector but is **never a literal**, so it never
  enters the business surface, the file, or the panel. A connector manifest
  resolves the same way: its config is produced by **evaluating** the manifest,
  so `WEBHOOK_URL = secret("slack/webhook")` yields the real value into the
  driver's config while the manifest file holds only the reference.
- Static `NAME = secret("…")` references are captured (`Program.secret_refs`)
  and surfaced via `/graph` and the `vejas_secrets` MCP tool — references only,
  never values.

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
