# Secrets

The rule is absolute (ADR-0008): **a credential never appears as a literal**
— not in a flow, not in a manifest, not in the panel, not in `git diff`.

`secret("path/key")` resolves at run time, fail-closed, from:

- `VEJAS_SECRET_<PATH>` environment entries (containers, CI);
- a file store (`VEJAS_SECRETS_FILE`) for dev;
- HashiCorp Vault (`VAULT_ADDR` / `VAULT_TOKEN` / `VEJAS_VAULT_MOUNT`).

What keeps the rule real, rather than aspirational:

- **One pattern, three enforcement points.** The credential-shaped-key
  pattern is a single constant in the runtime (`vejas-runtime
  secret-pattern` prints it); the panel masks with it, CI lints every
  certified recipe with it (env-file recipes included), and the agent
  generation contract embeds it. The pattern itself was chosen on a
  55-key labeled benchmark, and a CI test pins the exact profile — changing
  it reopens the decision with data, never silently.
- **Standalone binaries** follow the deployment's own machinery (env from a
  secret store, CCDT/TLS keystores) — their recipes are linted the same:
  a credential-shaped env key must be a `${VAR:?}` reference, never a
  value.
- Secret **paths** are listed (`GET /secrets`); values are never returned.
