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
- **Shadow runs hold no value.** Whatever shows a run instead of performing
  it (preview, `vejas_run_flow`, flow-as-tool, shadow-replay, time-travel,
  canary, golden capture) resolves `secret()` to the placeholder
  `<secret:path/key>`. It stays fail-closed: a missing secret still aborts.
  A candidate is arbitrary code, so it cannot be left to a filter on the
  output, and the value never enters the run. One consequence to know when
  reading a diff: a branch that compares with a secret takes the not-equal
  path in shadow.
