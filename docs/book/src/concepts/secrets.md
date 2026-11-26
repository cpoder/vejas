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
  generation contract embeds it. A key is read with camelCase as
  snake_case, so `accessToken` is masked as `ACCESS_TOKEN` is. The pattern
  was chosen on a labeled benchmark of 98 names, 43 of them camelCase, and a
  CI test pins the exact profile: changing it reopens the decision with
  data, never silently.
- **What the runtime shows is masked as the trace record is.** The events
  in Activity and `/events` (a hop's preview, a sink's answer, an error),
  dead letters, a unit's last error, the events a shadow replay, a time
  travel or a canary replays, a proposal's evidence and a connector test's
  detail leave the runtime with credential-named keys and pairs masked and
  every value a secret store handed out taken out, for the panel and for
  agents alike. The panel masks what it shows on top of that.
- **Pairs.** In a trace record and in what the runtime shows, a value whose
  sibling names a credential is masked too: `{"name": "Authorization", "value": "…"}` in a header list,
  `{"name": "DB_PASSWORD", "value": "…"}` in an environment list,
  `{"Key": "clientSecret", "Value": "…"}` in tags.
- **Standalone binaries** follow the deployment's own machinery (env from a
  secret store, CCDT/TLS keystores) — their recipes are linted the same:
  a credential-shaped env key must be a `${VAR:?}` reference, never a
  value.
- Secret **paths** are listed (`GET /secrets`); values are never returned.
- **Shadow runs hold no value.** Whatever shows a run instead of performing
  it (preview, `vejas_run_flow`, flow-as-tool, shadow-replay, time-travel,
  canary, golden capture) resolves `secret()` to the placeholder
  `<secret:path/key>`. It stays fail-closed: a missing secret still aborts.
  The real events a shadow replay, time travel, the canary or a golden
  capture reads are masked first: a credential-named field or a pair reads
  `<masked>`, a value a store handed out `<secret>`. Flags, nulls and
  numbers not named as a credential stay as they are, so a branch on them
  goes as it goes live. A result names in
  `hidden` the fields its code reads that the run did not see, and the
  panel says so under its verdict: a rule on one of them may go another way
  live. A candidate is
  arbitrary code, so it cannot be left to a filter on the output, and the
  value never enters the run. One consequence to know when
  reading a diff: a branch that compares with a secret takes the not-equal
  path in shadow.
