# Composing services

A flow does one job end to end. When several flows need the *same* piece of
logic — format an alert, classify a ticket, shape a payload — you factor it
into a **service** and `invoke` it. Same language, same purity, no
duplication.

## A service is a flow without a trigger

A service lives in `services/<name>.vjs` (or `packages/<pkg>/services/`). It
looks like a flow but declares no `source`, `api`, or `tool` — it never runs
on its own. It receives named arguments, and produces variables. Real
example (`services/format_alert.vjs`):

```
# service: format_alert — Inputs: sev, subj, email. Output: alert_text.
alert_text = f"[{sev}] {subj} - {lower(email)}"
```

Because a service is pure VejasScript, everything true of a flow holds: its
literals are part of the [business surface](business-surface.md), it is
statically analyzable, and it has no I/O of its own.

## `invoke` — merge, or capture

A flow calls a service with named arguments, two ways:

```
# merge: the service's outputs land in THIS flow's variables
invoke format_alert(sev: code, subj: subject, email: requester.email)
emit "vx.slack.out", {text: alert_text}          # alert_text came from the service

# capture: take the service's whole pipeline as one document
d = invoke format_alert(sev: code, subj: subject, email: requester.email)
emit "vx.slack.out", {text: d.alert_text}
```

Merge is the common case — the service extends the caller's pipeline.
Capture is for when you want the result as a self-contained document (to
nest, compare, or pass on).

## Across packages: `EXPORTS`

[Packages](../reference/vejascript.md) group flows and services. A service
is **package-private by default**. To call one from another package, the
owning package must export it in its `package.vjs`:

```
# packages/notifications/package.vjs
ENABLED = true
EXPORTS = ["notify_slack"]     # callable from other packages; the rest stays private
```

Then, from another package:

```
invoke notifications:notify_slack(channel: "#ops", text: alert_text)
```

## When to compose, and when to use the bus

- **`invoke`** when it's *synchronous shared logic* inside one processing
  step — formatting, classification, a lookup shape. It runs in the caller's
  pipeline, same event, no persistence boundary.
- **The bus** (`emit` to a subject another flow `source`s) when it's a
  *separate stage* that deserves its own delivery guarantee, retry/replay, or
  independent scaling — the persisted, at-least-once path. Between packages,
  prefer the bus; `EXPORTS` is the deliberate exception for genuinely shared
  synchronous helpers.

A rule of thumb: if you'd want the step to survive a crash and replay on its
own, it's a bus hop, not an `invoke`.
