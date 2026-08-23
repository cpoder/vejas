# 0029 — The failure-mode guardrails

- Status: Accepted (founder-directed, 2026-08-23; cross-review amendments
  welcome)
- Date: 2026-08-23

## Context

A structured review of comparable projects that failed — self-hosted
automation platforms, open-source integration tools, and the 2024-26 agent
wave — shows they almost never died of their technology. They died of seven
recurring failure modes. This ADR converts that post-mortem corpus into
**standing rules and tripwires**, so the project checks itself against the
graveyard instead of rediscovering it. The canonical cases per mode:
Huginn/Beehive (launch spike, no follow-through), RethinkDB and the
open-source ESBs (optimized for what developers praise, not what buyers
buy), Yahoo Pipes/StackStorm/Parse/Singer (orphaned by a steward),
Node-RED/Docker vs Redis/HashiCorp (the two ways to miss the open-core
boundary), bip.io and half the OSS graveyard (solo bus factor), Adept and
the post-2015 Zapier challengers (too early / too late), Builder.ai and
the 2025 MCP security scares (broken trust).

## Decision

Seven standing rules, each with a measurable tripwire. They are reviewed
at four gates: **pre-distribution**, **launch+30 days**, **+6 months**,
**+12 months** — the "graveyard review", a checklist pass over this table.

### R1 — Distribution is a regimen, not an event *(vs the spike-and-die)*

No public launch without a written post-launch cadence budget (hours/week
for six months, with the agent-handled share explicit).
**Tripwire:** a third-party issue unanswered for 7 days.

### R2 — Lead with the five-minute result *(vs the RethinkDB trap)*

Every public surface (README, landing, talks, launch posts) opens with
what the user gets in five minutes — say it, it runs, the expert corrects
it. The measured numbers are the **proof**, never the **hook**: rigor is
the moat, ease is the message.
**Tripwire:** any revision of a public surface that opens with benchmarks
or architecture instead of the result.

### R3 — No unresolved stewardship or legal dependency *(vs orphaning)*

Before distribution, every dependency that could orphan the project is
resolved **in writing**: personal/legal encumbrances of the founder,
licence and governance posture of load-bearing dependencies (NATS, the MCP
spec). Watch them quarterly afterward.
**Tripwire:** a licence, governance or stewardship change announcement in
a load-bearing dependency → assessed within two weeks, in an ADR if
consequential.

### R4 — The boundary holds in both directions *(vs open-core failure)*

ADR-0028 is the law: nothing shipped moves back, never a paid connector,
the core licence never changes — and the enterprise tier must exist
**early and sellable**, not late and polished (the Docker lesson: adoption
without something to sell cannot be monetized retroactively).
**Tripwire:** any feature request whose honest classification contradicts
ADR-0028 → answered by citing it, never by silent exception.

### R5 — Survivable by construction *(vs the solo bus factor)*

The project must survive a pause: ADRs, docs, CI and benches stay complete
enough that any competent successor (human or agent) can resume from the
repo alone. The split between what agents handle autonomously (issue
triage, recipe PRs, docs) and what requires the founder's voice is written
down before launch.
**Tripwire:** 30 days without activity while third-party issues are open →
a public status note; silence is the failure, not the pause.

### R6 — The window is reviewed, not assumed *(vs timing death)*

Quarterly: what did the funded players ship toward governed agent-written
integration? The positioning is re-derived from that review, not from the
original thesis.
**Tripwire:** a major player ships governed agent-authored changes →
positioning reassessed within two weeks.

### R7 — Trust is pre-earned *(vs the broken-trust death)*

An external security review of the write-capable surfaces (panel, /mcp,
exec drivers) before distribution. Every public claim stays reproducible
(the existing house rule). Security reports get a public response within
48 hours.
**Tripwire:** any CVE-class report → 48h acknowledged, publicly.

## Consequences

- The graveyard review joins the release process: four dated gates, one
  table, pass/fail per rule.
- Two rules create work before any launch: the cadence budget (R1) and
  the security review (R7).
- R2 obliges a re-read of the current README/landing opening order.

## Rejected

- **A private checklist** — a guardrail the process cannot see does not
  guard; this belongs with the other decisions.
- **CI-enforcing the editorial rules** — R2/R6 are judgment calls;
  automating them would fake precision. The tripwires are measurable, the
  review is human.
- **Treating the post-mortem corpus as one-off analysis** — the modes
  recur; the review must too.
