# 0021 — Versioning, canary, and time-travel

- Status: Accepted
- Date: 2026-08-23

## Context

ADR-0020 deliberately left one thing to this ADR: **cluster-wide live promotion**.
In a cluster, a live business-surface edit is refused (it would split-brain one
instance's files against the others), so promotes go through GitOps. That is a
safe *intermediate*, not the destination — an enterprise panel whose "Promote"
button only works on a single instance has lost its reason to exist. This ADR
must make a live promote fan out to every instance at once. That is the first
problem, and it is the hinge the rest hangs on.

The rest is the uniqueness bet. ADR-0018 already lets you preview a *single
literal* change against recent real traffic (shadow-replay) and roll it back. The
natural, and hard-to-copy, generalisation is to do the same for a **whole
version** of a flow:

- **Time-travel:** replay a *historical* window of the bus through a candidate
  version and diff its emissions against what the live version produced — before
  a single real message touches it.
- **Canary:** run a candidate version on *live* traffic in shadow (no real side
  effects) and watch the diff accumulate, then promote when it looks right.
- **Promote / rollback:** make the candidate the live version everywhere,
  atomically, audited — and step back to any previous version the same way.

This is ADR-0005's business-surface loop lifted from *literals* to *versions*, and
it only becomes possible now that clustering (ADR-0020) has defined how instances
coordinate through the bus.

## Decision (proposed)

### A version is a content-addressed snapshot on the bus

A **version** of a flow is a snapshot of **that flow's** VejasScript source,
content-addressed by a hash. Versions live in a JetStream KV bucket
`VEJAS_VERSIONS`, keyed by flow, newest value = the live version, history
retained (bounded, see Storage below) for rollback and audit. Git stays the
durable source of truth for *deployed* versions (a commit is a version); the
version bucket is the **live overlay** for changes made through the panel/MCP
between deploys — the same git-plus-stream duality as the ADR-0018 audit trail,
now carrying content, not just a record of it.

**Scope, v1: the overlay carries only the flow, not the services it invokes.**
Invoked services stay baseline-only (a service edit is GitOps). Snapshotting the
service graph opens shared-service-version questions — two flows pinning two
versions of the same service is hermetic but diverges from today's semantics — so
hermetic per-version service pinning is an explicit follow-up, not v1.

### Cluster-wide promotion = every instance watches the version bucket

This solves ADR-0020's delegated problem. Each instance **watches**
`VEJAS_VERSIONS` (JetStream KV `watch`). A promote publishes the new snapshot to
the bucket; every instance sees the update and hot-reloads that flow from the
snapshot — converging within the watch latency, no per-instance file write, no
split-brain. The cluster-mode write-refusal of ADR-0020 is then lifted for the
*promote* path specifically: a promote does not write local files, it publishes a
version, which is cluster-safe by construction.

**Atomicity primitive (review point a).** The authority is the KV value itself,
not the notification. A promote is a single KV write — for a per-flow key, a
`create`/`put`; for the case where two promoters race, a **CAS `update` on the
key's revision** (the same fencing primitive as the ADR-0020 lease), so exactly
one promote wins and the loser retries against the new base. There is a single
"active version" pointer per flow (its latest KV revision); switching it is one
atomic KV operation. **An instance that misses the watch event does not diverge:**
the watch is only a notification, and every instance re-reads the key's current
value on (re)connect (and can refresh on demand), so a dropped connection or a
missed update self-heals to the KV's authoritative value. The KV is the truth;
the watch is just how fast you hear about it.

### Git ↔ overlay: an overlay is valid only against the exact baseline it forked

This is the crux (it was the open question; the review settled it). An overlay
records the **parent baseline hash** — the git file's content hash at the moment
of the promote. At reconciliation, per flow:

- **Baseline unchanged** (same parent hash) → the overlay applies. This is the
  steady state between deploys.
- **Baseline advanced** (a deploy moved the git file B1 → B2) **or the flow was
  git-deleted** → **git wins; the overlay is evicted, loudly.** An audit record is
  written and the panel shows a banner: *"your live promote on X was superseded by
  a deploy — the content is still in VEJAS_VERSIONS history, re-promotable on the
  new baseline; diff here."* The content is never lost, but it never silently
  overrides the deploy.

Never the inverse. An overlay that silently masks a deploy is a deploy that does
not deploy; a flow that lives on as an overlay after `git rm` is a ghost that
breaks "take your code with you." Both are unacceptable, and this one rule closes
both edge cases. The eviction is performed by a **single actor** — the first
instance to detect the mismatch does a CAS delete on the key (first detector wins,
one audit record, no thundering herd). And **v1 does not allow flow *creation* via
overlay**: an overlay may only carry a flow that already exists in the baseline —
creation is GitOps — because otherwise the delete semantics above become
undecidable (is an absent baseline a deletion, or a not-yet-committed creation?).

Follow-up softener (noted, not v1): when an overlay is a literal-bump (the
ADR-0005 unification below), re-applying it on the advanced baseline is usually
mechanical — `set_literal` replays cleanly if the literal still exists — so an
"auto-rebase of literal-overlays" could avoid the eviction in the common case.
v1 evicts loudly: simple and safe first, clever later.

### Time-travel and canary are one shadow engine, two traffic sources

Both reuse the ADR-0018 read-only hydration and extend it from a literal to a
whole parsed version:

**The shadow invariant (review point c), named and load-bearing:** a candidate
version's emits NEVER reach the real subjects. Time-travel and canary both run the
candidate with its `emit`/`respond` diverted — recorded for the diff, published
nowhere real. This is ADR-0005's "preview writes nothing" invariant extended from
a literal to a whole version; it is what makes running unproven code against real
traffic safe. A violation is a correctness bug, not a tuning knob.

- **Time-travel** hydrates a chosen historical window for the flow's source
  subject (bounded, read-only ephemeral consumer — ADR-0018's `hydrate_recent`,
  generalised to a time/seq range) and runs each event through *both* the live
  and the candidate version, returning the side-by-side emit diff. Nothing is
  published; the bus is untouched.
- **Canary** is time-travel on the *tail* of the live stream: a **shadow**
  ephemeral consumer follows the live source subject (deliver-new, read-only,
  never acked), and each new event is run through **both** the live and the
  candidate version in the shadow engine — exactly as time-travel does over a
  historical window. The diff is therefore exact *by construction*, and the two
  runs line up on the source message's **stream sequence** (intrinsic to
  JetStream — no per-flow join key to invent; this is what answers "how do you
  correlate?" below). We do **not** scrape the incumbent's real emits off the bus
  to diff against — those aren't tagged to their source event and the trace ring
  is per-instance in a cluster, so that correlation is fragile; re-running the
  live version in shadow is both correct and uniform. The cost is 2× shadow
  compute (bounded, acceptable); canary is then *literally* the same engine as
  time-travel, zero special case. The diff accumulates over real traffic until an
  operator promotes or discards.

**Split vs. double — decided: double-consume shadow for v1 (review point b).**
Two shapes were on the table. A *split* canary routes a real subset of traffic to
the candidate that the incumbent does *not* see (competing consumers on one
durable) — a true canary with real side effects on that subset. A *double* canary
lets the candidate consume the *same* traffic in parallel (its own ephemeral
consumer) with emits shadowed. We choose **double** for v1: it has **zero blast
radius** (the shadow invariant holds, so a broken candidate cannot corrupt a
single real downstream), it needs no traffic-splitting logic, and it composes
cleanly with at-least-once (the incumbent still processes everything; the shadow
never acks). The cost is that it does not exercise real downstream effects — which
is exactly what you do *not* want an unproven version doing. Split canary (real
impact on a measured subset) is a deliberate later option with its own risk story,
for the case that genuinely needs to test the real side effect; it is not the
default because "let unproven code emit for real" is the thing to avoid by
default.

### Promote and rollback are forward-only, audited

**Every version switch is one audit record (review point e).** Promote publishes
the candidate snapshot as the new live version in `VEJAS_VERSIONS` and writes an
ADR-0018 audit record — actor, from-hash → to-hash, ts — and so does a rollback
and a canary-promote. The trail is the full version history of the flow, not just
its literal edits. Rollback is a promote to a previously recorded version's
snapshot — forward-only, never rewriting history, reusing the same fan-out and
audit path, previewable with the same shadow engine first. This is exactly the
ADR-0018 literal-rollback shape, one level up.

**Version-tagged dead letters (review point d).** The DLQ death envelope
(ADR-0015) gains the `version` (content hash) that failed the message. So when a
message that died under v1 is replayed after a promote to v2, the operator sees
*which* version rejected it, the replay records the v1 → v2 transition, and "it
failed under the old rule, does it still fail under the new one?" has an answer in
the envelope rather than in someone's memory. A DLQ replay is, in effect, a
per-message time-travel across a version bump.

## Decided in review

1. **Granularity: per-flow for v1**, with a grouped-promote ("change set": several
   flows, one version id, promoted atomically) as a follow-up — matches the
   durable-per-flow model today.
2. **Git ↔ overlay:** resolved by the parent-baseline-hash rule above (overlay
   valid only against its exact baseline; otherwise git wins and the overlay is
   evicted loudly; no flow creation via overlay in v1).
3. **Canary diff key:** the source message's **stream sequence** — intrinsic to
   JetStream, no per-flow key to invent — because both versions run in the shadow
   engine on the same event (see Canary above).
4. **Storage bounds:** `VEJAS_VERSIONS` KV keeps a bounded history (**64 revisions
   per flow key**) for rollback/audit; canary shadow recordings go to a bounded
   stream (**`max_msgs` + `max_age`, DLQ-style**) so a long-running canary cannot
   grow without limit. Both are safety caps, logged when they bite, never silent.

## Consequences

- The enterprise "Promote" button works in a cluster again — the whole reason the
  panel exists is restored, and ADR-0020's GitOps-only state becomes the safe
  fallback, not the ceiling.
- Preview stops being a single-literal trick: an operator can see a whole version
  change against historical *and* live traffic before it ships, and step back if
  it is wrong. That is the hard-to-copy capability the manifesto's counter-bet
  needs — not "we have connectors" but "you can change the meaning safely."
- New bus objects (`VEJAS_VERSIONS` KV, canary shadow recordings) on their own
  sibling roots, consistent with the DLQ/audit/lease pattern — no new dependency.
- The runtime gains a notion of "the live version is not necessarily the file on
  disk." That is a real conceptual cost, mitigated by the git-baseline rule (disk
  is always the baseline; the overlay is explicit and audited) and by keeping it
  off entirely when `VEJAS_VERSIONS` is unused (single-instance, GitOps-only).
- **ADR-0020 guard message updates when this ships.** Today the cluster-mode
  write-refusal says "promote through git." Once the version path exists, that
  message must point to it too — "promote through the panel (versions) or git" —
  and `/surface/set` in a cluster routes to a version promote instead of a flat
  refusal. Until then the refusal stands; the docs must not claim the version
  path before it is built.

## Interactions

- **ADR-0018 (shadow-replay + audit):** this is its generalisation — the same
  read-only hydration, the same forward-only audited promote/rollback, lifted from
  a literal to a version.
- **ADR-0020 (clustering):** this discharges the cluster-wide-promotion debt 0020
  recorded; the version watch is how instances converge, the same "bus is the
  coordination" principle as the singleton lease.
- **ADR-0005 (business surface):** literal edits remain the fine-grained loop;
  versioning is the coarse-grained one. A literal promote can be modelled as a
  minimal version bump, unifying the two — a possible simplification to explore.

## Rejected

- **Real-traffic % sampling as the v1 canary.** Emitting real side effects from an
  unproven version is a blast radius shadow canary avoids entirely; defer it.
- **Versions as an external artifact store (S3, a registry).** A new dependency
  for something JetStream KV already does, and it would sit outside the bus that
  every other coordinated object lives on.
- **Mutable version tags.** Versions are content-addressed and immutable; "latest"
  is a pointer. Mutable tags reintroduce the split-brain this ADR exists to remove.
