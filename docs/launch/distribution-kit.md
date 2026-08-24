# Distribution kit — v0.1.0 launch

Positioning: **the agent writes, under provable governance.** Honest about the
AI-authored code — it's the thesis, not a caveat to hide. Lead with the 5-minute
result; numbers as proof, not as hook. Reglyze/NIS2 is the real production proof —
use it.

Channels deliberately skipped:
- **r/rust** — an AI-co-authored repo is a dealbreaker there.
- **lobste.rs** — invite-only, no account (and no one to get an invite from).

---

## Show HN — URL post + first comment

A Show HN submission is **either a URL or text, never both** (HN shows the text
only when the URL field is blank). The modern, higher-converting form: post the
**URL**, then post the description as the **first comment**, yourself, immediately
after submitting. A wall of text with no clickable destination gets skipped.

**Submission**
- **Title (A, recommended):**
  `Show HN: Vejas – an integration platform with no builder UI; agents write the flows`
- **URL:** `https://github.com/cpoder/vejas`

Why the repo and not vejas.dev: the HN audience is technical and wary of a
marketing landing; for a "the code is AI-authored, here's the proof" thesis,
landing on the code is the coherent move. The README already carries the hero —
the demo gif, the measured numbers, the honest AI-authored line, and links to
vejas.dev + the docs — so vejas.dev is one click away. (Switch the URL to
`https://vejas.dev` if you decide the landing's demo lands harder at first glance;
if so, make the GitHub button on the landing unmissable.)

Title alternates (kept for reference):
- **B:** `Show HN: Vejas – agents write your integrations, a human approves what they mean`
- **C:** `Show HN: An integration platform where the agent writes and a human governs (Rust/NATS)`

**First comment — post immediately, seeds the discussion:**
> Vejas is an open-source integration platform with no builder UI. You describe an
> integration to your coding agent; it reads the language over MCP, writes the flow
> as readable code, tests it against a fixture, and it lands running. A domain
> expert then corrects the *meaning* — thresholds, mappings, rules — in a panel,
> without touching code.
>
> The bet: the agent owns *how*, the human owns *what it means*. In "governed mode"
> the agent can only *propose*; a human approves with a separate credential, with
> the evidence (a diff over real traffic) next to the button. Every change is
> audited (per-user attribution comes with the enterprise auth layer; the OSS core
> records the change with its actor).
>
> One Rust binary on NATS/JetStream, no other dependency. Measured, reproducible in
> bench/: cold start 11–13ms, 6–8MB RSS, end-to-end p50 2ms uncongested, every hop
> persisted, cluster promote 60ms lossless. Twenty-one connector recipes admitted
> by CI (MQTT/RabbitMQ against real brokers; IBM MQ, Kafka).
>
> Early — two production customers so far — but real: a compliance tool collects
> NIS2 supplier-compliance evidence across eight EU countries on it today.
>
> Full disclosure: most of this code was written by AI agents, reviewed and
> arbitrated by me across ~30 recorded decisions (ADRs). That's not a caveat — it's
> the thesis. The platform is built the way it says software should be built.
>
> Docs: https://vejas.dev/docs · a 45-second demo (which is also a green e2e test): https://vejas.dev/#demo
>
> Happy to answer anything — especially the sharp questions.

The demo is live at **https://vejas.dev/#demo** (45s, embedded on the landing) and in
the repo README (`docs/demo/vejas-bridge-film.mp4`).

Follow-up answers to have ready (post them yourself into the sub-threads if the
questions come):
- **Why no builder UI?** Builder UIs rot; code + an editable business surface is
  honest (ADR-0005 / ADR-0019).
- **How is this not just n8n?** Self-hosted, event-persisted every hop, enterprise
  brokers first-class, governance by construction, 8MB vs 1.2GB.
- **The AI-authored angle** — own it head-on.

---

## NATS community (Slack / forum)

The friendliest audience — they'll appreciate the dogfooding. Angle: *"a platform
that uses JetStream as its entire substrate — transport, persistence, KV, leases,
audit."* Show the clustering + the 60ms lossless promote.

## This Week in Rust (TWiR) — "Call for participation" / project spotlight

Short, factual, submitted via their PR process: one Rust binary, hand-rolled sync
clients (MQTT, MQI over FFI), rustls with no OpenSSL, measured footprint. TWiR is an
announcement, not a discussion thread, so the AI-authored angle is far less exposed
here than it would be on lobste.rs / r/rust — state it plainly and move on.

## awesome-mcp-servers (and awesome-nats)

A one-line PR each: *"Vejas — an integration platform that IS an MCP server; agents
write and test flows over MCP."* Factual, no hype.

---

## Timing

Tuesday, ~6–9am US Eastern (12–15h Paris). One shot on HN; if it sinks without a
trace, the second-chance pool (email hn@ycombinator.com) is the documented retry.
The other channels are not time-critical — space them across the week so the launch
is a *regimen*, not a one-day spike (guardrail R1).

## The honesty line (why it's a strength here)

Every number links to a script. The demo is a test. The code is AI-authored and
says so. In 2026, "measured, not claimed" + "built the way it preaches" is the
differentiator against a category full of claims.
