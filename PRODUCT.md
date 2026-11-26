# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Users

Two people use the panel, for two different jobs (docs/VISION.md, "Who does what"):

- **The domain expert.** Not a developer. Validates and corrects the business meaning of a flow: transcoding tables, thresholds, labels, which events go where. Reads a flow to answer "what does it do with my data?", changes one value, checks the effect on real events, makes it live.
- **The operator.** Watches the runtime: are events going through, what failed, where did this one event go. Follows an event end to end, handles dead letters, approves the changes agents propose, keeps the secrets and the connectors working.

A coding agent writes and rewrites the flows and drives the whole platform over MCP; it does not read the panel, but what it proposes lands in the panel for a human to approve.

## Product Purpose

Show what each integration flow does, in terms a non-developer reads, and let a human change its meaning safely: a change is replayed on the last real events before it goes live. Prove what happened to every event, from the source that let it in to the system that received it.

Success: a domain expert understands a flow at first sight and corrects a value without help; an operator finds what failed and why in under a minute.

## Positioning

No builder UI. The flow is plain code (VejasScript) in git, written by an agent; the panel is a projection derived from that code, never a second model of it. The human owns what the flow means, the agent owns how it is written.

## Operating Context

- The panel ships inside the runtime binary: one HTML file, served on the runtime's HTTP port, no build step, no external assets or CDN.
- Live data: events, dead letters and proposals refresh every few seconds.
- Used in two situations: a business review (calm, one flow at a time) and an incident (fast, one event at a time).
- Light and dark, following the system setting.

## Capabilities and Constraints

- Flows, services, packages and connectors are discovered from the code; the panel draws the map from it.
- Editable by a human: the business surface (UPPERCASE literals: tables, constants, thresholds), a flow's sample event, the whole script for those who want it, secrets (write-only), connector settings.
- Never editable in the panel: a flow's structure (conditions, loops); that goes through the agent.
- Governed mode: agents propose, a human approves with a separate token.
- Terminology the UI uses: flow, service, connector, subject, event, sample event, business value, dead letter, test.
- UI language: English.

## Brand Commitments

- Name: Vejas (the Baltic god of the wind: "wind moves things without anyone drawing the route").
- The mark: three wind lines (core/src/panel.html, at the top of the menu).

## Evidence on Hand

- Demo content shipped with the repo: flows/, services/, packages/notifications, connectors/ (order sync, invoice payment alerts, helpdesk ticket alerts, ops heartbeat).
- Screenshots: docs/screenshots/.

## Product Principles

1. The code is the truth: every screen is derived from it, nothing is drawn by hand.
2. The human owns the meaning: business values show where they act, editable in place, checked on real events before they go live.
3. Every event can be followed end to end.
4. Light: one file, no dependencies, fast on a small machine.
5. One main thing per view; details one click away.

## Accessibility & Inclusion

Low cognitive load is a requirement, not a polish: one main piece of information per view, visible structure, plain labels, details on demand. WCAG AA contrast, full keyboard use.
