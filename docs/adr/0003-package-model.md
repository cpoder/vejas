# 0003 — The package model

- Status: Accepted
- Date: 2026-08-19

## Context

A platform must let you start small and grow without turning into a monolith.
Users need a unit to group related flows and services, to enable/disable
together, to hot-add, and (later) to distribute and set rights on. The
The classic integration-server package model is a proven answer to exactly this.

## Decision

Adopt **packages**: a directory `packages/<pkg>/` containing a literal manifest
`package.vjs` (`ENABLED`, `EXPORTS = […]`), plus `flows/`, `services/`,
`fixtures/`. The `flows/` and `services/` at the repo root are the implicit
`default` package. Packages are **hot-addable**: drop the directory, `POST
/reload`. Processes are namespaced (`flow:notifications:ops_heartbeat`).

## Consequences

- Start with one flow in `flows/`; grow into packages when it helps. No upfront
  structure tax.
- A package is a natural **git-distributable unit** — hot-add is a `git clone`
  into `packages/`, versions are tags. This is the seam a future connector /
  package marketplace grows from.
- The manifest is a VejasScript literal, so it is edited with the same
  surgical `set_literal` mechanism as any other business surface (e.g. flipping
  `ENABLED`, editing `EXPORTS`).
- **Cost:** naming, resolution, and rights now have a package dimension
  (addressed in ADR-0004). Manifest-declared runtime for connectors is
  _(planned)_.

## Alternatives considered

- **Flat repo, no grouping:** simplest, but no unit for enable/disable,
  distribution, or rights — doesn't scale past a handful of flows.
- **Container-per-package (Airbyte-style isolation):** strong isolation but
  reintroduces an image orchestrator, contradicting ADR-0002's lightness.
- **A package registry with semver up front:** premature; git tags cover it
  until adoption justifies a registry.
