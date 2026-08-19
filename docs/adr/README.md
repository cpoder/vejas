# Architecture Decision Records

Each ADR captures one decision: its context, the decision, its status, and its
consequences. Format after Michael Nygard. New decisions get the next number
and never rewrite history — a reversed decision is a new ADR that supersedes
the old one.

**Status legend:** `Accepted` (decided & implemented) · `Accepted (partial)`
(decided, partly built) · `Proposed` (decided direction, not yet built) ·
`Superseded by NNNN`.

| # | Title | Status |
|---|---|---|
| [0001](0001-vejascript-native-language.md) | VejasScript as the native flow language | Accepted |
| [0002](0002-nats-only-infrastructure.md) | NATS JetStream as the only infrastructure dependency | Accepted |
| [0003](0003-webmethods-packages.md) | webMethods-style packages | Accepted |
| [0004](0004-service-composition-exports.md) | Service composition & EXPORTS visibility | Accepted |
| [0005](0005-business-surface.md) | Business surface: literals, corrected in place | Accepted |
| [0006](0006-runtime-is-mcp-server.md) | The runtime is its own MCP server; flow-as-tool | Accepted |
| [0007](0007-connectors.md) | Connectors: native bundled + bus contract + SDK | Accepted (partial) |
| [0008](0008-secrets-vault.md) | Secrets via a Vault, never in literals | Proposed |
| [0009](0009-all-rust-no-python.md) | All-Rust runtime, no Python | Accepted |
| [0010](0010-transformation-doctrine.md) | Transformation doctrine: small registry + code-by-example | Accepted |

## Template

```markdown
# NNNN — Title

- Status: Proposed | Accepted | Accepted (partial) | Superseded by NNNN
- Date: YYYY-MM-DD

## Context
What forces are at play? What problem or tension prompts a decision?

## Decision
What we decided, stated plainly.

## Consequences
What becomes easier, harder, or constrained. Include the negative ones.

## Alternatives considered
What else was on the table and why it lost.
```
