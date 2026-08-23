# Install & first run

## Docker (recommended)

```bash
git clone https://github.com/cpoder/vejas && cd vejas
docker compose up          # nats + vejas — nothing else
```

The panel is at <http://localhost:8686>. The webhook entry listens on
`:8787` — `POST /ingest/<subject>` publishes the JSON body on the bus.

## From source

```bash
cargo build --release --manifest-path core/Cargo.toml
nats-server -js &          # the only dependency
VEJAS_ROOT=./my-root core/target/release/vejas-runtime
```

`VEJAS_ROOT` is a plain directory: `flows/`, `connectors/`, `tables/`,
`tests/`. Everything is a file; git is the source of truth.

## Connect an agent

```bash
claude mcp add --transport http vejas http://localhost:8686/mcp
```

Any MCP client works — the runtime is the MCP server, no separate process.
Ask the agent for a flow in plain words; it reads `vejas_language` over MCP,
writes the `.vjs`, tests it against a fixture, and the supervisor picks it
up.

## Security defaults

The panel/MCP port (8686) binds to **localhost** by default — that surface
writes flows and can run commands (exec connectors), so expose it
deliberately. Set `VEJAS_TOKEN` and every write (`/mcp` included) requires
`Authorization: Bearer`. The webhook port (8787) only publishes events.
For a "no agent lands anything alone" posture, see
[governed mode](../guides/governed-mode.md).
