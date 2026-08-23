# Vejas

Vejas is an open-source integration platform with **no builder UI**: agents
write the integration code, humans own what it means. Flows are written in
VejasScript — a small, pure, per-event language whose business surface
(thresholds, transcoding tables, rules) is extracted from the code itself
and edited by domain experts in a panel, without touching the code.

Three design commitments shape everything:

1. **One infrastructure.** NATS/JetStream is the only dependency: transport,
   persistence, KV, locks, audit — the bus is the platform's memory and its
   scaling substrate. Two containers, one `docker compose up` (ADR-0002).
2. **Agent-native.** The runtime *is* an MCP server. An agent connects,
   reads the language contract (`vejas_language`), writes a flow, tests it
   against a fixture, and it lands running — governance optional but
   first-class: in governed mode agents can only *propose*; a human
   approves (ADR-0006, ADR-0024).
3. **Measured, not claimed.** Every number in these docs comes from a
   reproducible benchmark in [`bench/`](reference/benchmarks.md): cold start
   11 ms, 6–8 MB RSS under load, end-to-end p50 2 ms uncongested, every hop
   persisted, cluster promote in 60 ms — lossless.

## Where to go

- New here → [Install & first run](getting-started/install.md), then
  [your first flow](getting-started/first-flow.md).
- Building an API → [Expose an API — sync and async](guides/expose-an-api.md).
- Running in production → [Clustering](guides/clustering.md),
  [DLQ & replay](guides/dlq-replay.md),
  [changing safely](guides/change-safely.md),
  [governed mode](guides/governed-mode.md).
- Connecting systems → [the certified catalog](connectors/catalog.md).
- The full "why" → [the ADRs](decisions.md): 27 recorded decisions.
