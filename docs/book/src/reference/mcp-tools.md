# MCP tools

The runtime **is** the MCP server: JSON-RPC 2.0 over `POST /mcp` (ADR-0006).
Point any MCP client at `http://<host>:8686/mcp`; when `VEJAS_TOKEN` is set,
send it as `Authorization: Bearer <token>`. The list below is generated from
the tool descriptions the agent actually sees — they are the contract.

## `vejas_topology`

List running flows and connectors with their status.

## `vejas_graph`

The pipeline graph: sources, flows, composed services, destinations, connectors.

## `vejas_surface`

The business surface of every flow: mappings, transcoding tables, constants.

## `vejas_language`

The VejasScript reference: grammar, builtins, and the rules for flow files and connector manifests. Read this before writing any .vjs.

## `vejas_read`

Read a script file (.vjs) or fixture (.json).

## `vejas_write_flow`

Create or overwrite a .vjs script (parse-validated, hot-reloaded) or a .json fixture. path under flows/, connectors/, or packages/`<pkg>`/flows|services|fixtures.

## `vejas_set_literal`

Rewrite one literal of the business surface in place (constant, or a table/mapping entry via key).

## `vejas_rollback_literal`

Roll a business-surface literal back to the value it held before its most recent promote (from the VEJAS_AUDIT trail). Rollback is itself an audited promote to the recorded previous value â forward-only, hot-reloaded, previewable first with vejas_replay_literal. Returns {restored, was, rolled_back_promote_ts}.

## `vejas_time_travel`

Time-travel (ADR-0021): replay a window of REAL persisted traffic through a whole CANDIDATE version of a flow and diff its emissions against the current effective version, joined by stream sequence. Read-only, the bus untouched, a candidate's emits never reach a real subject. Use to preview an arbitrary rewrite (not just one literal) before promoting it. Returns {events, changed, results:[{seq, before, after, changed}]}.

## `vejas_canary_start`

Start a canary (ADR-0021): shadow-follow a flow's LIVE traffic and diff a candidate version against the current effective version as events arrive, accumulating a diff. Read-only (shadow â no real emit). Refuses if a canary is already running for the flow. Auto-stops if the live version changes under it (reason in status).

## `vejas_canary_status`

Read a canary's accumulating diff: {running, events, changed, stop_reason, results:[{seq, before, after, changed}]}.

## `vejas_canary_stop`

Stop a running canary for a flow (its shadow consumer exits; the last diff stays readable).

## `vejas_propose`

Submit a governed change PROPOSAL for a human to approve in the panel (ADR-0024) â you can propose but never approve. kind='set_literal' (payload {file, name, key, value}) or kind='version' (payload {file, candidate: whole source}). Attach `evidence` you gathered (vejas_time_travel results, vejas_canary stats) â the panel shows it next to Approve, and flags 'no evidence' loudly. The proposal is pinned to the current baseline and auto-expires if a deploy/promote moves it. Returns the stored proposal (id, status:pending).

## `vejas_proposals`

List the proposal queue with status (pending/approved/rejected/expired) and evidence. Read-only. Approve/reject are human panel actions, not tools.

## `vejas_replay_literal`

Shadow-replay a proposed literal change against REAL persisted traffic: hydrate the flow's recent events from JetStream (read-only, the bus untouched â falls back to the in-memory trace ring when the stream is empty or the flow has no bus source), rerun them against the current AND the patched script, and return the before/after emit diff (with `source`: jetstream|trace-ring). Nothing is written â promote with vejas_set_literal.

## `vejas_preview`

Run a flow on its fixture and return the emitted messages plus the final pipeline.

## `vejas_run_flow`

Run any flow on a supplied input event and return its emits (does not touch the bus).

## `vejas_events`

The most recent events processed by the flows â subject, ok/error, emitted subjects, payload preview â newest first. Optional filter: flow (e.g. "flow:stripe_alerts").

## `vejas_reload`

Rescan flows and packages; start new, stop removed, restart changed.

## `vejas_drivers`

List the available connector drivers (name, kind, description) for writing connector manifests.

## `vejas_secrets`

The secret references declared by flows and connectors, who uses each, and whether it RESOLVES against the store â references and statuses only, never values.

## `vejas_set_secret`

Write one secret value into the store (rotation included) and restart the units that reference it. WRITE-ONLY: no surface ever returns the value.

## `vejas_test_connector`

Synchronously test one connector instance: evaluate its manifest with the real secrets, reach the remote side with the driver's probe, touch nothing. Returns {ok, detail} in plain words.

## `vejas_provision`

Instantiate a tenant package from a template (templates/`<name>`/, ${param} substitution, every file parse-checked, hot-started). Returns created files, started units and the secret references left to write. Refuses an existing package unless force (which overwrites template-rendered files).

## `vejas_dlq`

List dead letters â poison messages parked in the DLQ (ADR-0015) instead of dropped: unit, original subject, attempts, last error, payload, each with a `seq` for replay/purge. Newest first.

## `vejas_dlq_replay`

Replay dead letters â re-inject each to its ORIGINAL subject so the (now corrected) flow reprocesses it, then remove it from the DLQ. Target one by `seq`, a whole `unit`, or all (omit both). Do this AFTER fixing the cause (vejas_set_literal, previewed with vejas_replay_literal).

## `vejas_dlq_purge`

Discard dead letters without replaying â by `seq`, by `unit`, or all (omit both).

## `vejas_new_flow`

Ask the agent to write a new VejasScript flow from a natural-language request; it lands running.

## `vejas_new_connector`

Ask the agent to write a new connector manifest from a natural-language request (picks a driver, writes config, uses secret() for credentials); it lands running.
