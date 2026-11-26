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

## `vejas_vpl_check`

Check a detect unit (a VPL program under detects/, run by the embedded Varpulis engine — ADR-0031): the engine's own verdict, the same refusal a running unit would give. Returns {ok} or {ok:false, error}.

## `vejas_read`

Read a script file (.vjs) or fixture (.json).

## `vejas_write_flow`

Create or overwrite a .vjs script (parse-validated, hot-reloaded) or a .json fixture. path under flows/, connectors/, or packages/`<pkg>`/flows|services|fixtures.

## `vejas_set_literal`

Rewrite one literal of the business surface in place (constant, or a table/mapping entry via key).

## `vejas_rollback_literal`

Roll a business-surface literal back to the value it held before its most recent promote (from the VEJAS_AUDIT trail). Rollback is itself an audited promote to the recorded previous value — forward-only, hot-reloaded, previewable first with vejas_replay_literal. Returns {restored, was, rolled_back_promote_ts}.

## `vejas_time_travel`

Time-travel (ADR-0021): replay a window of REAL persisted traffic through a whole CANDIDATE version of a flow and diff its emissions against the current effective version, joined by stream sequence. Read-only, the bus untouched, a candidate's emits never reach a real subject. Use to preview an arbitrary rewrite (not just one literal) before promoting it. Returns {events, changed, results:[{seq, before, after, changed}]}.

## `vejas_canary_start`

Start a canary (ADR-0021): shadow-follow a flow's LIVE traffic and diff a candidate version against the current effective version as events arrive, accumulating a diff. Read-only (shadow — no real emit). Refuses if a canary is already running for the flow. Auto-stops if the live version changes under it (reason in status).

## `vejas_canary_status`

Read a canary's accumulating diff: {running, events, changed, stop_reason, results:[{seq, before, after, changed}]}.

## `vejas_canary_stop`

Stop a running canary for a flow (its shadow consumer exits; the last diff stays readable).

## `vejas_propose`

Submit a governed change PROPOSAL for a human to approve in the panel (ADR-0024) — you can propose but never approve. kind='set_literal' (payload {file, name, key, value}) or kind='version' (payload {file, candidate: whole source}). Attach `evidence` you gathered (vejas_time_travel results, vejas_canary stats) — the panel shows it next to Approve, and flags 'no evidence' loudly. The proposal is pinned to the current baseline and auto-expires if a deploy/promote moves it. Returns the stored proposal (id, status:pending).

## `vejas_proposals`

List the proposal queue with status (pending/approved/rejected/expired) and evidence. Read-only. Approve/reject are human panel actions, not tools.

## `vejas_replay_literal`

Shadow-replay a proposed literal change against REAL persisted traffic: hydrate the flow's recent events from JetStream (read-only, the bus untouched — falls back to the in-memory trace ring when the stream is empty or the flow has no bus source), rerun them against the current AND the patched script, and return the before/after emit diff (with `source`: jetstream|trace-ring). Nothing is written — promote with vejas_set_literal.

## `vejas_preview`

Run a flow on its fixture and return the emitted messages plus the final pipeline.

## `vejas_run_flow`

Run any flow on a supplied input event and return its emits (does not touch the bus).

## `vejas_events`

The most recent events processed by the flows — subject, ok/error, emitted subjects, payload preview — newest first. Optional filter: flow (e.g. "flow:stripe_alerts").

## `vejas_reload`

Rescan flows and packages; start new, stop removed, restart changed.

## `vejas_drivers`

List the available connector drivers (name, kind, description) for writing connector manifests.

## `vejas_secrets`

The secret references declared by flows and connectors, who uses each, and whether it RESOLVES against the store — references and statuses only, never values.

## `vejas_set_secret`

Write one secret value into the store (rotation included) and restart the units that reference it. WRITE-ONLY: no surface ever returns the value.

## `vejas_test_connector`

Synchronously test one connector instance: evaluate its manifest with the real secrets, reach the remote side with the driver's probe, touch nothing. Returns {ok, detail} in plain words.

## `vejas_provision`

Instantiate a tenant package from a template (templates/`<name>`/, ${param} substitution, every file parse-checked, hot-started). Returns created files, started units and the secret references left to write. Refuses an existing package unless force (which overwrites template-rendered files).

## `vejas_trace`

What happened to one business event, end to end, from the trace record (ADR-0034). With `trace_id` (32 hex digits, as /events, vejas_events and every dead letter give it): every hop the event went through, in the order they started: unit, subject, delivery attempt, flow version, the subjects it emitted, the payload it received (credential-named keys masked, vault values taken out), the statements each flow ran with their line of code, a sink's downstream answer, the error and whether it was dead-lettered. With `search` instead (an order number, a customer id, any text): the latest traces whose payloads, subjects, units or errors mention it, newest first; read one of them by its trace_id. Answer from the record, not from a guess.

## `vejas_dlq`

List dead letters — poison messages parked in the DLQ (ADR-0015) instead of dropped: unit, original subject, attempts, last error, payload, each with a `seq` for replay/purge. Newest first, at most 200; a payload past 16 KB is cut there.

## `vejas_dlq_replay`

Replay dead letters — re-inject each to its ORIGINAL subject, for the unit it died in alone (no other consumer of the subject runs it), so the (now corrected) flow reprocesses it, then remove it from the DLQ. A letter that no unit would take (its unit removed, or reading another subject now) stays, and `errors` says why. Target one by `seq`, a whole `unit`, or all (omit both). Do this AFTER fixing the cause (vejas_set_literal, previewed with vejas_replay_literal). Refused in governed mode: a human replays them in the panel, after approving the fix.

## `vejas_dlq_purge`

Discard dead letters without replaying — by `seq`, by `unit`, or all (omit both). Refused in governed mode: a human discards them in the panel.

## `vejas_new_flow`

Ask the agent to write a new VejasScript flow from a natural-language request; it lands running.

## `vejas_new_connector`

Ask the agent to write a new connector manifest from a natural-language request (picks a driver, writes config, uses secret() for credentials); it lands running.

## `sap_list`

List SAP function modules (BAPIs/RFCs) on the live system whose name matches a pattern (SAP wildcards, e.g. "BAPI_USER*"). Needs an exec-rpc SAP connector running (driver "exec-rpc").

## `sap_describe`

Describe a SAP function module's interface: every parameter's name, direction (import/export/changing/tables), type and length. Read this before sap_call.

## `sap_call`

Call a SAP function module (BAPI/RFC) on the live system and return its outputs. `import` values may be scalars, structures (a JSON object) or tables (an array of row objects); every EXPORT/CHANGING scalar & structure and every TABLES parameter comes back auto-marshalled from metadata. `max_rows` caps table output. E.g. func="RFC_READ_TABLE", import={"QUERY_TABLE":"T000"}.

## `sap_send_idoc`

Send an IDoc INTO SAP via transactional RFC (exactly-once). `control` is the EDI_DC40 control record (e.g. {TABNAM:"EDI_DC40", IDOCTYP:"MATMAS05", MESTYP:"MATMAS", SNDPRN, SNDPRT:"LS", RCVPRN, RCVPRT:"LS", DIRECT:"2"}); `data` is the array of EDI_DD40 segments ({SEGNAM, SDATA}). Pass a stable `tid` (24 chars) derived from an idempotency key for dedup across retries; omit for a fresh one. Returns the transaction id.
