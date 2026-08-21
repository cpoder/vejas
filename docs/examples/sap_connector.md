# Connecting to SAP (native Rust, no JVM)

> **Superseded direction.** An earlier version of this note recommended keeping
> SAP JCo (Java) and wrapping it as an exec connector. **ADR-0014** replaces that:
> the SAP connector is a **native Rust binary over the SAP NW RFC SDK
> (`libsapnwrfc`)** — no JVM, no Python — run as an isolated exec process. This
> note now describes that connector, which is built and validated against a live
> SAP NetWeaver AS ABAP system (`connectors/sap-rfc/`).

## Shape

`vejas-sap-rfc` is a small Rust binary that **`dlopen`s `libsapnwrfc.so` at
runtime**. Consequences:

- **No JVM, no Python** — aligns with ADR-0009.
- **No build-time SAP dependency** — it builds anywhere; only *running* needs the
  SDK, which ships with the SAP kernel (`/usr/sap/<SID>/SYS/exe/.../libsapnwrfc.so`)
  and is present on every SAP host. Build once, run beside any SAP.
- **Isolated exec process** (ADR-0011) — the FFI lives in this binary, never in
  the Vejas runtime; a native crash restarts only the connector.
- **Secrets via `secret()`** (ADR-0008), handed to the process environment, never
  argv.

It speaks two shapes, both over stdio JSON (one object per line):

### 1. Request/reply (client) — read & call

Default mode is a request/reply loop: a JSON request per line on stdin, a JSON
reply per line on stdout.

- `{"op":"ping"}` — RfcPing.
- `{"op":"describe","func":"BAPI_USER_GETLIST"}` — interface metadata
  (name / direction / type / length), from `RfcGetParameterDescByIndex`.
- `{"op":"list","pattern":"BAPI_*"}` — function-module search
  (via the ABAP `RFC_FUNCTION_SEARCH`).
- `{"op":"call","func":"RFC_READ_TABLE","import":{"QUERY_TABLE":"T000"},"max_rows":50}`
  — invoke any function module. `call` **auto-marshals from metadata**: every
  EXPORT/CHANGING scalar & structure and every TABLES parameter comes back
  without the caller knowing types. Inputs may be scalars, structures (JSON
  object) or tables (JSON array of rows).

This is the path a future `exec-rpc` driver exposes as MCP tools
(`sap_list` / `sap_describe` / `sap_call`).

### 2. Streaming (server) — IDoc inbound

`vejas-sap-rfc idoc-server` **registers at the SAP gateway** as a server program
(`RfcRegisterServer`) and enters `RfcListenAndDispatch`. Every call SAP makes to
it is marshalled from metadata and emitted as one JSON line on stdout. For
`IDOC_INBOUND_ASYNCHRONOUS`, the IDoc control (EDI_DC40) and data (EDI_DD40)
records ride in the `tables`. This is the long-running **exec-stream-source**
shape, so a Vejas manifest publishes each line to the bus — see
`sap_idoc.vjs.example`.

```
driver "exec-stream-source"
SUBJECT = "vx.sap.idoc"
CMD = "/opt/vejas/vejas-sap-rfc idoc-server"
ENV = {LD_LIBRARY_PATH: "…/exe", SAP_ASHOST: "…", SAP_USER: "…",
       SAP_PASSWD: secret("sap/…"), SAP_PROGRAM_ID: "VEJAS_IDOC",
       SAP_GWHOST: "…", SAP_GWSERV: "sapgw00"}
```

SAP side (once, by a Basis admin): an RFC destination (SM59, "Registered Server
Program", Program ID = `SAP_PROGRAM_ID`), a port over it (WE21), and a partner
profile (WE20) routing the message type. Then outbound IDocs land on `vx.sap.idoc`.

## Why not a native `.so` loaded into the runtime

The reasoning ADR-0011 gives still holds — and the connector honours it: the
vendor C library is `dlopen`ed **inside the connector binary**, a separate
process, never inside the runtime. So we get the SDK's full capability (all ABAP
types, codepages, IDocs, DDIC introspection) with process isolation and no
unstable-ABI FFI boundary crossing into the runtime.

## Why not reverse-engineer the RFC protocol

Rejected in ADR-0014: weeks of high-risk work to remove a dependency that already
ships on every SAP host and that our Rust FFI drives in an evening. OData/SOAP
over Gateway stays a possible accelerator where exposed, but not the primary path
— the large installed base is classic NetWeaver AS ABAP where only the RFC
gateway is guaranteed.

## Validated

Against a live SAP NetWeaver AS ABAP (NPL, kernel 7.53): `ping`; `describe`;
`list`; `call` reading `RFC_READ_TABLE`/`RFC_SYSTEM_INFO` (scalars, structures,
tables) and running input-table FMs; and a real `IDOC_INBOUND_ASYNCHRONOUS`
received by the registered server and published end-to-end onto NATS via
exec-stream-source.
