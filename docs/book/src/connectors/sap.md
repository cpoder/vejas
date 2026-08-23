# SAP

The hard case that shaped the connector doctrine: a native protocol, a C
SDK, IDocs — none of it belongs in the core binary.

The SAP connector is a **standalone binary** (`connectors/sap-rfc`) that
loads SAP NW RFC at runtime (hand-declared FFI + `dlopen`, ADR-0014 — the
same move later reused for IBM MQ): the build needs no SAP anywhere; only
the run needs the SDK. It registers as an RFC server (`SAP_PROGRAM_ID` at
the gateway) for inbound IDocs and speaks over the bus like every other
citizen.

Configuration is the `SAP_*` env family (`SAP_ASHOST`, `SAP_SYSNR`,
`SAP_CLIENT`, `SAP_USER`, `SAP_PASSWD`, `SAP_PROGRAM_ID`, gateway host and
service) — credentials from your secret machinery, never inline. The
request/response path (`rpc:exec`) is cluster-safe through a queue group:
each instance holds its own SAP logon, the bus distributes.

The end-to-end bridge (IDoc in → flow → response) is exercised in the
recorded demo; a live SAP is required for the last mile, which is why the
recipe carries a stated exception rather than a mock that would prove
nothing.
