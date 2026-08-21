# 0014 — SAP connector: native Rust over the NW RFC SDK, no JVM

- Status: Accepted
- Date: 2026-08-21

## Context

SAP is the archetypal native-SDK system. ADR-0011 and `docs/examples/
sap_connector.md` framed the answer as "keep your working Java (SAP JCo) and
wrap it as an exec connector" — correct on isolation, but it drags a **JVM**
into every SAP deployment. The owner's requirement is explicit: no Java, no
Python, a connector **we** maintain, as light as the rest of Vejas (ADR-0009).

Three ways to reach a SAP system were on the table:

1. **JCo + JVM** — the SDK is Java; the connector is a jar. Complete, but the
   JVM is the weight we want gone.
2. **Reverse-engineer the RFC binary protocol in pure Rust** — zero SAP
   dependency, the "dream". The classic RFC / SAP NI protocol is proprietary,
   undocumented, with its own ABAP-type serialization, SAP codepages,
   compression and NI framing; a *production* RFC client (all types, tRFC/qRFC
   for IDocs, error handling) is weeks-to-months against a moving target.
3. **The SAP NW RFC SDK (`libsapnwrfc`, the official C library) via Rust FFI**
   — no JVM, our code is Rust, the vendor library is C.

A spike settled it. `libsapnwrfc.so` is **already present on every SAP server**
(shipped with the kernel: `/usr/sap/<SID>/SYS/exe/.../libsapnwrfc.so`). ~30
lines of hand-declared FFI (no headers needed) compiled and **connected to the
gateway, spoke the RFC protocol through the logon layer**, returning a
structured SAP reply — the only blocker was an expired developer-edition
license (`RFC_LOGON_FAILURE — error in license check`), an admin matter, not a
code one.

## Decision

The SAP connector is a **native Rust binary that calls the SAP NW RFC SDK
(`libsapnwrfc`) over FFI**, run as an **isolated exec process** (ADR-0011): no
JVM, no Python, our code in Rust, the vendor C library living in a separate
process so a native crash never takes the runtime down. It bridges to the bus
and is driven over MCP (`sap_list` / `sap_describe` / `sap_call`, and IDoc
in/out) — see the SAP connector doc.

We **reject reverse-engineering the RFC binary protocol**: it would cost weeks
of high-risk work to remove a dependency that is already present wherever we
deploy next to SAP, and that our Rust FFI exploits in an evening. Engineering
pride, not customer value.

We **reject JCo + JVM**: `libsapnwrfc` does the same work (all ABAP types,
codepages, IDocs, introspection) without the JVM.

OData/SOAP over HTTP stays a possible *accelerator* where a SAP exposes Gateway
(full-Rust and trivial, we already have `oauth-poll`), but **not** the primary
path: the large installed base is classic NetWeaver AS ABAP where only the RFC
gateway is guaranteed.

## Consequences

- Full-Rust connector code, no JVM/Python; aligns with ADR-0009 and the
  lightness promise.
- The `libsapnwrfc` dependency is light in practice: it ships with the SAP
  kernel (present on the box when the connector runs beside SAP) and is
  otherwise downloadable from SAP as the NW RFC SDK. It is licensed by SAP.
- Isolation by process (ADR-0011) is kept: link/`dlopen` happens in the SAP
  connector binary, never in the Vejas runtime.
- Capabilities the SDK gives natively: call BAPI/RFC, list & search function
  modules, introspect interfaces and DDIC structures, send/receive IDocs
  (tRFC/qRFC), all with correct codepage/type handling.
- **Cost / open items:** the FFI surface (UTF-16 `SAP_UC`, `RFC_ERROR_INFO`,
  connection params, table/structure marshalling) is ours to declare and test;
  IDoc *inbound* needs the connector registered at the gateway
  (`reginfo`/`secinfo`), a long-running server program — which drives the
  streaming-source work (ADR pending). Credentials via `secret()` (ADR-0008),
  never literals.

## Alternatives considered

- **JCo + JVM** — rejected: the JVM is the weight we set out to remove.
- **Reverse the RFC protocol in Rust** — rejected: disproportionate cost/risk
  vs. an official library already on every SAP host (see spike above).
- **OData/SOAP only** — rejected as the primary path: not guaranteed on the
  classic installed base; kept as an accelerator where Gateway exists.
