# Connecting to SAP (and other native-SDK systems)

Short answer to "how do we do SAP in Rust — is a `.so` enough?": **you don't
rewrite SAP in Rust, and no, a `.so` is the wrong tool.** SAP is the textbook
case for an **external-process connector** (ADR-0011): you keep your working
Java (SAP JCo) code and wrap it, and the runtime bridges it to the bus over
stdio.

## Why not a native `.so`

SAP JCo already ships a native library (`libsapjco3.so`), and there is a C RFC
SDK (`libsapnwrfc`). Loading either **into the Vejas runtime process** via FFI
means: SAP's proprietary, licensed native library running in your Rust binary's
memory and privileges, an unstable-ABI FFI boundary, and a JCo/JVM/native-lib
crash taking the whole runtime down. SAP supports its libraries next to a JVM in
a known environment — not embedded in an arbitrary Rust host. This is exactly
what ADR-0011 rejects.

## The right shape: your Java connector, as an exec connector

Keep the Java you already know. Package it as a small program that speaks
**stdio JSON**; the runtime does the NATS bridging, so your program needs no
NATS client. The SAP native library lives where SAP supports it — inside the
isolated JVM process — and if it crashes, the runtime restarts it.

### Source: poll SAP → the bus

`sap-connector.jar` uses JCo to call an RFC/BAPI (or read IDocs) and prints one
JSON object per line on stdout:

```
# connector: sap_material_master — polls SAP via your Java JCo jar
driver "exec-source"
CMD = "java -jar /opt/vejas/sap-connector.jar poll BAPI_MATERIAL_GETLIST"
SUBJECT = "vx.sap.materials"
INTERVAL_SECS = 300
```

Your `main(...)` (sketch): connect with JCo, call the BAPI, and for each row
`System.out.println(mapper.writeValueAsString(row))`. That's it — the runtime
publishes each line on `vx.sap.materials`, where your flows pick it up.

### Sink: the bus → SAP

Consume a subject and call a BAPI (e.g. create a sales order) with each message
piped to the jar's stdin:

```
# connector: sap_create_order
driver "exec-sink"
CMD = "java -jar /opt/vejas/sap-connector.jar create BAPI_SALESORDER_CREATEFROMDAT2"
SUBJECT = "vx.sap.orders.create"
```

Your `main(...)`: read stdin (the JSON message), map it to the BAPI import
parameters, call it, and exit non-zero on failure (the runtime will `nak` →
redeliver).

### Credentials

Never put SAP credentials in the manifest. Reference the Vault (ADR-0008) and
pass them into the process via env:

```
driver "exec-source"
CMD = "java -jar /opt/vejas/sap-connector.jar poll BAPI_MATERIAL_GETLIST"
SUBJECT = "vx.sap.materials"
INTERVAL_SECS = 300
SAP_PASSWORD = secret("sap/prod/password")
```

(Passing resolved config as child-process env to exec connectors is a small
runtime addition on the roadmap; until then the jar can read the same Vault or
its own env.)

## When SAP is modern (S/4HANA)

If you're on S/4HANA with OData/REST gateways, you may not need JCo at all: an
`http-poll` source and an `http-out` sink (pure manifests, no Java) can talk to
the OData services directly, with the token from the Vault. Use the Java/JCo
exec connector for classic RFC/BAPI/IDoc (ECC), the HTTP drivers for OData.

## The general rule

Any system with a native/vendor SDK (SAP, Tibco, MQ series, proprietary
drivers) follows this pattern: **wrap the working SDK in its own language as an
exec connector; bridge over stdio; isolate by process.** Rust built-in drivers
are for universal protocols (HTTP, timers, polling); WASM (later) is for pure,
portable, sandboxed connectors — never for embedding a vendor's native SDK.
