# Example: a bidirectional SAP ⇄ Salesforce bridge, streaming

Two heavyweight enterprise systems, two different paradigms — SAP **IDoc** (over
transactional RFC) and Salesforce **Bulk API 2.0** — wired together in **both
directions**, streaming, entirely through Vejas flows on NATS. All native Rust
(no JVM, no Python), validated live against a real SAP NetWeaver AS ABAP system
and a real Salesforce Developer org.

The point: the connectors are dumb pipes to each system; the **integration logic
lives in two small VejasScript flows** — the business surface a domain expert can
read and correct.

## The two legs

```
  Salesforce ──(Bulk 2.0 export, one account/msg)──▶ vx.sf.accounts
                                                          │
                                       flow: sf_to_sap_idoc  (map each → an IDoc)
                                                          ▼
                                                   vx.sap.idoc.out
                                                          │
                                          sap_out (sink) ── tRFC ──▶ SAP  (one IDoc per account)


  SAP ──(inbound IDoc, N account segments)──▶ sap_idoc_in (registered RFC server)
                                                          │
                                                     vx.sap.idoc
                                                          │
                                     flow: sap_idoc_to_sf  (collect the segments)
                                                          ▼
                                                     vx.sf.ingest
                                                          │
                                     sf_ingest (sink) ── Bulk 2.0 insert ──▶ Salesforce  (accounts created)
```

**SF → SAP** (`sf_to_sap_idoc.vjs`): the Salesforce export streams one account per
message; the flow maps each to an `IDOC_INBOUND_ASYNCHRONOUS` request; the SAP
sink sends it in over tRFC (exactly-once, dedup by transaction id).

**SAP → SF** (`sap_idoc_to_sf.vjs`): SAP pushes an inbound IDoc with many account
segments to our registered RFC server, which streams it; the flow collects the
`IDOC_DATA_REC_40` segments into one batch; the Salesforce sink Bulk-inserts them.

## The pieces

| File | Role |
|---|---|
| `connectors/sf_export.vjs`   | `exec-stream-source` — Salesforce Bulk 2.0 export → `vx.sf.accounts` |
| `flows/sf_to_sap_idoc.vjs`   | account → `send_idoc` request → `vx.sap.idoc.out` |
| `connectors/sap_out.vjs`     | `exec-sink` — `vejas-sap-rfc` sends the IDoc into SAP (tRFC) |
| `connectors/sap_idoc_in.vjs` | `exec-stream-source` — registered RFC server → `vx.sap.idoc` |
| `flows/sap_idoc_to_sf.vjs`   | IDoc segments → batch → `vx.sf.ingest` |
| `connectors/sf_ingest.vjs`   | `exec-sink` — `vejas-salesforce ingest` Bulk-inserts the batch |

The two connector binaries (`vejas-sap-rfc`, `vejas-salesforce`) live in
`connectors/sap-rfc/` and `connectors/salesforce/`. See ADR-0014 and
`docs/examples/sap_connector.md` for how the SAP connector works.

## Running it

1. Build the connectors (`cargo build --release` in each crate) and place the
   binaries where the manifests' `CMD` points (e.g. `/opt/vejas/`).
2. Put these `flows/` and `connectors/` into a Vejas root (`VEJAS_ROOT`), set the
   secrets they reference (`vejas_set_secret`, e.g. `sap/npl/passwd`,
   `sf/client_secret`), and start the runtime with NATS.
3. SF → SAP fires as soon as `sf_export` runs. To exercise SAP → SF, send an
   inbound IDoc to SAP addressed to the registered server's destination.

> Credentials here use `secret()` (ADR-0008) and placeholder URLs — fill in your
> org's My Domain and connected-app / user. For a quick dev run of the Salesforce
> side you can swap the client-credentials fields for
> `SF_ACCESS_TOKEN: secret("sf/access_token")` from `sf org display --verbose`.

## What this demonstrates

- **Streaming both ways** with back-pressure (bounded memory even on big volumes).
- **Two paradigms bridged** by declarative flows, not glue code.
- The integration **logic is the business surface** — editable, replayable,
  correctable — while the heavy protocol work is isolated in native connectors.
