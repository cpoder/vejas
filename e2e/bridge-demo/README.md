# SAP ⇄ Salesforce bridge — demo conductor (Act 1 video)

A pausable, beat-driven conductor for the **bidirectional streaming bridge**
video — the shot nobody else can show: real SAP IDocs and real Salesforce Bulk,
bridged both ways by two readable VejasScript flows, no JVM.

It is the productionised form of the orchestration validated live against a real
SAP NetWeaver AS ABAP (NPL) and a real Salesforce Developer org.

## Where it runs

**On the SAP host** (the RFC gateway and the connector binaries live there).
Film the panel from your laptop over an SSH tunnel:

```
ssh -L 8686:127.0.0.1:8686 cpo@<sap-host>
```
then open `http://localhost:8686/`.

## Setup

1. Put the four binaries where `BIN_DIR` points (default `/opt/vejas`):
   `vejas-runtime`, `nats-server`, `vejas-sap-rfc`, `vejas-salesforce`.
2. SAP: the connectors register under an existing idle destination
   (`SAP_PROGRAM_ID=WMETHODS_PROG` → the `WMETHODS_RFC` dest) so **no SAP config
   is changed**. Override the `SAP_*` env if your system differs. The dev
   license must be valid (renewed to 2026-11-21 on the NPL).
3. Salesforce: refresh a token right before filming (it is ephemeral, ~2h):
   ```
   sf org display --verbose --target-org <you>
   export SF_INSTANCE_URL=... SF_ACCESS_TOKEN=...
   ```

## Run

```
BIN_DIR=/opt/vejas SF_INSTANCE_URL=… SF_ACCESS_TOKEN=… ./run.sh
```
`PAUSE=manual` (default) waits for `<enter>` between beats so you can narrate and
film each one; `PAUSE=8` auto-advances for an unattended capture.

The connectors that carry credentials are generated from the environment into
`bridge-demo-root/connectors/` at boot (git-ignored — never committed with
secrets). The two flows are committed under `bridge-demo-root/flows/`.

## The beats

1. **Both connectors start** — the SAP RFC server registers at the gateway; the
   Salesforce Bulk export streams. Show the panel graph/topology.
2. **Salesforce → SAP** — each exported account became an IDoc in SAP
   (`EDIDC`, `SNDPRN=SFDC`). Real rows; show them in `WE05` if filming SAP too.
3. **SAP → Salesforce** — a big inbound IDoc (10 account segments) fans out to a
   Salesforce Bulk insert; the panel `/events` shows `sap_idoc_in → flow →
   sf_ingest (created: N)`, then a SOQL query shows the new `VEJAS-IDOC-*`
   accounts in the org.
4. **The panel while it flows** — traces, the pipeline graph, sink responses.
5. **The thesis, live** — correct a business literal (`DEFAULT_INDUSTRY`),
   **shadow-replay** the change on the real events (before/after diff, bus
   untouched), **promote**. The expert corrected the meaning; the pipes never
   moved. This is the anti-thesis of "the AI assistant that generates
   proprietary JSON".

## After filming

```
./cleanup.sh      # deletes the VEJAS-* demo accounts (and exercises the delete op)
```

## Rehearsing without SAP

The Salesforce half has a mock (`../mock-salesforce.mjs`); the SAP half needs the
live gateway (it is the real RFC protocol, not mockable cheaply). Rehearse the
panel/shadow-replay beats against the mock; film beats 2–3 on the real systems.
