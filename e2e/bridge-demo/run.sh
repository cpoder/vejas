#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# Vejas — SAP ⇄ Salesforce bridge demo conductor (for the Act 1 video).
#
# Runs ON the SAP host (where the RFC gateway + the connector binaries live),
# driving the bidirectional bridge with clean, pausable BEATS so an operator can
# narrate/screen-record the panel (http://<host>:8686) between beats.
#
# It is the productionised form of the orchestration that was validated live
# against a real SAP NetWeaver AS ABAP (NPL) and a real Salesforce Developer org.
#
# ── Setup (fill these in, or export them) ────────────────────────────────────
#   BIN_DIR         where vejas-sap-rfc / vejas-salesforce / vejas-runtime /
#                   nats-server live (default /opt/vejas)
#   VEJAS_ROOT      a root holding flows/ + connectors/ for the demo
#                   (default: this dir's ../bridge-demo-root, see README)
#   SAP_* env       SAP_ASHOST/SYSNR/CLIENT/USER/SAP_PASSWD/LANG, plus
#                   SAP_PROGRAM_ID + SAP_GWHOST/GWSERV for the registered server
#   SF_INSTANCE_URL, SF_ACCESS_TOKEN   from `sf org display --verbose`
#                   (ephemeral ~2h — refresh right before filming)
#   PAUSE=manual    wait for <enter> between beats (default); PAUSE=<seconds>
#                   auto-advances for an unattended capture.
#
# Nothing here is destructive beyond creating clearly-named VEJAS-* demo records
# (an SAP IDoc and a few Salesforce accounts) that the cleanup step removes.
# ─────────────────────────────────────────────────────────────────────────────
set -u

BIN_DIR="${BIN_DIR:-/opt/vejas}"
HERE="$(cd "$(dirname "$0")" && pwd)"
VEJAS_ROOT="${VEJAS_ROOT:-$HERE/bridge-demo-root}"
NATS_URL="${NATS_URL:-nats://127.0.0.1:4222}"
HTTP_ADDR="${HTTP_ADDR:-127.0.0.1:8686}"
PAUSE="${PAUSE:-manual}"
LD="${LD_LIBRARY_PATH:-/usr/sap/NPL/D00/exe}"

: "${SF_INSTANCE_URL:?set SF_INSTANCE_URL (sf org display --verbose)}"
: "${SF_ACCESS_TOKEN:?set SF_ACCESS_TOKEN (sf org display --verbose)}"
SAPRFC="$BIN_DIR/vejas-sap-rfc"
SAPENV="SAP_ASHOST=${SAP_ASHOST:-localhost} SAP_SYSNR=${SAP_SYSNR:-00} SAP_CLIENT=${SAP_CLIENT:-001} SAP_USER=${SAP_USER:-DEVELOPER} SAP_PASSWD=${SAP_PASSWD:-Down1oad} SAP_LANG=EN LD_LIBRARY_PATH=$LD"
MCP="http://$HTTP_ADDR/mcp"
# export what the sub-scripts (trigger-idoc.sh, cleanup.sh) inherit
export BIN_DIR LD SF_INSTANCE_URL SF_ACCESS_TOKEN
export SAP_ASHOST="${SAP_ASHOST:-localhost}" SAP_SYSNR="${SAP_SYSNR:-00}" SAP_CLIENT="${SAP_CLIENT:-001}"
export SAP_USER="${SAP_USER:-DEVELOPER}" SAP_PASSWD="${SAP_PASSWD:-Down1oad}"

beat() {
  echo ""
  echo "════════════════════════════════════════════════════════════════════"
  echo "  BEAT $1 — $2"
  echo "════════════════════════════════════════════════════════════════════"
  if [ "$PAUSE" = "manual" ]; then read -r -p "  (film, then press <enter>) "; else sleep "$PAUSE"; fi
}
mcp() { # $1 tool, $2 json args
  curl -s -X POST "$MCP" -H 'content-type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"$1\",\"arguments\":$2}}"
}

echo "Vejas SAP⇄Salesforce bridge demo — panel at http://$HTTP_ADDR/"

# ── prepare the root: flows are committed; connectors carry secrets, so we
#    generate them from the environment here (never committed with credentials) ─
mkdir -p "$VEJAS_ROOT/connectors"
cat > "$VEJAS_ROOT/connectors/sap_idoc_in.vjs" <<MAN
driver "exec-stream-source"
SUBJECT = "vx.sap.idoc"
CMD = "$SAPRFC idoc-server"
ENV = {LD_LIBRARY_PATH: "$LD", SAP_ASHOST: "${SAP_ASHOST:-localhost}", SAP_SYSNR: "${SAP_SYSNR:-00}", SAP_CLIENT: "${SAP_CLIENT:-001}", SAP_USER: "${SAP_USER:-DEVELOPER}", SAP_PASSWD: "${SAP_PASSWD:-Down1oad}", SAP_LANG: "EN", SAP_PROGRAM_ID: "${SAP_PROGRAM_ID:-WMETHODS_PROG}", SAP_GWHOST: "${SAP_GWHOST:-vhcalnplci}", SAP_GWSERV: "${SAP_GWSERV:-sapgw00}"}
RESTART_SECS = 5
MAN
cat > "$VEJAS_ROOT/connectors/sap_out.vjs" <<MAN
driver "exec-sink"
SUBJECT = "vx.sap.idoc.out"
CMD = "$SAPRFC"
ENV = {LD_LIBRARY_PATH: "$LD", SAP_ASHOST: "${SAP_ASHOST:-localhost}", SAP_SYSNR: "${SAP_SYSNR:-00}", SAP_CLIENT: "${SAP_CLIENT:-001}", SAP_USER: "${SAP_USER:-DEVELOPER}", SAP_PASSWD: "${SAP_PASSWD:-Down1oad}", SAP_LANG: "EN"}
MAN
cat > "$VEJAS_ROOT/connectors/sf_export.vjs" <<MAN
driver "exec-stream-source"
SUBJECT = "vx.sf.accounts"
CMD = "$BIN_DIR/vejas-salesforce"
ENV = {SF_INSTANCE_URL: "$SF_INSTANCE_URL", SF_ACCESS_TOKEN: "$SF_ACCESS_TOKEN", SF_QUERY: "SELECT Id, Name FROM Account WHERE Name LIKE 'United%' LIMIT 5", SF_API_VERSION: "v60.0", SF_INTERVAL_SECS: "3600"}
RESTART_SECS = 30
MAN
cat > "$VEJAS_ROOT/connectors/sf_ingest.vjs" <<MAN
driver "exec-sink"
SUBJECT = "vx.sf.ingest"
CMD = "$BIN_DIR/vejas-salesforce ingest"
ENV = {SF_INSTANCE_URL: "$SF_INSTANCE_URL", SF_ACCESS_TOKEN: "$SF_ACCESS_TOKEN", SF_OBJECT: "Account", SF_OPERATION: "insert", SF_API_VERSION: "v60.0"}
MAN

# ── boot ─────────────────────────────────────────────────────────────────────
pkill -f "$BIN_DIR/nats-server" 2>/dev/null; pkill -f "$BIN_DIR/vejas-runtime" 2>/dev/null
pkill -f "$BIN_DIR/vejas-sap-rfc" 2>/dev/null; pkill -f "$BIN_DIR/vejas-salesforce" 2>/dev/null
sleep 1
"$BIN_DIR/nats-server" -js -sd /tmp/vejas-demo-nats -a 127.0.0.1 -p 4222 >/tmp/demo-nats.log 2>&1 &
sleep 2
VEJAS_ROOT="$VEJAS_ROOT" NATS_URL="$NATS_URL" VEJAS_HTTP_ADDR="$HTTP_ADDR" \
  "$BIN_DIR/vejas-runtime" >/tmp/demo-rt.log 2>&1 &
sleep 5

beat 1 "The two connectors start — SAP RFC server registered at the gateway, Salesforce Bulk export streaming"
grep -iE "exec-stream|serving|register|ready|idoc-server" /tmp/demo-rt.log | head
echo "  → show the panel graph/topology: sap_idoc_in, sap_out, sf_export, sf_ingest + the two flows"

beat 2 "Salesforce → SAP : each account becomes an IDoc in SAP"
echo "  (sf_export streamed accounts on boot → sf_to_sap_idoc → sap_out → tRFC into SAP)"
printf '%s\n' '{"op":"call","func":"RFC_READ_TABLE","import":{"QUERY_TABLE":"EDIDC","OPTIONS":[{"TEXT":"SNDPRN = '"'"'SFDC'"'"'"}],"FIELDS":[{"FIELDNAME":"DOCNUM"},{"FIELDNAME":"MESTYP"},{"FIELDNAME":"SNDPRN"}]},"max_rows":10}' \
  | env $SAPENV "$SAPRFC" 2>/dev/null | tail -1
echo "  → IDocs created in SAP with SNDPRN=SFDC (real EDIDC rows). Show them in SAP GUI WE05 if filming SAP too."

beat 3 "SAP → Salesforce : a big inbound IDoc (10 account segments) becomes a Bulk insert in Salesforce"
"$HERE/trigger-idoc.sh" 2>/dev/null | tail -1
echo "  → watch the panel /events: sap_idoc_in → flow:sap_idoc_to_sf → sf_ingest (created: N)"
sleep 6
echo "  Verify in Salesforce:"
env SF_INSTANCE_URL="$SF_INSTANCE_URL" SF_ACCESS_TOKEN="$SF_ACCESS_TOKEN" \
  SF_QUERY="SELECT Id, Name FROM Account WHERE Name LIKE 'VEJAS-IDOC%'" SF_INTERVAL_SECS=0 \
  "$BIN_DIR/vejas-salesforce" 2>/dev/null | head -12

beat 4 "The panel while it flows — traces, the pipeline graph, sink responses"
echo "  → film: http://$HTTP_ADDR/ (Recent events card), /graph, /topology"

beat 5 "The thesis, live: correct a business literal, shadow-replay it on the REAL events, promote"
echo "  Proposed change: DEFAULT_INDUSTRY \"From SAP IDoc\" → \"SAP-sourced\" in sap_idoc_to_sf.vjs"
echo "  --- shadow-replay (before/after diff on the last real events, bus untouched) ---"
mcp vejas_replay_literal '{"file":"flows/sap_idoc_to_sf.vjs","name":"DEFAULT_INDUSTRY","key":"-","value":"SAP-sourced","n":20}' | head -c 1200; echo
if [ "$PAUSE" = "manual" ]; then read -r -p "  (show the diff, then <enter> to PROMOTE) "; fi
echo "  --- promote (write the literal, hot-reload) ---"
mcp vejas_set_literal '{"file":"flows/sap_idoc_to_sf.vjs","name":"DEFAULT_INDUSTRY","key":"-","value":"SAP-sourced"}'; echo
echo "  → the next SAP IDoc now stamps Industry=SAP-sourced. The expert corrected the meaning; the pipes never moved."

echo ""
echo "Demo complete. Cleanup (delete the VEJAS-* demo accounts) — run when done filming:"
echo "  $HERE/cleanup.sh"
