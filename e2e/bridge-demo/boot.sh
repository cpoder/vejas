#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# Vejas — SAP ⇄ Salesforce bridge : BOOT ONLY (no beats).
#
# Starts NATS + the runtime + the four bridge connectors, waits until they are
# up, prints a READY line, and exits leaving everything running — so a scripted
# browser film (e2e/bridge-film.mjs) can drive the panel and call trigger-idoc.sh
# at the right moments. Same env as run.sh (BIN_DIR, VEJAS_ROOT, SAP_*, SF_*).
#
# Stop with:  pkill -f "$BIN_DIR/vejas-runtime"; pkill -f "$BIN_DIR/nats-server"
# ─────────────────────────────────────────────────────────────────────────────
set -u

BIN_DIR="${BIN_DIR:-/opt/vejas}"
HERE="$(cd "$(dirname "$0")" && pwd)"
VEJAS_ROOT="${VEJAS_ROOT:-$HERE/bridge-demo-root}"
HTTP_ADDR="${HTTP_ADDR:-127.0.0.1:8686}"
LD="${LD_LIBRARY_PATH:-/usr/sap/NPL/D00/exe}"
: "${SF_INSTANCE_URL:?set SF_INSTANCE_URL}"
: "${SF_ACCESS_TOKEN:?set SF_ACCESS_TOKEN}"
SAPRFC="$BIN_DIR/vejas-sap-rfc"

# ── generate the credential-bearing connectors from the environment ──
mkdir -p "$VEJAS_ROOT/connectors"
cat > "$VEJAS_ROOT/connectors/sap_idoc_in.vjs" <<MAN
driver "exec-stream-source"
SUBJECT = "vx.sap.idoc"
CMD = "$SAPRFC idoc-server"
ENV = {LD_LIBRARY_PATH: "$LD", SAP_ASHOST: "${SAP_ASHOST:-localhost}", SAP_SYSNR: "${SAP_SYSNR:-00}", SAP_CLIENT: "${SAP_CLIENT:-001}", SAP_USER: "${SAP_USER:-DEVELOPER}", SAP_PASSWD: "${SAP_PASSWD:?set SAP_PASSWD}", SAP_LANG: "EN", SAP_PROGRAM_ID: "${SAP_PROGRAM_ID:?set SAP_PROGRAM_ID}", SAP_GWHOST: "${SAP_GWHOST:-vhcalnplci}", SAP_GWSERV: "${SAP_GWSERV:-sapgw00}"}
RESTART_SECS = 5
MAN
cat > "$VEJAS_ROOT/connectors/sap_out.vjs" <<MAN
driver "exec-sink"
SUBJECT = "vx.sap.idoc.out"
CMD = "$SAPRFC"
ENV = {LD_LIBRARY_PATH: "$LD", SAP_ASHOST: "${SAP_ASHOST:-localhost}", SAP_SYSNR: "${SAP_SYSNR:-00}", SAP_CLIENT: "${SAP_CLIENT:-001}", SAP_USER: "${SAP_USER:-DEVELOPER}", SAP_PASSWD: "${SAP_PASSWD:?set SAP_PASSWD}", SAP_LANG: "EN"}
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

# ── (re)start nats + runtime ──
pkill -f "$BIN_DIR/nats-server" 2>/dev/null; pkill -f "$BIN_DIR/vejas-runtime" 2>/dev/null
pkill -f "$BIN_DIR/vejas-sap-rfc" 2>/dev/null; pkill -f "$BIN_DIR/vejas-salesforce" 2>/dev/null
sleep 1
"$BIN_DIR/nats-server" -js -sd /tmp/vejas-demo-nats -a 127.0.0.1 -p 4222 >/tmp/demo-nats.log 2>&1 &
sleep 2
VEJAS_ROOT="$VEJAS_ROOT" NATS_URL="nats://127.0.0.1:4222" VEJAS_HTTP_ADDR="$HTTP_ADDR" \
  "$BIN_DIR/vejas-runtime" >/tmp/demo-rt.log 2>&1 &

# ── wait until the connectors are up (RFC server registered + SF export streaming) ──
for i in $(seq 1 30); do
  if grep -q '"ready":true' /tmp/demo-rt.log && grep -q 'sf_export.*streaming' /tmp/demo-rt.log; then
    break
  fi
  sleep 1
done
if grep -q '"ready":true' /tmp/demo-rt.log; then
  echo "READY · SAP RFC server registered, connectors up · panel http://$HTTP_ADDR/"
else
  echo "NOT-READY · check /tmp/demo-rt.log"
  grep -iE 'error|fail|panic' /tmp/demo-rt.log | head
  exit 1
fi
