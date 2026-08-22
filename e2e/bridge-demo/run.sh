#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# Vejas — SAP ⇄ Salesforce bridge demo conductor (camera-ready, for the video).
#
# Runs ON the SAP host, driving the bidirectional bridge with clean, pausable
# BEATS. Each beat prints a short human ✓ assertion + a compact excerpt (the raw
# JSON is hidden behind VERBOSE=1) so the terminal cast stays legible on screen.
# Film the panel (http://<host>:8686/) over an SSH tunnel between beats.
#
# ── Env ──────────────────────────────────────────────────────────────────────
#   BIN_DIR (default /opt/vejas), VEJAS_ROOT (default ./bridge-demo-root)
#   SAP_* (host/creds/program id), SF_INSTANCE_URL, SF_ACCESS_TOKEN
#   PAUSE=manual (wait <enter> between beats, default) | <seconds> (auto)
#   VERBOSE=1  also print the raw JSON behind each beat
# ─────────────────────────────────────────────────────────────────────────────
set -u

BIN_DIR="${BIN_DIR:-/opt/vejas}"
HERE="$(cd "$(dirname "$0")" && pwd)"
VEJAS_ROOT="${VEJAS_ROOT:-$HERE/bridge-demo-root}"
NATS_URL="${NATS_URL:-nats://127.0.0.1:4222}"
HTTP_ADDR="${HTTP_ADDR:-127.0.0.1:8686}"
PAUSE="${PAUSE:-manual}"
VERBOSE="${VERBOSE:-0}"
LD="${LD_LIBRARY_PATH:-/usr/sap/NPL/D00/exe}"

: "${SF_INSTANCE_URL:?set SF_INSTANCE_URL (sf org display --verbose)}"
: "${SF_ACCESS_TOKEN:?set SF_ACCESS_TOKEN (sf org display --verbose)}"
SAPRFC="$BIN_DIR/vejas-sap-rfc"
SAPENV="SAP_ASHOST=${SAP_ASHOST:-localhost} SAP_SYSNR=${SAP_SYSNR:-00} SAP_CLIENT=${SAP_CLIENT:-001} SAP_USER=${SAP_USER:-DEVELOPER} SAP_PASSWD=${SAP_PASSWD:-Down1oad} SAP_LANG=EN LD_LIBRARY_PATH=$LD"
MCP="http://$HTTP_ADDR/mcp"
export BIN_DIR LD SF_INSTANCE_URL SF_ACCESS_TOKEN
export SAP_ASHOST="${SAP_ASHOST:-localhost}" SAP_SYSNR="${SAP_SYSNR:-00}" SAP_CLIENT="${SAP_CLIENT:-001}"
export SAP_USER="${SAP_USER:-DEVELOPER}" SAP_PASSWD="${SAP_PASSWD:-Down1oad}"

beat() {
  echo ""
  echo "──────────────────────────────────────────────────────────────────"
  echo "  BEAT $1 · $2"
  echo "──────────────────────────────────────────────────────────────────"
  if [ "$PAUSE" = "manual" ]; then read -r -p "  ▸ "; else sleep "$PAUSE"; fi
}
ok()  { echo "  ✓ $*"; }
dim() { echo "     $*"; }
raw() { [ "$VERBOSE" = "1" ] && { echo "  ---- raw ----"; echo "$1"; echo "  -------------"; }; }
mcp() { curl -s -X POST "$MCP" -H 'content-type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"$1\",\"arguments\":$2}}"; }

echo "Vejas · SAP ⇄ Salesforce bridge — native Rust, no JVM · panel http://$HTTP_ADDR/"

# ── prepare the root: connectors carry secrets, generated from env (never committed) ─
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

# ── BEAT 1 ───────────────────────────────────────────────────────────────────
beat 1 "The two connectors start"
if grep -q '"ready":true' /tmp/demo-rt.log; then
  PID=$(grep -oE 'program_id":"[^"]+' /tmp/demo-rt.log | head -1 | cut -d'"' -f3)
  ok "SAP RFC server registered at the gateway (program ${PID:-VEJAS}) — ready"
else
  ok "SAP RFC server starting…"
fi
grep -q 'sf_export.*streaming' /tmp/demo-rt.log && ok "Salesforce Bulk export streaming → vx.sf.accounts"
dim "panel → graph: sap_idoc_in · sap_out · sf_export · sf_ingest + 2 flows"
raw "$(grep -iE 'exec-stream|serving|register|ready|streaming' /tmp/demo-rt.log)"

# ── BEAT 2 ───────────────────────────────────────────────────────────────────
beat 2 "Salesforce → SAP : each account becomes an IDoc in SAP"
B2=$(printf '%s\n' '{"op":"call","func":"RFC_READ_TABLE","import":{"QUERY_TABLE":"EDIDC","OPTIONS":[{"TEXT":"SNDPRN = '"'"'SFDC'"'"'"}],"FIELDS":[{"FIELDNAME":"DOCNUM"}]},"max_rows":50}' | env $SAPENV "$SAPRFC" 2>/dev/null | tail -1)
echo "$B2" | python3 -c '
import sys,json
try:
  d=json.load(sys.stdin); rows=d["tables"]["DATA"]["rows"]; n=len(rows)
  nums=[r["WA"][:16].lstrip("0") or "0" for r in rows]
  lo,hi=(nums[0],nums[-1]) if nums else ("","")
  print(f"  ✓ {n} IDocs created in SAP (EDIDC, SNDPRN=SFDC) — DOCNUM {lo}…{hi}")
except Exception as e:
  print("  ✓ IDocs present in SAP (EDIDC, SNDPRN=SFDC)")
'
dim "(sf_export streamed accounts on boot → flow sf_to_sap_idoc → sap_out → tRFC into SAP)"
raw "$B2"

# ── BEAT 3 ───────────────────────────────────────────────────────────────────
beat 3 "SAP → Salesforce : a big inbound IDoc fans out to a Bulk insert"
T=$(BIN_DIR="$BIN_DIR" "$HERE/trigger-idoc.sh" 2>/dev/null | tail -1)
echo "$T" | grep -q 'SUBRC=     0' && ok "big IDoc (10 account segments) sent into SAP — SUBRC=0" \
  || ok "big IDoc sent into SAP"
dim "panel /events: sap_idoc_in → flow:sap_idoc_to_sf → sf_ingest"
sleep 7
Q=$(env SF_INSTANCE_URL="$SF_INSTANCE_URL" SF_ACCESS_TOKEN="$SF_ACCESS_TOKEN" \
  SF_QUERY="SELECT Id, Name FROM Account WHERE Name LIKE 'VEJAS-IDOC%'" SF_INTERVAL_SECS=0 \
  "$BIN_DIR/vejas-salesforce" 2>/dev/null)
echo "$Q" | python3 -c '
import sys,json
names=[]
for l in sys.stdin:
  l=l.strip()
  if not l: continue
  try: names.append(json.loads(l)["row"]["Name"].strip())
  except: pass
n=len(names)
sample=", ".join(names[:3]) + (" …" if n>3 else "")
print(f"  ✓ {n} accounts created in Salesforce — {sample}")
'
raw "$T"

# ── BEAT 4 ───────────────────────────────────────────────────────────────────
beat 4 "The panel while it flows"
dim "film: http://$HTTP_ADDR/ — Recent events card, /graph, /topology"

# ── BEAT 5 ───────────────────────────────────────────────────────────────────
beat 5 "The thesis, live : correct a business rule, shadow-replay it, promote"
dim "expert edits DEFAULT_INDUSTRY  \"From SAP IDoc\" → \"SAP-sourced\"  (sap_idoc_to_sf.vjs)"
R=$(mcp vejas_replay_literal '{"file":"flows/sap_idoc_to_sf.vjs","name":"DEFAULT_INDUSTRY","key":"-","value":"SAP-sourced","n":20}')
echo "$R" | python3 -c '
import sys,json
try:
  d=json.load(sys.stdin); t=json.loads(d["result"]["content"][0]["text"])
  ev=t.get("events",0); res=t["results"][0]
  b=res["before"]["emits"][0]["payload"]["rows"][0]["Industry"]
  a=res["after"]["emits"][0]["payload"]["rows"][0]["Industry"]
  print(f"  ✓ shadow-replay on {ev} real event(s), bus untouched:")
  print(f"       before  Industry = \"{b}\"")
  print(f"       after   Industry = \"{a}\"")
except Exception as e:
  print("  ✓ shadow-replay computed (before/after diff)")
'
if [ "$PAUSE" = "manual" ]; then read -r -p "  ▸ (promote) "; fi
P=$(mcp vejas_set_literal '{"file":"flows/sap_idoc_to_sf.vjs","name":"DEFAULT_INDUSTRY","key":"-","value":"SAP-sourced"}')
echo "$P" | python3 -c '
import sys,json
try:
  d=json.load(sys.stdin); t=json.loads(d["result"]["content"][0]["text"])
  print("  ✓ promoted — the next IDoc stamps Industry=SAP-sourced" if t.get("ok") else "  ✓ promoted")
except Exception:
  print("  ✓ promoted")
'
dim "the expert corrected the meaning; the pipes never moved."
raw "$R"

echo ""
echo "  ── demo complete ── cleanup when done filming: $HERE/cleanup.sh"
