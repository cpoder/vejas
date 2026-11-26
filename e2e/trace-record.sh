#!/bin/bash
# The trace record (ADR-0034, lot B): every hop of an event is kept in the
# VEJAS_TRACES stream and read back as one trace, by its id or by a search,
# over HTTP and MCP. Credential-named keys are masked, in a sink's long answer
# too, and a vault value never reaches the stream's files; a search reaches
# past a thousand records; the record writes on a connection of its own and
# brings back a stream removed by hand; a trace the caller sampled out leaves
# only its failures; VEJAS_TRACES=off records nothing.
#   e2e/trace-record.sh                      (needs nats-server and the nats CLI)
set -uo pipefail
cd "$(dirname "$0")/.."
BIN="${BIN:-$PWD/core/target/release/vejas-runtime}"
[ -x "$BIN" ] || { echo "build first" >&2; exit 1; }
S=$(mktemp -d); R=$(mktemp -d)
RT=""; NP=""; RT2=""; NP2=""; DOWN=""
trap 'kill $RT $NP $RT2 $NP2 $DOWN 2>/dev/null; wait $RT $NP $RT2 $NP2 $DOWN 2>/dev/null; rm -rf "$S" "$R"' EXIT
fail=0
ok(){ echo "  ✓ $1"; }
ko(){ echo "  ✗ $1"; fail=1; }

URL=nats://127.0.0.1:4283; H="http://127.0.0.1:8755"; IN=18793; DOWN_PORT=18794; MON=8285
VAULT_VALUE="vault-canary-7f3a9c41"
mkdir -p "$R/flows" "$R/connectors"
cat > "$R/connectors/rec_in.vjs" <<VJS
# connector: rec_in
driver "http-in"
PORT = $IN
VJS
# The first flow puts a vault value in what it emits: the second hop receives it.
cat > "$R/flows/rec_check.vjs" <<'VJS'
source "vx.rec.in"
emit "vx.rec.checked", { order: event.order, key: secret("demo/key") }
VJS
cat > "$R/flows/rec_store.vjs" <<'VJS'
source "vx.rec.checked"
emit "vx.rec.stored", { order: event.order }
VJS
cat > "$R/flows/rec_bad.vjs" <<'VJS'
source "vx.rec.bad"
x = secret("missing/x")
emit "vx.rec.never", { x: x }
VJS
# A sink whose downstream answers more than the events ring shows, a key in it.
cat > "$R/connectors/rec_out.vjs" <<VJS
# connector: rec_out
driver "http-out"
SUBJECT = "vx.rec.out"
URL = "http://127.0.0.1:$DOWN_PORT/hook"
VJS
python3 -c '
import http.server, json, sys
answer = json.dumps({"id": "i" * 300, "api_key": "sk_live_canary_5d1e", "ok": True}).encode()
class H(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        self.rfile.read(int(self.headers.get("content-length", 0)))
        self.send_response(200); self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(answer))); self.end_headers(); self.wfile.write(answer)
    def log_message(self, *a): pass
http.server.HTTPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
' $DOWN_PORT & DOWN=$!

nats-server -js -sd "$S/n" -a 127.0.0.1 -p 4283 -m $MON > /dev/null 2>&1 & NP=$!
sleep 0.5
NATS_URL=$URL VEJAS_ROOT="$R" VEJAS_HTTP_ADDR=127.0.0.1:8755 VEJAS_ACK_WAIT_SECS=1 \
VEJAS_SECRET_DEMO_KEY="$VAULT_VALUE" \
  "$BIN" > "$S/rt.log" 2>&1 & RT=$!
until curl -sf -o /dev/null "$H/healthz"; do sleep 0.1; done
for _ in $(seq 1 100); do   # rec_in, rec_out, rec_check, rec_store, rec_bad
  n=$(curl -s "$H/topology" | grep -o '"status":"running"' | wc -l)
  [ "$n" -ge 5 ] && break; sleep 0.1
done
for _ in $(seq 1 50); do curl -sf "http://127.0.0.1:$IN/healthz" >/dev/null && break; sleep 0.1; done
grep -q "trace record -> stream VEJAS_TRACES" "$S/rt.log" && ok "the record is on by default" \
  || ko "no record line at boot: $(grep -i record "$S/rt.log" | head -2)"

mcp(){ curl -s -m 10 -H 'Content-Type: application/json' \
  -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"vejas_trace\",\"arguments\":$1}}" "$H/mcp"; }
hops(){ python3 -c 'import json,sys
try: print(len(json.load(open(sys.argv[1])).get("hops", [])))
except Exception: print(0)' "$1"; }

echo "── one event, three hops, one trace"
TA=4bf92f3577b34da6a3ce929d0e0e4736; PA=00f067aa0ba902b7
curl -s -X POST -H "traceparent: 00-$TA-$PA-01" -d '{"order":"4711","password":"hunter2"}' \
  "http://127.0.0.1:$IN/ingest/rec.in" > /dev/null
for _ in $(seq 1 100); do
  curl -s "$H/traces/$TA" > "$S/a.json"
  [ "$(hops "$S/a.json")" -ge 3 ] && break; sleep 0.1
done
VERDICT=$(python3 - "$S/a.json" "$TA" "$PA" <<'PY'
import json, sys
d = json.load(open(sys.argv[1])); trace, parent = sys.argv[2], sys.argv[3]
h = d["hops"]
def need(cond, why):
    if not cond:
        print(why); sys.exit(1)
need([x["unit"] for x in h] == ["connector:rec_in", "flow:rec_check", "flow:rec_store"],
     f"hops: {[x['unit'] for x in h]}")
need(all(x["trace_id"] == trace for x in h), "a hop of another trace")
need(h[0]["parent_id"] == parent, "the first hop does not hang under the caller")
need(h[1]["parent_id"] == h[0]["span_id"] and h[2]["parent_id"] == h[1]["span_id"], "the parent chain is broken")
need([x["kind"] for x in h] == ["server", "consumer", "consumer"], f"kinds: {[x['kind'] for x in h]}")
need(all(x["ok"] for x in h), "a hop failed")
need(h[0]["subject"] == "vx.rec.in" and h[1]["subject"] == "vx.rec.in" and h[2]["subject"] == "vx.rec.checked", "subjects")
need(h[1]["emits"] == ["vx.rec.checked"], f"emits: {h[1]['emits']}")
need(h[1]["attributes"].get("vejas.delivery.attempt") == 1, "no delivery attempt")
need(h[1]["attributes"].get("vejas.flow.version"), "no flow version")
need(h[1]["input"]["order"] == "4711", "the payload is not kept")
need(h[0]["input"]["password"] == "<masked>" and h[1]["input"]["password"] == "<masked>", "a password in the clear")
need(h[2]["input"]["key"] == "<secret>", f"the vault value is not taken out: {h[2]['input']}")
print("ok")
PY
)
[ "$VERDICT" = ok ] && ok "source, flow, flow: in order, parent under parent, input masked, vault value taken out" \
  || ko "the trace is wrong: $VERDICT"

echo "── a sink's long answer is masked, then cut"
TS=5bf92f3577b34da6a3ce929d0e0e4739; PS=30f067aa0ba902ba
nats -s $URL pub vx.rec.out '{"order":"S-1"}' -H "traceparent:00-$TS-$PS-01" > /dev/null 2>&1
for _ in $(seq 1 100); do
  curl -s "$H/traces/$TS" > "$S/s.json"
  grep -q 'connector:rec_out' "$S/s.json" && break; sleep 0.1
done
VERDICT=$(python3 - "$S/s.json" <<'PY'
import json, sys
try:
    h = [x for x in json.load(open(sys.argv[1]))["hops"] if x["unit"] == "connector:rec_out"][0]
except Exception as e:
    print(f"no sink hop: {e}"); sys.exit(1)
a = h["answer"] or ""
if "sk_live_canary" in a: print(f"the key is in the clear: {a[:120]}"); sys.exit(1)
if "<masked>" not in a: print(f"no mask: {a[:120]}"); sys.exit(1)
if json.loads(a)["ok"] is not True: print("the answer lost its shape"); sys.exit(1)
print("ok")
PY
)
[ "$VERDICT" = ok ] && ok "the 300-character answer keeps its shape and loses its api_key" || ko "sink answer: $VERDICT"

TR_DIR=$(find "$S/n" -type d -name VEJAS_TRACES | head -1)
for _ in $(seq 1 50); do grep -rqa '4711' "$TR_DIR" 2>/dev/null && break; sleep 0.1; done
grep -rqa '4711' "$TR_DIR" 2>/dev/null && ok "the records are in the stream's files" || ko "no record on disk under $TR_DIR"
if grep -rqa -e "$VAULT_VALUE" -e hunter2 -e sk_live_canary "$TR_DIR"; then ko "a secret reached the stream's files"
else ok "no password, vault value or API key in the stream's files"; fi

echo "── found by what it carries, over HTTP and MCP"
curl -s "$H/traces?search=4711" | grep -q "\"trace_id\":\"$TA\"" && ok "GET /traces?search finds it" \
  || ko "search: $(curl -s "$H/traces?search=4711" | cut -c1-200)"
mcp '{"search":"4711"}' | grep -q "$TA" && ok "vejas_trace finds it by search" || ko "vejas_trace search: $(mcp '{"search":"4711"}' | cut -c1-200)"
mcp "{\"trace_id\":\"$TA\"}" | grep -q 'flow:rec_store' && ok "vejas_trace reads it by id" || ko "vejas_trace by id"
case "$(curl -s -o /dev/null -w '%{http_code}' "$H/traces/not-a-trace")" in
  400) ok "a malformed id is refused";; *) ko "a malformed id was not refused";; esac

echo "── a search reaches past a thousand records"
kept(){ curl -s "$H/metrics" | awk '/^vejas_trace_records_total /{print $2}'; }
before=$(kept)
nats -s $URL pub vx.rec.checked '{"order":"B-{{Count}}"}' --count 1200 > /dev/null 2>&1
for _ in $(seq 1 150); do [ "$(( $(kept) - before ))" -ge 1200 ] && break; sleep 0.1; done
for _ in $(seq 1 30); do
  curl -s "$H/traces?search=B-1200&limit=5" | grep -q '"trace_id"' && break; sleep 0.2
done
curl -s "$H/traces?search=B-1200&limit=5" | grep -q '"trace_id"' && ok "the newest of 1200 more records is found" \
  || ko "search stopped short: $(curl -s "$H/traces?search=B-1200" | cut -c1-160) (kept $(( $(kept) - before )))"
in_stream=$(nats -s $URL stream info VEJAS_TRACES --json | python3 -c 'import json,sys; print(json.load(sys.stdin)["state"]["messages"])')
read_hops=$(curl -s "$H/traces?search=nothing-mentions-this" | python3 -c 'import json,sys; print(json.load(sys.stdin)["searched_hops"])')
[ "$read_hops" -ge "$in_stream" ] && [ "$in_stream" -gt 1200 ] && ok "a search says how many hops it read ($read_hops, $in_stream in the stream)" \
  || ko "a search read $read_hops hops of the $in_stream in the stream"

echo "── its own connection, and a stream that comes back"
n=$(curl -s "http://127.0.0.1:$MON/connz?limit=100" | grep -o '"name": *"vejas-runtime"' | wc -l)
[ "$n" -ge 2 ] && ok "the record writes on a connection of its own ($n runtime connections)" \
  || ko "one connection for everything ($n)"
nats -s $URL stream rm VEJAS_TRACES -f > /dev/null 2>&1
lost(){ curl -s "$H/metrics" | awk -F' ' '/reason="publish_failed"/{print $2}'; }
nats -s $URL pub vx.rec.checked '{"order":"C-1"}' > /dev/null 2>&1
for _ in $(seq 1 100); do [ "$(lost)" -ge 1 ] 2>/dev/null && break; sleep 0.1; done
TC=6bf92f3577b34da6a3ce929d0e0e473a; PC=40f067aa0ba902bb
for _ in $(seq 1 50); do nats -s $URL stream info VEJAS_TRACES > /dev/null 2>&1 && break; sleep 0.1; done
nats -s $URL pub vx.rec.checked '{"order":"C-2"}' -H "traceparent:00-$TC-$PC-01" > /dev/null 2>&1
for _ in $(seq 1 100); do curl -s "$H/traces/$TC" | grep -q 'flow:rec_store' && break; sleep 0.1; done
curl -s "$H/traces/$TC" | grep -q 'flow:rec_store' \
  && ok "removed by hand, the stream is back for the next records ($(lost) lost meanwhile, counted)" \
  || ko "no record after the stream was removed: $(curl -s "$H/traces/$TC" | cut -c1-160)"

echo "── a caller's sampled-out trace leaves only its failures"
TD=1bf92f3577b34da6a3ce929d0e0e4737; PD=10f067aa0ba902b8
curl -s -X POST -H "traceparent: 00-$TD-$PD-00" -d '{"order":"D-4"}' "http://127.0.0.1:$IN/ingest/rec.in" > /dev/null
for _ in $(seq 1 100); do curl -s "$H/events" | grep -q "$TD" && \
  curl -s "$H/events?flow=flow:rec_store" | grep -q "$TD" && break; sleep 0.1; done
sleep 0.5
case "$(curl -s -o /dev/null -w '%{http_code}' "$H/traces/$TD")" in
  404) ok "a successful sampled-out trace is not kept";; *) ko "a sampled-out trace was kept";; esac
TF=2bf92f3577b34da6a3ce929d0e0e4738; PF=20f067aa0ba902b9
nats -s $URL pub vx.rec.bad '{"order":"F-6"}' -H "traceparent:00-$TF-$PF-00" > /dev/null 2>&1
for _ in $(seq 1 150); do
  curl -s "$H/traces/$TF" > "$S/f.json"
  grep -q '"vejas.dlq":true' "$S/f.json" && break; sleep 0.1
done
VERDICT=$(python3 - "$S/f.json" <<'PY'
import json, sys
try:
    h = json.load(open(sys.argv[1]))["hops"]
except Exception as e:
    print(f"no trace: {e}"); sys.exit(1)
attempts = [x["attributes"].get("vejas.delivery.attempt") for x in h]
if not h or any(x["ok"] for x in h): print("a failure is missing or a hop succeeded"); sys.exit(1)
if attempts != list(range(1, len(h) + 1)) or len(h) < 2: print(f"attempts: {attempts}"); sys.exit(1)
if not h[-1]["attributes"].get("vejas.dlq"): print("the last attempt is not dead-lettered"); sys.exit(1)
if "missing/x" not in (h[0]["error"] or ""): print(f"error: {h[0]['error']}"); sys.exit(1)
print("ok")
PY
)
[ "$VERDICT" = ok ] && ok "each failed attempt is kept, the last one dead-lettered" || ko "failures: $VERDICT"

echo "── counted"
curl -s "$H/metrics" | grep -qE '^vejas_trace_records_total [1-9]' && ok "/metrics counts the records" \
  || ko "metrics: $(curl -s "$H/metrics" | grep trace_records | head -3)"

echo "── VEJAS_TRACES=off records nothing"
kill $RT 2>/dev/null; wait $RT 2>/dev/null; RT=""   # its http-in holds the port
nats-server -js -sd "$S/n2" -a 127.0.0.1 -p 4284 > /dev/null 2>&1 & NP2=$!
sleep 0.5
NATS_URL=nats://127.0.0.1:4284 VEJAS_ROOT="$R" VEJAS_HTTP_ADDR=127.0.0.1:8756 VEJAS_TRACES=off \
VEJAS_SECRET_DEMO_KEY="$VAULT_VALUE" \
  "$BIN" > "$S/rt2.log" 2>&1 & RT2=$!
until curl -sf -o /dev/null "http://127.0.0.1:8756/healthz"; do sleep 0.1; done
sleep 1
grep -q "trace record off" "$S/rt2.log" && ok "the runtime says so" || ko "no off line at boot"
nats -s nats://127.0.0.1:4284 stream info VEJAS_TRACES > /dev/null 2>&1 && ko "the stream was created" \
  || ok "no stream"

[ $fail -eq 0 ] && echo "trace record: all checks passed" || { echo "trace record: FAILED"; tail -20 "$S/rt.log"; exit 1; }
