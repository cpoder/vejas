#!/bin/bash
# A dead letter announces itself on vx.dlq.events (ADR-0015 Amendment A): metadata
# only, one event per unit per window with `count` for the letters it stands for,
# through an ordinary sink, and a failing reader of the events cannot feed itself.
#   e2e/dlq-events.sh                        (needs the nats CLI)
set -uo pipefail
cd "$(dirname "$0")/.."
BIN="$PWD/core/target/release/vejas-runtime"
[ -x "$BIN" ] || { echo "build first" >&2; exit 1; }
S=$(mktemp -d); R=$(mktemp -d)
RT=""; NP=""; SUB=""; HOOK=""
trap 'kill $RT $NP $SUB $HOOK 2>/dev/null; wait $RT $NP $SUB $HOOK 2>/dev/null; rm -rf "$S" "$R"' EXIT
fail=0
ok(){ echo "  ✓ $1"; }
ko(){ echo "  ✗ $1"; fail=1; }

URL=nats://127.0.0.1:4280; H="http://127.0.0.1:8752"; HOOK_PORT=18791
mkdir -p "$R/flows" "$R/connectors"
printf '# flow: demo\nsource "vx.demo.in"\nemit "vx.demo.out", {id: id}\n' > "$R/flows/demo.vjs"
# a reader of the events that fails on every one of them
printf '# flow: watcher\nsource "vx.dlq.events"\nx = secret("missing/x")\nemit "vx.demo.out", {x: x}\n' > "$R/flows/watcher.vjs"
printf '# connector: hook\ndriver "http-out"\nSUBJECT = "vx.dlq.events"\nURL = "http://127.0.0.1:%s/hook"\n' $HOOK_PORT \
  > "$R/connectors/hook.vjs"

python3 -c '
import http.server, sys
class H(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("content-length", 0)))
        open(sys.argv[2], "ab").write(body + b"\n")
        self.send_response(200); self.send_header("Content-Length", "0"); self.end_headers()
    def log_message(self, *a): pass
http.server.HTTPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
' $HOOK_PORT "$S/hook.out" & HOOK=$!

nats-server -js -sd "$S/n" -a 127.0.0.1 -p 4280 > /dev/null 2>&1 & NP=$!
sleep 0.5
NATS_URL=$URL VEJAS_ROOT="$R" VEJAS_HTTP_ADDR=127.0.0.1:8752 VEJAS_ACK_WAIT_SECS=1 VEJAS_DLQ_EVENT_SECS=2 \
  "$BIN" > "$S/rt.log" 2>&1 & RT=$!
until curl -sf -o /dev/null "$H/healthz"; do sleep 0.1; done
for _ in $(seq 1 100); do
  curl -s -m 5 -H 'Content-Type: application/json' \
    -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"vejas_topology","arguments":{}}}' "$H/mcp" \
    | grep -q 'flow:demo.*running\|running.*flow:demo' && break
  sleep 0.1
done
sleep 1
nats -s $URL sub vx.dlq.events --raw > "$S/events" 2>/dev/null & SUB=$!
sleep 0.5
counted(){ python3 -c '
import json, sys
print(sum(json.loads(l).get("count", 0) for l in open(sys.argv[1]) if l.startswith("{") and json.loads(l).get("unit") == sys.argv[2]))
' "$S/events" "$1"; }

echo "── one poison message: announced at once, metadata only"
nats -s $URL pub vx.demo.in 'CANARY-not-json' > /dev/null 2>&1
for _ in $(seq 1 50); do grep -q 'dead-lettered' "$S/events" && break; sleep 0.1; done
grep -q '"unit":"flow:demo"' "$S/events" && ok "the event names the unit" || ko "no event: $(cut -c1-120 "$S/events")"
grep -q '"count":1' "$S/events" && ok "it counts one letter" || ko "wrong count"
grep -q 'CANARY' "$S/events" && ko "the payload reached the event" || ok "the payload stays in the DLQ"
grep -q 'bad json' "$S/events" && ko "the error text reached the event" || ok "the error text stays in the DLQ"

echo "── a burst: one event, nothing lost"
for i in $(seq 1 20); do nats -s $URL pub vx.demo.in "poison-$i" > /dev/null 2>&1; done
LINES_DURING=$(grep -c 'flow:demo' "$S/events")
[ "$LINES_DURING" -le 3 ] && ok "$LINES_DURING event(s) during 20 letters" || ko "$LINES_DURING events for 20 letters"
for _ in $(seq 1 60); do [ "$(counted flow:demo)" = "21" ] && break; sleep 0.1; done
[ "$(counted flow:demo)" = "21" ] && ok "the counts add up to all 21 letters" || ko "the counts add up to $(counted flow:demo)"

echo "── an ordinary sink carries it"
for _ in $(seq 1 50); do grep -q 'dead-lettered' "$S/hook.out" 2>/dev/null && break; sleep 0.1; done
grep -q 'dead-lettered' "$S/hook.out" 2>/dev/null && ok "http-out posted the event" || ko "the sink got nothing"

echo "── a failing reader does not feed itself"
watcher_letters(){ curl -s -m 5 "$H/dlq" | python3 -c '
import json, sys
print(len([d for d in json.load(sys.stdin)["dead_letters"] if d.get("unit") == "flow:watcher"]))'; }
for _ in $(seq 1 200); do [ "$(watcher_letters)" -ge 1 ] && break; sleep 0.1; done
[ "$(watcher_letters)" -ge 1 ] && ok "the watcher dead-lettered the events it failed on" || ko "the watcher never dead-lettered"
sleep 4
grep -q '"unit":"flow:watcher"' "$S/events" && ko "a failing reader announced its own letters" || ok "its letters are not announced"

[ $fail -eq 0 ] && echo "DLQ EVENTS HOLD" || echo "DLQ EVENTS REGRESSION — do not ship"
exit $fail
