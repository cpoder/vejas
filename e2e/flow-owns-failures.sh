#!/bin/bash
# A flow owns its failures and its duplicates (ADR-0039, points 3 and 4). A
# `catch` takes a call's failure once no later delivery may change it: on the
# delivery that spends the service's attempts, or at once when the service
# does not try again or the code fails. The flow publishes its own reject,
# and the message is no dead letter. What the `try` sent before the failure
# is dropped, and the trace record says the failure was caught. `seen` reads
# the keys the flow's runs remembered, so a duplicate makes no call;
# `remember` marks a key once the run's emits are stored, in a bucket of the
# flow's that keeps it as long as the longest window. A run whose call failed
# marks nothing, so the same key is processed again later. Copies of one
# message find the first one's mark. An api flow remembers too, and
# `remember` is a write for the write gate. A replay of a flow's dead letters
# sends them back in the order their events came, not the order they died.
#   e2e/flow-owns-failures.sh                    (needs the nats CLI)
set -uo pipefail
cd "$(dirname "$0")/.."
BIN="${BIN:-$PWD/core/target/release/vejas-runtime}"
[ -x "$BIN" ] || { echo "build first" >&2; exit 1; }
S=$(mktemp -d); R=$(mktemp -d)
RT=""; NP=""; SUB=""; SYS=""
trap 'kill $RT $NP $SUB $SYS 2>/dev/null; wait $RT $NP $SUB $SYS 2>/dev/null; rm -rf "$S" "$R"' EXIT
fail=0
ok(){ echo "  ✓ $1"; }
ko(){ echo "  ✗ $1"; fail=1; }

PORT=4304; URL=nats://127.0.0.1:$PORT; H="http://127.0.0.1:8778"; SP=8779
TOK="e2e-owns-$$"

# A system that answers 503 for an id with DOWN in it, 404 for one with GONE,
# and the quote otherwise, or for every id once calls.up exists. It writes
# down each id it is asked for.
cat > "$S/system.py" <<'PY'
import http.server, json, os, sys
calls = sys.argv[2]
class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        qid = self.path.rsplit("/", 1)[-1]
        with open(calls, "a") as f:
            f.write(qid + "\n")
        up = os.path.exists(calls + ".up")
        status = 200 if up else 503 if "DOWN" in qid else 404 if "GONE" in qid else 200
        body = json.dumps({"quote": {"id": qid}} if status == 200 else {"error": "no"}).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *args):
        pass
http.server.ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), Handler).serve_forever()
PY
touch "$S/calls"
python3 "$S/system.py" $SP "$S/calls" & SYS=$!
calls(){ grep -cx "$1" "$S/calls"; }

mkdir -p "$R/flows" "$R/services"
# today's path, not the hold (ADR-0040): e2e/flow-holds.sh tries the hold
cat > "$R/services/get_quote.vjs" <<V
driver "http-rpc"
URL = "http://127.0.0.1:$SP/quotes/{id}"
ATTEMPTS = 3
DELAY_MS = 200
WHEN_DOWN = "dead-letter"
V
# the same system, a second apart: a message it fails dies on its second delivery
cat > "$R/services/slow_quote.vjs" <<V
driver "http-rpc"
URL = "http://127.0.0.1:$SP/quotes/{id}"
ATTEMPTS = 2
DELAY_MS = 1000
WHEN_DOWN = "dead-letter"
V
cat > "$R/flows/ordered.vjs" <<'V'
# flow: ordered
source "vx.fo.ord"
o = invoke slow_quote(id: event.id)
emit "vx.fo.ordered", {id: event.id}
V
cat > "$R/flows/refine.vjs" <<'V'
# flow: refine
source "vx.fo.in"
KEEP = "7d"
if seen(event.key):
    emit "vx.fo.duplicate", {key: event.key}
else:
    try:
        o = invoke get_quote(id: event.id)
        emit "vx.fo.refined", {id: event.id, quote: o.body.quote.id}
        remember event.key for KEEP
    catch e:
        code = e.status ?? "NETWORK"
        emit "vx.fo.rejected", {id: event.id, code: f"API_HTTP_{code}", service: e.service, attempts: e.attempts}
    end
end
V
cat > "$R/flows/dedupe.vjs" <<'V'
# flow: dedupe
source "vx.fo.d"
if seen(event.key):
    emit "vx.fo.d_dup", {key: event.key}
else:
    emit "vx.fo.d_new", {key: event.key}
    remember event.key for "1h"
end
V
cat > "$R/flows/code.vjs" <<'V'
# flow: code
source "vx.fo.code"
try:
    emit "vx.fo.code_inside", {n: event.n}
    x = event.n.deep
catch e:
    emit "vx.fo.code_caught", {error: e.error, attempts: e.attempts}
end
V
cat > "$R/flows/intake.vjs" <<'V'
# flow: intake
api "POST /intake"
if seen(event.key):
    respond 200, {duplicate: true}
else:
    emit "vx.fo.taken", {key: event.key}
    remember event.key for "1h"
    respond 202, {duplicate: false}
end
V
cat > "$R/flows/peek.vjs" <<'V'
# flow: peek
api "GET /peek/{k}"
remember k for "1h"
respond 200, {k: k}
V
cat > "$R/flows/ping.vjs" <<'V'
# flow: ping
api "GET /ping"
respond 200, {pong: true}
V
cat > "$R/flows/windows.vjs" <<'V'
# flow: windows
source "vx.fo.w"
if event.long:
    remember event.key for "2d"
else:
    remember event.key for "1h"
end
emit "vx.fo.w_done", {key: event.key}
V

nats-server -js -sd "$S/n" -a 127.0.0.1 -p $PORT > /dev/null 2>&1 & NP=$!
for _ in $(seq 1 50); do nats -s $URL server check connection > /dev/null 2>&1 && break; sleep 0.1; done
NATS_URL=$URL VEJAS_ROOT="$R" VEJAS_HTTP_ADDR=127.0.0.1:8778 VEJAS_TOKEN="$TOK" VEJAS_ACK_WAIT_SECS=2 \
  "$BIN" > "$S/rt.log" 2>&1 & RT=$!
until curl -sf -o /dev/null "$H/healthz"; do sleep 0.1; done
for _ in $(seq 1 100); do [ "$(grep -c 'consuming vx.fo' "$S/rt.log")" -ge 5 ] && break; sleep 0.1; done
nats -s $URL sub 'vx.fo.>' > "$S/bus" 2>/dev/null & SUB=$!
sleep 0.5

# The payloads sent to one subject, one per line.
msgs(){ python3 - "$S/bus" "$1" <<'PY'
import sys
subject, at = sys.argv[2], None
for line in open(sys.argv[1], errors="replace").read().split("\n"):
    if line.startswith("[#") and "Received on" in line:
        at = line.split('"')[1]
    elif at is not None and line.startswith("{"):
        if at == subject:
            print(line)
        at = None
PY
}
count(){ msgs "$1" | grep -c "$2"; }
until_sent(){ for _ in $(seq 1 100); do [ "$(count "$1" "$2")" -ge "${3:-1}" ] && return 0; sleep 0.1; done; return 1; }
hops(){ python3 -c 'import json, sys
try: print(len(json.load(open(sys.argv[1])).get("hops", [])))
except Exception: print(0)' "$1"; }
traced(){ for _ in $(seq 1 100); do curl -s "$H/traces/$1" > "$2"; [ "$(hops "$2")" -ge "$3" ] && break; sleep 0.1; done; }
letters(){ curl -s -m 5 "$H/dlq" | python3 -c 'import json,sys; print(" ".join(d.get("unit","?") for d in json.load(sys.stdin)["dead_letters"]))'; }

echo "── a catch takes a call's failure once no later delivery may change it"
TD=4a1b2c3d4e5f60718293a4b5c6d7e801; PD=0a1b2c3d4e5f6a01
nats -s $URL pub vx.fo.in '{"id":"Q-DOWN-1","key":"k-down"}' -H "traceparent:00-$TD-$PD-01" > /dev/null 2>&1
until_sent vx.fo.rejected Q-DOWN-1
A=$(msgs vx.fo.rejected | grep Q-DOWN-1)
[ "$(calls Q-DOWN-1)" = 3 ] && echo "$A" | grep -q '"code":"API_HTTP_503"' && echo "$A" | grep -q '"attempts":3' \
  && echo "$A" | grep -q '"service":"get_quote"' && [ "$(count vx.fo.refined Q-DOWN-1)" = 0 ] \
  && ok "the system down: 3 calls, one per delivery, then the flow's own reject: $A" \
  || ko "down: $(calls Q-DOWN-1) calls, rejected: $A"
sleep 2.5
[ "$(count vx.fo.rejected Q-DOWN-1)" = 1 ] && ! letters | grep -q 'flow:refine' \
  && ok "the message is done: no dead letter, no fourth delivery" || ko "after the catch: $(count vx.fo.rejected Q-DOWN-1) rejects, letters: $(letters)"
traced $TD "$S/down.json" 3
python3 - "$S/down.json" <<'PY' && ok "the trace: two failed deliveries, then a third whose step says the catch took the call's failure" || ko "trace of the down call: $(cut -c1-600 "$S/down.json")"
import json, sys
hops = [h for h in json.load(open(sys.argv[1]))["hops"] if h.get("unit") == "flow:refine"]
hops.sort(key=lambda h: h["start_us"])
assert [h["ok"] for h in hops] == [False, False, True], [h["ok"] for h in hops]
steps = hops[-1].get("steps", [])
assert any(s.get("caught") is True for s in steps), steps
assert not any(s.get("failed") for s in steps), steps
assert "vejas.remembered" not in hops[-1].get("attributes", {}), hops[-1].get("attributes")
PY
nats -s $URL pub vx.fo.in '{"id":"Q-OK-1","key":"k-down"}' > /dev/null 2>&1
until_sent vx.fo.refined Q-OK-1
[ "$(count vx.fo.refined Q-OK-1)" = 1 ] && [ "$(count vx.fo.duplicate k-down)" = 0 ] \
  && ok "a run whose call failed marked nothing: the same key, sent again, is processed" \
  || ko "after a failure: $(count vx.fo.refined Q-OK-1) refined, $(count vx.fo.duplicate k-down) duplicate"
nats -s $URL pub vx.fo.in '{"id":"Q-GONE-1","key":"k-gone"}' > /dev/null 2>&1
until_sent vx.fo.rejected Q-GONE-1
A=$(msgs vx.fo.rejected | grep Q-GONE-1)
[ "$(calls Q-GONE-1)" = 1 ] && echo "$A" | grep -q '"code":"API_HTTP_404"' && echo "$A" | grep -q '"attempts":1' \
  && ok "a status the service does not try again: one call, the reject at once" || ko "gone: $(calls Q-GONE-1) calls, $A"

echo "── seen reads what the flow's runs remembered"
TK=4a1b2c3d4e5f60718293a4b5c6d7e802; PK=0a1b2c3d4e5f6a02
nats -s $URL pub vx.fo.in '{"id":"Q-OK-2","key":"k-dup"}' -H "traceparent:00-$TK-$PK-01" > /dev/null 2>&1
until_sent vx.fo.refined Q-OK-2
nats -s $URL pub vx.fo.in '{"id":"Q-OK-2","key":"k-dup"}' > /dev/null 2>&1
until_sent vx.fo.duplicate k-dup
[ "$(count vx.fo.refined Q-OK-2)" = 1 ] && [ "$(count vx.fo.duplicate k-dup)" = 1 ] && [ "$(calls Q-OK-2)" = 1 ] \
  && ok "a duplicate: the flow's own answer, and no call" \
  || ko "duplicate: $(count vx.fo.refined Q-OK-2) refined, $(count vx.fo.duplicate k-dup) duplicate, $(calls Q-OK-2) calls"
traced $TK "$S/dup.json" 1
python3 - "$S/dup.json" <<'PY' && ok "the trace record counts the key the run remembered" || ko "trace of the first copy: $(cut -c1-400 "$S/dup.json")"
import json, sys
hop = next(h for h in json.load(open(sys.argv[1]))["hops"] if h.get("unit") == "flow:refine")
assert hop["ok"] and hop["attributes"].get("vejas.remembered") == 1, hop.get("attributes")
PY
nats -s $URL pub vx.fo.d '{"key":"same"}' --count 5 > /dev/null 2>&1
until_sent vx.fo.d_dup same 4
[ "$(count vx.fo.d_new same)" = 1 ] && [ "$(count vx.fo.d_dup same)" = 4 ] \
  && ok "five copies sent at once: one is new, four are duplicates" \
  || ko "copies: $(count vx.fo.d_new same) new, $(count vx.fo.d_dup same) duplicates"
age(){ nats -s $URL stream ls -n 2>/dev/null | grep "^KV_VEJAS_SEEN_flow_$1_" | head -1 \
  | xargs -r -I{} nats -s $URL stream info {} -j 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["config"]["max_age"] // 10**9)' 2>/dev/null; }
[ "$(age dedupe)" = 3600 ] && [ "$(age refine)" = 604800 ] \
  && ok "each flow has its bucket, which keeps a key as long as the flow's window" || ko "buckets: dedupe $(age dedupe) s, refine $(age refine) s"
nats -s $URL pub vx.fo.w '{"key":"w1"}' > /dev/null 2>&1
until_sent vx.fo.w_done w1
W1=$(age windows)
nats -s $URL pub vx.fo.w '{"key":"w2","long":true}' > /dev/null 2>&1
until_sent vx.fo.w_done w2
sleep 0.3
[ "$W1" = 3600 ] && [ "$(age windows)" = 172800 ] \
  && ok "a longer window makes the bucket keep its keys longer: $W1 s, then $(age windows) s" || ko "windows: $W1 s, then $(age windows) s"

echo "── a failure in the code is taken at once"
nats -s $URL pub vx.fo.code '{"n":5}' > /dev/null 2>&1
until_sent vx.fo.code_caught attempts
sleep 2.5
A=$(msgs vx.fo.code_caught)
[ "$(count vx.fo.code_caught attempts)" = 1 ] && echo "$A" | grep -q '"attempts":1' && echo "$A" | grep -q 'cannot read field' \
  && [ "$(count vx.fo.code_inside n)" = 0 ] && ! letters | grep -q 'flow:code' \
  && ok "caught on the first delivery, what the try sent dropped, the message done" \
  || ko "code: $A; inside $(count vx.fo.code_inside n); letters $(letters)"

echo "── an api flow remembers, and remember is a write"
post(){ curl -s -m 10 -w ' %{http_code}' "$@"; }
A=$(post -X POST -H "Authorization: Bearer $TOK" -H 'Content-Type: application/json' -d '{"key":"a1"}' "$H/api/intake")
B=$(post -X POST -H "Authorization: Bearer $TOK" -H 'Content-Type: application/json' -d '{"key":"a1"}' "$H/api/intake")
sleep 0.3
[ "$A" = '{"duplicate":false} 202' ] && [ "$B" = '{"duplicate":true} 200' ] && [ "$(count vx.fo.taken a1)" = 1 ] \
  && ok "the second request with the key is a duplicate: $A, then $B" || ko "api: $A, then $B, taken $(count vx.fo.taken a1)"
C=$(curl -s -o /dev/null -w "%{http_code}" "$H/api/peek/x")
D=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer $TOK" "$H/api/peek/x")
[ "$C" = 401 ] && [ "$D" = 200 ] && ok "a GET that remembers needs the token: $C without, $D with" || ko "gate: $C without, $D with"
P=$(curl -s -o /dev/null -w "%{http_code}" "$H/api/ping")
[ "$P" = 200 ] && ! nats -s $URL stream ls -n 2>/dev/null | grep -q "_ping_" \
  && ok "an api flow that remembers nothing opens no memory" || ko "ping: $P, $(nats -s $URL stream ls -n 2>/dev/null | tr '\n' ' ')"

echo "── a replay sends letters back in the order their events came"
# o1 is not found and dies at once, o2's system is down and it dies on its
# second delivery a second later, o3 dies at once in between: the letters are
# o1, o3, o2, newest first o2, o3, o1, and neither is the order they came in.
for id in o1-GONE o2-DOWN o3-GONE; do nats -s $URL pub vx.fo.ord "{\"id\":\"$id\"}" > /dev/null 2>&1; done
for _ in $(seq 1 100); do [ "$(letters | tr ' ' '\n' | grep -cx flow:ordered)" -ge 3 ] && break; sleep 0.1; done
died(){ curl -s -m 5 "$H/dlq" | python3 -c 'import json,sys
ls = [d for d in json.load(sys.stdin)["dead_letters"] if d.get("unit") == "flow:ordered"]
print(" ".join(json.loads(d["payload"])["id"] for d in reversed(ls)))'; }
DIED=$(died)
touch "$S/calls.up"
A=$(curl -s -m 10 -X POST -H "Authorization: Bearer $TOK" -H 'Content-Type: application/json' -d '{"unit":"flow:ordered"}' "$H/dlq/replay")
until_sent vx.fo.ordered '"id"' 3
GOT=$(msgs vx.fo.ordered | python3 -c 'import json,sys; print(" ".join(json.loads(l)["id"] for l in sys.stdin))')
[ "$DIED" = "o1-GONE o3-GONE o2-DOWN" ] && echo "$A" | grep -q '"replayed":3' && [ "$GOT" = "o1-GONE o2-DOWN o3-GONE" ] \
  && ok "they died $DIED, and their replay ran $GOT" || ko "replay order: died $DIED, replay $A, ran $GOT"

echo "── the bus is down"
kill $NP 2>/dev/null; wait $NP 2>/dev/null; NP=""
sleep 0.5
T0=$(date +%s)
A=$(post -X POST -H "Authorization: Bearer $TOK" -H 'Content-Type: application/json' -d '{"key":"a2"}' "$H/api/intake")
T1=$(date +%s)
P=$(curl -s -m 10 -w ' %{http_code}' "$H/api/ping")
echo "$A" | grep -q ' 503$' && echo "$A" | grep -q 'seen()' && [ $((T1 - T0)) -le 3 ] && [ "$P" = '{"pong":true} 200' ] \
  && ok "seen answers 503 at once, the caller sends again; a flow without memory still answers" \
  || ko "bus down: intake $A in $((T1 - T0)) s, ping $P"

[ $fail = 0 ] && echo "A FLOW OWNS ITS FAILURES AND ITS DUPLICATES" || { echo "FLOW OWNS FAILURES REGRESSION — do not ship"; tail -20 "$S/rt.log"; exit 1; }
