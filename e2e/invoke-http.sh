#!/bin/bash
# A flow invokes a service that answers from an HTTP API (ADR-0032). The answer
# reaches the flow; a call that gets no answer is tried again at the message's
# next delivery, after the service's delay and up to its attempts; one no later
# try can change is a dead letter at once; an api flow answers 502, or 504 after
# a timeout. The call carries the message's trace, and its error no URL. Calls
# that outlive ack_wait run once per message.
#   e2e/invoke-http.sh                       (needs the nats CLI)
set -uo pipefail
cd "$(dirname "$0")/.."
BIN="$PWD/core/target/release/vejas-runtime"
[ -x "$BIN" ] || { echo "build first" >&2; exit 1; }
S=$(mktemp -d); R=$(mktemp -d)
RT=""; NP=""; SUB=""; API=""
trap 'kill $RT $NP $SUB $API 2>/dev/null; wait $RT $NP $SUB $API 2>/dev/null; rm -rf "$S" "$R"' EXIT
fail=0
ok(){ echo "  ✓ $1"; }
ko(){ echo "  ✗ $1"; fail=1; }

URL=nats://127.0.0.1:4292; H="http://127.0.0.1:8763"; IN=18796; API_PORT=18797
mkdir -p "$R/flows" "$R/connectors" "$R/services"
printf '# connector: http_in\ndriver "http-in"\nPORT = %s\n' $IN > "$R/connectors/http_in.vjs"
# today's path, not the hold (ADR-0040): e2e/flow-holds.sh tries the hold
printf 'driver "http-rpc"\nBASE_URL = "http://127.0.0.1:%s"\nURL = "{BASE_URL}/orders/{id}"\nTIMEOUT_MS = 500\nATTEMPTS = 3\nDELAY_MS = 300\nWHEN_DOWN = "dead-letter"\n' \
  $API_PORT > "$R/services/get_order.vjs"
printf '# flow: enrich\nsource "vx.orders.raw"\no = invoke get_order(id: id)\nemit "vx.orders.ready", {id: id, state: o.body.status}\n' \
  > "$R/flows/enrich.vjs"
printf '# flow: lookup\napi "POST /lookup"\no = invoke get_order(id: id)\nrespond 200, {state: o.body.status}\n' \
  > "$R/flows/lookup.vjs"
# calls of 2.5 s, against the ack_wait of 1 s the runtime is started with
printf 'driver "http-rpc"\nBASE_URL = "http://127.0.0.1:%s"\nURL = "{BASE_URL}/orders/{id}"\nTIMEOUT_MS = 5000\n' \
  $API_PORT > "$R/services/get_lag.vjs"
printf '# flow: lag\nsource "vx.orders.lag"\no = invoke get_lag(id: id)\nemit "vx.orders.ready", {id: id}\n' > "$R/flows/lag.vjs"
# a key in the query, and nothing listening
printf 'driver "http-rpc"\nKEY = secret("orders/key")\nURL = "http://127.0.0.1:9/orders/{id}?api_key={KEY}"\nATTEMPTS = 1\n' \
  > "$R/services/get_keyed.vjs"
printf '# flow: keyed\napi "POST /keyed"\no = invoke get_keyed(id: id)\nrespond 200, {s: o.status}\n' > "$R/flows/keyed.vjs"

# The API: flaky-* fails twice then answers, down-* always 503, gone-* 404,
# slow-* answers after a second, lag-* after 2.5 s. It writes one line per
# call: id, traceparent.
python3 -c '
import http.server, sys, time, collections
calls = collections.Counter()
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        oid = self.path.rsplit("/", 1)[-1]
        calls[oid] += 1
        with open(sys.argv[2], "a") as f:
            f.write("%s %s\n" % (oid, self.headers.get("traceparent", "-")))
        status = 200
        if oid.startswith("flaky") and calls[oid] <= 2: status = 503
        if oid.startswith("down"): status = 503
        if oid.startswith("gone"): status = 404
        if oid.startswith("slow"): time.sleep(1)
        if oid.startswith("lag"): time.sleep(2.5)
        body = b"{\"status\":\"OK\"}" if status == 200 else b"{\"error\":\"no\"}"
        self.send_response(status); self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)
    def log_message(self, *a): pass
http.server.ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
' $API_PORT "$S/calls" & API=$!

nats-server -js -sd "$S/n" -a 127.0.0.1 -p 4292 > /dev/null 2>&1 & NP=$!
sleep 0.5
NATS_URL=$URL VEJAS_ROOT="$R" VEJAS_HTTP_ADDR=127.0.0.1:8763 VEJAS_ACK_WAIT_SECS=1 \
  VEJAS_SECRET_ORDERS_KEY=K3Y-e2e-SECRET "$BIN" > "$S/rt.log" 2>&1 & RT=$!
until curl -sf -o /dev/null "$H/healthz"; do sleep 0.1; done
send(){ curl -s -m 5 -X POST -H 'Content-Type: application/json' -d "{\"id\":\"$1\"}" "http://127.0.0.1:$IN/ingest/${2:-orders.raw}"; }
for _ in $(seq 1 100); do send warmup | grep -q published && break; sleep 0.1; done
nats -s $URL sub vx.orders.ready --raw > "$S/ready" 2>/dev/null & SUB=$!
sleep 0.5
# grep -c prints 0 itself when nothing matches; a missing file prints nothing
count(){ local n; n=$(grep -c "$1" "$2" 2>/dev/null); echo "${n:-0}"; }
calls(){ count "^$1 " "$S/calls"; }
ready(){ count "\"id\":\"$1\"" "$S/ready"; }
letter(){ curl -s -m 5 "$H/dlq" | python3 -c '
import json, sys
for d in json.load(sys.stdin)["dead_letters"]:
    if sys.argv[1] in json.dumps(d.get("payload")):
        print(d.get("attempts"), d.get("last_error")); break' "$1"; }

echo "── the answer reaches the flow, and the call carries the message's trace"
TRACE=$(send ok-1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["trace_id"])')
for _ in $(seq 1 50); do [ "$(ready ok-1)" = "1" ] && break; sleep 0.1; done
grep -q '"id":"ok-1","state":"OK"' "$S/ready" && ok "the flow emitted what the API answered" || ko "no emit: $(cat "$S/ready")"
grep "^ok-1 " "$S/calls" | grep -q -- "-$TRACE-" && ok "the API was called within the message's trace" \
  || ko "the call's traceparent: $(grep '^ok-1 ' "$S/calls") (trace $TRACE)"

echo "── no answer twice: tried again after the delay, then answered"
T0=$(date +%s%N); send flaky-1 > /dev/null
for _ in $(seq 1 100); do [ "$(ready flaky-1)" = "1" ] && break; sleep 0.1; done
MS=$(( ($(date +%s%N) - T0) / 1000000 ))
[ "$(ready flaky-1)" = "1" ] && ok "emitted after $(calls flaky-1) calls, in $MS ms" || ko "no emit after $(calls flaky-1) calls"
[ "$(calls flaky-1)" = "3" ] && ok "three calls" || ko "$(calls flaky-1) calls"
[ "$MS" -ge 600 ] && [ "$MS" -lt 1800 ] && ok "the deliveries waited the service's delay, not ack_wait" || ko "it took $MS ms"

echo "── never an answer: a dead letter after the service's attempts"
send down-1 > /dev/null
L=""; for _ in $(seq 1 100); do L=$(letter down-1); [ -n "$L" ] && break; sleep 0.1; done
echo "$L" | grep -q "get_order: HTTP 503" && ok "the letter names the service and the cause: $L" || ko "letter: '$L'"
sleep 1
[ "$(calls down-1)" = "3" ] && ok "three calls, as ATTEMPTS says" || ko "$(calls down-1) calls"
[ "$(ready down-1)" = "0" ] && ok "nothing emitted" || ko "it emitted"

echo "── an answer no later try can change: a dead letter at once"
send gone-1 > /dev/null
L=""; for _ in $(seq 1 100); do L=$(letter gone-1); [ -n "$L" ] && break; sleep 0.1; done
echo "$L" | grep -q "get_order: HTTP 404" && ok "dead letter: $L" || ko "letter: '$L'"
sleep 1
[ "$(calls gone-1)" = "1" ] && ok "one call" || ko "$(calls gone-1) calls"

echo "── an api flow answers what the call got"
api(){ curl -s -m 10 -o "$S/answer" -w '%{http_code}' -X POST -H 'Content-Type: application/json' -d "{\"id\":\"$1\"}" "$H/api/lookup"; }
C=$(api ok-api); [ "$C" = "200" ] && grep -q '"state":"OK"' "$S/answer" && ok "answered: $C" || ko "$C: $(cat "$S/answer")"
C=$(api down-api); [ "$C" = "502" ] && grep -q 'get_order: HTTP 503' "$S/answer" && ok "no answer: $C" || ko "$C: $(cat "$S/answer")"
C=$(api slow-api); [ "$C" = "504" ] && grep -q 'timeout after 500 ms' "$S/answer" && ok "too slow: $C" || ko "$C: $(cat "$S/answer")"

echo "── a call's error names no URL: the key in its query stays out"
C=$(curl -s -m 10 -o "$S/keyed" -w '%{http_code}' -X POST -H 'Content-Type: application/json' -d '{"id":"k1"}' "$H/api/keyed")
[ "$C" = "502" ] && ! grep -q 'K3Y-e2e-SECRET\|api_key' "$S/keyed" && ok "no answer, without the key: $(cat "$S/keyed")" || ko "$C: $(cat "$S/keyed")"
! grep -q 'K3Y-e2e-SECRET' "$S/rt.log" && ok "nor in the log" || ko "the key is in the log"

echo "── calls that outlive ack_wait: each message runs once"
for i in 1 2 3; do send "lag-$i" orders.lag > /dev/null; done
for _ in $(seq 1 150); do [ "$(count '"id":"lag-' "$S/ready")" -ge 3 ] && break; sleep 0.1; done
sleep 3  # time for a second run of one of them to call and emit again
[ "$(count '^lag-' "$S/calls")" = "3" ] && ok "three calls of 2.5 s, against an ack_wait of 1 s" || ko "$(count '^lag-' "$S/calls") calls"
[ "$(count '"id":"lag-' "$S/ready")" = "3" ] && ok "three emits" || ko "$(count '"id":"lag-' "$S/ready") emits"

[ $fail -eq 0 ] && echo "INVOKE HTTP HOLDS" || echo "INVOKE HTTP REGRESSION — do not ship"
exit $fail
