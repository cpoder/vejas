#!/bin/bash
# A flow holds its source while a system it calls is down (ADR-0040). A
# declared service holds by default: the message stays in hand, it is tried
# again `DELAY_MS` apart up to `ATTEMPTS` runs, then the flow is held and
# probes, its `HEALTH_URL` when it has one, until the system answers. No
# message behind it is called during the outage, none is a dead letter, and
# they all pass in order once the system is back. The hold is seen: the
# flow's state in /topology, `held`/`resumed`/`released` on vx.flow.events,
# a gauge in /metrics. Held longer than its `ack_wait`, the message is not
# delivered again. A person can release the held message, which is then a
# dead letter, or have the flow probe now; both take the write token. A
# status no later try changes is a dead letter at once, and a service that
# says `WHEN_DOWN = "dead-letter"` keeps today's path. A stop gives the held
# message back at once, and the next start runs it first.
#   e2e/flow-holds.sh                            (needs the nats CLI)
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

PORT=4306; URL=nats://127.0.0.1:$PORT; HP=8790; H="http://127.0.0.1:$HP"; SP=8791
TOK="e2e-holds-$$"

# A system that is down (503) until the file up exists, answers 404 for an id
# with GONE in it, and has a health URL. It writes down each path asked.
cat > "$S/system.py" <<'PY'
import http.server, json, os, sys
calls, up = sys.argv[2], sys.argv[2] + ".up"
class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        with open(calls, "a") as f:
            f.write(self.path + "\n")
        live = os.path.exists(up)
        if self.path == "/health":
            status = 200 if live else 503
        else:
            status = 404 if "GONE" in self.path else 200 if live else 503
        body = json.dumps({"ok": status == 200}).encode()
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
down(){ rm -f "$S/calls.up"; }
up(){ touch "$S/calls.up"; }

mkdir -p "$R/flows" "$R/services"
# no WHEN_DOWN: the default holds
cat > "$R/services/pay.vjs" <<V
driver "http-rpc"
URL = "http://127.0.0.1:$SP/pay/{id}"
ATTEMPTS = 2
DELAY_MS = 300
PROBE_MAX_MS = 1000
V
cat > "$R/services/pay_health.vjs" <<V
driver "http-rpc"
BASE_URL = "http://127.0.0.1:$SP"
URL = "{BASE_URL}/pay/{id}"
HEALTH_URL = "{BASE_URL}/health"
WHEN_DOWN = "hold"
ATTEMPTS = 2
DELAY_MS = 300
PROBE_MAX_MS = 600
V
cat > "$R/services/pay_dl.vjs" <<V
driver "http-rpc"
URL = "http://127.0.0.1:$SP/pay/{id}"
WHEN_DOWN = "dead-letter"
ATTEMPTS = 2
DELAY_MS = 200
V
cat > "$R/flows/pay.vjs" <<'V'
# flow: pay
source "vx.fh.pay"
o = invoke pay(id: event.id)
emit "vx.fh.paid", {id: event.id}
V
cat > "$R/flows/payh.vjs" <<'V'
# flow: payh
source "vx.fh.payh"
o = invoke pay_health(id: event.id)
emit "vx.fh.paidh", {id: event.id}
V
cat > "$R/flows/payd.vjs" <<'V'
# flow: payd
source "vx.fh.payd"
o = invoke pay_dl(id: event.id)
emit "vx.fh.paidd", {id: event.id}
V

nats-server -js -sd "$S/n" -a 127.0.0.1 -p $PORT > /dev/null 2>&1 & NP=$!
for _ in $(seq 1 50); do nats -s $URL server check connection > /dev/null 2>&1 && break; sleep 0.1; done
start(){
  NATS_URL=$URL VEJAS_ROOT="$R" VEJAS_HTTP_ADDR=127.0.0.1:$HP VEJAS_TOKEN="$TOK" VEJAS_ACK_WAIT_SECS=4 \
    VEJAS_DLQ_EVENT_SECS=0 "$BIN" >> "$S/rt.log" 2>&1 & RT=$!
  until curl -sf -o /dev/null "$H/healthz"; do sleep 0.1; done
  for _ in $(seq 1 100); do [ "$(grep -c 'consuming vx.fh' "$S/rt.log")" -ge "$1" ] && break; sleep 0.1; done
}
start 3
nats -s $URL sub 'vx.>' > "$S/bus" 2>/dev/null & SUB=$!
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
until_sent(){ for _ in $(seq 1 150); do [ "$(count "$1" "$2")" -ge "${3:-1}" ] && return 0; sleep 0.1; done; return 1; }
ids(){ msgs "$1" | python3 -c 'import json,sys; print(" ".join(json.loads(l)["id"] for l in sys.stdin))'; }
letters(){ curl -s -m 5 "$H/dlq" | python3 -c 'import json,sys
print(" ".join(json.loads(d["payload"]).get("id", "?") for d in json.load(sys.stdin)["dead_letters"]))'; }
hold(){ curl -s -m 5 "$H/topology" | python3 -c 'import json,sys
f = next(f for f in json.load(sys.stdin)["flows"] if f["name"] == sys.argv[1])
h = f.get("hold") or {}
print(f["status"], h.get("service"), h.get("behind"), h.get("subject"), h.get("probes", 0) > 0 or h.get("tries", 0) > 1)' "$1"; }
held_until(){ for _ in $(seq 1 100); do hold "$1" | grep -q "^held" && return 0; sleep 0.1; done; return 1; }
event(){ msgs vx.flow.events | python3 -c 'import json,sys
n = 0
for l in sys.stdin:
    e = json.loads(l)
    n += e["transition"] == sys.argv[1] and e["unit"] == sys.argv[2]
print(n)' "$1" "$2"; }
gauge(){ curl -s "$H/metrics" | grep "^vejas_flow_held{unit=\"$1\"" | awk '{print $2}'; }
act(){ curl -s -m 5 -w ' %{http_code}' -X POST -H 'Content-Type: application/json' "$@"; }

echo "── a system down: the flow holds its message, and the others wait"
down
for id in p1 p2 p3; do nats -s $URL pub vx.fh.pay "{\"id\":\"$id\"}" > /dev/null 2>&1; done
nats -s $URL pub vx.fh.payh '{"id":"h1"}' > /dev/null 2>&1
held_until flow:pay && held_until flow:payh
sleep 5   # longer than the ack wait of 4 s
STATE=$(hold flow:pay)
[ "$(echo "$STATE" | cut -d' ' -f1-4)" = "held pay 2 vx.fh.pay" ] \
  && ok "the flow says it holds: $STATE" || ko "state: $STATE"
[ "$(calls /pay/p2)" = 0 ] && [ "$(calls /pay/p3)" = 0 ] && [ -z "$(letters)" ] \
  && ok "no message behind it is called, none is a dead letter" \
  || ko "during the outage: p2 $(calls /pay/p2), p3 $(calls /pay/p3) calls, letters: $(letters)"
P1=$(calls /pay/p1)
[ "$P1" -ge 3 ] && ok "the held message is tried in hand, then probed: $P1 calls" || ko "p1: $P1 calls"
[ "$(calls /pay/h1)" = 2 ] && [ "$(calls /health)" -ge 2 ] \
  && ok "with a HEALTH_URL, the probes ask it and the message waits: $(calls /pay/h1) runs, $(calls /health) probes" \
  || ko "health: h1 $(calls /pay/h1), health $(calls /health)"
[ "$(event held flow:pay)" = 1 ] && [ "$(gauge flow:pay)" = 1 ] \
  && ok "announced on vx.flow.events and in /metrics" || ko "events $(event held flow:pay), gauge $(gauge flow:pay)"
MAP=$(nats -s $URL consumer info VEJAS flow_pay -j 2>/dev/null | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["config"].get("max_ack_pending"), d["num_redelivered"])')
[ "$MAP" = "1 0" ] && ok "one message at a time across instances, and the held one never delivered again" || ko "consumer: $MAP"

echo "── the system is back: in order, the held one first"
up
until_sent vx.fh.paid p3 && until_sent vx.fh.paidh h1
sleep 0.3
[ "$(ids vx.fh.paid)" = "p1 p2 p3" ] && [ "$(calls /pay/p2)" = 1 ] && [ "$(calls /pay/p3)" = 1 ] \
  && ok "p1, p2, p3 passed in order, one call each behind the held one" || ko "after: $(ids vx.fh.paid); p2 $(calls /pay/p2), p3 $(calls /pay/p3)"
[ "$(event resumed flow:pay)" = 1 ] && [ "$(event resumed flow:payh)" = 1 ] && [ "$(gauge flow:pay)" = 0 ] \
  && [ "$(hold flow:pay | cut -d' ' -f1-2)" = "running None" ] \
  && ok "resumed: announced, the gauge down, the state clear" || ko "resumed $(event resumed flow:pay), gauge $(gauge flow:pay), $(hold flow:pay)"

echo "── a person releases the held message, or has the flow probe now"
down
A=$(act -H "Authorization: Bearer $TOK" -d '{"flow":"flow:pay"}' "$H/hold/release")
echo "$A" | grep -q ' 409$' && ok "nothing held, nothing to release: 409" || ko "release with nothing held: $A"
nats -s $URL pub vx.fh.pay '{"id":"r1"}' > /dev/null 2>&1
held_until flow:pay
B=$(calls /pay/r1)
A=$(act -d '{"flow":"flow:pay"}' "$H/hold/probe")
echo "$A" | grep -q ' 401$' && ok "without the token: 401" || ko "probe without token: $A"
A=$(act -H "Authorization: Bearer $TOK" -d '{"flow":"flow:pay"}' "$H/hold/probe")
sleep 0.4
echo "$A" | grep -q ' 200$' && [ "$(calls /pay/r1)" -gt "$B" ] && ok "probe now: one more try at once" || ko "probe now: $A, calls $B then $(calls /pay/r1)"
A=$(act -H "Authorization: Bearer $TOK" -d '{"flow":"flow:pay"}' "$H/hold/release")
for _ in $(seq 1 50); do letters | grep -q r1 && break; sleep 0.1; done
echo "$A" | grep -q ' 200$' && letters | grep -q r1 && [ "$(event released flow:pay)" = 1 ] \
  && [ "$(hold flow:pay | cut -d' ' -f1)" = running ] \
  && ok "released: a dead letter now, announced, and the flow runs again" || ko "release: $A, letters $(letters), $(hold flow:pay)"
AUD=$(nats -s $URL stream info VEJAS_AUDIT -j 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["state"]["messages"])')
[ "$AUD" = 2 ] && ! grep -q "audit not written" "$S/rt.log" \
  && ok "the probe and the release are in the audit" || ko "audit: $AUD records"

echo "── what the hold does not take"
nats -s $URL pub vx.fh.pay '{"id":"GONE-1"}' > /dev/null 2>&1
for _ in $(seq 1 50); do letters | grep -q GONE-1 && break; sleep 0.1; done
[ "$(calls /pay/GONE-1)" = 1 ] && [ "$(hold flow:pay | cut -d' ' -f1)" = running ] \
  && ok "a 404 no later try changes: one call, a dead letter at once, no hold" || ko "404: $(calls /pay/GONE-1) calls, $(hold flow:pay), letters $(letters)"
nats -s $URL pub vx.fh.payd '{"id":"d1"}' > /dev/null 2>&1
for _ in $(seq 1 60); do letters | grep -q d1 && break; sleep 0.1; done
[ "$(calls /pay/d1)" = 2 ] && [ "$(event held flow:payd)" = 0 ] && letters | grep -q d1 \
  && ok "WHEN_DOWN = \"dead-letter\": today's path, two deliveries then a dead letter" \
  || ko "dead-letter service: $(calls /pay/d1) calls, held $(event held flow:payd), letters $(letters)"

echo "── a stop gives the held message back at once, and the next start runs it first"
nats -s $URL pub vx.fh.pay '{"id":"s1"}' > /dev/null 2>&1
nats -s $URL pub vx.fh.pay '{"id":"s2"}' > /dev/null 2>&1
held_until flow:pay
kill -TERM $RT; wait $RT 2>/dev/null; RT=""
# Well within its ack wait, the held message is there for the next reader at
# once (read here and not acked: it comes back after the ack wait).
NEXT=$(nats -s $URL consumer next VEJAS flow_pay --no-ack --timeout 1s 2>/dev/null | tr '\n' ' ')
echo "$NEXT" | grep -q '"id":"s1"' && ok "stopped: the held message went back at once" || ko "after the stop: $NEXT"
up
start 3
until_sent vx.fh.paid s2
[ "$(ids vx.fh.paid | tr ' ' '\n' | grep '^s' | tr '\n' ' ')" = "s1 s2 " ] \
  && ok "the next start ran s1, then s2" || ko "after the restart: $(ids vx.fh.paid)"

[ $fail = 0 ] && echo "A FLOW HOLDS ITS SOURCE WHILE A SYSTEM IS DOWN" || { echo "FLOW HOLDS REGRESSION — do not ship"; tail -30 "$S/rt.log"; exit 1; }
