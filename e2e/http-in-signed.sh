#!/bin/bash
# Signed webhooks on http-in (ADR-0037): a request without its sender's
# signature gets 401 and never reaches the bus, and a signed one is published.
# GitHub's scheme signs the body, Stripe's a timestamp too, on two ports. A
# manifest that asks for a signature without its secret does not start.
#   e2e/http-in-signed.sh
set -uo pipefail
cd "$(dirname "$0")/.."
BIN="${BIN:-$PWD/core/target/release/vejas-runtime}"
[ -x "$BIN" ] || { echo "build first" >&2; exit 1; }
S=$(mktemp -d); R=$(mktemp -d)
RT=""; NP=""; SUB=""
cleanup(){ kill $RT $NP $SUB 2>/dev/null; wait $RT $NP $SUB 2>/dev/null; rm -rf "$S" "$R"; }
trap cleanup EXIT
fail=0
ok(){ echo "  ✓ $1"; }
ko(){ echo "  ✗ $1"; fail=1; }

NP_PORT=4285; URL=nats://127.0.0.1:$NP_PORT
GH=18795; ST=18796; BAD=18797; H="http://127.0.0.1:8757"
GH_SECRET="It's a Secret to Everybody"; ST_SECRET="whsec_e2e_secret"

sign(){ # secret, message: the hex HMAC-SHA256, computed by Python, not by the runtime
  python3 -c 'import hashlib, hmac, sys; print(hmac.new(sys.argv[1].encode(), sys.argv[2].encode(), hashlib.sha256).hexdigest())' "$1" "$2"
}
post(){ # port, suffix, body, header…: prints the status, the answer lands in $S/answer
  local port=$1 suffix=$2 body=$3; shift 3
  local headers=(); for h in "$@"; do headers+=(-H "$h"); done
  curl -s -m 5 -o "$S/answer" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
    "${headers[@]}" --data-binary "$body" "http://127.0.0.1:$port/ingest/$suffix"
}
expect(){ # status wanted, text the answer must hold, what the check says, post arguments…
  local want=$1 text=$2 what=$3; shift 3
  local got; got=$(post "$@")
  if [ "$got" = "$want" ] && grep -qF -- "$text" "$S/answer"; then ok "$what ($got)"
  else ko "$what: $got $(cat "$S/answer")"; fi
}
unit(){ # the named unit of the topology, as JSON
  curl -s -m 5 -H 'Content-Type: application/json' \
    -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"vejas_topology","arguments":{}}}' "$H/mcp" \
    | python3 -c '
import json, sys
def units(v):
    if isinstance(v, dict):
        if "restarts" in v: yield v
        for x in v.values(): yield from units(x)
    elif isinstance(v, list):
        for x in v: yield from units(x)
doc = json.loads(json.load(sys.stdin)["result"]["content"][0]["text"])
print(json.dumps([u for u in units(doc) if sys.argv[1] in u.get("name", "")]))' "$1"
}

mkdir -p "$R/connectors"
cat > "$R/connectors/gh_in.vjs" <<EOF
# connector: gh_in
driver "http-in"
PORT = $GH
ALLOW = ["gh"]
SIGNATURE = "github"
SIGNING_SECRET = secret("hooks/github")
EOF
cat > "$R/connectors/stripe_in.vjs" <<EOF
# connector: stripe_in
driver "http-in"
PORT = $ST
SIGNATURE = "stripe"
SIGNING_SECRET = secret("hooks/stripe")
EOF
cat > "$R/connectors/bad_in.vjs" <<EOF
# connector: bad_in
driver "http-in"
PORT = $BAD
SIGNATURE = "github"
EOF
nats-server -js -sd "$S/n" -a 127.0.0.1 -p $NP_PORT > /dev/null 2>&1 & NP=$!
for _ in $(seq 1 50); do nats -s $URL rtt > /dev/null 2>&1 && break; sleep 0.1; done
# What reaches the bus, as it does.
nats -s $URL sub 'vx.>' > "$S/bus.log" 2>&1 & SUB=$!
sleep 0.3
NATS_URL=$URL VEJAS_ROOT="$R" VEJAS_HTTP_ADDR=127.0.0.1:8757 \
  VEJAS_SECRET_HOOKS_GITHUB="$GH_SECRET" VEJAS_SECRET_HOOKS_STRIPE="$ST_SECRET" \
  "$BIN" > "$S/rt.log" 2>&1 & RT=$!
until curl -sf -o /dev/null "$H/healthz"; do sleep 0.1; done
for port in $GH $ST; do
  for _ in $(seq 1 100); do curl -sf -o /dev/null "http://127.0.0.1:$port/healthz" && break; sleep 0.1; done
done

echo "── GitHub: X-Hub-Signature-256 over the body"
BODY='{"action":"opened","number":7}'
SIG=$(sign "$GH_SECRET" "$BODY")
expect 202 '"published":"vx.gh"' "a signed request is published" $GH gh "$BODY" "X-Hub-Signature-256: sha256=$SIG"
expect 401 'no X-Hub-Signature-256 header' "an unsigned request gets 401" $GH gh "$BODY"
expect 401 'does not match' "a request signed with another secret gets 401" \
  $GH gh "$BODY" "X-Hub-Signature-256: sha256=$(sign other-secret "$BODY")"
expect 401 'does not match' "a body changed after signing gets 401" \
  $GH gh '{"action":"opened","number":8}' "X-Hub-Signature-256: sha256=$SIG"
expect 401 'signature' "unsigned, a subject outside ALLOW gets 401, not 403: ALLOW stays hidden" $GH other "$BODY"
OTHER='{"action":"closed"}'
expect 403 'not permitted by ALLOW' "signed, a subject outside ALLOW still gets 403" \
  $GH other "$OTHER" "X-Hub-Signature-256: sha256=$(sign "$GH_SECRET" "$OTHER")"
if grep -qF "$GH_SECRET" "$S/answer" "$S/rt.log"; then ko "the secret shows in an answer or the log"
else ok "the secret shows in no answer and not in the log"; fi

echo "── Stripe: t=<timestamp>,v1=<signature> over <timestamp>.<body>"
EVT='{"id":"evt_e2e","type":"invoice.paid"}'
NOW=$(date +%s)
expect 202 '"published":"vx.stripe"' "a signed event is published" \
  $ST stripe "$EVT" "Stripe-Signature: t=$NOW,v1=$(sign "$ST_SECRET" "$NOW.$EVT")"
expect 202 '"published":"vx.stripe"' "while a secret is rolled, the second v1 may be the one that matches" \
  $ST stripe "$EVT" "Stripe-Signature: t=$NOW,v1=$(sign old-secret "$NOW.$EVT"),v1=$(sign "$ST_SECRET" "$NOW.$EVT")"
OLD=$((NOW - 400))
expect 401 'timestamp' "a request signed 400 s ago gets 401 (tolerance 300 s)" \
  $ST stripe "$EVT" "Stripe-Signature: t=$OLD,v1=$(sign "$ST_SECRET" "$OLD.$EVT")"
expect 401 'does not match' "the signature of another timestamp gets 401" \
  $ST stripe "$EVT" "Stripe-Signature: t=$((NOW + 1)),v1=$(sign "$ST_SECRET" "$NOW.$EVT")"

echo "── what reached the bus"
sleep 0.5
GOT_GH=$(grep -c 'Received on "vx.gh"' "$S/bus.log"); GOT_ST=$(grep -c 'Received on "vx.stripe"' "$S/bus.log")
GOT_OTHER=$(grep -c 'Received on "vx.other"' "$S/bus.log")
[ "$GOT_GH" = 1 ] && ok "one message on vx.gh, the signed one" || ko "$GOT_GH message(s) on vx.gh, want 1"
[ "$GOT_ST" = 2 ] && ok "two messages on vx.stripe, the signed ones" || ko "$GOT_ST message(s) on vx.stripe, want 2"
[ "$GOT_OTHER" = 0 ] && ok "nothing on vx.other" || ko "$GOT_OTHER message(s) on vx.other"
grep -qF '"number":7' "$S/bus.log" && ! grep -qF '"number":8' "$S/bus.log" \
  && ok "the body published is the one signed" || ko "the bus holds a body that was not signed"

echo "── what the operator sees"
METRICS=$(curl -s -m 5 "$H/metrics")
for line in 'unit="connector:gh_in",reason="missing"} 2' 'unit="connector:gh_in",reason="mismatch"} 2' \
            'unit="connector:stripe_in",reason="stale"} 1' 'unit="connector:stripe_in",reason="mismatch"} 1'; do
  [[ "$METRICS" == *"vejas_signature_failures_total{$line"* ]] && ok "/metrics: $line" || ko "/metrics lacks $line"
done
grep -q 'github signature required' "$S/rt.log" && ok "the log says which signature a port requires" \
  || ko "no 'github signature required' in the log"
grep -q 'refused so far for their signature' "$S/rt.log" && ok "the log counts the refusals" \
  || ko "no refusal line in the log"
curl -s -m 2 -o /dev/null "http://127.0.0.1:$BAD/healthz" && ko "a port that asks for a signature without its secret opened" \
  || ok "a port that asks for a signature without its secret does not open"
BADU=$(unit bad_in)
[[ "$BADU" == *SIGNING_SECRET* ]] && ok "its unit says SIGNING_SECRET is missing" || ko "bad_in: $BADU"

echo "── a forged Content-Length"
# The largest length there is: the end of the body must not wrap around. The
# read gives up after its 5 s timeout and the request is judged on what came.
FIRST=$(python3 -c '
import socket, sys
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=15)
s.sendall(b"POST /ingest/gh HTTP/1.1\r\nHost: e2e\r\nContent-Length: 18446744073709551615\r\n\r\n{}")
print(s.recv(200).split(b"\r\n")[0].decode())' $GH 2>&1)
[[ "$FIRST" == *" 401 "* ]] && ok "it gets an answer: $FIRST" || ko "a forged Content-Length got: $FIRST"
expect 202 '"published":"vx.gh"' "and the port still takes a signed request" $GH gh "$BODY" "X-Hub-Signature-256: sha256=$SIG"

[ $fail -eq 0 ] && echo "SIGNED WEBHOOKS HOLD" || echo "SIGNED WEBHOOKS REGRESSION — do not ship"
exit $fail
