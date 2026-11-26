#!/bin/bash
# Signed webhooks on http-in (ADR-0037): a request without its sender's
# signature gets 401 and never reaches the bus, and a signed one is published.
# GitHub's scheme signs the body, Stripe's and Slack's a timestamp too, each on
# its port; Slack's check of a Request URL gets its challenge back. A manifest
# that asks for a signature without its secret does not start. Last, how
# http-in finds the end of a body, on a port that asks for no signature.
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
GH=18795; ST=18796; BAD=18797; SL=18798; PL=18799; H="http://127.0.0.1:8757"
GH_SECRET="It's a Secret to Everybody"; ST_SECRET="whsec_e2e_secret"; SL_SECRET="8f742231b10e8888abcd99e2e85a5"

sign(){ # secret, message: the hex HMAC-SHA256, computed by Python, not by the runtime
  python3 -c 'import hashlib, hmac, sys; print(hmac.new(sys.argv[1].encode(), sys.argv[2].encode(), hashlib.sha256).hexdigest())' "$1" "$2"
}
post(){ # port, suffix, body, header…: prints the status; the answer lands in $S/answer, and in $S/answers
  local port=$1 suffix=$2 body=$3; shift 3
  local headers=(); for h in "$@"; do headers+=(-H "$h"); done
  curl -s -m 5 -o "$S/answer" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
    "${headers[@]}" --data-binary "$body" "http://127.0.0.1:$port/ingest/$suffix"
  cat "$S/answer" >> "$S/answers"; echo >> "$S/answers"
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
on_bus(){ grep -c "Received on \"$1\"" "$S/bus.log"; }

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
cat > "$R/connectors/slack_in.vjs" <<EOF
# connector: slack_in
driver "http-in"
PORT = $SL
ALLOW = ["slack"]
SIGNATURE = "slack"
SIGNING_SECRET = secret("hooks/slack")
EOF
cat > "$R/connectors/bad_in.vjs" <<EOF
# connector: bad_in
driver "http-in"
PORT = $BAD
SIGNATURE = "github"
EOF
cat > "$R/connectors/plain_in.vjs" <<EOF
# connector: plain_in
driver "http-in"
PORT = $PL
EOF
nats-server -js -sd "$S/n" -a 127.0.0.1 -p $NP_PORT > /dev/null 2>&1 & NP=$!
for _ in $(seq 1 50); do nats -s $URL rtt > /dev/null 2>&1 && break; sleep 0.1; done
# What reaches the bus, as it does.
nats -s $URL sub 'vx.>' > "$S/bus.log" 2>&1 & SUB=$!
sleep 0.3
NATS_URL=$URL VEJAS_ROOT="$R" VEJAS_HTTP_ADDR=127.0.0.1:8757 \
  VEJAS_SECRET_HOOKS_GITHUB="$GH_SECRET" VEJAS_SECRET_HOOKS_STRIPE="$ST_SECRET" \
  VEJAS_SECRET_HOOKS_SLACK="$SL_SECRET" \
  "$BIN" > "$S/rt.log" 2>&1 & RT=$!
for _ in $(seq 1 300); do curl -sf -o /dev/null "$H/healthz" && break; sleep 0.1; done
for port in $GH $ST $SL $PL; do
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
# 100,000 empty candidates and a 512 KiB body: one HMAC each, they would cost
# minutes of CPU. Not candidates at all, they cost one parse of the header.
CROWD=$(python3 -c '
import socket, sys, time
body = b"{\"pad\":\"" + b"x" * 524288 + b"\"}"
head = b"t=%d," % int(time.time()) + b"v1=," * 100000
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=30)
began = time.monotonic()
s.sendall(b"POST /ingest/stripe HTTP/1.1\r\nHost: e2e\r\nStripe-Signature: " + head
          + b"\r\nContent-Length: %d\r\n\r\n" % len(body) + body)
answer = b""
while b"\r\n\r\n" not in answer:
    chunk = s.recv(65536)
    if not chunk: break
    answer += chunk
print(answer.split(b"\r\n")[0].decode(), "in %.2f s" % (time.monotonic() - began))' $ST 2>&1)
[[ "$CROWD" =~ \ 401\ .*in\ [01]\. ]] && ok "a header of 100,000 empty candidates: $CROWD" \
  || ko "a header of 100,000 empty candidates: $CROWD"

echo "── Slack: v0=<signature> over v0:<timestamp>:<body>, and the Request URL check"
NOW=$(date +%s)
CHECK='{"token":"legacy","challenge":"3eZbrw1aBm2rZgRNFdxV2595E9CY3gmdALWMmHkvFXO7tYXAYM8P","type":"url_verification"}'
slack(){ echo "X-Slack-Request-Timestamp: $NOW"; echo "X-Slack-Signature: v0=$(sign "$SL_SECRET" "v0:$NOW:$1")"; }
mapfile -t SLACK_CHECK < <(slack "$CHECK")
expect 200 '{"challenge":"3eZbrw1aBm2rZgRNFdxV2595E9CY3gmdALWMmHkvFXO7tYXAYM8P"}' \
  "Slack's signed URL check gets its challenge back" $SL slack "$CHECK" "${SLACK_CHECK[@]}"
expect 401 'no X-Slack-Request-Timestamp header' "the same check unsigned gets 401" $SL slack "$CHECK"
expect 403 'not permitted by ALLOW' "a URL whose subject ALLOW refuses fails the check" \
  $SL other "$CHECK" "${SLACK_CHECK[@]}"
EVENT='{"type":"event_callback","event":{"type":"app_mention","text":"hi"}}'
mapfile -t SLACK_EVENT < <(slack "$EVENT")
expect 202 '"published":"vx.slack"' "a signed event is published" $SL slack "$EVENT" "${SLACK_EVENT[@]}"

echo "── how http-in finds the end of a body (a port without SIGNATURE)"
python3 - $PL <<'PY' || fail=1
import socket, sys
port, failed = int(sys.argv[1]), False

def check(good, what):
    global failed
    print(("  ✓ " if good else "  ✗ ") + what)
    failed |= not good

def ask(sock, request):
    """Sends a request, reads one answer: its status line and headers, or None."""
    sock.sendall(request)
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = sock.recv(65536)
        if not chunk:
            return None, {}
        data += chunk
    head = data.split(b"\r\n\r\n", 1)[0].decode().split("\r\n")
    return head[0], {k.strip().lower(): v.strip() for k, v in (l.split(":", 1) for l in head[1:])}

def closes(sock):
    """Does the server close the connection after its answer?"""
    sock.settimeout(3)
    try:
        while True:
            if not sock.recv(65536):
                return True
    except socket.timeout:
        return False
    except ConnectionResetError:
        return True

def connect():
    return socket.create_connection(("127.0.0.1", port), timeout=5)

with connect() as s:
    status, _ = ask(s, b'POST /ingest/plain HTTP/1.1\r\nHost: e2e\r\n\r\n{"length":"none"}')
    check(status and " 202 " in status, f"without Content-Length, the body that came with the head is taken as before ({status})")
with connect() as s:
    for n in (1, 2):
        body = b'{"keep":%d}' % n
        status, headers = ask(s, b"POST /ingest/plain HTTP/1.1\r\nHost: e2e\r\nContent-Length: %d\r\n\r\n" % len(body) + body)
        check(status and " 202 " in status and headers.get("connection") == "keep-alive",
              f"keep-alive, request {n} on one connection ({status}, Connection: {headers.get('connection')})")
for value, want in ((b"abc", " 400 "), (b"5, 6", " 400 "), (b"18446744073709551616", " 413 ")):
    with connect() as s:
        status, headers = ask(s, b"POST /ingest/plain HTTP/1.1\r\nHost: e2e\r\nContent-Length: " + value + b"\r\n\r\n{}")
        check(status and want in status and headers.get("connection") == "close" and closes(s),
              f"Content-Length {value.decode()!r}: {status}, Connection: close, and it closes")
with connect() as s:
    status, headers = ask(s, b'POST /ingest/plain HTTP/1.1\r\nHost: e2e\r\nTransfer-Encoding: chunked\r\n\r\n11\r\n{"chunked":"yes"}\r\n0\r\n\r\n')
    check(status and " 411 " in status and headers.get("connection") == "close" and closes(s),
          f"a chunked body: {status}, and it closes")
sys.exit(1 if failed else 0)
PY

echo "── what reached the bus"
sleep 0.5
[ "$(on_bus vx.gh)" = 1 ] && ok "one message on vx.gh, the signed one" || ko "$(on_bus vx.gh) message(s) on vx.gh, want 1"
[ "$(on_bus vx.stripe)" = 2 ] && ok "two messages on vx.stripe, the signed ones" || ko "$(on_bus vx.stripe) message(s) on vx.stripe, want 2"
[ "$(on_bus vx.slack)" = 1 ] && ok "one message on vx.slack, the event and not the URL check" \
  || ko "$(on_bus vx.slack) message(s) on vx.slack, want 1"
[ "$(on_bus vx.plain)" = 3 ] && ok "three messages on vx.plain, none of the refused" || ko "$(on_bus vx.plain) message(s) on vx.plain, want 3"
[ "$(on_bus vx.other)" = 0 ] && ok "nothing on vx.other" || ko "$(on_bus vx.other) message(s) on vx.other"
grep -qF '"number":7' "$S/bus.log" && ! grep -qF '"number":8' "$S/bus.log" \
  && ok "the body published is the one signed" || ko "the bus holds a body that was not signed"
! grep -qF 'url_verification' "$S/bus.log" && ! grep -qF 'chunked' "$S/bus.log" \
  && ok "neither the URL check nor the chunked body reached the bus" || ko "the URL check or the chunked body reached the bus"

echo "── what the operator sees"
METRICS=$(curl -s -m 5 "$H/metrics")
for line in 'unit="connector:gh_in",reason="missing"} 2' 'unit="connector:gh_in",reason="mismatch"} 2' \
            'unit="connector:stripe_in",reason="stale"} 1' 'unit="connector:stripe_in",reason="mismatch"} 1' \
            'unit="connector:stripe_in",reason="malformed"} 1' 'unit="connector:slack_in",reason="missing"} 1'; do
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

echo "── lengths"
# The largest length there is: the end of the body must not wrap around, and a
# request declared past the 1 MiB bound is refused at once, unread.
FIRST=$(python3 -c '
import socket, sys
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=15)
s.sendall(b"POST /ingest/gh HTTP/1.1\r\nHost: e2e\r\nContent-Length: 18446744073709551615\r\n\r\n{}")
print(s.recv(200).split(b"\r\n")[0].decode())' $GH 2>&1)
[[ "$FIRST" == *" 413 "* ]] && ok "a forged Content-Length gets: $FIRST" || ko "a forged Content-Length got: $FIRST"
python3 -c 'import json; print(json.dumps({"commits": ["x" * 1000] * 1200}))' > "$S/big.json"
BIG_SIG=$(python3 -c 'import hashlib, hmac, sys; print(hmac.new(sys.argv[1].encode(), open(sys.argv[2], "rb").read(), hashlib.sha256).hexdigest())' "$GH_SECRET" "$S/big.json")
BIG=$(curl -s -m 10 -D "$S/big-head" -o "$S/answer" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
  -H "X-Hub-Signature-256: sha256=$BIG_SIG" --data-binary @"$S/big.json" "http://127.0.0.1:$GH/ingest/gh")
cat "$S/answer" >> "$S/answers"; echo >> "$S/answers"
[ "$BIG" = 413 ] && grep -q 'at most' "$S/answer" && grep -qi '^connection: close' "$S/big-head" \
  && ok "a signed body over 1 MiB gets 413 and Connection: close, not a signature that does not match" \
  || ko "a 1.2 MB body got $BIG $(cat "$S/answer") $(grep -i '^connection' "$S/big-head")"
expect 202 '"published":"vx.gh"' "and the port still takes a signed request" $GH gh "$BODY" "X-Hub-Signature-256: sha256=$SIG"

echo "── the secrets"
LEAK=0
for secret in "$GH_SECRET" "$ST_SECRET" "$SL_SECRET"; do
  grep -qF -- "$secret" "$S/answers" "$S/rt.log" "$S/bus.log" && LEAK=1
done
[ "$(wc -l < "$S/answers")" -ge 16 ] && [ $LEAK = 0 ] \
  && ok "no secret shows in any of the $(wc -l < "$S/answers") answers, the log or the bus" \
  || ko "a secret shows in an answer, the log or the bus (or the answers were not kept)"

[ $fail -eq 0 ] && echo "SIGNED WEBHOOKS HOLD" || echo "SIGNED WEBHOOKS REGRESSION — do not ship"
exit $fail
