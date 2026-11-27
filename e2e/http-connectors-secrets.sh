#!/bin/bash
# The http connectors keep credentials out of what they write (ADR-0017). An
# answer that echoes the request, a key in its query, reaches neither the log
# nor the dead letters stored in the stream. A redirect is not followed, so the
# headers stay with the host they were set for. The poll envelope's endpoint is
# the URL without its query.
#   e2e/http-connectors-secrets.sh            (needs the nats CLI)
set -uo pipefail
cd "$(dirname "$0")/.."
BIN="${BIN:-$PWD/core/target/release/vejas-runtime}"
[ -x "$BIN" ] || { echo "build first" >&2; exit 1; }
S=$(mktemp -d); R=$(mktemp -d)
RT=""; NP=""; SUB=""; API=""; FARP=""
trap 'kill $RT $NP $SUB $API $FARP 2>/dev/null; wait $RT $NP $SUB $API $FARP 2>/dev/null; rm -rf "$S" "$R"' EXIT
fail=0
ok(){ echo "  ✓ $1"; }
ko(){ echo "  ✗ $1"; fail=1; }

URL=nats://127.0.0.1:4299; H="http://127.0.0.1:8773"; API_PORT=18801; FAR=18802
LIT=LITERAL-K3Y-in-query; SEC=K3Y-e2e-SECRET
mkdir -p "$R/connectors"
# a key written in the URL's query, and a key from the secret store in a header
cat > "$R/connectors/poll_echo.vjs" <<V
# connector: poll_echo
driver "http-poll"
URL = "http://127.0.0.1:$API_PORT/echo?api_key=$LIT&page=1"
SUBJECT = "vx.e2e.polled"
INTERVAL_SECS = 1
V
cat > "$R/connectors/poll_moved.vjs" <<V
# connector: poll_moved
driver "http-poll"
URL = "http://127.0.0.1:$API_PORT/moved"
HEADERS = {"X-Api-Key": secret("api/key")}
SUBJECT = "vx.e2e.polled"
INTERVAL_SECS = 1
V
cat > "$R/connectors/poll_env.vjs" <<V
# connector: poll_env
driver "http-poll"
URL = "http://127.0.0.1:$API_PORT/ok?api_key=$LIT"
SUBJECT = "vx.e2e.enveloped"
INTERVAL_SECS = 1
ENVELOPE = true
V
cat > "$R/connectors/out_echo.vjs" <<V
# connector: out_echo
driver "http-out"
SUBJECT = "vx.e2e.out"
URL = "http://127.0.0.1:$API_PORT/echo?api_key=$LIT"
V
cat > "$R/connectors/out_moved.vjs" <<V
# connector: out_moved
driver "http-out"
SUBJECT = "vx.e2e.moved"
URL = "http://127.0.0.1:$API_PORT/moved"
HEADERS = {"X-Api-Key": secret("api/key")}
V

# The API: /echo answers 400 with the request line, query included; /moved
# redirects to the far host; /ok answers. The far host writes down any visit.
python3 -c '
import http.server, sys
far = int(sys.argv[2])
class H(http.server.BaseHTTPRequestHandler):
    def answer(self):
        n = int(self.headers.get("Content-Length") or 0)
        if n: self.rfile.read(n)
        if self.path.startswith("/echo"):
            body = ("no route for %s %s" % (self.command, self.path)).encode()
            self.send_response(400)
        elif self.path.startswith("/moved"):
            body = b"moved"
            self.send_response(302); self.send_header("Location", "http://127.0.0.1:%d/stolen" % far)
        else:
            body = b"{\"ok\":true}"
            self.send_response(200)
        self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)
    do_GET = answer
    do_POST = answer
    def log_message(self, *a): pass
http.server.ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
' $API_PORT $FAR & API=$!
python3 -c '
import http.server, sys
class F(http.server.BaseHTTPRequestHandler):
    def answer(self):
        with open(sys.argv[2], "a") as f:
            f.write("%s %s %s\n" % (self.command, self.path, self.headers.get("X-Api-Key")))
        self.send_response(200); self.send_header("Content-Length", "0"); self.end_headers()
    do_GET = answer
    do_POST = answer
    def log_message(self, *a): pass
http.server.ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), F).serve_forever()
' $FAR "$S/far" & FARP=$!
touch "$S/far"

nats-server -js -sd "$S/n" -a 127.0.0.1 -p 4299 > /dev/null 2>&1 & NP=$!
for _ in $(seq 1 50); do nats -s $URL server check connection > /dev/null 2>&1 && break; sleep 0.1; done
NATS_URL=$URL VEJAS_ROOT="$R" VEJAS_HTTP_ADDR=127.0.0.1:8773 VEJAS_ACK_WAIT_SECS=1 VEJAS_SECRET_API_KEY=$SEC \
  "$BIN" > "$S/rt.log" 2>&1 & RT=$!
until curl -sf -o /dev/null "$H/healthz"; do sleep 0.1; done
nats -s $URL sub vx.e2e.enveloped --raw > "$S/env" 2>/dev/null & SUB=$!
for _ in $(seq 1 100); do [ "$(grep -c 'consuming vx.e2e' "$S/rt.log")" -ge 2 ] && break; sleep 0.1; done
nats -s $URL pub vx.e2e.out '{"n":1}' > /dev/null 2>&1
nats -s $URL pub vx.e2e.moved '{"n":2}' > /dev/null 2>&1

letters(){ curl -s -m 5 "$H/dlq" | python3 -c 'import json,sys; print(" ".join(sorted(d.get("unit","?") for d in json.load(sys.stdin)["dead_letters"])))'; }
L=""; for _ in $(seq 1 150); do L=$(letters); echo "$L" | grep -q out_echo && echo "$L" | grep -q out_moved && break; sleep 0.1; done
# what the dead letter stream keeps at rest, unmasked
stored(){
  last=$(nats -s $URL stream info VEJAS_DLQ --json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["state"]["last_seq"])' 2>/dev/null)
  for seq in $(seq 1 "${last:-0}"); do
    nats -s $URL stream get VEJAS_DLQ "$seq" --json 2>/dev/null \
      | python3 -c 'import json,sys,base64; print(base64.b64decode(json.load(sys.stdin).get("data","")).decode("utf-8","replace"))'
  done
}
stored > "$S/stored"
sleep 2   # a few polls more

echo "── an answer that echoes the request"
grep -q 'HTTP 400: no route for POST /echo?<masked>' "$S/stored" \
  && ok "the dead letter quotes the 400 without its query" || ko "letter: $(grep -o 'HTTP 400[^"]*' "$S/stored" | head -1)"
grep -q "$LIT\|$SEC" "$S/stored" && ko "a key is in the dead letters stored: $(grep -o "[^\"]*$LIT[^\"]*\|[^\"]*$SEC[^\"]*" "$S/stored" | head -1)" \
  || ok "no key in the dead letters stored ($(grep -c . "$S/stored") lines)"
grep -q 'poll: HTTP 400: no route for GET /echo?<masked>' "$S/rt.log" \
  && ok "the poll's error is logged without its query" || ko "poll error: $(grep 'poll_echo.*poll' "$S/rt.log" | head -1)"
grep -q "$LIT\|$SEC" "$S/rt.log" && ko "a key is in the log: $(grep "$LIT\|$SEC" "$S/rt.log" | head -1 | cut -c1-140)" \
  || ok "no key in the log"
grep -q "polling 127.0.0.1:$API_PORT every" "$S/rt.log" && ok "the poll logs its host" || ko "start line: $(grep polling "$S/rt.log" | head -1)"
curl -s -m 5 "$H/events?limit=200" > "$S/events"
grep -q "$LIT\|$SEC" "$S/events" && ko "a key is in /events" || ok "no key in /events"

echo "── a redirect"
grep -q "HTTP 302 from 127.0.0.1:$API_PORT: a redirect, not followed" "$S/stored" \
  && ok "the sink's dead letter says the redirect was not followed" || ko "moved letter: $(grep -o 'HTTP 302[^"]*' "$S/stored" | head -1)"
grep -q "poll: HTTP 302 from 127.0.0.1:$API_PORT: a redirect, not followed" "$S/rt.log" \
  && ok "so does the poll's error" || ko "poll moved: $(grep 'poll_moved' "$S/rt.log" | head -2 | tr '\n' ' ')"
[ ! -s "$S/far" ] && ok "the far host got no request, so no header" || ko "the far host was reached: $(head -2 "$S/far" | tr '\n' ' ')"

echo "── the poll envelope"
E=$(grep -m1 '^{' "$S/env")
echo "$E" | grep -q "\"endpoint\":\"http://127.0.0.1:$API_PORT/ok\"" && ok "its endpoint is the URL without its query" || ko "envelope: $E"

[ $fail -eq 0 ] && echo "HTTP CONNECTORS KEEP SECRETS" || echo "HTTP CONNECTORS LEAK — do not ship"
exit $fail
