#!/bin/bash
# The http connectors keep credentials out of what they write (ADR-0017). An
# answer that echoes the request, as it is, decoded, or through the escapes
# PHP, Go, .NET, Python and HTML write, reaches neither the log, /events, the
# trace record nor the dead letters stored in the stream, whatever the name of
# the key: `key`, `code` and `sig` are names no mask knows. A webhook's token
# in the path, a client secret a token endpoint echoes, a header's key, the
# access token oauth-poll fetches, and what a sink's system answers with a 2xx
# are kept out the same way. A redirect is not followed, nor a next page or an
# EXPAND detail on another host, so the headers and the token stay with the
# host they were set for. An envelope's endpoint has no query.
#   e2e/http-connectors-secrets.sh            (needs the nats CLI)
set -uo pipefail
cd "$(dirname "$0")/.."
BIN="${BIN:-$PWD/core/target/release/vejas-runtime}"
[ -x "$BIN" ] || { echo "build first" >&2; exit 1; }
S=$(mktemp -d); R=$(mktemp -d)
RT=""; NP=""; SUB=""; SUB2=""; API=""; FARP=""
trap 'kill $RT $NP $SUB $SUB2 $API $FARP 2>/dev/null; wait $RT $NP $SUB $SUB2 $API $FARP 2>/dev/null; rm -rf "$S" "$R"' EXIT
fail=0
ok(){ echo "  ✓ $1"; }
ko(){ echo "  ✗ $1"; fail=1; }

URL=nats://127.0.0.1:4299; H="http://127.0.0.1:8773"; API_PORT=18801; FAR=18802
A="http://127.0.0.1:$API_PORT"
# the keys, each with a tail no other text has: what must never be written
LIT=LITERAL-K3Y-0001            # in a query, as it is
LIT3=J50N%26K3Y-0003            # echoed decoded and escaped for JSON, as Go does
LIT4=H7ML%26%3CK3Y-0010         # echoed decoded and escaped for HTML
TOKEN=PATHT0K3N-0004-abcdefgh   # a webhook's token, in the path
CLIENT='s3cr/t+K3Y-0005'        # a client secret, from the secret store
LITCLIENT='l1t/eral+K3Y-0009'   # one written in the connector, echoed in a sentence
CODE=FUNC-K3Y-0006              # an endpoint's `code`
SEC=K3Y-e2e-SECRET              # a header's key, from the secret store
BEARER=tok-e2e-bearer-0007      # the token the token endpoint gives
PHP=AB%2FCD%2BEF-PHP01          # echoed by PHP's json_encode, its / escaped
NET=AB%2BCD-NET01               # by .NET's System.Text.Json, its + escaped
PYQ=AB%2FCD%2BEF-PYQ01          # by Python's quote(), which keeps the /
OKK=OKECHO-K3Y-0011             # echoed by a 2xx answer
NEVER="K3Y-0001\|K3Y-0003\|K3Y-0010\|PATHT0K3N\|K3Y-0005\|K3Y-0009\|K3Y-0006\|$SEC\|$BEARER\|PHP01\|NET01\|PYQ01\|K3Y-0011\|PAIR01"
mkdir -p "$R/connectors"
conn(){ cat > "$R/connectors/$1.vjs"; }
conn poll_echo <<V
# connector: poll_echo
driver "http-poll"
URL = "$A/echo?key=$LIT&page=1"
SUBJECT = "vx.e2e.polled"
INTERVAL_SECS = 1
V
conn poll_moved <<V
# connector: poll_moved
driver "http-poll"
URL = "$A/moved"
HEADERS = {"X-Api-Key": secret("api/key")}
SUBJECT = "vx.e2e.polled"
INTERVAL_SECS = 1
V
conn poll_env <<V
# connector: poll_env
driver "http-poll"
URL = "$A/ok?key=$LIT"
SUBJECT = "vx.e2e.enveloped"
INTERVAL_SECS = 1
ENVELOPE = true
V
SINKS="out_echo:out:/echo?key=$LIT out_moved:moved:/moved out_html:html:/html?key=$LIT4&page=1 out_json:json:/json?key=$LIT3
  out_hook:hook:/hook/T0001/B0001/$TOKEN out_php:php:/php?sig=$PHP out_net:net:/net?sig=$NET out_pyq:pyq:/pyq?sig=$PYQ
  out_pair:pair:/pair?include=password out_hdr:hdr:/hdr out_ok:ok:/okecho?key=$OKK"
for sink in $SINKS; do
  IFS=: read -r name subject path <<< "$sink"
  conn "$name" <<V
# connector: $name
driver "http-out"
SUBJECT = "vx.e2e.$subject"
URL = "$A$path"
HEADERS = {"X-Api-Key": secret("api/key")}
V
done
conn oauth_echo <<V
# connector: oauth_echo
driver "oauth-poll"
TOKEN_URL = "$A/token-echo"
CLIENT_ID = "e2e-client"
CLIENT_SECRET = "$LITCLIENT"
BASE_URL = "$A"
ENDPOINTS = ["/never"]
SUBJECT = "vx.e2e.oauth"
INTERVAL_SECS = 1
V
conn oauth_next <<V
# connector: oauth_next
driver "oauth-poll"
TOKEN_URL = "$A/token"
CLIENT_ID = "e2e-client"
CLIENT_SECRET = secret("oauth/secret")
BASE_URL = "$A"
ENDPOINTS = ["/data?code=$CODE", "/missing?code=$CODE", "/bearer"]
SUBJECT = "vx.e2e.oauth"
INTERVAL_SECS = 1
V
conn oauth_expand <<V
# connector: oauth_expand
driver "oauth-poll"
TOKEN_URL = "$A/token"
CLIENT_ID = "e2e-client"
CLIENT_SECRET = secret("oauth/secret")
BASE_URL = "$A"
EXPAND = [{"name": "items", "list": "/items", "detail": "{href}", "key": "href", "as": "detail"}, {"name": "things", "list": "/things", "detail": "/thing/{id}", "key": "id", "as": "detail"}]
SUBJECT = "vx.e2e.oauth"
INTERVAL_SECS = 1
V

# The API answers each route as a system might: /echo and /missing quote the
# request line; /html, /json, /php, /net and /pyq the URL or its value as HTML,
# Go, PHP, .NET and Python's quote() write it; /hook the path; /hdr the key
# header; /pair a credential named by the query; /okecho the URL, with a 200;
# /token-echo the client secret in a sentence; /bearer the token it got.
# /moved redirects to the far host, /data names a next page there, /items an
# item there. The far host writes down any visit with the headers that would
# carry a credential.
python3 -c '
import html, http.server, json, sys, urllib.parse
far = int(sys.argv[2])
BS = chr(92)
class H(http.server.BaseHTTPRequestHandler):
    def answer(self):
        n = int(self.headers.get("Content-Length") or 0)
        form = self.rfile.read(n).decode() if n else ""
        p, code, ctype = self.path, 400, "text/plain"
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(p).query)
        if p.startswith("/echo") or p.startswith("/missing"):
            body = "no route for %s %s" % (self.command, p)
            code = 400 if p.startswith("/echo") else 404
        elif p.startswith("/html"):
            body, ctype = "<p>bad request: %s</p>" % html.escape(urllib.parse.unquote(p)), "text/html"
        elif p.startswith("/json"):
            body = json.dumps({"error": "bad request", "url": urllib.parse.unquote(p)})
            body = body.replace("&", BS + "u0026").replace("<", BS + "u003c").replace(">", BS + "u003e")
            ctype = "application/json"
        elif p.startswith("/php"):
            body = json.dumps({"message": "Invalid signature: " + query["sig"][0]}).replace("/", BS + "/")
            ctype = "application/json"
        elif p.startswith("/net"):
            body = json.dumps({"message": query["sig"][0] + " is not valid"}).replace("+", BS + "u002B")
            ctype = "application/json"
        elif p.startswith("/pyq"):
            body = "bad pyq=" + urllib.parse.quote(query["sig"][0])
        elif p.startswith("/pair"):
            body, ctype = json.dumps({"password": "hunter2-PAIR01"}), "application/json"
        elif p.startswith("/hdr"):
            body = "bad key %s" % self.headers.get("X-Api-Key")
        elif p.startswith("/okecho"):
            body, code, ctype = json.dumps({"ok": True, "url": p}), 200, "application/json"
        elif p.startswith("/hook/"):
            body, code = "no webhook %s" % p, 404
        elif p.startswith("/moved"):
            body, code = "moved", 302
        elif p.startswith("/token-echo"):
            body, code = "invalid_client: no client holds %s" % urllib.parse.parse_qs(form)["client_secret"][0], 401
        elif p.startswith("/token"):
            body, code = json.dumps({"access_token": sys.argv[3], "expires_in": 3600}), 200
        elif p.startswith("/bearer"):
            body, code = "access token %s lacks the scope" % (self.headers.get("Authorization") or "").replace("Bearer ", ""), 403
        elif p.startswith("/data"):
            body, code = json.dumps({"value": [1], "@odata.nextLink": "http://127.0.0.1:%d/stolen?page=2" % far}), 200
        elif p.startswith("/items"):
            body, code = json.dumps({"value": [{"href": "http://127.0.0.1:%d/steal-detail" % far}, {"href": "/item/1"}]}), 200
        elif p.startswith("/item/"):
            body, code = json.dumps({"item": p}), 200
        elif p.startswith("/things"):
            body, code = json.dumps({"value": [{"id": "../far?x"}]}), 200
        elif p.startswith("/thing/"):
            body, code = json.dumps({"path": p}), 200
        else:
            body, code = "{\"ok\":true}", 200
        b = body.encode()
        self.send_response(code)
        if code == 302: self.send_header("Location", "http://127.0.0.1:%d/stolen" % far)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b)
    do_GET = answer
    do_POST = answer
    def log_message(self, *a): pass
http.server.ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
' $API_PORT $FAR $BEARER & API=$!
python3 -c '
import http.server, sys
class F(http.server.BaseHTTPRequestHandler):
    def answer(self):
        with open(sys.argv[2], "a") as f:
            f.write("%s %s %s %s\n" % (self.command, self.path, self.headers.get("X-Api-Key"), self.headers.get("Authorization")))
        self.send_response(200); self.send_header("Content-Length", "0"); self.end_headers()
    do_GET = answer
    do_POST = answer
    def log_message(self, *a): pass
http.server.ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), F).serve_forever()
' $FAR "$S/far" & FARP=$!

# the far host's record is the proof of the redirect checks: it must write
# down a visit before they can say it got none
seen=""; for _ in $(seq 1 50); do
  curl -s -m 1 -o /dev/null -H "X-Api-Key: control" "http://127.0.0.1:$FAR/control" && grep -q "GET /control control" "$S/far" 2>/dev/null && { seen=1; break; }
  sleep 0.1
done
[ -n "$seen" ] && ok "the far host writes down a visit, headers included" || ko "the far host records nothing: its checks would prove nothing"
: > "$S/far"
for _ in $(seq 1 50); do curl -s -m 1 -o /dev/null "$A/ok" && break; sleep 0.1; done

nats-server -js -sd "$S/n" -a 127.0.0.1 -p 4299 > /dev/null 2>&1 & NP=$!
for _ in $(seq 1 50); do nats -s $URL server check connection > /dev/null 2>&1 && break; sleep 0.1; done
NATS_URL=$URL VEJAS_ROOT="$R" VEJAS_HTTP_ADDR=127.0.0.1:8773 VEJAS_ACK_WAIT_SECS=1 \
  VEJAS_SECRET_API_KEY=$SEC VEJAS_SECRET_OAUTH_SECRET="$CLIENT" \
  "$BIN" > "$S/rt.log" 2>&1 & RT=$!
until curl -sf -o /dev/null "$H/healthz"; do sleep 0.1; done
nats -s $URL sub vx.e2e.enveloped --raw > "$S/env" 2>/dev/null & SUB=$!
nats -s $URL sub vx.e2e.oauth --raw > "$S/oauth" 2>/dev/null & SUB2=$!
for _ in $(seq 1 100); do [ "$(grep -c 'consuming vx.e2e' "$S/rt.log")" -ge 11 ] && break; sleep 0.1; done
for subject in out moved html json hook php net pyq pair hdr ok; do nats -s $URL pub "vx.e2e.$subject" '{"n":1}' > /dev/null 2>&1; done

DEAD="out_echo out_moved out_html out_json out_hook out_php out_net out_pyq out_pair out_hdr"
letters(){ curl -s -m 5 "$H/dlq" | python3 -c 'import json,sys; print(" ".join(sorted(d.get("unit","?") for d in json.load(sys.stdin)["dead_letters"])))'; }
L=""; for _ in $(seq 1 200); do
  L=$(letters); n=0; for u in $DEAD; do echo "$L" | grep -q "connector:$u\b" && n=$((n+1)); done
  [ $n -eq 10 ] && break; sleep 0.1
done
# what the dead letter stream keeps at rest, unmasked: one line per letter,
# its unit then its last error
stored(){
  last=$(nats -s $URL stream info VEJAS_DLQ --json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["state"]["last_seq"])' 2>/dev/null)
  for seq in $(seq 1 "${last:-0}"); do
    nats -s $URL stream get VEJAS_DLQ "$seq" --json 2>/dev/null \
      | python3 -c 'import json,sys,base64; print(base64.b64decode(json.load(sys.stdin).get("data","")).decode("utf-8","replace"))'
  done
}
stored > "$S/stored"
python3 -c '
import json, sys
for line in open(sys.argv[1]):
    try: d = json.loads(line)
    except ValueError: continue
    print(d.get("unit", "?"), d.get("last_error", ""))
' "$S/stored" > "$S/errors"
sleep 2   # a few polls more
curl -s -m 5 "$H/events?limit=500" > "$S/events"
curl -s -m 5 "$H/topology" > "$S/topology"
# what the trace record keeps at rest, every hop
n=$(nats -s $URL stream info VEJAS_TRACES --json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["state"]["messages"])' 2>/dev/null)
nats -s $URL consumer add VEJAS_TRACES e2edump --pull --deliver all --ack none --replay instant --defaults > /dev/null 2>&1
timeout 30 nats -s $URL consumer next VEJAS_TRACES e2edump --count "${n:-1}" --raw --no-ack > "$S/traces" 2>/dev/null
letter(){ grep "^connector:$1 " "$S/errors" | head -1 | cut -d' ' -f2-; }
has(){ grep -qF -- "$2" <<< "$1"; }

echo "── an answer that echoes the request"
e=$(letter out_echo); has "$e" "HTTP 400: no route for POST /echo?key=<masked>" \
  && ok "as it is: the dead letter quotes the 400 with the key masked" || ko "echo letter: $e"
e=$(letter out_html); has "$e" "HTTP 400: <p>bad request: /html?key=<masked>&amp;page=1</p>" \
  && ok "decoded and escaped for HTML, & and < among it" || ko "html letter: $e"
e=$(letter out_json); has "$e" '"url":"/json?key=<masked>"' \
  && ok "decoded and escaped for JSON, as Go does" || ko "json letter: $e"
e=$(letter out_php); has "$e" "Invalid signature: <masked>" \
  && ok "escaped as PHP's json_encode does, its / too" || ko "php letter: $e"
e=$(letter out_net); has "$e" "<masked> is not valid" \
  && ok "escaped as .NET's System.Text.Json does, its + too" || ko "net letter: $e"
e=$(letter out_pyq); has "$e" "HTTP 400: bad pyq=<masked>" \
  && ok "encoded as Python's quote() does, its / kept" || ko "pyq letter: $e"
e=$(letter out_hook); has "$e" "HTTP 404: no webhook /hook/T0001/B0001/<masked>" \
  && ok "a webhook's token, in the path" || ko "hook letter: $e"
e=$(letter out_hdr); has "$e" "HTTP 400: bad key <secret>" \
  && ok "a header's key from the secret store" || ko "hdr letter: $e"
e=$(letter out_pair); has "$e" "HTTP 400: {" && ! has "$e" "hunter2" \
  && ok "a credential the query names keeps its value masked: $e" || ko "pair letter: $e"
grep -q 'poll: HTTP 400: no route for GET /echo?key=<masked>&page=1' "$S/rt.log" \
  && ok "the poll's error is logged with the key masked" || ko "poll error: $(grep 'poll_echo.*poll' "$S/rt.log" | head -1)"
grep -q "polling 127.0.0.1:$API_PORT every" "$S/rt.log" && ok "the poll logs its host" || ko "start line: $(grep polling "$S/rt.log" | head -1)"

echo "── a 2xx answer that echoes the URL"
grep -qF 'okecho?key=<masked>' "$S/events" && ok "/events keeps the sink's answer with the key masked" \
  || ko "events: $(grep -o '.\{0,60\}okecho.\{0,40\}' "$S/events" | head -1)"
grep -qF 'okecho?key=<masked>' "$S/traces" && ok "so does the trace record" \
  || ko "traces: $(grep -o '.\{0,60\}okecho.\{0,40\}' "$S/traces" | head -1)"

echo "── oauth-poll"
grep -q "connector connector:oauth_echo: token endpoint HTTP 401: invalid_client: no client holds <masked>" "$S/rt.log" \
  && ok "a client secret written in the connector, echoed in a sentence, is masked" || ko "token error: $(grep 'oauth_echo' "$S/rt.log" | head -1 | cut -c1-200)"
grep -q "oauth_next\] /missing: HTTP 404: no route for GET /missing?code=<masked>" "$S/rt.log" \
  && ok "an endpoint's error names it without its query, and masks its echo" || ko "endpoint error: $(grep 'oauth_next.*missing' "$S/rt.log" | head -1 | cut -c1-200)"
grep -q "oauth_next\] /bearer: HTTP 403: access token <secret> lacks the scope" "$S/rt.log" \
  && ok "the access token an answer quotes is masked as a store's value" || ko "bearer error: $(grep 'oauth_next.*bearer' "$S/rt.log" | head -1 | cut -c1-200)"
grep -q "oauth_next\] /data: the next page is at http://127.0.0.1:$FAR, not http://127.0.0.1:$API_PORT: not followed" "$S/rt.log" \
  && ok "a next page on another host is not followed" || ko "next page: $(grep 'oauth_next.*data' "$S/rt.log" | head -1 | cut -c1-200)"
grep -q "oauth_expand\] items: detail for http://127.0.0.1:$FAR/steal-detail: its detail is at http://127.0.0.1:$FAR, not http://127.0.0.1:$API_PORT: not followed" "$S/rt.log" \
  && ok "nor an item's detail there" || ko "expand detail: $(grep 'oauth_expand' "$S/rt.log" | head -1 | cut -c1-200)"
grep -q '"item":"/item/1"' "$S/oauth" && ok "an item's detail on the page's host is fetched" || ko "expand page: $(grep -m1 items "$S/oauth" | cut -c1-200)"
grep -q '"path":"/thing/..%2Ffar%3Fx"' "$S/oauth" && ok "an id is one segment of the detail path" || ko "expand path: $(grep -m1 thing "$S/oauth" | cut -c1-200)"
E=$(grep -m1 '"endpoint":"/data"' "$S/oauth")
python3 -c 'import json,sys; sys.exit(json.loads(sys.argv[1])["endpoint"] != "/data")' "$E" 2>/dev/null \
  && ok "its page's endpoint has no query" || ko "oauth envelope: $(grep -m1 endpoint "$S/oauth" | cut -c1-160)"

echo "── a redirect"
e=$(letter out_moved); has "$e" "HTTP 302 from 127.0.0.1:$API_PORT: a redirect, not followed" \
  && ok "the sink's dead letter says the redirect was not followed" || ko "moved letter: $e"
grep -q "poll: HTTP 302 from 127.0.0.1:$API_PORT: a redirect, not followed" "$S/rt.log" \
  && ok "so does the poll's error" || ko "poll moved: $(grep 'poll_moved' "$S/rt.log" | head -2 | tr '\n' ' ')"
[ ! -s "$S/far" ] && ok "the far host got no request, so no header and no token" || ko "the far host was reached: $(head -2 "$S/far" | tr '\n' ' ')"

echo "── nothing written holds a key"
for f in stored rt.log events topology oauth env traces; do
  grep -q "$NEVER" "$S/$f" && ko "a key is in $f: $(grep -o ".\{0,40\}\($NEVER\).\{0,20\}" "$S/$f" | head -1)" || ok "no key in $f ($(wc -c < "$S/$f") bytes)"
done

echo "── the poll envelope"
E=$(grep -m1 '^{' "$S/env")
echo "$E" | grep -q "\"endpoint\":\"$A/ok\"" && ok "its endpoint is the URL without its query" || ko "envelope: $E"

[ $fail -eq 0 ] && echo "HTTP CONNECTORS KEEP SECRETS" || echo "HTTP CONNECTORS LEAK — do not ship"
exit $fail
