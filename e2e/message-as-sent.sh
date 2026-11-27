#!/bin/bash
# A flow sees its message as it came (ADR-0039). An api flow's event is the
# body as sent, an array included; its path parameters are variables and
# `request` holds the rest of the request, so a body field named `query` is
# the body's. Of the headers, the flow sees those that say what the request
# is and those its API_HEADERS list names, never one whose name reads as a
# credential, as the request or the flow writes it; the trace record masks
# the value of each header the list names and says which the flow was not
# given. A body that is not JSON answers 400, unless the flow reads
# `unreadable`: its event holds the message once, its text, or its bytes in
# base64 when they are not text, which the record masks when they hold a
# credential, and whole when no mask can read them. On the bus, a flow that
# reads `unreadable` runs a message that is not JSON instead of leaving it to
# the dead letter queue, and time travel runs such messages too.
#   e2e/message-as-sent.sh                    (needs the nats CLI)
set -uo pipefail
cd "$(dirname "$0")/.."
BIN="${BIN:-$PWD/core/target/release/vejas-runtime}"
[ -x "$BIN" ] || { echo "build first" >&2; exit 1; }
S=$(mktemp -d); R=$(mktemp -d)
RT=""; NP=""; SUB=""
trap 'kill $RT $NP $SUB 2>/dev/null; wait $RT $NP $SUB 2>/dev/null; rm -rf "$S" "$R"' EXIT
fail=0
ok(){ echo "  ✓ $1"; }
ko(){ echo "  ✗ $1"; fail=1; }

PORT=4298; URL=nats://127.0.0.1:$PORT; H="http://127.0.0.1:8771"
mkdir -p "$R/flows"
cat > "$R/flows/bulk.vjs" <<'V'
# flow: bulk
api "POST /bulk"
respond 200, {n: len(event), ids: event[].id}
V
cat > "$R/flows/search.vjs" <<'V'
# flow: search
api "POST /search/{kind}"
API_HEADERS = ["x-github-event", "x-functions-key", "X-AuthToken", "X-MfaCode"]
respond 200, {kind: kind, query: query, page: request.query.page, method: request.method, path: request.path, headers: request.headers}
V
cat > "$R/flows/put.vjs" <<'V'
# flow: put
api "PUT /orders/{id}"
respond 200, {id: id, body_id: event.id}
V
cat > "$R/flows/empty.vjs" <<'V'
# flow: empty
api "GET /ping"
respond 200, {event: event}
V
cat > "$R/flows/form.vjs" <<'V'
# flow: form
api "POST /form"
if unreadable:
  respond 200, {text: unreadable.text, base64: unreadable.base64}
else:
  respond 200, {json: event}
end
V
cat > "$R/flows/raw.vjs" <<'V'
# flow: raw
source "vx.mas.raw"
if unreadable:
  emit "vx.mas.rejected", {raw: unreadable.text, why: unreadable.error}
else:
  emit "vx.mas.ok", {id: id}
end
V
cat > "$R/flows/strict.vjs" <<'V'
# flow: strict
source "vx.mas.strict"
emit "vx.mas.strict_ok", {id: id}
V

nats-server -js -sd "$S/n" -a 127.0.0.1 -p $PORT > /dev/null 2>&1 & NP=$!
for _ in $(seq 1 50); do nats -s $URL server check connection > /dev/null 2>&1 && break; sleep 0.1; done
NATS_URL=$URL VEJAS_ROOT="$R" VEJAS_HTTP_ADDR=127.0.0.1:8771 "$BIN" > "$S/rt.log" 2>&1 & RT=$!
until curl -sf -o /dev/null "$H/healthz"; do sleep 0.1; done
for _ in $(seq 1 100); do [ "$(grep -c 'consuming vx.mas' "$S/rt.log")" -ge 2 ] && break; sleep 0.1; done
nats -s $URL sub 'vx.mas.>' > "$S/bus" 2>/dev/null & SUB=$!
sleep 0.5
post(){ curl -s -m 10 -w ' %{http_code}' "$@"; }
hops(){ python3 -c 'import json, sys
try: print(len(json.load(open(sys.argv[1])).get("hops", [])))
except Exception: print(0)' "$1"; }
traced(){ for _ in $(seq 1 100); do curl -s "$H/traces/$1" > "$2"; [ "$(hops "$2")" -ge 1 ] && break; sleep 0.1; done; }

echo "── an api flow's event is the body as sent"
A=$(post -X POST -H 'Content-Type: application/json' -d '[{"id":"a"},{"id":"b"}]' "$H/api/bulk")
[ "$A" = '{"ids":["a","b"],"n":2} 200' ] && ok "an array body: $A" || ko "array body: $A"
# headers that carry a credential under names of every kind, two of them
# named by the flow's API_HEADERS
CREDS=(Authorization Authentication Proxy-Authorization X-Api-Key Cookie Cookie2 X-Amzn-Oidc-Accesstoken X-Amzn-Oidc-Data
  Cf-Access-Jwt-Assertion X-Goog-Iap-Jwt-Assertion Ocp-Apim-Subscription-Key X-Functions-Key X-Access-Key X-CSRFToken
  X-Session-Id X-AuthToken X-AccessToken X-RefreshToken X-SessionToken X-Hub-Signature-256 X-Shopify-Hmac-Sha256)
HDRS=(); i=0; for h in "${CREDS[@]}"; do i=$((i+1)); HDRS+=(-H "$h: cred$i-e2e-SECRET"); done
TS=7a1b2c3d4e5f60718293a4b5c6d7e8f9; PS=0a1b2c3d4e5f6a7b
# and one whose camelCase tells in the flow's spelling only
A=$(post -X POST -H 'Content-Type: application/json' -H 'X-Request-ID: rq-7' -H 'X-Github-Event: push' -H 'X-Tenant: t1' \
  -H "traceparent: 00-$TS-$PS-01" "${HDRS[@]}" -H 'x-mfacode: mfa-e2e-SECRET' -d '{"query":"blue shoes"}' "$H/api/search/products?page=2")
echo "$A" | grep -q '"query":"blue shoes"' && ok "a body field named query is the body's" || ko "query: $A"
echo "$A" | grep -q '"page":"2"' && echo "$A" | grep -q '"kind":"products"' \
  && ok "the query string in request.query, the path parameter a variable" || ko "page, kind: $A"
echo "$A" | grep -q '"method":"POST"' && echo "$A" | grep -q '"path":"/search/products"' \
  && ok "request has the method and the path" || ko "request: $A"
GOT=$(python3 -c 'import json, sys; print(" ".join(sorted(json.loads(sys.argv[1].rsplit(" ", 1)[0])["headers"])))' "$A" 2>/dev/null)
[ "$GOT" = "accept content-type user-agent x-github-event x-request-id" ] \
  && ok "the headers: those that say what the request is, and the one API_HEADERS names" || ko "headers: $GOT ($A)"
! echo "$A" | grep -q 'e2e-SECRET' && ok "none of the $((${#CREDS[@]} + 1)) that carry a credential, three of them named by API_HEADERS" \
  || ko "credentials: $A"
traced $TS "$S/search.json"
python3 - "$S/search.json" <<'PY' && ok "the trace record masks the header the flow named, says which it was not given, and holds no credential" \
  || ko "record: $(cut -c1-400 "$S/search.json")"
import json, sys
raw = open(sys.argv[1]).read()
assert "e2e-SECRET" not in raw, "a credential in the record"
hop = [h for h in json.loads(raw)["hops"] if isinstance(h.get("input"), dict) and "request" in h["input"]][0]
req = hop["input"]["request"]
got = req["headers"]
assert (got["x-github-event"], got["x-request-id"]) == ("<masked>", "rq-7"), got
named = {"x-functions-key", "x-authtoken", "x-mfacode"}
assert not named & set(got) and named <= set(req.get("withheld_headers", [])), req
PY
A=$(post -X PUT -H 'Content-Type: application/json' -d '{"id":"from-body"}' "$H/api/orders/from-path")
[ "$A" = '{"body_id":"from-body","id":"from-path"} 200' ] && ok "a path parameter over a body field: $A" || ko "put: $A"
A=$(post "$H/api/ping")
[ "$A" = '{"event":null} 200' ] && ok "no body: event is null" || ko "ping: $A"

echo "── a body that is not JSON"
A=$(post -X POST -d 'not json' "$H/api/bulk")
echo "$A" | grep -q "the request's body is not JSON" && echo "$A" | grep -q ' 400$' \
  && ok "400 for a flow that does not read unreadable" || ko "bulk not JSON: $A"
A=$(post -X POST -d 'a=1&b=2' "$H/api/form")
[ "$A" = '{"base64":null,"text":"a=1&b=2"} 200' ] && ok "a flow that reads unreadable takes it, as text: $A" || ko "form: $A"
python3 -c 'import sys; open(sys.argv[1], "wb").write(b"{\"password\":\"hunter2-B64\",\"city\":\"" + bytes([0xe9]) + b"\"}")' "$S/latin1"
B64=$(base64 -w0 < "$S/latin1")
TB=8b2c3d4e5f60718293a4b5c6d7e8f9a0; PB=1b2c3d4e5f6a7b8c
A=$(post -X POST -H "traceparent: 00-$TB-$PB-01" --data-binary @"$S/latin1" "$H/api/form")
echo "$A" | grep -qF "\"base64\":\"$B64\"" && echo "$A" | grep -q '"text":null' \
  && ok "bytes that are not UTF-8 come in base64, and only so" || ko "not UTF-8: $A"
traced $TB "$S/form.json"
[ "$(hops "$S/form.json")" -ge 1 ] && grep -q '<masked>' "$S/form.json" && ! grep -qF "$B64" "$S/form.json" && ! grep -q 'hunter2' "$S/form.json" \
  && ok "the trace record masks those bytes, which hold a credential" || ko "record: $(cut -c1-400 "$S/form.json")"
# UTF-16, whose NULs a decoder takes for UTF-8
python3 -c 'import sys; open(sys.argv[1], "wb").write("id=7&note=hello".encode("utf-16-le"))' "$S/utf16"
B16=$(base64 -w0 < "$S/utf16")
TU=9c3d4e5f60718293a4b5c6d7e8f9a0b1; PU=2c3d4e5f6a7b8c9d
A=$(post -X POST -H "traceparent: 00-$TU-$PU-01" --data-binary @"$S/utf16" "$H/api/form")
echo "$A" | grep -qF "\"base64\":\"$B16\"" && echo "$A" | grep -q '"text":null' \
  && ok "UTF-16 comes in base64 too, not as a text" || ko "UTF-16: $A"
traced $TU "$S/utf16.json"
[ "$(hops "$S/utf16.json")" -ge 1 ] && grep -q '<masked>' "$S/utf16.json" && ! grep -qF "$B16" "$S/utf16.json" \
  && ok "the trace record masks bytes no mask can read, whole" || ko "record: $(cut -c1-400 "$S/utf16.json")"
# a fixed-width record its NULs pad: a text
python3 -c 'import sys; open(sys.argv[1], "wb").write(b"CUST0001Dupont" + bytes(14) + b"Paris" + bytes(15) + b"000123")' "$S/padded"
A=$(post -X POST --data-binary @"$S/padded" "$H/api/form")
echo "$A" | grep -q '"text":"CUST0001Dupont' && echo "$A" | grep -q '"base64":null' \
  && ok "a record its NULs pad comes as a text" || ko "padded: $A"
# FIX, whose fields SOH delimits
python3 -c 'import sys; open(sys.argv[1], "wb").write(b"8=FIX.4.2" + bytes([1]) + b"35=A" + bytes([1]) + b"49=SENDER" + bytes([1]) + b"98=0" + bytes([1]) + b"108=30" + bytes([1]))' "$S/fix"
A=$(post -X POST --data-binary @"$S/fix" "$H/api/form")
echo "$A" | grep -q '"text":"8=FIX.4.2' && echo "$A" | grep -q '"base64":null' \
  && ok "a FIX message comes as a text" || ko "fix: $A"
API=$(curl -s -m 5 "$H/api/openapi.json")
python3 - "$API" <<'PY' && ok "OpenAPI: any JSON, not required; any bytes for the flow that reads unreadable, a 400 for the others; the headers a flow is given" \
  || ko "openapi: $(echo "$API" | cut -c1-300)"
import json, sys
paths = json.loads(sys.argv[1])["paths"]
form, bulk, ping = paths["/form"]["post"], paths["/bulk"]["post"], paths["/ping"]["get"]
assert form["requestBody"]["required"] is False, form
assert form["requestBody"]["content"]["*/*"]["schema"] == {"type": "string", "format": "binary"}, form
assert "400" not in form["responses"] and "400" in bulk["responses"] and "400" in ping["responses"], (form, bulk, ping)
assert bulk["requestBody"]["content"]["application/json"]["schema"] == {}, bulk
search = paths["/search/{kind}"]["post"]
assert [p["name"] for p in search["parameters"] if p["in"] == "header"] == ["x-github-event"], search["parameters"]
PY
A=$(post -X POST -H 'Content-Type: application/json' -d '{"k":1}' "$H/api/form")
[ "$A" = '{"json":{"k":1}} 200' ] && ok "and JSON as usual" || ko "form JSON: $A"

echo "── on the bus, a message that is not JSON"
nats -s $URL pub vx.mas.raw 'id=42;bad' > /dev/null 2>&1
nats -s $URL pub vx.mas.raw '{"id":"j1"}' > /dev/null 2>&1
nats -s $URL pub vx.mas.strict 'still bad' > /dev/null 2>&1
for _ in $(seq 1 50); do grep -q 'vx.mas.ok' "$S/bus" && grep -q 'vx.mas.rejected' "$S/bus" && break; sleep 0.1; done
grep -q '"raw":"id=42;bad"' "$S/bus" && ok "a flow that reads unreadable publishes its own reject, with the message" || ko "bus: $(cat "$S/bus")"
grep -q '"id":"j1"' "$S/bus" && ok "and runs a JSON message as before" || ko "bus JSON: $(cat "$S/bus")"
letters(){ curl -s -m 5 "$H/dlq" | python3 -c 'import json,sys; print(" ".join(d.get("last_error","")[:8] + ":" + d.get("unit","?") for d in json.load(sys.stdin)["dead_letters"]))'; }
L=""; for _ in $(seq 1 50); do L=$(letters); echo "$L" | grep -q strict && break; sleep 0.1; done
echo "$L" | grep -q "bad json:flow:strict" && ok "a flow that does not read it leaves it to the dead letters" || ko "letters: $L"
# UTF-16 on the bus: its NULs keep every mask from reading it as a text
python3 -c 'import sys; open(sys.argv[1], "wb").write("password=hunter2-U16BUS&user=bob".encode("utf-16-le"))' "$S/u16bus"
nats -s $URL pub vx.mas.strict --force-stdin < "$S/u16bus" > /dev/null 2>&1
for _ in $(seq 1 50); do curl -s -m 5 "$H/dlq" > "$S/dlq"; [ "$(grep -o 'bad json' "$S/dlq" | wc -l)" -ge 2 ] && break; sleep 0.1; done
curl -s -m 5 "$H/events?flow=flow%3Astrict" > "$S/strict_events"
[ "$(grep -o 'bad json' "$S/dlq" | wc -l)" -ge 2 ] && grep -q '<masked>' "$S/strict_events" \
  && ! grep -qE 'h.u0000u.u0000n|hunter2' "$S/dlq" "$S/strict_events" \
  && ok "a dead letter of UTF-16 is masked whole, in /dlq and /events" || ko "UTF-16 letter: $(cut -c1-300 "$S/dlq") $(cut -c1-300 "$S/strict_events")"
! echo "$L" | grep -q "flow:raw" && ok "the reading flow left no dead letter" || ko "letters: $L"

echo "── time travel runs a message that is not JSON"
CAND=$(python3 -c 'import json; print(json.dumps({"file": "flows/raw.vjs", "candidate": "# flow: raw\nsource \"vx.mas.raw\"\nemit \"vx.mas.ok\", {id: id}\n", "n": 10}))')
TT=$(curl -s -m 20 -X POST -H 'Content-Type: application/json' -d "$CAND" "$H/surface/timetravel")
python3 - "$TT" <<'PY' && ok "both messages replayed; the version that does not read unreadable leaves the one that is not JSON to the dead letters" || ko "time travel: $(echo "$TT" | cut -c1-400)"
import json, sys
t = json.loads(sys.argv[1])
assert (t["events"], t["changed"]) == (2, 1), t
gone = [r for r in t["results"] if r["changed"]]
assert gone[0]["before"]["emits"][0]["subject"] == "vx.mas.rejected" and "not JSON" in gone[0]["after"]["error"], gone
PY

[ $fail -eq 0 ] && echo "MESSAGE AS SENT HOLDS" || echo "MESSAGE AS SENT REGRESSION — do not ship"
exit $fail
