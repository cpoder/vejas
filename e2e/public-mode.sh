#!/bin/bash
# The public mode (ADR-0041): a runtime with VEJAS_PUBLIC=1 serves the reads the
# console needs and answers 404 to everything else, whatever the credential.
# The suite reads every route the runtime serves from core/src/main.rs, so a
# route added later is tried here without anyone listing it: it must be refused
# until the allowlist in core/src/public.rs names it, and that list must match
# the one written below, so that widening it is a change made twice on purpose.
#   e2e/public-mode.sh            (BIN=path/to/vejas-runtime to try another build)
set -uo pipefail
cd "$(dirname "$0")/.."
BIN="${BIN:-core/target/release/vejas-runtime}"
[ -x "$BIN" ] || { echo "build first" >&2; exit 1; }
S=$(mktemp -d); R=$(mktemp -d)
RT=; RT2=; NP=
trap 'kill $RT $RT2 $NP 2>/dev/null; wait $RT $RT2 $NP 2>/dev/null; rm -rf "$S" "$R"' EXIT
fail=0
ok(){ echo "  ✓ $1"; }
# a failed check prints the last answer it read, status and body
ko(){ echo "  ✗ $1"; [ -s "$S/body" ] && echo "      last body: $(head -c 300 "$S/body" | tr '\n' ' ')"; fail=1; }
# stop here: what follows would test someone else's process
die(){ echo "  ✗ $1"; echo "PUBLIC MODE NOT TESTED"; tail -20 "$S/rt.log" 2>/dev/null; exit 1; }

URL=nats://127.0.0.1:4307
H="http://127.0.0.1:8792"
TOK="public-tok-$$"
ALLOWED="/ /panel /healthz /topology /graph /surface /events /dlq /traces /file /shape /rules /drivers /evictions"

mkdir -p "$R/flows" "$R/services" "$R/connectors"
printf '# flow: demo\nsource "vx.demo.in"\nTIER = "gold"\nemit "vx.demo.out", {id: event.id, tier: TIER}\n' > "$R/flows/demo.vjs"
printf '{"id": "sample-1"}\n' > "$R/flows/demo.fixture.json"
printf 'api "GET /hello"\nrespond 200, {hello: "world"}\n' > "$R/flows/hello.vjs"
printf 'emit "vx.demo.note", {n: n}\n' > "$R/services/note.vjs"
# a flow whose literal is named as a credential: its value must leave in no answer
printf '# flow: keyed\nsource "vx.keyed.in"\nAPI_TOKEN = "s3cr3t-value-77"\nPARTNER = {"name": "Authorization", "value": "pair-s3cr3t-88"}\nemit "vx.keyed.out", {ok: true}\n' > "$R/flows/keyed.vjs"
printf '# connector: tick\ndriver "timer"\nSUBJECT = "vx.demo.tick"\nINTERVAL_SECS = 3600\n' > "$R/connectors/tick.vjs"
# a flow that RUNS with a credential in its code: its trace must not carry it
printf '# flow: bearer\nsource "vx.bearer.in"\nh = {Authorization: "Bearer xyz-tok-55"}\nu = "https://bob:pw-s3cr3t-66@example.com/x"\nemit "vx.bearer.out", {id: event.id}\n' > "$R/flows/bearer.vjs"
# a connector that reads a secret: which one is not a public page's
printf '# connector: vaulted\ndriver "timer"\nSUBJECT = "vx.demo.vault"\nINTERVAL_SECS = 3600\nKEY = secret("vault/partner-key")\n' > "$R/connectors/vaulted.vjs"
# an http-in connector would open a port of its own: it must not start
printf '# connector: inbound\ndriver "http-in"\nPORT = 8794\nSUBJECT = "vx.demo.inbound"\n' > "$R/connectors/inbound.vjs"

for port in 4307 8792 8793 8794; do
  (exec 3<>/dev/tcp/127.0.0.1/$port) 2>/dev/null && die "port $port is already taken: another run, or another process"
done
nats-server -js -sd "$S/n" -a 127.0.0.1 -p 4307 > "$S/nats.log" 2>&1 & NP=$!
for _ in $(seq 1 50); do nats -s $URL account info > /dev/null 2>&1 && break; sleep 0.1; done
kill -0 $NP 2>/dev/null || die "nats-server did not start: $(tail -3 "$S/nats.log")"
NATS_URL=$URL VEJAS_ROOT="$R" VEJAS_HTTP_ADDR=127.0.0.1:8792 VEJAS_PUBLIC=1 VEJAS_TOKEN="$TOK" \
  VEJAS_APPROVAL_TOKEN="approve-$$" VEJAS_STREAM_MAX_AGE_SECS=3600 "$BIN" > "$S/rt.log" 2>&1 & RT=$!
for _ in $(seq 1 100); do curl -sf -o /dev/null "$H/healthz" && break; sleep 0.1; done
grep -qi 'cannot bind' "$S/rt.log" && die "the runtime could not bind: $(grep -i 'cannot bind' "$S/rt.log" | head -1)"
kill -0 $RT 2>/dev/null || die "the runtime exited: $(tail -3 "$S/rt.log")"
curl -sf -o /dev/null "$H/healthz" || die "the runtime's own port does not answer"

# answer CODE BODY of a request
call(){ curl -s -o "$S/body" -w "%{http_code}" "$@"; }
refused(){ [ "$1" = "404" ] && [ "$(cat "$S/body")" = "not found" ]; }

echo "── the allowlist is the one this suite expects"
listed=$(sed -n '/^pub const READS/,/^];/p' core/src/public.rs | grep -oE '"/[^"]*"' | tr -d '"' | sort | tr '\n' ' ')
expected=$(echo $ALLOWED | tr ' ' '\n' | sort | tr '\n' ' ')
[ "$listed" = "$expected" ] && ok "public.rs lists the same $(echo $ALLOWED | wc -w) reads" \
  || ko "public.rs lists [$listed], this suite expects [$expected]"
grep -q "public mode (VEJAS_PUBLIC)" "$S/rt.log" && ok "the runtime says at start that the mode is on" || ko "no start line: $(tail -3 "$S/rt.log")"

echo "── every route of main.rs outside the allowlist is refused, with and without the token"
grep -oE 'tiny_http::Method::[A-Za-z]+, "/[^"]*"' core/src/main.rs \
  | sed -E 's/tiny_http::Method::([A-Za-z]+), "(.*)"/\1 \2/' | sort -u > "$S/routes"
# the prefix routes: an api flow, its OpenAPI document, a decision on a proposal
printf 'Get /api/hello\nGet /api/openapi.json\nPost /proposals/p1/approve\nPost /proposals/p1/reject\nPost /mcp\nGet /metrics\nGet /nothing-here\nDelete /api/hello\n' >> "$S/routes"
n_routes=$(sort -u "$S/routes" | wc -l)
refused_n=0; served_n=0; wrong=0
while read -r m p; do
  M=$(echo "$m" | tr 'a-z' 'A-Z')
  if [ "$M" = "GET" ] && [[ " $ALLOWED " == *" $p "* ]]; then
    case "$p" in
      /file) q="?path=flows/demo.vjs" ;;
      /shape|/rules) q="?file=flows/demo.vjs" ;;
      /traces) q="?search=demo" ;;
      *) q="" ;;
    esac
    c=$(call "$H$p$q")
    if refused "$c" || [ "$c" -ge 500 ]; then ko "GET $p is an allowed read and answered $c"; wrong=1; else served_n=$((served_n + 1)); fi
    # the same path with any other method is a write: refused
    c=$(call -X POST -H "Authorization: Bearer $TOK" "$H$p")
    refused "$c" || { ko "POST $p with the token answered $c"; wrong=1; }
    continue
  fi
  c1=$(call -X "$M" -d '{}' "$H$p"); r1=0; refused "$c1" && r1=1
  c2=$(call -X "$M" -d '{}' -H "Authorization: Bearer $TOK" -H "X-Approval-Token: approve-$$" "$H$p"); r2=0; refused "$c2" && r2=1
  if [ $r1 = 1 ] && [ $r2 = 1 ]; then refused_n=$((refused_n + 1)); else ko "$M $p answered $c1 without and $c2 with the token"; wrong=1; fi
done < <(sort -u "$S/routes")
[ $wrong = 0 ] && ok "$refused_n routes refused both ways, $served_n allowed reads served, out of $n_routes" || ko "see above"
grep -q 'Post /reload' "$S/routes" && grep -q 'Post /mcp' "$S/routes" && grep -q 'Post /dlq/replay' "$S/routes" \
  && ok "the enumeration found the write routes (/reload, /mcp, /dlq/replay)" || ko "the enumeration missed routes: $(wc -l < "$S/routes")"

echo "── the doors the ADR names, one by one"
c=$(call -X POST -H "Authorization: Bearer $TOK" -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' "$H/mcp")
refused "$c" && ok "POST /mcp with the token: 404" || ko "POST /mcp answered $c"
c=$(call -H "Authorization: Bearer $TOK" "$H/api/hello")
refused "$c" && ok "an api flow: 404, with the token too" || ko "GET /api/hello answered $c: $(head -c 80 "$S/body")"
c=$(call "$H/secrets"); refused "$c" && ok "the secrets: 404" || ko "GET /secrets answered $c"
c=$(call "$H/preview?file=flows/demo.vjs"); refused "$c" && ok "a preview (a shown run): 404" || ko "GET /preview answered $c"
c=$(call "$H/fixture?file=flows/demo.vjs"); refused "$c" && ok "a sample: 404" || ko "GET /fixture answered $c"
c=$(call "$H/metrics"); refused "$c" && ok "the metrics: 404" || ko "GET /metrics answered $c"
c=$(curl -s -o /dev/null -I -m 5 -w "%{http_code}" "$H/metrics"); [ "$c" = "404" ] && ok "HEAD /metrics: 404" || ko "HEAD /metrics answered $c"

echo "── a public page reads code, and only code"
c=$(call "$H/file?path=flows/demo.vjs"); [ "$c" = "200" ] && grep -q 'TIER' "$S/body" && ok "a flow's code: 200" || ko "flow code answered $c"
c=$(call "$H/file?path=services/note.vjs"); [ "$c" = "200" ] && ok "a service's code: 200" || ko "service code answered $c"
c=$(call "$H/file?path=connectors/tick.vjs"); [ "$c" = "200" ] && ok "a connector's code: 200" || ko "connector code answered $c"
c=$(call "$H/file?path=flows/demo.fixture.json"); refused "$c" && ok "a sample file: 404" || ko "the sample file answered $c"
c=$(call "$H/file?path=flows/../flows/demo.vjs"); refused "$c" && ok "a path with ..: 404" || ko "a .. path answered $c"
c=$(call "$H/file"); refused "$c" && ok "no path: 404" || ko "no path answered $c"
c=$(call "$H/shape?file=flows/demo.fixture.json"); refused "$c" && ok "the shape of a non-code file: 404" || ko "shape answered $c"

echo "── a literal named as a credential leaves in no answer"
leaked=0; asked=0
for q in / /panel /topology /graph /surface /events /dlq "/traces?search=keyed" /drivers /evictions \
         "/file?path=flows/keyed.vjs" "/shape?file=flows/keyed.vjs" "/rules?file=flows/keyed.vjs"; do
  asked=$((asked + 1))
  curl -s "$H$q" > "$S/b"
  grep -q 's3cr3t-value-77\|pair-s3cr3t-88' "$S/b" && { ko "GET $q shows the credential"; leaked=1; }
done
[ $leaked = 0 ] && ok "$asked allowed reads, none shows either value" || true
grep -q '"API_TOKEN"' <(curl -s "$H/surface") && ok "/surface still lists the literal, masked" || ko "/surface lost the literal"
c=$(call "$H/file?path=flows/keyed.vjs"); refused "$c" && ok "the code of a flow that holds one: 404" || ko "its code answered $c"
c=$(call "$H/shape?file=flows/keyed.vjs"); refused "$c" && ok "its shape: 404" || ko "its shape answered $c"
c=$(call "$H/file?path=flows/demo.vjs"); [ "$c" = "200" ] && ok "a flow without one is still served" || ko "demo code answered $c"

echo "── a trace shows no credential from the code that ran"
for _ in $(seq 1 100); do curl -s "$H/topology" | grep -q '"flow:bearer"' && break; sleep 0.1; done
nats -s $URL pub vx.bearer.in '{"id":"ord-88"}' > /dev/null 2>&1
nats -s $URL pub vx.demo.in '{"id":"ord-99"}' > /dev/null 2>&1
for _ in $(seq 1 50); do curl -s "$H/traces?search=ord-88" | grep -q 'trace_id' && break; sleep 0.1; done
c=$(call "$H/traces?search=ord-88"); cp "$S/body" "$S/search"
tid=$(python3 -c 'import json,sys; t=json.load(open(sys.argv[1])).get("traces") or [{}]; print(t[0].get("trace_id",""))' "$S/search" 2>/dev/null)
[ -n "$tid" ] && ok "the run of a flow with a credential in its code is in the record" || ko "no trace for ord-88 ($c)"
c=$(call "$H/traces/$tid"); cp "$S/body" "$S/trace"
[ "$c" = "200" ] && grep -q '"steps"' "$S/trace" && ok "its trace: 200, with its steps" || ko "its trace answered $c"
grep -q 'xyz-tok-55\|pw-s3cr3t-66' "$S/trace" "$S/search" && ko "the trace shows the credential" || ok "neither value is in the trace or the search"
grep -q '"code"' "$S/trace" && ko "the trace carries code from a file that holds a credential" || ok "no code line from that file"
for _ in $(seq 1 50); do curl -s "$H/traces?search=ord-99" | grep -q 'trace_id' && break; sleep 0.1; done
did=$(curl -s "$H/traces?search=ord-99" | python3 -c 'import json,sys; t=json.load(sys.stdin).get("traces") or [{}]; print(t[0].get("trace_id",""))' 2>/dev/null)
c=$(call "$H/traces/$did"); grep -q 'TIER' "$S/body" && ok "a flow without one still shows its code in its trace" || ko "the demo trace has no code ($c)"

echo "── what a public page is not shown"
for q in /topology /graph /surface "/file?path=flows/demo.vjs"; do
  c=$(call "$H$q"); grep -qF "$R/" "$S/body" && ko "GET $q shows the host's path $R ($c)"
done
grep -qF "$R/" <(curl -s "$H/topology"; curl -s "$H/graph"; curl -s "$H/surface"; curl -s "$H/file?path=flows/demo.vjs") \
  || ok "paths are relative to the root in /topology, /graph, /surface and /file"
c=$(call "$H/graph"); grep -q 'secret_refs\|partner-key' "$S/body" && ko "/graph names the connector's secrets" || ok "/graph names no secret a connector reads"
c=$(call "$H/dlq"); [ "$c" = "200" ] && ok "GET /dlq before any dead letter: 200" || ko "GET /dlq answered $c"
nats -s $URL stream info VEJAS_DLQ > /dev/null 2>&1 && ko "reading /dlq made the dead-letter stream" || ok "reading /dlq made no stream"
(exec 3<>/dev/tcp/127.0.0.1/8794) 2>/dev/null && ko "the http-in connector opened port 8794" || ok "the http-in connector opened no port"
grep -q 'http-in does not start on a public runtime' "$S/rt.log" && ok "the log says why it did not start" || ko "no log line for http-in"

echo "── reads are capped"
long=$(printf 'x%.0s' $(seq 1 201))
c=$(call "$H/traces?search=$long"); [ "$c" = "400" ] && ok "a search past 200 characters: 400" || ko "a long search answered $c"
c=$(call "$H/traces/$(printf 'a%.0s' $(seq 1 65))"); [ "$c" = "400" ] && ok "a trace id past 64 characters: 400" || ko "a long trace id answered $c"

echo "── the panel knows the mode"
c=$(call "$H/"); [ "$c" = "200" ] && grep -q "const PUBLIC = 'true' === 'true'" "$S/body" \
  && ok "the page carries the read-only flag" || ko "no flag in the page ($c)"
grep -q 'A live demo, read only' "$S/body" && ok "the page says it is a read-only demo" || ko "no banner text"

echo "── the flows still run, and the console shows them"
for _ in $(seq 1 100); do curl -s "$H/topology" | grep -q '"flow:demo"' && break; sleep 0.1; done
nats -s $URL pub vx.demo.in '{"id":"ord-77"}' > /dev/null 2>&1
for _ in $(seq 1 50); do curl -s "$H/events" | grep -q 'ord-77' && break; sleep 0.1; done
curl -s "$H/events" | grep -q 'ord-77' && ok "a message ran through the flow and shows in /events" || ko "no event: $(curl -s "$H/events" | head -c 200)"

echo "── the stream keeps an hour"
age=$(nats -s $URL stream info VEJAS --json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["config"].get("max_age", 0))')
[ "$age" = "3600000000000" ] && ok "VEJAS max_age is 1 h" || ko "VEJAS max_age is $age"

echo "── without the mode, the same build serves what the mode refuses"
rm -f "$R/connectors/inbound.vjs"   # this runtime would start it
# an operator's limit on the stream, which a new age must leave as it is
nats -s $URL stream edit VEJAS --max-msgs 12345 -f > /dev/null 2>&1
NATS_URL=$URL VEJAS_ROOT="$R" VEJAS_HTTP_ADDR=127.0.0.1:8793 VEJAS_STREAM_MAX_AGE_SECS=7200 "$BIN" > "$S/rt2.log" 2>&1 & RT2=$!
for _ in $(seq 1 100); do curl -sf -o /dev/null "http://127.0.0.1:8793/healthz" && break; sleep 0.1; done
grep -qi 'cannot bind' "$S/rt2.log" && die "the second runtime could not bind: $(grep -i 'cannot bind' "$S/rt2.log" | head -1)"
kill -0 $RT2 2>/dev/null || die "the second runtime exited: $(tail -3 "$S/rt2.log")"
lim=$(nats -s $URL stream info VEJAS --json 2>/dev/null | python3 -c 'import json,sys; c=json.load(sys.stdin)["config"]; print(c.get("max_age", 0), c.get("max_msgs"))')
[ "$lim" = "7200000000000 12345" ] && ok "a new age changes the age only: 2 h, max_msgs still 12345" || ko "VEJAS max_age and max_msgs are $lim"
c=$(call "http://127.0.0.1:8793/metrics"); [ "$c" = "200" ] && ok "GET /metrics: 200" || ko "GET /metrics answered $c"
c=$(call "http://127.0.0.1:8793/"); grep -q "const PUBLIC = 'false' === 'true'" "$S/body" && ok "the page carries no read-only flag" || ko "the flag is wrong ($c)"

echo
[ $fail = 0 ] && echo "PUBLIC MODE HOLDS" || { echo "PUBLIC MODE BROKEN"; tail -20 "$S/rt.log"; }
exit $fail
