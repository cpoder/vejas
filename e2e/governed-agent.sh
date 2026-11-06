#!/bin/bash
# What an agent holding only VEJAS_TOKEN can reach in governed mode, and what an
# open GET can read at all (ADR-0008, ADR-0024). Secret values are write-only:
# every path that SHOWS a run evaluates it in shadow, so a secret comes back as
# the placeholder <secret:path/key>, while a real run still gets the value. A
# time-travel candidate is arbitrary code, so this cannot be a filter on the
# output (v0.3.6 returned the value in the diff, and over GET /preview with no
# token at all).
#   e2e/governed-agent.sh                    (needs the nats CLI)
set -uo pipefail
cd "$(dirname "$0")/.."
BIN="$PWD/core/target/release/vejas-runtime"
[ -x "$BIN" ] || { echo "build first" >&2; exit 1; }
S=$(mktemp -d); R=$(mktemp -d)
trap 'kill $RT $NP 2>/dev/null; wait $RT $NP 2>/dev/null; rm -rf "$S" "$R"' EXIT
fail=0
ok(){ echo "  ✓ $1"; }
ko(){ echo "  ✗ $1"; fail=1; }

SECRET="canary-s3cret-$$"
AGENT="agent-tok-$$"
HUMAN="human-tok-$$"
H="http://127.0.0.1:8749"
URL=nats://127.0.0.1:4278
PLACEHOLDER='<secret:demo/key>'

mkdir -p "$R/flows/fixtures"
cat > "$R/flows/sdemo.vjs" <<'VJS'
# flow: sdemo
source "vx.sdemo.in"
tool "secret demo"
CUTOFF = 1
api_key = secret("demo/key")
emit "vx.sdemo.out", {id: id, auth: f"Bearer {api_key}"}
VJS
echo '{"id": 1}' > "$R/flows/fixtures/sdemo.json"

nats-server -js -sd "$S/n" -a 127.0.0.1 -p 4278 > /dev/null 2>&1 & NP=$!
sleep 0.5
NATS_URL=$URL VEJAS_ROOT="$R" VEJAS_HTTP_ADDR=127.0.0.1:8749 \
  VEJAS_REQUIRE_APPROVAL=1 VEJAS_TOKEN=$AGENT VEJAS_APPROVAL_TOKEN=$HUMAN \
  VEJAS_SECRET_DEMO_KEY=$SECRET "$BIN" > "$S/rt.log" 2>&1 & RT=$!
until curl -sf -o /dev/null "$H/healthz"; do sleep 0.1; done

mcp(){ # tool, arguments as JSON
  curl -s -m 20 -H 'Content-Type: application/json' -H "Authorization: Bearer $AGENT" \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"$1\",\"arguments\":$2}}" \
    "$H/mcp"
}
# The stream and the durable exist only once the flow runs: an event published
# before that is lost, and replay and time-travel then have nothing to show.
for _ in $(seq 1 100); do
  mcp vejas_topology '{}' | grep -q 'running' && break
  sleep 0.1
done
shadowed(){ # label, output: the placeholder is there, the value is not
  if echo "$2" | grep -q "$SECRET"; then ko "$1 returned the secret VALUE"
  elif echo "$2" | grep -q "$PLACEHOLDER"; then ok "$1 shows the placeholder"
  else ko "$1 answered neither value nor placeholder: $(echo "$2" | cut -c1-160)"; fi
}

echo "── a real run still gets the real value"
nats -s $URL sub vx.sdemo.out --count=1 --raw > "$S/live.out" 2>/dev/null & SUB=$!
sleep 0.5
nats -s $URL pub vx.sdemo.in '{"id": 7}' > /dev/null 2>&1
for _ in $(seq 1 50); do [ -s "$S/live.out" ] && break; sleep 0.1; done
kill $SUB 2>/dev/null
grep -q "$SECRET" "$S/live.out" && ok "the live emit carries the value" \
  || ko "the live run lost its secret: $(cut -c1-120 "$S/live.out")"

echo "── every path that shows a run is shadowed"
shadowed "GET /preview (no token)" "$(curl -s -m 10 "$H/preview?file=flows/sdemo.vjs")"
shadowed "vejas_preview" "$(mcp vejas_preview '{"file":"flows/sdemo.vjs"}')"
shadowed "vejas_run_flow" "$(mcp vejas_run_flow '{"file":"flows/sdemo.vjs","input":{"id":1}}')"
shadowed "flow-as-tool" "$(mcp flow_sdemo '{"id":1}')"
shadowed "vejas_replay_literal" "$(mcp vejas_replay_literal '{"file":"flows/sdemo.vjs","name":"CUTOFF","key":"-","value":2,"n":5}')"

CAND='# flow: sdemo\nsource \"vx.sdemo.in\"\nv = secret(\"demo/key\")\nemit \"vx.sdemo.out\", {id: id, leak: v, loud: upper(v)}\n'
UPPER=$(echo "$SECRET" | tr '[:lower:]' '[:upper:]')
OUT=$(mcp vejas_time_travel "{\"file\":\"flows/sdemo.vjs\",\"candidate\":\"$CAND\",\"n\":5}")
shadowed "vejas_time_travel" "$OUT"
echo "$OUT" | grep -q "$UPPER" && ko "a transformed value left through time-travel" \
  || ok "a candidate cannot transform the value out either"

mcp vejas_canary_start "{\"file\":\"flows/sdemo.vjs\",\"candidate\":\"$CAND\"}" > /dev/null
sleep 1
nats -s $URL pub vx.sdemo.in '{"id": 8}' > /dev/null 2>&1
OUT=""
for _ in $(seq 1 50); do
  OUT=$(mcp vejas_canary_status '{"file":"flows/sdemo.vjs"}')
  echo "$OUT" | grep -q 'leak' && break
  sleep 0.1
done
shadowed "vejas_canary_status" "$OUT"
mcp vejas_canary_stop '{"file":"flows/sdemo.vjs"}' > /dev/null

echo "── shadow stays fail-closed"
CAND='# flow: sdemo\nsource \"vx.sdemo.in\"\nv = secret(\"nope/x\")\nemit \"vx.sdemo.out\", {v: v}\n'
mcp vejas_time_travel "{\"file\":\"flows/sdemo.vjs\",\"candidate\":\"$CAND\",\"n\":1}" | grep -q 'nope/x' \
  && ok "a missing secret aborts a shadow run" || ko "a missing secret did not abort in shadow"

grep -q "$SECRET" "$S/rt.log" && ko "the value reached the runtime log" || ok "the value is not in the log"

[ $fail -eq 0 ] && echo "GOVERNED AGENT SURFACE HOLDS" || echo "GOVERNED AGENT SURFACE REGRESSION — do not ship"
exit $fail
