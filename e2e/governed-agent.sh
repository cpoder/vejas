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
mkdir -p "$R/connectors"
printf '# connector: exec_env\ndriver "exec-sink"\nSUBJECT = "vx.execenv"\nENV = {"DECLARED": secret("demo/key")}\nCMD = "cat > /dev/null; env > %s/child.env"\n' "$S" \
  > "$R/connectors/exec_env.vjs"

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

echo "── dead letters: the agent reads them, a human acts on them"
code(){ curl -s -o /dev/null -w "%{http_code}" -m 10 "$@"; }
dead_letters(){ curl -s -m 10 "$H/dlq"; }
bury(){ # a dead letter on the bus, as the runtime parks one
  nats -s $URL pub vxdlq.vx.sdemo.in \
    "{\"unit\":\"flow:sdemo\",\"original_subject\":\"vx.sdemo.in\",\"payload\":\"{\\\"id\\\":$1}\",\"error\":\"boom\",\"attempts\":5}" \
    > /dev/null 2>&1
}
BEARER="Authorization: Bearer $AGENT"
JSON='Content-Type: application/json'
dead_letters > /dev/null   # ensures the stream
bury 9
mcp vejas_dlq '{"limit":5}' | grep -q 'flow:sdemo' && ok "the agent reads them" || ko "vejas_dlq shows nothing"
mcp vejas_dlq_purge '{"unit":"flow:sdemo"}' | grep -q 'human action' \
  && ok "vejas_dlq_purge refused" || ko "vejas_dlq_purge NOT governed"
mcp vejas_dlq_replay '{"unit":"flow:sdemo"}' | grep -q 'human action' \
  && ok "vejas_dlq_replay refused" || ko "vejas_dlq_replay NOT governed"
C=$(code -X POST -H "$BEARER" -H "$JSON" -d '{}' "$H/dlq/purge")
[ "$C" = "409" ] && ok "POST /dlq/purge with the agent's token: $C" || ko "/dlq/purge NOT governed ($C)"
C=$(code -X POST -H "$BEARER" -H "$JSON" -d '{}' "$H/dlq/replay")
[ "$C" = "409" ] && ok "POST /dlq/replay with the agent's token: $C" || ko "/dlq/replay NOT governed ($C)"
C=$(code -X POST -H "X-Approval-Token: wrong" -H "$JSON" -d '{}' "$H/dlq/purge")
[ "$C" = "401" ] && ok "a wrong approval token: $C" || ko "a wrong approval token got $C"
C=$(code -X POST -H "$JSON" -d '{}' "$H/dlq/purge")
[ "$C" = "401" ] && ok "no credential at all: $C" || ko "no credential at all got $C"
dead_letters | grep -q 'flow:sdemo' && ok "the dead letter is still there" || ko "the refused calls lost the dead letter"

# what the panel sends: the approval token, never the agent's
OUT=$(curl -s -m 10 -X POST -H "X-Approval-Token: $HUMAN" -H "$JSON" -d '{"unit":"flow:sdemo"}' "$H/dlq/replay")
echo "$OUT" | grep -q '"replayed":1' && ok "the human replays with the approval token alone" \
  || ko "human replay: $OUT"
bury 10
OUT=$(curl -s -m 10 -X POST -H "X-Approval-Token: $HUMAN" -H "$JSON" -d '{"unit":"flow:sdemo"}' "$H/dlq/purge")
echo "$OUT" | grep -q '"purged":1' && ok "the human purges with the approval token alone" \
  || ko "human purge: $OUT"
AUDIT=$(for seq in 1 2 3 4 5 6; do
  nats -s $URL stream get VEJAS_AUDIT $seq --json 2>/dev/null \
    | python3 -c 'import base64,json,sys; print(base64.b64decode(json.load(sys.stdin)["data"]).decode())' 2>/dev/null
done)
echo "$AUDIT" | grep -q '"dlq-replay"' && ok "the replay is audited" || ko "no audit record for the replay"
echo "$AUDIT" | grep -q '"dlq-purge"' && ok "the purge is audited" || ko "no audit record for the purge"

echo "── an exec child gets what its manifest declares, not the runtime's credentials"
nats -s $URL pub vx.execenv '{}' > /dev/null 2>&1
for _ in $(seq 1 50); do [ -s "$S/child.env" ] && break; sleep 0.1; done
[ -s "$S/child.env" ] || ko "the exec child never ran"
grep -q "^DECLARED=$SECRET" "$S/child.env" && ok "a declared secret reaches it" || ko "the declared secret is gone"
LEAKS=$(grep -cE "^VEJAS_(TOKEN|APPROVAL_TOKEN|SECRET_)|$HUMAN|$AGENT" "$S/child.env")
[ "$LEAKS" = "0" ] && ok "no runtime credential by inheritance" || ko "$LEAKS credential line(s) inherited"

echo "── the human can decide from the panel with both tokens set"
C=$(code -X POST -H "X-Approval-Token: $HUMAN" "$H/proposals/nope/approve")
[ "$C" = "422" ] && ok "approve passes the write gate on the approval token alone ($C: no such proposal)" \
  || ko "approve got $C"
C=$(code -X POST -H "$BEARER" "$H/proposals/nope/approve")
[ "$C" = "401" ] && ok "the agent's token still cannot approve ($C)" || ko "the agent's token got $C on approve"

[ $fail -eq 0 ] && echo "GOVERNED AGENT SURFACE HOLDS" || echo "GOVERNED AGENT SURFACE REGRESSION — do not ship"
exit $fail
