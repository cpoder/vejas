#!/bin/bash
# The documented API surface, executed against master (ADR-0029 R7: a guide
# whose commands silently diverge from the code is trust erosion in slow
# motion). Each block below guards a docs page — the assertion mirrors what
# the page SHOWS, so a behavior change that invalidates the docs fails CI
# with the page named. Not a feature test suite: the feature suites live in
# e2e/transport, e2e/admission, core tests. This checks the CONTRACT the
# docs promise.
#   e2e/doc-examples.sh            (needs the nats CLI)
set -uo pipefail
cd "$(dirname "$0")/.."

BIN="core/target/release/vejas-runtime"
[ -x "$BIN" ] || { echo "build first" >&2; exit 1; }
S=$(mktemp -d); R=$(mktemp -d)
trap 'kill $RT $NP 2>/dev/null; wait $RT $NP 2>/dev/null; rm -rf "$S" "$R"' EXIT
fail=0
say() { echo "── $1"; }
ko() { echo "  ✗ $1 (docs page: $2)"; fail=1; }
ok() { echo "  ✓ $1"; }

mkdir -p "$R/flows" "$R/connectors" "$R/tests/vjs"
# the first-flow example, verbatim from getting-started/first-flow.md
cat > "$R/flows/helpdesk_ticket_alerts.vjs" << 'VJS'
# flow: helpdesk_ticket_alerts
source "vx.helpdesk.tickets"

SEVERITY_CODES = {"critique": "P1", "haute": "P2"}
ALERT_LEVELS = ["P1", "P2"]

code = SEVERITY_CODES[priority] ?? "P3"
email = lower(requester?.email)

if code in ALERT_LEVELS:
    emit "vx.slack.out", {text: f"[{code}] {subject} — {email}"}
end
VJS
# the sync API example, from guides/expose-an-api.md
cat > "$R/flows/order_status.vjs" << 'VJS'
# flow: order_status
api "GET /orders/{id}"
API_RESPONSE = {id: "string", status: "string"}

respond 200, {id: id, status: "shipped"}
VJS
# the async ingestion example, from guides/expose-an-api.md
printf '# connector: orders_webhook\ndriver "http-in"\nPORT = 8787\n' > "$R/connectors/orders_webhook.vjs"

nats-server -js -sd "$S" -a 127.0.0.1 -p 4270 > /dev/null 2>&1 &
NP=$!
sleep 0.5
NATS_URL=nats://127.0.0.1:4270 VEJAS_ROOT="$R" \
  VEJAS_HTTP_ADDR=127.0.0.1:8740 "$BIN" > "$S/rt.log" 2>&1 &
RT=$!
until curl -sf -o /dev/null http://127.0.0.1:8740/healthz; do sleep 0.1; done
# the http-in connector binds its port AFTER the runtime is healthy — poll the
# webhook itself, not just healthz (a 202 warmup proves it is listening)
for _ in $(seq 100); do
  [ "$(curl -s -o /dev/null -w '%{http_code}' -X POST http://127.0.0.1:8787/ingest/__warmup -d '{}')" = "202" ] && break
  sleep 0.1
done

say "guides/expose-an-api — sync API flow"
BODY=$(curl -sf http://127.0.0.1:8740/api/orders/42)
echo "$BODY" | python3 -c 'import json,sys; d=json.load(sys.stdin); assert d["id"]=="42" and d["status"]=="shipped", d' \
  && ok "GET /api/orders/{id} answers the flow's respond" \
  || ko "sync api flow response" "guides/expose-an-api.md"
curl -sf http://127.0.0.1:8740/api/openapi.json | grep -q '"/orders/{id}"' \
  && ok "route present in openapi.json" \
  || ko "openapi.json misses the documented route" "guides/expose-an-api.md"

say "guides/expose-an-api — async ingestion (202 after pub-ack)"
( timeout 10 nats -s nats://127.0.0.1:4270 sub vx.helpdesk.tickets --count=1 --raw > "$S/ing" 2>/dev/null ) &
CAP=$!
sleep 0.5
CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST http://127.0.0.1:8787/ingest/helpdesk.tickets \
  -d '{"priority":"inconnue","subject":"SAP down","requester":{"email":"Jane@ACME.com"}}')
wait $CAP 2>/dev/null
[ "$CODE" = "202" ] && [ -s "$S/ing" ] \
  && ok "POST /ingest → 202 and the event is on the bus" \
  || ko "ingest contract (got HTTP $CODE)" "guides/expose-an-api.md + getting-started/first-flow.md"

say "getting-started/first-flow — the flow transforms as documented"
# match by CONTENT, not by position — other emits may share the subject
( timeout 10 nats -s nats://127.0.0.1:4270 sub vx.slack.out --count=5 --raw > "$S/alert" 2>/dev/null ) &
CAP=$!
sleep 0.5
curl -sf -o /dev/null -X POST http://127.0.0.1:8787/ingest/helpdesk.tickets \
  -d '{"priority":"haute","subject":"Printer on fire","requester":{"email":"Bob@ACME.com"}}'
for _ in $(seq 40); do grep -q 'Printer on fire' "$S/alert" 2>/dev/null && break; sleep 0.25; done
kill $CAP 2>/dev/null
grep -q '\[P2\] Printer on fire — bob@acme.com' "$S/alert" \
  && ok "transcoding + lower() + f-string emit as shown" \
  || ko "first-flow emit differs from the docs ($(cat "$S/alert" 2>/dev/null))" "getting-started/first-flow.md"

say "guides/rules-view — /surface and /rules shapes"
curl -sf "http://127.0.0.1:8740/surface" | python3 -c '
import json,sys; d=json.load(sys.stdin); s=json.dumps(d)
assert "SEVERITY_CODES" in s and "critique" in s, "documented literal missing"' \
  && ok "/surface lists the editable table" \
  || ko "/surface shape" "guides/rules-view.md"
curl -sf "http://127.0.0.1:8740/rules?file=flows/helpdesk_ticket_alerts.vjs" | python3 -c '
import json,sys; d=json.load(sys.stdin)
rs=d.get("rules",[]); assert rs, "no rules projected"
assert any("ALERT_LEVELS" in json.dumps(r) for r in rs), rs' \
  && ok "/rules projects the documented condition" \
  || ko "/rules shape" "guides/rules-view.md"

say "guides/change-safely — /surface/set edits one literal"
curl -sf -o /dev/null -X POST http://127.0.0.1:8740/surface/set -H 'content-type: application/json' \
  -d '{"file":"flows/helpdesk_ticket_alerts.vjs","name":"SEVERITY_CODES","key":"haute","value":"P1"}' \
  && grep -q '"haute": "P1"' "$R/flows/helpdesk_ticket_alerts.vjs" \
  && ok "editing an existing key lands span-exact in the file" \
  || ko "/surface/set edit" "guides/change-safely.md + concepts/business-surface.md"
# Adding a NEW key to an existing table is a data extension (N1) — the demo
# gesture ("bloquante" → P1), no agent needed. It inserts span-exact and parses.
curl -sf -o /dev/null -X POST http://127.0.0.1:8740/surface/set -H 'content-type: application/json' \
  -d '{"file":"flows/helpdesk_ticket_alerts.vjs","name":"SEVERITY_CODES","key":"bloquante","value":"P1"}' \
  && grep -q '"bloquante": "P1"' "$R/flows/helpdesk_ticket_alerts.vjs" \
  && ok "adding a new table row lands span-exact in the file" \
  || ko "/surface/set add-key" "guides/rules-view.md + concepts/business-surface.md"

say "guides/observability — /metrics, /events, /healthz"
M=$(curl -sf http://127.0.0.1:8740/metrics)
echo "$M" | grep -q '^vejas_up 1' && echo "$M" | grep -q 'vejas_units' \
  && ok "/metrics carries the documented gauges" \
  || ko "/metrics lines" "guides/observability.md"
curl -sf http://127.0.0.1:8740/events | python3 -c 'import json,sys; json.load(sys.stdin)' \
  && ok "/events answers JSON" || ko "/events" "guides/observability.md"

say "guides/dlq-replay — the DLQ surface answers"
curl -sf http://127.0.0.1:8740/dlq | python3 -c 'import json,sys; json.load(sys.stdin)' \
  && ok "GET /dlq answers JSON" || ko "GET /dlq" "guides/dlq-replay.md"

say "guides/governed-mode — refusal, propose, 401, approve (fresh runtime)"
kill $RT 2>/dev/null; wait $RT 2>/dev/null
NATS_URL=nats://127.0.0.1:4270 VEJAS_ROOT="$R" VEJAS_REQUIRE_APPROVAL=1 \
  VEJAS_APPROVAL_TOKEN=humansonly VEJAS_HTTP_ADDR=127.0.0.1:8742 "$BIN" > "$S/rt2.log" 2>&1 &
RT=$!
until curl -sf -o /dev/null http://127.0.0.1:8742/healthz; do sleep 0.1; done
sleep 1
R1=$(curl -s -X POST http://127.0.0.1:8742/surface/set -H 'content-type: application/json' \
  -d '{"file":"flows/helpdesk_ticket_alerts.vjs","name":"SEVERITY_CODES","key":"critique","value":"P0"}')
echo "$R1" | grep -q "approval required" \
  && ok "direct write refused didactically" \
  || ko "governed refusal wording ($R1)" "guides/governed-mode.md"
PID=$(curl -s -X POST http://127.0.0.1:8742/mcp -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"vejas_propose","arguments":{"kind":"set_literal","payload":{"file":"flows/helpdesk_ticket_alerts.vjs","name":"SEVERITY_CODES","key":"critique","value":"P0"}}}}' \
  | python3 -c 'import json,sys; print(json.loads(json.load(sys.stdin)["result"]["content"][0]["text"])["id"])')
[ -n "$PID" ] && ok "vejas_propose returns a pending proposal" || ko "vejas_propose" "guides/governed-mode.md"
CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "http://127.0.0.1:8742/proposals/$PID/approve")
[ "$CODE" = "401" ] && ok "approve without token → 401" || ko "approve sans token → $CODE" "guides/governed-mode.md"
curl -sf -o /dev/null -X POST "http://127.0.0.1:8742/proposals/$PID/approve" -H "X-Approval-Token: humansonly" \
  && sleep 1 && grep -q '"critique": "P0"' "$R/flows/helpdesk_ticket_alerts.vjs" \
  && ok "approve with X-Approval-Token applies the change" \
  || ko "approve+apply" "guides/governed-mode.md"

[ $fail -eq 0 ] && echo "ALL DOCUMENTED CONTRACTS HOLD" || echo "DOC/CODE DIVERGENCE — fix the docs or the code WITH the change"
exit $fail
