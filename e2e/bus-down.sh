#!/bin/bash
# An event the bus did not confirm is never answered as taken. With the bus down,
# an `api` flow that emits answered 200 and its event was lost; it now answers
# 503, as http-in does, and the caller sends it again (ADR-0002). A flow that
# emits nothing does not need the bus to answer.
#   e2e/bus-down.sh                          (needs the nats CLI)
set -uo pipefail
cd "$(dirname "$0")/.."
BIN="$PWD/core/target/release/vejas-runtime"
[ -x "$BIN" ] || { echo "build first" >&2; exit 1; }
S=$(mktemp -d); R=$(mktemp -d)
RT=""; NP=""; SUB=""
trap 'kill $RT $NP $SUB 2>/dev/null; wait $RT $NP $SUB 2>/dev/null; rm -rf "$S" "$R"' EXIT
fail=0
ok(){ echo "  ✓ $1"; }
ko(){ echo "  ✗ $1"; fail=1; }

URL=nats://127.0.0.1:4291; H="http://127.0.0.1:8762"; IN=18795
mkdir -p "$R/flows" "$R/connectors"
printf '# flow: order\napi "POST /orders"\nemit "vx.orders.new", {id: id}\nrespond 201, {id: id}\n' > "$R/flows/order.vjs"
printf '# flow: price\napi "POST /price"\nrespond 200, {total: total * 2}\n' > "$R/flows/price.vjs"
printf '# connector: http_in\ndriver "http-in"\nPORT = %s\n' $IN > "$R/connectors/http_in.vjs"

nats-server -js -sd "$S/n" -a 127.0.0.1 -p 4291 > /dev/null 2>&1 & NP=$!
sleep 0.5
NATS_URL=$URL VEJAS_ROOT="$R" VEJAS_HTTP_ADDR=127.0.0.1:8762 \
  "$BIN" > "$S/rt.log" 2>&1 & RT=$!
for _ in $(seq 1 300); do curl -sf -o /dev/null "$H/healthz" && break; sleep 0.1; done
curl -sf -o /dev/null "$H/healthz" || { echo "the runtime never came up"; tail -20 "$S/rt.log"; exit 1; }
order(){ curl -s -m 15 -o "$S/answer" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
  -d "{\"id\":$1}" "$H/api/orders"; }
ingest(){ curl -s -m 15 -o "$S/answer" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
  -d '{"id":1}' "http://127.0.0.1:$IN/ingest/orders.raw"; }
for _ in $(seq 1 100); do [ "$(ingest)" = "202" ] && break; sleep 0.1; done
[ "$(ingest)" = "202" ] || { echo "http-in never took an event"; tail -20 "$S/rt.log"; exit 1; }
nats -s $URL sub vx.orders.new --raw > "$S/bus" 2>/dev/null & SUB=$!
sleep 0.5

echo "── the bus up: the order is answered once its event is on the bus"
C=$(order 1)
[ "$C" = "201" ] && ok "the flow answered $C" || ko "the flow answered $C: $(cat "$S/answer")"
for _ in $(seq 1 50); do grep -q '"id":1' "$S/bus" && break; sleep 0.1; done
grep -q '"id":1' "$S/bus" && ok "its event is on the bus" || ko "its event is not on the bus"

echo "── the bus down: no event is answered as taken"
kill $NP; wait $NP 2>/dev/null; NP=""
sleep 1
C=$(order 2)
[ "$C" = "503" ] && ok "the flow that emits answered $C" || ko "the flow that emits answered $C: $(cat "$S/answer")"
grep -q 'send the request again' "$S/answer" && ok "the answer says what to do" || ko "the answer: $(cat "$S/answer")"
C=$(curl -s -m 15 -o "$S/answer" -w '%{http_code}' -X POST -H 'Content-Type: application/json' -d '{"total":21}' "$H/api/price")
[ "$C" = "200" ] && grep -q '"total":42' "$S/answer" && ok "a flow that emits nothing still answers ($C)" \
  || ko "the flow that emits nothing answered $C: $(cat "$S/answer")"
C=$(ingest)
[ "$C" = "503" ] && ok "http-in answered $C" || ko "http-in answered $C: $(cat "$S/answer")"
grep -q 'send it again' "$S/answer" && ok "its answer says what to do" || ko "its answer: $(cat "$S/answer")"

echo "── the bus back: the order sent again is taken"
nats-server -js -sd "$S/n" -a 127.0.0.1 -p 4291 > /dev/null 2>&1 & NP=$!
C=""
for _ in $(seq 1 100); do C=$(order 2); [ "$C" = "201" ] && break; sleep 0.2; done
[ "$C" = "201" ] && ok "the order sent again answered $C" || ko "the order sent again answered $C: $(cat "$S/answer")"

[ $fail -eq 0 ] && echo "BUS DOWN HOLDS" || echo "BUS DOWN REGRESSION — do not ship"
exit $fail
