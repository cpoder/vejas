#!/bin/bash
# What a detect unit's engine loses without an error is counted and said.
#
#   L1 cap       with VEJAS_DETECT_MAX_RUNS=3, a sequence that waits on more
#                than 3 open runs drops the rest: vejas_detect_losses_total
#                {kind="runs_dropped"} counts them on /metrics
#   L2 filter    an event a .where() cannot judge (the field is missing) is
#                counted as filters_without_value
#   L3 topology  /topology carries the same totals under the detect unit
#   L4 said      a loss of runs is a `losses` event on <root>.flow.events and
#                a line on the runtime's log, once; a filter that could not
#                judge an event is counted, not said
#   L5 reload    runs keep being lost while the unit is reloaded again and
#                again (old and new programs overlap): the exported total
#                never goes down
#
# Throwaway nats(-js)+runtime on their own ports; teardown by captured PID.
#   e2e/detect-losses.sh
set -uo pipefail
cd "$(dirname "$0")/.."
REL="core/target/release/vejas-runtime"; DBG="core/target/debug/vejas-runtime"
if [ -x "$REL" ] && { [ ! -x "$DBG" ] || [ "$REL" -nt "$DBG" ]; }; then BIN="$REL"; else BIN="$DBG"; fi
BIN="${VEJAS_BIN:-$BIN}"
NATSD="$(command -v nats-server || echo "$HOME/.local/bin/nats-server")"
NATS="$(command -v nats || echo "$HOME/.local/bin/nats")"
[ -x "$BIN" ] || { echo "build first: cargo build --release --manifest-path core/Cargo.toml" >&2; exit 1; }
[ -x "$NATSD" ] || { echo "need nats-server on PATH or ~/.local/bin" >&2; exit 1; }
NATS_P=4331; HTTP_P=8731
URL="nats://127.0.0.1:$NATS_P"
API="http://127.0.0.1:$HTTP_P"
WORK="$(mktemp -d)"
ROOT="$WORK/root"; mkdir -p "$ROOT/detects"
NATS_PID=""; RT_PID=""; SUB_PID=""
fail=0
ok()   { echo "  ✓ $1"; }
bad()  { echo "  ✗ $1"; fail=1; }
cleanup() {
  [ -n "$SUB_PID" ] && { kill "$SUB_PID" 2>/dev/null; wait "$SUB_PID" 2>/dev/null; }
  [ -n "$RT_PID" ] && { kill "$RT_PID" 2>/dev/null; wait "$RT_PID" 2>/dev/null; }
  [ -n "$NATS_PID" ] && { kill "$NATS_PID" 2>/dev/null; wait "$NATS_PID" 2>/dev/null; }
  rm -rf "$WORK"
}
trap cleanup EXIT

cat > "$ROOT/detects/capped.vpl" <<'VPL'
connector Bus = nats (
    url: "ignored-by-vejas: the bus is NATS_URL"
)

event Login:
    user: str
event Payment:
    user: str

stream Logins = Login
    .from(Bus, topic: "vxl.login")

stream Payments = Payment
    .from(Bus, topic: "vxl.payment")

stream LoginThenPay = Login as l
    -> Payment where user == l.user as p
    .within(10m)
    .emit(rule: "login_then_pay", user: p.user)
    .to(Bus, topic: "vxl.alerts")
VPL

cat > "$ROOT/detects/filtered.vpl" <<'VPL'
connector Bus = nats (
    url: "ignored-by-vejas: the bus is NATS_URL"
)

event Order:
    id: int
    total: float

stream Orders = Order
    .from(Bus, topic: "vxl.orders")

stream Big = Order
    .where(total > 100.0 and id != 0)
    .emit(id: id, total: total)
    .to(Bus, topic: "vxl.big")
VPL

start_nats() { "$NATSD" -js -sd "$WORK/js" -a 127.0.0.1 -p "$NATS_P" > /dev/null 2>&1 & NATS_PID=$!; sleep 0.5; }
start_rt() {
  VEJAS_ROOT="$ROOT" NATS_URL="$URL" VEJAS_STREAM=TLOSS VEJAS_SUBJECT_ROOT=vxl \
    VEJAS_HTTP_ADDR="127.0.0.1:$HTTP_P" VEJAS_ACK_WAIT_SECS=1 VEJAS_SNAPSHOT_SECS=1 \
    VEJAS_DETECT_MAX_RUNS=3 \
    "$BIN" >> "$WORK/rt.log" 2>&1 &
  RT_PID=$!
  until curl -sf -o /dev/null "$API/healthz"; do sleep 0.05; done
  sleep 1.5
}
loss() { # unit kind -> exported total
  curl -s "$API/metrics" | grep "vejas_detect_losses_total{unit=\"$1\",kind=\"$2\"}" | awk '{print $2}'
}
wait_loss() { # unit kind min
  local deadline=$((SECONDS+15)) v
  while [ "$SECONDS" -lt "$deadline" ]; do
    v=$(loss "$1" "$2"); [ "${v:-0}" -ge "$3" ] && { echo "$v"; return 0; }; sleep 0.2
  done
  echo "${v:-0}"; return 1
}

start_nats
( timeout 60 "$NATS" -s "$URL" sub vxl.flow.events --raw > "$WORK/events.txt" 2>/dev/null ) & SUB_PID=$!
start_rt

echo "L1 cap"
for i in $(seq 1 8); do "$NATS" -s "$URL" pub vxl.login "{\"event_type\":\"Login\",\"user\":\"u$i\"}" > /dev/null 2>&1; done
if dropped=$(wait_loss detect:capped runs_dropped 5); then
  ok "8 logins waiting on a cap of 3: runs_dropped=$dropped"
else
  bad "runs_dropped stayed at $dropped (want >= 5)"
fi

echo "L2 filter"
"$NATS" -s "$URL" pub vxl.orders '{"event_type":"Order","id":7}' > /dev/null 2>&1
"$NATS" -s "$URL" pub vxl.orders '{"event_type":"Order","id":8,"total":150.0}' > /dev/null 2>&1
if nv=$(wait_loss detect:filtered filters_without_value 1); then
  ok "an order without a total is counted: filters_without_value=$nv"
else
  bad "filters_without_value stayed at $nv"
fi
[ "$(loss detect:filtered runs_dropped)" = "0" ] && ok "a unit that lost no run says 0" || bad "detect:filtered runs_dropped is not 0"

echo "L3 topology"
curl -s "$API/topology" > "$WORK/topo.json"
if python3 - "$WORK/topo.json" "$dropped" <<'PY'
import json, sys
t = json.load(open(sys.argv[1]))
d = {u["name"]: u for u in t["detects"]}
l = d["detect:capped"]["losses"]
assert l["runs_dropped"] == int(sys.argv[2]), l
assert set(l) == {"runs_dropped", "runs_evicted", "filters_without_value", "timer_periods_skipped"}, l
assert d["detect:filtered"]["losses"]["filters_without_value"] >= 1, d["detect:filtered"]
PY
then ok "/topology carries the totals"; else bad "/topology losses wrong: $(cat "$WORK/topo.json")"; fi

echo "L4 said"
sleep 0.5
if grep -q '"transition":"losses"' "$WORK/events.txt" && grep -q '"unit":"detect:capped"' "$WORK/events.txt"; then
  ok "a losses event on vxl.flow.events"
else
  bad "no losses event: $(cat "$WORK/events.txt")"
fi
grep -q 'detect:capped: the engine lost runs or timer periods without an error' "$WORK/rt.log" && ok "a line on the log" || bad "no log line"
grep -q '"unit":"detect:filtered"' "$WORK/events.txt" && bad "a filter loss was said on the bus" || ok "a filter that could not judge is not said"
n=$(grep -c '"unit":"detect:capped"' "$WORK/events.txt")
[ "$n" -le 1 ] && ok "said once, not per event ($n)" || bad "said $n times in a minute"

echo "L5 reload"
( i=0; while [ -f "$WORK/feeding" ]; do i=$((i+1)); "$NATS" -s "$URL" pub vxl.login "{\"event_type\":\"Login\",\"user\":\"r$i\"}" > /dev/null 2>&1; done ) &
FEED_PID=$!; touch "$WORK/feeding"
( while [ -f "$WORK/feeding" ]; do loss detect:capped runs_dropped; sleep 0.1; done ) > "$WORK/samples.txt" &
SAMPLE_PID=$!
for r in 1 2 3 4 5; do
  sleep 1; touch "$ROOT/detects/capped.vpl"; curl -s -X POST -o /dev/null "$API/reload"
done
sleep 2; rm -f "$WORK/feeding"; wait "$FEED_PID" "$SAMPLE_PID" 2>/dev/null
if python3 - "$WORK/samples.txt" "$dropped" <<'PY'
import sys
v = [int(x) for x in open(sys.argv[1]).read().split() if x.strip().isdigit()]
assert len(v) > 20, f"only {len(v)} samples"
back = [(a, b) for a, b in zip(v, v[1:]) if b < a]
assert not back, f"went down: {back[:5]}"
assert v[-1] > int(sys.argv[2]), f"did not grow past {sys.argv[2]}: {v[-1]}"
print(f"{len(v)} samples, {v[0]} -> {v[-1]}")
PY
then ok "five reloads under load: the total never went down"; else bad "the total went down across a reload"; fi
grep -q 'detect:capped' <(curl -s "$API/topology") && ok "the unit runs after the reloads" || bad "unit gone after reloads"

[ "$fail" = 0 ] && echo "detect-losses: all passed" || { echo "detect-losses: FAILED"; tail -20 "$WORK/rt.log"; }
exit "$fail"
