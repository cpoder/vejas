#!/bin/bash
# Detect-unit invariants (ADR-0031) — a VPL program under detects/, run by the
# embedded Varpulis engine, on a throwaway nats(-js)+runtime on disjoint ports.
#
#   D1 no-loss        a stateless detection (threshold) survives kill -9 mid-stream:
#                     every alert still arrives after restart (at-least-once,
#                     publish-before-ack, redelivery)
#   D2 sequence       a two-source SASE sequence with .within(2m) fires once per
#                     matching pair, judged in EVENT time: a start 3 min later in
#                     event time does not match, however soon it arrives
#   D3 vpl-check      the engine's verdict on the CLI: ok / refused, with exit codes
#   D4 topology       /topology lists the units under "detects", lang vpl, running
#
# Redelivery latency is set low (VEJAS_ACK_WAIT_SECS=1) so the suite runs in
# seconds. No real credentials, no shared state; teardown by captured PID.
#   e2e/detect/run.sh
set -uo pipefail
cd "$(dirname "$0")/../.."
REL="core/target/release/vejas-runtime"; DBG="core/target/debug/vejas-runtime"
if [ -x "$REL" ] && { [ ! -x "$DBG" ] || [ "$REL" -nt "$DBG" ]; }; then BIN="$REL"; else BIN="$DBG"; fi
NATSD="$(command -v nats-server || echo "$HOME/.local/bin/nats-server")"
NATS="$(command -v nats || echo "$HOME/.local/bin/nats")"
[ -x "$BIN" ] || { echo "build first: cargo build --release --manifest-path core/Cargo.toml" >&2; exit 1; }
[ -x "$NATSD" ] || { echo "need nats-server on PATH or ~/.local/bin" >&2; exit 1; }
export LLMFORMAT=1
NATS_P=4320; HTTP_P=8720
URL="nats://127.0.0.1:$NATS_P"
WORK="$(mktemp -d)"
ROOT="$WORK/root"; mkdir -p "$ROOT/detects"
NATS_PID=""; RT_PID=""
fail=0
ok()   { echo "  ✓ $1"; }
bad()  { echo "  ✗ $1"; fail=1; }
cleanup() { [ -n "$RT_PID" ] && kill "$RT_PID" 2>/dev/null; [ -n "$NATS_PID" ] && kill "$NATS_PID" 2>/dev/null; sleep 0.3; rm -rf "$WORK"; }
trap cleanup EXIT

cat > "$ROOT/detects/threshold.vpl" <<'VPL'
connector Bus = nats (
    url: "ignored-by-vejas: the bus is NATS_URL"
)

event Order:
    id: int
    total: float

stream Orders = Order
    .from(Bus, topic: "vxt.orders")

stream Big = Order
    .where(total > 100.0)
    .emit(id: id, total: total)
    .to(Bus, topic: "vxt.alerts")
VPL

cat > "$ROOT/detects/lateral.vpl" <<'VPL'
connector Bus = nats (
    url: "ignored-by-vejas: the bus is NATS_URL"
)

event SmbConnect:
    host: str
    target: str

event ServiceStart:
    host: str
    image: str

stream Net = SmbConnect
    .from(Bus, topic: "vxt.sysmon.net")

stream Proc = ServiceStart
    .from(Bus, topic: "vxt.sysmon.proc")

stream LateralMovement = SmbConnect as smb
    -> ServiceStart where host == smb.target as svc
    .within(2m)
    .emit(rule: "lateral_movement", from: smb.host, to: svc.host, image: svc.image)
    .to(Bus, topic: "vxt.lateral")
VPL

start_nats() { "$NATSD" -js -sd "$WORK/js" -a 127.0.0.1 -p "$NATS_P" > /dev/null 2>&1 & NATS_PID=$!; sleep 0.5; }
start_rt() {
  VEJAS_ROOT="$ROOT" NATS_URL="$URL" VEJAS_STREAM=TTEST VEJAS_SUBJECT_ROOT=vxt \
    VEJAS_HTTP_ADDR="127.0.0.1:$HTTP_P" VEJAS_ACK_WAIT_SECS=1 "$BIN" >> "$WORK/rt.log" 2>&1 &
  RT_PID=$!
  until curl -sf -o /dev/null "http://127.0.0.1:$HTTP_P/healthz"; do sleep 0.05; done
  sleep 1.5
}
processed() { # unit -> events processed ok
  curl -s "http://127.0.0.1:$HTTP_P/metrics" | grep "vejas_events_processed_total{unit=\"$1\",result=\"ok\"}" | awk '{print $2}'
}
distinct_ids() { python3 - "$1" "$2" <<'PY'
import json, sys
ids=set()
for line in open(sys.argv[1], errors="replace"):
    line=line.strip()
    if not line.startswith("{"): continue
    try: ids.add(json.loads(line)[sys.argv[2]])
    except Exception: pass
print(len(ids))
PY
}

echo "── D3 vpl-check"
out=$("$BIN" vpl-check "$ROOT/detects/threshold.vpl" 2>&1); [ "$out" = "ok" ] && ok "threshold.vpl: ok" || bad "threshold.vpl: $out"
out=$("$BIN" vpl-check "$ROOT/detects/lateral.vpl" 2>&1); [ "$out" = "ok" ] && ok "lateral.vpl: ok" || bad "lateral.vpl: $out"
printf 'event Order:\n    id: int\n\nstream Bad = Order\n    .filter(id == 1)\n    .emit(x: 1)\n' > "$WORK/bad.vpl"
if "$BIN" vpl-check "$WORK/bad.vpl" > /dev/null 2>&1; then bad "a program the engine refuses was accepted"; else ok "a refused program exits non-zero"; fi

start_nats; start_rt

echo "── D4 topology"
topo=$(curl -s "http://127.0.0.1:$HTTP_P/topology")
python3 - "$topo" <<'PY' && ok "two detects listed, lang vpl, running" || bad "topology: $topo"
import json, sys
t=json.loads(sys.argv[1]); d=t.get("detects", [])
names=sorted(x["name"] for x in d)
assert names==["detect:lateral","detect:threshold"], names
assert all(x["lang"]=="vpl" for x in d), d
assert all(x["status"]=="running" for x in d), [x["status"] for x in d]
PY

echo "── D1 no-loss under kill -9 + restart (stateless threshold)"
N=300
( timeout 40 "$NATS" -s "$URL" sub vxt.alerts --raw > "$WORK/alerts.txt" 2>/dev/null ) & SUB_PID=$!
sleep 0.5
for i in $(seq 1 $((N/2))); do "$NATS" -s "$URL" pub vxt.orders "{\"event_type\":\"Order\",\"id\":$i,\"total\":150.5}" > /dev/null 2>&1; done
( for i in $(seq $((N/2+1)) $N); do "$NATS" -s "$URL" pub vxt.orders "{\"event_type\":\"Order\",\"id\":$i,\"total\":150.5}" > /dev/null 2>&1; done ) & P2=$!
sleep 0.4
kill -9 "$RT_PID"; wait "$RT_PID" 2>/dev/null; RT_PID=""
wait "$P2"
start_rt   # the durables resume; un-acked messages redeliver after ack_wait (1 s)
deadline=$((SECONDS+20)); got=0
while [ "$SECONDS" -lt "$deadline" ]; do got=$(distinct_ids "$WORK/alerts.txt" id); [ "$got" -ge "$N" ] && break; sleep 0.5; done
kill "$SUB_PID" 2>/dev/null; pkill -f "[s]ub vxt.alerts" 2>/dev/null
[ "$got" -eq "$N" ] && ok "all $N alerts arrived after the crash (distinct ids)" || bad "expected $N distinct alerts, got $got (see $WORK/rt.log)"

echo "── D2 two-source sequence, judged in event time"
( timeout 30 "$NATS" -s "$URL" sub vxt.lateral --raw > "$WORK/lateral.txt" 2>/dev/null ) & SUB_PID=$!
sleep 0.5
M=20
for i in $(seq 1 $M); do "$NATS" -s "$URL" pub vxt.sysmon.net "{\"event_type\":\"SmbConnect\",\"@timestamp\":\"2026-09-21T10:00:00Z\",\"host\":\"ws-$i\",\"target\":\"srv-$i\"}" > /dev/null 2>&1; done
deadline=$((SECONDS+10)); while [ "$SECONDS" -lt "$deadline" ]; do [ "$(processed detect:lateral)" = "$M" ] && break; sleep 0.2; done
# matching starts one minute later in event time, plus five three minutes later (must not match)
for i in $(seq 1 $M); do "$NATS" -s "$URL" pub vxt.sysmon.proc "{\"event_type\":\"ServiceStart\",\"@timestamp\":\"2026-09-21T10:01:00Z\",\"host\":\"srv-$i\",\"image\":\"psexesvc.exe\"}" > /dev/null 2>&1; done
for i in $(seq 1 5); do "$NATS" -s "$URL" pub vxt.sysmon.net "{\"event_type\":\"SmbConnect\",\"@timestamp\":\"2026-09-21T11:00:00Z\",\"host\":\"late-$i\",\"target\":\"lsrv-$i\"}" > /dev/null 2>&1; done
sleep 0.5
for i in $(seq 1 5); do "$NATS" -s "$URL" pub vxt.sysmon.proc "{\"event_type\":\"ServiceStart\",\"@timestamp\":\"2026-09-21T11:03:00Z\",\"host\":\"lsrv-$i\",\"image\":\"psexesvc.exe\"}" > /dev/null 2>&1; done
deadline=$((SECONDS+10)); got=0
while [ "$SECONDS" -lt "$deadline" ]; do got=$(distinct_ids "$WORK/lateral.txt" to); [ "$got" -ge "$M" ] && break; sleep 0.3; done
sleep 1.5; got=$(distinct_ids "$WORK/lateral.txt" to)
kill "$SUB_PID" 2>/dev/null; pkill -f "[s]ub vxt.lateral" 2>/dev/null
[ "$got" -eq "$M" ] && ok "$M pairs within 2m matched once each; 5 pairs 3m apart did not" || bad "expected $M lateral alerts, got $got (see $WORK/rt.log)"

if [ "$fail" -eq 0 ]; then echo "detect: all invariants hold ✓"; else echo "detect: FAILED"; tail -30 "$WORK/rt.log"; fi
exit $fail
