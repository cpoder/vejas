#!/bin/bash
# Cluster-wide promotion validation (ADR-0021, increment 1): two clustered
# instances process a steady stream from a probe flow whose emits carry the
# version tag; promote TAG v1 -> v2 mid-stream through the version path,
# then measure:
#   - convergence window: promote time -> last v1-tagged emit anywhere
#   - skew: first v2 emit -> last v1 emit (both versions interleaving)
#   - integrity: outputs v1+v2 == published (nothing lost in the switch)
#   bench/cluster-promote.sh [events] [rate-per-s]      (needs the nats CLI)
set -uo pipefail
cd "$(dirname "$0")/.."

N="${1:-40000}"
BIN="core/target/release/vejas-runtime"
S=$(mktemp -d); R=$(mktemp -d)
trap 'kill $PA $PB $NP $CAP $PUB 2>/dev/null || true; rm -rf "$S" "$R"' EXIT

mkdir -p "$R/flows"
cat > "$R/flows/promote_probe.vjs" << 'EOF'
# flow: promote_probe — every emit carries the version tag
source "vx.bench.p"
TAG = "v1"
emit "vx.bench.pout", {tag: TAG, i: i}
EOF

nats-server -js -sd "$S" -a 127.0.0.1 -p 4231 > /dev/null 2>&1 &
NP=$!
sleep 0.5
start_inst() {
  VEJAS_CLUSTER=1 VEJAS_INSTANCE="$1" VEJAS_ACK_WAIT_SECS=1 \
    NATS_URL=nats://127.0.0.1:4231 VEJAS_ROOT="$R" \
    VEJAS_HTTP_ADDR="127.0.0.1:$2" "$BIN" > "$S/$1.log" 2>&1 & echo $!
}
PA=$(start_inst A 8713); PB=$(start_inst B 8714)
until curl -sf -o /dev/null http://127.0.0.1:8713/healthz \
   && curl -sf -o /dev/null http://127.0.0.1:8714/healthz; do sleep 0.1; done
sleep 2

# capture EVERY output with arrival time + tag (waits for all N)
( timeout 120 stdbuf -oL nats -s nats://127.0.0.1:4231 sub vx.bench.pout --count="$N" --raw 2>/dev/null \
    | python3 -u -c 'import sys,time
for line in sys.stdin:
    sys.stdout.write(f"{time.time()} {line}")' > "$S/out" ) &
CAP=$!
sleep 0.5

# full-speed publisher (one connection, ~2.7k/s) — long enough to straddle the promote
nats -s nats://127.0.0.1:4231 pub vx.bench.p --count="$N" '{"i":0}' > /dev/null 2>&1 &
PUB=$!

sleep 5
# THE promote: v1 -> v2 through the cluster version path (instance A)
T_PROMOTE=$(date +%s.%N)
PCODE=$(curl -s -o "$S/promote.json" -w "%{http_code}" -X POST http://127.0.0.1:8713/surface/set \
  -H 'content-type: application/json' \
  -d '{"file":"flows/promote_probe.vjs","name":"TAG","key":"-","value":"v2"}')
echo "promote → HTTP $PCODE $(head -c 120 "$S/promote.json")"

wait $PUB 2>/dev/null || true
wait $CAP 2>/dev/null || true

python3 - "$S/out" "$T_PROMOTE" "$N" << 'PY'
import json, sys
tp = float(sys.argv[2]); n = int(sys.argv[3])
v1 = []; v2 = []
for line in open(sys.argv[1]):
    try:
        ts, payload = line.split(' ', 1)
        tag = json.loads(payload)['tag']
        (v1 if tag == 'v1' else v2).append(float(ts))
    except Exception:
        pass
out = {
  "published": n, "outputs": len(v1) + len(v2),
  "v1": len(v1), "v2": len(v2),
  "integrity": "ok" if len(v1) + len(v2) >= n else f"LOSS(-{n - len(v1) - len(v2)})",
}
if v2 and v1:
    last_v1 = max(v1); first_v2 = min(v2)
    out["convergence_window_s"] = round(max(0, last_v1 - tp), 2)
    out["first_v2_after_promote_s"] = round(first_v2 - tp, 2)
    out["skew_interleave_s"] = round(max(0, last_v1 - first_v2), 2)
    out["v1_after_convergence"] = 0 if last_v1 <= max(v2) else "CHECK"
elif not v2:
    out["verdict"] = "PROMOTE NEVER TOOK EFFECT"
print(json.dumps(out, indent=2))
PY
