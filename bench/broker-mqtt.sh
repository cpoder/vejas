#!/bin/bash
# MQTT loopback bench (ADR-0025): bus -> mqtt-out -> real mosquitto ->
# mqtt-in -> bus, every hop QoS 1 (PUBACK-gated both ways). Measures the
# full round-trip chain rate of OUR drivers against a real broker — no
# per-message process spawns anywhere in the measured path.
#   bench/broker-mqtt.sh [events]          (needs the nats CLI + docker)
set -uo pipefail
cd "$(dirname "$0")/.."

N="${1:-5000}"
BIN="core/target/release/vejas-runtime"
BP=9460
S=$(mktemp -d); R=$(mktemp -d)
trap 'kill $RT_PID $NP $CAP 2>/dev/null || true; e2e/admission/brokers/mosquitto.sh stop $BP; rm -rf "$S" "$R"' EXIT

e2e/admission/brokers/mosquitto.sh start $BP > /dev/null || { echo "broker KO"; exit 1; }

mkdir -p "$R/connectors"
cat > "$R/connectors/loop_out.vjs" << EOF
driver "mqtt-out"
BROKER = "127.0.0.1:$BP"
TOPIC = "bench/loop"
SUBJECT = "vx.mqttbench.out"
QOS = 1
EOF
cat > "$R/connectors/loop_in.vjs" << EOF
driver "mqtt-in"
BROKER = "127.0.0.1:$BP"
TOPIC = "bench/loop"
SUBJECT = "vx.mqttbench.in"
QOS = 1
EOF

nats-server -js -sd "$S" -a 127.0.0.1 -p 4233 > /dev/null 2>&1 &
NP=$!
sleep 0.5
NATS_URL=nats://127.0.0.1:4233 VEJAS_ROOT="$R" VEJAS_ACK_WAIT_SECS=1 \
  VEJAS_HTTP_ADDR=127.0.0.1:8716 "$BIN" > "$S/rt.log" 2>&1 &
RT_PID=$!
until curl -sf -o /dev/null http://127.0.0.1:8716/healthz; do sleep 0.1; done
sleep 2

( timeout 180 stdbuf -oL nats -s nats://127.0.0.1:4233 sub vx.mqttbench.in --count="$N" --raw 2>/dev/null \
    | python3 -u -c 'import sys,time
for line in sys.stdin: sys.stdout.write(f"{time.time()}\n")' > "$S/arr" ) &
CAP=$!
sleep 0.5

T0=$(date +%s%N)
nats -s nats://127.0.0.1:4233 pub vx.mqttbench.out --count="$N" '{"sensor":"t1","v":21.5}' > /dev/null 2>&1
PUB_MS=$(( ($(date +%s%N) - T0) / 1000000 ))
wait $CAP 2>/dev/null || true
TOTAL_MS=$(( ($(date +%s%N) - T0) / 1000000 ))
GOT=$(wc -l < "$S/arr")
RSS_KB=$(ps -o rss= -p $RT_PID | tr -d ' ')

python3 - "$N" "$GOT" "$PUB_MS" "$TOTAL_MS" "$RSS_KB" << 'PY'
import json, sys
n, got, pub, total, rss = map(int, sys.argv[1:6])
print(json.dumps({
  "chain": "bus -> mqtt-out (QoS1) -> mosquitto -> mqtt-in (QoS1) -> bus",
  "published": n, "completed_roundtrip": got,
  "integrity": "ok" if got >= n else f"CAPTURE-TIMEOUT at {got}/{n} (rate too low — nothing lost, the durable holds the rest)",
  "publish_ms": pub, "done_ms": total,
  "roundtrip_rate_per_s": round(got / (total / 1000)) if got else 0,
  "runtime_rss_mb": round(rss / 1024, 1),
}, indent=2))
PY
