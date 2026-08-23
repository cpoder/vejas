#!/bin/bash
# Real-broker data flow: publish the fixture INTO mosquitto, expect it on the bus.
set -euo pipefail
( timeout 15 nats -s "nats://127.0.0.1:$NATS_P" sub vx.mqtt.sensors.in --count=1 --raw 2>/dev/null | head -1 > /tmp/mqtt-src-$BROKER_P ) &
SUB=$!
sleep 1
docker exec "mosq-$BROKER_P" mosquitto_pub -p "$BROKER_P" -q 1 -t sensors/temp -m "$(cat "$DIR/fixture.json" | tr -d '\n')"
wait $SUB
python3 - "$DIR/fixture.json" "$(cat /tmp/mqtt-src-$BROKER_P)" << 'PY'
import json, sys
assert set(json.load(open(sys.argv[1]))) == set(json.loads(sys.argv[2])), "shape mismatch"
PY
echo "mqtt->bus ok"

# ── burst: more than one QoS-1 PUBLISH in flight ─────────────────────────
# A sequential single publish never exceeds one in-flight message, which once
# hid a drain-rate bug (the nats-crate 5ms flusher floor) from this very
# check. Fire 50 rapid QoS-1 publishes so the broker's in-flight window
# (mosquitto default: 20) is actually exercised; every one must reach the bus.
( timeout 20 nats -s "nats://127.0.0.1:$NATS_P" sub vx.mqtt.sensors.in --count=50 --raw 2>/dev/null | wc -l > /tmp/mqtt-burst-$BROKER_P ) &
BURST=$!
sleep 1
docker exec "mosq-$BROKER_P" sh -c "for i in \$(seq 50); do mosquitto_pub -p $BROKER_P -q 1 -t sensors/temp -m '{\"sensor\":\"burst\",\"v\":'\$i'}'; done"
wait $BURST
GOT=$(cat /tmp/mqtt-burst-$BROKER_P)
[ "$GOT" -eq 50 ] || { echo "burst: only $GOT/50 reached the bus"; exit 1; }
echo "mqtt->bus burst 50/50 ok"
