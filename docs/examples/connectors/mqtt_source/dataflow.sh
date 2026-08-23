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
