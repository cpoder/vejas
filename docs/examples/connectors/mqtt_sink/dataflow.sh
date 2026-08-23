#!/bin/bash
# Real-broker data flow: publish the fixture on the bus, expect it at mosquitto.
set -euo pipefail
docker exec -d "mosq-$BROKER_P" sh -c "mosquitto_sub -p $BROKER_P -q 1 -t alerts/out -C 1 > /tmp/got 2>/dev/null"
sleep 1
nats -s "nats://127.0.0.1:$NATS_P" pub vx.mqtt.alerts.out "$(cat "$DIR/fixture.json" | tr -d '\n')" > /dev/null 2>&1
GOT=""
for _ in $(seq 50); do
  GOT=$(docker exec "mosq-$BROKER_P" cat /tmp/got 2>/dev/null || true)
  [ -n "$GOT" ] && break
  sleep 0.2
done
python3 - "$DIR/fixture.json" "$GOT" << 'PY'
import json, sys
assert sys.argv[2], "nothing arrived at the broker"
assert set(json.load(open(sys.argv[1]))) == set(json.loads(sys.argv[2])), "shape mismatch"
PY
echo "bus->mqtt ok"
