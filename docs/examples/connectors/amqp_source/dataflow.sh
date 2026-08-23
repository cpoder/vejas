#!/bin/bash
# Real-broker data flow (broker -> bus leg): inject a message into RabbitMQ
# through our own certified sink binary, and require the fixture's shape on
# the bus, delivered by the source under test. Staged env: NATS_P, BROKER_P,
# DIR. The connector binary must be built (CI builds it before admission).
set -euo pipefail
BIN=connectors/amqp/target/release/vejas-amqp
[ -x "$BIN" ] || { echo "build first: cargo build --release --manifest-path connectors/amqp/Cargo.toml"; exit 1; }
URL="amqp://guest:guest@127.0.0.1:$BROKER_P"
NURL="nats://127.0.0.1:$NATS_P"

nats -s "$NURL" stream add VEJAS --subjects 'vx.>' --defaults > /dev/null

VEJAS_AMQP_URL="$URL" VEJAS_AMQP_MODE=source VEJAS_AMQP_QUEUE=cert.loop \
  VEJAS_AMQP_SUBJECT=vx.amqpcert.in VEJAS_AMQP_COMPETING=1 NATS_URL="$NURL" \
  "$BIN" > /tmp/amqp-src-$BROKER_P.log 2>&1 &
SRC=$!
VEJAS_AMQP_URL="$URL" VEJAS_AMQP_MODE=sink VEJAS_AMQP_QUEUE=cert.loop \
  VEJAS_AMQP_ROUTING_KEY=cert.loop VEJAS_AMQP_SUBJECT=vx.amqpcert.out \
  VEJAS_AMQP_DURABLE=amqpcert_inject NATS_URL="$NURL" \
  "$BIN" > /tmp/amqp-snk-$BROKER_P.log 2>&1 &
SNK=$!
trap 'kill $SRC $SNK 2>/dev/null || true' EXIT
sleep 2

( timeout 20 nats -s "$NURL" sub vx.amqpcert.in --count=1 --raw 2>/dev/null | head -1 > /tmp/amqp-got-$BROKER_P ) &
SUB=$!
sleep 0.5
nats -s "$NURL" pub vx.amqpcert.out "$(cat "$DIR/fixture.json" | tr -d '\n')" > /dev/null
wait $SUB
python3 - "$DIR/fixture.json" "$(cat /tmp/amqp-got-$BROKER_P)" << 'PY'
import json, sys
assert set(json.load(open(sys.argv[1]))) == set(json.loads(sys.argv[2])), "shape mismatch"
PY
echo "rabbitmq->bus ok"
