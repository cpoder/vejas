#!/bin/bash
# Real-broker data flow (bus -> broker leg): publish the fixture on the bus,
# require it QUEUED in RabbitMQ (rabbitmqctl counts it — no consumer eats
# it, so the count is the proof the publish routed and was confirmed).
# The queue is declared by a brief pass of the source binary (the sink
# publishes to the default exchange and declares nothing — routing to a
# missing queue would be silently unroutable). Staged env: NATS_P, BROKER_P,
# DIR.
set -euo pipefail
BIN=connectors/amqp/target/release/vejas-amqp
[ -x "$BIN" ] || { echo "build first: cargo build --release --manifest-path connectors/amqp/Cargo.toml"; exit 1; }
URL="amqp://guest:guest@127.0.0.1:$BROKER_P"
NURL="nats://127.0.0.1:$NATS_P"

nats -s "$NURL" stream add VEJAS --subjects 'vx.>' --defaults > /dev/null

# declare the durable queue: run the source for a moment, then stop it
VEJAS_AMQP_URL="$URL" VEJAS_AMQP_MODE=source VEJAS_AMQP_QUEUE=cert.out \
  VEJAS_AMQP_SUBJECT=vx.amqpcert.tmp VEJAS_AMQP_COMPETING=1 NATS_URL="$NURL" \
  "$BIN" > /dev/null 2>&1 &
DECL=$!
sleep 2; kill $DECL 2>/dev/null; wait $DECL 2>/dev/null || true

VEJAS_AMQP_URL="$URL" VEJAS_AMQP_MODE=sink VEJAS_AMQP_QUEUE=cert.out \
  VEJAS_AMQP_ROUTING_KEY=cert.out VEJAS_AMQP_SUBJECT=vx.amqpcert.out \
  VEJAS_AMQP_DURABLE=amqpcert_sink NATS_URL="$NURL" \
  "$BIN" > /tmp/amqp-snk2-$BROKER_P.log 2>&1 &
SNK=$!
trap 'kill $SNK 2>/dev/null || true' EXIT
sleep 2

nats -s "$NURL" pub vx.amqpcert.out "$(cat "$DIR/fixture.json" | tr -d '\n')" > /dev/null
for _ in $(seq 50); do
  N=$(docker exec "rmq-$BROKER_P" rabbitmqctl list_queues -q name messages 2>/dev/null \
      | awk '$1 == "cert.out" {print $2}')
  [ "${N:-0}" -ge 1 ] && { echo "bus->rabbitmq ok (queued: $N)"; exit 0; }
  sleep 0.3
done
echo "fixture never queued in rabbitmq"; exit 1
