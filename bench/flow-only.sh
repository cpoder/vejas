#!/bin/bash
# Flow-hop throughput, isolated: publish N events straight onto the bus
# (no http-in), run ONLY the flow (no sink connector), count its emits with a
# plain NATS subscription. This is the interpreter + JetStream number — what
# the end-to-end pipeline can hope for once the I/O ceilings fall.
#   bench/flow-only.sh [count]     (needs the `nats` CLI)
set -euo pipefail
cd "$(dirname "$0")/.."

N="${1:-20000}"
PUBS="${PUBS:-4}"           # parallel publishers — one sequential publisher caps ~2.8k/s
N=$(( (N / PUBS) * PUBS ))  # keep the count exactly divisible
BIN="core/target/release/vejas-runtime"
NATS_PORT=4224
STORE=$(mktemp -d)
ROOT=$(mktemp -d)
trap 'kill $NATS_PID $RUNTIME_PID $SUB_PID 2>/dev/null || true; rm -rf "$STORE" "$ROOT"' EXIT

# a root with only the flow — no connectors, so no curl, no webhook
mkdir -p "$ROOT/flows/fixtures"
cp bench/root/flows/bench_orders.vjs "$ROOT/flows/"
cp bench/root/flows/fixtures/bench_orders.json "$ROOT/flows/fixtures/"

nats-server -js -sd "$STORE" -a 127.0.0.1 -p $NATS_PORT > /dev/null 2>&1 &
NATS_PID=$!
sleep 0.5

NATS_URL="nats://127.0.0.1:$NATS_PORT" VEJAS_ROOT="$ROOT" \
  VEJAS_HTTP_ADDR=127.0.0.1:8688 "$BIN" > "$STORE/runtime.log" 2>&1 &
RUNTIME_PID=$!
until curl -sf -o /dev/null http://127.0.0.1:8688/healthz; do sleep 0.05; done
sleep 2   # durable consumer up

# count the flow's emits on a plain subscription (timing = our own)
nats -s nats://127.0.0.1:$NATS_PORT sub vx.bench.out --count="$N" > /dev/null 2>&1 &
SUB_PID=$!
sleep 0.5

EVENT=$(tr -d '\n' < bench/root/flows/fixtures/bench_orders.json)
T0=$(date +%s%N)
PUB_PIDS=()
for _ in $(seq "$PUBS"); do
  nats -s nats://127.0.0.1:$NATS_PORT pub vx.bench.orders --count=$((N / PUBS)) "$EVENT" > /dev/null 2>&1 &
  PUB_PIDS+=($!)
done
wait "${PUB_PIDS[@]}"
PUB_MS=$(( ($(date +%s%N) - T0) / 1000000 ))

wait $SUB_PID
TOTAL_MS=$(( ($(date +%s%N) - T0) / 1000000 ))
RSS_KB=$(ps -o rss= -p $RUNTIME_PID | tr -d ' ')

python3 - "$N" "$PUB_MS" "$TOTAL_MS" "$RSS_KB" << 'PY'
import json, sys
n, pub_ms, total_ms, rss = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
print(json.dumps({
  "scenario": "bus -> flow (lookup + conversions + projection loop) -> bus, no HTTP anywhere",
  "events": n,
  "publish_ms": pub_ms,
  "done_ms": total_ms,
  "flow_rate_per_s": round(n / (total_ms / 1000)),
  "runtime_rss_mb": round(rss / 1024, 1),
}, indent=2))
PY
