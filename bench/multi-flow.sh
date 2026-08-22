#!/bin/bash
# Multi-flow scaling: N copies of the bench flow, each on its own subject,
# events spread across all of them — aggregate throughput and RSS vs N.
#   bench/multi-flow.sh [flows] [events-total]     (needs the nats CLI)
set -euo pipefail
cd "$(dirname "$0")/.."

FLOWS="${1:-10}"
N="${2:-40000}"
PER=$(( N / FLOWS ))
N=$(( PER * FLOWS ))
BIN="core/target/release/vejas-runtime"
NATS_PORT=4227
STORE=$(mktemp -d); ROOT=$(mktemp -d)
trap 'kill $NATS_PID $RUNTIME_PID ${SUB_PIDS[@]:-} 2>/dev/null || true; rm -rf "$STORE" "$ROOT"' EXIT

mkdir -p "$ROOT/flows/fixtures"
for i in $(seq "$FLOWS"); do
  sed -e "s|vx.bench.orders|vx.bench.o$i|" -e "s|\"vx.bench.out\"|\"vx.bench.out$i\"|" \
    bench/root/flows/bench_orders.vjs > "$ROOT/flows/bench_o$i.vjs"
done

nats-server -js -sd "$STORE" -a 127.0.0.1 -p $NATS_PORT > /dev/null 2>&1 &
NATS_PID=$!
sleep 0.5
NATS_URL="nats://127.0.0.1:$NATS_PORT" VEJAS_ROOT="$ROOT" \
  VEJAS_HTTP_ADDR=127.0.0.1:8698 "$BIN" > "$STORE/runtime.log" 2>&1 &
RUNTIME_PID=$!
until curl -sf -o /dev/null http://127.0.0.1:8698/healthz; do sleep 0.1; done
sleep 2

SUB_PIDS=()
for i in $(seq "$FLOWS"); do
  nats -s nats://127.0.0.1:$NATS_PORT sub "vx.bench.out$i" --count="$PER" > /dev/null 2>&1 &
  SUB_PIDS+=($!)
done
sleep 0.5

EVENT=$(tr -d '\n' < bench/root/flows/fixtures/bench_orders.json)
T0=$(date +%s%N)
PUB_PIDS=()
for i in $(seq "$FLOWS"); do
  nats -s nats://127.0.0.1:$NATS_PORT pub "vx.bench.o$i" --count="$PER" "$EVENT" > /dev/null 2>&1 &
  PUB_PIDS+=($!)
done
wait "${PUB_PIDS[@]}"
PUB_MS=$(( ($(date +%s%N) - T0) / 1000000 ))
wait "${SUB_PIDS[@]}"
TOTAL_MS=$(( ($(date +%s%N) - T0) / 1000000 ))
RSS_KB=$(ps -o rss= -p $RUNTIME_PID | tr -d ' ')

python3 - "$FLOWS" "$N" "$PUB_MS" "$TOTAL_MS" "$RSS_KB" << 'PY'
import json, sys
f, n, pub, total, rss = map(int, sys.argv[1:6])
print(json.dumps({
  "flows": f, "events_total": n, "publish_ms": pub, "done_ms": total,
  "aggregate_rate_per_s": round(n / (total / 1000)),
  "runtime_rss_mb": round(rss / 1024, 1),
}, indent=2))
PY
