#!/bin/bash
# The reproducible benchmark: webhook -> flow (lookup, conversions, a
# projection loop) -> sink, end to end, on a dedicated NATS. Prints a JSON
# report: cold start, sustained throughput, end-to-end latency percentiles,
# runtime RSS, binary size. Methodology: bench/README.md.
#   bench/run.sh [seconds] [concurrency]
set -euo pipefail
cd "$(dirname "$0")/.."

SECS="${1:-30}"
CONC="${2:-32}"
BIN="core/target/release/vejas-runtime"
NATS_PORT=4223
STORE=$(mktemp -d)
trap 'kill $NATS_PID $RUNTIME_PID $SINK_PID 2>/dev/null || true; rm -rf "$STORE"' EXIT

[ -x "$BIN" ] || { echo "build first: cargo build --release --manifest-path core/Cargo.toml" >&2; exit 1; }

# dedicated NATS (own port, throwaway store — never your dev instance)
nats-server -js -sd "$STORE" -a 127.0.0.1 -p $NATS_PORT > /dev/null 2>&1 &
NATS_PID=$!
sleep 0.5

# counting sink
node bench/sink-counter.mjs > /dev/null 2>&1 &
SINK_PID=$!

# cold start: spawn -> healthz 200
T0=$(date +%s%N)
NATS_URL="nats://127.0.0.1:$NATS_PORT" VEJAS_ROOT=bench/root \
  VEJAS_HTTP_ADDR=127.0.0.1:8689 "$BIN" > "$STORE/runtime.log" 2>&1 &
RUNTIME_PID=$!
until curl -sf -o /dev/null http://127.0.0.1:8689/healthz; do sleep 0.05; done
COLD_MS=$(( ($(date +%s%N) - T0) / 1000000 ))

# wait for the webhook entry + flow consumer to be up
until curl -sf -o /dev/null -X POST -d '{"warmup":1}' http://127.0.0.1:8790/ingest/bench.warmup; do sleep 0.2; done
sleep 2

# warmup (10s), then reset the sink and measure
node bench/loadgen.mjs 10 "$CONC" > /dev/null
sleep 2
curl -sf http://127.0.0.1:9099/reset > /dev/null

LOAD=$(node bench/loadgen.mjs "$SECS" "$CONC")
# drain: let in-flight messages reach the sink
sleep 5
STATS=$(curl -sf http://127.0.0.1:9099/stats)
RSS_KB=$(ps -o rss= -p $RUNTIME_PID | tr -d ' ')
BIN_BYTES=$(stat -c %s "$BIN")

python3 - "$LOAD" "$STATS" "$COLD_MS" "$RSS_KB" "$BIN_BYTES" "$SECS" "$CONC" << 'PY'
import json, sys
load, stats = json.loads(sys.argv[1]), json.loads(sys.argv[2])
out = {
  "scenario": "webhook -> flow (lookup + conversions + projection loop) -> HTTP sink",
  "seconds": int(sys.argv[6]), "concurrency": int(sys.argv[7]),
  "cold_start_ms": int(sys.argv[3]),
  "ingest_rate_per_s": load["ingest_rate"],
  "delivered": stats["delivered"],
  "delivered_rate_per_s": round(stats["delivered"] / max(stats["elapsed_s"], 1e-9)),
  "latency_ms": stats["latency_ms"],
  "runtime_rss_mb": round(int(sys.argv[4]) / 1024, 1),
  "binary_mb": round(int(sys.argv[5]) / 1e6, 1),
  "loadgen": load,
}
print(json.dumps(out, indent=2))
PY
