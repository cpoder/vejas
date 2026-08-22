#!/bin/bash
# The comparison leg: same loadgen, same sink, same mapping work — on
# Redpanda Connect (ex-Benthos). The binary is NOT vendored (323 MB);
# point RPCONNECT at it.
#   RPCONNECT=/path/to/redpanda-connect bench/compare/run-benthos.sh [seconds] [concurrency]
set -euo pipefail
cd "$(dirname "$0")/../.."

SECS="${1:-30}"
CONC="${2:-32}"
RP="${RPCONNECT:?point RPCONNECT at the redpanda-connect binary}"
trap 'kill $RP_PID $SINK_PID 2>/dev/null || true' EXIT

node bench/sink-counter.mjs > /dev/null 2>&1 &
SINK_PID=$!

T0=$(date +%s%N)
"$RP" run bench/compare/benthos.yaml > /tmp/benthos-bench.log 2>&1 &
RP_PID=$!
until curl -sf -o /dev/null -X POST -d '{"warmup":1,"id":"SO#0","email":"a@b.c","total_price":"1","t":0,"shipping_address":{"country":"France"},"line_items":[]}' http://127.0.0.1:8791/ingest; do sleep 0.05; done
COLD_MS=$(( ($(date +%s%N) - T0) / 1000000 ))

node bench/loadgen.mjs 10 "$CONC" http://127.0.0.1:8791/ingest > /dev/null   # warmup
sleep 2
curl -sf http://127.0.0.1:9099/reset > /dev/null

LOAD=$(node bench/loadgen.mjs "$SECS" "$CONC" http://127.0.0.1:8791/ingest)
sleep 5
STATS=$(curl -sf http://127.0.0.1:9099/stats)
RSS_KB=$(ps -o rss= -p $RP_PID | tr -d ' ')
BIN_BYTES=$(stat -c %s "$RP")

python3 - "$LOAD" "$STATS" "$COLD_MS" "$RSS_KB" "$BIN_BYTES" "$SECS" "$CONC" << 'PY'
import json, sys
load, stats = json.loads(sys.argv[1]), json.loads(sys.argv[2])
print(json.dumps({
  "engine": "redpanda-connect (in-flight only, no broker persistence)",
  "seconds": int(sys.argv[6]), "concurrency": int(sys.argv[7]),
  "cold_start_ms": int(sys.argv[3]),
  "ingest_rate_per_s": load["ingest_rate"],
  "delivered_rate_per_s": round(stats["delivered"] / max(stats["elapsed_s"], 1e-9)),
  "latency_ms": stats["latency_ms"],
  "rss_mb": round(int(sys.argv[4]) / 1024, 1),
  "binary_mb": round(int(sys.argv[5]) / 1e6, 1),
}, indent=2))
PY
