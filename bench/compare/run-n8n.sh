#!/bin/bash
# The n8n comparison leg — same loadgen, same sink, same mapping work.
# Runs n8n's official image, single instance, TUNED per their docs for a
# fair fight: execution persistence disabled (success/error/progress),
# diagnostics off. Their horizontal answer (queue mode + worker fleet) is
# out of scope for a single-node table — noted in the README.
#   bench/compare/run-n8n.sh [seconds] [concurrency]   (needs docker + node)
set -euo pipefail
cd "$(dirname "$0")/../.."

SECS="${1:-20}"
CONC="${2:-32}"
VOL=$(mktemp -d) && chmod 777 "$VOL"
trap 'docker rm -f n8n-bench > /dev/null 2>&1 || true; kill $SINK_PID 2>/dev/null || true; rm -rf "$VOL"' EXIT

node bench/sink-counter.mjs > /dev/null 2>&1 &
SINK_PID=$!

docker run --rm -v "$VOL":/home/node/.n8n \
  -v "$PWD/bench/compare/n8n/workflow.json":/wf.json:ro \
  n8nio/n8n import:workflow --input=/wf.json > /dev/null 2>&1
docker run --rm -v "$VOL":/home/node/.n8n \
  n8nio/n8n update:workflow --id=benchwf001 --active=true > /dev/null 2>&1

T0=$(date +%s%N)
docker run -d --name n8n-bench --network host -v "$VOL":/home/node/.n8n \
  -e N8N_SECURE_COOKIE=false -e N8N_DIAGNOSTICS_ENABLED=false \
  -e EXECUTIONS_DATA_SAVE_ON_SUCCESS=none -e EXECUTIONS_DATA_SAVE_ON_ERROR=none \
  -e EXECUTIONS_DATA_SAVE_ON_PROGRESS=false -e N8N_LOG_LEVEL=warn \
  n8nio/n8n > /dev/null
until curl -sf -o /dev/null http://127.0.0.1:5678/healthz; do sleep 0.2; done
COLD_MS=$(( ($(date +%s%N) - T0) / 1000000 ))
# webhook registration lags healthz — poll until it answers
until curl -s -X POST http://127.0.0.1:5678/webhook/bench -d '{"warmup":1}' \
  -o /dev/null -w '%{http_code}' | grep -q 200; do sleep 0.5; done

node bench/loadgen.mjs 10 "$CONC" http://127.0.0.1:5678/webhook/bench > /dev/null
sleep 3
curl -sf http://127.0.0.1:9099/reset > /dev/null
LOAD=$(node bench/loadgen.mjs "$SECS" "$CONC" http://127.0.0.1:5678/webhook/bench)
sleep 5
STATS=$(curl -sf http://127.0.0.1:9099/stats)
MEM=$(docker stats n8n-bench --no-stream --format '{{.MemUsage}}' | cut -d/ -f1 | tr -d ' ')
IMG=$(docker image inspect n8nio/n8n:latest --format '{{.Size}}')

python3 - "$LOAD" "$STATS" "$COLD_MS" "$MEM" "$IMG" << 'PY'
import json, sys
load, stats = json.loads(sys.argv[1]), json.loads(sys.argv[2])
print(json.dumps({
  "engine": "n8n (official image, single instance, execution persistence off)",
  "cold_start_ms": int(sys.argv[3]),
  "ingest_rate_per_s": load["ingest_rate"],
  "delivered_rate_per_s": round(stats["delivered"] / max(stats["elapsed_s"], 1e-9)),
  "latency_ms": stats["latency_ms"],
  "mem": sys.argv[4],
  "image_mb": round(int(sys.argv[5]) / 1e6),
}, indent=2))
PY
