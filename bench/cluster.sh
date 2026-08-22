#!/bin/bash
# Multi-instance behavior probe + bench: N runtimes on ONE NATS, same root.
# Answers, with numbers, the questions the clustering ADR needs:
#   Q1 — do flows load-balance across instances (shared durables), and is
#        the total exactly-once-delivered (no dupes, no loss)?
#   Q2 — do singleton sources (timer) duplicate their work? (expected today: yes)
#   Q3 — kill -9 one instance under load: does the fleet still deliver
#        everything, and how fast does work rebalance?
#   bench/cluster.sh [instances] [events]     (needs the nats CLI)
set -euo pipefail
cd "$(dirname "$0")/.."

INST="${1:-2}"
N="${2:-20000}"
BIN="core/target/release/vejas-runtime"
NATS_PORT=4228
STORE=$(mktemp -d); ROOT=$(mktemp -d)
declare -a RT_PIDS
trap 'kill $NATS_PID ${RT_PIDS[@]:-} $SUB_PID $TSUB_PID 2>/dev/null || true; rm -rf "$STORE" "$ROOT"' EXIT

mkdir -p "$ROOT/flows" "$ROOT/connectors"
cp bench/root/flows/bench_orders.vjs "$ROOT/flows/"
cat > "$ROOT/connectors/tick.vjs" << 'EOF'
# connector: tick — the singleton-duplication probe
driver "timer"
SUBJECT = "vx.bench.tick"
INTERVAL_SECS = 1
PAYLOAD = {"probe": "tick"}
EOF

nats-server -js -sd "$STORE" -a 127.0.0.1 -p $NATS_PORT > /dev/null 2>&1 &
NATS_PID=$!
sleep 0.5

for i in $(seq "$INST"); do
  NATS_URL="nats://127.0.0.1:$NATS_PORT" VEJAS_ROOT="$ROOT" \
    VEJAS_HTTP_ADDR="127.0.0.1:$((8692 + i))" "$BIN" > "$STORE/rt$i.log" 2>&1 &
  RT_PIDS[$i]=$!
done
for i in $(seq "$INST"); do
  until curl -sf -o /dev/null "http://127.0.0.1:$((8692 + i))/healthz"; do sleep 0.1; done
done
sleep 2

# ── Q2 first, all instances alive: singleton duplication over 8s ─────────
TICKS_FILE="$STORE/ticks"
timeout 8 nats -s nats://127.0.0.1:$NATS_PORT sub vx.bench.tick --raw 2>/dev/null | wc -l > "$TICKS_FILE" || true
TSUB_PID=""

# ── Q1+Q3: publish N, kill instance 1 mid-stream, count deliveries ───────
EVENT=$(tr -d '\n' < bench/root/flows/fixtures/bench_orders.json)
OUT_FILE="$STORE/out"
( timeout 90 nats -s nats://127.0.0.1:$NATS_PORT sub vx.bench.out --count="$N" --raw 2>/dev/null | wc -l > "$OUT_FILE" ) &
SUB_PID=$!
sleep 0.5

T0=$(date +%s%N)
PUB_PIDS=()
for _ in 1 2 3 4; do
  nats -s nats://127.0.0.1:$NATS_PORT pub vx.bench.orders --count=$((N / 4)) "$EVENT" > /dev/null 2>&1 &
  PUB_PIDS+=($!)
done
# kill -9 the first instance while the stream is in flight
( sleep 1.5; kill -9 "${RT_PIDS[1]}" 2>/dev/null ) &
wait "${PUB_PIDS[@]}"
wait $SUB_PID 2>/dev/null || true
TOTAL_MS=$(( ($(date +%s%N) - T0) / 1000000 ))

DELIVERED=$(cat "$OUT_FILE" 2>/dev/null || echo 0)
TICKS=$(cat "$TICKS_FILE" 2>/dev/null || echo 0)
# per-instance share of the work, from each runtime's own metrics
SHARES=""
for i in $(seq 2 "$INST"); do
  M=$(curl -sf "http://127.0.0.1:$((8692 + i))/metrics" 2>/dev/null | grep -oP 'vejas_events_processed_total\{unit="flow:bench_orders",result="ok"\} \K[0-9]+' || echo "?")
  SHARES="$SHARES rt$i=$M"
done

python3 - "$INST" "$N" "$DELIVERED" "$TOTAL_MS" "$TICKS" "$SHARES" << 'PY'
import json, sys
inst, n, dlv, ms, ticks = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5])
print(json.dumps({
  "instances": inst, "published": n,
  "q1_delivered": dlv,
  "q1_verdict": "exactly-all" if dlv == n else ("DUPES(+%d)" % (dlv - n) if dlv > n else "LOSS(-%d)" % (n - dlv)),
  "q3_kill9_instance1_at": "1.5s under load",
  "done_ms": ms, "rate_per_s": round(n / (ms / 1000)) if dlv >= n else None,
  "q2_timer_ticks_8s": ticks,
  "q2_expected": {"singleton": "~%d" % 8, "duplicated": "~%d" % (8 * inst)},
  "q2_verdict": "DUPLICATED" if ticks > 8 + 3 else "single",
  "survivor_shares": sys.argv[6].strip(),
}, indent=2))
PY
