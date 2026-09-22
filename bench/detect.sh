#!/bin/bash
# Detect-unit throughput, isolated: publish N events straight onto the bus and
# run ONLY detect units — no http-in, no sink connector. The completion signal
# is the units' own counter on /metrics, not the alerts, because a correlation
# emits far fewer events than it reads.
#
#   bench/detect.sh [count] [units]          (needs the `nats` CLI)
#   PROGRAM=orders    the twin of flows/bench_orders.vjs — lookup, conversions,
#                     threshold, one emit per event. Compare with bench/flow-only.sh.
#   PROGRAM=sequence  a two-step correlation in event time: what a flow cannot
#                     do. Every pair closes, so this measures carrying state,
#                     not leaking it.
#   PARTITION=0       strip the sequence's .partition_by — the contrast that
#                     shows what an unpartitioned correlation costs
#   SHAPE=interleaved (sequence, default) both steps as they happen: event time
#                     advances monotonically and every pair matches — a stream
#   SHAPE=backlog     (sequence) every first step, then every second. With event
#                     time advancing this is a LATENESS case, not a correlation
#                     one: the watermark passes the second steps before they
#                     arrive, so most are late and few pairs match. Kept because
#                     that is what a careless replay looks like
#   HOSTS=500         the live key pool — what a correlation carries at once
#   LAG=1             (sequence) correlations open at once: the second step of
#                     a pair arrives LAG pairs after its first
set -euo pipefail
cd "$(dirname "$0")/.."

N="${1:-64000}"
UNITS="${2:-1}"
PROGRAM="${PROGRAM:-orders}"
PARTITION="${PARTITION:-1}"
SHAPE="${SHAPE:-interleaved}"
HOSTS="${HOSTS:-500}"       # the live key pool: a fleet has a finite number of machines
LAG="${LAG:-1}"             # correlations open at once — the number that costs
PUBS="${PUBS:-16}"
BIN="core/target/release/vejas-runtime"
NATS_PORT="${NATS_PORT:-4229}"
HTTP_PORT="${HTTP_PORT:-8689}"
URL="nats://127.0.0.1:$NATS_PORT"
STORE=$(mktemp -d); ROOT=$(mktemp -d)
cleanup() {
  # wait for them, do not merely signal them: a run that leaves its port held
  # makes the NEXT run fail silently, which is how a matrix grows holes.
  [ -n "${RUNTIME_PID:-}" ] && { kill "$RUNTIME_PID" 2>/dev/null; wait "$RUNTIME_PID" 2>/dev/null; }
  [ -n "${NATS_PID:-}" ] && { kill "$NATS_PID" 2>/dev/null; wait "$NATS_PID" 2>/dev/null; }
  rm -rf "$STORE" "$ROOT"
}
trap cleanup EXIT

port_free() { ! (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }
for port in "$NATS_PORT" "$HTTP_PORT"; do
  deadline=$((SECONDS+20))
  until port_free "$port"; do
    [ "$SECONDS" -ge "$deadline" ] && { echo "port $port still held after 20s — a previous run has not let go" >&2; exit 1; }
    sleep 0.5
  done
done

# One unit per slice: each reads its own subject, so the fleet is what a
# partitioned deployment looks like (ADR-0020, partition_by as the slice key).
mkdir -p "$ROOT/detects"
for u in $(seq "$UNITS"); do
  if [ "$PROGRAM" = "sequence" ]; then
    sed -e "s#IN_SUBJECT_PROC#vx.bench.d$u.proc#" \
        -e "s#IN_SUBJECT#vx.bench.d$u.net#" \
        -e "s#OUT_SUBJECT#vx.bench.d$u.out#" \
        bench/root/detects/bench_sequence.vpl > "$ROOT/detects/d$u.vpl"
    [ "$PARTITION" = "0" ] && sed -i '/\.partition_by(Hostname)/d' "$ROOT/detects/d$u.vpl"
  else
    sed -e "s#IN_SUBJECT#vx.bench.d$u.in#" \
        -e "s#OUT_SUBJECT#vx.bench.d$u.out#" \
        bench/root/detects/bench_orders.vpl > "$ROOT/detects/d$u.vpl"
  fi
done

nats-server -js -sd "$STORE" -a 127.0.0.1 -p "$NATS_PORT" > /dev/null 2>&1 &
NATS_PID=$!
sleep 0.5
NATS_URL="$URL" VEJAS_ROOT="$ROOT" VEJAS_SUBJECT_ROOT=vx VEJAS_STREAM=BENCH \
  VEJAS_HTTP_ADDR="127.0.0.1:$HTTP_PORT" "$BIN" > "$STORE/runtime.log" 2>&1 &
RUNTIME_PID=$!
until curl -sf -o /dev/null "http://127.0.0.1:$HTTP_PORT/healthz"; do sleep 0.05; done
# every unit consuming before the clock starts
deadline=$((SECONDS+30))
while [ "$SECONDS" -lt "$deadline" ]; do
  [ "$(curl -s "http://127.0.0.1:$HTTP_PORT/topology" | grep -o '"status":"running"' | wc -l)" -ge "$UNITS" ] && break
  sleep 0.2
done
sleep 2   # durable consumers up

processed() {
  curl -s "http://127.0.0.1:$HTTP_PORT/metrics" \
    | grep -E '^vejas_events_processed_total\{unit="detect:' | grep 'result="ok"' \
    | awk '{s += $2} END {print s + 0}'
}
alerts() {
  curl -s "http://127.0.0.1:$HTTP_PORT/metrics" \
    | grep -E '^vejas_emits_published_total\{unit="detect:' | awk '{s += $2} END {print s + 0}'
}

PER_UNIT=$(( N / UNITS ))


T0=$(date +%s%N)
PIDS=()
# One publisher process per unit: bench/pub.py saturates a socket on its own
# (2.1 M messages/s onto a subject nobody reads), so more processes would only
# measure the box's scheduler instead of the runtime.
for u in $(seq "$UNITS"); do
  if [ "$PROGRAM" = "sequence" ]; then
    shape_flag=""
    [ "$SHAPE" = "interleaved" ] && shape_flag="--interleaved"
    python3 bench/pub.py --url "$URL" --pairs "$(( PER_UNIT / 2 ))" --hosts "$HOSTS" \
      --lag "$LAG" --net "vx.bench.d$u.net" --proc "vx.bench.d$u.proc" $shape_flag > /dev/null &
  else
    python3 bench/pub.py --url "$URL" --orders "$PER_UNIT" --subject "vx.bench.d$u.in" > /dev/null &
  fi
  PIDS+=($!)
done
wait "${PIDS[@]}"
PUB_MS=$(( ($(date +%s%N) - T0) / 1000000 ))

PUBLISHED=$(( PER_UNIT * UNITS ))
deadline=$((SECONDS+180))
while [ "$SECONDS" -lt "$deadline" ]; do
  [ "$(processed)" -ge "$PUBLISHED" ] && break
  sleep 0.1
done
TOTAL_MS=$(( ($(date +%s%N) - T0) / 1000000 ))
DONE=$(processed); ALERTS=$(alerts)
RSS_KB=$(ps -o rss= -p $RUNTIME_PID | tr -d ' ')
THREADS=$(ls /proc/$RUNTIME_PID/task 2>/dev/null | wc -l)

python3 - "$PROGRAM" "$UNITS" "$PUBLISHED" "$DONE" "$ALERTS" "$PUB_MS" "$TOTAL_MS" "$RSS_KB" "$THREADS" <<'PY'
import json, sys
prog = sys.argv[1]
units, published, done, alerts, pub_ms, total_ms, rss, threads = map(int, sys.argv[2:10])
rate = round(done / (total_ms / 1000))
print(json.dumps({
    "scenario": f"bus -> {units} detect unit(s) ({prog}) -> bus, no HTTP anywhere",
    "program": prog,
    "partitioned": __import__("os").environ.get("PARTITION", "1") != "0",
    "shape": __import__("os").environ.get("SHAPE", "interleaved") if prog == "sequence" else None,
    "live_keys": int(__import__("os").environ.get("HOSTS", "500")) if prog == "sequence" else None,
    "open_at_once": int(__import__("os").environ.get("LAG", "1")) if prog == "sequence" else None,
    "units": units,
    "published": published,
    "processed": done,
    "complete": done >= published,
    "alerts": alerts,
    "publish_ms": pub_ms,
    "done_ms": total_ms,
    "rate_per_s": rate,
    "rate_per_s_per_unit": round(rate / units),
    "runtime_rss_mb": round(rss / 1024, 1),
    "threads": threads,
}, indent=2))
PY
