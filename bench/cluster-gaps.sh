#!/bin/bash
# Singleton availability windows (ADR-0020, increment 1): run a 1s timer on
# two instances, then measure the tick gap through (a) a graceful SIGTERM of
# the leader — the lease-delete handoff — and (b) a kill -9 — the TTL
# failover. Prints every gap > 1.5s with its instant.
#   bench/cluster-gaps.sh [ttl-secs]       (needs the nats CLI)
set -uo pipefail
cd "$(dirname "$0")/.."

TTL="${1:-3}"
BIN="core/target/release/vejas-runtime"
S=$(mktemp -d); R=$(mktemp -d)
trap 'kill $PA $PB $NP $CAP 2>/dev/null || true; rm -rf "$S" "$R"' EXIT

mkdir -p "$R/connectors"
printf 'driver "timer"\nSUBJECT = "vx.bench.tick"\nINTERVAL_SECS = 1\nPAYLOAD = {"p":1}\n' > "$R/connectors/tick.vjs"
nats-server -js -sd "$S" -a 127.0.0.1 -p 4230 > /dev/null 2>&1 &
NP=$!
sleep 0.5

start_inst() {
  VEJAS_CLUSTER=1 VEJAS_INSTANCE="$1" VEJAS_LEASE_TTL_SECS="$TTL" \
    NATS_URL=nats://127.0.0.1:4230 VEJAS_ROOT="$R" \
    VEJAS_HTTP_ADDR="127.0.0.1:$2" "$BIN" > "$S/$1.log" 2>&1 & echo $!
}
PA=$(start_inst A 8710); PB=$(start_inst B 8711)
until curl -sf -o /dev/null http://127.0.0.1:8710/healthz \
   && curl -sf -o /dev/null http://127.0.0.1:8711/healthz; do sleep 0.1; done

( timeout 34 nats -s nats://127.0.0.1:4230 sub vx.bench.tick --raw 2>/dev/null \
    | while read -r _; do date +%s.%N; done > "$S/ticks" ) &
CAP=$!
sleep 8
kill -TERM "$PA"            # graceful: lease released -> instant-ish handoff
sleep 6
PA=$(start_inst A 8710)     # back as standby
sleep 6
kill -9 "$PB"               # crash: failover bounded by the TTL
sleep 12
wait $CAP 2>/dev/null || true

python3 - "$S/ticks" "$TTL" << 'PY'
import json, sys
ts = [float(l) for l in open(sys.argv[1])]
t0 = ts[0]
gaps = [(round(ts[i]-t0,1), round(ts[i]-ts[i-1],2)) for i in range(1,len(ts))]
big = [{"at_s": at, "gap_s": g} for at, g in gaps if g > 1.5]
print(json.dumps({
  "ttl_secs": int(sys.argv[2]), "ticks": len(ts),
  "window_s": round(ts[-1]-t0,1),
  "gaps_over_1.5s": big,
  "expected": {"graceful(SIGTERM at ~8s)": "~ tick + 1s standby retry",
               "crash(kill -9 at ~20s)": "~ TTL + retry + tick"},
}, indent=2))
PY
