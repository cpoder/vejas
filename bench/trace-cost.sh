#!/bin/bash
# What tracing costs on the flow hop (ADR-0034, point 6). Runs
# bench/flow-only.sh RUNS times per configuration, interleaved so that drift
# hits every configuration alike, and prints the medians: throughput, and the
# runtime's own CPU time per event (the better measure, since four publishers
# bound the throughput).
#   bench/trace-cost.sh [runs] [events]          (needs the `nats` CLI)
# BIN is the runtime under test (core/target/release/vejas-runtime). BASE_BIN,
# if set, is a runtime from before tracing (v0.6.0): the difference is what
# propagation costs with no exporter. Spans go to bench/otlp-discard.py, which
# only counts them.
set -euo pipefail
cd "$(dirname "$0")/.."
RUNS="${1:-5}"
N="${2:-20000}"
BIN="${BIN:-core/target/release/vejas-runtime}"
BASE_BIN="${BASE_BIN:-}"
COL_P="${COL_P:-4399}"
OUT=$(mktemp)
COUNT=$(mktemp)
TP="00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
EP="http://127.0.0.1:$COL_P"
python3 bench/otlp-discard.py "$COL_P" "$COUNT" &
COL=$!
trap 'kill $COL 2>/dev/null || true; rm -f "$OUT" "$COUNT"' EXIT
sleep 0.5

one() {  # a label, then the environment of that configuration
  local label="$1"; shift
  local json
  json=$(env "$@" bench/flow-only.sh "$N" 2>/dev/null)
  python3 -c 'import json, sys; print(json.dumps({"config": sys.argv[1], "result": json.loads(sys.argv[2])}))' \
    "$label" "$json" >>"$OUT"
  echo "  $label done" >&2
}
for r in $(seq "$RUNS"); do
  echo "run $r/$RUNS" >&2
  if [ -n "$BASE_BIN" ]; then
    one "before tracing"           BIN="$BASE_BIN"
    one "before, caller header"    BIN="$BASE_BIN" BENCH_TRACEPARENT="$TP"
  fi
  one "no exporter"                BIN="$BIN"
  one "no exporter, caller header" BIN="$BIN" BENCH_TRACEPARENT="$TP"
  one "export all"                 BIN="$BIN" OTEL_EXPORTER_OTLP_ENDPOINT="$EP"
  one "export all, caller header"  BIN="$BIN" BENCH_TRACEPARENT="$TP" OTEL_EXPORTER_OTLP_ENDPOINT="$EP"
  one "export 1 in 10"             BIN="$BIN" OTEL_EXPORTER_OTLP_ENDPOINT="$EP" \
                                   OTEL_TRACES_SAMPLER=parentbased_traceidratio OTEL_TRACES_SAMPLER_ARG=0.1
done
python3 - "$OUT" <<'PY'
import json, statistics, sys
by = {}
for line in open(sys.argv[1]):
    row = json.loads(line)
    by.setdefault(row["config"], []).append(row["result"])
ref = "before tracing" if "before tracing" in by else "no exporter"
med = lambda rows, k: statistics.median(r[k] for r in rows)
base_rate = med(by[ref], "flow_rate_per_s")
base_cpu = med(by[ref], "runtime_cpu_us_per_event")
print(f"{'configuration':<28}{'events/s':>9}{'':>9}{'CPU µs/event':>14}{'':>9}{'RSS MB':>8}")
for name, rows in by.items():
    rate, cpu = med(rows, "flow_rate_per_s"), med(rows, "runtime_cpu_us_per_event")
    print(f"{name:<28}{rate:>9.0f}{(rate / base_rate - 1) * 100:>+8.1f}%"
          f"{cpu:>14.1f}{(cpu / base_cpu - 1) * 100:>+8.1f}%{med(rows, 'runtime_rss_mb'):>8}")
print(f"(medians of {len(by[ref])} runs; percentages against '{ref}')")
PY
echo "spans received by the collector: $(cat "$COUNT")"
