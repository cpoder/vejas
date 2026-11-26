#!/bin/bash
# Two runtimes on the isolated flow hop (bench/flow-only.sh), in alternating
# pairs (A then B, B then A, ...), so that a machine whose speed drifts during
# the series slows both alike. Prints, for the runtime and for each of its
# busy threads, the CPU time per event of each side and the difference within
# each pair, with its median: for a change smaller than what moves from one
# series of bench/trace-cost.sh to the next.
#   A=<runtime> B=<runtime> bench/pairs.sh [pairs] [events]   (needs the `nats` CLI)
# Any other variable reaches both runtimes alike (VEJAS_TRACES=off, ...).
set -euo pipefail
cd "$(dirname "$0")/.."
PAIRS="${1:-8}"
N="${2:-20000}"
: "${A:?A=<path of the first runtime>}" "${B:?B=<path of the second runtime>}"
OUT=$(mktemp -d)
trap 'rm -rf "$OUT"' EXIT
for p in $(seq "$PAIRS"); do
  if [ $((p % 2)) = 1 ]; then order=(A B); else order=(B A); fi
  for side in "${order[@]}"; do
    BIN="${!side}" THREADS_OUT="$OUT/$side-$p.tsv" bench/flow-only.sh "$N" > "$OUT/$side-$p.json" 2>/dev/null
  done
  echo "pair $p/$PAIRS done" >&2
done
python3 - "$OUT" "$PAIRS" "$(getconf CLK_TCK)" <<'PY'
import json, statistics, sys
out, pairs, hz = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
def run(side, p):
    result = json.load(open(f"{out}/{side}-{p}.json"))
    n = result["events"]
    threads = {}
    for line in open(f"{out}/{side}-{p}.tsv"):
        name, user, system = line.rstrip("\n").split("\t")
        threads[name] = threads.get(name, 0) + (int(user) + int(system)) * 1e6 / hz / n
    return result, threads
runs = {(side, p): run(side, p) for side in "AB" for p in range(1, pairs + 1)}
rows = [("runtime", lambda side, p: runs[side, p][0]["runtime_cpu_us_per_event"])]
busy = sorted({t for (_, threads) in runs.values() for t in threads},
              key=lambda t: -statistics.median(runs[s, p][1].get(t, 0) for s in "AB" for p in range(1, pairs + 1)))
for t in busy:
    if statistics.median(runs["A", p][1].get(t, 0) for p in range(1, pairs + 1)) >= 1:
        rows.append((t, lambda side, p, t=t: runs[side, p][1].get(t, 0)))
print(f"{'µs of CPU per event':<20}{'A':>8}{'B':>8}{'B - A, each pair':>{10 + 6 * pairs}}{'median':>9}")
for name, value in rows:
    a = [value("A", p) for p in range(1, pairs + 1)]
    b = [value("B", p) for p in range(1, pairs + 1)]
    diff = [y - x for x, y in zip(a, b)]
    each = "".join(f"{d:>+6.1f}" for d in diff)
    print(f"{name:<20}{statistics.median(a):>8.1f}{statistics.median(b):>8.1f}{each:>{10 + 6 * pairs}}{statistics.median(diff):>+9.1f}")
kept = {side: [runs[side, p][0].get("trace_records_kept", 0) for p in range(1, pairs + 1)] for side in "AB"}
print(f"(medians of {pairs} pairs; hop records kept, A: {min(kept['A'])} to {max(kept['A'])}, B: {min(kept['B'])} to {max(kept['B'])})")
PY
