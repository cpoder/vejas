#!/bin/bash
# The offset-resume invariant, in CI (ADR-0022, hardened after the cadence
# change 11bb759): exec-stream-source + OFFSET_KV must survive a kill -9 of
# the RUNTIME mid-stream with ZERO GAP — every offset reaches the bus — and
# duplicates bounded by the commit cadence (offsets published after the last
# cadenced commit re-stream on restart; at-least-once tolerates them, loss
# would not be tolerated). This is the mechanism the Kafka recipes ride
# (their EXCEPTION.md points here); the child is a fake — the invariant is
# OURS, not the broker's.
#   e2e/offset-resume.sh          (needs the nats CLI)
set -uo pipefail
cd "$(dirname "$0")/.."

BIN="core/target/release/vejas-runtime"
[ -x "$BIN" ] || { echo "build first" >&2; exit 1; }
S=$(mktemp -d); R=$(mktemp -d)
trap 'kill $RT $NP $CAP 2>/dev/null || true; rm -rf "$S" "$R"' EXIT

# fake stream child: one record every 10ms from $OFFSET, forever (SIGPIPE on
# a dead stdout kills it, so a kill -9'd runtime does not leak orphans)
cat > "$S/child.sh" << 'CHILD'
#!/bin/bash
i="${OFFSET:-0}"; [ "$i" = "beginning" ] && i=0
while :; do printf '{"offset":%d}\n' "$i" || exit 0; i=$((i+1)); sleep 0.01; done
CHILD
chmod +x "$S/child.sh"

mkdir -p "$R/connectors"
cat > "$R/connectors/resume_probe.vjs" << VJS
driver "exec-stream-source"
CMD = "$S/child.sh"
SUBJECT = "vx.resume.in"
OFFSET_KV = "resume_probe"
OFFSET_START = "beginning"
VJS

nats-server -js -sd "$S/nats" -a 127.0.0.1 -p 4261 > /dev/null 2>&1 &
NP=$!
sleep 0.5
# capture every arrival's offset for the WHOLE test (both runtime lives)
( timeout 90 stdbuf -oL nats -s nats://127.0.0.1:4261 sub vx.resume.in --raw 2>/dev/null \
    | grep --line-buffered -o '"offset":[0-9]*' > "$S/arr" ) &
CAP=$!
sleep 0.5

start_rt() {
  NATS_URL=nats://127.0.0.1:4261 VEJAS_ROOT="$R" \
    VEJAS_HTTP_ADDR=127.0.0.1:8731 "$BIN" >> "$S/rt.log" 2>&1 & echo $!
}
RT=$(start_rt)
# let it stream past a couple of commit ticks, then CRASH it (no final flush)
until [ "$(wc -l < "$S/arr")" -ge 150 ]; do sleep 0.2; done
kill -9 "$RT"
sleep 1
RT=$(start_rt)
grep -q "resuming at OFFSET=" "$S/rt.log" || true  # asserted below, once settled
until [ "$(sort -t: -k2 -n -u "$S/arr" | tail -1 | cut -d: -f2)" -ge 400 ]; do sleep 0.2; done
kill "$RT" 2>/dev/null

python3 - "$S/arr" "$S/rt.log" << 'PY'
import re, sys
offs = [int(l.split(':')[1]) for l in open(sys.argv[1])]
log = open(sys.argv[2]).read()
top = max(offs)
missing = sorted(set(range(top + 1)) - set(offs))
dups = len(offs) - len(set(offs))
# run 1 logs "resuming at OFFSET=beginning" (non-numeric); the restart is
# the numeric one — it must exist and be a real committed offset (> 0)
resumes = re.findall(r'resuming at OFFSET=(\d+)', log)
assert missing == [], f"GAP: missing offsets {missing[:10]}"
assert 'resuming at OFFSET=beginning' in log, "first run did not start at the beginning"
assert resumes and int(resumes[-1]) > 0, f"no real resume: {resumes}"
# duplicates = the uncommitted window at the crash (cadence 100ms @ ~100/s
# ≈ 10 records) + in-flight margin; bound it loosely but meaningfully
assert dups <= 60, f"duplicate window too wide: {dups}"
print(f"offset-resume ok: 0..{top} complete, resume at {resumes[-1]}, "
      f"{dups} duplicate(s) within the cadence window")
PY
