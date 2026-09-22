#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# Vejas — the detect demo: a signature and a behaviour, on the same bus,
# against the same attack, with a crash in the middle.
#
# Four beats, each asserted, so this is a test as much as a film:
#   1  two detect units boot and consume the Sysmon subjects
#   2  a lazy attacker runs PsExec under its own name — both rules fire
#   3  the attacker renames the binary — the signature goes quiet, the
#      behavioural sequence still fires (MITRE T1021.002)
#   4  the runtime is killed with -9 between the two halves of a sequence;
#      after the restart the alert still arrives, out of the unit's snapshot
#
# Runs anywhere nats-server and the runtime binary are on hand; no credentials,
# no shared state, teardown by captured PID.
#
#   e2e/detect-demo/run.sh                  assert everything, quickly
#   PAUSE=manual e2e/detect-demo/run.sh     wait for <enter> between beats (filming)
#   PAUSE=3      e2e/detect-demo/run.sh     three seconds between beats
# ─────────────────────────────────────────────────────────────────────────────
set -uo pipefail
cd "$(dirname "$0")/../.."
HERE="$(cd "$(dirname "$0")" && pwd)"
REL="core/target/release/vejas-runtime"; DBG="core/target/debug/vejas-runtime"
if [ -x "$REL" ] && { [ ! -x "$DBG" ] || [ "$REL" -nt "$DBG" ]; }; then BIN="$REL"; else BIN="$DBG"; fi
BIN="${VEJAS_BIN:-$BIN}"
NATSD="$(command -v nats-server || echo "$HOME/.local/bin/nats-server")"
NATS="$(command -v nats || echo "$HOME/.local/bin/nats")"
[ -x "$BIN" ] || { echo "build first: cargo build --release --manifest-path core/Cargo.toml" >&2; exit 1; }
[ -x "$NATSD" ] || { echo "need nats-server on PATH or ~/.local/bin" >&2; exit 1; }
export LLMFORMAT=1
NATS_P="${NATS_P:-4330}"; HTTP_P="${HTTP_P:-8730}"
URL="nats://127.0.0.1:$NATS_P"
PAUSE="${PAUSE:-0}"
WORK="$(mktemp -d)"
ROOT="$HERE/detect-demo-root"
NATS_PID=""; RT_PID=""
fail=0
BOLD=$'\033[1m'; DIM=$'\033[2m'; RED=$'\033[31m'; GREEN=$'\033[32m'; YELLOW=$'\033[33m'; OFF=$'\033[0m'

ok()   { echo "  ${GREEN}✓${OFF} $1"; }
bad()  { echo "  ${RED}✗${OFF} $1"; fail=1; }
beat() { echo; echo "${BOLD}── $1${OFF}"; }
pause() { case "$PAUSE" in manual) read -r -p "  ${DIM}<enter>${OFF} " _ ;; 0) : ;; *) sleep "$PAUSE" ;; esac; }
cleanup() {
  [ -n "$RT_PID" ] && { kill "$RT_PID" 2>/dev/null; wait "$RT_PID" 2>/dev/null; }
  [ -n "$NATS_PID" ] && { kill "$NATS_PID" 2>/dev/null; wait "$NATS_PID" 2>/dev/null; }
  rm -rf "$WORK"
}
trap cleanup EXIT

start_nats() { "$NATSD" -js -sd "$WORK/js" -a 127.0.0.1 -p "$NATS_P" > /dev/null 2>&1 & NATS_PID=$!; sleep 0.5; }
start_rt() {
  VEJAS_ROOT="$ROOT" NATS_URL="$URL" VEJAS_STREAM=SYSMON VEJAS_SUBJECT_ROOT=vx \
    VEJAS_HTTP_ADDR="127.0.0.1:$HTTP_P" VEJAS_ACK_WAIT_SECS=1 VEJAS_SNAPSHOT_SECS="${SNAP_SECS:-1}" \
    "$BIN" >> "$WORK/rt.log" 2>&1 &
  RT_PID=$!
  until curl -sf -o /dev/null "http://127.0.0.1:$HTTP_P/healthz"; do sleep 0.05; done
  deadline=$((SECONDS+10))
  while [ "$SECONDS" -lt "$deadline" ]; do
    [ "$(curl -s "http://127.0.0.1:$HTTP_P/topology" | grep -o '"status":"running"' | wc -l)" -ge 2 ] && break
    sleep 0.2
  done
}
processed() { curl -s "http://127.0.0.1:$HTTP_P/metrics" | grep "vejas_events_processed_total{unit=\"$1\",result=\"ok\"}" | awk '{print $2}'; }
# publish <file> [first N events] — a Sysmon line goes to the subject its
# channel implies: EventID 3 is a network connect, everything else a process.
publish() {
  local file="$1" limit="${2:-0}" n=0
  while IFS= read -r line; do
    [ -z "$line" ] && continue
    n=$((n+1)); [ "$limit" -gt 0 ] && [ "$n" -gt "$limit" ] && break
    local id; id=$(printf '%s' "$line" | python3 -c 'import json,sys; print(json.load(sys.stdin)["EventID"])')
    local subject="vx.sysmon.process"; [ "$id" = "3" ] && subject="vx.sysmon.network"
    "$NATS" -s "$URL" pub "$subject" "$line" > /dev/null 2>&1
  done < "$file"
  echo "$n"
}
settle() { # wait until both units have processed everything published so far
  local want="$1" deadline=$((SECONDS+15))
  while [ "$SECONDS" -lt "$deadline" ]; do
    local l s; l=$(processed detect:lateral_movement); s=$(processed detect:sigma_psexec)
    [ "$(( ${l:-0} + ${s:-0} ))" -ge "$want" ] && break
    sleep 0.2
  done
  sleep 0.8
}
alerts() { grep -c '^{' "$1" 2>/dev/null || echo 0; }
# at <file> <hour> [host suffix] -> a copy of the scenario moved to that hour.
# The engine judges in EVENT time, so each beat has to happen after the last;
# a scenario replayed with earlier timestamps is simply late, and late events
# cannot open a sequence the watermark has passed.
at() {
  local out="$WORK/$(basename "$1" .jsonl)-$2${3:-}.jsonl"
  python3 - "$1" "$2" "${3:-}" "$out" <<'PY'
import json, re, sys
src, hour, suffix, out = sys.argv[1:5]
rows = [json.loads(l) for l in open(src) if l.strip()]
with open(out, "w") as f:
    for r in rows:
        r["@timestamp"] = re.sub(r"T\d\d:", f"T{hour}:", r["@timestamp"])
        if suffix and "Hostname" in r:
            r["Hostname"] = r["Hostname"] + suffix
        f.write(json.dumps(r) + "\n")
PY
  echo "$out"
}

echo "${BOLD}Vejas — a signature, a behaviour, and a crash${OFF}"
echo "${DIM}Two detect units read the same Sysmon subjects off one JetStream bus.${OFF}"

beat "1. Boot"
start_nats
( timeout 120 "$NATS" -s "$URL" sub vx.alerts.lateral --raw > "$WORK/lateral.txt" 2>/dev/null ) & SUB_L=$!
( timeout 120 "$NATS" -s "$URL" sub vx.alerts.sigma   --raw > "$WORK/sigma.txt"   2>/dev/null ) & SUB_S=$!
sleep 0.4
start_rt
topo=$(curl -s "http://127.0.0.1:$HTTP_P/topology")
if python3 - "$topo" <<'PY'
import json, sys
t = json.loads(sys.argv[1]); d = t.get("detects", [])
names = sorted(x["name"] for x in d)
assert names == ["detect:lateral_movement", "detect:sigma_psexec"], names
assert all(x["lang"] == "vpl" and x["status"] == "running" for x in d), d
PY
then ok "two units running: the Sigma-style rule and the behavioural sequence"
else bad "topology: $topo"; fi
pause

beat "2. A lazy attacker: PsExec, under its own name"
echo "  ${DIM}4 Sysmon events — PsExec spawns, connects to 10.0.0.10:445, cmd.exe appears under services.exe${OFF}"
n=$(publish "$(at "$HERE/data/normal_dataset.jsonl" 10)"); settle "$((n*2))"
sig=$(alerts "$WORK/sigma.txt"); lat=$(alerts "$WORK/lateral.txt")
[ "$sig" -ge 1 ] && ok "the signature fires: $sig alert(s) on vx.alerts.sigma" || bad "the signature should have fired, got $sig"
[ "$lat" -ge 1 ] && ok "the behaviour fires too: $lat alert(s) on vx.alerts.lateral" || bad "the behaviour should have fired, got $lat"
pause

beat "3. The same attack, with the binary renamed to svcupdate.exe"
echo "  ${DIM}Same mechanism, different file name — the only thing a signature looks at${OFF}"
sig_before=$sig; lat_before=$lat
n=$(publish "$(at "$HERE/data/evasion_dataset.jsonl" 12)"); settle "$(( (4+n)*2 ))"
sig=$(alerts "$WORK/sigma.txt"); lat=$(alerts "$WORK/lateral.txt")
if [ "$sig" -eq "$sig_before" ]; then ok "${YELLOW}the signature stays silent${OFF} — nothing is called PsExec any more"
else bad "the signature fired $((sig - sig_before)) time(s) on a renamed binary"; fi
if [ "$lat" -gt "$lat_before" ]; then ok "${GREEN}the behaviour catches it anyway${OFF}: $((lat - lat_before)) alert, T1021.002"
else bad "the behavioural rule missed the renamed attack"; fi
python3 - "$WORK/lateral.txt" <<'PY' || true
import json, sys
rows = [json.loads(l) for l in open(sys.argv[1]) if l.startswith("{")]
a = rows[-1]
print(f"      {a['summary']}")
base = lambda path: path.replace("\\", "/").rsplit("/", 1)[-1]
print(f"      {base(a['source_image'])} → {a['target_ip']}, then {base(a['remote_process'])} under {base(a['remote_parent'])} on {a['remote_host']}")
PY
pause

beat "4. The crash, mid-sequence"
echo "  ${DIM}Publish the SMB connection, kill -9 the runtime, restart it, publish the rest${OFF}"
lat_before=$(alerts "$WORK/lateral.txt")
half="$WORK/half.jsonl"; tail="$WORK/tail.jsonl"
python3 - "$(at "$HERE/data/evasion_dataset.jsonl" 14 -2)" "$half" "$tail" <<'PY'
import json, sys
rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
cut = next(i for i, r in enumerate(rows) if r["EventID"] == 3 and r.get("DestinationPort") == 445) + 1
open(sys.argv[2], "w").write("".join(json.dumps(r) + "\n" for r in rows[:cut]))
open(sys.argv[3], "w").write("".join(json.dumps(r) + "\n" for r in rows[cut:]))
PY
n=$(publish "$half"); sleep 1.6   # past one snapshot cadence
snap=$(curl -s "http://127.0.0.1:$HTTP_P/metrics" | grep 'vejas_snapshot_seq{unit="detect:lateral_movement"}' | awk '{print $2}')
kill -9 "$RT_PID"; wait "$RT_PID" 2>/dev/null; RT_PID=""
echo "  ${DIM}killed at stream sequence ${snap:-?}; the open sequence lives only in the snapshot now${OFF}"
start_rt
grep -q 'restored its snapshot' "$WORK/rt.log" && ok "the unit restored its snapshot and resumed after that sequence" || bad "no snapshot was restored (see $WORK/rt.log)"
n=$(publish "$tail")
deadline=$((SECONDS+15)); while [ "$SECONDS" -lt "$deadline" ]; do [ "$(alerts "$WORK/lateral.txt")" -gt "$lat_before" ] && break; sleep 0.3; done
lat=$(alerts "$WORK/lateral.txt")
[ "$lat" -gt "$lat_before" ] && ok "${GREEN}the alert still arrives${OFF} — the half-finished sequence survived the kill" || bad "the sequence opened before the crash was lost"
kill "$SUB_L" "$SUB_S" 2>/dev/null

echo
if [ "$fail" -eq 0 ]; then echo "${GREEN}${BOLD}detect demo: every beat holds ✓${OFF}"; else echo "${RED}${BOLD}detect demo: FAILED${OFF}"; tail -30 "$WORK/rt.log"; fi
exit $fail
