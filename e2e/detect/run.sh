#!/bin/bash
# Detect-unit invariants (ADR-0031) — a VPL program under detects/, run by the
# embedded Varpulis engine, on a throwaway nats(-js)+runtime on disjoint ports.
#
#   D1 no-loss        a stateless detection (threshold) survives kill -9 mid-stream:
#                     every alert still arrives after restart (at-least-once,
#                     publish-before-ack, redelivery)
#   D2 sequence       a two-source SASE sequence with .within(2m) fires once per
#                     matching pair, judged in EVENT time: a start 3 min later in
#                     event time does not match, however soon it arrives — both
#                     when the events are consumed as they are published and
#                     when they reach the unit as one backlog (runtime paused
#                     during publication: a restart with a queue behind it)
#   D5 snapshot       a stateful sequence survives kill -9: the open sequences
#                     come back from the unit's snapshot (round A), and what was
#                     acked after the last snapshot is replayed from the bus
#                     (round B) — resume by sequence, ADR-0031 points 4-5
#   D6 count close    a count per address closes on the next event of its source
#                     past the window, from any address: a brute force whose
#                     attacker got in and stopped is still raised
#   D7 quiet source   a count on a source that goes silent still closes: past
#                     the idle grace (VEJAS_IDLE_CLOSE_SECS, 1 s here) the
#                     source's event time moves on with the wall clock
#   D8 absence        "no acknowledgement within 5s" is raised on subjects that
#                     go silent after the order, and not for an order acked
#   D9 absence, kill  an absence the unit was waiting out when killed -9 is
#                     still raised after the restart (from the snapshot)
#   D3 vpl-check      the engine's verdict on the CLI: ok / refused, with exit codes
#   D4 topology       /topology lists the units under "detects", lang vpl, running
#
# Redelivery latency is set low (VEJAS_ACK_WAIT_SECS=1) so the suite runs in
# seconds. No real credentials, no shared state; teardown by captured PID.
#   e2e/detect/run.sh
set -uo pipefail
cd "$(dirname "$0")/../.."
REL="core/target/release/vejas-runtime"; DBG="core/target/debug/vejas-runtime"
if [ -x "$REL" ] && { [ ! -x "$DBG" ] || [ "$REL" -nt "$DBG" ]; }; then BIN="$REL"; else BIN="$DBG"; fi
BIN="${VEJAS_BIN:-$BIN}"   # VEJAS_BIN=path runs the suite against another build
NATSD="$(command -v nats-server || echo "$HOME/.local/bin/nats-server")"
NATS="$(command -v nats || echo "$HOME/.local/bin/nats")"
[ -x "$BIN" ] || { echo "build first: cargo build --release --manifest-path core/Cargo.toml" >&2; exit 1; }
[ -x "$NATSD" ] || { echo "need nats-server on PATH or ~/.local/bin" >&2; exit 1; }
export LLMFORMAT=1
NATS_P=4320; HTTP_P=8720
URL="nats://127.0.0.1:$NATS_P"
WORK="$(mktemp -d)"
ROOT="$WORK/root"; mkdir -p "$ROOT/detects"
NATS_PID=""; RT_PID=""
fail=0
ok()   { echo "  ✓ $1"; }
bad()  { echo "  ✗ $1"; fail=1; }
cleanup() {
  # wait for the runtime to exit: its ports must be free for whoever runs next
  [ -n "$RT_PID" ] && { kill "$RT_PID" 2>/dev/null; wait "$RT_PID" 2>/dev/null; }
  [ -n "$NATS_PID" ] && { kill "$NATS_PID" 2>/dev/null; wait "$NATS_PID" 2>/dev/null; }
  rm -rf "$WORK"
}
trap cleanup EXIT

cat > "$ROOT/detects/threshold.vpl" <<'VPL'
connector Bus = nats (
    url: "ignored-by-vejas: the bus is NATS_URL"
)

event Order:
    id: int
    total: float

stream Orders = Order
    .from(Bus, topic: "vxt.orders")

stream Big = Order
    .where(total > 100.0)
    .emit(id: id, total: total)
    .to(Bus, topic: "vxt.alerts")
VPL

cat > "$ROOT/detects/lateral.vpl" <<'VPL'
connector Bus = nats (
    url: "ignored-by-vejas: the bus is NATS_URL"
)

event SmbConnect:
    host: str
    target: str

event ServiceStart:
    host: str
    image: str

stream Net = SmbConnect
    .from(Bus, topic: "vxt.sysmon.net")

stream Proc = ServiceStart
    .from(Bus, topic: "vxt.sysmon.proc")

stream LateralMovement = SmbConnect as smb
    -> ServiceStart where host == smb.target as svc
    .within(2m)
    .emit(rule: "lateral_movement", from: smb.host, to: svc.host, image: svc.image)
    .to(Bus, topic: "vxt.lateral")
VPL

cat > "$ROOT/detects/bruteforce.vpl" <<'VPL'
connector Bus = nats (
    url: "ignored-by-vejas: the bus is NATS_URL"
)

event Auth:
    ip: str
    status: str

stream Logons = Auth
    .from(Bus, topic: "vxt.auth")

stream Failed = Auth
    .where(status == "failure")

stream Brute = Failed
    .partition_by(ip)
    .window(5m)
    .aggregate(ip: last(ip), n: count())
    .where(n >= 3)
    .emit(rule: "brute_force", ip: ip, n: n)
    .to(Bus, topic: "vxt.brute")
VPL

cat > "$ROOT/detects/quiet.vpl" <<'VPL'
connector Bus = nats (
    url: "ignored-by-vejas: the bus is NATS_URL"
)

event Vpn:
    ip: str
    status: str

stream Logons = Vpn
    .from(Bus, topic: "vxt.vpn")

stream Failed = Vpn
    .where(status == "failure")

stream Brute = Failed
    .partition_by(ip)
    .window(5s)
    .aggregate(ip: last(ip), n: count())
    .where(n >= 3)
    .emit(rule: "vpn_brute_force", ip: ip, n: n)
    .to(Bus, topic: "vxt.vpnbrute")
VPL

cat > "$ROOT/detects/unacked.vpl" <<'VPL'
connector Bus = nats (
    url: "ignored-by-vejas: the bus is NATS_URL"
)

event Order:
    id: str
event Ack:
    id: str

stream Orders = Order
    .from(Bus, topic: "vxt.shop.order")

stream Acks = Ack
    .from(Bus, topic: "vxt.shop.ack")

pattern Unacked =
    Order as o
    -> NOT Ack where id == o.id
    within 5s
    partition by id

stream Late = Unacked
    .emit(rule: "unacked", order: o.id)
    .to(Bus, topic: "vxt.unacked")
VPL

start_nats() { "$NATSD" -js -sd "$WORK/js" -a 127.0.0.1 -p "$NATS_P" > /dev/null 2>&1 & NATS_PID=$!; sleep 0.5; }
start_rt() {
  VEJAS_ROOT="$ROOT" NATS_URL="$URL" VEJAS_STREAM=TTEST VEJAS_SUBJECT_ROOT=vxt \
    VEJAS_HTTP_ADDR="127.0.0.1:$HTTP_P" VEJAS_ACK_WAIT_SECS=1 VEJAS_SNAPSHOT_SECS="${SNAP_SECS:-1}" \
    VEJAS_IDLE_CLOSE_SECS=1 \
    "$BIN" >> "$WORK/rt.log" 2>&1 &
  RT_PID=$!
  until curl -sf -o /dev/null "http://127.0.0.1:$HTTP_P/healthz"; do sleep 0.05; done
  sleep 1.5
}
processed() { # unit -> events processed ok
  curl -s "http://127.0.0.1:$HTTP_P/metrics" | grep "vejas_events_processed_total{unit=\"$1\",result=\"ok\"}" | awk '{print $2}'
}
distinct_ids() { python3 - "$1" "$2" "${3:-}" <<'PY'
import json, sys
ids=set(); prefix=sys.argv[3]
for line in open(sys.argv[1], errors="replace"):
    line=line.strip()
    if not line.startswith("{"): continue
    try:
        v=json.loads(line)[sys.argv[2]]
        if str(v).startswith(prefix): ids.add(v)
    except Exception: pass
print(len(ids))
PY
}

echo "── D3 vpl-check"
want="vejas-runtime $(grep -m1 '^version' core/Cargo.toml | cut -d'"' -f2)"
# A binary without the flag starts a runtime here: give it nothing to reach.
out=$(NATS_URL=nats://127.0.0.1:1 VEJAS_ROOT="$WORK/none" timeout -k 2 5 "$BIN" --version 2>&1)
[ "$out" = "$want" ] && ok "--version answers and exits: $out" || bad "--version: expected '$want', got: $(printf '%s' "$out" | head -c 200)"
out=$("$BIN" vpl-check "$ROOT/detects/threshold.vpl" 2>&1); [ "$out" = "ok" ] && ok "threshold.vpl: ok" || bad "threshold.vpl: $out"
out=$("$BIN" vpl-check "$ROOT/detects/lateral.vpl" 2>&1); [ "$out" = "ok" ] && ok "lateral.vpl: ok" || bad "lateral.vpl: $out"
printf 'event Order:\n    id: int\n\nstream Bad = Order\n    .filter(id == 1)\n    .emit(x: 1)\n' > "$WORK/bad.vpl"
if "$BIN" vpl-check "$WORK/bad.vpl" > /dev/null 2>&1; then bad "a program the engine refuses was accepted"; else ok "a refused program exits non-zero"; fi

start_nats; start_rt

echo "── D4 topology"
# both units report running within a few seconds (consumer creation is a round trip each)
deadline=$((SECONDS+10))
while [ "$SECONDS" -lt "$deadline" ]; do
  topo=$(curl -s "http://127.0.0.1:$HTTP_P/topology")
  [ "$(printf '%s' "$topo" | grep -o '"status":"running"' | wc -l)" -ge 5 ] && break
  sleep 0.2
done
python3 - "$topo" <<'PY' && ok "five detects listed, lang vpl, running" || bad "topology: $topo"
import json, sys
t=json.loads(sys.argv[1]); d=t.get("detects", [])
names=sorted(x["name"] for x in d)
assert names==["detect:bruteforce","detect:lateral","detect:quiet","detect:threshold","detect:unacked"], names
assert all(x["lang"]=="vpl" for x in d), d
assert all(x["status"]=="running" for x in d), [x["status"] for x in d]
PY

echo "── D1 no-loss under kill -9 + restart (stateless threshold)"
N=300
( timeout 40 "$NATS" -s "$URL" sub vxt.alerts --raw > "$WORK/alerts.txt" 2>/dev/null ) & SUB_PID=$!
sleep 0.5
for i in $(seq 1 $((N/2))); do "$NATS" -s "$URL" pub vxt.orders "{\"event_type\":\"Order\",\"id\":$i,\"total\":150.5}" > /dev/null 2>&1; done
( for i in $(seq $((N/2+1)) $N); do "$NATS" -s "$URL" pub vxt.orders "{\"event_type\":\"Order\",\"id\":$i,\"total\":150.5}" > /dev/null 2>&1; done ) & P2=$!
sleep 0.4
kill -9 "$RT_PID"; wait "$RT_PID" 2>/dev/null; RT_PID=""
wait "$P2"
start_rt   # the durables resume; un-acked messages redeliver after ack_wait (1 s)
deadline=$((SECONDS+20)); got=0
while [ "$SECONDS" -lt "$deadline" ]; do got=$(distinct_ids "$WORK/alerts.txt" id); [ "$got" -ge "$N" ] && break; sleep 0.5; done
kill "$SUB_PID" 2>/dev/null; pkill -f "[s]ub vxt.alerts" 2>/dev/null
[ "$got" -eq "$N" ] && ok "all $N alerts arrived after the crash (distinct ids)" || bad "expected $N distinct alerts, got $got (see $WORK/rt.log)"

echo "── D2 two-source sequence, judged in event time"
( timeout 60 "$NATS" -s "$URL" sub vxt.lateral --raw > "$WORK/lateral.txt" 2>/dev/null ) & SUB_PID=$!
sleep 0.5
M=20
# lateral_round <prefix> <backlog 0|1> <hour>: M SmbConnect at H:00, then M
# matching ServiceStart at H:01 (must match) plus 5 pairs three minutes apart,
# an hour later (must not). Event time moves forward from one round to the
# next, as on a real bus: the engine judges in event time, and a round dated
# before the previous round's late events would itself be late. With backlog=1
# the runtime is paused while the second step is published, so the second
# steps and the late first steps reach it as one backlog: the engine must
# still see them in the stream's order, not source by source (one consumer
# per subject broke this: 0 or 1 of 20).
lateral_round() {
  local pfx="$1" backlog="$2" h="$3" before now i t0 t1 l0 l3
  t0="2026-09-21T$(printf '%02d' "$h"):00:00Z"; t1="2026-09-21T$(printf '%02d' "$h"):01:00Z"
  l0="2026-09-21T$(printf '%02d' $((h+1))):00:00Z"; l3="2026-09-21T$(printf '%02d' $((h+1))):03:00Z"
  before=$(processed detect:lateral); before=${before:-0}
  for i in $(seq 1 $M); do "$NATS" -s "$URL" pub vxt.sysmon.net "{\"event_type\":\"SmbConnect\",\"@timestamp\":\"$t0\",\"host\":\"$pfx-ws-$i\",\"target\":\"$pfx-srv-$i\"}" > /dev/null 2>&1; done
  deadline=$((SECONDS+10)); while [ "$SECONDS" -lt "$deadline" ]; do now=$(processed detect:lateral); [ $(( ${now:-0} - before )) -ge "$M" ] && break; sleep 0.2; done
  [ "$backlog" = 1 ] && kill -STOP "$RT_PID"
  for i in $(seq 1 $M); do "$NATS" -s "$URL" pub vxt.sysmon.proc "{\"event_type\":\"ServiceStart\",\"@timestamp\":\"$t1\",\"host\":\"$pfx-srv-$i\",\"image\":\"psexesvc.exe\"}" > /dev/null 2>&1; done
  for i in $(seq 1 5); do "$NATS" -s "$URL" pub vxt.sysmon.net "{\"event_type\":\"SmbConnect\",\"@timestamp\":\"$l0\",\"host\":\"$pfx-late-$i\",\"target\":\"$pfx-lsrv-$i\"}" > /dev/null 2>&1; done
  [ "$backlog" = 1 ] || sleep 0.5
  for i in $(seq 1 5); do "$NATS" -s "$URL" pub vxt.sysmon.proc "{\"event_type\":\"ServiceStart\",\"@timestamp\":\"$l3\",\"host\":\"$pfx-lsrv-$i\",\"image\":\"psexesvc.exe\"}" > /dev/null 2>&1; done
  [ "$backlog" = 1 ] && kill -CONT "$RT_PID"
  deadline=$((SECONDS+10)); got=0
  while [ "$SECONDS" -lt "$deadline" ]; do got=$(distinct_ids "$WORK/lateral.txt" to "$pfx-"); [ "$got" -ge "$M" ] && break; sleep 0.3; done
  sleep 1.5; got=$(distinct_ids "$WORK/lateral.txt" to "$pfx-")
}
diag_lateral() {
  echo "    received $(grep -c '^{' "$WORK/lateral.txt") alert lines; first 3:"; head -3 "$WORK/lateral.txt" | sed 's/^/      /'
  echo "    unit metrics:"; curl -s "http://127.0.0.1:$HTTP_P/metrics" | grep -E '^vejas_(events_processed|emits_published|fetch_rounds|dead_letter)[a-z_]*\{unit="detect:lateral"' | sed 's/^/      /'
  "$NATS" -s "$URL" consumer info TTEST detect_lateral 2>/dev/null | grep -E 'Filter|Unprocessed|Ack Pending|Redelivered' | sed 's/^/      /'
}
lateral_round live 0 10
[ "$got" -eq "$M" ] && ok "$M pairs within 2m matched once each; 5 pairs 3m apart did not (consumed as published)" || { bad "live: expected $M lateral alerts, got $got"; diag_lateral; }
lateral_round backlog 1 12
[ "$got" -eq "$M" ] && ok "the same $M, and no more, when the second step arrives as one backlog" || { bad "backlog: expected $M lateral alerts, got $got"; diag_lateral; }
kill "$SUB_PID" 2>/dev/null; pkill -f "[s]ub vxt.lateral" 2>/dev/null

echo "── D6 a count closes on its source's event time (the attacker stops)"
( timeout 30 "$NATS" -s "$URL" sub vxt.brute --raw > "$WORK/brute.txt" 2>/dev/null ) & SUB_PID=$!
sleep 0.5
auth() { "$NATS" -s "$URL" pub vxt.auth "{\"event_type\":\"Auth\",\"@timestamp\":\"2026-09-21T18:$1Z\",\"ip\":\"$2\",\"status\":\"$3\"}" > /dev/null 2>&1; }
for t in 00:00 00:10 00:20; do auth "$t" 10.0.0.66 failure; done
auth 00:30 10.0.0.66 success   # the attacker got in, and stops
sleep 1.5
[ "$(grep -c '^{' "$WORK/brute.txt")" -eq 0 ] && ok "nothing while the window runs" || bad "an alert before the window ended: $(cat "$WORK/brute.txt")"
auth 06:00 10.0.0.7 success    # any later logon, from anyone
deadline=$((SECONDS+10)); while [ "$SECONDS" -lt "$deadline" ]; do grep -q '10.0.0.66' "$WORK/brute.txt" && break; sleep 0.2; done
sleep 1
python3 - "$WORK/brute.txt" <<'PY' && ok "the next logon of anyone raises the brute force, with its count" || bad "brute force: $(cat "$WORK/brute.txt")"
import json, sys
alerts=[json.loads(l) for l in open(sys.argv[1]) if l.startswith("{")]
assert len(alerts)==1 and alerts[0]["ip"]=="10.0.0.66" and alerts[0]["n"]==3, alerts
PY
kill "$SUB_PID" 2>/dev/null; pkill -f "[s]ub vxt.brute" 2>/dev/null

echo "── D7 a count on a source that goes quiet still closes (idle grace)"
( timeout 30 "$NATS" -s "$URL" sub vxt.vpnbrute --raw > "$WORK/vpnbrute.txt" 2>/dev/null ) & SUB_PID=$!
sleep 0.5
for s in 00 01 02; do
  "$NATS" -s "$URL" pub vxt.vpn "{\"event_type\":\"Vpn\",\"@timestamp\":\"2026-09-21T19:00:${s}Z\",\"ip\":\"10.0.0.88\",\"status\":\"failure\"}" > /dev/null 2>&1
done
# Nothing else is ever published on the VPN subject.
deadline=$((SECONDS+15)); while [ "$SECONDS" -lt "$deadline" ]; do grep -q '10.0.0.88' "$WORK/vpnbrute.txt" && break; sleep 0.2; done
sleep 1
python3 - "$WORK/vpnbrute.txt" <<'PY' && ok "the silent source's brute force is raised once the grace has passed, with its count" || bad "quiet source: $(cat "$WORK/vpnbrute.txt")"
import json, sys
alerts=[json.loads(l) for l in open(sys.argv[1]) if l.startswith("{")]
assert len(alerts)==1 and alerts[0]["ip"]=="10.0.0.88" and alerts[0]["n"]==3, alerts
PY
kill "$SUB_PID" 2>/dev/null; pkill -f "[s]ub vxt.vpnbrute" 2>/dev/null

echo "── D8 an absence on subjects that go silent is raised (idle grace)"
( timeout 90 "$NATS" -s "$URL" sub vxt.unacked --raw > "$WORK/unacked.txt" 2>/dev/null ) & SUB_PID=$!
sleep 0.5
shop() { # <order|ack> <Order|Ack> <time> <id>
  "$NATS" -s "$URL" pub "vxt.shop.$1" "{\"event_type\":\"$2\",\"@timestamp\":\"2026-09-21T$3Z\",\"id\":\"$4\"}" > /dev/null 2>&1
}
shop order Order 20:00:00 o-1
shop order Order 20:00:00 o-2
shop ack Ack 20:00:01 o-2
# Nothing else is ever published on the shop subjects.
deadline=$((SECONDS+20)); while [ "$SECONDS" -lt "$deadline" ]; do grep -q '"o-1"' "$WORK/unacked.txt" && break; sleep 0.2; done
sleep 1.5
python3 - "$WORK/unacked.txt" <<'PY' && ok "the unacknowledged order is raised once its five seconds and the grace have passed, the acked one is not" || bad "absence: $(cat "$WORK/unacked.txt")"
import json, sys
alerts=[json.loads(l) for l in open(sys.argv[1]) if l.startswith("{")]
assert [a["order"] for a in alerts]==["o-1"], alerts
PY

echo "── D9 an absence survives kill -9 (snapshot)"
unacked_snapshots() { curl -s "http://127.0.0.1:$HTTP_P/metrics" | grep "vejas_snapshots_total{unit=\"detect:unacked\"}" | awk '{print $2}'; }
before=$(processed detect:unacked); before=${before:-0}
# The pattern waits for the slower of its two subjects: an acknowledgement for
# another order keeps the ack subject's event time current, as on a live bus.
shop ack Ack 21:00:00 o-0
shop order Order 21:00:00 o-3
deadline=$((SECONDS+5)); while [ "$SECONDS" -lt "$deadline" ]; do now=$(processed detect:unacked); [ $(( ${now:-0} - before )) -ge 2 ] && break; sleep 0.1; done
s0=$(unacked_snapshots); s0=${s0:-0}
deadline=$((SECONDS+3)); while [ "$SECONDS" -lt "$deadline" ]; do [ "$(unacked_snapshots)" -gt "$s0" ] 2>/dev/null && break; sleep 0.1; done
kill -9 "$RT_PID"; wait "$RT_PID" 2>/dev/null; RT_PID=""
grep -q '"o-3"' "$WORK/unacked.txt" && bad "o-3 was raised before the crash: the check proves nothing" || ok "o-3 still open when the runtime is killed"
start_rt
deadline=$((SECONDS+20)); while [ "$SECONDS" -lt "$deadline" ]; do grep -q '"o-3"' "$WORK/unacked.txt" && break; sleep 0.2; done
sleep 1.5
python3 - "$WORK/unacked.txt" <<'PY' && ok "the absence the unit was waiting out is raised after the restart, once" || bad "absence after kill -9: $(cat "$WORK/unacked.txt")"
import json, sys
alerts=[json.loads(l) for l in open(sys.argv[1]) if l.startswith("{")]
assert [a["order"] for a in alerts]==["o-1","o-3"], alerts
PY
kill "$SUB_PID" 2>/dev/null; pkill -f "[s]ub vxt.unacked" 2>/dev/null

echo "── D5 a stateful sequence survives kill -9 (snapshot, resume by sequence)"
( timeout 60 "$NATS" -s "$URL" sub vxt.lateral --raw > "$WORK/lateral5.txt" 2>/dev/null ) & SUB_PID=$!
sleep 0.5
snapshots() { curl -s "http://127.0.0.1:$HTTP_P/metrics" | grep "vejas_snapshots_total{unit=\"detect:lateral\"}" | awk '{print $2}'; }
crash_and_restart() { # <snapshot cadence seconds for the new runtime>
  kill -9 "$RT_PID"; wait "$RT_PID" 2>/dev/null; RT_PID=""
  SNAP_SECS="$1" start_rt
}
open_sequences() { # <prefix> <hour>: M SmbConnect, consumed
  local pfx="$1" h="$2" before now i
  before=$(processed detect:lateral); before=${before:-0}
  for i in $(seq 1 $M); do "$NATS" -s "$URL" pub vxt.sysmon.net "{\"event_type\":\"SmbConnect\",\"@timestamp\":\"2026-09-21T$h:00:00Z\",\"host\":\"$pfx-ws-$i\",\"target\":\"$pfx-srv-$i\"}" > /dev/null 2>&1; done
  deadline=$((SECONDS+10)); while [ "$SECONDS" -lt "$deadline" ]; do now=$(processed detect:lateral); [ $(( ${now:-0} - before )) -ge "$M" ] && break; sleep 0.2; done
}
close_sequences() { # <prefix> <hour>: M ServiceStart a minute on; got = distinct alerts with the prefix
  local pfx="$1" h="$2" i
  for i in $(seq 1 $M); do "$NATS" -s "$URL" pub vxt.sysmon.proc "{\"event_type\":\"ServiceStart\",\"@timestamp\":\"2026-09-21T$h:01:00Z\",\"host\":\"$pfx-srv-$i\",\"image\":\"psexesvc.exe\"}" > /dev/null 2>&1; done
  deadline=$((SECONDS+10)); got=0
  while [ "$SECONDS" -lt "$deadline" ]; do got=$(distinct_ids "$WORK/lateral5.txt" to "$pfx-"); [ "$got" -ge "$M" ] && break; sleep 0.3; done
  sleep 1.5; got=$(distinct_ids "$WORK/lateral5.txt" to "$pfx-")
}
# Round A: the open sequences are in a snapshot when the runtime dies.
open_sequences snapA 14
s0=$(snapshots); s0=${s0:-0}
deadline=$((SECONDS+3)); while [ "$SECONDS" -lt "$deadline" ]; do [ "$(snapshots)" -gt "$s0" ] 2>/dev/null && break; sleep 0.2; done
crash_and_restart 1
grep -q 'restored its snapshot' "$WORK/rt.log" && ok "the restart restored the unit's snapshot" || bad "no 'restored its snapshot' in the runtime log"
close_sequences snapA 14
[ "$got" -eq "$M" ] && ok "$M sequences opened before the crash closed after it (from the snapshot)" || { bad "round A: expected $M alerts, got $got"; diag_lateral; }
# Round B: the crash comes before any snapshot holds the open sequences
# (cadence one hour); the restart replays what was acked after the last
# snapshot, and the sequences come back from the bus.
crash_and_restart 3600
open_sequences replay 16
crash_and_restart 1
close_sequences replay 16
[ "$got" -eq "$M" ] && ok "$M sequences acked after the last snapshot came back from the bus (replay by sequence)" || { bad "round B: expected $M alerts, got $got"; diag_lateral; }
restores=$(curl -s "http://127.0.0.1:$HTTP_P/metrics" | grep 'vejas_restores_total{unit="detect:lateral"}' | awk '{print $2}')
[ "${restores:-0}" -ge 1 ] && ok "restores are counted (vejas_restores_total=$restores)" || bad "vejas_restores_total is ${restores:-missing}"
kill "$SUB_PID" 2>/dev/null; pkill -f "[s]ub vxt.lateral" 2>/dev/null

if [ "$fail" -eq 0 ]; then echo "detect: all invariants hold ✓"; else echo "detect: FAILED"; tail -30 "$WORK/rt.log"; fi
exit $fail
