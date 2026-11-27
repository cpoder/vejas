#!/bin/bash
# An emit the stream refuses to store fails its run: its message is not acked
# for it, comes back, and is a dead letter naming the refusal once its
# deliveries are spent. The other runs of its batch stand, and an api flow
# whose emit is refused answers 503. Before, the emit was lost and its message
# acked. Then, with streams outside the subject root: a refusal no later try
# changes is a dead letter at the first delivery (500 for an api flow), and a
# detect unit's alert that a stream refuses is a dead letter of its own, which
# a replay publishes, while its message is acked and not delivered again.
# Last, a detect unit whose acks never reach the server: the engine of a
# program with state takes each message in once, however often it comes back.
#   e2e/emit-stored.sh                       (needs the nats CLI)
set -uo pipefail
cd "$(dirname "$0")/.."
BIN="${BIN:-$PWD/core/target/release/vejas-runtime}"
[ -x "$BIN" ] || { echo "build first" >&2; exit 1; }
S=$(mktemp -d); R=$(mktemp -d)
RT=""; NP=""
trap 'kill $RT $NP 2>/dev/null; wait $RT $NP 2>/dev/null; rm -rf "$S" "$R"' EXIT
fail=0
ok(){ echo "  ✓ $1"; }
ko(){ echo "  ✗ $1"; fail=1; }

PORT=4296; URL=nats://127.0.0.1:$PORT; H="http://127.0.0.1:8767"
mkdir -p "$R/flows"
printf '# flow: fan\nsource "vx.s6.in"\nemit f"vx.s6.out.{id}", {id: id}\n' > "$R/flows/fan.vjs"
printf '# flow: post\napi "POST /post"\nemit f"vx.s6.out.{id}", {id: id}\nrespond 200, {ok: true}\n' > "$R/flows/post.vjs"

nats-server -js -sd "$S/n" -a 127.0.0.1 -p $PORT > /dev/null 2>&1 & NP=$!
for _ in $(seq 1 50); do nats -s $URL server check connection > /dev/null 2>&1 && break; sleep 0.1; done
# VEJAS made before the runtime, which keeps a stream it finds: three messages
# at most, and a fourth is refused rather than an old one dropped.
nats -s $URL req '$JS.API.STREAM.CREATE.VEJAS' \
  '{"name":"VEJAS","subjects":["vx.>"],"retention":"limits","storage":"file","discard":"new","max_msgs":3}' \
  > "$S/create" 2>&1
grep -q '"type":"io.nats.jetstream.api.v1.stream_create_response"' "$S/create" && ! grep -q '"error"' "$S/create" \
  || { echo "stream create failed: $(cat "$S/create")"; exit 1; }
# Two events wait for the flow: its first batch takes both. a's emit is the
# third message; b's would be the fourth.
for id in ev-a ev-b; do nats -s $URL req vx.s6.in "{\"id\":\"$id\"}" > /dev/null 2>&1; done
count(){ nats -s $URL stream info VEJAS --json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["state"]["messages"])'; }
[ "$(count)" = "2" ] || { echo "the two events are not stored: $(count)"; exit 1; }

NATS_URL=$URL VEJAS_ROOT="$R" VEJAS_HTTP_ADDR=127.0.0.1:8767 VEJAS_ACK_WAIT_SECS=1 \
  "$BIN" > "$S/rt.log" 2>&1 & RT=$!
until curl -sf -o /dev/null "$H/healthz"; do sleep 0.1; done
letter(){ curl -s -m 5 "$H/dlq" | python3 -c '
import json, sys
for d in json.load(sys.stdin)["dead_letters"]:
    if sys.argv[1] in json.dumps(d.get("payload")):
        print(d.get("attempts"), d.get("last_error")); break' "$1"; }

echo "── a batch of two runs: one emit stored, one refused"
L=""; for _ in $(seq 1 150); do L=$(letter ev-b); [ -n "$L" ] && break; sleep 0.1; done
echo "$L" | grep -q "publish vx.s6.out.ev-b: maximum messages exceeded" \
  && ok "b is a dead letter that names the refusal: $L" || ko "letter of b: '$L'"
echo "$L" | grep -q "^5 " && ok "after its 5 deliveries" || ko "attempts: '$L'"
[ -z "$(letter ev-a)" ] && ok "a is no dead letter" || ko "a was dead-lettered: $(letter ev-a)"
nats -s $URL stream subjects VEJAS --json 2>/dev/null > "$S/subjects"
grep -q '"vx.s6.out.ev-a"' "$S/subjects" && ok "a's emit is stored" || ko "subjects: $(cat "$S/subjects")"
! grep -q '"vx.s6.out.ev-b"' "$S/subjects" && ok "b's emit is not" || ko "b's emit is stored"
PENDING=$(nats -s $URL consumer info VEJAS flow_fan --json 2>/dev/null | python3 -c 'import json,sys; c=json.load(sys.stdin); print(c["num_ack_pending"], c["num_pending"])')
[ "$PENDING" = "0 0" ] && ok "both messages are settled: a acked, b acked after its dead letter" || ko "ack pending, pending: $PENDING"
curl -s -m 5 "$H/events" > "$S/events"
python3 - "$S/events" <<'PY' && ok "/events shows b's runs as failed, with the refusal" || ko "/events: $(head -c 600 "$S/events")"
import json, sys
rows = json.load(open(sys.argv[1]))
rows = rows.get("events", rows) if isinstance(rows, dict) else rows
b = [r for r in rows if "ev-b" in (r.get("preview") or "")]
assert b and not any(r.get("ok") for r in b), b
assert any("maximum messages exceeded" in (r.get("error") or "") for r in b), b
PY

echo "── an api flow whose emit the stream refuses"
C=$(curl -s -m 10 -o "$S/answer" -w '%{http_code}' -X POST -H 'Content-Type: application/json' -d '{"id":"ev-c"}' "$H/api/post")
[ "$C" = "503" ] && grep -q "maximum messages exceeded" "$S/answer" && ok "503: $(cat "$S/answer")" || ko "$C: $(cat "$S/answer")"

# Part two, on a bus of its own.
kill $RT $NP 2>/dev/null; wait $RT $NP 2>/dev/null; RT=""; NP=""
PORT=4294; URL=nats://127.0.0.1:$PORT; H="http://127.0.0.1:8766"; R2="$S/root2"
mkdir -p "$R2/flows" "$R2/detects"
PAD=$(python3 -c 'print("x" * 100)')
printf '# flow: wide\nsource "vx.s6.wide"\nemit "small.out", {id: id, pad: "%s"}\n' "$PAD" > "$R2/flows/wide.vjs"
printf '# flow: wideapi\napi "POST /wide"\nemit "vx.s6.kept", {id: id}\nemit "small.out", {id: id, pad: "%s"}\nrespond 200, {ok: true}\n' "$PAD" \
  > "$R2/flows/wideapi.vjs"
cat > "$R2/detects/big.vpl" <<'VPL'
connector Bus = nats (
    url: "ignored-by-vejas: the bus is NATS_URL"
)

event Order:
    id: str
    total: float

stream Orders = Order
    .from(Bus, topic: "vx.s6.orders")

stream Big = Order
    .where(total > 100.0)
    .emit(id: id, total: total)
    .to(Bus, topic: "alerts.big")
VPL
nats-server -js -sd "$S/n2" -a 127.0.0.1 -p $PORT > /dev/null 2>&1 & NP=$!
for _ in $(seq 1 50); do nats -s $URL server check connection > /dev/null 2>&1 && break; sleep 0.1; done
# outside the root: ALERTS takes one message and refuses the next, SMALL
# refuses any over 64 bytes
mk(){ nats -s $URL req "\$JS.API.STREAM.CREATE.$1" "$2" > "$S/create-$1" 2>&1
  grep -q '"error"' "$S/create-$1" && { echo "stream $1: $(cat "$S/create-$1")"; exit 1; }; }
mk ALERTS '{"name":"ALERTS","subjects":["alerts.>"],"retention":"limits","storage":"file","discard":"new","max_msgs":1}'
mk SMALL '{"name":"SMALL","subjects":["small.>"],"retention":"limits","storage":"file","max_msg_size":64}'
NATS_URL=$URL VEJAS_ROOT="$R2" VEJAS_HTTP_ADDR=127.0.0.1:8766 VEJAS_ACK_WAIT_SECS=1 \
  "$BIN" > "$S/rt2.log" 2>&1 & RT=$!
until curl -sf -o /dev/null "$H/healthz"; do sleep 0.1; done
for _ in $(seq 1 100); do [ "$(grep -c 'consuming' "$S/rt2.log")" -ge 2 ] && break; sleep 0.1; done
for id in o1 o2; do nats -s $URL pub vx.s6.orders "{\"id\":\"$id\",\"total\":150.0}" > /dev/null 2>&1; done
nats -s $URL pub vx.s6.wide '{"id":"w1"}' > /dev/null 2>&1
letters(){ curl -s -m 5 "$H/dlq" | python3 -c '
import json, sys
for d in json.load(sys.stdin)["dead_letters"]:
    print(d.get("seq"), d.get("unit"), d.get("original_subject"), d.get("attempts"), bool(d.get("emit")),
          json.dumps(d.get("payload")), d.get("last_error"))'; }
L=""; for _ in $(seq 1 150); do L=$(letters); echo "$L" | grep -q "detect:big" && echo "$L" | grep -q "flow:wide" && break; sleep 0.1; done

echo "── a detect unit's alert that a stream outside the root refuses"
A=$(echo "$L" | grep " detect:big ")
echo "$A" | grep -q " alerts.big 1 True " && echo "$A" | grep -q 'o2' && echo "$A" | grep -q "maximum messages exceeded" \
  && ok "o2's alert is a dead letter of its own: $(echo "$A" | cut -c1-150)" || ko "alert letter: '$A'"
[ "$(echo "$A" | grep -c .)" = "1" ] && ok "o1's alert is stored, so one letter" || ko "letters of detect:big: $(echo "$A" | grep -c .)"
sleep 3   # three ack waits: a message left un-acked would be back by now
P=$(nats -s $URL consumer info VEJAS detect_big --json 2>/dev/null | python3 -c '
import json, sys; c = json.load(sys.stdin); print(c["num_ack_pending"], c["num_pending"], c["delivered"]["consumer_seq"])')
[ "$P" = "0 0 2" ] && ok "both orders settled, each delivered once" || ko "ack pending, pending, deliveries: $P"
SEQ=$(echo "$A" | awk 'NR==1 {print $1}')
nats -s $URL stream purge ALERTS -f > /dev/null 2>&1
REPLAY=$(curl -s -m 10 -X POST -H 'Content-Type: application/json' -d "{\"seq\":${SEQ:-0}}" "$H/dlq/replay")
LAST=$(nats -s $URL stream info ALERTS --json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["state"]["last_seq"])')
GOT=$(nats -s $URL stream get ALERTS "${LAST:-0}" --json 2>/dev/null | python3 -c '
import json, sys, base64; print(base64.b64decode(json.load(sys.stdin).get("data", "")).decode())' 2>/dev/null)
echo "$REPLAY" | grep -q '"replayed":1' && echo "$GOT" | grep -q '"o2"' \
  && ok "its replay publishes it where it was going: $GOT" || ko "replay: $REPLAY, stored: '$GOT'"

echo "── an emit no later try could store"
W=$(echo "$L" | grep " flow:wide ")
echo "$W" | grep -q " vx.s6.wide 1 False " && echo "$W" | grep -q "exceeds maximum" \
  && ok "the flow's message is a dead letter at its first delivery: $(echo "$W" | cut -c1-120)" || ko "letter of wide: '$W'"
C=$(curl -s -m 10 -o "$S/wide" -w '%{http_code}' -X POST -H 'Content-Type: application/json' -d '{"id":"a1"}' "$H/api/wide")
[ "$C" = "500" ] && grep -q "which no later try changes: 1 of this request's 2 events went out" "$S/wide" \
  && ok "an api flow answers 500: $(cat "$S/wide")" || ko "$C: $(cat "$S/wide")"

# Part three: the runtime may not ack its detect unit's messages, so the
# server delivers each again at every ack wait, as after a lost ack.
kill $RT $NP 2>/dev/null; wait $RT $NP 2>/dev/null; RT=""; NP=""
PORT=4297; ADMIN=nats://admin:admin@127.0.0.1:$PORT; H="http://127.0.0.1:8768"; R3="$S/root3"
mkdir -p "$R3/detects"
cat > "$S/nats3.conf" <<EOF
port: $PORT
host: 127.0.0.1
jetstream: { store_dir: "$S/n3" }
accounts: {
  A: {
    jetstream: enabled
    users: [
      {user: rt, password: rt, permissions: { publish: { allow: [">"], deny: ["\$JS.ACK.VEJAS.detect_guard.>"] }, subscribe: { allow: [">"] } }},
      {user: admin, password: admin}
    ]
  }
}
EOF
# two failed logons where a brute force takes three, and one big order
cat > "$R3/detects/guard.vpl" <<'VPL'
connector Bus = nats (
    url: "ignored-by-vejas: the bus is NATS_URL"
)

event Auth:
    ip: str
    status: str
    total: float

stream Logons = Auth
    .from(Bus, topic: "vx.s6.auth")

stream Failed = Auth
    .where(status == "failure")

stream Brute = Failed
    .partition_by(ip)
    .window(5m)
    .aggregate(ip: last(ip), n: count())
    .where(n >= 3)
    .emit(rule: "brute_force", ip: ip, n: n)
    .to(Bus, topic: "vx.s6.brute")

stream Big = Auth
    .where(total > 100.0)
    .emit(ip: ip, total: total)
    .to(Bus, topic: "vx.s6.big")
VPL
nats-server -c "$S/nats3.conf" > /dev/null 2>&1 & NP=$!
for _ in $(seq 1 50); do nats -s $ADMIN server check connection > /dev/null 2>&1 && break; sleep 0.1; done
NATS_URL=nats://rt:rt@127.0.0.1:$PORT VEJAS_ROOT="$R3" VEJAS_HTTP_ADDR=127.0.0.1:8768 VEJAS_ACK_WAIT_SECS=1 VEJAS_IDLE_CLOSE_SECS=0 \
  "$BIN" > "$S/rt3.log" 2>&1 & RT=$!
until curl -sf -o /dev/null "$H/healthz"; do sleep 0.1; done
for _ in $(seq 1 100); do grep -q 'consuming vx.s6.auth' "$S/rt3.log" && break; sleep 0.1; done
auth(){ nats -s $ADMIN pub vx.s6.auth "{\"event_type\":\"Auth\",\"@timestamp\":\"2026-09-21T18:$1Z\",\"ip\":\"$2\",\"status\":\"$3\",\"total\":$4}" > /dev/null 2>&1; }
auth 00:00 10.0.0.1 failure 1.0
auth 00:10 10.0.0.1 failure 1.0
auth 00:20 10.0.0.2 success 500.0
sleep 4   # four ack waits: each message is delivered again
D=$(nats -s $ADMIN consumer info VEJAS detect_guard --json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["delivered"]["consumer_seq"])')
echo "── a detect unit whose messages come back"
[ "${D:-0}" -gt 6 ] && ok "the server delivered the three messages $D times, its acks denied" || ko "deliveries: '${D:-none}', the acks were not denied"
auth 06:00 10.0.0.9 success 1.0   # closes 10.0.0.1's window, in event time
sleep 2
nats -s $ADMIN stream subjects VEJAS --json 2>/dev/null > "$S/subjects3"
python3 - "$S/subjects3" <<'PY' && ok "the engine took each in once: one big order alert, no brute force" || ko "alerts: $(cat "$S/subjects3")"
import json, sys
subjects = json.load(open(sys.argv[1]))
assert subjects.get("vx.s6.big") == 1 and "vx.s6.brute" not in subjects, subjects
PY
# the control: three failures of one address raise one brute force, counted
# three, however often their messages come back
auth 10:00 10.0.0.5 failure 1.0
auth 10:10 10.0.0.5 failure 1.0
auth 10:20 10.0.0.5 failure 1.0
sleep 3
auth 16:00 10.0.0.9 success 1.0
B=""; for _ in $(seq 1 50); do
  B=$(nats -s $ADMIN req '$JS.API.STREAM.MSG.GET.VEJAS' '{"last_by_subj":"vx.s6.brute"}' 2>/dev/null)
  echo "$B" | grep -q '"message"' && break; sleep 0.1; done
sleep 1   # a second brute force, from a copy, would be stored by now
nats -s $ADMIN stream subjects VEJAS --json 2>/dev/null > "$S/subjects3"
python3 - "$S/subjects3" "$B" <<'PY' && ok "the control: one brute force, of three failures" || ko "brute force: $B, subjects: $(cat "$S/subjects3")"
import json, sys, base64
subjects = json.load(open(sys.argv[1]))
answer = [l for l in sys.argv[2].splitlines() if l.startswith("{")][-1]
alert = json.loads(base64.b64decode(json.loads(answer)["message"]["data"]))
assert subjects.get("vx.s6.brute") == 1 and alert["n"] == 3 and alert["ip"] == "10.0.0.5", (subjects, alert)
PY

[ $fail -eq 0 ] && echo "EMIT STORED HOLDS" || echo "EMIT STORED REGRESSION — do not ship"
exit $fail
