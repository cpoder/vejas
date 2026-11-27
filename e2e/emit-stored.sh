#!/bin/bash
# An emit the stream refuses to store fails its run: its message is not acked
# for it, comes back, and is a dead letter naming the refusal once its
# deliveries are spent. The other runs of its batch stand, and an api flow
# whose emit is refused answers 503. Before, the emit was lost and its message
# acked.
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

[ $fail -eq 0 ] && echo "EMIT STORED HOLDS" || echo "EMIT STORED REGRESSION — do not ship"
exit $fail
