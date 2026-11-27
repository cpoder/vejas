#!/bin/bash
# An emit subject cannot carry protocol. A subject built from an event field used
# to reach the NATS wire as it was: a line break in the field became protocol
# commands, and through http-in that needed no credential. A refused subject fails
# the run like any other error, so the message reaches the DLQ. So does an emit
# over the server's limit in a flow, and the emits its run made before it never
# leave. In a detect unit none of the run's alerts leave either: each is a dead
# letter at once, the large one cut, since its message delivered again would
# feed the engine twice.
#   e2e/emit-subject.sh                      (needs the nats CLI)
set -uo pipefail
cd "$(dirname "$0")/.."
BIN="$PWD/core/target/release/vejas-runtime"
[ -x "$BIN" ] || { echo "build first" >&2; exit 1; }
S=$(mktemp -d); R=$(mktemp -d)
RT=""; NP=""; SUB=""; EVIL=""; FIRST=""; BLOB=""
trap 'kill $RT $NP $SUB $EVIL $FIRST $BLOB 2>/dev/null; wait $RT $NP $SUB $EVIL $FIRST $BLOB 2>/dev/null; rm -rf "$S" "$R"' EXIT
fail=0
ok(){ echo "  ✓ $1"; }
ko(){ echo "  ✗ $1"; fail=1; }

URL=nats://127.0.0.1:4281; H="http://127.0.0.1:8753"; IN=18794
mkdir -p "$R/flows" "$R/connectors" "$R/detects"
printf '# flow: dyn\nsource "vx.dyn.in"\nemit f"vx.out.{tenant}", {id: id}\n' > "$R/flows/dyn.vjs"
printf '# connector: http_in\ndriver "http-in"\nPORT = %s\n' $IN > "$R/connectors/http_in.vjs"
# an emit over the server's 1 MB limit: 16 bytes doubled seventeen times
printf '# flow: big\nsource "vx.big.in"\ns = "xxxxxxxxxxxxxxxx"\nfor i in items:\n    s = s + s\nend\nemit "vx.big.out", {s: s}\n' \
  > "$R/flows/big.vjs"
# the same, after an emit that the refusal must take back with it
printf '# flow: pair\nsource "vx.pair.in"\nemit "vx.pair.first", {n: 1}\ns = "xxxxxxxxxxxxxxxx"\nfor i in items:\n    s = s + s\nend\nemit "vx.pair.big", {s: s}\n' \
  > "$R/flows/pair.vjs"

# an api flow whose emit is over the limit: none of its events can leave
printf '# flow: bigapi\napi "POST /bigapi"\nemit "vx.bigapi.first", {n: 1}\ns = "xxxxxxxxxxxxxxxx"\nfor i in items:\n    s = s + s\nend\nemit "vx.bigapi.out", {s: s}\nrespond 201, {ok: true}\n' \
  > "$R/flows/bigapi.vjs"
# a detect unit with two emits for each event, the second over the limit:
# five copies of a field the engine keeps 256 KiB of
cat > "$R/detects/blob.vpl" <<'VPL'
connector Bus = nats (
    url: "ignored-by-vejas: the bus is NATS_URL"
)

event Blob:
    s: str

stream Blobs = Blob
    .from(Bus, topic: "vx.blob.in")

stream First = Blob
    .emit(rule: "first")
    .to(Bus, topic: "vx.blob.first")

stream Huge = Blob
    .emit(a: s, b: s, c: s, d: s, e: s)
    .to(Bus, topic: "vx.blob.big")
VPL

nats-server -js -sd "$S/n" -a 127.0.0.1 -p 4281 > /dev/null 2>&1 & NP=$!
sleep 0.5
NATS_URL=$URL VEJAS_ROOT="$R" VEJAS_HTTP_ADDR=127.0.0.1:8753 VEJAS_ACK_WAIT_SECS=1 \
  "$BIN" > "$S/rt.log" 2>&1 & RT=$!
until curl -sf -o /dev/null "$H/healthz"; do sleep 0.1; done
post(){ curl -s -m 5 -o /dev/null -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
  --data-binary "$1" "http://127.0.0.1:$IN/ingest/dyn.in"; }
for _ in $(seq 1 100); do [ "$(post '{"id":0,"tenant":"warmup"}')" = "202" ] && break; sleep 0.1; done
sleep 1.5
messages(){ nats -s $URL stream info VEJAS --json 2>/dev/null \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["state"]["messages"])'; }
hostile(){ python3 -c 'import json,sys; print(json.dumps({"id": 1, "tenant": sys.argv[1].encode().decode("unicode_escape")}))' "$1"; }

nats -s $URL sub 'vx.out.>' --raw > "$S/bus" 2>/dev/null & SUB=$!
nats -s $URL sub vx.evil --raw > "$S/evil" 2>/dev/null & EVIL=$!
sleep 0.5

echo "── a clean field still routes"
C=$(post '{"id":1,"tenant":"acme"}')
for _ in $(seq 1 50); do grep -q '"id":1}' "$S/bus" && break; sleep 0.1; done
grep -q '"id":1}' "$S/bus" && ok "the flow emitted for tenant acme ($C)" || ko "the clean emit is gone"

echo "── a hostile field carries no protocol"
BEFORE=$(messages)
post "$(hostile 'x 0\r\n\r\nPUB vx.evil 6\r\ninject\r\nPUB vx.pad')" > /dev/null
post "$(hostile 'x 0\r\n\r\nPUB $JS.API.STREAM.PURGE.VEJAS 0\r\n\r\nPUB vx.pad')" > /dev/null
sleep 2
[ -s "$S/evil" ] && ko "a field published on the bus" || ok "nothing was published by the field"
[ "$(messages)" -ge "$BEFORE" ] && ok "the stream was not purged" || ko "the stream lost messages"

echo "── the refused run takes the poison path"
letters(){ curl -s -m 5 "$H/dlq" | python3 -c '
import json, sys
print(len([d for d in json.load(sys.stdin)["dead_letters"] if "not a valid subject" in d.get("last_error", "")]))'; }
for _ in $(seq 1 150); do [ "$(letters)" = "2" ] && break; sleep 0.1; done
[ "$(letters)" = "2" ] && ok "both messages reached the DLQ with the reason" || ko "$(letters) of 2 reached the DLQ"

echo "── an emit the server would refuse takes it too"
curl -s -m 5 -o /dev/null -X POST -H 'Content-Type: application/json' \
  -d '{"items":[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17]}' "http://127.0.0.1:$IN/ingest/big.in"
big_letters(){ curl -s -m 5 "$H/dlq" | python3 -c '
import json, sys
print(len([d for d in json.load(sys.stdin)["dead_letters"] if d.get("unit") == "flow:big"]))'; }
for _ in $(seq 1 150); do [ "$(big_letters)" = "1" ] && break; sleep 0.1; done
[ "$(big_letters)" = "1" ] && ok "an oversized emit reached the DLQ" || ko "the oversized emit never reached the DLQ"

echo "── what the refused run emitted before the refusal never leaves"
nats -s $URL sub vx.pair.first --raw > "$S/first" 2>/dev/null & FIRST=$!
sleep 0.5
curl -s -m 5 -o /dev/null -X POST -H 'Content-Type: application/json' \
  -d '{"items":[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17]}' "http://127.0.0.1:$IN/ingest/pair.in"
pair_letters(){ curl -s -m 5 "$H/dlq" | python3 -c '
import json, sys
print(len([d for d in json.load(sys.stdin)["dead_letters"] if d.get("unit") == "flow:pair"]))'; }
for _ in $(seq 1 150); do [ "$(pair_letters)" = "1" ] && break; sleep 0.1; done
[ "$(pair_letters)" = "1" ] && ok "the run reached the DLQ after its deliveries" || ko "the run never reached the DLQ"
[ -s "$S/first" ] && ko "the first emit left $(wc -l < "$S/first") times" || ok "the first emit never left"

echo "── an api flow says that sending its request again changes nothing"
C=$(curl -s -m 10 -o "$S/bigapi" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
  -d '{"items":[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17]}' "$H/api/bigapi")
[ "$C" = "500" ] && grep -q "over the server's limit" "$S/bigapi" && grep -q "none of this request's events left" "$S/bigapi" \
  && ok "answered $C: $(head -c 160 "$S/bigapi")" || ko "answered $C: $(head -c 200 "$S/bigapi")"

echo "── a detect unit's emit over the limit takes back the others too"
nats -s $URL sub vx.blob.first --raw > "$S/blob-first" 2>/dev/null & BLOB=$!
sleep 0.5
python3 -c 'import json; print(json.dumps({"event_type": "Blob", "s": "x" * 400000}))' > "$S/blob.json"
curl -s -m 5 -o /dev/null -X POST -H 'Content-Type: application/json' --data-binary @"$S/blob.json" \
  "http://127.0.0.1:$IN/ingest/blob.in"
blob_letters(){ curl -s -m 5 "$H/dlq" | python3 -c '
import json, sys
print(" ".join(sorted("%s:%s:%s:%s" % (d.get("original_subject"), d.get("attempts"), bool(d.get("emit")), bool(d.get("cut")))
    for d in json.load(sys.stdin)["dead_letters"] if d.get("unit") == "detect:blob")))'; }
for _ in $(seq 1 150); do [ "$(blob_letters | wc -w)" = "2" ] && break; sleep 0.1; done
L=$(blob_letters)
[ "$L" = "vx.blob.big:1:True:True vx.blob.first:1:True:False" ] \
  && ok "both alerts are dead letters at once, the large one cut: $L" || ko "letters: '$L'"
[ -s "$S/blob-first" ] && ko "the first emit left $(grep -c . "$S/blob-first") times" || ok "the first emit never left"

echo "── a shadow run shows the same error"
curl -s -m 10 -H 'Content-Type: application/json' -d \
  '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"vejas_run_flow","arguments":{"file":"flows/dyn.vjs","input":{"id":1,"tenant":"a b"}}}}' \
  "$H/mcp" | grep -q 'not a valid subject' && ok "vejas_run_flow reports it" || ko "the shadow run hides it"

echo "── http-in refuses such a suffix"
C=$(python3 -c '
import socket, sys
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=5)
s.sendall(b"POST /ingest/x\t0\n\nPUB\tvx.evil\t6 HTTP/1.1\r\nContent-Length: 2\r\n\r\n{}")
s.settimeout(4)
head = b""
while b"\r\n" not in head:  # the status line can arrive in several segments
    chunk = s.recv(100)
    if not chunk:
        break
    head += chunk
print(head.split(b" ")[1].decode())' $IN)
[ "$C" = "400" ] && ok "a tab and a bare line feed in the path: $C" || ko "the suffix got $C"
C=$(post '{"id":3,"tenant":"acme"}')
[ "$C" = "202" ] && ok "ingestion carries on ($C)" || ko "ingestion broke ($C)"

[ $fail -eq 0 ] && echo "EMIT SUBJECT HOLDS" || echo "EMIT SUBJECT REGRESSION — do not ship"
exit $fail
