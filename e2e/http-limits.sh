#!/bin/bash
# The panel and api server holds every client to a bound. tiny_http 0.12.0, as
# released, drained an unread body with one allocation of its declared length:
# `GET /healthz` with `Content-Length: 1099511627776` answered, then aborted the
# runtime. It read with no timeout and no thread bound, so slow clients pinned
# threads for good, and the accept loop's 503 drained bodies inline. The
# vendored copy (third_party/tiny_http/README.md) bounds the body, the time and
# the connections; this suite holds those bounds on the real binary.
#   e2e/http-limits.sh            (BIN=path/to/vejas-runtime to test another build)
set -uo pipefail
cd "$(dirname "$0")/.."
BIN="${BIN:-$PWD/core/target/release/vejas-runtime}"
[ -x "$BIN" ] || { echo "build first" >&2; exit 1; }
S=$(mktemp -d); R=$(mktemp -d)
RT=""; NP=""
cleanup(){ kill $RT $NP 2>/dev/null; wait $RT $NP 2>/dev/null; rm -rf "$S" "$R"; }
trap cleanup EXIT
fail=0
ok(){ echo "  ✓ $1"; }
ko(){ echo "  ✗ $1"; fail=1; }

NP_PORT=4305; PORT=8774; H="http://127.0.0.1:$PORT"
MAX=65536; TIMEOUT=3

mkdir -p "$R/flows"
printf '# flow: echo\napi "POST /echo"\nrespond 200, {n: n}\n' > "$R/flows/echo.vjs"

nats-server -js -sd "$S/n" -a 127.0.0.1 -p $NP_PORT > /dev/null 2>&1 & NP=$!
sleep 0.5
start_runtime(){ # extra env assignments, then the runtime
  env NATS_URL=nats://127.0.0.1:$NP_PORT VEJAS_ROOT="$R" VEJAS_HTTP_ADDR=127.0.0.1:$PORT \
    VEJAS_HTTP_MAX_BODY_BYTES=$MAX VEJAS_HTTP_TIMEOUT_SECS=$TIMEOUT "$@" "$BIN" >> "$S/rt.log" 2>&1 &
  RT=$!
  for _ in $(seq 1 100); do curl -sf -o /dev/null "$H/healthz" && break; sleep 0.1; done
  # the port answers, and it is this runtime that answers it
  kill -0 $RT 2>/dev/null && grep -q "panel + monitoring on http://127.0.0.1:$PORT" "$S/rt.log" \
    || { echo "the runtime did not bind $PORT"; cat "$S/rt.log"; exit 1; }
}
healthz(){ curl -s -m 2 -o /dev/null -w '%{http_code}' "$H/healthz"; }
alive(){ kill -0 $RT 2>/dev/null; }
# raw: send a request head (and a body), print the status line or "closed"
raw(){ python3 - "$PORT" "$1" <<'PY'
import socket, sys
port, what = int(sys.argv[1]), sys.argv[2]
s = socket.create_connection(("127.0.0.1", port), timeout=10)
if what == "terabyte":
    s.sendall(b"GET /healthz HTTP/1.1\r\nHost: x\r\nContent-Length: 1099511627776\r\n\r\n")
    try:
        s.sendall(b"x" * 1_000_000)  # a body starts to come, as an attacker would send it
    except OSError:
        pass
try:
    line = s.makefile("rb").readline().decode().strip()
except OSError:
    line = ""
print(line or "closed")
PY
}

start_runtime

echo "── a 1 TB Content-Length: an answer or a closed connection, and the runtime lives"
OUT=$(raw terabyte)
case "$OUT" in
  *413*|closed) ok "answered: $OUT" ;;
  *) ko "got: $OUT" ;;
esac
sleep 0.5
alive && ok "the runtime is still running" || ko "the runtime died: $(tail -3 "$S/rt.log")"
[ "$(healthz)" = "200" ] && ok "/healthz answers after it" || ko "/healthz does not answer after it"

echo "── a declared body over the bound is 413"
head -c $((MAX + 1)) /dev/zero | tr '\0' 'a' > "$S/big"
C=$(curl -s -m 10 -o /dev/null -w '%{http_code}' -H 'Content-Type: application/json' \
  -H 'Expect: 100-continue' --data-binary @"$S/big" "$H/api/echo")
[ "$C" = "413" ] && ok "Content-Length $((MAX + 1)) for a bound of $MAX: $C" || ko "Content-Length over the bound: $C"

echo "── a chunked body over the bound is refused"
C=$(curl -s -m 10 -o "$S/chunked.out" -w '%{http_code}' -H 'Content-Type: application/json' \
  -H 'Transfer-Encoding: chunked' --data-binary @"$S/big" "$H/api/echo")
[ "$C" = "413" ] && ok "chunked, $((MAX + 1)) bytes: $C $(cat "$S/chunked.out")" || ko "chunked over the bound: $C"
head -c $((MAX * 8)) /dev/zero | tr '\0' 'a' > "$S/huge"
C=$(curl -s -m 10 -o /dev/null -w '%{http_code}' -H 'Transfer-Encoding: chunked' \
  --data-binary @"$S/huge" "$H/api/echo")
case "$C" in
  413|000) ok "chunked, $((MAX * 8)) bytes: refused ($C)" ;;
  *) ko "chunked far over the bound: $C" ;;
esac

echo "── a normal api flow with a normal body still works"
python3 -c 'import json; print(json.dumps({"n": 7, "pad": "p" * 20000}))' > "$S/normal.json"
OUT=$(curl -s -m 10 -w ' %{http_code}' -H 'Content-Type: application/json' --data-binary @"$S/normal.json" "$H/api/echo")
[[ "$OUT" == *'"n":7'*' 200' ]] && ok "20 KB body: $OUT" || ko "normal body: $OUT"
OUT=$(curl -s -m 10 -w ' %{http_code}' -H 'Content-Type: application/json' -H 'Transfer-Encoding: chunked' \
  --data-binary @"$S/normal.json" "$H/api/echo")
[[ "$OUT" == *'"n":7'*' 200' ]] && ok "the same body chunked: $OUT" || ko "normal chunked body: $OUT"

echo "── a slow sender is cut off after the timeout and blocks nobody"
python3 - "$PORT" > "$S/slow.out" <<'PY' &
import socket, sys, time
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])))
s.settimeout(30)
s.sendall(b"POST /api/echo HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: 2000\r\n\r\n")
t0 = time.time(); how = "still open"
while time.time() - t0 < 25:
    try:
        s.sendall(b" ")
        s.settimeout(1)
        got = s.recv(4096)
        how = got.split(b"\r\n")[0].decode() if got else "closed"
        break
    except socket.timeout:
        continue
    except OSError:
        how = "closed"
        break
print(f"{time.time() - t0:.1f} {how}")
PY
SLOW=$!
sleep 1
T0=$(date +%s.%N)
C=$(healthz)
T=$(python3 -c "print(f'{$(date +%s.%N) - $T0:.2f}')")
[ "$C" = "200" ] && ok "/healthz answers in ${T}s while it sends" || ko "/healthz with a slow sender: $C"
wait $SLOW
read -r SECS HOW < "$S/slow.out"
if python3 -c "import sys; sys.exit(0 if float('$SECS') <= 2 * $TIMEOUT + 2 else 1)"; then
  ok "cut off after ${SECS}s (timeout ${TIMEOUT}s): $HOW"
else
  ko "the slow sender lasted ${SECS}s: $HOW"
fi

echo "── 40 slow connections do not stop /healthz"
python3 - "$PORT" > "$S/loris.out" <<'PY' &
import socket, sys, time
port = int(sys.argv[1])
socks = []
for _ in range(40):
    s = socket.create_connection(("127.0.0.1", port))
    s.sendall(b"GET /healthz HTTP/1.1\r\nHost: x\r\n")  # a head that never ends
    socks.append(s)
t0 = time.time(); open_ = set(range(40))
while open_ and time.time() - t0 < 20:
    for i in list(open_):
        try:
            socks[i].sendall(b"X-a: b\r\n")
        except OSError:
            open_.discard(i)
    time.sleep(0.5)
print(f"{len(open_)} {time.time() - t0:.1f}")
PY
LORIS=$!
sleep 1
GOOD=0
for _ in 1 2 3 4 5; do [ "$(healthz)" = "200" ] && GOOD=$((GOOD + 1)); sleep 0.2; done
[ $GOOD -eq 5 ] && ok "/healthz answered 5 of 5 under 40 slow connections" || ko "/healthz answered $GOOD of 5"
wait $LORIS
read -r LEFT SECS < "$S/loris.out"
[ "$LEFT" = "0" ] && ok "all 40 cut off by the server (within ${SECS}s)" || ko "$LEFT of 40 still open after ${SECS}s"
alive && ok "the runtime is still running" || ko "the runtime died"

kill $RT 2>/dev/null; wait $RT 2>/dev/null; RT=""

echo "── the in-flight 503 does not wait on a body"
# One slot. A slow body holds it; the next requests are shed by the accept
# loop, which must not read what their bodies would be.
start_runtime VEJAS_HTTP_MAX_INFLIGHT=1
python3 - "$PORT" <<'PY' &
import socket, sys, time
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])))
s.sendall(b"POST /api/echo HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: 2000\r\n\r\n")
time.sleep(6)
PY
HOLD=$!
sleep 0.5
OUT=$(python3 - "$PORT" <<'PY'
import socket, sys, time
codes, held = [], []
t0 = time.time()
for _ in range(3):
    s = socket.create_connection(("127.0.0.1", int(sys.argv[1])))
    held.append(s)  # each stays open: a body that may still come
    s.settimeout(5)
    # a body is declared and never sent
    s.sendall(b"POST /api/echo HTTP/1.1\r\nHost: x\r\nContent-Length: 50000\r\n\r\n")
    try:
        codes.append(s.makefile("rb").readline().split(b" ")[1].decode())
    except Exception:
        codes.append("none")
print(" ".join(codes), f"{time.time() - t0:.1f}")
PY
)
read -r C1 C2 C3 SECS <<< "$OUT"
if [ "$C1 $C2 $C3" = "503 503 503" ] && python3 -c "import sys; sys.exit(0 if float('$SECS') < 2 else 1)"; then
  ok "three 503s in ${SECS}s, none waited on its body"
else
  ko "shed requests: $OUT"
fi
wait $HOLD 2>/dev/null

[ $fail -eq 0 ] && echo "HTTP LIMITS HOLD" || echo "HTTP LIMITS REGRESSION — do not ship"
exit $fail
