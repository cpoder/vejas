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

echo "── an endless chunk-size line is cut at its bound, memory flat"
rss(){ awk '/^VmRSS:/{print $2}' "/proc/$RT/status"; }
BEFORE=$(rss)
OUT=$(python3 - "$PORT" <<'PY'
import socket, sys, time
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])))
s.sendall(b"GET /healthz HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n")
t0, sent, how = time.time(), 0, "still open after 10s"
s.settimeout(0.2)
while time.time() - t0 < 10:
    try:
        sent += s.send(b"1" * 4096)
    except socket.timeout:
        pass
    except OSError:
        how = "cut"; break
    try:
        if s.recv(65536) == b"":
            how = "cut"; break
    except socket.timeout:
        pass
    except OSError:
        how = "cut"; break
print(f"{how} {time.time() - t0:.1f} {sent}")
PY
)
read -r HOW SECS SENT <<< "$OUT"
GROWTH=$(( ($(rss) - BEFORE) / 1024 ))
[ "$HOW" = "cut" ] && ok "cut after ${SECS}s, $SENT bytes sent" || ko "endless size line: $OUT"
[ "$GROWTH" -lt 16 ] && ok "memory grew ${GROWTH} MB" || ko "memory grew ${GROWTH} MB"

echo "── framing is strict: an idle head is 408, an unknown coding 501, a bare LF 400"
strict(){ python3 - "$PORT" "$1" <<'PY'
import socket, sys
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])))
s.settimeout(10)
s.sendall(sys.argv[2].encode().decode("unicode_escape").encode())
try:
    print(s.makefile("rb").readline().split(b" ")[1].decode())
except Exception:
    print("none")
PY
}
C=$(strict 'GET /healthz HTTP/1.1\r\n'); [ "$C" = "408" ] && ok "idle head: $C" || ko "idle head: $C"
C=$(strict 'POST /api/echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: gzip\r\n\r\n'); [ "$C" = "501" ] && ok "Transfer-Encoding gzip: $C" || ko "Transfer-Encoding gzip: $C"
C=$(strict 'POST /api/echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\nContent-Length: 3\r\n\r\n'); [ "$C" = "400" ] && ok "chunked with a Content-Length: $C" || ko "chunked with a Content-Length: $C"
C=$(strict 'GET /healthz HTTP/1.1\nHost: x\n\n'); [ "$C" = "400" ] && ok "bare LF: $C" || ko "bare LF: $C"

kill $RT 2>/dev/null; wait $RT 2>/dev/null; RT=""

# Four handler slots from here: what one client holds shows.
start_runtime VEJAS_HTTP_MAX_INFLIGHT=4

echo "── a chunk extension sent a byte at a time is cut at the timeout"
OUT=$(python3 - "$PORT" <<'PY'
import socket, sys, threading, time
port = int(sys.argv[1]); res = []
def one():
    s = socket.create_connection(("127.0.0.1", port))
    t0 = time.time()
    s.sendall(b"POST /api/echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n1;")
    s.settimeout(1)
    while time.time() - t0 < 20:
        try:
            s.sendall(b"a")
            if s.recv(4096) == b"":
                break
            break  # an answer: the server gave up on the body
        except socket.timeout:
            continue
        except OSError:
            break
    res.append(time.time() - t0)
ts = [threading.Thread(target=one) for _ in range(4)]
[t.start() for t in ts]; [t.join() for t in ts]
print(f"{max(res):.1f}")
PY
)
python3 -c "import sys; sys.exit(0 if float('$OUT') <= $TIMEOUT + 1.5 else 1)" \
  && ok "4 connections, all cut within ${OUT}s (timeout ${TIMEOUT}s)" || ko "slow extensions lasted ${OUT}s"
[ "$(healthz)" = "200" ] && ok "/healthz answers once they are cut" || ko "/healthz after slow extensions"

echo "── clients that pipeline and never read do not stall the server"
python3 - "$PORT" <<'PY' &
import socket, sys, time
port = int(sys.argv[1]); socks = []
for _ in range(3):
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
    s.connect(("127.0.0.1", port))
    s.sendall(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n" * 30)  # the panel, 30 times, never read
    socks.append(s)
time.sleep(14)
PY
PIPE=$!
sleep 1
SLOWEST=0; CODES=""
for _ in 1 2 3 4 5 6; do
  OUT=$(curl -s -m 5 -o /dev/null -w '%{http_code} %{time_total}' "$H/healthz")
  CODES="$CODES ${OUT%% *}"
  SLOWEST=$(python3 -c "print(max($SLOWEST, ${OUT##* }))")
  sleep 0.5
done
# each holds one request at most: three of the four slots, the fourth is free
python3 -c "import sys; sys.exit(0 if $SLOWEST < 1 else 1)" && [ "${CODES# }" = "200 200 200 200 200 200" ] \
  && ok "/healthz answered 200 every time under 3 such clients, the slowest in ${SLOWEST}s" \
  || ko "/healthz under pipelining clients: ${CODES# }, slowest ${SLOWEST}s"
sleep 6  # 10 s from the start: past the write timeout twice, and its linger
LEFT=$(ss -Htn state established "( sport = :$PORT )" | wc -l)
[ "$LEFT" = "0" ] && ok "the server closed all 3 pipelining connections within 10s" \
  || ko "$LEFT server sockets still established after 10s"
wait $PIPE
[ "$(healthz)" = "200" ] && ok "/healthz is 200 again" || ko "/healthz after pipelining"

echo "── the in-flight 503 does not wait on a body"
# Four slow bodies hold the four slots; the next requests are shed by the
# dispatch loop, which must not read what their bodies would be.
python3 - "$PORT" <<'PY' &
import socket, sys, time
held = []
for _ in range(4):
    s = socket.create_connection(("127.0.0.1", int(sys.argv[1])))
    s.sendall(b"POST /api/echo HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: 2000\r\n\r\n")
    held.append(s)
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
