#!/bin/bash
# http-in is an unauthenticated port with one OS thread per connection. A burst
# must cost 503s, never the connector: in v0.3.6 `thread::spawn` panicked when
# the OS refused a thread, the accept loop died with it and nothing restarted
# it, while /healthz said 200 and the topology said running, restarts 0,
# last_error null. Part 1 holds the bound. Part 2 takes the thread budget away
# for real, with a pids limit, and needs docker (skipped without it).
#   e2e/http-in-bound.sh
set -uo pipefail
cd "$(dirname "$0")/.."
BIN="$PWD/core/target/release/vejas-runtime"
[ -x "$BIN" ] || { echo "build first" >&2; exit 1; }
S=$(mktemp -d); R=$(mktemp -d); R2=$(mktemp -d)
CT=vejas-http-in-bound-$$
RT=""; NP=""
cleanup(){
  kill $RT $NP 2>/dev/null; wait $RT $NP 2>/dev/null
  command -v docker > /dev/null && docker rm -f "$CT" > /dev/null 2>&1
  rm -rf "$S" "$R" "$R2"
}
trap cleanup EXIT
fail=0
ok(){ echo "  ✓ $1"; }
ko(){ echo "  ✗ $1"; fail=1; }

NP_PORT=4279; URL=nats://127.0.0.1:$NP_PORT
IN=18789; H="http://127.0.0.1:8750"
hold(){ # port, connections, seconds: idle sockets, as a burst leaves them
  python3 -c '
import socket, sys, time
port, n, secs = int(sys.argv[1]), int(sys.argv[2]), float(sys.argv[3])
socks = [socket.create_connection(("127.0.0.1", port), timeout=5) for _ in range(n)]
time.sleep(secs)
' "$1" "$2" "$3" &
}
ingest(){ curl -s -m 3 -o /dev/null -w '%{http_code}' -X POST \
  -H 'Content-Type: application/json' -d '{"a":1}' "http://127.0.0.1:$1/ingest/t"; }
topology(){ # the unit as JSON: the MCP result carries it as an escaped string
  curl -s -m 5 -H 'Content-Type: application/json' \
    -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"vejas_topology","arguments":{}}}' "$1/mcp" \
    | python3 -c '
import json, sys
def units(v):
    if isinstance(v, dict):
        if "restarts" in v: yield v
        for x in v.values(): yield from units(x)
    elif isinstance(v, list):
        for x in v: yield from units(x)
doc = json.loads(json.load(sys.stdin)["result"]["content"][0]["text"])
print(json.dumps([u for u in units(doc) if "http_in" in u.get("name", "")]))'
}
threads(){ awk '/^Threads:/{print $2}' "/proc/$1/status"; }
wait_ingest(){ # port: until it answers 202
  for _ in $(seq 1 100); do [ "$(ingest "$1")" = "202" ] && return 0; sleep 0.1; done
  return 1
}

mkdir -p "$R/connectors"
printf '# connector: http_in\ndriver "http-in"\nPORT = %s\nMAX_CONNECTIONS = 20\n' $IN > "$R/connectors/http_in.vjs"
nats-server -js -sd "$S/n" -a 127.0.0.1 -p $NP_PORT > /dev/null 2>&1 & NP=$!
sleep 0.5
NATS_URL=$URL VEJAS_ROOT="$R" VEJAS_HTTP_ADDR=127.0.0.1:8750 "$BIN" > "$S/rt.log" 2>&1 & RT=$!
until curl -sf -o /dev/null "$H/healthz"; do sleep 0.1; done
wait_ingest $IN || ko "http-in never came up"

echo "── part 1: past MAX_CONNECTIONS a client gets a refusal, the thread count stays bounded"
BEFORE=$(threads $RT)
hold $IN 100 4; HOLD=$!
sleep 1.5
GROWTH=$(( $(threads $RT) - BEFORE ))
[ "$GROWTH" -le 22 ] && ok "100 connections, $GROWTH more threads (bound 20)" \
  || ko "$GROWTH more threads for a bound of 20"
C=$(ingest $IN)
case "$C" in
  503|000) ok "a request past the bound is refused ($C)" ;;
  *) ko "a request past the bound got $C" ;;
esac
wait $HOLD 2>/dev/null
wait_ingest $IN && ok "ingestion is back once the connections are gone" || ko "http-in did not recover"
METRICS=$(curl -s -m 5 "$H/metrics")
[[ "$METRICS" == *'vejas_http_rejected_total{unit="connector:http_in"}'* ]] \
  && ok "the refusals are on /metrics" || ko "no vejas_http_rejected_total on /metrics"
topology "$H" | grep -q 'running' && ok "the connector still reports running" || ko "the connector left running"

kill $RT 2>/dev/null; wait $RT 2>/dev/null; RT=""

echo "── part 2: the OS has no more threads to give"
if ! { command -v docker > /dev/null && docker info > /dev/null 2>&1; }; then
  echo "  - skipped: no docker"
else
  IN2=18790; H2="http://127.0.0.1:8751"
  mkdir -p "$R2/connectors"
  printf '# connector: http_in\ndriver "http-in"\nPORT = %s\n' $IN2 > "$R2/connectors/http_in.vjs"
  chmod -R a+rX "$R2"
  docker run -d --name "$CT" --network host --pids-limit=64 \
    -e NATS_URL=$URL -e VEJAS_ROOT=/vroot -e VEJAS_HTTP_ADDR=127.0.0.1:8751 \
    -v "$BIN":/usr/local/bin/vejas-runtime:ro -v "$R2":/vroot \
    debian:trixie-slim vejas-runtime > /dev/null
  for _ in $(seq 1 100); do curl -sf -o /dev/null "$H2/healthz" && break; sleep 0.1; done
  wait_ingest $IN2 || ko "http-in never came up in the container"
  hold $IN2 300 3; HOLD=$!
  sleep 1
  wait $HOLD 2>/dev/null
  wait_ingest $IN2 && ok "http-in survives 300 connections on a pids limit of 64" \
    || ko "http-in died on the pids limit"
  P=$(docker logs "$CT" 2>&1 | grep -c panicked)
  [ "$P" = "0" ] && ok "no panic in the log" || ko "$P panic(s) in the log"
  W=$(docker exec "$CT" sh -c 'cat /proc/1/task/*/comm' | grep -c version-watch)
  [ "$W" -ge 1 ] && ok "the version watch is alive" || ko "the version watch died"
  UNIT=$(topology "$H2")
  echo "$UNIT" | grep -q '"restarts": 0' && ok "no restart was needed" || ko "the connector restarted: $UNIT"
fi

[ $fail -eq 0 ] && echo "HTTP-IN BOUND HOLDS" || echo "HTTP-IN BOUND REGRESSION — do not ship"
exit $fail
