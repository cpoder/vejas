#!/bin/bash
# Transport-level invariant tests (ROADMAP Phase 4) — beyond the language golden
# cases. Each boots a throwaway nats(-js)+runtime on disjoint ports against a
# purpose-built flow, drives one scenario, and asserts a transport contract:
#
#   T1 ordering        per-subject FIFO is preserved end to end
#   T2 redelivery+DLQ  a poison message redelivers, is capped at MAX_DELIVERIES,
#                      lands in the DLQ, and its source is acked (not stuck)
#   T3 no-loss         kill -9 the runtime mid-stream; every event still arrives
#                      after restart (at-least-once, publish-before-ack)
#   T4 reconnection    kill + restart nats; the flow reconnects and resumes
#   T5 anti-zombie     SIGTERM while idle exits promptly (no blocking pull)
#
# Redelivery latency is set low (VEJAS_ACK_WAIT_SECS=1) so the whole suite runs
# in seconds. No real credentials, no shared state; teardown by captured PID.
#   e2e/transport/run.sh
set -uo pipefail
cd "$(dirname "$0")/../.."

# Pick the NEWER of release/debug (a stale binary of the other kind silently
# using the wrong ack-wait was a real footgun); print which, so it is never a
# mystery. CI builds --release fresh, so release wins there.
REL="core/target/release/vejas-runtime"; DBG="core/target/debug/vejas-runtime"
if [ -x "$REL" ] && { [ ! -x "$DBG" ] || [ "$REL" -nt "$DBG" ]; }; then BIN="$REL"; else BIN="$DBG"; fi
NATSD="$(command -v nats-server || echo "$HOME/.local/bin/nats-server")"
NATS="$(command -v nats || echo "$HOME/.local/bin/nats")"
[ -x "$BIN" ] || { echo "build first: cargo build --manifest-path core/Cargo.toml" >&2; exit 1; }
[ -x "$NATSD" ] || { echo "need nats-server on PATH or ~/.local/bin" >&2; exit 1; }

NATS_P=4310; HTTP_P=8710
URL="nats://127.0.0.1:$NATS_P"
WORK="$(mktemp -d)"
ROOT="$WORK/root"; mkdir -p "$ROOT/flows"
NATS_PID=""; RT_PID=""
fail=0

cat > "$ROOT/flows/echo.vjs" <<'VJS'
source "vxt.in"
event = event
emit "vxt.out", { seq: event.seq, id: event.id }
VJS

# A flow that PARSES its input but errors at run time on every delivery (reads a
# field of a scalar) — a *transient*-looking failure, so it exercises the
# redelivery-then-cap path, unlike bad JSON which is permanent poison.
cat > "$ROOT/flows/strict.vjs" <<'VJS'
source "vxt.strict"
event = event
deep = event.a.b
emit "vxt.out", { deep: deep }
VJS

nats_up() { "$NATSD" -js -p "$NATS_P" -sd "$WORK/nats" >"$WORK/nats.log" 2>&1 & NATS_PID=$!; }
rt_up() {
  VEJAS_ROOT="$ROOT" NATS_URL="$URL" VEJAS_STREAM=TTEST VEJAS_SUBJECT_ROOT=vxt \
  VEJAS_HTTP_ADDR="127.0.0.1:$HTTP_P" VEJAS_ACK_WAIT_SECS=1 \
    "$BIN" >>"$WORK/rt.log" 2>&1 & RT_PID=$!
}
wait_health() {  # poll /healthz instead of a fixed sleep
  for _ in $(seq 1 100); do
    curl -sf "http://127.0.0.1:$HTTP_P/healthz" >/dev/null 2>&1 && return 0
    sleep 0.1
  done
  echo "  runtime did not become healthy" >&2; return 1
}
teardown() {
  [ -n "$RT_PID" ] && kill "$RT_PID" 2>/dev/null
  [ -n "$NATS_PID" ] && kill "$NATS_PID" 2>/dev/null
  wait 2>/dev/null
  rm -rf "$WORK"
}
trap teardown EXIT
pass() { echo "  ✓ $1"; }
bad()  { echo "  ✗ $1"; fail=$((fail+1)); }

pub()  { "$NATS" -s "$URL" pub "$1" "$2" >/dev/null 2>&1; }

echo "== transport invariants == ($BIN)"
nats_up; sleep 1; rt_up; wait_health || exit 1
# /healthz is up before the flows' consumers are bound; wait for BOTH flows to
# report running so the first publishes are actually consumed (not just buffered).
for _ in $(seq 1 100); do
  n=$(curl -s "http://127.0.0.1:$HTTP_P/topology" | grep -o '"status":"running"' | wc -l)
  [ "$n" -ge 2 ] && break; sleep 0.1
done

# ── T1 — per-subject ordering preserved end to end ────────────────────────────
echo "── T1 ordering"
timeout 15 "$NATS" -s "$URL" sub 'vxt.out' --count=20 >"$WORK/t1.out" 2>/dev/null & SUB=$!
sleep 0.5
for i in $(seq 1 20); do pub 'vxt.in' "{\"seq\":$i,\"id\":\"o$i\"}"; done
wait $SUB 2>/dev/null
GOT=$(grep -oE '"seq":[0-9]+' "$WORK/t1.out" | grep -oE '[0-9]+' | tr '\n' ' ')
WANT=$(seq 1 20 | tr '\n' ' ')
if [ "$GOT" = "$WANT" ]; then pass "20 events arrive in publish order"; else bad "order mismatch: $GOT"; fi

# ── T2 — redelivery caps at MAX_DELIVERIES → DLQ; permanent poison is immediate ─
echo "── T2 redelivery + poison→DLQ cap"
# a run-time error redelivers (ack_wait) and is capped at MAX_DELIVERIES=5
pub 'vxt.strict' '{"a":5}'                # event.a.b: reading .b of a number errors every time
# bad JSON is permanent poison: dead-lettered on first delivery, never retried
pub 'vxt.in' 'this is not json'
# (a) redelivery — the trace ring records every failed attempt; ≥2 proves the
# message came back rather than being dropped on first failure (fast signal).
REDELIV=0
for _ in $(seq 1 30); do
  REDELIV=$(curl -s "http://127.0.0.1:$HTTP_P/events?flow=flow:strict" | python3 -c 'import sys,json
try: d=json.load(sys.stdin)["events"]
except: d=[]
print(sum(1 for e in d if e.get("ok") is False))' 2>/dev/null)
  [ "${REDELIV:-0}" -ge 2 ] && break; sleep 0.5
done
[ "${REDELIV:-0}" -ge 2 ] && pass "run-time error redelivered ($REDELIV attempts seen, at-least-once)" || bad "no redelivery: only ${REDELIV:-0} attempt(s)"
# (b) the cap — it eventually lands in the DLQ at MAX_DELIVERIES, not forever
dlq_json() { curl -s "http://127.0.0.1:$HTTP_P/dlq"; }
for _ in $(seq 1 40); do
  dlq_json | python3 -c 'import sys,json,os
d=json.load(sys.stdin)["dead_letters"]
s=[e for e in d if e.get("unit")=="flow:strict"]
os._exit(0 if s and s[0].get("attempts",0)>=5 else 1)' 2>/dev/null && break
  sleep 1
done
dlq_json | python3 -c '
import sys,json
d=json.load(sys.stdin)["dead_letters"]
by={e.get("unit"): e for e in d}
s=by.get("flow:strict"); e=by.get("flow:echo")
ok=True
if s and s.get("attempts",0)>=5: print("  ✓ capped at %d deliveries → DLQ (not infinite)"%s["attempts"])
else: print("  ✗ strict cap: %r (want attempts≥5)"%(s and s.get("attempts"))); ok=False
if e and e.get("attempts",0)==1: print("  ✓ bad JSON dead-lettered on first delivery (permanent poison, not retried)")
else: print("  ✗ echo/bad-json: %r (want attempts=1)"%(e and e.get("attempts"))); ok=False
sys.exit(0 if ok else 1)' || fail=$((fail+1))

# ── T3 — no loss when the runtime is killed -9 mid-stream ──────────────────────
echo "── T3 no-loss under kill -9 + restart"
timeout 20 "$NATS" -s "$URL" sub 'vxt.out' >"$WORK/t3.out" 2>/dev/null & SUB=$!   # spans the kill
sleep 0.5
for i in $(seq 100 174); do pub 'vxt.in' "{\"seq\":$i,\"id\":\"k$i\"}"; done  # 75 events
sleep 0.2
kill -9 "$RT_PID" 2>/dev/null                # crash mid-consume
sleep 1
rt_up; wait_health || exit 1                 # restart: un-acked redeliver
sleep 4
kill $SUB 2>/dev/null; wait $SUB 2>/dev/null
DISTINCT=$(grep -oE '"seq":1[0-7][0-9]' "$WORK/t3.out" | sort -u | wc -l)
if [ "$DISTINCT" -eq 75 ]; then pass "all 75 events survived the crash (distinct seq=75, at-least-once)"; else bad "only $DISTINCT/75 distinct events after restart"; fi

# ── T4 — reconnection after nats restart ──────────────────────────────────────
echo "── T4 reconnection"
kill "$NATS_PID" 2>/dev/null; wait "$NATS_PID" 2>/dev/null
sleep 1
nats_up; sleep 2                              # same store dir → stream/consumer persist
timeout 20 "$NATS" -s "$URL" sub 'vxt.out' --count=1 >"$WORK/t4.out" 2>/dev/null & SUB=$!
# the flow supervisor reconnects on its own; give it room, then publish
for _ in $(seq 1 15); do
  pub 'vxt.in' '{"seq":900,"id":"reconn"}'
  sleep 1
  grep -q '"seq":900' "$WORK/t4.out" && break
done
wait $SUB 2>/dev/null
if grep -q '"seq":900' "$WORK/t4.out"; then pass "flow reconnected and resumed after nats restart"; else bad "no emit after nats restart (reconnect failed)"; fi

# ── T5 — anti-zombie: SIGTERM while idle exits promptly ───────────────────────
echo "── T5 anti-zombie shutdown"
# runtime is idle now (no traffic); time SIGTERM → exit
if ! kill -0 "$RT_PID" 2>/dev/null; then bad "runtime not alive before T5 — invalid test"; fi
START=$(date +%s.%N)
kill -TERM "$RT_PID" 2>/dev/null
for _ in $(seq 1 50); do kill -0 "$RT_PID" 2>/dev/null || break; sleep 0.1; done
END=$(date +%s.%N)
if kill -0 "$RT_PID" 2>/dev/null; then
  bad "runtime still alive 5s after SIGTERM (blocking pull / zombie)"
else
  EL=$(awk "BEGIN{printf \"%.2f\", $END-$START}")
  ok=$(awk "BEGIN{print ($END-$START < 3.0)?1:0}")
  [ "$ok" = "1" ] && pass "idle SIGTERM → exit in ${EL}s (<3s, loop re-checks stop)" || bad "slow shutdown: ${EL}s"
fi
RT_PID=""   # already down

echo
if [ "$fail" -eq 0 ]; then echo "transport: all invariants hold ✓"; else echo "transport: $fail FAILED"; fi
exit $((fail > 0 ? 1 : 0))
