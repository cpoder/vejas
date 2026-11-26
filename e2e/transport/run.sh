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
#   T6 one trace       an event is one trace from http-in to the last sink:
#                      same trace id at every hop, parents chained, the
#                      caller's traceparent honoured (http-in and an api
#                      flow), the sink's HTTP call carries it on, a caller's
#                      unsampled flag is followed, no span dropped, a
#                      graceful stop flushes the last spans (ADR-0034,
#                      against a stub collector)
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
NATS_PID=""; RT_PID=""; STUB_PID=""
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
  [ -n "$STUB_PID" ] && kill "$STUB_PID" 2>/dev/null
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

# ── T6 — one trace per event, end to end (ADR-0034) ──────────────────────────
echo "── T6 one trace per event"
COL_P=8712; IN_P=8713
T6="$WORK/t6"; mkdir -p "$T6/root/flows" "$T6/root/services" "$T6/root/connectors"
: > "$T6/received"
# The stub collector and HTTP sink target, in one listener: every POST lands
# in $T6/received as {path, traceparent, body}.
cat > "$T6/stub.py" <<'PY'
import http.server, json, sys
port, out = int(sys.argv[1]), sys.argv[2]
class Stub(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("content-length") or 0))
        with open(out, "a") as f:
            f.write(json.dumps({"path": self.path, "traceparent": self.headers.get("traceparent"),
                                "body": body.decode("utf-8", "replace")}) + "\n")
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.end_headers()
        self.wfile.write(b"{}")
    def log_message(self, *args):
        pass
http.server.ThreadingHTTPServer(("127.0.0.1", port), Stub).serve_forever()
PY
python3 "$T6/stub.py" "$COL_P" "$T6/received" & STUB_PID=$!
cat > "$T6/root/connectors/t6_in.vjs" <<VJS
# connector: t6_in
driver "http-in"
PORT = $IN_P
VJS
cat > "$T6/root/connectors/t6_out.vjs" <<VJS
# connector: t6_out
driver "http-out"
SUBJECT = "vxt.t6.out"
URL = "http://127.0.0.1:$COL_P/sink"
VJS
cat > "$T6/root/services/t6_tag.vjs" <<'VJS'
# service: t6_tag
tag = f"tagged {order}"
VJS
cat > "$T6/root/flows/t6_first.vjs" <<'VJS'
source "vxt.t6.in"
invoke t6_tag(order: event.order)
emit "vxt.t6.mid", { order: event.order, tag: tag }
VJS
cat > "$T6/root/flows/t6_second.vjs" <<'VJS'
source "vxt.t6.mid"
emit "vxt.t6.out", { order: event.order }
VJS
cat > "$T6/root/flows/t6_api.vjs" <<'VJS'
api "POST /t6"
emit "vxt.t6.mid", { order: event.order }
respond 202, { accepted: event.order }
VJS
VEJAS_ROOT="$T6/root" NATS_URL="$URL" VEJAS_STREAM=TTEST VEJAS_SUBJECT_ROOT=vxt \
VEJAS_HTTP_ADDR="127.0.0.1:$HTTP_P" VEJAS_ACK_WAIT_SECS=1 \
OTEL_EXPORTER_OTLP_ENDPOINT="http://127.0.0.1:$COL_P" OTEL_SERVICE_NAME=vejas-t6 \
OTEL_BSP_SCHEDULE_DELAY=3000 \
  "$BIN" >>"$WORK/rt.log" 2>&1 & RT_PID=$!
# The 3 s schedule delay outlasts a graceful stop (about 1 s): case E below
# then proves the stop flushes, since no scheduled export can send its spans.
wait_health || bad "T6 runtime did not start"
for _ in $(seq 1 100); do   # t6_first, t6_second, t6_in, t6_out
  n=$(curl -s "http://127.0.0.1:$HTTP_P/topology" | grep -o '"status":"running"' | wc -l)
  [ "$n" -ge 4 ] && break; sleep 0.1
done
for _ in $(seq 1 50); do curl -sf "http://127.0.0.1:$IN_P/healthz" >/dev/null && break; sleep 0.1; done

TA=4bf92f3577b34da6a3ce929d0e0e4736; PA=00f067aa0ba902b7
TC=0af7651916cd43dd8448eb211c80319c; PC=b7ad6b7169203331
TD=1bf92f3577b34da6a3ce929d0e0e4737; PD=10f067aa0ba902b8
RA=$(curl -s -X POST -H "traceparent: 00-$TA-$PA-01" -d '{"order":"A-1"}' "http://127.0.0.1:$IN_P/ingest/t6.in")
RB=$(curl -s -X POST -d '{"order":"B-2"}' "http://127.0.0.1:$IN_P/ingest/t6.in")
# D: the caller sampled its trace out (flag 00); the default sampler follows it.
curl -s -X POST -H "traceparent: 00-$TD-$PD-00" -d '{"order":"D-4"}' "http://127.0.0.1:$IN_P/ingest/t6.in" >/dev/null
curl -s -X POST -H "TraceParent: 00-$TC-$PC-01" -H 'content-type: application/json' \
  -d '{"order":"C-3"}' "http://127.0.0.1:$HTTP_P/api/t6" >/dev/null
TB=$(printf '%s' "$RB" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("trace_id",""))' 2>/dev/null)
case "$RA" in *"\"trace_id\":\"$TA\""*) pass "http-in answers with the trace id it continued";;
  *) bad "http-in answer lacks the caller's trace id: $RA";; esac
[ ${#TB} -eq 32 ] && pass "http-in starts a trace when the caller sends none" || bad "no trace id for a fresh request: $RB"

# Every span the three events produce, checked once the last ones are in.
cat > "$T6/check.py" <<'PY'
import json, sys
received, ta, pa, tb, tc, pc, td = sys.argv[1:8]
spans, sinks = [], []
for line in open(received):
    rec = json.loads(line)
    if rec["path"] == "/v1/traces":
        for rs in json.loads(rec["body"])["resourceSpans"]:
            for ss in rs["scopeSpans"]:
                spans += ss["spans"]
    elif rec["path"] == "/sink":
        sinks.append(rec)
def unit(s):
    return next((a["value"]["stringValue"] for a in s["attributes"] if a["key"] == "vejas.unit"), "")
def chain(trace, root_parent, units):
    of = [s for s in spans if s["traceId"] == trace]
    by = {}
    for s in of:
        by.setdefault((unit(s), s["name"].startswith("invoke ")), s)
    need = [(u, False) for u in units] + [("flow:t6_first", True)] * ("flow:t6_first" in units)
    missing = [u for u in need if u not in by]
    if missing:
        return f"missing {missing} among {sorted(by)}"
    hops = [by[(u, False)] for u in units]
    if hops[0].get("parentSpanId", "") != root_parent:
        return f"{units[0]} hangs under {hops[0].get('parentSpanId')!r}, not {root_parent!r}"
    for up, down in zip(hops, hops[1:]):
        if down.get("parentSpanId") != up["spanId"]:
            return f"{unit(down)} does not hang under {unit(up)}"
    if "flow:t6_first" in units and by[("flow:t6_first", True)].get("parentSpanId") != by[("flow:t6_first", False)]["spanId"]:
        return "the invoke span does not hang under its flow"
    sink = hops[-1]
    if not any(r["traceparent"] == f"00-{trace}-{sink['spanId']}-01" for r in sinks):
        return "the sink's HTTP call does not carry the sink span as traceparent"
    return ""
def sampled_out(trace):
    if any(s["traceId"] == trace for s in spans):
        return "spans exported for a trace its caller sampled out"
    if not any((r["traceparent"] or "").startswith(f"00-{trace}-") and r["traceparent"].endswith("-00") for r in sinks):
        return "the sink's HTTP call lost the trace or its unsampled flag"
    return ""
done = sys.argv[8] in ("final", "stop")
if sys.argv[8] == "stop":   # E only: the spans a graceful stop had to flush
    te, pe = sys.argv[9:11]
    print("E", chain(te, pe, ["connector:t6_in", "flow:t6_first", "flow:t6_second", "connector:t6_out"]) or "ok")
    sys.exit(0)
errors = {
    "A": chain(ta, pa, ["connector:t6_in", "flow:t6_first", "flow:t6_second", "connector:t6_out"]),
    "B": chain(tb, "", ["connector:t6_in", "flow:t6_first", "flow:t6_second", "connector:t6_out"]),
    "C": chain(tc, pc, ["flow:t6_api", "flow:t6_second", "connector:t6_out"]),
    "D": sampled_out(td),
}
if any(errors.values()) and not done:
    sys.exit(1)
for k, e in errors.items():
    print(f"{k} {e or 'ok'}")
PY
for _ in $(seq 1 150); do
  python3 "$T6/check.py" "$T6/received" "$TA" "$PA" "$TB" "$TC" "$PC" "$TD" wait >/dev/null 2>&1 && break
  sleep 0.1
done
VERDICT=$(python3 "$T6/check.py" "$T6/received" "$TA" "$PA" "$TB" "$TC" "$PC" "$TD" final 2>&1)
for case_ in A B C D; do
  case "$case_" in
    A) what="http-in with a caller's traceparent: one trace, parents chained to the sink's HTTP call";;
    B) what="http-in without one: one trace, parents chained to the sink's HTTP call";;
    C) what="an api flow with a caller's traceparent: one trace, parents chained to the sink's HTTP call";;
    D) what="a trace the caller sampled out: no span exported, the flag carried to the sink's call";;
  esac
  verdict=$(printf '%s\n' "$VERDICT" | sed -n "s/^$case_ //p")
  [ "$verdict" = "ok" ] && pass "$what" || bad "$what: ${verdict:-no verdict: $VERDICT}"
done
M=$(curl -s "http://127.0.0.1:$HTTP_P/metrics")
if printf '%s' "$M" | grep -q 'vejas_spans_dropped_total{reason="queue_full"} 0' \
   && printf '%s' "$M" | grep -q 'vejas_spans_dropped_total{reason="export_failed"} 0'; then
  pass "no span dropped"
else
  bad "spans dropped: $(printf '%s' "$M" | grep vejas_spans_dropped_total | tr '\n' ' ')"
fi
if curl -s "http://127.0.0.1:$HTTP_P/events?flow=flow:t6_second" | grep -q "\"trace_id\":\"$TA\""; then
  pass "the event ring shows the trace id"
else
  bad "the event ring has no trace id for event A"
fi
# E: stop the runtime as soon as the sink has delivered an event. Its spans
# are still in the exporter's batch; only the flush on stop can send them.
TE=2cf92f3577b34da6a3ce929d0e0e4739; PE=30f067aa0ba902ba
curl -s -X POST -H "traceparent: 00-$TE-$PE-01" -d '{"order":"E-5"}' "http://127.0.0.1:$IN_P/ingest/t6.in" >/dev/null
for _ in $(seq 1 50); do grep -q "00-$TE-" "$T6/received" && break; sleep 0.1; done
kill -TERM "$RT_PID" 2>/dev/null
for _ in $(seq 1 80); do kill -0 "$RT_PID" 2>/dev/null || break; sleep 0.1; done
verdict=$(python3 "$T6/check.py" "$T6/received" "$TA" "$PA" "$TB" "$TC" "$PC" "$TD" stop "$TE" "$PE" 2>&1 | sed -n 's/^E //p')
[ "$verdict" = "ok" ] && pass "a graceful stop exports the spans of the last event" \
  || bad "a graceful stop lost spans: ${verdict:-no verdict}"
kill "$STUB_PID" 2>/dev/null

echo
if [ "$fail" -eq 0 ]; then echo "transport: all invariants hold ✓"; else echo "transport: $fail FAILED"; fi
exit $((fail > 0 ? 1 : 0))
