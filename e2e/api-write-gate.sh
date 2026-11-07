#!/bin/bash
# Regression test for the write-gate class (ADR-0029 R7, Finding A). The Bearer
# gate (VEJAS_TOKEN) once keyed on the POST verb only, but the /api flow surface
# runs a flow for ANY verb and publishes its emits to the bus. So an
# `api "DELETE /x"` (a mutating verb) and an emitting `api "GET /x"` (a bus write)
# both executed WITHOUT the token — the very control meant to gate mutation.
# Fixed by gating every non-read method, plus any read-method flow that emits
# (detected statically via emit_subjects). This guards the invariant: a write
# needs the token whatever verb carries it; a pure read stays open.
#   e2e/api-write-gate.sh
set -uo pipefail
cd "$(dirname "$0")/.."
BIN="core/target/release/vejas-runtime"
[ -x "$BIN" ] || { echo "build first" >&2; exit 1; }
S=$(mktemp -d); R=$(mktemp -d)
trap 'kill $RT $NP 2>/dev/null; wait $RT $NP 2>/dev/null; rm -rf "$S" "$R"' EXIT
fail=0
ok(){ echo "  ✓ $1"; }
ko(){ echo "  ✗ $1"; fail=1; }

TOK="test-write-tok-$$"
H="http://127.0.0.1:8747"

mkdir -p "$R/flows"
# a mutating REST verb (DELETE) — a write, whatever the body
printf 'api "DELETE /orders/{id}"\nrespond 200, {deleted: id}\n'      > "$R/flows/del.vjs"
# a read-method flow that EMITS to the bus — a write dressed as a GET
printf 'api "GET /audit"\nemit "vx.audit.read", {at: "x"}\nrespond 200, {ok: true}\n' > "$R/flows/audit.vjs"
# a read-method flow that emits to a DYNAMICALLY computed subject (a lowercase
# local, invisible to emit_subjects) — still a bus write, must be gated (A')
printf 'api "GET /leak"\ns = "vx.leak.evt"\nemit s, {stolen: 1}\nrespond 200, {ok: true}\n' > "$R/flows/dyn.vjs"
# a read-method flow whose bus write is INDIRECT — via a service invoke (the
# service emits). No direct emit in the flow, so emit_subjects/has_emit miss it;
# must still be gated (A''). The service is real so the with-token run succeeds.
mkdir -p "$R/services"
printf 'emit "vx.notified", {got: m}\n'                              > "$R/services/notify.vjs"
printf 'api "GET /viainvoke"\ninvoke notify(m: 1)\nrespond 200, {ok: true}\n' > "$R/flows/inv.vjs"
# a pure read: respond only, no emit — must stay OPEN
printf 'api "GET /read"\nrespond 200, {ok: true}\n'                   > "$R/flows/read.vjs"

nats-server -js -sd "$S/n" -a 127.0.0.1 -p 4276 > /dev/null 2>&1 & NP=$!
sleep 0.5
NATS_URL=nats://127.0.0.1:4276 VEJAS_ROOT="$R" VEJAS_TOKEN="$TOK" \
  VEJAS_HTTP_ADDR=127.0.0.1:8747 "$BIN" > "$S/rt.log" 2>&1 & RT=$!
until curl -sf -o /dev/null "$H/healthz"; do sleep 0.1; done

code(){ curl -s -o /dev/null -w "%{http_code}" "$@"; }

echo "── mutating verb (DELETE) is gated like POST"
C=$(code -X DELETE "$H/api/orders/1")
[ "$C" = "401" ] && ok "DELETE without token refused ($C)" || ko "DELETE without token NOT refused ($C)"
C=$(code -X DELETE -H "Authorization: Bearer $TOK" "$H/api/orders/1")
[ "$C" = "200" ] && ok "DELETE with token allowed ($C)" || ko "DELETE with token blocked ($C)"

echo "── a read-method flow that EMITS is a bus write → gated"
C=$(code "$H/api/audit")
[ "$C" = "401" ] && ok "emitting GET without token refused ($C)" || ko "emitting GET without token NOT refused ($C)"
C=$(code -H "Authorization: Bearer $TOK" "$H/api/audit")
[ "$C" = "200" ] && ok "emitting GET with token allowed ($C)" || ko "emitting GET with token blocked ($C)"

echo "── a read-method flow that emits to a DYNAMIC subject is still gated (A')"
C=$(code "$H/api/leak")
[ "$C" = "401" ] && ok "dynamic-subject emitting GET without token refused ($C)" || ko "dynamic-subject emitting GET NOT refused ($C) — bus write slips the gate"
C=$(code -H "Authorization: Bearer $TOK" "$H/api/leak")
[ "$C" = "200" ] && ok "dynamic-subject emitting GET with token allowed ($C)" || ko "dynamic-subject emitting GET with token blocked ($C)"

echo "── a read-method flow that writes the bus via a service INVOKE is gated (A'')"
C=$(code "$H/api/viainvoke")
[ "$C" = "401" ] && ok "invoke-mediated bus write without token refused ($C)" || ko "invoke-mediated bus write NOT refused ($C) — indirect write slips the gate"
C=$(code -H "Authorization: Bearer $TOK" "$H/api/viainvoke")
[ "$C" = "200" ] && ok "invoke-mediated write with token allowed ($C)" || ko "invoke-mediated write with token blocked ($C)"

echo "── a pure read (respond-only, no emit) stays OPEN"
C=$(code "$H/api/read")
[ "$C" = "200" ] && ok "pure-read GET open without token ($C)" || ko "pure-read GET over-blocked ($C)"
C=$(code "$H/healthz")
[ "$C" = "200" ] && ok "built-in read (/healthz) open ($C)" || ko "built-in read blocked ($C)"

echo "── fail-closed: an unknown/exotic verb is gated BEFORE routing"
C=$(code -X PURGE "$H/api/read")
[ "$C" = "401" ] && ok "exotic verb (PURGE) without token → 401 (fail-closed)" || ko "exotic verb NOT gated ($C)"

echo "── HEAD never executes a flow → no bus write via HEAD"
# HEAD does not match a GET route, so the emitting flow never runs (404, not an
# unauthenticated emit). More restrictive than strict HTTP, deliberately so.
C=$(code -I "$H/api/audit")
[ "$C" = "404" ] && ok "HEAD on GET route → 404, flow not executed (no emit)" || ko "HEAD on GET route unexpected ($C)"

echo "── the listen address: loopback unless told otherwise, and loud about no token"
kill $RT 2>/dev/null; wait $RT 2>/dev/null
boot(){ # log, then the env for this run
  L=$1; shift
  env "$@" NATS_URL=nats://127.0.0.1:4276 VEJAS_ROOT="$R" "$BIN" > "$L" 2>&1 & RT=$!
  for _ in $(seq 1 100); do grep -q 'panel + monitoring' "$L" 2>/dev/null && break; sleep 0.1; done
}
boot "$S/rt-default.log" -u VEJAS_HTTP_ADDR -u VEJAS_TOKEN
curl -sf -o /dev/null http://127.0.0.1:8686/healthz && ok "no VEJAS_HTTP_ADDR: answers on 127.0.0.1:8686" || ko "no VEJAS_HTTP_ADDR: nothing on 127.0.0.1:8686"
BOUND=$(ss -ltnH 'sport = :8686' | awk '{print $4}')
[ "$BOUND" = "127.0.0.1:8686" ] && ok "bound to loopback only ($BOUND)" || ko "bound to '$BOUND'"
grep -q WARNING "$S/rt-default.log" && ko "warned on loopback" || ok "no warning on loopback"
kill $RT 2>/dev/null; wait $RT 2>/dev/null
boot "$S/rt-open.log" -u VEJAS_TOKEN VEJAS_HTTP_ADDR=0.0.0.0:8747
grep -q 'WARNING: 0.0.0.0:8747 with no VEJAS_TOKEN' "$S/rt-open.log" && ok "0.0.0.0 with no token warns" || ko "0.0.0.0 with no token is silent"
kill $RT 2>/dev/null; wait $RT 2>/dev/null
boot "$S/rt-token.log" VEJAS_TOKEN="$TOK" VEJAS_HTTP_ADDR=0.0.0.0:8747
grep -q WARNING "$S/rt-token.log" && ko "warned despite the token" || ok "0.0.0.0 with a token does not warn"

[ $fail -eq 0 ] && echo "WRITE GATE HOLDS" || echo "WRITE GATE REGRESSION — do not ship"
exit $fail
