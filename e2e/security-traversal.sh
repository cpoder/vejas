#!/bin/bash
# Regression test for the path-traversal class (ADR-0029 R7, finding F1).
# guard_path once blocked ".." but not SYMLINKS: a link flows/x.vjs ->
# /etc/passwd passed the check and GET /file (unauthenticated — the Bearer
# gate is POST-only) read it in the clear; a link to the secrets file
# exfiltrated every secret. Fixed by canonicalizing the resolved path under
# root.canonicalize() (83dfc34). This guards the invariant forever: every
# escape refused, every legitimate in-root access still works.
#   e2e/security-traversal.sh          (needs the nats CLI)
set -uo pipefail
cd "$(dirname "$0")/.."
BIN="core/target/release/vejas-runtime"
[ -x "$BIN" ] || { echo "build first" >&2; exit 1; }
S=$(mktemp -d); R=$(mktemp -d)
trap 'kill $RT $NP 2>/dev/null; wait $RT $NP 2>/dev/null; rm -rf "$S" "$R"' EXIT
fail=0
ok(){ echo "  ✓ $1"; }
ko(){ echo "  ✗ $1"; fail=1; }

mkdir -p "$R/flows"
echo 'source "vx.x"' > "$R/flows/ok.vjs"
printf 'SUPER_SECRET_HOST_CONTENT\n' > "$S/outside-secret.txt"
# the attack plants: symlinks that pass the old check (in flows/, .vjs/.json,
# no "..") but resolve OUTSIDE root
ln -s /etc/passwd            "$R/flows/passwd.vjs"
ln -s "$S/outside-secret.txt" "$R/flows/exfil.json"
# a symlink that stays WITHIN root must remain allowed (not over-blocked)
echo 'source "vx.within"' > "$R/flows/real.vjs"
ln -s "$R/flows/real.vjs"    "$R/flows/link-within.vjs"

nats-server -js -sd "$S/n" -a 127.0.0.1 -p 4275 > /dev/null 2>&1 & NP=$!
sleep 0.5
NATS_URL=nats://127.0.0.1:4275 VEJAS_ROOT="$R" \
  VEJAS_HTTP_ADDR=127.0.0.1:8746 "$BIN" > "$S/rt.log" 2>&1 & RT=$!
until curl -sf -o /dev/null http://127.0.0.1:8746/healthz; do sleep 0.1; done

echo "── path traversal via symlink (F1 regression)"
CODE=$(curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:8746/file?path=flows/passwd.vjs")
[ "$CODE" = "400" ] && ok "symlink → /etc/passwd refused ($CODE)" || ko "symlink → /etc/passwd NOT refused ($CODE)"
BODY=$(curl -s "http://127.0.0.1:8746/file?path=flows/exfil.json")
echo "$BODY" | grep -q "SUPER_SECRET_HOST_CONTENT" && ko "symlink → out-of-root secret LEAKED" || ok "symlink → out-of-root secret refused"

echo "── classic traversal still refused"
for p in "../../../etc/passwd" "/etc/passwd" "%2e%2e/x.json" "flows/../../etc/passwd"; do
  CODE=$(curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:8746/file?path=$p")
  [ "$CODE" = "400" ] && ok "refused: $p" || ko "NOT refused: $p ($CODE)"
done

echo "── legitimate in-root access still works (no over-blocking)"
curl -sf "http://127.0.0.1:8746/file?path=flows/ok.vjs" | grep -q 'vx.x' \
  && ok "real file reads" || ko "real file blocked (over-correction)"
curl -sf "http://127.0.0.1:8746/file?path=flows/link-within.vjs" | grep -q 'vx.within' \
  && ok "symlink WITHIN root reads (allowed)" || ko "in-root symlink over-blocked"

[ $fail -eq 0 ] && echo "TRAVERSAL CONTAINED" || echo "TRAVERSAL REGRESSION — do not ship"
exit $fail
