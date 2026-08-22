#!/bin/bash
# The connector admission job (ADR-0017). For every certified-recipe
# directory under docs/examples/connectors/: lint the credential rule, boot
# a throwaway nats+runtime on recipe-disjoint ports, point the manifest at
# the recipe's mock THROUGH the product's own write path (/surface/set),
# require a green probe, and require the data to actually flow (a published
# message for sources, a received call for sinks). No real credentials,
# ever. ~3s per recipe.
#   e2e/admission/run.sh [recipe-name ...]     (default: all directories)
set -uo pipefail
cd "$(dirname "$0")/../.."

BIN="core/target/release/vejas-runtime"
RECIPES_DIR="docs/examples/connectors"
[ -x "$BIN" ] || { echo "build first: cargo build --release --manifest-path core/Cargo.toml" >&2; exit 1; }

# the credential pattern — single-sourced from the runtime when available
PAT=$(timeout 2 "$BIN" secret-pattern 2>/dev/null) || PAT='pass(wd|word)|secret|token|api[_-]?key'

RECIPES=("$@")
[ ${#RECIPES[@]} -eq 0 ] && RECIPES=($(find "$RECIPES_DIR" -mindepth 1 -maxdepth 1 -type d -printf '%f\n' | sort))

fail=0
i=0
for NAME in "${RECIPES[@]}"; do
  i=$((i+1))
  DIR="$RECIPES_DIR/$NAME"
  MANIFEST="$DIR/$NAME.vjs.example"
  NATS_P=$((4300 + i)); HTTP_P=$((8700 + i)); MOCK_P=$((9200 + i))
  echo "── $NAME"

  # ── lint: credential-shaped keys must be secret() ─────────────────────
  LINT=$(python3 - "$MANIFEST" "$PAT" << 'PY'
import re, sys
bad = []
for line in open(sys.argv[1]):
    m = re.match(r'\s*([A-Z][A-Z0-9_]*)\s*=\s*(.+)$', line)
    if m and re.search(sys.argv[2], m.group(1), re.I) and 'secret(' not in m.group(2):
        bad.append(m.group(1))
print(','.join(bad))
PY
)
  if [ -n "$LINT" ]; then echo "  ✗ lint: literal credential(s): $LINT"; fail=1; continue; fi
  "$BIN" vjs-check "$MANIFEST" > /dev/null || { echo "  ✗ parse"; fail=1; continue; }

  # ── stage: mock + throwaway root + dummy secrets ──────────────────────
  STORE=$(mktemp -d); ROOT=$(mktemp -d)
  mkdir -p "$ROOT/connectors"
  cp "$MANIFEST" "$ROOT/connectors/$NAME.vjs"
  node "$DIR/mock.mjs" "$MOCK_P" > "$STORE/mock.log" 2>&1 &
  MOCK_PID=$!
  # dummy secrets from overrides.json (env-store form)
  eval "$(python3 - "$DIR/overrides.json" << 'PY'
import json, re, sys
o = json.load(open(sys.argv[1]))
for path, v in o.get('secrets', {}).items():
    print(f"export VEJAS_SECRET_{re.sub(r'[^A-Za-z0-9]', '_', path).upper()}='{v}'")
PY
)"
  nats-server -js -sd "$STORE" -a 127.0.0.1 -p "$NATS_P" > /dev/null 2>&1 &
  NATS_PID=$!
  NATS_URL="nats://127.0.0.1:$NATS_P" VEJAS_ROOT="$ROOT" \
    VEJAS_HTTP_ADDR="127.0.0.1:$HTTP_P" "$BIN" > "$STORE/runtime.log" 2>&1 &
  RT_PID=$!
  ok=1
  for _ in $(seq 100); do curl -sf -o /dev/null "http://127.0.0.1:$HTTP_P/healthz" && break; sleep 0.1; done

  # ── point the manifest at the mock through the product's write path ───
  python3 - "$DIR/overrides.json" "$MOCK_P" "$NAME" "$HTTP_P" << 'PY' || ok=0
import json, sys, urllib.request
o = json.load(open(sys.argv[1])); port, name, http = sys.argv[2], sys.argv[3], sys.argv[4]
for k, v in o.get('literals', {}).items():
    if isinstance(v, str): v = v.replace('{PORT}', port)
    body = json.dumps({"file": f"connectors/{name}.vjs", "name": k, "key": "-", "value": v}).encode()
    r = urllib.request.urlopen(urllib.request.Request(
        f"http://127.0.0.1:{http}/surface/set", body, {'content-type': 'application/json'}), timeout=10)
    assert r.status == 200, (k, r.status)
PY
  sleep 2   # targeted restart picks the new literals

  # ── probe ─────────────────────────────────────────────────────────────
  if [ $ok -eq 1 ]; then
    PROBE=$(curl -sf -X POST "http://127.0.0.1:$HTTP_P/connectors/test" \
      -H 'content-type: application/json' -d "{\"file\":\"connectors/$NAME.vjs\"}")
    echo "$PROBE" | python3 -c "import json,sys; d=json.load(sys.stdin); sys.exit(0 if d.get('ok') else 1)" \
      || { echo "  ✗ probe: $PROBE"; ok=0; }
  fi

  # ── the data must flow ────────────────────────────────────────────────
  if [ $ok -eq 1 ]; then
    SUBJECT=$(grep -oP 'SUBJECT\s*=\s*"\K[^"]+' "$MANIFEST")
    if [ -f "$DIR/fixture.json" ] && grep -q 'driver "http-poll"' "$MANIFEST"; then
      # source: one real published message, shape-checked against the fixture
      MSG=$(timeout 15 nats -s "nats://127.0.0.1:$NATS_P" sub "$SUBJECT" --count=1 --raw 2>/dev/null | head -1)
      python3 - "$DIR/fixture.json" "$MSG" << 'PY' || { echo "  ✗ source: shape mismatch or no message"; exit 1; } || ok=0
import json, sys
want = set(json.load(open(sys.argv[1])).keys())
got = set(json.loads(sys.argv[2]).keys())
assert want == got, (want, got)
PY
    else
      # sink: publish the fixture, the mock must receive one more call
      BEFORE=$(curl -sf "http://127.0.0.1:$MOCK_P/__count")
      nats -s "nats://127.0.0.1:$NATS_P" pub "$SUBJECT" "$(cat "$DIR/fixture.json")" > /dev/null 2>&1
      DELIVERED=0
      for _ in $(seq 50); do
        AFTER=$(curl -sf "http://127.0.0.1:$MOCK_P/__count"); [ "$AFTER" -gt "$BEFORE" ] && { DELIVERED=1; break; }; sleep 0.2
      done
      [ $DELIVERED -eq 1 ] || { echo "  ✗ sink: fixture never reached the mock"; ok=0; }
    fi
  fi

  kill "$RT_PID" "$NATS_PID" "$MOCK_PID" 2>/dev/null
  rm -rf "$STORE" "$ROOT"
  if [ $ok -eq 1 ]; then echo "  ✓ admitted"; else fail=1; fi
done
exit $fail
