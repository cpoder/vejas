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

  # ── standalone-binary recipe (ADR-0023 shape): env-file config, no .vjs ──
  # First-class connector binaries (IBM MQ) are configured by environment, not
  # by a runtime manifest. Same credential rule, same verdicts: a credential-
  # shaped env key must reference the deployment's secret machinery (the
  # ${VAR:?} required-env form), never carry an inline literal. No in-runtime
  # probe exists for a standalone binary, so a stated exception is REQUIRED.
  if [ ! -f "$MANIFEST" ] && [ -f "$DIR/$NAME.env.example" ]; then
    LINT=$(python3 - "$DIR/$NAME.env.example" "$PAT" << 'PY'
import re, sys
pat = sys.argv[2]
bad = []
for line in open(sys.argv[1]):
    m = re.match(r'\s*(?:export\s+)?([A-Z][A-Z0-9_]*)=(.*)$', line)
    if not m or not re.search(pat, m.group(1), re.I):
        continue
    val = m.group(2).split('#')[0].strip().strip('"').strip("'")
    if val and not re.match(r'^\$\{[A-Z0-9_]+(:\?[^}]*)?\}$', val):
        bad.append(m.group(1))
print(','.join(bad))
PY
)
    if [ -n "$LINT" ]; then echo "  ✗ lint: literal credential(s) in env file: $LINT"; fail=1; continue; fi
    if [ -f "$DIR/EXCEPTION.md" ]; then
      echo "  ✓ admitted (stated exception: $(head -1 "$DIR/EXCEPTION.md" | sed 's/^#* *//'))"
    else
      echo "  ✗ standalone-binary recipe: EXCEPTION.md required (no in-runtime probe exists)"
      fail=1
    fi
    continue
  fi

  # ── lint: credential-shaped keys must be secret() ─────────────────────
  LINT=$(python3 - "$MANIFEST" "$PAT" << 'PY'
import re, sys
pat = sys.argv[2]
bad = []
for line in open(sys.argv[1]):
    m = re.match(r'\s*([A-Z][A-Z0-9_]*)\s*=\s*(.+)$', line)
    if not m:
        continue
    key, val = m.group(1), m.group(2)
    if re.search(pat, key, re.I) and 'secret(' not in val:
        bad.append(key)
    # config-carrying doc literals: their SUB-keys must honor the rule too
    # (the panel masks them; the lint matches its scope). Test/sample bodies
    # are exempt — they carry fake data by definition.
    if val.lstrip().startswith('{') and 'BODY' not in key and 'PAYLOAD' not in key:
        for sk, sv in re.findall(r'["\']?([A-Za-z_][A-Za-z0-9_-]*)["\']?\s*:\s*([^,}]*)', val):
            if re.search(pat, sk, re.I) and 'secret(' not in sv:
                bad.append(f'{key}.{sk}')
print(','.join(bad))
PY
)
  if [ -n "$LINT" ]; then echo "  ✗ lint: literal credential(s): $LINT"; fail=1; continue; fi
  "$BIN" vjs-check "$MANIFEST" > /dev/null || { echo "  ✗ parse"; fail=1; continue; }

  # a certified dir ships COMPLETE in one commit: admission material or a
  # stated exception — a half-recipe fails with a verdict, not a traceback
  if [ ! -f "$DIR/overrides.json" ] && [ ! -f "$DIR/EXCEPTION.md" ]; then
    echo "  ✗ incomplete recipe: missing overrides.json (or EXCEPTION.md) — ship the dir admission-complete in one commit"
    fail=1; continue
  fi

  # ── stated exception (ADR-0017): remote not meaningfully mockable ─────
  # Lint + parse are still enforced above; the exception file says what was
  # verified instead, and its first line is printed so it is never silent.
  if [ -f "$DIR/EXCEPTION.md" ]; then
    echo "  ✓ admitted (stated exception: $(head -1 "$DIR/EXCEPTION.md" | sed 's/^#* *//'))"
    continue
  fi

  # ── stage: broker (real) + mock + throwaway root + dummy secrets ──────
  STORE=$(mktemp -d); ROOT=$(mktemp -d)
  BROKER_P=$((9400 + i))
  if [ -f "$DIR/broker.sh" ]; then
    "$DIR/broker.sh" start "$BROKER_P" > "$STORE/broker.log" 2>&1 \
      || { echo "  ✗ broker: failed to start ($(tail -1 "$STORE/broker.log"))"; fail=1; rm -rf "$STORE" "$ROOT"; continue; }
  fi
  mkdir -p "$ROOT/connectors"
  cp "$MANIFEST" "$ROOT/connectors/$NAME.vjs"
  MOCK_PID=""
  if [ -f "$DIR/mock.mjs" ]; then
    node "$DIR/mock.mjs" "$MOCK_P" > "$STORE/mock.log" 2>&1 &
    MOCK_PID=$!
    # never race the mock: a cold CI runner starts node slower than the probe
    for _ in $(seq 100); do curl -sf -o /dev/null "http://127.0.0.1:$MOCK_P/__count" && break; sleep 0.1; done
  fi
  # dummy secrets from overrides.json (env-store form)
  eval "$(python3 - "$DIR/overrides.json" "$MOCK_P" "$BROKER_P" << 'PY'
import json, re, sys
o = json.load(open(sys.argv[1]))
for path, v in o.get('secrets', {}).items():
    v = str(v).replace('{PORT}', sys.argv[2]).replace('{BROKER_PORT}', sys.argv[3] if len(sys.argv) > 3 else '')
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
  python3 - "$DIR/overrides.json" "$MOCK_P" "$NAME" "$HTTP_P" "$BROKER_P" << 'PY' || ok=0
import json, sys, urllib.request
o = json.load(open(sys.argv[1])); port, name, http = sys.argv[2], sys.argv[3], sys.argv[4]
broker = sys.argv[5] if len(sys.argv) > 5 else ''
for k, v in o.get('literals', {}).items():
    if isinstance(v, str): v = v.replace('{PORT}', port).replace('{BROKER_PORT}', broker)
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
    # a driver WITHOUT a probe is not a failed probe — the data-flow check
    # below is then the sole (and stronger) gate
    echo "$PROBE" | python3 -c "
import json, sys
d = json.load(sys.stdin)
sys.exit(0 if d.get('ok') or 'no test probe' in d.get('detail', '') else 1)" \
      || { echo "  ✗ probe: $PROBE"; ok=0; }
  fi

  # ── the data must flow ────────────────────────────────────────────────
  if [ $ok -eq 1 ] && [ -x "$DIR/dataflow.sh" ]; then
    # recipe-owned data-flow check (real-broker recipes): env carries the stage
    HTTP_P="$HTTP_P" NATS_P="$NATS_P" BROKER_P="$BROKER_P" DIR="$DIR" \
      "$DIR/dataflow.sh" > "$STORE/dataflow.log" 2>&1 \
      || { echo "  ✗ dataflow: $(tail -1 "$STORE/dataflow.log")"; ok=0; }
  elif [ $ok -eq 1 ]; then
    SUBJECT=$(grep -oP 'SUBJECT\s*=\s*"\K[^"]+' "$MANIFEST" || true)
    INGEST=$(python3 -c "import json,sys; print(json.load(open('$DIR/overrides.json')).get('ingest_path',''))")
    if [ -n "$INGEST" ]; then
      # webhook source: POST the fixture to its own ingest, expect it on the bus
      IPORT=$(grep -oP 'PORT\s*=\s*\K[0-9]+' "$MANIFEST")
      ( timeout 10 nats -s "nats://127.0.0.1:$NATS_P" sub "vx.$INGEST" --count=1 --raw 2>/dev/null | head -1 > "$STORE/inmsg" ) &
      WSUB=$!
      sleep 0.5
      curl -sf -o /dev/null -X POST "http://127.0.0.1:$IPORT/ingest/$INGEST" -d @"$DIR/fixture.json" || true
      wait $WSUB 2>/dev/null || true
      [ -s "$STORE/inmsg" ] || { echo "  ✗ webhook: fixture never reached the bus"; ok=0; }
    elif [ -f "$DIR/fixture.json" ] && grep -q 'driver "http-poll"' "$MANIFEST"; then
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
  [ -f "$DIR/broker.sh" ] && "$DIR/broker.sh" stop "$BROKER_P" 2>/dev/null
  rm -rf "$STORE" "$ROOT"
  if [ $ok -eq 1 ]; then echo "  ✓ admitted"; else fail=1; fi
done
exit $fail
