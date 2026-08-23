#!/bin/bash
# Paced end-to-end latency (webhook -> flow -> sink) at a FIXED event rate —
# the latency numbers bench/run.sh cannot give (its loadgen runs flat-out, so
# past the ingest fix every run saturates the pipeline and measures queue
# depth, not path latency). Two published rows come from here:
#   uncongested:  bench/paced.sh 20 15      (~20/s, far below capacity)
#   sustained:    bench/paced.sh 2000 15    (steady load below the sink bound)
set -euo pipefail
cd "$(dirname "$0")/.."

RATE="${1:-20}"
SECS="${2:-15}"
BIN="core/target/release/vejas-runtime"
STORE=$(mktemp -d)
trap 'kill $NATS_PID $RUNTIME_PID $SINK_PID 2>/dev/null || true; rm -rf "$STORE"' EXIT
[ -x "$BIN" ] || { echo "build first" >&2; exit 1; }

nats-server -js -sd "$STORE" -a 127.0.0.1 -p 4224 > /dev/null 2>&1 &
NATS_PID=$!
sleep 0.5
node bench/sink-counter.mjs > /dev/null 2>&1 &
SINK_PID=$!
NATS_URL="nats://127.0.0.1:4224" VEJAS_ROOT=bench/root \
  VEJAS_HTTP_ADDR=127.0.0.1:8689 "$BIN" > "$STORE/runtime.log" 2>&1 &
RUNTIME_PID=$!
until curl -sf -o /dev/null http://127.0.0.1:8689/healthz; do sleep 0.05; done
until curl -sf -o /dev/null -X POST -d '{"warmup":1,"t":1}' \
  http://127.0.0.1:8790/ingest/bench.orders; do sleep 0.2; done
sleep 2
curl -sf http://127.0.0.1:9099/reset > /dev/null

python3 - "$RATE" "$SECS" << 'PY'
import http.client, json, math, sys, threading, time
rate, secs = int(sys.argv[1]), int(sys.argv[2])
total = rate * secs
# spread the rate over enough keep-alive connections that pacing, not the
# connection, is the limit (~500/s per connection is comfortable)
workers = max(1, math.ceil(rate / 500))
per, sent = total // workers, 0
def worker(n):
    c = http.client.HTTPConnection("127.0.0.1", 8790)
    interval = workers / rate
    t_next = time.perf_counter()
    for i in range(n):
        body = json.dumps({"id": f"SO#{i}", "email": "Jane.Doe@ACME.com",
            "total_price": "347.00", "t": int(time.time() * 1000),
            "shipping_address": {"country": "France"},
            "line_items": [{"sku": "A-12", "quantity": "2", "unit_price_cents": 9900}]})
        c.request("POST", "/ingest/bench.orders", body,
                  {"Content-Type": "application/json"})
        c.getresponse().read()
        t_next += interval
        d = t_next - time.perf_counter()
        if d > 0: time.sleep(d)
ts = [threading.Thread(target=worker, args=(per,)) for _ in range(workers)]
t0 = time.time()
for t in ts: t.start()
for t in ts: t.join()
print(json.dumps({"paced_rate_per_s": rate, "sent": per * workers,
                  "actual_rate_per_s": round(per * workers / (time.time() - t0))}))
PY
sleep 3
STATS=$(curl -sf http://127.0.0.1:9099/stats)
echo "$STATS"
