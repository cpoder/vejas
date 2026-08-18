"""http-in connector: HTTP webhook -> bus.

POST /ingest/<suffix> with a JSON body publishes it on `vx.<suffix>`.
Example: POST /ingest/stripe.events -> subject vx.stripe.events

A connector is just a process that talks NATS (see docs/SUBJECTS.md).
This one is Python because it ships with the runtime; it could be anything.
"""

import asyncio
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import nats
from vejas import ensure_stream, SUBJECT_ROOT, NATS_URL

PORT = int(os.environ.get("HTTP_IN_PORT", "8787"))

loop = asyncio.new_event_loop()
_js = None


async def _setup():
    global _js
    nc = await nats.connect(NATS_URL)
    _js = nc.jetstream()
    await ensure_stream(_js)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print(f"[http-in] {fmt % args}", flush=True)

    def _respond(self, code, payload):
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/healthz":
            self._respond(200, {"ok": True})
        else:
            self._respond(404, {"error": "not found"})

    def do_POST(self):
        if not self.path.startswith("/ingest/"):
            return self._respond(404, {"error": "POST /ingest/<subject-suffix>"})
        suffix = self.path[len("/ingest/"):].strip("/")
        if not suffix:
            return self._respond(400, {"error": "missing subject suffix"})
        subject = f"{SUBJECT_ROOT}.{suffix}"
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)
        try:
            json.loads(raw.decode())
        except Exception:
            return self._respond(400, {"error": "body must be JSON"})
        future = asyncio.run_coroutine_threadsafe(_js.publish(subject, raw), loop)
        try:
            future.result(timeout=5)
        except Exception as exc:
            return self._respond(502, {"error": f"publish failed: {exc}"})
        self._respond(202, {"published": subject})


if __name__ == "__main__":
    threading.Thread(target=loop.run_forever, daemon=True).start()
    asyncio.run_coroutine_threadsafe(_setup(), loop).result(timeout=10)
    print(f"[http-in] listening on :{PORT}, publishing under {SUBJECT_ROOT}.*", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
