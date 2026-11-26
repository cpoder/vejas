"""A collector that accepts OTLP/HTTP JSON, keeps nothing, and counts spans.

    python3 bench/otlp-discard.py <port> <count-file>

bench/trace-cost.sh points the runtime at it, so the cost measured is the
runtime's, not a real collector's.
"""
import http.server
import sys
import threading

port, count_file = int(sys.argv[1]), sys.argv[2]
lock = threading.Lock()
spans = 0


class Discard(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        global spans
        body = self.rfile.read(int(self.headers.get("content-length") or 0))
        with lock:
            spans += body.count(b'"spanId"')
            with open(count_file, "w") as f:
                f.write(str(spans))
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.end_headers()
        self.wfile.write(b"{}")

    def log_message(self, *args):
        pass


http.server.ThreadingHTTPServer(("127.0.0.1", port), Discard).serve_forever()
