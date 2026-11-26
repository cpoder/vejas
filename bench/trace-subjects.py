"""What a subject per trace costs the NATS server (ADR-0034 point 7).

python3 bench/trace-subjects.py <per-trace|one-subject> <traces> <hops>
Starts its own nats-server, creates a stream, publishes <traces> x <hops>
hop records of about 400 bytes, and prints the server's memory at each step
and after a restart. Run both layouts and compare: the difference, per trace, is
what the subject index costs. NATS_BIN picks the server; the nats CLI is needed.
"""
import json, os, random, shutil, socket, subprocess, sys, tempfile, time

layout, traces, hops = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
port = 4391
store = tempfile.mkdtemp()
server = None

def start():
    global server
    server = subprocess.Popen([os.environ.get("NATS_BIN", "nats-server"), "-js", "-sd", store, "-a", "127.0.0.1", "-p", str(port)],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(100):
        try:
            socket.create_connection(("127.0.0.1", port)).close()
            return
        except OSError:
            time.sleep(0.05)

def rss_mb():
    for line in open(f"/proc/{server.pid}/status"):
        if line.startswith("VmRSS:"):
            return int(line.split()[1]) / 1024

def nats(*args):
    return subprocess.run(["nats", "-s", f"nats://127.0.0.1:{port}", *args], capture_output=True, text=True)

def connect():
    s = socket.create_connection(("127.0.0.1", port))
    s.recv(4096)
    s.sendall(b'CONNECT {"verbose":false,"pedantic":false}\r\n')
    return s

def flush(s):
    s.sendall(b"PING\r\n")
    buf = b""
    while b"PONG" not in buf:
        buf += s.recv(65536)

def stream_state():
    info = json.loads(nats("stream", "info", "TRC", "--json").stdout)
    st = info["state"]
    return st["messages"], st.get("num_subjects", 0), st["bytes"]

start()
nats("stream", "add", "TRC", "--subjects", "vxtr.>", "--storage", "file", "--retention", "limits",
     "--discard", "old", "--max-age", "24h", "--max-msgs=-1", "--max-bytes=-1", "--max-msg-size=-1",
     "--dupe-window", "2m", "--replicas", "1", "--defaults")
time.sleep(0.5)
print(f"{layout}: empty stream, server RSS {rss_mb():.1f} MB", flush=True)
record = {"unit": "flow:order_sync", "version": "d5768a2de6e8cbb0", "attempt": 1, "ok": True,
          "emits": ["vx.orders.validated"], "subject": "vx.orders.in", "seq": 0,
          "start_ns": 0, "end_ns": 0, "span_id": "", "parent": "", "answer": None,
          "preview": "x" * 160}
s = connect()
t0 = time.time()
step = max(1, traces // 4)
for t in range(traces):
    trace = "%032x" % random.getrandbits(128)
    subject = f"vxtr.{trace}" if layout == "per-trace" else "vxtr.all"
    out = []
    for h in range(hops):
        record["seq"] = t * hops + h
        body = json.dumps(record).encode()
        out.append(b"PUB %s %d\r\n%s\r\n" % (subject.encode(), len(body), body))
    s.sendall(b"".join(out))
    if (t + 1) % step == 0:
        flush(s)
        time.sleep(1)
        m, n, b = stream_state()
        print(f"  {t + 1:>7} traces: {m} msgs, {n} subjects, {b / 1e6:.0f} MB stored, server RSS {rss_mb():.1f} MB", flush=True)
print(f"  published in {time.time() - t0:.1f}s", flush=True)
server.terminate(); server.wait()
t1 = time.time()
start()
for _ in range(200):
    if nats("stream", "info", "TRC", "--json").returncode == 0:
        break
    time.sleep(0.1)
time.sleep(1)
m, n, b = stream_state()
print(f"  after restart ({time.time() - t1:.1f}s to answer): {m} msgs, {n} subjects, server RSS {rss_mb():.1f} MB", flush=True)
t2 = time.time()
r = nats("stream", "get", "TRC", "--last-for", f"vxtr.{trace}" if layout == "per-trace" else "vxtr.all", "--json")
print(f"  last hop of one trace by subject: {(time.time() - t2) * 1000:.0f} ms (incl. CLI start)", flush=True)
server.terminate(); server.wait()
shutil.rmtree(store)
