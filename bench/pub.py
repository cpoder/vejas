#!/usr/bin/env python3
"""Publish generated events onto NATS, fast, with no client library.

The NATS protocol is a line and a payload, so a socket and a buffer are the
whole publisher. The `nats` CLI cannot vary a payload beyond a counter, and a
correlation benchmark needs three things it cannot express: a bounded pool of
keys (a fleet has a finite number of machines), an event time that advances
(so `.within()` windows actually close and state is released), and the two
steps of a pair carrying the same key.

  bench/pub.py --url ... --pairs 8000 --hosts 500 --net <subj> --proc <subj>
  bench/pub.py --url ... --orders 64000 --subject <subj>     (the flow's twin)

`--lag N` is the knob that matters for a correlation: the second step of a
pair is published N pairs after its first, so N runs are open at once. Lag 1
opens and closes immediately; lag 10 000 carries ten thousand half-matched
sequences, which is what a rule with a slow second step really does.

Prints the milliseconds it took, so the caller can tell publishing apart from
processing.
"""

import argparse
import json
import socket
import time
from urllib.parse import urlparse


def connect(url: str) -> socket.socket:
    u = urlparse(url)
    s = socket.create_connection((u.hostname or "127.0.0.1", u.port or 4222))
    s.recv(4096)  # INFO
    s.sendall(b'CONNECT {"verbose":false,"pedantic":false,"name":"bench/pub.py"}\r\n')
    return s


def frame(subject: str, payload: dict) -> bytes:
    body = json.dumps(payload, separators=(",", ":")).encode()
    return b"PUB " + subject.encode() + b" " + str(len(body)).encode() + b"\r\n" + body + b"\r\n"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--url", default="nats://127.0.0.1:4222")
    p.add_argument("--pairs", type=int, default=0)
    p.add_argument("--orders", type=int, default=0, help="publish the flow fixture N times instead")
    p.add_argument("--subject", default="", help="subject for --orders")
    p.add_argument("--lag", type=int, default=1, help="how many pairs stay open at once")
    p.add_argument("--hosts", type=int, default=500, help="live key pool")
    p.add_argument("--net", default="")
    p.add_argument("--proc", default="")
    p.add_argument("--step-ms", type=int, default=1, help="event time between pairs")
    p.add_argument("--gap-ms", type=int, default=0,
                   help="event time inside a pair; 0 derives it from the lag, so a pair's "
                        "two steps stay inside the rule's window however many runs are open")
    p.add_argument("--interleaved", action="store_true",
                   help="publish both steps as they come, instead of every first step then every second")
    args = p.parse_args()

    if args.orders:
        import pathlib
        fixture = json.loads(pathlib.Path("bench/root/flows/fixtures/bench_orders.json").read_text())
        stream = [frame(args.subject, fixture)] * args.orders
        publish(args.url, stream)
        return

    base = 1600000000000
    # The second step of a pair arrives `lag` pairs later, so in event time it
    # is `lag * step` behind. Deriving the gap from that keeps every pair
    # inside `.within()` at any lag: the only thing the lag changes is how many
    # correlations are open at once, which is the point of the knob.
    gap = args.gap_ms or max(1, args.lag) * args.step_ms
    net, proc = [], []
    for i in range(args.pairs):
        host = f"h{i % args.hosts}"
        t = base + i * args.step_ms
        net.append(frame(args.net, {
            "event_type": "SysmonNetworkConnect", "ts": t, "t": t,
            "Image": "C:\\tools\\svc.exe", "DestinationIp": "10.0.0.20",
            "DestinationPort": 445, "Hostname": host}))
        proc.append(frame(args.proc, {
            "event_type": "SysmonProcessCreate", "ts": t + gap, "t": t + gap,
            "Image": "C:\\Windows\\System32\\cmd.exe",
            "ParentImage": "C:\\Windows\\System32\\services.exe", "Hostname": host}))

    stream = []
    if args.interleaved:
        lag = max(1, args.lag)
        for i, a in enumerate(net):
            stream.append(a)
            if i >= lag - 1:
                stream.append(proc[i - lag + 1])
        stream.extend(proc[len(net) - lag + 1:])
    else:
        stream = net + proc

    publish(args.url, stream)


def publish(url: str, stream: list) -> None:
    sock = connect(url)
    t0 = time.monotonic()
    buf = bytearray()
    for msg in stream:
        buf += msg
        if len(buf) >= 256 * 1024:
            sock.sendall(buf)
            buf.clear()
    if buf:
        sock.sendall(buf)
    sock.sendall(b"PING\r\n")          # the server answers once it has them all
    while b"PONG" not in sock.recv(4096):
        pass
    print(int((time.monotonic() - t0) * 1000))
    sock.close()


if __name__ == "__main__":
    main()
