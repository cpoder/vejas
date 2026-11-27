# tiny_http 0.12.0, patched for Vejas

This is [tiny_http](https://github.com/tiny-http/tiny-http) 0.12.0 as published
on crates.io (upstream commit `212b1c45852fef2093dc1374875a9393c55eb4b9`), the
last release when it was vendored (2026-11-27). The runtime takes it through
`[patch.crates-io]` in `core/Cargo.toml`. It is MIT OR Apache-2.0, like
upstream: `LICENSE-MIT` and `LICENSE-APACHE` are upstream's, unchanged.

Only `src/`, the licenses and the changelog were copied: no benches, examples,
integration tests or dev-dependencies. The first commit that added this
directory holds the published source unchanged, so `git log -p` on it shows the
patch alone. Every changed place carries a `Vejas patch` comment.

## Why

Upstream 0.12.0 let one request take the runtime down or pin it:

- An unread body was drained on drop with `vec![0; remaining]`, one allocation
  of the whole declared `Content-Length`. `GET /healthz` with
  `Content-Length: 1099511627776` answered, then aborted the process.
- No socket timeout and no thread bound: a client sending one byte at a time
  (slowloris) kept a thread for good, as many times as it opened connections.
- A chunked body had no size bound, and was never drained: what the handler
  left unread was parsed as the next request.
- A request line or a header line had no length bound and the headers no count.
- One `accept()` error (`EMFILE`) stopped the accept loop for good, and a
  refused thread panicked it.

## What changed

- `Limits` (in `ServerConfig`, and `Server::from_listener_with_limits`): the
  largest body (default 8 MiB), one timeout (default 30 s) and the connections
  served at once (default 512).
- `ClientConnection` answers before the body is read, then closes: 413 for a
  declared `Content-Length` past the bound, 400 for one that is not one number
  (or two that differ), 431 for a line past 8 KiB or more than 100 headers, 408
  for a head that does not arrive within the timeout (idle keep-alive included).
- Every body read from the socket goes through `BodyGuard`
  (`src/util/body_guard.rs`): at most the bound (past it, the read fails with
  `BodyTooLarge`), within the timeout (past it, `TimedOut`). On drop it drains
  what the handler left unread into a fixed 8 KiB buffer, within the timeout
  and up to 64 KiB past the bound (so a 413 for a body a little over it is not
  lost to a reset). A body it does not read to its end closes the connection: the next
  request is never read from the middle of a body. `EqualReader` no longer
  drains on its own drop.
- `Request::respond_and_close`: answers, then closes the connection without
  draining. The runtime's accept loop sheds load with it.
- The socket gets the timeout as its read and write timeout, before the TLS
  handshake. Past `max_connections`, a new connection is closed as accepted.
- The task pool spawns with `thread::Builder` and drops the connection when the
  OS refuses a thread; the accept loop logs an `accept()` error and goes on.

A handler that reads a body can still be held for up to twice the timeout: the
body deadline is checked between reads, and a read waits at most the socket
timeout.

## Upgrading

If upstream releases a version with an equivalent fix, drop this directory and
the `[patch.crates-io]` entry, and keep the runtime's tests in
`core/src/main.rs` (`http_limits_tests`) and `e2e/http-limits.sh` green.
