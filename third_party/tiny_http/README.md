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
  left unread was parsed as the next request. The decoder it used
  (chunked_transfer 1.5) read a chunk-size line into memory with no bound and
  no clock: an endless line of `1`s grew memory for as long as it came.
- A connection parsed pipelined requests ahead with no bound while its
  responses waited on one another: one client that pipelined and never read
  filled every handler slot.
- A request line or a header line had no length bound and the headers no count.
- One `accept()` error (`EMFILE`) stopped the accept loop for good, and a
  refused thread panicked it.

## What changed

- `Limits` (in `ServerConfig`, and `Server::from_listener_with_limits`): the
  largest body (default 8 MiB), one timeout (default 30 s) and the connections
  served at once (default 512).
- `ClientConnection` answers before the body is read, then closes: 413 for a
  declared `Content-Length` past the bound, 400 for one that is not one number
  (or two that differ), for a `Content-Length` with a `Transfer-Encoding`, or for
  a line that ends in a bare LF, 501 for a `Transfer-Encoding` other than
  `chunked` (`identity` before it is allowed), 431 for a line past 8 KiB or more
  than 100 headers, 408 for a head that does not arrive within the timeout (idle
  keep-alive included). After a 400, 413, 431 or 501 it reads and throws away
  up to 64 KiB, for at most 2 s and until the client pauses 250 ms, so the
  answer is not lost to a reset while the client is still sending.
- One deadline per request (`Clock`, `src/util/refined_tcp_stream.rs`): the
  timeout from the first byte of its head, for the head and the body together.
  Every read from the socket waits at most until then (the socket timeout is set
  to what is left before each read), the body's reads and its drain included, so
  a body never takes more than the timeout. Before the first byte, the connection
  may idle for the timeout. A socket timeout (`WouldBlock` on Linux) reads as
  `TimedOut`. Over TLS only the plain socket timeouts apply.
- A response may take the timeout, plus one second per 16 KiB it has written,
  up to its flush; each write waits at most the timeout. A response that could
  not be written ends the connection.
- No request head is read before the previous response on the connection is
  written: a connection has at most one request in flight.
- A chunked body is read by `ChunkedReader` (`src/util/chunked.rs`), in place of
  chunked_transfer's decoder: each line (size and extensions, each trailer) at
  most 4 KiB (a size of at most 16 hex digits past its leading zeros), the trailers 8 KiB, CRLF only, the deadline checked before every
  read, and every wire byte (sizes, extensions, CRLFs, data, trailers) counted
  against the bound plus the drain's 64 KiB grace; the data alone is held to the
  bound.
- Every body read from the socket goes through `BodyGuard`
  (`src/util/body_guard.rs`): at most the bound (past it, the read fails with
  `BodyTooLarge`), within the timeout (past it, `TimedOut`). On drop it drains
  what the handler left unread into a fixed 8 KiB buffer, within the timeout
  and up to 64 KiB past the bound (so a 413 for a body a little over it is not
  lost to a reset). A body it does not read to its end closes the connection: the next
  request is never read from the middle of a body. `EqualReader` no longer
  drains on its own drop.
- `Request::respond_and_close`: answers, then closes the connection without
  draining. `Request::close_without_response`: closes it unanswered (a dropped
  request answers 500). The runtime answers its 503 with the first on a thread
  of its own, and closes a request that gets no thread with the second, so its
  dispatch loop never writes.
- The socket gets the timeout as its read and write timeout, before the TLS
  handshake. Past `max_connections`, a new connection is closed as accepted.
- The task pool spawns with `thread::Builder` and drops the connection when the
  OS refuses a thread; the accept loop logs an `accept()` error and goes on.

A request holds a handler for at most the timeout to read its body, and its
response for the timeout plus its write allowance. The timeout bounds the whole
request, head and body: at 30 s, a body of 8 MiB needs about 280 KB/s.

A request line in HTTP/2.0 or 3.0 is answered 505 through the request itself,
and the connection closes. Upstream wrote the 505 with the writer after the
request's own, while the request lived, so that write waited for good and held
the connection's thread and slot. As a backstop, a response's writer now waits
at most the timeout for the one before it, then fails with `TimedOut`.

## Upgrading

If upstream releases a version with an equivalent fix, drop this directory and
the `[patch.crates-io]` entry, and keep the runtime's tests in
`core/src/main.rs` (`http_limits_tests`) and `e2e/http-limits.sh` green.
