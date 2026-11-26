//! http-in: the webhook source. POST /ingest/<suffix> publishes the body on
//! vx.<suffix>, signed or not (ADR-0037).

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::bus;
use crate::connectors::clock::epoch_secs;
use crate::connectors::hops::source_publish;
use crate::connectors::slot::Slot;
use crate::connectors::{Ctx, Driver};
use crate::metrics::Kind;
use crate::trace;

pub(super) struct HttpIn;
impl Driver for HttpIn {
    fn kind(&self) -> &'static str {
        "source:webhook"
    }
    fn about(&self) -> &'static str {
        "HTTP webhook: POST /ingest/<suffix> publishes the JSON body on vx.<suffix>. Config: PORT, MAX_CONNECTIONS (default 512, past it a 503), ALLOW (optional list of permitted subject suffixes/prefixes — without ALLOW any vx.* subject is reachable), SIGNATURE (optional: github, stripe, slack, shopify, standard-webhooks, gitlab, hmac-sha256 or token) with SIGNING_SECRET = secret(\"…\") — each request must then carry its sender's signature or gets 401, and SIGNATURE_TOLERANCE_SECS (default 300) bounds a signed timestamp. Without SIGNATURE the port is unauthenticated."
    }
    fn run(&self, ctx: &Ctx) -> Result<(), String> {
        let port = ctx.config.u64_or("PORT", 8787) as u16;
        // One OS thread per connection on an unauthenticated port: bounded, so a
        // burst costs a 503 rather than the thread budget of the whole runtime.
        let max_connections = ctx.config.u64_or("MAX_CONNECTIONS", 512).max(1) as usize;
        let open = Arc::new(AtomicUsize::new(0));
        let mut refused = 0u64;
        let js = ctx.jetstream()?;
        // Optional ALLOW: the subjects this unauthenticated surface may publish to.
        // Absent = any suffix (compat); set = only these (defence in depth, ADR-0029
        // R7 F2 — recommended when the port is reachable by anyone but the operator).
        let allow: Arc<Vec<String>> = Arc::new(
            ctx.config
                .value("ALLOW")
                .and_then(|v| v.as_array().cloned())
                .map(|a| a.into_iter().filter_map(|x| x.as_str().map(String::from)).collect())
                .unwrap_or_default(),
        );
        // Optional SIGNATURE (ADR-0037): each request must carry its sender's
        // signature. A manifest that asks for one the runtime cannot check stops
        // here, before the port opens.
        let signature: Option<Arc<crate::signature::Verifier>> =
            crate::signature::Verifier::from_config(&ctx.config)?.map(Arc::new);
        let listener = TcpListener::bind(("0.0.0.0", port)).map_err(|e| e.to_string())?;
        listener.set_nonblocking(true).ok();
        eprintln!(
            "[{}] http-in on :{port}, publishing under {}.*{}{}",
            ctx.name,
            ctx.subj_root,
            if allow.is_empty() { String::new() } else { format!(" (ALLOW {:?})", allow) },
            signature
                .as_ref()
                .map(|v| format!(", {} signature required", v.scheme()))
                .unwrap_or_default()
        );
        for s in listener.incoming() {
            if !ctx.alive() {
                break;
            }
            match s {
                Ok(mut sock) => {
                    let Some(slot) = Slot::try_take(&open, max_connections) else {
                        crate::metrics::inc_rejected(&ctx.name);
                        refused += 1;
                        if refused.is_power_of_two() {
                            eprintln!(
                                "[{}] {refused} connection(s) refused so far: MAX_CONNECTIONS is {max_connections}",
                                ctx.name
                            );
                        }
                        refuse(sock);
                        continue;
                    };
                    let js = js.clone();
                    let root = ctx.subj_root.clone();
                    let name = ctx.name.clone();
                    let allow = allow.clone();
                    let signature = signature.clone();
                    // `thread::spawn` panics when the OS has no thread to give (a
                    // pids limit, memory), which here killed the connector and left
                    // it reported as running. The closure drops with the error,
                    // which closes this one connection and frees its slot.
                    let spawned = thread::Builder::new().name(format!("{name}:conn")).spawn(
                        move || {
                            let _slot = slot;
                            handle_http_in(&name, &mut sock, &js, &root, &allow, signature.as_deref())
                        },
                    );
                    if let Err(e) = spawned {
                        crate::metrics::inc_rejected(&ctx.name);
                        refused += 1;
                        if refused.is_power_of_two() {
                            eprintln!("[{}] {refused} connection(s) refused so far: {e}", ctx.name);
                        }
                        thread::sleep(Duration::from_millis(20));
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // Poll responsively (stop is re-checked each turn) without a
                    // busy spin; keep-alive connections (below) reuse a socket, so
                    // this only bounds NEW-connection accept latency.
                    thread::sleep(Duration::from_millis(5));
                }
                Err(e) => eprintln!("[{}] accept: {e}", ctx.name),
            }
        }
        Ok(())
    }
}

/// A request suffix is permitted if it exactly matches an ALLOW entry or sits
/// under it as a subject-segment prefix (entry "shop" allows "shop.orders").
fn suffix_allowed(suffix: &str, allow: &[String]) -> bool {
    allow
        .iter()
        .any(|e| suffix == e || suffix.starts_with(&format!("{e}.")))
}

/// Past the bound: tell the client, without a thread, then close.
fn refuse(mut sock: TcpStream) {
    sock.set_write_timeout(Some(Duration::from_millis(200))).ok();
    let _ = sock.write_all(
        b"HTTP/1.1 503 Service Unavailable\r\nRetry-After: 1\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
    );
}

/// How long one request may take to arrive. The 5 s read timeout restarts on
/// every byte, so without this a client trickling bytes keeps its slot for good.
const REQUEST_READ_LIMIT: Duration = Duration::from_secs(30);

/// The largest request `http-in` takes, head and body: past it, 413 and the
/// connection closes, rather than a body cut short that would fail its JSON or
/// its signature with a reason that is not the real one.
const MAX_REQUEST_BYTES: usize = 1_048_576;

fn handle_http_in(
    name: &str,
    sock: &mut std::net::TcpStream,
    js: &bus::JetStream,
    subj_root: &str,
    allow: &[String],
    signature: Option<&crate::signature::Verifier>,
) {
    // TCP_NODELAY: without it, the small response write + the client's delayed
    // ACK collide with Nagle for a ~40ms stall on every keep-alive request.
    sock.set_nodelay(true).ok();
    // The 5s read timeout doubles as the keep-alive idle-session end.
    sock.set_read_timeout(Some(Duration::from_secs(5))).ok();
    // Keep-alive: serve successive requests on the same socket (HTTP/1.1) until
    // the client closes or idles out — so a keep-alive client reuses one socket
    // instead of reconnecting per request. Each iteration reads its OWN request
    // (and its own Content-Length).
    loop {
        let mut buf = Vec::new();
        let mut tmp = [0u8; 4096];
        let mut header_end = None;
        let mut framing = Framing::Unframed;
        let mut eof = false;
        let deadline = Instant::now() + REQUEST_READ_LIMIT;
        loop {
            if Instant::now() >= deadline {
                eof = true;
                break;
            }
            match sock.read(&mut tmp) {
                Ok(0) => {
                    eof = true;
                    break;
                }
                Ok(n) => {
                    buf.extend_from_slice(&tmp[..n]);
                    if header_end.is_none() {
                        // Only the bytes just read, and the three before them, can
                        // complete the blank line that ends the head.
                        let from = buf.len().saturating_sub(n + 3);
                        if let Some(pos) = buf[from..].windows(4).position(|w| w == b"\r\n\r\n") {
                            let he = from + pos + 4;
                            header_end = Some(he);
                            framing = Framing::of(&String::from_utf8_lossy(&buf[..he]));
                        }
                    }
                    if let Some(he) = header_end {
                        // A request declared past the bound is not read any further,
                        // and one whose length is not known is taken as it came.
                        let Framing::Length(len) = framing else { break };
                        let end = he.saturating_add(len);
                        if buf.len() >= end || end > MAX_REQUEST_BYTES {
                            break;
                        }
                    }
                    if buf.len() > MAX_REQUEST_BYTES {
                        break;
                    }
                }
                Err(_) => {
                    eof = true;
                    break;
                }
            }
        }
        let Some(he) = header_end else {
            break; // no complete request (client closed or idled out)
        };
        // The head is decoded on its own: an index into the lossy text of the
        // whole buffer could fall inside a character.
        let head = String::from_utf8_lossy(&buf[..he]);
        // Saturating: a forged Content-Length must not wrap. Without one, the body
        // is what came with the head, as http-in always took it.
        let end = match framing {
            Framing::Length(len) => he.saturating_add(len),
            Framing::Unframed | Framing::Chunked | Framing::BadLength => buf.len(),
        };
        let too_big = end > MAX_REQUEST_BYTES;
        // Where this request ends on the wire is not known, or not all of it was
        // read: what follows is not a next request, and the connection closes.
        let close = too_big || matches!(framing, Framing::Chunked | Framing::BadLength);
        let (code, json): (&str, String) = {
            let mut parts = head.split("\r\n").next().unwrap_or("").split(' ');
            let method = parts.next().unwrap_or("");
            let path = parts.next().unwrap_or("");
            // The body is what Content-Length says: a signature covers those
            // bytes, and nothing read past them.
            let body = &buf[he..end.min(buf.len())];
            if method == "GET" && path == "/healthz" {
                ("200 OK", "{\"ok\":true}".to_string())
            } else if method != "POST" || !path.starts_with("/ingest/") {
                ("404 Not Found", "{\"error\":\"POST /ingest/<subject-suffix>\"}".to_string())
            } else if framing == Framing::Chunked {
                (
                    "411 Length Required",
                    "{\"error\":\"send the body with a Content-Length: a chunked body is not read here\"}".to_string(),
                )
            } else if framing == Framing::BadLength {
                ("400 Bad Request", "{\"error\":\"Content-Length is not one number\"}".to_string())
            } else if too_big {
                (
                    "413 Payload Too Large",
                    format!("{{\"error\":\"a request is at most {MAX_REQUEST_BYTES} bytes, head and body\"}}"),
                )
            } else if let Some((verifier, refusal)) = signature.and_then(|v| {
                // Before anything else is read from the request, so a caller without
                // the secret learns nothing of the subjects ALLOW lets through.
                v.check(|name| header_value(&head, name), body, epoch_secs())
                    .err()
                    .map(|refusal| (v, refusal))
            }) {
                let refused = verifier.note_refusal();
                crate::metrics::inc_signature_failure(name, refusal.reason());
                if refused.is_power_of_two() {
                    eprintln!(
                        "[{name}] {refused} request(s) refused so far for their signature, the last because {refusal}"
                    );
                }
                (
                    "401 Unauthorized",
                    serde_json::json!({ "error": format!("signature: {refusal}") }).to_string(),
                )
            } else {
                let suffix = path.trim_start_matches("/ingest/").trim_matches('/');
                if suffix.is_empty() {
                    ("400 Bad Request", "{\"error\":\"missing subject suffix\"}".to_string())
                } else if !crate::vjs::valid_subject(suffix) {
                    ("400 Bad Request", "{\"error\":\"invalid subject suffix\"}".to_string())
                } else {
                    match serde_json::from_slice::<Value>(body) {
                        Err(_) => ("400 Bad Request", "{\"error\":\"body must be JSON\"}".to_string()),
                        // ALLOW set: this unauthenticated surface may only publish to the
                        // declared subjects (defence in depth — without ALLOW a caller can
                        // inject any vx.* event, a SINK subject included; ADR-0029 R7 F2).
                        Ok(_) if !allow.is_empty() && !suffix_allowed(suffix, allow) => (
                            "403 Forbidden",
                            format!("{{\"error\":\"subject {suffix:?} not permitted by ALLOW\"}}"),
                        ),
                        Ok(event) => match signature.and_then(|v| v.handshake(&event)) {
                            // A sender checking this URL before it sends events to it:
                            // its challenge goes back, and nothing is published.
                            Some(answer) => ("200 OK", answer),
                            None => {
                                let subject = format!("{subj_root}.{suffix}");
                                // A caller that traces sends its context; the event's trace
                                // then starts in the caller's (ADR-0034).
                                let incoming = header_value(&head, trace::TRACEPARENT).and_then(|tp| {
                                    trace::Context::parse(tp, header_value(&head, trace::TRACESTATE))
                                });
                                match source_publish(name, js, &subject, body, incoming, Kind::Server) {
                                    (hop, Ok(_)) => (
                                        "202 Accepted",
                                        format!(
                                            "{{\"published\":\"{subject}\",\"trace_id\":\"{}\"}}",
                                            hop.trace_hex()
                                        ),
                                    ),
                                    (_, Err(e)) => ("502 Bad Gateway", format!("{{\"error\":\"{e}\"}}")),
                                }
                            }
                        },
                    }
                }
            }
        };
        // One buffer, one write: with TCP_NODELAY `write!` puts each piece of the
        // format string in a segment of its own.
        let response = format!(
            "HTTP/1.1 {code}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: {}\r\n\r\n{json}",
            json.len(),
            if close { "close" } else { "keep-alive" }
        );
        let _ = sock.write_all(response.as_bytes());
        if close {
            // The rest of the request may still be on its way, and it is not a
            // next request.
            linger_close(sock);
            break;
        }
        if eof {
            break; // client sent its last request, then closed
        }
    }
}

/// Close after an answer the client may still be sending into: the write side
/// first, then what keeps coming is read and dropped for at most two seconds.
/// Closed at once, the socket would answer the unread bytes with a reset, and
/// the client could lose the answer before it reads it.
fn linger_close(sock: &mut TcpStream) {
    let _ = sock.shutdown(std::net::Shutdown::Write);
    sock.set_read_timeout(Some(Duration::from_millis(200))).ok();
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut dropped = [0u8; 8192];
    while Instant::now() < deadline {
        match sock.read(&mut dropped) {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
    }
}

/// A request header's value, its name matched without regard to case.
fn header_value<'a>(head: &'a str, name: &str) -> Option<&'a str> {
    head.split("\r\n").skip(1).find_map(|line| {
        let (k, v) = line.split_once(':')?;
        k.trim().eq_ignore_ascii_case(name).then(|| v.trim())
    })
}

/// Where a request's body ends, as its head says.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Framing {
    /// `Content-Length`: the body is that many bytes. Digits past what a `usize`
    /// holds count as the largest length, which is too large, not malformed.
    Length(usize),
    /// Neither `Content-Length` nor `Transfer-Encoding`: the body is what came
    /// with the head, as `http-in` always took it (a small device's HTTP stack
    /// may send no length).
    Unframed,
    /// A `Transfer-Encoding`: a chunked body is not read here.
    Chunked,
    /// A `Content-Length` that is not one number, or two that differ.
    BadLength,
}

impl Framing {
    fn of(head: &str) -> Framing {
        let mut length = None;
        for line in head.split("\r\n").skip(1) {
            let Some((name, value)) = line.split_once(':') else { continue };
            let name = name.trim();
            if name.eq_ignore_ascii_case("transfer-encoding") {
                return Framing::Chunked;
            }
            if !name.eq_ignore_ascii_case("content-length") {
                continue;
            }
            // Sent twice, or as a list, it must still say one number (RFC 9110, 8.6).
            for item in value.split(',').map(str::trim) {
                if item.is_empty() || !item.bytes().all(|b| b.is_ascii_digit()) {
                    return Framing::BadLength;
                }
                let len = item.parse::<usize>().unwrap_or(usize::MAX);
                if length.is_some_and(|known| known != len) {
                    return Framing::BadLength;
                }
                length = Some(len);
            }
        }
        length.map_or(Framing::Unframed, Framing::Length)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_request_header_is_found_whatever_its_case() {
        let head = "POST /ingest/orders HTTP/1.1\r\nHost: x\r\nTraceParent:  00-ab  \r\nContent-Length: 2\r\n\r\n";
        assert_eq!(header_value(head, "traceparent"), Some("00-ab"));
        assert_eq!(header_value(head, "content-length"), Some("2"));
        assert_eq!(header_value(head, "tracestate"), None);
        // The request line is not a header, whatever it holds.
        assert_eq!(header_value("GET /a:b HTTP/1.1\r\n\r\n", "get /a"), None);
    }

    #[test]
    fn http_in_allow_matching() {
        let allow = vec!["shop.orders".to_string(), "helpdesk".to_string()];
        assert!(suffix_allowed("shop.orders", &allow)); // exact
        assert!(suffix_allowed("shop.orders.eu", &allow)); // segment prefix
        assert!(suffix_allowed("helpdesk.tickets", &allow)); // segment prefix
        assert!(!suffix_allowed("slack.out", &allow)); // the F2 sink-injection case
        assert!(!suffix_allowed("shop", &allow)); // a prefix of an entry is NOT under it
        assert!(!suffix_allowed("shop.ordersX", &allow)); // segment boundary respected
    }

    #[test]
    fn a_body_ends_where_its_head_says() {
        let head = |lines: &str| format!("POST /ingest/a HTTP/1.1\r\nHost: h\r\n{lines}\r\n");
        assert_eq!(Framing::of(&head("Content-Length: 7\r\n")), Framing::Length(7));
        assert_eq!(Framing::of(&head("content-length:007\r\n")), Framing::Length(7));
        // Sent twice, or as a list, the same number is one length.
        assert_eq!(Framing::of(&head("Content-Length: 5\r\nContent-Length: 5\r\n")), Framing::Length(5));
        assert_eq!(Framing::of(&head("Content-Length: 5, 5\r\n")), Framing::Length(5));
        assert_eq!(Framing::of(&head("Content-Length: 5, 6\r\n")), Framing::BadLength);
        assert_eq!(Framing::of(&head("Content-Length: 5\r\nContent-Length: 6\r\n")), Framing::BadLength);
        for bad in ["abc", "", "+5", "-1", "5 5", "0x10", "1e3"] {
            assert_eq!(Framing::of(&head(&format!("Content-Length: {bad}\r\n"))), Framing::BadLength, "{bad:?}");
        }
        // Digits a usize cannot hold: too large, which the bound refuses.
        assert_eq!(
            Framing::of(&head("Content-Length: 18446744073709551616\r\n")),
            Framing::Length(usize::MAX)
        );
        assert_eq!(Framing::of(&head("Transfer-Encoding: chunked\r\n")), Framing::Chunked);
        assert_eq!(
            Framing::of(&head("Content-Length: 5\r\nTransfer-Encoding: chunked\r\n")),
            Framing::Chunked
        );
        assert_eq!(Framing::of(&head("")), Framing::Unframed);
        // The request line is not a header, whatever it holds.
        assert_eq!(Framing::of("POST /content-length:5 HTTP/1.1\r\n\r\n"), Framing::Unframed);
    }
}
