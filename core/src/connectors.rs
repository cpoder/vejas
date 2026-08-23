// Connector SDK (ADR-0007).
//
// A connector is a native Rust **driver** + a declarative **instance manifest**
// (`connectors/<name>.vjs`: `driver "http-in"` plus UPPERCASE literal config).
// Drivers are compiled in; instances are data — hot-addable, editable in the
// panel like any other business surface. Two families:
//
//   Source  — pushes onto the bus. Trigger kinds: webhook, poll, interval,
//             (queue/stream = future drivers). Publishes vx.<...>.
//   Sink    — consumes from the bus (durable pull consumer) and does a side
//             effect. Ack after success, else nak (redelivery = retry).
//
// The subject convention (SUBJECTS.md) remains the whole interface, so an
// external connector in any language is still a first-class citizen over the
// bus. These native drivers are the batteries included.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{Map, Value};

// ───────────────────────── config & context ─────────────────────────

/// A connector instance's config: the UPPERCASE literals of its manifest.
#[derive(Clone, Default)]
pub struct Config(pub Map<String, Value>);

impl Config {
    pub fn str(&self, key: &str) -> Option<String> {
        self.0.get(key).and_then(|v| v.as_str().map(|s| s.to_string()))
    }
    pub fn str_or(&self, key: &str, default: &str) -> String {
        self.str(key).unwrap_or_else(|| default.to_string())
    }
    pub fn u64_or(&self, key: &str, default: u64) -> u64 {
        self.0.get(key).and_then(|v| v.as_u64()).unwrap_or(default)
    }
    pub fn value(&self, key: &str) -> Option<Value> {
        self.0.get(key).cloned()
    }
    /// Optional HEADERS doc: `HEADERS = {"Authorization": f"Bearer {secret("…")}"}`.
    /// Manifests are evaluated, so secret()/f-strings resolve before this runs —
    /// and an expression is not a literal, so it never enters the business surface.
    pub fn headers(&self) -> Vec<(String, String)> {
        self.0
            .get("HEADERS")
            .and_then(|v| v.as_object())
            .map(|o| {
                o.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default()
    }
    /// Optional ENV doc: `ENV = {"SAP_PASSWD": secret("…"), "SAP_USER": "DEV"}`.
    /// Evaluated at manifest time, so secret()/f-strings resolve here; passed to
    /// an exec child's environment — never argv (`/proc/<pid>/environ` is
    /// owner-only, `cmdline` is world-readable). Non-string values are stringified.
    pub fn env_vars(&self) -> Vec<(String, String)> {
        self.0
            .get("ENV")
            .and_then(|v| v.as_object())
            .map(|o| {
                o.iter()
                    .map(|(k, v)| match v {
                        Value::String(s) => (k.clone(), s.clone()),
                        other => (k.clone(), other.to_string()),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

pub struct Ctx {
    pub name: String,
    pub nats_url: String,
    pub stream: String,
    pub subj_root: String,
    pub config: Config,
    pub running: Arc<AtomicBool>,
}

impl Ctx {
    fn alive(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
    fn jetstream(&self) -> Result<nats::jetstream::JetStream, String> {
        Ok(self.jetstream_and_conn()?.0)
    }
    /// Like `jetstream()`, but also returns the raw `Connection` (a cheap clone —
    /// `Connection` is `Arc` inside) so a driver can use `publish_confirmed`, which
    /// needs the connection to flush directly past the 5ms flusher floor.
    fn jetstream_and_conn(
        &self,
    ) -> Result<(nats::jetstream::JetStream, nats::Connection), String> {
        let nc = nats::connect(&self.nats_url).map_err(|e| e.to_string())?;
        let js = nats::jetstream::new(nc.clone());
        let _ = js.add_stream(&nats::jetstream::StreamConfig {
            name: self.stream.clone(),
            subjects: vec![format!("{}.>", self.subj_root)],
            ..Default::default()
        });
        Ok((js, nc))
    }
    /// Full subject from a config value that may be a bare suffix or a vx.* path.
    fn subject(&self, raw: &str) -> String {
        if raw.starts_with(&format!("{}.", self.subj_root)) {
            raw.to_string()
        } else {
            format!("{}.{}", self.subj_root, raw)
        }
    }
}

// ───────────────────────── driver trait & registry ─────────────────────────

pub trait Driver: Send + Sync {
    /// e.g. "source:webhook", "source:poll", "source:interval", "sink".
    fn kind(&self) -> &'static str;
    /// One-line description for the panel / MCP.
    fn about(&self) -> &'static str;
    /// Blocking; loops until `ctx.running` clears. Returning Err triggers a
    /// supervised restart.
    fn run(&self, ctx: &Ctx) -> Result<(), String>;
    /// One synchronous end-to-end check of THIS instance's config — reach the
    /// remote side with the real credentials, touch nothing (no publish, no
    /// data written). The admin's "does my secret and my network work?"
    /// button. Default: the driver has no probe.
    fn probe(&self, _ctx: &Ctx) -> Result<String, String> {
        Err("this driver has no test probe".into())
    }
}

/// Plain-words prefix for the errors an admin actually meets.
pub fn humanize(e: &str) -> String {
    if e.contains("HTTP 401") || e.contains("HTTP 403") {
        format!("authentication refused (invalid or expired secret?) — {e}")
    } else if e.contains("not set") || e.contains("not found (set env") || (e.contains("secret") && e.contains("not found")) {
        format!("missing secret — {e}")
    } else if e.starts_with("http:") || e.contains("Connection refused") || e.contains("Could not resolve") || e.contains("timed out") {
        format!("unreachable (network, URL, proxy?) — {e}")
    } else {
        e.to_string()
    }
}

/// The compiled-in driver catalog. New drivers are added here (and, later,
/// generated onto this trait via `vejas_new_connector`).
pub fn driver_for(name: &str) -> Option<Box<dyn Driver>> {
    match name {
        "http-in" => Some(Box::new(HttpIn)),
        "timer" => Some(Box::new(Timer)),
        "http-poll" => Some(Box::new(HttpPoll)),
        "oauth-poll" => Some(Box::new(OAuthPoll)),
        "slack-out" => Some(Box::new(SlackOut)),
        "http-out" => Some(Box::new(HttpOut)),
        "exec-source" => Some(Box::new(ExecSource)),
        "exec-sink" => Some(Box::new(ExecSink)),
        "exec-stream-source" => Some(Box::new(ExecStreamSource)),
        "exec-rpc" => Some(Box::new(ExecRpc)),
        "mqtt-in" => Some(Box::new(MqttIn)),
        "mqtt-out" => Some(Box::new(MqttOut)),
        _ => None,
    }
}

pub fn catalog() -> Vec<(&'static str, &'static str, &'static str)> {
    [
        "http-in", "timer", "http-poll", "oauth-poll", "slack-out", "http-out",
        "exec-source", "exec-sink", "exec-stream-source", "exec-rpc",
        "mqtt-in", "mqtt-out",
    ]
    .iter()
    .filter_map(|n| driver_for(n).map(|d| (*n, d.kind(), d.about())))
    .collect()
}

// ───────────────────────── source: webhook ─────────────────────────

struct HttpIn;
impl Driver for HttpIn {
    fn kind(&self) -> &'static str {
        "source:webhook"
    }
    fn about(&self) -> &'static str {
        "HTTP webhook: POST /ingest/<suffix> publishes the JSON body on vx.<suffix>. Config: PORT."
    }
    fn run(&self, ctx: &Ctx) -> Result<(), String> {
        let port = ctx.config.u64_or("PORT", 8787) as u16;
        // nc for publish_confirmed (direct flush past the 5ms flusher floor —
        // measured: /ingest p50 5.2ms→0.77ms, no throughput regression at conc=32);
        // _js keeps the stream ensured.
        let (_js, nc) = ctx.jetstream_and_conn()?;
        let listener = TcpListener::bind(("0.0.0.0", port)).map_err(|e| e.to_string())?;
        listener.set_nonblocking(true).ok();
        eprintln!("[{}] http-in on :{port}, publishing under {}.*", ctx.name, ctx.subj_root);
        for s in listener.incoming() {
            if !ctx.alive() {
                break;
            }
            match s {
                Ok(mut sock) => {
                    let nc = nc.clone();
                    let root = ctx.subj_root.clone();
                    let name = ctx.name.clone();
                    thread::spawn(move || handle_http_in(&name, &mut sock, &nc, &root));
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

fn handle_http_in(name: &str, sock: &mut std::net::TcpStream, nc: &nats::Connection, subj_root: &str) {
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
        let mut eof = false;
        loop {
            match sock.read(&mut tmp) {
                Ok(0) => {
                    eof = true;
                    break;
                }
                Ok(n) => {
                    buf.extend_from_slice(&tmp[..n]);
                    if header_end.is_none() {
                        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            header_end = Some(pos + 4);
                        }
                    }
                    if let Some(he) = header_end {
                        let clen = content_length(&String::from_utf8_lossy(&buf[..he]));
                        if buf.len() >= he + clen {
                            break;
                        }
                    }
                    if buf.len() > 1_048_576 {
                        break;
                    }
                }
                Err(_) => {
                    eof = true;
                    break;
                }
            }
        }
        if header_end.is_none() {
            break; // no complete request (client closed or idled out)
        }
        let (code, json): (&str, String) = {
            let text = String::from_utf8_lossy(&buf);
            let mut parts = text.split("\r\n").next().unwrap_or("").split(' ');
            let method = parts.next().unwrap_or("");
            let path = parts.next().unwrap_or("");
            let body = header_end.map(|he| &buf[he.min(buf.len())..]).unwrap_or(&[]);
            if method == "GET" && path == "/healthz" {
                ("200 OK", "{\"ok\":true}".to_string())
            } else if method != "POST" || !path.starts_with("/ingest/") {
                ("404 Not Found", "{\"error\":\"POST /ingest/<subject-suffix>\"}".to_string())
            } else {
                let suffix = path.trim_start_matches("/ingest/").trim_matches('/');
                if suffix.is_empty() {
                    ("400 Bad Request", "{\"error\":\"missing subject suffix\"}".to_string())
                } else if serde_json::from_slice::<Value>(body).is_err() {
                    ("400 Bad Request", "{\"error\":\"body must be JSON\"}".to_string())
                } else {
                    let subject = format!("{subj_root}.{suffix}");
                    match publish_confirmed(nc, &subject, body, Duration::from_secs(5)) {
                        Ok(_) => {
                            trace_pub(name, &subject, body);
                            ("202 Accepted", format!("{{\"published\":\"{subject}\"}}"))
                        }
                        Err(e) => {
                            trace_fail(name, &subject, format!("publish: {e}"));
                            ("502 Bad Gateway", format!("{{\"error\":\"{e}\"}}"))
                        }
                    }
                }
            }
        };
        let _ = write!(
            sock,
            "HTTP/1.1 {code}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{json}",
            json.len()
        );
        if eof {
            break; // client sent its last request, then closed
        }
    }
}

fn content_length(head: &str) -> usize {
    for line in head.lines() {
        if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
            return v.trim().parse().unwrap_or(0);
        }
    }
    0
}

// ───────────────────────── source: interval (timer) ─────────────────────────

/// A timer event carries its tick timestamp: an object payload without a `ts`
/// field gets one (ISO 8601 UTC). A dated event is what lets downstream flows
/// build idempotency keys — same reasoning as oauth-poll's `fetched_at` (the
/// language has no clock, on purpose).
fn stamp_ts(payload: &Value, secs: u64) -> Value {
    match payload {
        Value::Object(o) if !o.contains_key("ts") => {
            let mut o = o.clone();
            o.insert("ts".into(), Value::String(iso8601_utc(secs)));
            Value::Object(o)
        }
        other => other.clone(),
    }
}

struct Timer;
impl Driver for Timer {
    fn kind(&self) -> &'static str {
        "source:interval"
    }
    fn about(&self) -> &'static str {
        "Emits PAYLOAD on SUBJECT every INTERVAL_SECS; an object payload gains a ts field (ISO 8601 UTC) when absent. Config: SUBJECT, INTERVAL_SECS, PAYLOAD."
    }
    fn run(&self, ctx: &Ctx) -> Result<(), String> {
        let subject = ctx.subject(&ctx.config.str("SUBJECT").ok_or("SUBJECT required")?);
        let interval = ctx.config.u64_or("INTERVAL_SECS", 60).max(1);
        let payload = ctx.config.value("PAYLOAD").unwrap_or(Value::Object(Map::new()));
        let js = ctx.jetstream()?;
        eprintln!("[{}] timer every {interval}s -> {subject}", ctx.name);
        let mut waited = 0;
        loop {
            if !ctx.alive() {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(250));
            waited += 250;
            if waited >= interval * 1000 {
                waited = 0;
                let bytes = serde_json::to_vec(&stamp_ts(&payload, crate::now_secs()))
                    .unwrap_or_default();
                if let Err(e) = js.publish(&subject, &bytes) {
                    return Err(format!("publish: {e}"));
                }
                trace_pub(&ctx.name, &subject, &bytes);
            }
        }
    }
}

// ───────────────────────── source: poll ─────────────────────────

struct HttpPoll;
impl Driver for HttpPoll {
    fn kind(&self) -> &'static str {
        "source:poll"
    }
    fn about(&self) -> &'static str {
        "GETs a URL every INTERVAL_SECS and publishes the JSON body. Config: URL, SUBJECT, INTERVAL_SECS, HEADERS (optional doc, e.g. {\"Authorization\": …}), ENVELOPE (optional bool: when true, publish {endpoint, fetched_at, body} like oauth-poll so a stateless flow gets a collected_at — the language has no clock)."
    }
    fn probe(&self, ctx: &Ctx) -> Result<String, String> {
        let url = ctx.config.str("URL").ok_or("URL required")?;
        match http_get(&url, &ctx.config.headers()) {
            Ok(body) => Ok(format!("GET OK ({} bytes)", body.len())),
            Err(e) => Err(humanize(&e)),
        }
    }
    fn run(&self, ctx: &Ctx) -> Result<(), String> {
        let url = ctx.config.str("URL").ok_or("URL required")?;
        let subject = ctx.subject(&ctx.config.str("SUBJECT").ok_or("SUBJECT required")?);
        let interval = ctx.config.u64_or("INTERVAL_SECS", 60).max(1);
        let headers = ctx.config.headers();
        let envelope = ctx.config.0.get("ENVELOPE").and_then(|v| v.as_bool()).unwrap_or(false);
        let js = ctx.jetstream()?;
        eprintln!("[{}] polling {url} every {interval}s -> {subject}", ctx.name);
        let mut waited = interval * 1000; // fire immediately on start
        loop {
            if !ctx.alive() {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(250));
            waited += 250;
            if waited >= interval * 1000 {
                waited = 0;
                match http_get(&url, &headers) {
                    Ok(raw) => match serde_json::from_slice::<Value>(&raw) {
                        Ok(parsed) => {
                            // ENVELOPE=true: {endpoint, fetched_at, body} (stateless
                            // flow gets collected_at); else the raw body (compat)
                            let bytes = if envelope {
                                serde_json::to_vec(&serde_json::json!({
                                    "endpoint": url,
                                    "fetched_at": iso8601_utc(crate::now_secs()),
                                    "body": parsed,
                                }))
                                .unwrap_or_default()
                            } else {
                                raw.clone()
                            };
                            let _ = js.publish(&subject, &bytes);
                            trace_pub(&ctx.name, &subject, &bytes);
                        }
                        Err(_) => eprintln!("[{}] poll: non-JSON body, skipped", ctx.name),
                    },
                    Err(e) => {
                        eprintln!("[{}] poll: {e}", ctx.name);
                        trace_fail(&ctx.name, &subject, e);
                    }
                }
            }
        }
    }
}

// ───────────────────────── pull rounds ─────────────────────────

/// One bounded pull round: up to 10 messages, and the server resolves the
/// request within PULL_EXPIRES_NS (messages or a 408 timeout status), so the
/// calling loop gets control back and can re-check its stop flag. A plain
/// `fetch()` parks forever on an idle subject: a stopped flow's thread stayed
/// wedged in `recv()`, reported "running", and processed one more message
/// before dying (the zombie-consumer trap caught while rehearsing the demo).
/// Consumer ack-wait (redelivery latency): a message not acked within this window
/// redelivers (at-least-once, ADR-0002). `VEJAS_ACK_WAIT_SECS` tunes it; unset (or
/// ≤0) keeps `Duration::ZERO`, which JetStream reads as its 30s default — the
/// historical behaviour. Lower it to make transient failures retry sooner (and to
/// let the transport tests exercise redelivery + the poison→DLQ cap at CI speed).
/// A positive value is floored at 1s: JetStream rejects a sub-second ack-wait
/// (the consumer would silently fail to create), so we never pass one through.
pub fn ack_wait() -> Duration {
    std::env::var("VEJAS_ACK_WAIT_SECS")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|&s| s > 0.0)
        .map(|s| Duration::from_secs_f64(s.max(1.0)))
        .unwrap_or_default()
}

/// The idle poll window (`expires`). One outstanding pull covers this whole span,
/// so a message arriving any time inside it is delivered at once — there is NO gap
/// between rounds where a message would sit unpolled. It is therefore two things at
/// once: the idle pull cadence (one pull per window when quiet) AND the worst-case
/// stop-response latency (the loop re-checks its `alive` flag each time a pull
/// returns). 500ms keeps idle chatter to ~2 pulls/s per consumer while leaving stop
/// far inside the transport suite's 3s bound (T5).
const PULL_EXPIRES_MS: u64 = 500;

pub fn fetch_round(sub: &nats::jetstream::PullSubscription) -> Result<Vec<nats::Message>, String> {
    // A continuous long-poll. `no_wait` returns the instant messages are available
    // (a single message is delivered at once — no batch-fill latency, and the
    // benched high-rate drain still fills 64-batches in one round). When the subject
    // is idle the pull is HELD server-side for `expires` and returns empty — so the
    // loop paces itself on the server with no client sleep, and crucially no
    // coverage gap: previously a 150ms idle sleep sat between rounds with no pull
    // outstanding, and a message landing in that window waited it out (measured tail
    // ~150ms — the connector-latency bug the MQTT bench surfaced). No pull parks in
    // max_waiting: one is in flight at a time and each resolves within `expires`, so
    // the anti-zombie invariant holds and stop is re-checked every round.
    let expires_ns = (PULL_EXPIRES_MS * 1_000_000) as usize;
    let iter = sub
        .timeout_fetch(
            nats::jetstream::BatchOptions {
                batch: 64,
                expires: Some(expires_ns),
                no_wait: true,
            },
            // client-side backstop: comfortably past the server's `expires` so the
            // server's own empty/timeout status ends the round, not this timeout.
            Duration::from_millis(PULL_EXPIRES_MS + 250),
        )
        .map_err(|e| e.to_string())?;
    Ok(iter.map_while(|m| m.ok()).collect())
}

/// Publish to a JetStream subject and wait for the pub-ack, flushing the write
/// DIRECTLY in this thread rather than via the shared flusher.
///
/// `nats` 0.25's flusher thread imposes a hard-coded 5ms floor between flushes
/// (`client.rs` `MIN_FLUSH_BETWEEN`, a never-done `TODO(dlc)`). Since a JetStream
/// pub-ack is a request/reply, EVERY sequential `js.publish` is capped at ~200/s by
/// that floor — the ceiling the MQTT loopback bench surfaced (js.publish p50
/// ≈5.1ms, dead on the tick). `Connection::flush()` does a direct writer flush +
/// PING/PONG in the calling thread, bypassing the floor, which lifts sequential
/// publish to ~1-2k/s. This reimplements `request_with_headers_or_timeout` (nats
/// `lib.rs`) verbatim — inbox, buffered publish-with-reply, the **no-responders
/// guard** — and only adds the direct flush, so the at-least-once contract is
/// unchanged: a returned Ok still means the message was persisted (a real pub-ack
/// with a `stream`/`seq`), never a mis-read of a 503 or an error ack.
pub fn publish_confirmed(
    nc: &nats::Connection,
    subject: &str,
    payload: &[u8],
    wait: Duration,
) -> Result<(), String> {
    let reply = nc.new_inbox();
    let sub = nc.subscribe(&reply).map_err(|e| e.to_string())?;
    nc.publish_request(subject, &reply, payload)
        .map_err(|e| e.to_string())?;
    nc.flush().map_err(|e| e.to_string())?; // direct flush — bypass the 5ms floor
    let msg = sub
        .next_timeout(wait)
        .map_err(|_| "timed out waiting for JetStream pub-ack".to_string())?;
    if msg.is_no_responders() {
        return Err("no JetStream responder for the subject (stream missing?)".into());
    }
    // ApiResponse<PublishAck>: an "error" key = rejected; a "stream" key = persisted.
    let ack: serde_json::Value =
        serde_json::from_slice(&msg.data).map_err(|e| format!("bad pub-ack: {e}"))?;
    if let Some(err) = ack.get("error") {
        return Err(format!("JetStream rejected the publish: {err}"));
    }
    if ack.get("stream").is_none() {
        return Err(format!(
            "unexpected pub-ack (no stream): {}",
            String::from_utf8_lossy(&msg.data)
        ));
    }
    Ok(())
}

/// Hydrate up to `n` recent REAL events for `subject` from the persisted stream,
/// for shadow-replay (ADR-0018). Read-only: an **ephemeral** pull consumer (no
/// durable name → a server-assigned name, wholly separate from the flow's own
/// durable) reads history and is never acked — so the live flow's delivery state
/// is untouched and the bus is not consumed. Returns (subject, event) newest
/// last, at most `n`.
///
/// To bound work on a large stream, it starts a window before the stream's last
/// sequence rather than from the beginning: at most `window` messages are
/// scanned, of which the last `n` matching `subject` are kept.
pub fn hydrate_recent(
    js: &nats::jetstream::JetStream,
    stream: &str,
    subject: &str,
    n: usize,
) -> Result<Vec<(u64, String, serde_json::Value)>, String> {
    let n = n.clamp(1, 5000);
    let last = js
        .stream_info(stream)
        .map(|si| si.state.last_seq)
        .unwrap_or(0);
    if last == 0 {
        return Ok(Vec::new()); // empty stream: nothing persisted yet
    }
    // Over-scan so we still find n matching events when other subjects interleave,
    // but keep it bounded (an operator action, not the hot path).
    let window = (n as u64).saturating_mul(8).max(256).min(200_000);
    let start = last.saturating_sub(window).max(1);
    let cfg = nats::jetstream::ConsumerConfig {
        deliver_policy: nats::jetstream::DeliverPolicy::ByStartSeq,
        opt_start_seq: Some(start),
        // Explicit but never acked: the consumer is ephemeral and discarded, so
        // un-acked messages simply expire with it — nothing redelivers to the
        // flow, nothing is consumed off the stream.
        ack_policy: nats::jetstream::AckPolicy::Explicit,
        filter_subject: subject.to_string(),
        // auto-reap the ephemeral consumer shortly after we stop pulling
        inactive_threshold: Duration::from_secs(30),
        ..Default::default()
    };
    let sub = js
        .pull_subscribe_with_options(
            subject,
            &nats::jetstream::PullSubscribeOptions::new().consumer_config(cfg),
        )
        .map_err(|e| e.to_string())?;
    let mut ring: std::collections::VecDeque<(u64, String, serde_json::Value)> =
        std::collections::VecDeque::with_capacity(n);
    let mut scanned = 0u64;
    loop {
        let batch = fetch_round(&sub)?;
        if batch.is_empty() {
            break; // no_wait + fully-persisted stream: empty means exhausted
        }
        for m in &batch {
            scanned += 1;
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&m.data) {
                // the stream sequence is the intrinsic join key for time-travel /
                // canary diffs (ADR-0021) — no per-flow key to invent.
                let seq = m.jetstream_message_info().map(|i| i.stream_seq).unwrap_or(0);
                ring.push_back((seq, m.subject.clone(), v));
                while ring.len() > n {
                    ring.pop_front();
                }
            }
        }
        if scanned >= window {
            break; // safety bound hit
        }
    }
    Ok(ring.into_iter().collect())
}

// ───────────────────────── source: oauth-poll ─────────────────────────
//
// The generic OAuth2 client-credentials REST poller — the driver that stands
// in for the overwhelming majority of "400 connectors": one token endpoint,
// a Bearer, N REST endpoints, cursor pagination. Publishes one message per
// page, enveloped as {endpoint, fetched_at, body}: `endpoint` is the logical
// route for the mapping flow, `fetched_at` (ISO 8601 UTC, stamped here — the
// language has no clock) is what the flow copies into collected_at, so a
// JetStream redelivery reproduces byte-identical facts and idempotency keys.

struct OAuthPoll;

/// Client-credentials token cache: refresh a minute early, invalidate on 401.
struct TokenCache {
    token_url: String,
    client_id: String,
    client_secret: String,
    scope: String,
    cached: Option<(String, Instant)>,
}

impl TokenCache {
    fn bearer(&mut self) -> Result<String, String> {
        if let Some((t, deadline)) = &self.cached {
            if Instant::now() < *deadline {
                return Ok(t.clone());
            }
        }
        let (t, ttl) =
            OAuthPoll::token(&self.token_url, &self.client_id, &self.client_secret, &self.scope)?;
        self.cached = Some((t.clone(), Instant::now() + ttl));
        Ok(t)
    }
    fn invalidate(&mut self) {
        self.cached = None;
    }
}

/// GET with the cached bearer; ONE fresh-token retry on 401.
fn authed_get(tc: &mut TokenCache, url: &str) -> Result<(u16, Vec<u8>), String> {
    let tok = tc.bearer()?;
    let headers = [("Authorization".to_string(), format!("Bearer {tok}"))];
    match http_request("GET", url, &headers, None)? {
        (401, _) => {
            tc.invalidate();
            let tok = tc.bearer()?;
            let headers = [("Authorization".to_string(), format!("Bearer {tok}"))];
            http_request("GET", url, &headers, None)
        }
        ok => Ok(ok),
    }
}

/// `{key}` placeholder substitution for EXPAND detail templates.
fn fill_template(template: &str, key: &str, value: &str) -> String {
    template.replace(&format!("{{{key}}}"), value)
}

/// A client-side $expand: list endpoint + per-item detail endpoint, joined by
/// the driver into ONE envelope per page — so flows stay stateless (no
/// cross-message correlation) exactly as with a server-side $expand.
struct Expand {
    name: String,
    list: String,
    detail: String,
    key: String,
    as_field: String,
    /// The array field in the list response (default "value" à la Graph;
    /// "resources" for CrowdStrike, "data" for many others).
    list_field: String,
}

fn parse_expands(cfg: &Config) -> Result<Vec<Expand>, String> {
    let Some(raw) = cfg.value("EXPAND") else { return Ok(Vec::new()) };
    let entries = raw
        .as_array()
        .cloned()
        .ok_or("EXPAND must be a list of {name, list, detail, key, as} docs")?;
    let mut out = Vec::new();
    for e in &entries {
        let field = |k: &str| e.get(k).and_then(|v| v.as_str()).map(|s| s.to_string());
        match (field("name"), field("list"), field("detail"), field("key"), field("as")) {
            (Some(name), Some(list), Some(detail), Some(key), Some(as_field)) => out.push(Expand {
                name,
                list,
                detail,
                key,
                as_field,
                list_field: field("list_field").unwrap_or_else(|| "value".into()),
            }),
            _ => return Err("every EXPAND entry needs name, list, detail, key and as".into()),
        }
    }
    Ok(out)
}

impl OAuthPoll {
    fn token(
        token_url: &str,
        client_id: &str,
        client_secret: &str,
        scope: &str,
    ) -> Result<(String, Duration), String> {
        // scope is optional: CrowdStrike (and other client-credentials APIs)
        // reject a scope param they don't define — omit it entirely when empty
        let mut form = format!(
            "grant_type=client_credentials&client_id={}&client_secret={}",
            urlencode(client_id),
            urlencode(client_secret)
        );
        if !scope.is_empty() {
            form.push_str(&format!("&scope={}", urlencode(scope)));
        }
        let headers = [(
            "Content-Type".to_string(),
            "application/x-www-form-urlencoded".to_string(),
        )];
        let (code, body) = http_request("POST", token_url, &headers, Some(form.as_bytes()))?;
        if !(200..300).contains(&code) {
            return Err(format!(
                "token endpoint HTTP {code}: {}",
                String::from_utf8_lossy(&body[..body.len().min(300)]).trim()
            ));
        }
        let v: Value =
            serde_json::from_slice(&body).map_err(|e| format!("token endpoint: bad JSON: {e}"))?;
        let tok = v["access_token"]
            .as_str()
            .ok_or("token endpoint: no access_token in response")?
            .to_string();
        // refresh a minute early; floor for endpoints that answer tiny lifetimes
        let ttl = v["expires_in"].as_u64().unwrap_or(300).saturating_sub(60).max(30);
        Ok((tok, Duration::from_secs(ttl)))
    }
}

impl Driver for OAuthPoll {
    fn kind(&self) -> &'static str {
        "source:poll"
    }
    fn about(&self) -> &'static str {
        "OAuth2 client-credentials REST poller: takes a token from TOKEN_URL, GETs each of ENDPOINTS with the Bearer (pagination via NEXT_LINK_FIELD, default \"@odata.nextLink\", capped by MAX_PAGES), publishes one {endpoint, fetched_at, body} message per page on SUBJECT every INTERVAL_SECS. EXPAND = [{name, list, detail, key, as, list_field?}] does a client-side $expand: each item of the list response's array (list_field, default \"value\"; use \"resources\"/\"data\" for other APIs; a bare-string item becomes {key: id}) is enriched with its per-item detail call ({key} substituted in the detail path) and the page ships as ONE envelope under endpoint=name — sized for list APIs without a server-side expand (sequential detail calls, keep pages small). Config: TOKEN_URL, CLIENT_ID, CLIENT_SECRET (use secret()), SCOPE (optional — omitted when empty, e.g. CrowdStrike), BASE_URL, ENDPOINTS and/or EXPAND, SUBJECT, INTERVAL_SECS, MAX_PAGES."
    }
    fn probe(&self, ctx: &Ctx) -> Result<String, String> {
        let token_url = ctx.config.str("TOKEN_URL").ok_or("TOKEN_URL required")?;
        let client_id = ctx.config.str("CLIENT_ID").ok_or("CLIENT_ID required")?;
        let client_secret = ctx.config.str("CLIENT_SECRET").ok_or("CLIENT_SECRET required")?;
        let scope = ctx.config.str_or("SCOPE", ""); // optional (CrowdStrike: none)
        let base = ctx.config.str("BASE_URL").ok_or("BASE_URL required")?;
        let ep = ctx
            .config
            .value("ENDPOINTS")
            .and_then(|v| v.as_array().and_then(|a| a.first().and_then(|e| e.as_str().map(|s| s.to_string()))))
            .or_else(|| {
                parse_expands(&ctx.config).ok().and_then(|ex| ex.first().map(|s| s.list.clone()))
            })
            .ok_or("ENDPOINTS or EXPAND required")?;
        let (tok, _) = Self::token(&token_url, &client_id, &client_secret, &scope)
            .map_err(|e| humanize(&e))?;
        let url = join_url(&base, &ep);
        let headers = [("Authorization".to_string(), format!("Bearer {tok}"))];
        match http_request("GET", &url, &headers, None) {
            Ok((code, body)) if (200..300).contains(&code) => {
                let items = serde_json::from_slice::<Value>(&body)
                    .ok()
                    .and_then(|v| v["value"].as_array().map(|a| a.len()));
                Ok(match items {
                    Some(n) => format!("token OK · GET {ep} OK ({n} items)"),
                    None => format!("token OK · GET {ep} OK ({} bytes)", body.len()),
                })
            }
            Ok((code, _)) => Err(humanize(&format!(
                "token OK, but GET {ep} answered HTTP {code} (missing permission / admin consent?)"
            ))),
            Err(e) => Err(humanize(&format!("GET {ep}: {e}"))),
        }
    }
    fn run(&self, ctx: &Ctx) -> Result<(), String> {
        let token_url = ctx.config.str("TOKEN_URL").ok_or("TOKEN_URL required")?;
        let client_id = ctx.config.str("CLIENT_ID").ok_or("CLIENT_ID required")?;
        let client_secret = ctx.config.str("CLIENT_SECRET").ok_or("CLIENT_SECRET required")?;
        let scope = ctx.config.str_or("SCOPE", ""); // optional: omitted from the token form when empty
        let base = ctx.config.str("BASE_URL").ok_or("BASE_URL required")?;
        let endpoints: Vec<String> = ctx
            .config
            .value("ENDPOINTS")
            .and_then(|v| {
                v.as_array().map(|a| {
                    a.iter()
                        .filter_map(|e| e.as_str().map(|s| s.to_string()))
                        .collect::<Vec<_>>()
                })
            })
            .unwrap_or_default();
        let expands = parse_expands(&ctx.config)?;
        if endpoints.is_empty() && expands.is_empty() {
            return Err("ENDPOINTS or EXPAND required".into());
        }
        let subject = ctx.subject(&ctx.config.str("SUBJECT").ok_or("SUBJECT required")?);
        let interval = ctx.config.u64_or("INTERVAL_SECS", 300).max(1);
        let max_pages = ctx.config.u64_or("MAX_PAGES", 10).max(1);
        let next_field = ctx.config.str_or("NEXT_LINK_FIELD", "@odata.nextLink");
        let js = ctx.jetstream()?;
        eprintln!(
            "[{}] oauth-poll {} endpoint(s) + {} expand(s) on {base} every {interval}s -> {subject}",
            ctx.name,
            endpoints.len(),
            expands.len()
        );
        let mut tc = TokenCache {
            token_url,
            client_id,
            client_secret,
            scope,
            cached: None,
        };
        let mut waited = interval * 1000; // fire immediately on start
        loop {
            if !ctx.alive() {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(250));
            waited += 250;
            if waited < interval * 1000 {
                continue;
            }
            waited = 0;
            // a broken token config never comes back on its own: validate it
            // once per tick, and fail the run (supervised backoff) if not
            tc.bearer().map_err(|e| {
                trace_fail(&ctx.name, &subject, e.clone());
                e
            })?;
            let publish_page =
                |parsed: &Value, logical: &str, js: &nats::jetstream::JetStream| -> Result<(), String> {
                    let envelope = serde_json::json!({
                        "endpoint": logical,
                        "fetched_at": iso8601_utc(crate::now_secs()),
                        "body": parsed,
                    });
                    let bytes = serde_json::to_vec(&envelope).unwrap_or_default();
                    if let Err(e) = js.publish(&subject, &bytes) {
                        return Err(format!("publish: {e}"));
                    }
                    trace_pub(&ctx.name, &subject, &bytes);
                    Ok(())
                };
            for ep in &endpoints {
                let mut url = join_url(&base, ep);
                let mut pages = 0;
                while pages < max_pages {
                    if !ctx.alive() {
                        return Ok(());
                    }
                    match authed_get(&mut tc, &url) {
                        Ok((code, body)) if (200..300).contains(&code) => {
                            let parsed: Value = match serde_json::from_slice(&body) {
                                Ok(v) => v,
                                Err(e) => {
                                    eprintln!("[{}] {ep}: non-JSON page skipped: {e}", ctx.name);
                                    break;
                                }
                            };
                            publish_page(&parsed, ep, &js)?;
                            pages += 1;
                            match next_url(&base, &parsed, &next_field) {
                                Some(next) => url = next,
                                None => break,
                            }
                        }
                        Ok((code, body)) => {
                            let msg = format!(
                                "{ep}: HTTP {code}: {}",
                                String::from_utf8_lossy(&body[..body.len().min(200)]).trim()
                            );
                            eprintln!("[{}] {msg} — endpoint skipped this tick", ctx.name);
                            trace_fail(&ctx.name, &subject, msg);
                            break;
                        }
                        Err(e) => {
                            eprintln!("[{}] {ep}: {e} — endpoint skipped this tick", ctx.name);
                            trace_fail(&ctx.name, &subject, format!("{ep}: {e}"));
                            break;
                        }
                    }
                }
            }
            // client-side $expand: list page fetched, every item enriched with
            // its detail call, ONE envelope per page — flows stay stateless
            for spec in &expands {
                let mut url = join_url(&base, &spec.list);
                let mut pages = 0;
                while pages < max_pages {
                    if !ctx.alive() {
                        return Ok(());
                    }
                    match authed_get(&mut tc, &url) {
                        Ok((code, body)) if (200..300).contains(&code) => {
                            let mut parsed: Value = match serde_json::from_slice(&body) {
                                Ok(v) => v,
                                Err(e) => {
                                    eprintln!("[{}] {}: non-JSON page skipped: {e}", ctx.name, spec.name);
                                    break;
                                }
                            };
                            if let Some(items) =
                                parsed.get_mut(&spec.list_field).and_then(|v| v.as_array_mut())
                            {
                                for item in items.iter_mut() {
                                    if !ctx.alive() {
                                        return Ok(());
                                    }
                                    // an item may be an OBJECT with a {key} field, or a
                                    // bare STRING id (CrowdStrike queries → [ids]); a
                                    // scalar becomes {key: id} so detail + as land on it
                                    if item.is_string() {
                                        let id = item.as_str().unwrap().to_string();
                                        let mut o = serde_json::Map::new();
                                        o.insert(spec.key.clone(), Value::String(id));
                                        *item = Value::Object(o);
                                    }
                                    let Some(id) = item
                                        .get(&spec.key)
                                        .and_then(|v| v.as_str())
                                        .map(|s| s.to_string())
                                    else {
                                        continue;
                                    };
                                    let durl = join_url(
                                        &base,
                                        &fill_template(&spec.detail, &spec.key, &id),
                                    );
                                    // a failing detail leaves null on THAT item;
                                    // the page still ships (per-item resilience)
                                    let detail = match authed_get(&mut tc, &durl) {
                                        Ok((c, b)) if (200..300).contains(&c) => {
                                            serde_json::from_slice::<Value>(&b)
                                                .unwrap_or(Value::Null)
                                        }
                                        Ok((c, _)) => {
                                            eprintln!(
                                                "[{}] {}: detail for {id} -> HTTP {c}",
                                                ctx.name, spec.name
                                            );
                                            Value::Null
                                        }
                                        Err(e) => {
                                            eprintln!(
                                                "[{}] {}: detail for {id}: {e}",
                                                ctx.name, spec.name
                                            );
                                            Value::Null
                                        }
                                    };
                                    if let Some(o) = item.as_object_mut() {
                                        o.insert(spec.as_field.clone(), detail);
                                    }
                                }
                            }
                            publish_page(&parsed, &spec.name, &js)?;
                            pages += 1;
                            match next_url(&base, &parsed, &next_field) {
                                Some(next) => url = next,
                                None => break,
                            }
                        }
                        Ok((code, body)) => {
                            let msg = format!(
                                "{}: HTTP {code}: {}",
                                spec.name,
                                String::from_utf8_lossy(&body[..body.len().min(200)]).trim()
                            );
                            eprintln!("[{}] {msg} — expand skipped this tick", ctx.name);
                            trace_fail(&ctx.name, &subject, msg);
                            break;
                        }
                        Err(e) => {
                            eprintln!("[{}] {}: {e} — expand skipped this tick", ctx.name, spec.name);
                            trace_fail(&ctx.name, &subject, format!("{}: {e}", spec.name));
                            break;
                        }
                    }
                }
            }
        }
    }
}

// ───────────────────────── connector traces ─────────────────────────

fn trace_preview(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).chars().take(160).collect()
}

/// A source publish, in the trace ring (the panel's "is it flowing?").
fn trace_pub(name: &str, subject: &str, bytes: &[u8]) {
    crate::record_trace_full(
        name, subject, true, None,
        vec![subject.to_string()], trace_preview(bytes), None, None,
    );
}

/// A source-side failure worth showing in the panel.
fn trace_fail(name: &str, subject: &str, err: String) {
    crate::record_trace_full(name, subject, false, Some(err), vec![], String::new(), None, None);
}

/// A sink's response-body summary (what the downstream API answered).
fn summarize(bytes: &[u8]) -> Option<String> {
    let s = String::from_utf8_lossy(bytes);
    let s = s.trim();
    if s.is_empty() {
        None
    } else {
        Some(s.chars().take(200).collect())
    }
}

// ───────────────────────── dead-letter queue (ADR-0015) ─────────────────────────
// Poison messages are parked, not dropped, in a DEDICATED JetStream stream on a
// sibling root (vxdlq.<unit>, never vx.> — overlapping stream subjects are
// forbidden and the DLQ's retention must be independent of the hot path). The
// caller acks the original ONLY when to_dlq() returns Ok (publish-before-ack).

pub const DLQ_STREAM: &str = "VEJAS_DLQ";
pub const DLQ_ROOT: &str = "vxdlq";
pub const DLQ_MAX_MSGS: i64 = 100_000;

/// Ensure the dead-letter stream exists (idempotent). Bounded + discard-oldest:
/// a full DLQ is itself an operator signal, never a silent unbounded growth.
pub fn ensure_dlq_stream(js: &nats::jetstream::JetStream) {
    let _ = js.add_stream(&nats::jetstream::StreamConfig {
        name: DLQ_STREAM.to_string(),
        subjects: vec![format!("{DLQ_ROOT}.>")],
        max_msgs: DLQ_MAX_MSGS,
        discard: nats::jetstream::DiscardPolicy::Old,
        ..Default::default()
    });
}

/// A unit name (e.g. "connector:sap_out", "flow:orders") as a NATS subject token.
pub fn dlq_unit_token(unit: &str) -> String {
    unit.chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' })
        .collect()
}

fn epoch_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Park one poison message in the DLQ as a death envelope. Returns Ok only when
/// JetStream has stored it — the caller must ack the original only on Ok.
#[allow(clippy::too_many_arguments)]
pub fn to_dlq(
    js: &nats::jetstream::JetStream,
    unit: &str,
    subject: &str,
    attempts: i64,
    last_error: &str,
    payload: &[u8],
    version: &str,
) -> Result<(), String> {
    ensure_dlq_stream(js);
    let now = epoch_secs();
    let body = serde_json::to_vec(&serde_json::json!({
        "original_subject": subject,
        "unit": unit,
        "attempts": attempts,
        "first_seen": now,
        "dead_at": now,
        "last_error": last_error,
        // The version (content hash) that failed this message (ADR-0021). Empty
        // for unversioned units. So a replay after a promote to v2 is legible:
        // "this died under v1 — does it still fail under the current version?"
        "version": version,
        // Raw text of the original message — replayed verbatim, so JSON and
        // non-JSON alike re-inject faithfully; the panel pretty-prints it if JSON.
        "payload": String::from_utf8_lossy(payload),
    }))
    .map_err(|e| e.to_string())?;
    let dlq_subject = format!("{DLQ_ROOT}.{}", dlq_unit_token(unit));
    js.publish(&dlq_subject, body).map(|_| ()).map_err(|e| e.to_string())
}

// ───────────────────────── exec-source offset resume (ADR-0011/0021) ─────────────────────────
//
// A generic resumption seam for exec stream sources whose remote has an offset
// (Kafka via kcat, above all): the driver reads the last committed offset from a
// KV bucket at (re)start and hands it to the child as `$OFFSET`, and commits each
// record's offset AFTER publishing it to the bus (publish-before-commit, so a
// crash re-consumes from the last committed offset — at-least-once, our model,
// not the remote's opaque consumer-group rebalancing).

pub const OFFSET_BUCKET: &str = "VEJAS_OFFSETS";

pub fn open_offset_store(js: &nats::jetstream::JetStream) -> Option<nats::kv::Store> {
    let cfg = nats::kv::Config {
        bucket: OFFSET_BUCKET.to_string(),
        history: 1,
        ..Default::default()
    };
    js.create_key_value(&cfg)
        .or_else(|_| js.key_value(OFFSET_BUCKET))
        .ok()
}

pub fn offset_get(store: &nats::kv::Store, key: &str) -> Option<String> {
    store
        .get(key)
        .ok()
        .flatten()
        .map(|b| String::from_utf8_lossy(&b).trim().to_string())
        .filter(|s| !s.is_empty())
}

pub fn offset_put(store: &nats::kv::Store, key: &str, value: &str) {
    let _ = store.put(key, value.as_bytes());
}

// ───────────────────────── promote audit trail (ADR-0018) ─────────────────────────

pub const AUDIT_STREAM: &str = "VEJAS_AUDIT";
pub const AUDIT_ROOT: &str = "vxaudit";
// Generous: the audit trail must not evict on churn. The oldest-discard cap is a
// last-resort safety bound (a full audit stream is itself an operator signal),
// never a routine truncation — like the DLQ, on its own sibling root so no
// `max_age` on the hot stream can touch it.
pub const AUDIT_MAX_MSGS: i64 = 1_000_000;

pub fn ensure_audit_stream(js: &nats::jetstream::JetStream) {
    let _ = js.add_stream(&nats::jetstream::StreamConfig {
        name: AUDIT_STREAM.to_string(),
        subjects: vec![format!("{AUDIT_ROOT}.>")],
        max_msgs: AUDIT_MAX_MSGS,
        discard: nats::jetstream::DiscardPolicy::Old,
        ..Default::default()
    });
}

/// Append one promote record to the audit stream. `record` carries file, name,
/// key, before, after, actor, ts. Publish-confirmed (JetStream stores it) so the
/// caller can trust the trail; the live-promote path logs but does not fail the
/// promote if this errors (git remains the trail for committed changes).
pub fn to_audit(
    js: &nats::jetstream::JetStream,
    unit: &str,
    record: &serde_json::Value,
) -> Result<(), String> {
    ensure_audit_stream(js);
    let body = serde_json::to_vec(record).map_err(|e| e.to_string())?;
    let subject = format!("{AUDIT_ROOT}.{}", dlq_unit_token(unit));
    js.publish(&subject, body).map(|_| ()).map_err(|e| e.to_string())
}

/// Recent audit records for a unit, oldest last (read-only, ephemeral consumer).
pub fn audit_recent(
    js: &nats::jetstream::JetStream,
    unit: &str,
    n: usize,
) -> Result<Vec<serde_json::Value>, String> {
    let subject = format!("{AUDIT_ROOT}.{}", dlq_unit_token(unit));
    Ok(hydrate_recent(js, AUDIT_STREAM, &subject, n)?
        .into_iter()
        .map(|(_, _, v)| v)
        .collect())
}

// ───────────────────────── sinks ─────────────────────────

/// Shared sink loop: durable pull consumer -> handler; ack on Ok, nak on Err.
/// The handler may return a response summary, recorded in the trace ring so
/// the panel shows what the downstream answered (rejected facts, skips…).
fn run_sink(
    ctx: &Ctx,
    subject: &str,
    handler: impl Fn(&[u8]) -> Result<Option<String>, String>,
) -> Result<(), String> {
    let js = ctx.jetstream()?;
    let durable = ctx.name.replace([':', '.', '-'], "_");
    let _ = js.add_consumer(
        &ctx.stream,
        nats::jetstream::ConsumerConfig {
            durable_name: Some(durable.clone()),
            filter_subject: subject.to_string(),
            ack_wait: ack_wait(),
            ..Default::default()
        },
    );
    let sub = js
        .pull_subscribe_with_options(
            subject,
            &nats::jetstream::PullSubscribeOptions::new().durable_name(durable),
        )
        .map_err(|e| e.to_string())?;
    eprintln!("[{}] consuming {subject}", ctx.name);
    loop {
        if !ctx.alive() {
            return Ok(());
        }
        for msg in fetch_round(&sub)? {
            if !ctx.alive() {
                // stopped mid-batch: leave the message un-acked, it redelivers
                return Ok(());
            }
            let preview = trace_preview(&msg.data);
            let t0 = Instant::now();
            let start_nanos = crate::metrics::now_nanos();
            match handler(&msg.data) {
                Ok(response) => {
                    crate::metrics::observe(&ctx.name, true, 0, t0.elapsed().as_secs_f64());
                    crate::metrics::span(crate::metrics::Span {
                        unit: ctx.name.clone(), subject: subject.to_string(), ok: true,
                        error: None, start_nanos, end_nanos: crate::metrics::now_nanos(), emits: 0,
                    });
                    crate::record_trace_full(
                        &ctx.name, subject, true, None, vec![], preview, None, response,
                    );
                    let _ = msg.ack();
                }
                Err(e) => {
                    crate::metrics::observe(&ctx.name, false, 0, t0.elapsed().as_secs_f64());
                    crate::metrics::span(crate::metrics::Span {
                        unit: ctx.name.clone(), subject: subject.to_string(), ok: false,
                        error: Some(e.clone()), start_nanos, end_nanos: crate::metrics::now_nanos(), emits: 0,
                    });
                    // poison guard: give up after MAX_DELIVERIES attempts
                    let delivered =
                        msg.jetstream_message_info().map(|i| i.delivered).unwrap_or(1);
                    if delivered >= crate::MAX_DELIVERIES {
                        // Poison: park it in the DLQ, then ack — publish before ack
                        // so it is never lost (ADR-0015). On DLQ failure, nak.
                        match to_dlq(&js, &ctx.name, subject, delivered, &e, &msg.data, "") {
                            Ok(()) => {
                                crate::metrics::inc_dead_letter(&ctx.name);
                                eprintln!("[{}] {e} -> DLQ after {delivered} deliveries", ctx.name);
                                crate::record_trace_full(
                                    &ctx.name, subject, false,
                                    Some(format!("{e} — dead-lettered after {delivered} deliveries")),
                                    vec![], preview, None, None,
                                );
                                let _ = msg.ack();
                            }
                            Err(de) => {
                                eprintln!("[{}] {e} -> DLQ publish failed ({de}); nak", ctx.name);
                                crate::record_trace_full(
                                    &ctx.name, subject, false,
                                    Some(format!("{e} — DLQ publish failed: {de}")),
                                    vec![], preview, None, None,
                                );
                                let _ = msg.ack_kind(nats::jetstream::AckKind::Nak);
                            }
                        }
                    } else {
                        eprintln!("[{}] {e} -> nak", ctx.name);
                        crate::record_trace_full(
                            &ctx.name, subject, false, Some(e), vec![], preview, None, None,
                        );
                        let _ = msg.ack_kind(nats::jetstream::AckKind::Nak);
                    }
                }
            }
        }
    }
}

struct SlackOut;
impl Driver for SlackOut {
    fn kind(&self) -> &'static str {
        "sink"
    }
    fn about(&self) -> &'static str {
        "Consumes vx.slack.out and posts {text} to a Slack webhook (DRY-RUN if unset). Config: SUBJECT, WEBHOOK_URL."
    }
    fn run(&self, ctx: &Ctx) -> Result<(), String> {
        let subject = ctx.subject(&ctx.config.str_or("SUBJECT", "slack.out"));
        let webhook = ctx
            .config
            .str("WEBHOOK_URL")
            .or_else(|| std::env::var("SLACK_WEBHOOK_URL").ok())
            .unwrap_or_default();
        let name = ctx.name.clone();
        run_sink(ctx, &subject, move |data| {
            let text = serde_json::from_slice::<Value>(data)
                .ok()
                .and_then(|v| v["text"].as_str().map(|s| s.to_string()))
                .unwrap_or_else(|| String::from_utf8_lossy(data).into_owned());
            if webhook.is_empty() {
                eprintln!("[{name}] DRY-RUN would post: {text}");
                Ok(Some(format!("DRY-RUN: {}", text.chars().take(160).collect::<String>())))
            } else {
                let payload = serde_json::json!({ "text": text }).to_string();
                let headers = [("Content-Type".to_string(), "application/json".to_string())];
                http_post(&webhook, &headers, payload.as_bytes()).map(|b| summarize(&b))
            }
        })
    }
}

struct HttpOut;
impl Driver for HttpOut {
    fn kind(&self) -> &'static str {
        "sink"
    }
    fn about(&self) -> &'static str {
        "Consumes a subject and POSTs each message body to a URL. Config: SUBJECT, URL, HEADERS (optional doc — put credentials there via secret(), e.g. {\"Authorization\": …}), TEST_BODY (optional harmless payload enabling the test probe)."
    }
    fn probe(&self, ctx: &Ctx) -> Result<String, String> {
        let url = ctx.config.str("URL").ok_or("URL required")?;
        let body = ctx.config.value("TEST_BODY").ok_or(
            "no TEST_BODY in the manifest: add a harmless payload the target accepts (e.g. an empty batch) to enable this probe",
        )?;
        let mut headers = ctx.config.headers();
        if !headers.iter().any(|(k, _)| k.eq_ignore_ascii_case("content-type")) {
            headers.push(("Content-Type".into(), "application/json".into()));
        }
        let bytes = serde_json::to_vec(&body).map_err(|e| e.to_string())?;
        match http_request("POST", &url, &headers, Some(&bytes)) {
            Ok((code, resp)) if (200..300).contains(&code) => Ok(format!(
                "POST OK · answered: {}",
                summarize(&resp).unwrap_or_else(|| "(empty)".into())
            )),
            Ok((401 | 403, _)) => Err(humanize(&format!("HTTP 401: authentication rejected by {url}"))),
            // any other answer past auth still proves network + credentials;
            // only the probe payload was refused
            Ok((code, resp)) => Ok(format!(
                "reachable and authenticated · but TEST_BODY was refused (HTTP {code}: {})",
                summarize(&resp).unwrap_or_default()
            )),
            Err(e) => Err(humanize(&e)),
        }
    }
    fn run(&self, ctx: &Ctx) -> Result<(), String> {
        let subject = ctx.subject(&ctx.config.str("SUBJECT").ok_or("SUBJECT required")?);
        let url = ctx.config.str("URL").ok_or("URL required")?;
        let mut headers = ctx.config.headers();
        if !headers.iter().any(|(k, _)| k.eq_ignore_ascii_case("content-type")) {
            headers.push(("Content-Type".into(), "application/json".into()));
        }
        run_sink(ctx, &subject, move |data| http_post(&url, &headers, data).map(|b| summarize(&b)))
    }
}

// ───────────────────────── external process connectors ─────────────────────
//
// The hot-add extension path for NEW connector types without recompiling the
// core and without loading native libraries (no .so/.dll, no unstable ABI, no
// in-process arbitrary code). A connector is an external program in ANY
// language; the runtime bridges it to the bus over stdio, so the program needs
// no NATS client. Isolation is by process. See ADR-0011.

struct ExecSource;
impl Driver for ExecSource {
    fn kind(&self) -> &'static str {
        "source:exec"
    }
    fn about(&self) -> &'static str {
        "Runs CMD every INTERVAL_SECS; each JSON line it prints on stdout is published to SUBJECT. Any language, no NATS client. Config: CMD, SUBJECT, INTERVAL_SECS."
    }
    fn run(&self, ctx: &Ctx) -> Result<(), String> {
        let cmd = ctx.config.str("CMD").ok_or("CMD required")?;
        let subject = ctx.subject(&ctx.config.str("SUBJECT").ok_or("SUBJECT required")?);
        let interval = ctx.config.u64_or("INTERVAL_SECS", 10).max(1);
        let js = ctx.jetstream()?;
        eprintln!("[{}] exec-source: `{cmd}` every {interval}s -> {subject}", ctx.name);
        let mut waited = interval * 1000; // fire immediately on start
        loop {
            if !ctx.alive() {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(250));
            waited += 250;
            if waited >= interval * 1000 {
                waited = 0;
                let out = std::process::Command::new("sh")
                    .arg("-c")
                    .arg(&cmd)
                    .output()
                    .map_err(|e| e.to_string())?;
                if !out.status.success() {
                    let err = String::from_utf8_lossy(&out.stderr).trim().to_string();
                    eprintln!("[{}] exec: {err}", ctx.name);
                    trace_fail(&ctx.name, &subject, format!("exec: {err}"));
                }
                for line in out.stdout.split(|&b| b == b'\n') {
                    let line = line.strip_suffix(b"\r").unwrap_or(line);
                    if line.is_empty() {
                        continue;
                    }
                    if serde_json::from_slice::<Value>(line).is_ok() {
                        let _ = js.publish(&subject, line);
                        trace_pub(&ctx.name, &subject, line);
                    } else {
                        eprintln!("[{}] exec: non-JSON line skipped", ctx.name);
                    }
                }
            }
        }
    }
}

struct ExecSink;
impl Driver for ExecSink {
    fn kind(&self) -> &'static str {
        "sink:exec"
    }
    fn about(&self) -> &'static str {
        "Consumes SUBJECT; pipes each message body to CMD's stdin. Any language, no NATS client. ENV = {\"KEY\": secret(\"…\")} is handed to the child environment (secrets never touch argv). Config: CMD, SUBJECT, ENV (optional)."
    }
    fn run(&self, ctx: &Ctx) -> Result<(), String> {
        let cmd = ctx.config.str("CMD").ok_or("CMD required")?;
        let subject = ctx.subject(&ctx.config.str("SUBJECT").ok_or("SUBJECT required")?);
        let name = ctx.name.clone();
        let env = ctx.config.env_vars();
        run_sink(ctx, &subject, move |data| {
            let mut child = std::process::Command::new("sh")
                .arg("-c")
                .arg(&cmd)
                .envs(env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .map_err(|e| e.to_string())?;
            child
                .stdin
                .take()
                .ok_or("no stdin")?
                .write_all(data)
                .map_err(|e| e.to_string())?;
            let out = child.wait_with_output().map_err(|e| e.to_string())?;
            if out.status.success() {
                Ok(summarize(&out.stdout))
            } else {
                Err(format!("{name}: exit {:?}: {}", out.status.code(), String::from_utf8_lossy(&out.stderr).trim()))
            }
        })
    }
}

// ───────────────────────── source: streaming exec ─────────────────────────

struct ExecStreamSource;
impl Driver for ExecStreamSource {
    fn kind(&self) -> &'static str {
        "source:stream"
    }
    fn about(&self) -> &'static str {
        "Runs CMD as a long-running process and publishes each JSON line it streams on stdout to SUBJECT — line by line, with back-pressure (a bounded internal buffer; when the bus is slow the child's stdout blocks, so a burst can't overrun memory). For push sources like SAP IDoc inbound, Salesforce Bulk 2.0, and Kafka (wrap kcat). ENV = {\"KEY\": secret(\"…\")} is handed to the child's environment (secrets never touch argv). On child exit it restarts after RESTART_SECS. Optional offset RESUME (Kafka etc.): set OFFSET_KV to a key and the driver hands the child the last committed offset as $OFFSET at (re)start and commits each record's OFFSET_FIELD (default \"offset\") after publishing it — publish-before-commit, at-least-once, resume in OUR JetStream KV; OFFSET_START (default \"end\") is used on the first run. Config: CMD, SUBJECT, ENV (optional), RESTART_SECS (optional, default 2), OFFSET_KV/OFFSET_FIELD/OFFSET_START (optional)."
    }
    fn run(&self, ctx: &Ctx) -> Result<(), String> {
        use std::io::{BufRead, BufReader};
        use std::process::{Command, Stdio};
        use std::sync::mpsc::{sync_channel, RecvTimeoutError};

        let cmd = ctx.config.str("CMD").ok_or("CMD required")?;
        let subject = ctx.subject(&ctx.config.str("SUBJECT").ok_or("SUBJECT required")?);
        let env = ctx.config.env_vars();
        let restart = ctx.config.u64_or("RESTART_SECS", 2).max(1);
        // js for the offset KV store; nc for publish_confirmed (this streams stdout
        // line-by-line sequentially — the exact ~200/s flusher-floor case).
        let (js, nc) = ctx.jetstream_and_conn()?;
        // Optional offset resume: when OFFSET_KV is set, hand the child the last
        // committed offset as $OFFSET at (re)start (e.g. `kcat -o $OFFSET`) and
        // commit each record's OFFSET_FIELD after publishing it.
        let offset_kv = ctx.config.str("OFFSET_KV");
        let offset_field = ctx.config.str("OFFSET_FIELD").unwrap_or_else(|| "offset".into());
        let offset_start = ctx.config.str("OFFSET_START").unwrap_or_else(|| "end".into());
        let offset_store = offset_kv.as_ref().and_then(|_| open_offset_store(&js));
        eprintln!("[{}] exec-stream-source: `{cmd}` -> {subject} (streaming)", ctx.name);

        while ctx.alive() {
            // resume from the last committed offset (or OFFSET_START on first run)
            let mut run_env = env.clone();
            if let (Some(store), Some(key)) = (&offset_store, &offset_kv) {
                let start = offset_get(store, key).unwrap_or_else(|| offset_start.clone());
                eprintln!("[{}] resuming at OFFSET={start}", ctx.name);
                run_env.push(("OFFSET".to_string(), start));
            }
            let mut child = Command::new("sh")
                .arg("-c")
                .arg(&cmd)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit()) // child status/errors flow to our logs
                .envs(run_env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
                .spawn()
                .map_err(|e| e.to_string())?;
            let stdout = child.stdout.take().ok_or("no child stdout")?;

            // Bounded channel = back-pressure: when we stop consuming (slow bus),
            // the reader blocks on send, the OS pipe fills, the child blocks on
            // write. Memory stays bounded no matter how fast SAP pushes.
            let (tx, rx) = sync_channel::<String>(64);
            let reader = thread::spawn(move || {
                for line in BufReader::new(stdout).lines() {
                    match line {
                        Ok(l) => {
                            if tx.send(l).is_err() {
                                break; // consumer gone
                            }
                        }
                        Err(_) => break,
                    }
                }
            });

            loop {
                if !ctx.alive() {
                    let _ = child.kill();
                    break;
                }
                match rx.recv_timeout(Duration::from_millis(250)) {
                    Ok(line) => {
                        let line = line.trim();
                        if line.is_empty() {
                            continue;
                        }
                        if let Ok(rec) = serde_json::from_str::<Value>(line) {
                            match publish_confirmed(&nc, &subject, line.as_bytes(), Duration::from_secs(5)) {
                                Ok(_) => {
                                    trace_pub(&ctx.name, &subject, line.as_bytes());
                                    // publish-before-commit: store next offset only
                                    // after the record is on the bus (at-least-once).
                                    if let (Some(store), Some(key)) = (&offset_store, &offset_kv) {
                                        if let Some(off) =
                                            rec.get(&offset_field).and_then(|x| x.as_i64())
                                        {
                                            offset_put(store, key, &(off + 1).to_string());
                                        }
                                    }
                                }
                                Err(e) => {
                                    trace_fail(&ctx.name, &subject, format!("publish: {e}"))
                                }
                            }
                        } else {
                            eprintln!("[{}] stream: non-JSON line skipped", ctx.name);
                        }
                    }
                    Err(RecvTimeoutError::Timeout) => continue,
                    Err(RecvTimeoutError::Disconnected) => break, // child stdout closed
                }
            }

            let _ = reader.join();
            let status = child.wait();
            if !ctx.alive() {
                return Ok(());
            }
            eprintln!(
                "[{}] stream process ended ({:?}); restart in {restart}s",
                ctx.name,
                status.ok().and_then(|s| s.code())
            );
            let mut slept = 0;
            while slept < restart * 1000 && ctx.alive() {
                thread::sleep(Duration::from_millis(200));
                slept += 200;
            }
        }
        Ok(())
    }
}

// ───────────────────────── MQTT (hand-rolled sync, ADR-0025) ─────────────────────────

/// The MQTT client's stream: plain TCP or a rustls TLS stream. One monomorphic
/// type so `Client<MqttTransport>` covers both without a trait object.
enum MqttTransport {
    Plain(std::net::TcpStream),
    Tls(Box<rustls::StreamOwned<rustls::ClientConnection, std::net::TcpStream>>),
}
impl std::io::Read for MqttTransport {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(s) => s.read(buf),
            Self::Tls(s) => s.read(buf),
        }
    }
}
impl std::io::Write for MqttTransport {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(s) => s.write(buf),
            Self::Tls(s) => s.write(buf),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Plain(s) => s.flush(),
            Self::Tls(s) => s.flush(),
        }
    }
}

/// Shared rustls client config (Mozilla roots via webpki-roots), built once.
fn mqtt_tls_config() -> std::sync::Arc<rustls::ClientConfig> {
    static CFG: std::sync::OnceLock<std::sync::Arc<rustls::ClientConfig>> = std::sync::OnceLock::new();
    CFG.get_or_init(|| {
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        std::sync::Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        )
    })
    .clone()
}

/// Connect an MQTT session (plain TCP or TLS) with the given read timeout (drives
/// the keepalive tick / PUBACK wait). The read timeout is set on the underlying
/// TcpStream before the TLS wrap, so an idle keepalive read times out cleanly
/// before any TLS record starts.
fn mqtt_connect(
    broker: &str,
    client_id: &str,
    clean_session: bool,
    user: Option<&str>,
    pass: Option<&str>,
    keepalive: u16,
    read_timeout: Duration,
    tls: bool,
) -> Result<crate::mqtt::Client<MqttTransport>, String> {
    let tcp = std::net::TcpStream::connect(broker).map_err(|e| e.to_string())?;
    tcp.set_read_timeout(Some(read_timeout)).ok();
    tcp.set_nodelay(true).ok();
    let transport = if tls {
        let host = broker.rsplit_once(':').map(|(h, _)| h).unwrap_or(broker);
        let name = rustls::pki_types::ServerName::try_from(host.to_string())
            .map_err(|e| format!("bad TLS host {host:?}: {e}"))?;
        let conn = rustls::ClientConnection::new(mqtt_tls_config(), name)
            .map_err(|e| e.to_string())?;
        MqttTransport::Tls(Box::new(rustls::StreamOwned::new(conn, tcp)))
    } else {
        MqttTransport::Plain(tcp)
    };
    let mut client = crate::mqtt::Client::new(transport, keepalive);
    client.connect(client_id, clean_session, user, pass)?;
    Ok(client)
}

fn mqtt_client_id(ctx: &Ctx) -> String {
    ctx.config
        .str("CLIENT_ID")
        .unwrap_or_else(|| format!("vejas-{}", ctx.name.replace([':', '.'], "-")))
}

struct MqttIn;
impl Driver for MqttIn {
    fn kind(&self) -> &'static str {
        // structurally singleton in 3.1.1: shared subscriptions are MQTT5-only, so
        // two subscribers on the same client id can't split a topic (ADR-0025).
        "source:mqtt"
    }
    fn about(&self) -> &'static str {
        "Subscribes to an MQTT topic and publishes each message to SUBJECT (hand-rolled sync client, 3.1.1, QoS 0/1). At-least-once maps onto QoS 1 with no KV: the PUBACK to the broker is sent only AFTER the bus publish is confirmed, so the broker retransmits anything not yet acked — CLEAN_SESSION=false keeps the subscription across reconnects. Singleton (structural in 3.1.1). Config: BROKER (host:port), TOPIC, SUBJECT, QOS (0/1, default 1), CLIENT_ID (default vejas-<name>), USERNAME/PASSWORD (secret()), KEEPALIVE_SECS (default 30), TLS (bool, default false → rustls). QoS2/MQTT5 → mosquitto exec-bridge."
    }
    fn run(&self, ctx: &Ctx) -> Result<(), String> {
        let broker = ctx.config.str("BROKER").ok_or("BROKER required")?;
        let topic = ctx.config.str("TOPIC").ok_or("TOPIC required")?;
        let subject = ctx.subject(&ctx.config.str("SUBJECT").ok_or("SUBJECT required")?);
        let qos = ctx.config.u64_or("QOS", 1).min(1) as u8;
        let keepalive = ctx.config.u64_or("KEEPALIVE_SECS", 30) as u16;
        let tls = ctx.config.value("TLS").and_then(|v| v.as_bool()).unwrap_or(false);
        let client_id = mqtt_client_id(ctx);
        let user = ctx.config.str("USERNAME");
        let pass = ctx.config.str("PASSWORD");
        // nc for publish_confirmed (direct flush past the 5ms flusher floor — the
        // MQTT-loopback ceiling); _js keeps the stream ensured.
        let (_js, nc) = ctx.jetstream_and_conn()?;
        while ctx.alive() {
            let attempt = (|| -> Result<(), String> {
                let mut client = mqtt_connect(
                    &broker, &client_id, false, user.as_deref(), pass.as_deref(),
                    keepalive, Duration::from_millis(500), tls,
                )?;
                client.subscribe(&topic, qos)?;
                eprintln!("[{}] mqtt subscribed {topic} @qos{qos} -> {subject}", ctx.name);
                while ctx.alive() {
                    match client.read_packet()? {
                        Some(crate::mqtt::Packet::Publish { payload, qos: pq, pid, .. }) => {
                            // publish-before-ack: bus first (await JetStream pub-ack),
                            // THEN PUBACK — a crash before the bus write re-delivers
                            // from the broker (ADR-0025).
                            publish_confirmed(&nc, &subject, &payload, Duration::from_secs(5))
                                .map_err(|e| format!("bus publish: {e}"))?;
                            trace_pub(&ctx.name, &subject, &payload);
                            if pq > 0 {
                                client.puback(pid)?;
                            }
                        }
                        Some(_) => {} // PingResp / other
                        None => client.keepalive_tick()?, // idle read → maybe PINGREQ
                    }
                }
                client.disconnect();
                Ok(())
            })();
            match attempt {
                Ok(()) => break,
                Err(e) => {
                    trace_fail(&ctx.name, &subject, e.clone());
                    eprintln!("[{}] mqtt: {e}; reconnect in 2s", ctx.name);
                    let mut slept = 0;
                    while slept < 2000 && ctx.alive() {
                        thread::sleep(Duration::from_millis(200));
                        slept += 200;
                    }
                }
            }
        }
        Ok(())
    }
}

struct MqttOut;
impl Driver for MqttOut {
    fn kind(&self) -> &'static str {
        "sink:mqtt"
    }
    fn about(&self) -> &'static str {
        "Consumes SUBJECT and PUBLISHes each message to an MQTT topic (hand-rolled sync client, 3.1.1, QoS 0/1). QoS 1: PUBLISH → await the broker's PUBACK → THEN ack the bus message (side-effect-before-ack); a crash between them redelivers → duplicate PUBLISH (standard at-least-once). Competing-safe (durable pull). Config: BROKER, TOPIC, SUBJECT, QOS (default 1), CLIENT_ID, USERNAME/PASSWORD (secret()), KEEPALIVE_SECS. TLS + QoS2/MQTT5 → mosquitto exec-bridge."
    }
    fn run(&self, ctx: &Ctx) -> Result<(), String> {
        let broker = ctx.config.str("BROKER").ok_or("BROKER required")?;
        let topic = ctx.config.str("TOPIC").ok_or("TOPIC required")?;
        let subject = ctx.subject(&ctx.config.str("SUBJECT").ok_or("SUBJECT required")?);
        let qos = ctx.config.u64_or("QOS", 1).min(1) as u8;
        let keepalive = ctx.config.u64_or("KEEPALIVE_SECS", 30) as u16;
        let tls = ctx.config.value("TLS").and_then(|v| v.as_bool()).unwrap_or(false);
        let client_id = mqtt_client_id(ctx);
        let user = ctx.config.str("USERNAME");
        let pass = ctx.config.str("PASSWORD");
        // one MQTT session, reconnected on error (Mutex<Option<_>> for the Fn handler)
        let slot: std::sync::Mutex<Option<crate::mqtt::Client<MqttTransport>>> =
            std::sync::Mutex::new(None);
        run_sink(ctx, &subject, move |data| {
            let mut guard = slot.lock().unwrap();
            if guard.is_none() {
                *guard = Some(mqtt_connect(
                    &broker, &client_id, true, user.as_deref(), pass.as_deref(),
                    keepalive, Duration::from_secs(5), tls,
                )?);
            }
            let client = guard.as_mut().unwrap();
            let publish = (|| -> Result<(), String> {
                let pid = client.publish(&topic, data, qos)?;
                if let Some(want) = pid {
                    // side-effect-before-ack: wait for the broker's PUBACK
                    loop {
                        match client.read_packet()? {
                            Some(crate::mqtt::Packet::PubAck { pid }) if pid == want => break,
                            Some(_) => {}
                            None => return Err("timed out waiting for PUBACK".into()),
                        }
                    }
                }
                Ok(())
            })();
            if publish.is_err() {
                *guard = None; // drop the broken session so the next call reconnects
            }
            publish.map(|_| None) // Ok → ack the bus message (side-effect done)
        })
    }
}

// ───────────────────────── rpc: request/reply exec ─────────────────────────

struct ExecRpc;
impl Driver for ExecRpc {
    fn kind(&self) -> &'static str {
        "rpc:exec"
    }
    fn about(&self) -> &'static str {
        "Runs CMD as a long-running request/reply process (one JSON request per line on stdin -> one JSON reply per line on stdout) and serves it over NATS request/reply on REQUEST_SUBJECT — so MCP tools (and flows) can drive an interactive connector like SAP (sap_list / sap_describe / sap_call). Keep REQUEST_SUBJECT OUTSIDE the vx.* JetStream subjects (e.g. \"vxrpc.sap\"). ENV = {\"KEY\": secret(\"…\")} is handed to the child environment (secrets never touch argv). One SAP logon is held open and requests are serialized. Config: CMD, REQUEST_SUBJECT, ENV (optional)."
    }
    fn run(&self, ctx: &Ctx) -> Result<(), String> {
        use std::io::{BufRead, BufReader, Write};
        use std::process::{Command, Stdio};

        let cmd = ctx.config.str("CMD").ok_or("CMD required")?;
        let subject = ctx
            .config
            .str("REQUEST_SUBJECT")
            .ok_or("REQUEST_SUBJECT required")?;
        if subject.starts_with(&format!("{}.", ctx.subj_root)) {
            return Err(format!(
                "REQUEST_SUBJECT must not be under {}.* (JetStream-captured); use e.g. vxrpc.sap",
                ctx.subj_root
            ));
        }
        let env = ctx.config.env_vars();
        let nc = nats::connect(&ctx.nats_url).map_err(|e| e.to_string())?;

        let mut child = Command::new("sh")
            .arg("-c")
            .arg(&cmd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .envs(env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
            .spawn()
            .map_err(|e| e.to_string())?;
        let mut stdin = child.stdin.take().ok_or("no child stdin")?;
        let mut stdout = BufReader::new(child.stdout.take().ok_or("no child stdout")?);
        // The process announces readiness with a first stdout line — consume it
        // so it isn't mistaken for a reply.
        let mut ready = String::new();
        stdout.read_line(&mut ready).map_err(|e| e.to_string())?;
        eprintln!("[{}] exec-rpc: `{cmd}` serving {subject}", ctx.name);

        // Queue group (not a plain subscribe) so N clustered instances share the
        // requests — exactly one answers each, load-balanced (ADR-0020). Each
        // instance keeps its own child process (e.g. its own SAP logon).
        let sub = nc
            .queue_subscribe(&subject, "vejas")
            .map_err(|e| e.to_string())?;
        loop {
            if !ctx.alive() {
                let _ = child.kill();
                return Ok(());
            }
            match sub.next_timeout(Duration::from_millis(300)) {
                Ok(msg) => {
                    // One request line in, one reply line out (serialized).
                    let mut req = msg.data.clone();
                    req.push(b'\n');
                    if stdin.write_all(&req).is_err() {
                        return Err("child stdin closed".into());
                    }
                    let _ = stdin.flush();
                    let mut line = String::new();
                    match stdout.read_line(&mut line) {
                        Ok(0) => return Err("child stdout closed".into()),
                        Ok(_) => {
                            let _ = msg.respond(line.trim().as_bytes());
                            trace_pub(&ctx.name, &subject, line.trim().as_bytes());
                        }
                        Err(e) => return Err(e.to_string()),
                    }
                }
                Err(_) => {
                    // Idle timeout: notice if the child died so we restart.
                    if let Ok(Some(status)) = child.try_wait() {
                        return Err(format!("child exited: {:?}", status.code()));
                    }
                }
            }
        }
    }
    fn probe(&self, ctx: &Ctx) -> Result<String, String> {
        use std::io::{BufRead, BufReader, Write};
        use std::process::{Command, Stdio};
        let cmd = ctx.config.str("CMD").ok_or("CMD required")?;
        let env = ctx.config.env_vars();
        let mut child = Command::new("sh")
            .arg("-c")
            .arg(&cmd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .envs(env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
            .spawn()
            .map_err(|e| e.to_string())?;
        let mut stdin = child.stdin.take().ok_or("no child stdin")?;
        let mut stdout = BufReader::new(child.stdout.take().ok_or("no child stdout")?);
        let mut ready = String::new();
        stdout.read_line(&mut ready).map_err(|e| e.to_string())?; // ready line
        stdin
            .write_all(b"{\"op\":\"ping\"}\n")
            .map_err(|e| e.to_string())?;
        let _ = stdin.flush();
        let mut line = String::new();
        stdout.read_line(&mut line).map_err(|e| e.to_string())?;
        let _ = child.kill();
        let v: Value = serde_json::from_str(line.trim()).map_err(|e| e.to_string())?;
        if v.get("ok").and_then(|b| b.as_bool()).unwrap_or(false) {
            Ok(format!("ping ok — {}", line.trim()))
        } else {
            Err(format!("ping failed: {}", line.trim()))
        }
    }
}

// ───────────────────────── HTTP client (ureq/rustls, in-binary) ─────────────────────────
// A pure-Rust HTTP client compiled into the runtime — no `curl` shell-out, no
// process-per-request, and nothing sensitive on argv (/proc/<pid>/cmdline is
// world-readable): the URL and every header, Authorization above all, live only
// in memory. This is what let `curl` leave the Docker image entirely.

/// A pooled, blocking HTTP(S) agent shared by http-out / http-poll / oauth-poll,
/// so connections stay alive across requests (no process-per-request). Pure-Rust
/// TLS (rustls); credentials ride in the headers, in memory, never argv.
static HTTP_AGENT: std::sync::OnceLock<ureq::Agent> = std::sync::OnceLock::new();
fn http_agent() -> &'static ureq::Agent {
    HTTP_AGENT.get_or_init(|| {
        ureq::AgentBuilder::new()
            .timeout(Duration::from_secs(30))
            .user_agent("vejas")
            .build()
    })
}

/// One HTTP request. Returns (status, body) — the caller decides what a given
/// status means (401 → token refresh, etc.), so 4xx/5xx are handed back, not
/// errored.
pub fn http_request(
    method: &str,
    url: &str,
    headers: &[(String, String)],
    body: Option<&[u8]>,
) -> Result<(u16, Vec<u8>), String> {
    use std::io::Read as _;
    let mut req = http_agent().request(method, url);
    for (k, v) in headers {
        req = req.set(k, v);
    }
    let resp = match body {
        Some(b) => req.send_bytes(b),
        None => req.call(),
    };
    let response = match resp {
        Ok(r) => r,
        // A non-2xx status is not an error here — return it to the caller.
        Err(ureq::Error::Status(_code, r)) => r,
        Err(ureq::Error::Transport(t)) => return Err(format!("http: {t}")),
    };
    let code = response.status();
    let mut buf = Vec::new();
    response
        .into_reader()
        .read_to_end(&mut buf)
        .map_err(|e| e.to_string())?;
    Ok((code, buf))
}

fn http_get(url: &str, headers: &[(String, String)]) -> Result<Vec<u8>, String> {
    match http_request("GET", url, headers, None)? {
        (code, body) if (200..300).contains(&code) => Ok(body),
        (code, body) => Err(format!(
            "HTTP {code}: {}",
            String::from_utf8_lossy(&body[..body.len().min(300)]).trim()
        )),
    }
}

fn http_post(url: &str, headers: &[(String, String)], body: &[u8]) -> Result<Vec<u8>, String> {
    match http_request("POST", url, headers, Some(body))? {
        (code, out) if (200..300).contains(&code) => Ok(out),
        (code, out) => Err(format!(
            "HTTP {code}: {}",
            String::from_utf8_lossy(&out[..out.len().min(300)]).trim()
        )),
    }
}

/// Percent-encoding for form values (RFC 3986 unreserved set passes).
fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Seconds since the epoch → ISO 8601 UTC (civil-from-days, Hinnant).
pub fn iso8601_utc(secs: u64) -> String {
    let days = (secs / 86400) as i64;
    let rem = secs % 86400;
    let (h, m, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mo = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(mo <= 2);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{m:02}:{s:02}Z")
}

/// Join a base URL and an endpoint path; absolute endpoints pass through.
fn join_url(base: &str, endpoint: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("{}/{}", base.trim_end_matches('/'), endpoint.trim_start_matches('/'))
    }
}

/// The next page URL out of a response body, if any (absolute or relative).
fn next_url(base: &str, body: &serde_json::Value, field: &str) -> Option<String> {
    body.get(field)
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| join_url(base, s))
}

// ───────────────────────── tests ─────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn iso8601_epoch_billennium_and_leap() {
        assert_eq!(iso8601_utc(0), "1970-01-01T00:00:00Z");
        assert_eq!(iso8601_utc(1_000_000_000), "2001-09-09T01:46:40Z");
        assert_eq!(iso8601_utc(951_782_400), "2000-02-29T00:00:00Z"); // leap day
    }

    #[test]
    fn url_join_and_next_link() {
        assert_eq!(join_url("https://api.x/v1/", "/users?a=1"), "https://api.x/v1/users?a=1");
        assert_eq!(join_url("https://api.x/v1", "users"), "https://api.x/v1/users");
        assert_eq!(join_url("https://api.x/v1", "https://other/abs"), "https://other/abs");
        let body = json!({"value": [], "@odata.nextLink": "https://api.x/v1/users?$skip=2"});
        assert_eq!(
            next_url("https://api.x/v1", &body, "@odata.nextLink").as_deref(),
            Some("https://api.x/v1/users?$skip=2")
        );
        let rel = json!({"next": "/users?page=2"});
        assert_eq!(
            next_url("https://api.x/v1", &rel, "next").as_deref(),
            Some("https://api.x/v1/users?page=2")
        );
        assert_eq!(next_url("https://api.x/v1", &json!({"value": []}), "next"), None);
    }

    #[test]
    fn urlencode_encodes_reserved() {
        assert_eq!(urlencode("a-b_c.d~e"), "a-b_c.d~e");
        assert_eq!(urlencode("p@ss wörd+/="), "p%40ss%20w%C3%B6rd%2B%2F%3D");
    }

    #[test]
    fn timer_payload_gains_ts_once() {
        let stamped = stamp_ts(&json!({"origin": "collector"}), 1_000_000_000);
        assert_eq!(stamped, json!({"origin": "collector", "ts": "2001-09-09T01:46:40Z"}));
        // an explicit ts is preserved, non-objects pass through untouched
        assert_eq!(stamp_ts(&json!({"ts": "x"}), 0), json!({"ts": "x"}));
        assert_eq!(stamp_ts(&json!("ping"), 0), json!("ping"));
    }

    #[test]
    fn expand_template_and_config() {
        assert_eq!(
            fill_template("/users/{id}/authentication/methods", "id", "u-42"),
            "/users/u-42/authentication/methods"
        );
        let mut m = Map::new();
        m.insert(
            "EXPAND".into(),
            json!([{"name": "ua", "list": "/users", "detail": "/users/{id}/x", "key": "id", "as": "x"}]),
        );
        let ex = parse_expands(&Config(m)).unwrap();
        assert_eq!(ex.len(), 1);
        assert_eq!(ex[0].name, "ua");
        assert_eq!(ex[0].as_field, "x");
        assert_eq!(ex[0].list_field, "value"); // default à la Graph
        // list_field is configurable (CrowdStrike: "resources")
        let mut cs = Map::new();
        cs.insert("EXPAND".into(), json!([{"name":"d","list":"/q","detail":"/e?ids={id}","key":"id","as":"device","list_field":"resources"}]));
        let cex = parse_expands(&Config(cs)).unwrap();
        assert_eq!(cex[0].list_field, "resources");
        // malformed entries are an error, not a silent drop
        let mut bad = Map::new();
        bad.insert("EXPAND".into(), json!([{"name": "ua", "list": "/users"}]));
        assert!(parse_expands(&Config(bad)).is_err());
        assert!(parse_expands(&Config::default()).unwrap().is_empty());
    }

    #[test]
    fn config_headers_doc() {
        let mut m = Map::new();
        m.insert(
            "HEADERS".into(),
            json!({"Authorization": "Bearer x", "X-N": 42}),
        );
        let cfg = Config(m);
        // non-string values are ignored, string ones pass through
        assert_eq!(cfg.headers(), vec![("Authorization".to_string(), "Bearer x".to_string())]);
        assert!(Config::default().headers().is_empty());
    }

    /// Verifies `publish_confirmed` clears the nats flusher throttle AND keeps the
    /// at-least-once contract: messages actually persist, and a subject with no
    /// stream errors (never a false Ok). Needs a live NATS on 127.0.0.1:4222;
    /// excluded from CI. Run:
    /// `cargo test publish_confirmed_beats_throttle -- --ignored --nocapture`
    #[test]
    #[ignore]
    fn publish_confirmed_beats_throttle_and_persists() {
        let url = std::env::var("NATS_URL").unwrap_or_else(|_| "127.0.0.1:4222".into());
        let nc = nats::connect(&url).expect("connect");
        let js = nats::jetstream::new(nc.clone());
        let pid = std::process::id();
        let stream = format!("PBFAST_{pid}");
        let subject = format!("pbfast.{pid}.in");
        let _ = js.delete_stream(&stream);
        js.add_stream(nats::jetstream::StreamConfig {
            name: stream.clone(),
            subjects: vec![format!("pbfast.{pid}.>")],
            ..Default::default()
        })
        .expect("add_stream");

        const N: usize = 200;
        // (A) baseline: throttled js.publish
        let t = Instant::now();
        for _ in 0..N {
            js.publish(&subject, b"x").expect("js.publish");
        }
        let base = N as f64 / t.elapsed().as_secs_f64();
        // (B) direct-flush publish_confirmed
        let t = Instant::now();
        for _ in 0..N {
            publish_confirmed(&nc, &subject, b"x", Duration::from_secs(5)).expect("confirmed");
        }
        let fast = N as f64 / t.elapsed().as_secs_f64();
        eprintln!("[pbfast] js.publish={base:.0}/s  publish_confirmed={fast:.0}/s  ({:.1}x)", fast / base);
        assert!(fast > base * 2.0, "direct flush should clear the 5ms floor (got {fast:.0} vs {base:.0}/s)");

        // (C) durability: all 2N messages actually landed on the stream
        let msgs = js.stream_info(&stream).expect("info").state.messages;
        assert!(msgs >= (2 * N) as u64, "every confirmed publish persisted (have {msgs})");

        // (D) no-responders guard: a subject with no stream must ERROR, not false-Ok
        let orphan = format!("orphan.{pid}.no.stream");
        let r = publish_confirmed(&nc, &orphan, b"x", Duration::from_secs(1));
        assert!(r.is_err(), "publish to a stream-less subject must error, got {r:?}");

        let _ = js.delete_stream(&stream);
    }

    /// Latency probe for `fetch_round` — the shared pull loop every sink/flow runs.
    /// Measures (A) idle empty-round wall time and (B) publish→receipt latency when
    /// a message lands mid-idle. Needs a live NATS on 127.0.0.1:4222; excluded from
    /// CI. Run: `cargo test fetch_round_latency_probe -- --ignored --nocapture`.
    #[test]
    #[ignore]
    fn fetch_round_latency_probe() {
        let url = std::env::var("NATS_URL").unwrap_or_else(|_| "127.0.0.1:4222".into());
        let nc = nats::connect(&url).expect("connect nats");
        let js = nats::jetstream::new(nc.clone());
        let pid = std::process::id();
        let stream = format!("PROBE_{pid}");
        let subject = format!("probe.{pid}.in");
        let _ = js.delete_stream(&stream);
        js.add_stream(nats::jetstream::StreamConfig {
            name: stream.clone(),
            subjects: vec![format!("probe.{pid}.>")],
            ..Default::default()
        })
        .expect("add_stream");
        let durable = format!("probe_dur_{pid}");
        let _ = js.add_consumer(
            &stream,
            nats::jetstream::ConsumerConfig {
                durable_name: Some(durable.clone()),
                filter_subject: subject.clone(),
                ..Default::default()
            },
        );
        let sub = js
            .pull_subscribe_with_options(
                &subject,
                &nats::jetstream::PullSubscribeOptions::new().durable_name(durable),
            )
            .expect("pull_subscribe");

        // (A) idle: how long does an empty round block?
        let mut empties = Vec::new();
        for _ in 0..5 {
            let t = Instant::now();
            let msgs = fetch_round(&sub).expect("fetch");
            assert!(msgs.is_empty(), "probe subject should be idle");
            empties.push(t.elapsed().as_millis());
        }
        eprintln!("[probe] empty-round ms: {empties:?}");

        // (B) publish->receipt: a message lands at a SWEPT offset into an idle
        // stretch (20..~300ms) so samples hit every phase of the poll cycle — most
        // importantly the coverage GAP where no pull is outstanding (the current
        // 150ms idle sleep). Detection latency = receipt - the publish offset.
        let mut lat = Vec::new();
        for i in 0..30u32 {
            let delay_ms = 20 + (i as u64 * 23) % 280; // sweep across the ~252ms cycle
            let (ncp, subj, payload) = (nc.clone(), subject.clone(), format!("m{i}"));
            let t0 = Instant::now();
            let pubber = std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(delay_ms));
                ncp.publish(&subj, payload.as_bytes()).expect("publish");
                ncp.flush().ok();
            });
            let detected = loop {
                let msgs = fetch_round(&sub).expect("fetch");
                if !msgs.is_empty() {
                    for m in &msgs {
                        let _ = m.ack();
                    }
                    break t0.elapsed().as_millis() as i64 - delay_ms as i64;
                }
            };
            pubber.join().unwrap();
            lat.push(detected.max(0));
        }
        lat.sort_unstable();
        eprintln!(
            "[probe] publish->receipt detection ms: min={} p50={} p90={} max={} all={:?}",
            lat[0],
            lat[lat.len() / 2],
            lat[(lat.len() * 9) / 10],
            lat[lat.len() - 1],
            lat
        );
        let _ = js.delete_stream(&stream);
    }
}
