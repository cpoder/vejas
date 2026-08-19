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
use std::time::Duration;

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
        let nc = nats::connect(&self.nats_url).map_err(|e| e.to_string())?;
        let js = nats::jetstream::new(nc);
        let _ = js.add_stream(&nats::jetstream::StreamConfig {
            name: self.stream.clone(),
            subjects: vec![format!("{}.>", self.subj_root)],
            ..Default::default()
        });
        Ok(js)
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
}

/// The compiled-in driver catalog. New drivers are added here (and, later,
/// generated onto this trait via `vejas_new_connector`).
pub fn driver_for(name: &str) -> Option<Box<dyn Driver>> {
    match name {
        "http-in" => Some(Box::new(HttpIn)),
        "timer" => Some(Box::new(Timer)),
        "http-poll" => Some(Box::new(HttpPoll)),
        "slack-out" => Some(Box::new(SlackOut)),
        "http-out" => Some(Box::new(HttpOut)),
        "exec-source" => Some(Box::new(ExecSource)),
        "exec-sink" => Some(Box::new(ExecSink)),
        _ => None,
    }
}

pub fn catalog() -> Vec<(&'static str, &'static str, &'static str)> {
    [
        "http-in", "timer", "http-poll", "slack-out", "http-out", "exec-source",
        "exec-sink",
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
        let js = ctx.jetstream()?;
        let listener = TcpListener::bind(("0.0.0.0", port)).map_err(|e| e.to_string())?;
        listener.set_nonblocking(true).ok();
        eprintln!("[{}] http-in on :{port}, publishing under {}.*", ctx.name, ctx.subj_root);
        for s in listener.incoming() {
            if !ctx.alive() {
                break;
            }
            match s {
                Ok(mut sock) => {
                    let js = js.clone();
                    let root = ctx.subj_root.clone();
                    thread::spawn(move || handle_http_in(&mut sock, &js, &root));
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(100));
                }
                Err(e) => eprintln!("[{}] accept: {e}", ctx.name),
            }
        }
        Ok(())
    }
}

fn handle_http_in(sock: &mut std::net::TcpStream, js: &nats::jetstream::JetStream, subj_root: &str) {
    sock.set_read_timeout(Some(Duration::from_secs(5))).ok();
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    let mut header_end = None;
    loop {
        match sock.read(&mut tmp) {
            Ok(0) => break,
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
            Err(_) => break,
        }
    }
    let text = String::from_utf8_lossy(&buf);
    let mut lines = text.split("\r\n");
    let mut parts = lines.next().unwrap_or("").split(' ');
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("");
    let body = header_end.map(|he| &buf[he.min(buf.len())..]).unwrap_or(&[]);
    let reply = |sock: &mut std::net::TcpStream, code: &str, json: &str| {
        let _ = write!(
            sock,
            "HTTP/1.1 {code}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{json}",
            json.len()
        );
    };
    if method == "GET" && path == "/healthz" {
        return reply(sock, "200 OK", "{\"ok\":true}");
    }
    if method != "POST" || !path.starts_with("/ingest/") {
        return reply(sock, "404 Not Found", "{\"error\":\"POST /ingest/<subject-suffix>\"}");
    }
    let suffix = path.trim_start_matches("/ingest/").trim_matches('/');
    if suffix.is_empty() {
        return reply(sock, "400 Bad Request", "{\"error\":\"missing subject suffix\"}");
    }
    if serde_json::from_slice::<Value>(body).is_err() {
        return reply(sock, "400 Bad Request", "{\"error\":\"body must be JSON\"}");
    }
    let subject = format!("{subj_root}.{suffix}");
    match js.publish(&subject, body) {
        Ok(_) => reply(sock, "202 Accepted", &format!("{{\"published\":\"{subject}\"}}")),
        Err(e) => reply(sock, "502 Bad Gateway", &format!("{{\"error\":\"{e}\"}}")),
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

struct Timer;
impl Driver for Timer {
    fn kind(&self) -> &'static str {
        "source:interval"
    }
    fn about(&self) -> &'static str {
        "Emits a fixed payload on a subject every INTERVAL_SECS. Config: SUBJECT, INTERVAL_SECS, PAYLOAD."
    }
    fn run(&self, ctx: &Ctx) -> Result<(), String> {
        let subject = ctx.subject(&ctx.config.str("SUBJECT").ok_or("SUBJECT required")?);
        let interval = ctx.config.u64_or("INTERVAL_SECS", 60).max(1);
        let payload = ctx.config.value("PAYLOAD").unwrap_or(Value::Object(Map::new()));
        let js = ctx.jetstream()?;
        eprintln!("[{}] timer every {interval}s -> {subject}", ctx.name);
        let bytes = serde_json::to_vec(&payload).unwrap_or_default();
        let mut waited = 0;
        loop {
            if !ctx.alive() {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(250));
            waited += 250;
            if waited >= interval * 1000 {
                waited = 0;
                if let Err(e) = js.publish(&subject, &bytes) {
                    return Err(format!("publish: {e}"));
                }
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
        "GETs a URL every INTERVAL_SECS and publishes the JSON body. Config: URL, SUBJECT, INTERVAL_SECS."
    }
    fn run(&self, ctx: &Ctx) -> Result<(), String> {
        let url = ctx.config.str("URL").ok_or("URL required")?;
        let subject = ctx.subject(&ctx.config.str("SUBJECT").ok_or("SUBJECT required")?);
        let interval = ctx.config.u64_or("INTERVAL_SECS", 60).max(1);
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
                match http_get(&url) {
                    Ok(body) if serde_json::from_slice::<Value>(&body).is_ok() => {
                        let _ = js.publish(&subject, &body);
                    }
                    Ok(_) => eprintln!("[{}] poll: non-JSON body, skipped", ctx.name),
                    Err(e) => eprintln!("[{}] poll: {e}", ctx.name),
                }
            }
        }
    }
}

// ───────────────────────── sinks ─────────────────────────

/// Shared sink loop: durable pull consumer -> handler; ack on Ok, nak on Err.
fn run_sink(
    ctx: &Ctx,
    subject: &str,
    handler: impl Fn(&[u8]) -> Result<(), String>,
) -> Result<(), String> {
    let js = ctx.jetstream()?;
    let durable = ctx.name.replace([':', '.', '-'], "_");
    let _ = js.add_consumer(
        &ctx.stream,
        nats::jetstream::ConsumerConfig {
            durable_name: Some(durable.clone()),
            filter_subject: subject.to_string(),
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
        let batch = sub.fetch(10).map_err(|e| e.to_string())?;
        for msg in batch {
            match handler(&msg.data) {
                Ok(()) => {
                    let _ = msg.ack();
                }
                Err(e) => {
                    eprintln!("[{}] {e} -> nak", ctx.name);
                    let _ = msg.ack_kind(nats::jetstream::AckKind::Nak);
                }
            }
        }
        thread::sleep(Duration::from_millis(150));
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
                Ok(())
            } else {
                let payload = serde_json::json!({ "text": text }).to_string();
                http_post(&webhook, payload.as_bytes()).map(|_| ())
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
        "Consumes a subject and POSTs each message body to a URL. Config: SUBJECT, URL."
    }
    fn run(&self, ctx: &Ctx) -> Result<(), String> {
        let subject = ctx.subject(&ctx.config.str("SUBJECT").ok_or("SUBJECT required")?);
        let url = ctx.config.str("URL").ok_or("URL required")?;
        run_sink(ctx, &subject, move |data| http_post(&url, data).map(|_| ()))
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
                    eprintln!("[{}] exec: {}", ctx.name, String::from_utf8_lossy(&out.stderr).trim());
                }
                for line in out.stdout.split(|&b| b == b'\n') {
                    let line = line.strip_suffix(b"\r").unwrap_or(line);
                    if line.is_empty() {
                        continue;
                    }
                    if serde_json::from_slice::<Value>(line).is_ok() {
                        let _ = js.publish(&subject, line);
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
        "Consumes SUBJECT; pipes each message body to CMD's stdin. Any language, no NATS client. Config: CMD, SUBJECT."
    }
    fn run(&self, ctx: &Ctx) -> Result<(), String> {
        let cmd = ctx.config.str("CMD").ok_or("CMD required")?;
        let subject = ctx.subject(&ctx.config.str("SUBJECT").ok_or("SUBJECT required")?);
        let name = ctx.name.clone();
        run_sink(ctx, &subject, move |data| {
            let mut child = std::process::Command::new("sh")
                .arg("-c")
                .arg(&cmd)
                .stdin(std::process::Stdio::piped())
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
                Ok(())
            } else {
                Err(format!("{name}: exit {:?}: {}", out.status.code(), String::from_utf8_lossy(&out.stderr).trim()))
            }
        })
    }
}

// ───────────────────────── tiny HTTP client (curl) ─────────────────────────
// v0 keeps the dependency graph light; a Rust HTTP client replaces this later.

fn http_get(url: &str) -> Result<Vec<u8>, String> {
    let out = std::process::Command::new("curl")
        .args(["-sS", "-m", "15", url])
        .output()
        .map_err(|e| e.to_string())?;
    if out.status.success() {
        Ok(out.stdout)
    } else {
        Err(String::from_utf8_lossy(&out.stderr).into_owned())
    }
}

fn http_post(url: &str, body: &[u8]) -> Result<Vec<u8>, String> {
    use std::io::Write as _;
    let mut child = std::process::Command::new("curl")
        .args(["-sS", "-m", "15", "-X", "POST", "-H", "Content-Type: application/json", "--data-binary", "@-", url])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| e.to_string())?;
    child.stdin.take().unwrap().write_all(body).map_err(|e| e.to_string())?;
    let out = child.wait_with_output().map_err(|e| e.to_string())?;
    if out.status.success() {
        Ok(out.stdout)
    } else {
        Err(String::from_utf8_lossy(&out.stderr).into_owned())
    }
}
