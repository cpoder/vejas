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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
        "oauth-poll" => Some(Box::new(OAuthPoll)),
        "slack-out" => Some(Box::new(SlackOut)),
        "http-out" => Some(Box::new(HttpOut)),
        "exec-source" => Some(Box::new(ExecSource)),
        "exec-sink" => Some(Box::new(ExecSink)),
        _ => None,
    }
}

pub fn catalog() -> Vec<(&'static str, &'static str, &'static str)> {
    [
        "http-in", "timer", "http-poll", "oauth-poll", "slack-out", "http-out",
        "exec-source", "exec-sink",
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
        "GETs a URL every INTERVAL_SECS and publishes the JSON body. Config: URL, SUBJECT, INTERVAL_SECS, HEADERS (optional doc, e.g. {\"Authorization\": …})."
    }
    fn run(&self, ctx: &Ctx) -> Result<(), String> {
        let url = ctx.config.str("URL").ok_or("URL required")?;
        let subject = ctx.subject(&ctx.config.str("SUBJECT").ok_or("SUBJECT required")?);
        let interval = ctx.config.u64_or("INTERVAL_SECS", 60).max(1);
        let headers = ctx.config.headers();
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

// ───────────────────────── pull rounds ─────────────────────────

/// One bounded pull round: up to 10 messages, and the server resolves the
/// request within PULL_EXPIRES_NS (messages or a 408 timeout status), so the
/// calling loop gets control back and can re-check its stop flag. A plain
/// `fetch()` parks forever on an idle subject: a stopped flow's thread stayed
/// wedged in `recv()`, reported "running", and processed one more message
/// before dying (the zombie-consumer trap caught while rehearsing the demo).
pub fn fetch_round(sub: &nats::jetstream::PullSubscription) -> Result<Vec<nats::Message>, String> {
    // server-side expiry (ns) strictly below the client-side wait, so every
    // pull is resolved by the server and none accumulates in max_waiting
    const PULL_EXPIRES_NS: usize = 700_000_000;
    let iter = sub
        .timeout_fetch(
            nats::jetstream::BatchOptions {
                batch: 10,
                expires: Some(PULL_EXPIRES_NS),
                no_wait: false,
            },
            Duration::from_millis(1000),
        )
        .map_err(|e| e.to_string())?;
    Ok(iter.map_while(|m| m.ok()).collect())
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

impl OAuthPoll {
    fn token(
        token_url: &str,
        client_id: &str,
        client_secret: &str,
        scope: &str,
    ) -> Result<(String, Duration), String> {
        let form = format!(
            "grant_type=client_credentials&client_id={}&client_secret={}&scope={}",
            urlencode(client_id),
            urlencode(client_secret),
            urlencode(scope)
        );
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
        "OAuth2 client-credentials REST poller: takes a token from TOKEN_URL, GETs each of ENDPOINTS with the Bearer (pagination via NEXT_LINK_FIELD, default \"@odata.nextLink\", capped by MAX_PAGES), publishes one {endpoint, fetched_at, body} message per page on SUBJECT every INTERVAL_SECS. Config: TOKEN_URL, CLIENT_ID, CLIENT_SECRET (use secret()), SCOPE, BASE_URL, ENDPOINTS, SUBJECT, INTERVAL_SECS, MAX_PAGES."
    }
    fn run(&self, ctx: &Ctx) -> Result<(), String> {
        let token_url = ctx.config.str("TOKEN_URL").ok_or("TOKEN_URL required")?;
        let client_id = ctx.config.str("CLIENT_ID").ok_or("CLIENT_ID required")?;
        let client_secret = ctx.config.str("CLIENT_SECRET").ok_or("CLIENT_SECRET required")?;
        let scope = ctx.config.str("SCOPE").ok_or(
            "SCOPE required (e.g. \"https://graph.microsoft.com/.default\" — the API origin, not BASE_URL)",
        )?;
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
            .filter(|v: &Vec<String>| !v.is_empty())
            .ok_or("ENDPOINTS required (a non-empty list of paths)")?;
        let subject = ctx.subject(&ctx.config.str("SUBJECT").ok_or("SUBJECT required")?);
        let interval = ctx.config.u64_or("INTERVAL_SECS", 300).max(1);
        let max_pages = ctx.config.u64_or("MAX_PAGES", 10).max(1);
        let next_field = ctx.config.str_or("NEXT_LINK_FIELD", "@odata.nextLink");
        let js = ctx.jetstream()?;
        eprintln!(
            "[{}] oauth-poll {} endpoint(s) on {base} every {interval}s -> {subject}",
            ctx.name,
            endpoints.len()
        );
        let mut cached: Option<(String, Instant)> = None;
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
            for ep in &endpoints {
                let mut url = join_url(&base, ep);
                let mut retried_401 = false;
                let mut pages = 0;
                while pages < max_pages {
                    if !ctx.alive() {
                        return Ok(());
                    }
                    let tok = match &cached {
                        Some((t, deadline)) if Instant::now() < *deadline => t.clone(),
                        _ => {
                            // a broken token config never comes back on its own:
                            // fail the run, let supervision back off and retry
                            let (t, ttl) =
                                Self::token(&token_url, &client_id, &client_secret, &scope)?;
                            cached = Some((t.clone(), Instant::now() + ttl));
                            t
                        }
                    };
                    let headers = [("Authorization".to_string(), format!("Bearer {tok}"))];
                    match http_request("GET", &url, &headers, None) {
                        Ok((401, _)) if !retried_401 => {
                            cached = None; // expired/revoked mid-flight: one fresh retry
                            retried_401 = true;
                        }
                        Ok((code, body)) if (200..300).contains(&code) => {
                            let parsed: Value = match serde_json::from_slice(&body) {
                                Ok(v) => v,
                                Err(e) => {
                                    eprintln!("[{}] {ep}: non-JSON page skipped: {e}", ctx.name);
                                    break;
                                }
                            };
                            let envelope = serde_json::json!({
                                "endpoint": ep,
                                "fetched_at": iso8601_utc(crate::now_secs()),
                                "body": parsed,
                            });
                            if let Err(e) =
                                js.publish(&subject, serde_json::to_vec(&envelope).unwrap_or_default())
                            {
                                return Err(format!("publish: {e}"));
                            }
                            pages += 1;
                            match next_url(&base, &parsed, &next_field) {
                                Some(next) => url = next,
                                None => break,
                            }
                        }
                        Ok((code, body)) => {
                            eprintln!(
                                "[{}] {ep}: HTTP {code}, endpoint skipped this tick: {}",
                                ctx.name,
                                String::from_utf8_lossy(&body[..body.len().min(200)]).trim()
                            );
                            break;
                        }
                        Err(e) => {
                            eprintln!("[{}] {ep}: {e} — endpoint skipped this tick", ctx.name);
                            break;
                        }
                    }
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
        for msg in fetch_round(&sub)? {
            if !ctx.alive() {
                // stopped mid-batch: leave the message un-acked, it redelivers
                return Ok(());
            }
            match handler(&msg.data) {
                Ok(()) => {
                    let _ = msg.ack();
                }
                Err(e) => {
                    // poison guard: give up after MAX_DELIVERIES attempts
                    let delivered =
                        msg.jetstream_message_info().map(|i| i.delivered).unwrap_or(1);
                    if delivered >= crate::MAX_DELIVERIES {
                        eprintln!("[{}] {e} -> dropped after {delivered} deliveries", ctx.name);
                        let _ = msg.ack();
                    } else {
                        eprintln!("[{}] {e} -> nak", ctx.name);
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
                Ok(())
            } else {
                let payload = serde_json::json!({ "text": text }).to_string();
                let headers = [("Content-Type".to_string(), "application/json".to_string())];
                http_post(&webhook, &headers, payload.as_bytes()).map(|_| ())
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
        "Consumes a subject and POSTs each message body to a URL. Config: SUBJECT, URL, HEADERS (optional doc — put credentials there via secret(), e.g. {\"Authorization\": …})."
    }
    fn run(&self, ctx: &Ctx) -> Result<(), String> {
        let subject = ctx.subject(&ctx.config.str("SUBJECT").ok_or("SUBJECT required")?);
        let url = ctx.config.str("URL").ok_or("URL required")?;
        let mut headers = ctx.config.headers();
        if !headers.iter().any(|(k, _)| k.eq_ignore_ascii_case("content-type")) {
            headers.push(("Content-Type".into(), "application/json".into()));
        }
        run_sink(ctx, &subject, move |data| http_post(&url, &headers, data).map(|_| ()))
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

// ───────────────────────── HTTP client (curl, argv-safe) ─────────────────────────
// v0 keeps the dependency graph light (curl); a Rust HTTP client replaces this
// later. Everything sensitive — the URL and the headers, Authorization above
// all — travels in a 0600 `--config` file, NEVER in argv: /proc/<pid>/cmdline
// is world-readable. The body streams over stdin.

static REQ_SEQ: AtomicU64 = AtomicU64::new(0);

/// A value quoted for a curl config file.
fn curl_quote(v: &str) -> String {
    format!("\"{}\"", v.replace('\\', "\\\\").replace('"', "\\\""))
}

/// One HTTP request through curl. Returns (status, body) — the caller decides
/// what a given status means (401 → token refresh, etc.).
pub fn http_request(
    method: &str,
    url: &str,
    headers: &[(String, String)],
    body: Option<&[u8]>,
) -> Result<(u16, Vec<u8>), String> {
    use std::io::Write as _;
    use std::os::unix::fs::OpenOptionsExt as _;
    let mut cfg = String::new();
    cfg.push_str(&format!("url = {}\n", curl_quote(url)));
    for (k, v) in headers {
        cfg.push_str(&format!("header = {}\n", curl_quote(&format!("{k}: {v}"))));
    }
    if body.is_some() {
        cfg.push_str("data-binary = \"@-\"\n");
    } else if method != "GET" {
        cfg.push_str(&format!("request = {}\n", curl_quote(method)));
    }
    let path = std::env::temp_dir().join(format!(
        "vejas-req-{}-{}",
        std::process::id(),
        REQ_SEQ.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)
        .and_then(|mut f| f.write_all(cfg.as_bytes()))
        .map_err(|e| e.to_string())?;
    let result = (|| {
        let mut child = std::process::Command::new("curl")
            .args(["-sS", "-m", "20", "-w", "\n%{http_code}", "--config"])
            .arg(&path)
            .stdin(if body.is_some() {
                std::process::Stdio::piped()
            } else {
                std::process::Stdio::null()
            })
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| e.to_string())?;
        if let Some(b) = body {
            child
                .stdin
                .take()
                .ok_or("no stdin")?
                .write_all(b)
                .map_err(|e| e.to_string())?;
        }
        let out = child.wait_with_output().map_err(|e| e.to_string())?;
        if !out.status.success() {
            return Err(format!("curl: {}", String::from_utf8_lossy(&out.stderr).trim()));
        }
        // split the "\n<code>" marker appended by --write-out
        let stdout = out.stdout;
        let pos = stdout
            .iter()
            .rposition(|&b| b == b'\n')
            .ok_or("curl: missing status marker")?;
        let code: u16 = std::str::from_utf8(&stdout[pos + 1..])
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .ok_or("curl: bad status marker")?;
        Ok((code, stdout[..pos].to_vec()))
    })();
    let _ = std::fs::remove_file(&path);
    result
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
    fn urlencode_and_curl_quote() {
        assert_eq!(urlencode("a-b_c.d~e"), "a-b_c.d~e");
        assert_eq!(urlencode("p@ss wörd+/="), "p%40ss%20w%C3%B6rd%2B%2F%3D");
        assert_eq!(curl_quote(r#"a"b\c"#), r#""a\"b\\c""#);
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
}
