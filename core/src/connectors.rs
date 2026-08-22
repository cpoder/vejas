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
    } else if e.starts_with("curl:") || e.contains("Connection refused") || e.contains("Could not resolve") || e.contains("timed out") {
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
        _ => None,
    }
}

pub fn catalog() -> Vec<(&'static str, &'static str, &'static str)> {
    [
        "http-in", "timer", "http-poll", "oauth-poll", "slack-out", "http-out",
        "exec-source", "exec-sink", "exec-stream-source", "exec-rpc",
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
                    let name = ctx.name.clone();
                    thread::spawn(move || handle_http_in(&name, &mut sock, &js, &root));
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

fn handle_http_in(name: &str, sock: &mut std::net::TcpStream, js: &nats::jetstream::JetStream, subj_root: &str) {
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
        Ok(_) => {
            trace_pub(name, &subject, body);
            reply(sock, "202 Accepted", &format!("{{\"published\":\"{subject}\"}}"))
        }
        Err(e) => {
            trace_fail(name, &subject, format!("publish: {e}"));
            reply(sock, "502 Bad Gateway", &format!("{{\"error\":\"{e}\"}}"))
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
pub fn to_dlq(
    js: &nats::jetstream::JetStream,
    unit: &str,
    subject: &str,
    attempts: i64,
    last_error: &str,
    payload: &[u8],
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
        // Raw text of the original message — replayed verbatim, so JSON and
        // non-JSON alike re-inject faithfully; the panel pretty-prints it if JSON.
        "payload": String::from_utf8_lossy(payload),
    }))
    .map_err(|e| e.to_string())?;
    let dlq_subject = format!("{DLQ_ROOT}.{}", dlq_unit_token(unit));
    js.publish(&dlq_subject, body).map(|_| ()).map_err(|e| e.to_string())
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
            match handler(&msg.data) {
                Ok(response) => {
                    crate::record_trace_full(
                        &ctx.name, subject, true, None, vec![], preview, None, response,
                    );
                    let _ = msg.ack();
                }
                Err(e) => {
                    // poison guard: give up after MAX_DELIVERIES attempts
                    let delivered =
                        msg.jetstream_message_info().map(|i| i.delivered).unwrap_or(1);
                    if delivered >= crate::MAX_DELIVERIES {
                        // Poison: park it in the DLQ, then ack — publish before ack
                        // so it is never lost (ADR-0015). On DLQ failure, nak.
                        match to_dlq(&js, &ctx.name, subject, delivered, &e, &msg.data) {
                            Ok(()) => {
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
        "Runs CMD as a long-running process and publishes each JSON line it streams on stdout to SUBJECT — line by line, with back-pressure (a bounded internal buffer; when the bus is slow the child's stdout blocks, so a burst can't overrun memory). For push sources like SAP IDoc inbound and Salesforce Bulk 2.0. ENV = {\"KEY\": secret(\"…\")} is handed to the child's environment (secrets never touch argv). On child exit it restarts after RESTART_SECS. Config: CMD, SUBJECT, ENV (optional), RESTART_SECS (optional, default 2)."
    }
    fn run(&self, ctx: &Ctx) -> Result<(), String> {
        use std::io::{BufRead, BufReader};
        use std::process::{Command, Stdio};
        use std::sync::mpsc::{sync_channel, RecvTimeoutError};

        let cmd = ctx.config.str("CMD").ok_or("CMD required")?;
        let subject = ctx.subject(&ctx.config.str("SUBJECT").ok_or("SUBJECT required")?);
        let env = ctx.config.env_vars();
        let restart = ctx.config.u64_or("RESTART_SECS", 2).max(1);
        let js = ctx.jetstream()?;
        eprintln!("[{}] exec-stream-source: `{cmd}` -> {subject} (streaming)", ctx.name);

        while ctx.alive() {
            let mut child = Command::new("sh")
                .arg("-c")
                .arg(&cmd)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit()) // child status/errors flow to our logs
                .envs(env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
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
                        if serde_json::from_str::<Value>(line).is_ok() {
                            match js.publish(&subject, line.as_bytes()) {
                                Ok(_) => trace_pub(&ctx.name, &subject, line.as_bytes()),
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

        let sub = nc.subscribe(&subject).map_err(|e| e.to_string())?;
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
}
