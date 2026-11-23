//! Vejas Stripe connector — Events polling + payment intent creation
//!
//! Two modes:
//! 1. Events mode (default): Polls Stripe events API, emits new events to stdout as JSON lines
//! 2. Payment mode (STRIPE_MODE=payment): Reads payment intent requests from stdin
//!    (JSON), creates them via Stripe API, and emits results to stdout.
//!
//! Isolated exec process (ADR-0011); the runtime's exec-stream-source driver
//! bridges stdout to the bus. HTTP goes through `curl` with a 0600 `--config`
//! file, so the API key never lands in argv (ADR-0008).
//!
//! Config via env (the runtime injects secret() values):
//!   STRIPE_API_KEY (required): sk_test_... or sk_live_...
//!   STRIPE_INTERVAL_SECS (events mode, default 60): polling interval
//!   STRIPE_MODE (optional): "events" (default) or "payment"

use serde_json::{json, Value};
use std::io::{Read, Write};
use std::process::{Command, Stdio};

fn env_or(k: &str, d: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| d.to_string())
}

fn emit(v: &Value) {
    let mut o = std::io::stdout().lock();
    let _ = writeln!(o, "{v}");
    let _ = o.flush();
}

fn status(v: &Value) {
    let mut o = std::io::stderr().lock();
    let _ = writeln!(o, "{v}");
    let _ = o.flush();
}

// ─────────────────────────── HTTP via curl ───────────────────────────
fn q(v: &str) -> String {
    format!("\"{}\"", v.replace('\\', "\\\\").replace('"', "\\\""))
}

/// Percent-encode a form value (application/x-www-form-urlencoded).
fn enc(s: &str) -> String {
    let mut out = String::new();
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

/// One HTTP request through curl. URL + headers travel in a 0600 config file;
/// the body streams over stdin. Returns (status, body).
fn http(
    method: &str,
    url: &str,
    headers: &[(String, String)],
    body: Option<&str>,
) -> Result<(u16, String), String> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut cfg = String::new();
    cfg.push_str("silent\nshow-error\n");
    cfg.push_str(&format!("request = {}\n", q(method)));
    cfg.push_str(&format!("url = {}\n", q(url)));
    for (k, v) in headers {
        cfg.push_str(&format!("header = {}\n", q(&format!("{k}: {v}"))));
    }
    if body.is_some() {
        cfg.push_str("data-binary = @-\n");
    }
    let path = std::env::temp_dir().join(format!("vstripe-{}-{}.cfg", std::process::id(), rand_tag()));
    {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&path)
            .map_err(|e| e.to_string())?;
        f.write_all(cfg.as_bytes()).map_err(|e| e.to_string())?;
    }
    let mut cmd = Command::new("curl");
    cmd.arg("-K").arg(&path).arg("-w").arg("\n%{http_code}")
        .stdout(Stdio::piped()).stderr(Stdio::piped());
    cmd.stdin(if body.is_some() { Stdio::piped() } else { Stdio::null() });
    let mut child = cmd.spawn().map_err(|e| e.to_string())?;
    if let Some(b) = body {
        child
            .stdin
            .take()
            .ok_or("no stdin")?
            .write_all(b.as_bytes())
            .map_err(|e| e.to_string())?;
    }
    let out = child.wait_with_output().map_err(|e| e.to_string())?;
    let _ = std::fs::remove_file(&path);
    if !out.status.success() {
        return Err(format!(
            "curl: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    let output = String::from_utf8_lossy(&out.stdout);
    let lines: Vec<&str> = output.lines().collect();
    let code = lines.last()
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or("no status code")?;
    let body = lines[..lines.len().saturating_sub(1)].join("\n");
    Ok((code, body))
}

fn rand_tag() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    N.fetch_add(1, Ordering::Relaxed)
}

// ─────────────────────────── Stripe Events Polling ───────────────────────────
fn poll_events(api_key: &str, starting_after: Option<&str>) -> Result<Value, String> {
    let mut url = "https://api.stripe.com/v1/events?limit=100".to_string();
    if let Some(after) = starting_after {
        url.push_str(&format!("&starting_after={}", enc(after)));
    }
    
    let headers = vec![
        ("Authorization".into(), format!("Bearer {}", api_key)),
    ];
    
    let (code, body) = http("GET", &url, &headers, None)?;
    
    if code != 200 {
        return Err(format!("Stripe API error ({code}): {}", body.trim()));
    }
    
    serde_json::from_str(&body).map_err(|e| format!("JSON parse error: {}", e))
}

fn run_events_mode() {
    let api_key = match std::env::var("STRIPE_API_KEY") {
        Ok(k) if !k.is_empty() => k,
        _ => {
            status(&json!({"ok": false, "fatal": true, "error": "STRIPE_API_KEY required"}));
            std::process::exit(1);
        }
    };
    
    let interval = env_or("STRIPE_INTERVAL_SECS", "60")
        .parse::<u64>()
        .unwrap_or(60);
    
    let mut last_event_id: Option<String> = None;
    
    status(&json!({"stage": "events_poll", "interval_secs": interval}));
    
    loop {
        match poll_events(&api_key, last_event_id.as_deref()) {
            Ok(response) => {
                if let Some(data) = response["data"].as_array() {
                    for event in data.iter().rev() {
                        // Emit each event as a separate JSON line
                        emit(&json!({
                            "stream": true,
                            "source": "stripe",
                            "event": event
                        }));
                        
                        // Track the latest event ID for pagination
                        if let Some(id) = event["id"].as_str() {
                            last_event_id = Some(id.to_string());
                        }
                    }
                    
                    if !data.is_empty() {
                        status(&json!({"stage": "poll", "events": data.len()}));
                    }
                }
            }
            Err(e) => {
                status(&json!({"ok": false, "error": e}));
                if interval == 0 {
                    std::process::exit(1);
                }
            }
        }
        
        if interval == 0 {
            break; // one-shot
        }
        std::thread::sleep(std::time::Duration::from_secs(interval));
    }
}

// ─────────────────────────── Stripe Payment Intent Creation ───────────────────────────
fn create_payment_intent(api_key: &str, amount: u64, currency: &str, customer: Option<&str>, description: Option<&str>) -> Result<Value, String> {
    let mut form = format!("amount={}&currency={}", amount, enc(currency));
    if let Some(c) = customer {
        form.push_str(&format!("&customer={}", enc(c)));
    }
    if let Some(d) = description {
        form.push_str(&format!("&description={}", enc(d)));
    }
    
    let headers = vec![
        ("Authorization".into(), format!("Bearer {}", api_key)),
        ("Content-Type".into(), "application/x-www-form-urlencoded".into()),
    ];
    
    let (code, body) = http("POST", "https://api.stripe.com/v1/payment_intents", &headers, Some(&form))?;
    
    if code != 200 {
        return Err(format!("Stripe API error ({code}): {}", body.trim()));
    }
    
    serde_json::from_str(&body).map_err(|e| format!("JSON parse error: {}", e))
}

fn run_payment_mode() {
    let api_key = match std::env::var("STRIPE_API_KEY") {
        Ok(k) if !k.is_empty() => k,
        _ => {
            status(&json!({"ok": false, "fatal": true, "error": "STRIPE_API_KEY required"}));
            std::process::exit(1);
        }
    };
    
    let mut input = String::new();
    if std::io::stdin().read_to_string(&mut input).is_err() {
        status(&json!({"ok": false, "error": "read stdin failed"}));
        std::process::exit(1);
    }
    
    // Parse input - support both single object and array
    let requests: Vec<Value> = if let Ok(v) = serde_json::from_str::<Value>(input.trim()) {
        match v {
            Value::Array(arr) => arr,
            obj => vec![obj],
        }
    } else {
        // Try JSON-lines
        input.lines()
            .filter(|l| !l.trim().is_empty())
            .filter_map(|l| serde_json::from_str(l).ok())
            .collect()
    };
    
    if requests.is_empty() {
        status(&json!({"ok": true, "note": "no payment requests"}));
        emit(&json!({"ok": true, "processed": 0}));
        return;
    }
    
    let mut results = Vec::new();
    for req in requests {
        let amount = req["amount"].as_u64().unwrap_or(0);
        let currency = req["currency"].as_str().unwrap_or("usd");
        let customer = req["customer"].as_str();
        let description = req["description"].as_str();
        
        match create_payment_intent(&api_key, amount, currency, customer, description) {
            Ok(intent) => {
                let result = json!({
                    "ok": true,
                    "id": intent["id"],
                    "amount": intent["amount"],
                    "currency": intent["currency"],
                    "status": intent["status"],
                    "client_secret": intent["client_secret"]
                });
                status(&result);
                results.push(result);
            }
            Err(e) => {
                let result = json!({"ok": false, "error": e, "request": req});
                status(&result);
                results.push(result);
            }
        }
    }
    
    emit(&json!({"ok": true, "processed": results.len(), "results": results}));
}

fn main() {
    let mode = std::env::args()
        .nth(1)
        .or_else(|| std::env::var("STRIPE_MODE").ok())
        .unwrap_or_else(|| "events".to_string());
    
    match mode.as_str() {
        "events" => run_events_mode(),
        "payment" => run_payment_mode(),
        _ => {
            status(&json!({"ok": false, "error": format!("unknown mode: {}", mode)}));
            std::process::exit(1);
        }
    }
}