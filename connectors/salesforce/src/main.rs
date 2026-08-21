//! Vejas Salesforce connector — OAuth2 + Bulk API 2.0 query, streaming large
//! exports row by row as JSON lines on stdout.
//!
//! Isolated exec process (ADR-0011); the runtime's exec-stream-source driver
//! bridges stdout to the bus with back-pressure — so a multi-million-row export
//! flows through bounded memory. HTTP goes through `curl` with a 0600 `--config`
//! file, so the bearer token never lands in argv (ADR-0008).
//!
//! stdout = data (one JSON object per exported row); stderr = status. Config via
//! env (the runtime injects secret() values, never literals):
//!   SF_LOGIN_URL (default https://login.salesforce.com), SF_GRANT_TYPE
//!   (default "password"), SF_CLIENT_ID, SF_CLIENT_SECRET, SF_USERNAME,
//!   SF_PASSWORD, SF_QUERY (SOQL), SF_API_VERSION (default v60.0),
//!   SF_MAX_RECORDS (page size, default 10000), SF_INTERVAL_SECS (0 = one-shot).

use serde_json::{json, Value};
use std::io::Write;
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
/// the body streams over stdin. Returns (status, headers, body).
fn http(
    method: &str,
    url: &str,
    headers: &[(String, String)],
    body: Option<&str>,
) -> Result<(u16, Vec<(String, String)>, String), String> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut cfg = String::new();
    cfg.push_str("silent\nshow-error\ninclude\n");
    cfg.push_str(&format!("request = {}\n", q(method)));
    cfg.push_str(&format!("url = {}\n", q(url)));
    for (k, v) in headers {
        cfg.push_str(&format!("header = {}\n", q(&format!("{k}: {v}"))));
    }
    if body.is_some() {
        cfg.push_str("data-binary = @-\n");
    }
    let path = std::env::temp_dir().join(format!("vsf-{}-{}.cfg", std::process::id(), rand_tag()));
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
    cmd.arg("-K").arg(&path).stdout(Stdio::piped()).stderr(Stdio::piped());
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
    parse_response(&String::from_utf8_lossy(&out.stdout))
}

/// A weak unique tag (no rand crate); pid+monotonic-ish from an atomic counter.
fn rand_tag() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    N.fetch_add(1, Ordering::Relaxed)
}

/// Split curl's `-i` output into (status, headers, body), skipping any leading
/// 1xx informational block (e.g. 100 Continue).
fn parse_response(raw: &str) -> Result<(u16, Vec<(String, String)>, String), String> {
    let mut rest = raw;
    loop {
        let sep = rest.find("\r\n\r\n").ok_or("no header/body separator")?;
        let head = &rest[..sep];
        let after = &rest[sep + 4..];
        let status_line = head.lines().next().unwrap_or("");
        let code = status_line
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse::<u16>().ok())
            .ok_or_else(|| format!("bad status line: {status_line}"))?;
        if code == 100 {
            rest = after; // informational; the real response follows
            continue;
        }
        let headers = head
            .lines()
            .skip(1)
            .filter_map(|l| {
                let i = l.find(':')?;
                Some((l[..i].trim().to_string(), l[i + 1..].trim().to_string()))
            })
            .collect();
        return Ok((code, headers, after.to_string()));
    }
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

// ─────────────────────────── CSV ───────────────────────────
/// Parse a full CSV document into records, honoring quoted fields (with ""
/// escapes) that may contain commas and newlines.
fn parse_csv(data: &str) -> Vec<Vec<String>> {
    let mut records = Vec::new();
    let mut record = Vec::new();
    let mut field = String::new();
    let mut in_quotes = false;
    let mut chars = data.chars().peekable();
    while let Some(c) = chars.next() {
        if in_quotes {
            if c == '"' {
                if chars.peek() == Some(&'"') {
                    field.push('"');
                    chars.next();
                } else {
                    in_quotes = false;
                }
            } else {
                field.push(c);
            }
        } else {
            match c {
                '"' => in_quotes = true,
                ',' => record.push(std::mem::take(&mut field)),
                '\n' => {
                    record.push(std::mem::take(&mut field));
                    records.push(std::mem::take(&mut record));
                }
                '\r' => {}
                _ => field.push(c),
            }
        }
    }
    if !field.is_empty() || !record.is_empty() {
        record.push(field);
        records.push(record);
    }
    records
}

// ─────────────────────────── Salesforce ───────────────────────────
struct Session {
    token: String,
    instance: String,
}

fn oauth() -> Result<Session, String> {
    let login = env_or("SF_LOGIN_URL", "https://login.salesforce.com");
    let grant = env_or("SF_GRANT_TYPE", "password");
    let mut form = format!("grant_type={}", enc(&grant));
    form += &format!("&client_id={}", enc(&env_or("SF_CLIENT_ID", "")));
    form += &format!("&client_secret={}", enc(&env_or("SF_CLIENT_SECRET", "")));
    if grant == "password" {
        form += &format!("&username={}", enc(&env_or("SF_USERNAME", "")));
        form += &format!("&password={}", enc(&env_or("SF_PASSWORD", "")));
    }
    let (code, _h, body) = http(
        "POST",
        &format!("{login}/services/oauth2/token"),
        &[(
            "Content-Type".into(),
            "application/x-www-form-urlencoded".into(),
        )],
        Some(&form),
    )?;
    if code != 200 {
        return Err(format!("oauth failed ({code}): {}", body.trim()));
    }
    let v: Value = serde_json::from_str(body.trim()).map_err(|e| e.to_string())?;
    let token = v["access_token"]
        .as_str()
        .ok_or("no access_token in token response")?
        .to_string();
    let instance = v["instance_url"]
        .as_str()
        .ok_or("no instance_url in token response")?
        .to_string();
    Ok(Session { token, instance })
}

fn auth(s: &Session) -> Vec<(String, String)> {
    vec![("Authorization".into(), format!("Bearer {}", s.token))]
}

/// Create a Bulk 2.0 query job, return its id.
fn create_job(s: &Session, ver: &str, query: &str) -> Result<String, String> {
    let url = format!("{}/services/data/{ver}/jobs/query", s.instance);
    let mut h = auth(s);
    h.push(("Content-Type".into(), "application/json".into()));
    let body = json!({"operation": "query", "query": query}).to_string();
    let (code, _hd, resp) = http("POST", &url, &h, Some(&body))?;
    if code != 200 {
        return Err(format!("create job failed ({code}): {}", resp.trim()));
    }
    let v: Value = serde_json::from_str(resp.trim()).map_err(|e| e.to_string())?;
    v["id"]
        .as_str()
        .map(|x| x.to_string())
        .ok_or_else(|| "no job id".into())
}

/// Poll until the job completes; returns Ok on JobComplete, Err on failure.
fn poll_job(s: &Session, ver: &str, id: &str) -> Result<(), String> {
    let url = format!("{}/services/data/{ver}/jobs/query/{id}", s.instance);
    loop {
        let (code, _h, resp) = http("GET", &url, &auth(s), None)?;
        if code != 200 {
            return Err(format!("poll failed ({code}): {}", resp.trim()));
        }
        let v: Value = serde_json::from_str(resp.trim()).map_err(|e| e.to_string())?;
        match v["state"].as_str().unwrap_or("") {
            "JobComplete" => return Ok(()),
            "Failed" | "Aborted" => {
                return Err(format!("job {}: {}", v["state"], v["errorMessage"]))
            }
            other => {
                status(&json!({"stage": "poll", "state": other}));
                std::thread::sleep(std::time::Duration::from_millis(1000));
            }
        }
    }
}

/// Stream all result pages of a completed job, emitting one JSON object per row.
/// Returns the number of rows streamed.
fn stream_results(s: &Session, ver: &str, id: &str, max_records: u64) -> Result<u64, String> {
    let base = format!("{}/services/data/{ver}/jobs/query/{id}/results", s.instance);
    let mut locator: Option<String> = None;
    let mut total = 0u64;
    loop {
        let mut url = format!("{base}?maxRecords={max_records}");
        if let Some(l) = &locator {
            url += &format!("&locator={}", enc(l));
        }
        let (code, hd, body) = http("GET", &url, &auth(s), None)?;
        if code != 200 {
            return Err(format!("results failed ({code}): {}", body.trim()));
        }
        let rows = parse_csv(&body);
        if let Some((head, data)) = rows.split_first() {
            for r in data {
                let mut obj = serde_json::Map::new();
                for (i, col) in head.iter().enumerate() {
                    obj.insert(
                        col.clone(),
                        Value::String(r.get(i).cloned().unwrap_or_default()),
                    );
                }
                emit(&json!({"stream": true, "source": "salesforce", "row": obj}));
                total += 1;
            }
        }
        // Sforce-Locator header carries the next page cursor; empty/"null" = done.
        match header(&hd, "Sforce-Locator") {
            Some(l) if !l.is_empty() && l != "null" => locator = Some(l.to_string()),
            _ => break,
        }
    }
    Ok(total)
}

/// A ready session: either a caller-supplied access token + instance URL
/// (e.g. from `sf org display --verbose`, or any prior OAuth), or a fresh OAuth.
fn session() -> Result<Session, String> {
    match (
        std::env::var("SF_ACCESS_TOKEN").ok().filter(|s| !s.is_empty()),
        std::env::var("SF_INSTANCE_URL").ok().filter(|s| !s.is_empty()),
    ) {
        (Some(token), Some(instance)) => Ok(Session { token, instance }),
        _ => oauth(),
    }
}

fn export_once(ver: &str, query: &str, max_records: u64) -> Result<u64, String> {
    let s = session()?;
    status(&json!({"stage": "auth", "instance": s.instance}));
    let id = create_job(&s, ver, query)?;
    status(&json!({"stage": "job", "id": id}));
    poll_job(&s, ver, &id)?;
    let n = stream_results(&s, ver, &id, max_records)?;
    status(&json!({"stage": "done", "rows": n, "job": id}));
    Ok(n)
}

fn main() {
    let ver = env_or("SF_API_VERSION", "v60.0");
    let query = match std::env::var("SF_QUERY") {
        Ok(q) if !q.is_empty() => q,
        _ => {
            status(&json!({"ok": false, "fatal": true, "error": "SF_QUERY required"}));
            std::process::exit(1);
        }
    };
    let max_records = env_or("SF_MAX_RECORDS", "10000").parse::<u64>().unwrap_or(10000);
    let interval = env_or("SF_INTERVAL_SECS", "0").parse::<u64>().unwrap_or(0);

    loop {
        match export_once(&ver, &query, max_records) {
            Ok(_n) => {}
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
