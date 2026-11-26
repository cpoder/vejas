//! The HTTP client (ureq and rustls, in the binary).

use std::time::Duration;

use crate::trace;

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

pub(super) fn http_get(url: &str, headers: &[(String, String)]) -> Result<Vec<u8>, String> {
    match http_request("GET", url, headers, None)? {
        (code, body) if (200..300).contains(&code) => Ok(body),
        (code, body) => Err(format!(
            "HTTP {code}: {}",
            String::from_utf8_lossy(&body[..body.len().min(300)]).trim()
        )),
    }
}

/// POST for the sinks. With a trace, the request carries it as `traceparent`
/// (and `tracestate`), replacing any the manifest set (ADR-0034).
pub(super) fn http_post(
    url: &str,
    headers: &[(String, String)],
    body: &[u8],
    trace: Option<&trace::Context>,
) -> Result<Vec<u8>, String> {
    let traced: Vec<(String, String)>;
    let headers = match trace {
        Some(trace) => {
            let is_trace = |k: &str| {
                k.eq_ignore_ascii_case(trace::TRACEPARENT) || k.eq_ignore_ascii_case(trace::TRACESTATE)
            };
            let mut h: Vec<(String, String)> =
                headers.iter().filter(|(k, _)| !is_trace(k)).cloned().collect();
            h.push((trace::TRACEPARENT.into(), trace.traceparent()));
            if let Some(state) = &trace.state {
                h.push((trace::TRACESTATE.into(), state.clone()));
            }
            traced = h;
            &traced[..]
        }
        None => headers,
    };
    match http_request("POST", url, headers, Some(body))? {
        (code, out) if (200..300).contains(&code) => Ok(out),
        (code, out) => Err(format!(
            "HTTP {code}: {}",
            String::from_utf8_lossy(&out[..out.len().min(300)]).trim()
        )),
    }
}

/// Percent-encoding for form values (RFC 3986 unreserved set passes).
pub(super) fn urlencode(s: &str) -> String {
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

/// Join a base URL and an endpoint path; absolute endpoints pass through.
pub(super) fn join_url(base: &str, endpoint: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("{}/{}", base.trim_end_matches('/'), endpoint.trim_start_matches('/'))
    }
}

/// The next page URL out of a response body, if any (absolute or relative).
pub(super) fn next_url(base: &str, body: &serde_json::Value, field: &str) -> Option<String> {
    body.get(field)
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| join_url(base, s))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
}
