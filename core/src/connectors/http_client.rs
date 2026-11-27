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

/// The agent of the calls a flow makes (ADR-0032). It follows no redirect: a
/// redirect would take the service's headers, its credentials among them, to
/// wherever the answer points. A service that takes a 3xx lists it in ANSWERS
/// and reads `location` itself.
static CALL_AGENT: std::sync::OnceLock<ureq::Agent> = std::sync::OnceLock::new();
fn call_agent() -> &'static ureq::Agent {
    CALL_AGENT.get_or_init(|| ureq::AgentBuilder::new().redirects(0).user_agent("vejas").build())
}

/// What a request that did not get through says: the host it went to, the kind
/// of failure and its cause. Never the URL or a header: ureq's own text starts
/// with the whole URL, its query and user info included, and a header it
/// cannot send is quoted with its value. A secret in either would reach the
/// run's error, its span, the log and the dead letter.
fn transport_error(t: &ureq::Transport) -> String {
    let mut text = String::new();
    if let Some(host) = t.url().and_then(|url| Some((url.host_str()?, url.port_or_known_default()))) {
        text.push_str(host.0);
        if let Some(port) = host.1 {
            text.push_str(&format!(":{port}"));
        }
        text.push_str(": ");
    }
    text.push_str(&t.kind().to_string());
    if let Some(cause) = std::error::Error::source(t) {
        text.push_str(&format!(": {cause}"));
    }
    text
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
        Err(ureq::Error::Transport(t)) => return Err(format!("http: {}", transport_error(&t))),
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

/// The headers a request sends: with a trace, it carries it as `traceparent`
/// (and `tracestate`), replacing any the manifest set (ADR-0034).
fn traced(headers: &[(String, String)], trace: Option<&trace::Context>) -> Vec<(String, String)> {
    let Some(trace) = trace else {
        return headers.to_vec();
    };
    let is_trace =
        |k: &str| k.eq_ignore_ascii_case(trace::TRACEPARENT) || k.eq_ignore_ascii_case(trace::TRACESTATE);
    let mut h: Vec<(String, String)> = headers.iter().filter(|(k, _)| !is_trace(k)).cloned().collect();
    h.push((trace::TRACEPARENT.into(), trace.traceparent()));
    if let Some(state) = &trace.state {
        h.push((trace::TRACESTATE.into(), state.clone()));
    }
    h
}

/// What a system answered a call (ADR-0032): its status, its headers (names in
/// lower case, a repeated one's values joined) and its body.
pub(crate) struct Answer {
    pub status: u16,
    pub headers: serde_json::Map<String, serde_json::Value>,
    pub body: Vec<u8>,
}

/// Why a call got no answer.
pub(crate) enum NoAnswer {
    /// None came within the call's timeout.
    TimedOut,
    /// The request did not get through, or its answer did not come back
    /// whole: a later try may get one.
    Network(String),
    /// The request cannot be made, or its answer cannot be taken, however
    /// often it is tried: a URL that does not parse, an answer over the cap.
    Broken(String),
}

/// The most of an answer a call reads.
const ANSWER_MAX: u64 = 8 << 20;

/// One call of a service that answers: any method, a timeout of its own, the
/// run's trace carried on. Every status is an answer here; the service decides
/// which ones it takes.
pub(crate) fn http_call(
    method: &str,
    url: &str,
    headers: &[(String, String)],
    body: Option<&[u8]>,
    timeout: Duration,
    trace: Option<&trace::Context>,
) -> Result<Answer, NoAnswer> {
    use std::io::Read as _;
    let timed_out = |e: &std::io::Error| e.kind() == std::io::ErrorKind::TimedOut;
    let mut req = call_agent().request(method, url).timeout(timeout);
    for (k, v) in traced(headers, trace) {
        req = req.set(&k, &v);
    }
    let sent = match body {
        Some(b) => req.send_bytes(b),
        None => req.call(),
    };
    let response = match sent {
        Ok(r) | Err(ureq::Error::Status(_, r)) => r,
        Err(ureq::Error::Transport(t)) => {
            let cause = std::error::Error::source(&t).and_then(|s| s.downcast_ref::<std::io::Error>());
            return Err(match t.kind() {
                _ if cause.is_some_and(timed_out) => NoAnswer::TimedOut,
                ureq::ErrorKind::Dns
                | ureq::ErrorKind::ConnectionFailed
                | ureq::ErrorKind::Io
                | ureq::ErrorKind::BadStatus
                | ureq::ErrorKind::ProxyConnect => NoAnswer::Network(transport_error(&t)),
                _ => NoAnswer::Broken(transport_error(&t)),
            });
        }
    };
    let status = response.status();
    let mut names = serde_json::Map::new();
    for name in response.headers_names() {
        let values = response.all(&name).join(", ");
        names.insert(name.to_ascii_lowercase(), values.into());
    }
    let mut body = Vec::new();
    response
        .into_reader()
        .take(ANSWER_MAX + 1)
        .read_to_end(&mut body)
        .map_err(|e| if timed_out(&e) { NoAnswer::TimedOut } else { NoAnswer::Network(e.to_string()) })?;
    if body.len() as u64 > ANSWER_MAX {
        return Err(NoAnswer::Broken(format!("its answer is over {} MiB", ANSWER_MAX >> 20)));
    }
    Ok(Answer { status, headers: names, body })
}

/// POST for the sinks, carrying the trace (see [`traced`]).
pub(super) fn http_post(
    url: &str,
    headers: &[(String, String)],
    body: &[u8],
    trace: Option<&trace::Context>,
) -> Result<Vec<u8>, String> {
    match http_request("POST", url, &traced(headers, trace), Some(body))? {
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

    #[test]
    fn a_request_that_does_not_get_through_names_no_url_and_no_header() {
        // a port nothing listens on any more: the connection is refused
        let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let url = format!("http://svc:pw-SECRET-1@127.0.0.1:{port}/orders/1?api_key=SECRET-2");
        let Err(NoAnswer::Network(e)) = http_call("GET", &url, &[], None, Duration::from_secs(2), None) else {
            panic!("the connection was not refused")
        };
        assert!(e.starts_with(&format!("127.0.0.1:{port}: Connection Failed: ")), "{e}");
        assert!(!e.contains("SECRET") && !e.contains("api_key"), "{e}");
        let e = http_request("GET", &url, &[], None).unwrap_err();
        assert!(e.starts_with("http: 127.0.0.1:") && !e.contains("SECRET"), "{e}");
        // a header that cannot be sent is not quoted, value included
        let headers = [("Authorization".to_string(), "Bearer SECRET-3\n".to_string())];
        let Err(NoAnswer::Broken(e)) = http_call("GET", &url, &headers, None, Duration::from_secs(2), None) else {
            panic!("the header was sent")
        };
        assert!(e.contains("Bad Header") && !e.contains("SECRET"), "{e}");
    }
}
