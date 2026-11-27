//! The HTTP client (ureq and rustls, in the binary).

use std::time::Duration;

use crate::trace;

// A pure-Rust HTTP client compiled into the runtime — no `curl` shell-out, no
// process-per-request, and nothing sensitive on argv (/proc/<pid>/cmdline is
// world-readable): the URL and every header, Authorization above all, live only
// in memory. This is what let `curl` leave the Docker image entirely.

/// A pooled, blocking HTTP(S) agent shared by http-out / http-poll / oauth-poll,
/// so connections stay alive across requests (no process-per-request). Pure-Rust
/// TLS (rustls); credentials ride in the headers, in memory, never argv. It
/// follows no redirect, as the calls of a flow do not: ureq would take every
/// header but `Authorization` to wherever the answer points, an `X-Api-Key`
/// among them. A redirect is an error that says so (see [`refused`]).
static HTTP_AGENT: std::sync::OnceLock<ureq::Agent> = std::sync::OnceLock::new();
fn http_agent() -> &'static ureq::Agent {
    HTTP_AGENT.get_or_init(|| {
        ureq::AgentBuilder::new()
            .timeout(Duration::from_secs(30))
            .redirects(0)
            .user_agent("vejas")
            .build()
    })
}

/// What a URL a secret store handed out is named: none of its parts, since
/// its host may be the credential (`<id>.m.pipedream.net`).
const STORED_URL: &str = "a URL a secret store holds";

/// The host a URL names, `host[:port]`, for a log line or an error: never its
/// user info, its path or its query, where credentials ride, and nothing of a
/// URL a store handed out. A URL that does not parse is not guessed at.
pub(crate) fn host_of(url: &str) -> String {
    if crate::secrets::is_handed_out(url) {
        return STORED_URL.into();
    }
    match url::Url::parse(url) {
        Ok(url) => match (url.host_str(), url.port()) {
            (Some(host), Some(port)) => format!("{host}:{port}"),
            (Some(host), None) => host.to_string(),
            (None, _) => "a URL with no host".into(),
        },
        Err(_) => "a URL that does not parse".into(),
    }
}

/// A URL as an event names it: its scheme, host and path, without its user
/// info and its query, and with any value a secret store handed out masked;
/// nothing of a URL a store handed out.
pub(crate) fn url_shown(url: &str) -> String {
    if crate::secrets::is_handed_out(url) {
        return STORED_URL.into();
    }
    match url::Url::parse(url) {
        Ok(parsed) => crate::record::shown().error(&format!("{}://{}{}", parsed.scheme(), host_of(url), parsed.path())),
        Err(_) => "a URL that does not parse".into(),
    }
}

/// An endpoint the configuration names, `/data?code=…` or a whole URL, as an
/// error or a page names it: without its query, and a whole URL as
/// [`url_shown`] shows one, without its user info.
pub(crate) fn path_shown(endpoint: &str) -> std::borrow::Cow<'_, str> {
    if endpoint.contains("://") {
        return std::borrow::Cow::Owned(url_shown(endpoint));
    }
    std::borrow::Cow::Borrowed(&endpoint[..endpoint.find(['?', '#']).unwrap_or(endpoint.len())])
}

/// The shortest user info or query value taken for a credential: shorter
/// ones, `page=1`, would mask the answer's text everywhere they occur.
const SHORTEST_SECRET: usize = 6;
/// The shortest path segment taken for a token, which has a digit in it or
/// reads as random: a webhook's address carries its credential in the path,
/// where a resource is named shorter, or in words (`deviceManagement`). A
/// shorter token is not found.
const SHORTEST_TOKEN: usize = 16;
/// The shortest reading of a value looked for in an answer.
const SHORTEST_READING: usize = 4;
/// What an escape takes, at most, for each byte it stands for (`\u002B`).
const ESCAPED_MOST: usize = 6;
/// What is kept of a 2xx answer for the trace record and the events, once
/// the URL's values are masked in it: what the record keeps of one.
const ANSWER_KEPT: usize = 16 * 1024;

/// What of `url` could carry a credential, and the values in `also`: the user
/// info, each value of the query, a segment of the path long enough to be a
/// token, each as it is and decoded. An echo in any other form reads as one
/// of them once its escapes are read (`record::Values::found_in`).
fn credentials(url: &str, also: &[&str]) -> crate::record::Values {
    // a URL a secret store handed out is a credential whole, each of its parts
    credentials_in(url, also, crate::secrets::is_handed_out(url))
}

fn credentials_in(url: &str, also: &[&str], stored: bool) -> crate::record::Values {
    let mut values: Vec<String> = also.iter().filter(|v| !v.is_empty()).map(|v| v.to_string()).collect();
    let given = values.len();
    if let Ok(parsed) = url::Url::parse(url) {
        let mut secret = |value: &str, shortest: usize| {
            if value.len() >= shortest {
                values.push(value.to_string());
            }
        };
        secret(parsed.username(), SHORTEST_SECRET);
        secret(parsed.password().unwrap_or_default(), SHORTEST_SECRET);
        if stored {
            // a capability host, `<id>.m.pipedream.net`
            secret(parsed.host_str().unwrap_or_default(), SHORTEST_SECRET);
        }
        for pair in parsed.query().unwrap_or_default().split('&') {
            // a pair with no `=` is a value alone, `?<token>`
            let value = pair.split_once('=').map_or(pair, |(_, value)| value);
            // a `+` of a query is a space to a form: `2024-02-01+10:00:00`
            let data = is_date_or_version(&percent_decoded(value))
                || is_date_or_version(&percent_decoded(&value.replace('+', " ")));
            if stored || !data {
                secret(value, SHORTEST_SECRET);
            }
        }
        let mut uuids = Vec::new();
        for segment in parsed.path_segments().into_iter().flatten() {
            if stored {
                secret(segment, SHORTEST_SECRET);
            } else if is_uuid(segment) {
                uuids.push(segment);
            } else if (segment.bytes().any(|b| b.is_ascii_digit()) && !is_date_or_version(&percent_decoded(segment)))
                || reads_as_random(segment)
            {
                secret(segment, SHORTEST_TOKEN);
            }
        }
        // a UUID names a resource, unless nothing else of the URL can be its
        // credential: then it is one, as a webhook's (`/webhook/6f7b288e-…`)
        if values.len() == given {
            values.extend(uuids.into_iter().map(str::to_string));
        }
    }
    let mut readings = Vec::with_capacity(values.len() * 3);
    for value in values {
        // a `+` of a query is a space to a form, itself to RFC 3986
        readings.push(percent_decoded(&value.replace('+', " ")));
        readings.push(percent_decoded(&value));
        readings.push(value);
    }
    // a value that decodes to a few characters, `%3D%3D`, would mask them
    // everywhere in the answer
    readings.retain(|r| r.len() >= SHORTEST_READING);
    crate::record::Values::of(&readings)
}

/// The start of `text`, `most` bytes of it at most, with `values` masked
/// wherever they are found. The cut never splits a masked value: one that
/// starts before it is masked whole. True when the text was cut.
fn masked_start(text: &str, values: &crate::record::Values, most: usize) -> (String, bool) {
    let window = &text[..floor_char(text, most.saturating_add(values.longest().saturating_mul(ESCAPED_MOST)))];
    let cut = floor_char(window, most);
    let mut shown = String::with_capacity(cut + 16);
    let mut at = 0;
    for (start, end) in values.found_in(window) {
        if start >= cut {
            break;
        }
        if start < at {
            // overlaps the value masked before it
            at = at.max(end);
            continue;
        }
        shown.push_str(&window[at..start]);
        shown.push_str("<masked>");
        at = end;
    }
    if at < cut {
        shown.push_str(&window[at..cut]);
    }
    (shown, cut < text.len())
}

/// `at`, or the character boundary of `text` before it.
fn floor_char(text: &str, at: usize) -> usize {
    let mut at = at.min(text.len());
    while !text.is_char_boundary(at) {
        at -= 1;
    }
    at
}

/// The start of what a system answered, at most `most` bytes, for an error.
/// The record's mask first (ADR-0017): credential-named pairs and the values
/// the secret stores handed out. Then a system may echo the request in any
/// form, so what of the URL could carry a credential (see [`credentials`]) and
/// the values in `also` are masked wherever they appear, as they are or
/// through the escapes of JSON, a URL or HTML. The error reaches the log, the
/// unit's last error and the dead letter at rest.
pub(crate) fn excerpt(url: &str, body: &[u8], most: usize, also: &[&str]) -> String {
    let values = credentials(url, also);
    let window = most.saturating_add(values.longest().saturating_mul(ESCAPED_MOST));
    // the record's mask, without the cut of an error: a value of the URL that
    // ran past that cut would show its start
    let read = String::from_utf8_lossy(&body[..body.len().min(window)]);
    let text = crate::record::shown().text(&read, body.len() > window);
    masked_start(&text, &values, most).0.trim().to_string()
}

/// A date, a time or a version, which no credential is: `api-version=2024-02-01`
/// would mask each such date in the answer. Its suffix is a word, and a
/// number (`-preview`, `-rc.1`, `-beta2`), not a token (`1.0-AbCdEf0123…`). A
/// number may be a credential, a PIN or a key of digits.
fn is_date_or_version(value: &str) -> bool {
    static DATA: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    DATA.get_or_init(|| {
        regex::RegexBuilder::new(
            r"^(\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})?)?(-[a-z]{1,12})?|v?\d+(\.\d+)+(-[a-z]{1,12}(\.?\d{1,4})?)?)$",
        )
        .case_insensitive(true)
        .build()
        .expect("a valid pattern")
    })
    .is_match(value)
}

/// Letters that read as random, as a token without a digit does, base64url's
/// `-` and `_` among them: as many capitals as small letters, give or take,
/// and a change of case at least every fourth byte. A word in camel case has
/// few capitals (`CustomerOrderItems`), an acronym few changes
/// (`HTTPServerConfig`).
fn reads_as_random(segment: &str) -> bool {
    let bytes = segment.as_bytes();
    let letters = bytes.iter().filter(|b| b.is_ascii_alphabetic()).count();
    let capitals = bytes.iter().filter(|b| b.is_ascii_uppercase()).count();
    let changes = bytes.windows(2).filter(|w| w[0].is_ascii_uppercase() != w[1].is_ascii_uppercase()).count();
    bytes.iter().all(|b| b.is_ascii_alphabetic() || matches!(b, b'-' | b'_'))
        && (letters * 3..=letters * 7).contains(&(capitals * 10))
        && changes * 4 >= bytes.len()
}

/// A UUID, which in a path names a resource (`/devices/3f2b8c1e-…`).
fn is_uuid(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 36
        && bytes.iter().enumerate().all(|(i, b)| if matches!(i, 8 | 13 | 18 | 23) { *b == b'-' } else { b.is_ascii_hexdigit() })
}

/// What of a sink's URL could carry a credential, to mask in what its system
/// answers: an answer may echo the request, as a 2xx does as well as an error.
pub(crate) struct Echoes(crate::record::Values);

impl Echoes {
    pub(crate) fn of(url: &str) -> Echoes {
        Echoes(credentials(url, &[]))
    }

    /// A 2xx answer as the trace record and the events keep it: the record's
    /// mask, then the URL's values masked wherever they are found, and of an
    /// answer longer than the record keeps, only its start.
    pub(crate) fn masked(&self, text: String) -> String {
        if self.0.is_empty() {
            return text;
        }
        let read = &text[..floor_char(&text, ANSWER_KEPT.saturating_add(self.0.longest().saturating_mul(ESCAPED_MOST)))];
        let shown = crate::record::shown().text(read, read.len() < text.len());
        let (mut kept, cut) = masked_start(&shown, &self.0, ANSWER_KEPT);
        if cut || read.len() < text.len() {
            kept.push('…');
        }
        kept
    }
}

fn percent_decoded(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = |b: u8| (b as char).to_digit(16);
        match (bytes[i], bytes.get(i + 1).copied().and_then(hex), bytes.get(i + 2).copied().and_then(hex)) {
            (b'%', Some(high), Some(low)) => {
                out.push((high * 16 + low) as u8);
                i += 3;
            }
            (byte, _, _) => {
                out.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The statuses of a redirect, whose `Location` the answer points to.
fn redirects(code: u16) -> bool {
    matches!(code, 301 | 302 | 303 | 307 | 308)
}

/// The error of an answer outside 2xx: a redirect, which is not followed, or
/// the status and the start of the answer (see [`excerpt`]).
pub(super) fn refused(code: u16, url: &str, body: &[u8], most: usize, also: &[&str]) -> String {
    if redirects(code) {
        return format!(
            "HTTP {code} from {}: a redirect, not followed, since the request's headers would go along: set the URL to the address it points to",
            host_of(url)
        );
    }
    format!("HTTP {code}: {}", excerpt(url, body, most, also))
}

/// The agent of the calls a flow makes (ADR-0032). It follows no redirect: a
/// redirect would take the service's headers, its credentials among them, to
/// wherever the answer points. A service that takes a 3xx lists it in ANSWERS
/// and reads `location` itself.
static CALL_AGENT: std::sync::OnceLock<ureq::Agent> = std::sync::OnceLock::new();
fn call_agent() -> &'static ureq::Agent {
    CALL_AGENT.get_or_init(|| ureq::AgentBuilder::new().redirects(0).user_agent("vejas").build())
}

/// What a request to `url` that did not get through says: the host it went
/// to, the kind of failure and its cause. Never the URL or a header: ureq's
/// own text starts with the whole URL, its query and user info included, and
/// a header it cannot send is quoted with its value. A secret in either would
/// reach the run's error, its span, the log and the dead letter. Nothing of a
/// URL a store handed out.
fn transport_error(t: &ureq::Transport, url: &str) -> String {
    let mut text = String::new();
    if crate::secrets::is_handed_out(url) {
        text.push_str(STORED_URL);
        text.push_str(": ");
    } else if let Some(host) = t.url().and_then(|url| Some((url.host_str()?, url.port_or_known_default()))) {
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
        Err(ureq::Error::Transport(t)) => return Err(format!("http: {}", transport_error(&t, url))),
    };
    let code = response.status();
    let mut buf = Vec::new();
    // of an answer outside 2xx, only its start is ever shown
    let most = if (200..300).contains(&code) { u64::MAX } else { ERROR_BODY_MOST };
    response
        .into_reader()
        .take(most)
        .read_to_end(&mut buf)
        .map_err(|e| e.to_string())?;
    Ok((code, buf))
}

pub(super) fn http_get(url: &str, headers: &[(String, String)]) -> Result<Vec<u8>, String> {
    match http_request("GET", url, headers, None)? {
        (code, body) if (200..300).contains(&code) => Ok(body),
        (code, body) => Err(refused(code, url, &body, 300, &[])),
    }
}

/// How much of an answer outside 2xx is read: its start is all an error shows.
const ERROR_BODY_MOST: u64 = 64 * 1024;

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
                | ureq::ErrorKind::ProxyConnect => NoAnswer::Network(transport_error(&t, url)),
                _ => NoAnswer::Broken(transport_error(&t, url)),
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
        (code, out) => Err(refused(code, url, &out, 300, &[])),
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

/// The URL a link in the answer of the page at `page` points to: absolute;
/// a query alone or a link without its scheme, read against the page; else
/// relative to `base`, or to the page's origin when the page is elsewhere (an
/// absolute endpoint). One elsewhere than `page`, another scheme, host or
/// port, is an error that calls the link `what`: the request's token would go
/// along, as it would with a redirect.
pub(super) fn link_url(base: &str, page: &str, link: &str, what: &str) -> Result<String, String> {
    let origin = |url: &str| url::Url::parse(url).map(|url| url.origin());
    let shown = |url: &str| match url::Url::parse(url) {
        _ if crate::secrets::is_handed_out(url) => STORED_URL.to_string(),
        Ok(parsed) => format!("{}://{}", parsed.scheme(), host_of(url)),
        Err(_) => "a URL that does not parse".into(),
    };
    let next = if link.starts_with("http://") || link.starts_with("https://") {
        link.to_string()
    } else if link.starts_with("//") || link.starts_with('?') {
        url::Url::parse(page).and_then(|page| page.join(link)).map_or_else(|_| link.to_string(), String::from)
    } else {
        match (origin(base), origin(page)) {
            (Ok(base_origin), Ok(page_origin)) if base_origin != page_origin => {
                join_url(&page_origin.ascii_serialization(), link)
            }
            _ => join_url(base, link),
        }
    };
    match (origin(&next), origin(page)) {
        (Ok(there), Ok(here)) if there == here => Ok(next),
        _ => Err(format!(
            "{what} is at {}, not {}: not followed, since the token would go along",
            shown(&next),
            shown(page)
        )),
    }
}

/// The next page URL out of the body of the page at `page`, if any (see
/// [`link_url`]).
pub(super) fn next_url(base: &str, page: &str, body: &serde_json::Value, field: &str) -> Result<Option<String>, String> {
    let Some(next) = body.get(field).and_then(|v| v.as_str()).filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    link_url(base, page, next, "the next page").map(Some)
}

/// An id as one segment of a path, percent-encoded, so that a `/` or a `?` of
/// its own does not move the call (`../y?z`). None for an id that is no
/// segment, empty, `.` or `..`, which a URL reads as a move.
pub(super) fn path_segment(id: &str) -> Option<String> {
    (!matches!(id, "" | "." | "..")).then(|| urlencode(id))
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
        let page = "https://api.x/v1/users";
        assert_eq!(
            next_url("https://api.x/v1", page, &body, "@odata.nextLink").unwrap().as_deref(),
            Some("https://api.x/v1/users?$skip=2")
        );
        let rel = json!({"next": "/users?page=2"});
        assert_eq!(
            next_url("https://api.x/v1", page, &rel, "next").unwrap().as_deref(),
            Some("https://api.x/v1/users?page=2")
        );
        // the page of an absolute endpoint elsewhere than the base: its links
        // read against its own origin
        let pages = json!({"next": "/pages?p=2"});
        assert_eq!(
            next_url("https://api.x/v1", "http://localhost:4494/pages", &pages, "next").unwrap().as_deref(),
            Some("http://localhost:4494/pages?p=2")
        );
        // a query alone reads against the page, a link without its scheme too
        let query = json!({"next": "?p=2"});
        assert_eq!(
            next_url("https://api.x/v1", "https://api.x/v1/users?p=1", &query, "next").unwrap().as_deref(),
            Some("https://api.x/v1/users?p=2")
        );
        let far = json!({"next": "//evil.x/steal"});
        assert!(next_url("https://api.x/v1", page, &far, "next").unwrap_err().starts_with("the next page is at https://evil.x"));
        // a detail that is an item's link, elsewhere: not followed
        let e = link_url("https://api.x/v1", "https://api.x/v1/items", "http://127.0.0.1:4492/steal", "its detail").unwrap_err();
        assert!(e.starts_with("its detail is at http://127.0.0.1:4492, not https://api.x"), "{e}");
        // an id is one segment of a path
        assert_eq!(path_segment("../y?z").as_deref(), Some("..%2Fy%3Fz"));
        assert_eq!(path_segment("u-42").as_deref(), Some("u-42"));
        assert_eq!(path_segment(".."), None);
        assert_eq!(path_segment(""), None);
        assert_eq!(next_url("https://api.x/v1", page, &json!({"value": []}), "next"), Ok(None));
        // elsewhere, the token would go along: another host, the same in clear, another port
        for elsewhere in ["https://evil.example/steal?$skip=2", "http://api.x/v1/users?$skip=2", "https://api.x:8443/v1"] {
            let e = next_url("https://api.x/v1", page, &json!({"next": elsewhere}), "next").unwrap_err();
            assert!(e.starts_with("the next page is at ") && e.ends_with("not https://api.x: not followed, since the token would go along"), "{e}");
        }
        // an endpoint the configuration puts on another host pages there
        let other = "https://other.example/items";
        let next = json!({"next": "https://other.example/items?page=2"});
        assert_eq!(next_url("https://api.x/v1", other, &next, "next").unwrap().as_deref(), Some("https://other.example/items?page=2"));
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

    #[test]
    fn a_url_is_named_without_its_user_info_and_its_query() {
        let url = "https://svc:pw-SECRET-1@api.x:8443/v1/orders?api_key=SECRET-2#top";
        assert_eq!(host_of(url), "api.x:8443");
        assert_eq!(url_shown(url), "https://api.x:8443/v1/orders");
        assert_eq!((host_of("http://h"), url_shown("http://h").as_str()), ("h".into(), "http://h/"));
        assert_eq!((host_of("https://h?x=1"), url_shown("https://h?x=1").as_str()), ("h".into(), "https://h/"));
        assert_eq!(host_of("http://[::1]:8080/x"), "[::1]:8080");
        // a password with a raw `/` is not taken for the host
        assert_eq!(host_of("http://u:pa/ss@h/"), "a URL that does not parse");
        assert_eq!((host_of("h/p?q=1"), url_shown("h/p?q=1")), ("a URL that does not parse".into(), "a URL that does not parse".into()));
        assert_eq!(path_shown("/data?code=K3Y#x"), "/data");
    }

    #[test]
    fn an_answer_that_echoes_the_request_keeps_its_credentials_out() {
        let url = "http://svc:pw-SECRET-1@h:1/orders?api_key=SECRET-2&page=1";
        let e = refused(400, url, b"no route for GET /orders?api_key=SECRET-2&page=1 (svc:pw-SECRET-1)", 300, &[]);
        assert_eq!(e, "HTTP 400: no route for GET /orders?api_key=<masked>&page=1 (svc:<masked>)");
        // a credential-named pair, under the record's mask (ADR-0017)
        let e = refused(401, "http://h/x", br#"{"error":"bad","client_secret":"K3Y"}"#, 300, &[]);
        assert!(!e.contains("K3Y"), "{e}");
        // a redirect is not followed, and says where to look
        let e = refused(302, url, b"<a href=/elsewhere?api_key=SECRET-2>", 300, &[]);
        assert!(e.starts_with("HTTP 302 from h:1: a redirect, not followed") && !e.contains("SECRET"), "{e}");
        // 304 is no redirect
        assert!(refused(304, url, b"", 300, &[]).starts_with("HTTP 304: "));
    }

    #[test]
    fn an_echo_is_masked_in_every_form_it_takes() {
        // names the pair mask does not know, a value with characters to escape
        let url = "https://alice@h/hook/T0001/B0001/SECRETPATHTOKEN99?key=LIT-K3Y-0001&sig=ab/cd+ef&owner=alice&page=1";
        for body in [
            // HTML, Go's JSON, decoded, the value alone, the path
            "<p>bad request: /hook?key=LIT-K3Y-0001&amp;sig=ab%2Fcd%2Bef&amp;page=1</p>",
            r#"{"url":"/x?key=LIT-K3Y-0001\u0026sig=ab/cd ef"}"#,
            "the signature ab/cd ef is not valid for LIT-K3Y-0001",
            "sig=ab%2fcd%20ef",
            "\"LIT-K3Y-0001\" is not a valid key",
            "no webhook /hook/T0001/B0001/SECRETPATHTOKEN99",
        ] {
            let e = excerpt(url, body.as_bytes(), 300, &[]);
            let shown = e.to_ascii_lowercase();
            assert!(!e.contains("LIT-K3Y") && !e.contains("SECRETPATH") && !shown.contains("ab/cd") && !shown.contains("ab%2fcd"), "{body} → {e}");
        }
        // a user name that is also a query value does not stop the query's mask
        let e = excerpt(url, b"owner alice, key LIT-K3Y-0001", 300, &[]);
        assert!(!e.contains("LIT-K3Y"), "{e}");
        // what the caller names, as a client secret, is masked the same way,
        // in a text that names it as nothing the record's mask reads
        let e = excerpt("http://h/token", b"no client holds s3cr%2Ft-value", 300, &["s3cr/t-value"]);
        assert_eq!(e, "no client holds <masked>");
    }

    #[test]
    fn an_echo_is_found_through_any_escape() {
        let url = "http://h/x?php=AB%2FCD%2BEF-PHP01&net=AB%2BCD-NET01&pyq=AB%2FCD%2BEF-PYQ01&htm=AB%2FCD%2BEF-HTM01&til=AB~CD-TIL01";
        for body in [
            // PHP's json_encode, .NET's System.Text.Json, Python's quote(),
            // HTML's numeric entities, a `~` percent-encoded
            r#"{"message":"Invalid signature: AB\/CD+EF-PHP01"}"#,
            r#"{"message":"AB\u002BCD-NET01 is not valid"}"#,
            "bad pyq=AB/CD%2BEF-PYQ01",
            "<p>AB&#x2F;CD&#43;EF-HTM01</p>",
            "til=AB%7ECD-TIL01",
            // PHP's htmlentities(…, ENT_HTML5)
            "<p>AB&sol;CD&plus;EF-HTM01</p>",
        ] {
            let e = excerpt(url, body.as_bytes(), 300, &[]);
            assert!(!["PHP01", "NET01", "PYQ01", "HTM01", "TIL01"].iter().any(|tail| e.contains(tail)), "{body} → {e}");
            assert!(e.contains("<masked>"), "{body} → {e}");
        }
    }

    #[test]
    fn a_value_that_names_a_credential_leaves_the_record_its_pair() {
        // the record's mask goes first: the pair's value is masked by its name
        let e = excerpt("http://h/x?include=password", br#"{"password":"hunter2-PAIR01"}"#, 300, &[]);
        assert!(!e.contains("hunter2"), "{e}");
    }

    #[test]
    fn a_date_or_a_version_in_the_url_is_no_credential() {
        let url = "http://h/v1/reports/2024-02-01T00:00:00?api-version=2024-02-01-preview&v=1.2.3&key=K3Y-DATE-01";
        let e = excerpt(url, b"created 2024-02-01-preview for 2024-02-01T00:00:00 in 1.2.3 with K3Y-DATE-01", 300, &[]);
        assert_eq!(e, "created 2024-02-01-preview for 2024-02-01T00:00:00 in 1.2.3 with <masked>");
        // a number may be a key, a PIN
        let e = excerpt("http://h/x?key=73950264817", b"key 73950264817 is unknown", 300, &[]);
        assert_eq!(e, "key <masked> is unknown");
        // a version's suffix is a word and a number, not a token
        let url = "http://h/x?a=2.0.0-rc.1&b=1.4.2-beta2&c=1.0-SNAPSHOT&key=1.0-AbCdEf0123456789XyZ";
        let e = excerpt(url, b"2.0.0-rc.1 1.4.2-beta2 1.0-SNAPSHOT 1.0-AbCdEf0123456789XyZ", 300, &[]);
        assert_eq!(e, "2.0.0-rc.1 1.4.2-beta2 1.0-SNAPSHOT <masked>");
        // a UUID in a path names a resource, when the URL has a credential
        let url = "http://h/sap/devices/3f2b8c1e-9a4d-4e6b-8c2f-1a2b3c4d5e6f?sap-client=100&code=K3Y-UUID-01";
        let e = excerpt(url, b"Resource not found for segment '3f2b8c1e-9a4d-4e6b-8c2f-1a2b3c4d5e6f'", 300, &[]);
        assert_eq!(e, "Resource not found for segment '3f2b8c1e-9a4d-4e6b-8c2f-1a2b3c4d5e6f'");
        // and is the credential when nothing else can be, as a webhook's
        let url = "http://h/webhook/6f7b288e-1efe-4504-a6fd-660931327269";
        let e = excerpt(url, br#"The requested webhook "POST 6f7b288e-1efe-4504-a6fd-660931327269" is not registered."#, 300, &[]);
        assert_eq!(e, r#"The requested webhook "POST <masked>" is not registered."#);
    }

    #[test]
    fn a_token_of_letters_alone_reads_as_random() {
        assert!(reads_as_random("hTqWzKeLmPbXcVnRaYsDfGuJ"));
        // base64url's signs among them
        assert!(reads_as_random("aBcD-eFgH_iJkLmNoPqRsTuV"));
        for words in [
            "deviceManagement", "CustomerOrderItemsCollection", "HTTPServerConfig", "abcdefghijklmnopqrstuvwx", "A_BusinessPartnerX",
            "customer-orders-by-region", "Sales_Order_Header_Items",
        ] {
            assert!(!reads_as_random(words), "{words}");
        }
        let e = excerpt("http://h/hook/T0001/B0001/hTqWzKeLmPbXcVnRaYsDfGuJ", b"no webhook hTqWzKeLmPbXcVnRaYsDfGuJ", 300, &[]);
        assert_eq!(e, "no webhook <masked>");
        let e = excerpt("http://h/odata/CustomerOrderItemsCollection", b"no CustomerOrderItemsCollection", 300, &[]);
        assert_eq!(e, "no CustomerOrderItemsCollection");
        // a time sent as a form writes it, its space a `+`
        let e = excerpt("http://h/x?since=2024-02-01+10:00:00", b"nothing since 2024-02-01+10:00:00", 300, &[]);
        assert_eq!(e, "nothing since 2024-02-01+10:00:00");
    }

    #[test]
    fn of_an_answer_outside_2xx_only_64_kib_are_read() {
        use std::io::{Read as _, Write as _};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            for code in [500, 200] {
                let Ok((mut stream, _)) = listener.accept() else { return };
                let mut request = [0u8; 1024];
                let _ = stream.read(&mut request);
                let body = vec![b'x'; 200 * 1024];
                let head = format!("HTTP/1.1 {code} X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
                let _ = stream.write_all(head.as_bytes());
                let _ = stream.write_all(&body);
            }
        });
        let url = format!("http://127.0.0.1:{port}/big");
        let (code, body) = http_request("GET", &url, &[], None).unwrap();
        assert_eq!((code, body.len()), (500, ERROR_BODY_MOST as usize));
        let (code, body) = http_request("GET", &url, &[], None).unwrap();
        assert_eq!((code, body.len()), (200, 200 * 1024));
    }

    #[test]
    fn a_url_a_store_handed_out_is_a_credential_whole() {
        // a webhook's token with no digit, and a date, in a URL a store holds
        let url = "https://h/services/T0001/B0001/abcdefghijklmnopqrstuvwx?since=2024-02-01";
        let text = "no webhook /services/T0001/B0001/abcdefghijklmnopqrstuvwx since 2024-02-01";
        // each segment of 6 bytes or more, a word too: the store holds it whole
        let (e, _) = masked_start(text, &credentials_in(url, &[], true), 300);
        assert_eq!(e, "no webhook /<masked>/T0001/B0001/<masked> since <masked>");
        let (e, _) = masked_start(text, &credentials_in(url, &[], false), 300);
        assert_eq!(e, text);
        // its host, and its segments from 6 bytes
        let url = "https://capsecret7f3k9qzx.invalid/hook/catch/1234567/abcdefg/";
        let text = "no hook catch/1234567/abcdefg at capsecret7f3k9qzx.invalid";
        let (e, _) = masked_start(text, &credentials_in(url, &[], true), 300);
        assert_eq!(e, "no hook catch/<masked>/<masked> at <masked>");
    }

    #[test]
    fn a_url_a_store_handed_out_is_named_by_none_of_its_parts() {
        // a scheme no text of another test ends in the start of
        let url = "zqnamed://Nm4-stored-host.invalid:8443/x/y?k=v";
        crate::secrets::hand_out(url);
        assert_eq!((host_of(url), url_shown(url)), (STORED_URL.to_string(), STORED_URL.to_string()));
        assert_eq!(path_shown(url), STORED_URL);
        let refused = refused(302, url, b"", 300, &[]);
        assert!(refused.contains(STORED_URL) && !refused.contains("Nm4"), "{refused}");
    }

    #[test]
    fn a_long_value_of_the_url_is_masked_past_the_cut_of_an_error() {
        // past the 2048 bytes the record keeps of an error
        let sts = "Zq9".repeat(700);
        let url = format!("http://h/x?sts={sts}");
        let body = format!("{}{sts} is expired", "x".repeat(100));
        let e = excerpt(&url, body.as_bytes(), 300, &[]);
        assert!(!e.contains("Zq9Zq9"), "{e}");
        // the cut falls inside it: what follows is past the excerpt
        assert!(e.ends_with("<masked>"), "{e}");
    }

    #[test]
    fn a_resource_named_in_words_is_no_token() {
        let url = "http://h/sap/opu/odata/sap/API_BUSINESS_PARTNER/A_BusinessPartnerX";
        let e = excerpt(url, b"Resource not found for segment 'A_BusinessPartnerX'", 300, &[]);
        assert_eq!(e, "Resource not found for segment 'A_BusinessPartnerX'");
        let e = excerpt("https://graph.x/v1.0/deviceManagement/managedDevices", b"no deviceManagement here", 300, &[]);
        assert_eq!(e, "no deviceManagement here");
    }

    #[test]
    fn a_2xx_answer_keeps_the_values_of_its_url_out() {
        let echoes = Echoes::of("http://h/okecho?key=OKECHO-K3Y-01");
        let kept = echoes.masked(r#"{"ok":true,"url":"/okecho?key=OKECHO-K3Y-01"}"#.to_string());
        assert!(!kept.contains("OKECHO-K3Y") && kept.contains("<masked>"), "{kept}");
        // past what the record keeps, the start only
        let long = format!("{}OKECHO-K3Y-01", "x".repeat(ANSWER_KEPT + 100));
        let kept = echoes.masked(long);
        assert!(kept.ends_with('…') && !kept.contains("OKECHO") && kept.len() < ANSWER_KEPT + 16, "{}", kept.len());
        // a URL with nothing to mask leaves the answer as it came
        assert_eq!(Echoes::of("http://h/plain").masked("as it came".to_string()), "as it came");
    }

    #[test]
    fn an_absolute_endpoint_is_shown_without_its_user_info() {
        assert_eq!(path_shown("http://svc:pw-USERINFO-01@h:1/missing?x=1"), "http://h:1/missing");
        assert_eq!(path_shown("/data?code=K3Y"), "/data");
    }

    #[test]
    fn the_cut_never_splits_a_masked_value() {
        let url = "http://h/x?key=LIT-K3Y-SPLIT";
        // an echo across the cut is masked whole
        let body = format!("{}LIT-K3Y-SPLIT and more", "x".repeat(295));
        let e = excerpt(url, body.as_bytes(), 300, &[]);
        assert!(e.ends_with("<masked>") && !e.contains("LIT"), "{e}");
        // and one after the cut is not shown at all
        let body = format!("key=LIT-K3Y-SPLIT {} LIT-K3Y-SPLIT&units=metric", "y".repeat(300));
        let e = excerpt(url, body.as_bytes(), 300, &[]);
        assert!(!e.contains("LIT") && !e.contains("metri"), "{e}");
        // a cut inside a character backs off to its start
        let e = excerpt(url, "ééé".as_bytes(), 3, &[]);
        assert_eq!(e, "é");
    }

    #[test]
    fn a_redirect_takes_no_header_elsewhere() {
        use std::io::{Read as _, Write as _};
        let elsewhere = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        elsewhere.set_nonblocking(true).unwrap();
        let there = elsewhere.local_addr().unwrap().port();
        let server = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let here = server.local_addr().unwrap().port();
        // two requests, a GET and a POST, each answered with a redirect
        let answering = std::thread::spawn(move || {
            for _ in 0..2 {
                let (mut conn, _) = server.accept().unwrap();
                let mut request = [0u8; 2048];
                let _ = conn.read(&mut request);
                let answer = format!("HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:{there}/stolen\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                conn.write_all(answer.as_bytes()).unwrap();
            }
        });
        let headers = [("X-Api-Key".to_string(), "SECRET-4".to_string())];
        let url = format!("http://127.0.0.1:{here}/orders");
        let get = http_get(&url, &headers).unwrap_err();
        let post = http_post(&url, &headers, b"{}", None).unwrap_err();
        answering.join().unwrap();
        for e in [get, post] {
            assert!(e.starts_with(&format!("HTTP 302 from 127.0.0.1:{here}: a redirect, not followed")), "{e}");
        }
        assert!(elsewhere.accept().is_err(), "the redirect's target got a connection");
    }
}
