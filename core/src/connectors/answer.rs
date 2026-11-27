//! Services that answer (ADR-0032). A file in `services/` may declare a
//! connector instead of holding code: `driver "http-rpc"` and its settings. A
//! flow invokes it as it invokes VejasScript and gets what the system
//! answered, or its run fails with an error that names the service and the
//! cause, and says whether a later delivery of the message may try again.

use std::time::Duration;

use serde_json::{Map, Value};

use super::http_client::{excerpt, http_call, urlencode, NoAnswer};
use super::Config;
use crate::trace;

/// The drivers a service can declare.
pub const DRIVERS: &[&str] = &["http-rpc"];

/// The longest a message waits for its next try, whatever DELAY_MS says.
const DELAY_MAX: Duration = Duration::from_secs(3600);

/// Why a call gave its run no answer, and what the message does next.
#[derive(Clone, Debug)]
pub struct Failure {
    /// The run's error, which names the service: `get_order: HTTP 503`.
    pub error: String,
    /// The service, as the flow named it: what a `catch` says failed.
    pub service: String,
    /// The status the system answered with, when it answered.
    pub status: Option<u16>,
    /// A later delivery may get an answer: by default after a 429, a 5xx, a
    /// timeout or a network error. The message is then delivered again after
    /// `delay`, while it has had fewer deliveries than `attempts`; otherwise it
    /// is a dead letter at once.
    pub retry: bool,
    /// No answer came within the call's `TIMEOUT_MS`: an `api` flow answers 504.
    pub timed_out: bool,
    pub attempts: i64,
    pub delay: Duration,
    /// What the system's outage does to a flow on the bus (ADR-0040): it
    /// holds its message, or the message comes back until it is a dead letter.
    pub down: WhenDown,
}

/// What a service's `WHEN_DOWN` says (ADR-0040). `Hold` unless the service
/// says `"dead-letter"`: the founder chose hold as the default.
#[derive(Clone, Debug, Default)]
pub enum WhenDown {
    /// Today's path: the message comes back after `DELAY_MS`, then it is a
    /// dead letter or its catch runs. Also what a failure that is not a call's
    /// takes: a secret store or a memory that did not answer.
    #[default]
    DeadLetter,
    /// The flow holds its message and probes until the system answers.
    Hold(Probe),
}

/// How a held flow asks whether the system is back (ADR-0040).
#[derive(Clone)]
pub struct Probe {
    /// `HEALTH_URL`, filled from the service's settings, with the service's
    /// headers: a GET that answers 2xx when the system is up. None: the probe
    /// is the held message's run itself.
    health: Option<(String, Vec<(String, String)>, Duration)>,
    /// `PROBE_MAX_MS`: the longest wait between two probes.
    pub max: Duration,
}

impl std::fmt::Debug for Probe {
    // The headers hold the service's credentials: never printed.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Probe")
            .field("health", &self.health.as_ref().map(|(url, ..)| url))
            .field("max", &self.max)
            .finish()
    }
}

impl Probe {
    /// Whether the probe asks a health URL rather than running the message.
    pub fn has_health(&self) -> bool {
        self.health.is_some()
    }
    /// Ask the health URL: whether it answered 2xx, or why not. Only called
    /// when `has_health`.
    pub fn ask(&self) -> Result<(), String> {
        let Some((url, headers, timeout)) = &self.health else {
            return Ok(());
        };
        match http_call("GET", url, headers, None, *timeout, None) {
            Ok(answer) if (200..300).contains(&answer.status) => Ok(()),
            Ok(answer) => Err(format!("HEALTH_URL answered {}", answer.status)),
            Err(NoAnswer::TimedOut) => Err(format!("HEALTH_URL: timeout after {} ms", timeout.as_millis())),
            Err(NoAnswer::Network(e)) => Err(shown(format!("HEALTH_URL: network: {e}"))),
            Err(NoAnswer::Broken(e)) => Err(shown(format!("HEALTH_URL: {e}"))),
        }
    }
}

/// What a declaration's `WHEN_DOWN`, `HEALTH_URL` and `PROBE_MAX_MS` say.
fn when_down(config: &Config) -> WhenDown {
    if config.str("WHEN_DOWN").is_some_and(|w| w == "dead-letter") {
        return WhenDown::DeadLetter;
    }
    let timeout = Duration::from_millis(config.u64_or("TIMEOUT_MS", 10_000));
    let health = config
        .str("HEALTH_URL")
        .and_then(|template| filled(&template, &Map::new(), config).ok())
        .map(|url| (url, config.headers(), timeout));
    let max = Duration::from_millis(config.u64_or("PROBE_MAX_MS", 60_000).max(1)).min(DELAY_MAX);
    WhenDown::Hold(Probe { health, max })
}

/// The values `WHEN_DOWN` takes.
pub const WHEN_DOWN: &[&str] = &["hold", "dead-letter"];

impl Failure {
    /// Whether a flow on the bus holds its message on this failure: a later
    /// try may change it, and its service holds while its system is down
    /// (ADR-0040).
    pub fn holds(&self) -> bool {
        self.retry && matches!(self.down, WhenDown::Hold(_))
    }

    /// A declaration whose settings could not be resolved. A secret store
    /// that did not answer may answer a later delivery, at the pace a message
    /// comes back (`unanswered`); a secret the store says it does not have,
    /// or a setting its code cannot give, no delivery changes.
    pub fn unresolved(service: &str, error: String, later: bool) -> Self {
        let failure = if later {
            Failure::unanswered(error)
        } else {
            Failure {
                error: shown(error),
                service: String::new(),
                status: None,
                retry: false,
                timed_out: false,
                attempts: 1,
                delay: Duration::ZERO,
                down: WhenDown::DeadLetter,
            }
        };
        Failure { service: service.to_string(), ..failure }
    }
}

impl Failure {
    /// What a run needed from the runtime did not come: a secret store, or
    /// the flow's memory, did not answer. A later delivery may get it, at the
    /// pace a message comes back when nothing says otherwise, up to the last
    /// delivery the runtime makes (ADR-0039).
    pub fn unanswered(error: String) -> Self {
        Failure {
            error: shown(error),
            service: String::new(),
            status: None,
            retry: true,
            timed_out: false,
            attempts: vejas_connector::MAX_DELIVERIES,
            // the consumer's ack wait; unset, it is JetStream's 30 s, and a
            // nak with no delay would spend the deliveries at once
            delay: Some(vejas_connector::pull::ack_wait())
                .filter(|wait| !wait.is_zero())
                .unwrap_or(Duration::from_secs(30)),
            down: WhenDown::DeadLetter,
        }
    }
}

/// An error as it leaves a call: masked as the record masks one (ADR-0017).
/// A value a secret store handed out, or a credential-named pair a system
/// echoed in its answer, then reaches no log, span, answer or dead letter.
fn shown(error: String) -> String {
    crate::record::shown().error(&error)
}

/// What went wrong, before the service's name and its policy are added.
struct Why {
    error: String,
    retry: bool,
    timed_out: bool,
    status: Option<u16>,
}

impl Why {
    /// A cause no later try can change.
    fn fixed(error: String) -> Self {
        Why { error, retry: false, timed_out: false, status: None }
    }
}

/// Call the service the flow names `name`, declared with `driver` and its
/// `config` (its secrets resolved), with the flow's arguments, carrying the
/// run's trace.
pub fn call(
    name: &str,
    driver: &str,
    config: &Config,
    args: &Map<String, Value>,
    trace: Option<&trace::Context>,
) -> Result<Map<String, Value>, Failure> {
    let answered = match driver {
        "http-rpc" => http_rpc(config, args, trace),
        other => Err(Why::fixed(format!("no driver {other:?} answers a call; a service declares http-rpc"))),
    };
    answered.map_err(|why| Failure {
        error: shown(format!("{name}: {}", why.error)),
        service: name.to_string(),
        status: why.status,
        retry: why.retry,
        timed_out: why.timed_out,
        attempts: config.0.get("ATTEMPTS").and_then(Value::as_i64).unwrap_or(3).max(1),
        delay: Duration::from_millis(config.u64_or("DELAY_MS", 1000)).min(DELAY_MAX),
        down: when_down(config),
    })
}

/// `driver "http-rpc"`: one HTTP request, its answer `{status, body, headers}`.
fn http_rpc(
    config: &Config,
    args: &Map<String, Value>,
    trace: Option<&trace::Context>,
) -> Result<Map<String, Value>, Why> {
    let method = config.str_or("METHOD", "GET").to_ascii_uppercase();
    let template = config.str("URL").ok_or_else(|| Why::fixed("its declaration has no URL".into()))?;
    let url = filled(&template, args, config).map_err(Why::fixed)?;
    let body = args.get("body").map(|b| serde_json::to_vec(b).unwrap_or_default());
    let mut headers = config.headers();
    if body.is_some() && !headers.iter().any(|(k, _)| k.eq_ignore_ascii_case("content-type")) {
        headers.push(("Content-Type".into(), "application/json".into()));
    }
    let timeout = config.u64_or("TIMEOUT_MS", 10_000);
    let answer = http_call(&method, &url, &headers, body.as_deref(), Duration::from_millis(timeout), trace)
        .map_err(|no| match no {
            NoAnswer::TimedOut => Why { error: format!("timeout after {timeout} ms"), retry: true, timed_out: true, status: None },
            NoAnswer::Network(e) => Why { error: format!("network: {e}"), retry: true, timed_out: false, status: None },
            NoAnswer::Broken(e) => Why::fixed(e),
        })?;
    let status = answer.status;
    if !listed(config, "ANSWERS", status).unwrap_or((200..300).contains(&status)) {
        return Err(Why {
            error: format!("HTTP {status}{}", refusal(&url, &answer.body)),
            retry: listed(config, "RETRY_ON", status).unwrap_or(status == 429 || status >= 500),
            timed_out: false,
            status: Some(status),
        });
    }
    let mut out = Map::new();
    out.insert("status".into(), status.into());
    out.insert("body".into(), read(&answer.body));
    out.insert("headers".into(), Value::Object(answer.headers));
    Ok(out)
}

/// A URL as its declaration writes it: text, and the `{name}`s to fill.
enum Piece<'a> {
    Text(&'a str),
    Name(&'a str),
}

fn pieces(template: &str) -> Result<Vec<Piece<'_>>, String> {
    let mut out = Vec::new();
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        if open > 0 {
            out.push(Piece::Text(&rest[..open]));
        }
        let after = &rest[open + 1..];
        let close = after.find('}').ok_or("a { in its URL is not closed")?;
        out.push(Piece::Name(&after[..close]));
        rest = &after[close + 1..];
    }
    if !rest.is_empty() {
        out.push(Piece::Text(rest));
    }
    Ok(out)
}

/// Whether arguments named `args` fit a service whose URL is `template` and
/// whose declaration names `settings` (ADR-0032, point 1). Each `{name}` of the
/// URL is an argument or a setting, and each argument but `body` fills one, so
/// that a misspelled argument is an error and not a request made without it.
/// No argument has the name of a setting: the settings are the service's own,
/// and a flow does not choose where the service's credentials go.
pub fn fits(template: &str, args: &[&str], settings: &[&str]) -> Result<(), String> {
    let names: Vec<&str> = pieces(template)?
        .into_iter()
        .filter_map(|piece| match piece {
            Piece::Name(name) => Some(name),
            Piece::Text(_) => None,
        })
        .collect();
    if let Some(arg) = args.iter().find(|arg| settings.contains(arg)) {
        return Err(format!("the argument {arg} has the name of a setting of the service"));
    }
    if let Some(arg) = args.iter().find(|arg| **arg != "body" && !names.contains(arg)) {
        return Err(format!("the argument {arg} appears nowhere in its URL"));
    }
    match names.iter().find(|name| !args.contains(name) && !settings.contains(name)) {
        Some(name) => Err(format!("its URL names {{{name}}}, which is no argument and no setting of the service")),
        None => Ok(()),
    }
}

/// The URL with each `{name}` filled: an argument, percent-encoded, or one of
/// the declaration's settings, as written. An argument may not be null, nor
/// make a segment of the path empty, `.` or `..`, which would move the call to
/// another resource of the system.
fn filled(template: &str, args: &Map<String, Value>, config: &Config) -> Result<String, String> {
    let given: Vec<&str> = args.keys().map(String::as_str).collect();
    let settings: Vec<&str> = config.0.keys().map(String::as_str).collect();
    fits(template, &given, &settings)?;
    let mut url = String::with_capacity(template.len() + 16);
    // where each argument went in the URL
    let mut spans: Vec<(&str, usize, usize)> = Vec::new();
    for piece in pieces(template)? {
        match piece {
            Piece::Text(text) => url.push_str(text),
            Piece::Name(name) => match args.get(name) {
                Some(Value::Null) => return Err(format!("the argument {name} is null")),
                Some(value) => {
                    let start = url.len();
                    url.push_str(&urlencode(&text(value)));
                    spans.push((name, start, url.len()));
                }
                None => url.push_str(&config.0.get(name).map(text).unwrap_or_default()),
            },
        }
    }
    // An argument is percent-encoded: no `/`, `?` or `#` comes from one, so the
    // template and the settings say where the path's segments are.
    let path = &url[..url.find(['?', '#']).unwrap_or(url.len())];
    let mut start = 0;
    for segment in path.split('/') {
        let end = start + segment.len();
        if matches!(segment, "" | "." | "..") {
            // the argument that fills most of this segment, empty ones included
            let within = spans.iter().filter(|(_, s, e)| *s <= end && *e >= start);
            if let Some((name, ..)) = within.max_by_key(|(_, s, e)| e - s) {
                return Err(format!(
                    "the argument {name} makes the path segment {segment:?}, which would move the call to another path"
                ));
            }
        }
        start = end + 1;
    }
    Ok(url)
}

/// A value as it goes into a URL: a string as it is, anything else as JSON.
fn text(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Whether the setting `key` lists `status`: None when the service has no
/// such setting. A single number is a list of one.
fn listed(config: &Config, key: &str, status: u16) -> Option<bool> {
    let is = |v: &Value| v.as_u64() == Some(u64::from(status));
    Some(match config.0.get(key)? {
        Value::Array(list) => list.iter().any(is),
        one => is(one),
    })
}

/// A body as JSON where it reads as JSON, else as text.
fn read(body: &[u8]) -> Value {
    serde_json::from_slice(body).unwrap_or_else(|_| Value::String(String::from_utf8_lossy(body).into_owned()))
}

/// The start of a refused answer's body, for the error: what of the URL could
/// carry a credential is masked wherever the answer echoes it (see
/// [`excerpt`]).
fn refusal(url: &str, body: &[u8]) -> String {
    match excerpt(url, body, 200, &[]).as_str() {
        "" => String::new(),
        text => format!(": {text}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn config(v: Value) -> Config {
        Config(v.as_object().cloned().unwrap_or_default())
    }

    #[test]
    fn a_service_holds_unless_it_says_dead_letter() {
        let held = |v: Value| match when_down(&config(v)) {
            WhenDown::Hold(p) => Some((p.health.map(|(url, ..)| url), p.max)),
            WhenDown::DeadLetter => None,
        };
        assert_eq!(held(json!({})), Some((None, Duration::from_secs(60))));
        assert_eq!(held(json!({"WHEN_DOWN": "dead-letter"})), None);
        assert_eq!(
            held(json!({"WHEN_DOWN": "hold", "BASE_URL": "http://h:1", "HEALTH_URL": "{BASE_URL}/health", "PROBE_MAX_MS": 5000})),
            Some((Some("http://h:1/health".to_string()), Duration::from_secs(5)))
        );
        // a HEALTH_URL that names no setting is no probe: the run is
        assert_eq!(held(json!({"HEALTH_URL": "{NOPE}/health"})), Some((None, Duration::from_secs(60))));
    }

    fn args(v: Value) -> Map<String, Value> {
        v.as_object().cloned().unwrap_or_default()
    }

    #[test]
    fn a_url_takes_its_arguments_encoded_and_its_settings_as_written() {
        let c = config(json!({"BASE_URL": "https://orders.example/v1", "URL": "{BASE_URL}/orders/{id}"}));
        let url = filled("{BASE_URL}/orders/{id}", &args(json!({"id": "A/1 b"})), &c).unwrap();
        assert_eq!(url, "https://orders.example/v1/orders/A%2F1%20b");
        assert_eq!(filled("/n/{n}", &args(json!({"n": 42})), &c).unwrap(), "/n/42");
        // `body` goes in the body, not in the URL.
        assert!(filled("/n/{n}", &args(json!({"n": 1, "body": {"a": 1}})), &c).is_ok());
    }

    #[test]
    fn a_url_refuses_what_it_cannot_fill() {
        let c = config(json!({}));
        let e = filled("/orders/{id}", &args(json!({"id": 1, "idd": 2})), &c).unwrap_err();
        assert_eq!(e, "the argument idd appears nowhere in its URL");
        let e = filled("/orders/{id}", &args(json!({})), &c).unwrap_err();
        assert!(e.contains("{id}") && e.contains("no argument"), "{e}");
        let e = filled("/orders/{id}", &args(json!({"id": null})), &c).unwrap_err();
        assert!(e.contains("id is null"), "{e}");
        assert!(filled("/orders/{id", &args(json!({"id": 1})), &c).unwrap_err().contains("not closed"));
    }

    #[test]
    fn an_argument_moves_the_call_nowhere_else() {
        let c = config(json!({"BASE_URL": "http://orders.example", "HOST": "orders.example"}));
        // a path the system resolves to another resource
        for bad in ["..", ".", ""] {
            let e = filled("{BASE_URL}/customers/{cid}/orders", &args(json!({"cid": bad})), &c).unwrap_err();
            assert!(e.contains("would move the call to another path"), "{bad:?}: {e}");
        }
        assert_eq!(filled("{BASE_URL}/c/{cid}", &args(json!({"cid": "..x"})), &c).unwrap(), "http://orders.example/c/..x");
        // the segment counts, not each value
        let two = |a: &str, b: &str| filled("{BASE_URL}/files/{a}{b}", &args(json!({"a": a, "b": b})), &c);
        assert_eq!(two("x", ".").unwrap(), "http://orders.example/files/x.");
        assert_eq!(two("", "x").unwrap(), "http://orders.example/files/x");
        for (a, b) in [(".", "."), ("", ".."), ("", "")] {
            assert!(two(a, b).unwrap_err().contains("makes the path segment"), "{a:?} {b:?}");
        }
        assert!(two("", "..").unwrap_err().starts_with("the argument b makes the path segment \"..\""));
        // the host and the empty segments of the template are not the flow's
        assert_eq!(filled("{BASE_URL}//c/{cid}", &args(json!({"cid": "1"})), &c).unwrap(), "http://orders.example//c/1");
        // in the query, an empty value or a dot is a value
        assert_eq!(filled("{BASE_URL}/search?q={q}", &args(json!({"q": ""})), &c).unwrap(), "http://orders.example/search?q=");
        // a setting is the service's own, whatever the flow passes
        let e = filled("http://{HOST}/orders/{id}", &args(json!({"id": 1, "HOST": "127.0.0.2"})), &c).unwrap_err();
        assert_eq!(e, "the argument HOST has the name of a setting of the service");
        // what a check reads, from the names alone
        assert!(fits("{BASE_URL}/orders/{id}", &["id", "body"], &["BASE_URL"]).is_ok());
        assert!(fits("{BASE_URL}/orders/{id}", &["idd"], &["BASE_URL"]).unwrap_err().contains("idd appears nowhere"));
        assert!(fits("{BASE_URL}/orders/{id}", &[], &["BASE_URL"]).unwrap_err().contains("{id}"));
    }

    #[test]
    fn what_a_service_takes_and_retries_is_its_own() {
        let c = config(json!({"ANSWERS": [200, 404], "RETRY_ON": 409}));
        assert_eq!(listed(&c, "ANSWERS", 404), Some(true));
        assert_eq!(listed(&c, "ANSWERS", 500), Some(false));
        assert_eq!(listed(&c, "RETRY_ON", 409), Some(true));
        assert_eq!(listed(&config(json!({})), "ANSWERS", 200), None);
        assert_eq!(read(b"{\"a\":1}"), json!({"a": 1}));
        assert_eq!(read(b"plain"), json!("plain"));
        assert_eq!(refusal("http://h/x", b"  "), "");
        assert_eq!(refusal("http://h/x", b"{\"error\":\"gone\"}"), ": {\"error\":\"gone\"}");
        let url = "http://h/x?key=LIT-K3Y-1234&page=1";
        assert_eq!(refusal(url, b"no key LIT-K3Y-1234 here"), ": no key <masked> here");
    }
}
