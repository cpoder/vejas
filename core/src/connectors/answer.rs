//! Services that answer (ADR-0032). A file in `services/` may declare a
//! connector instead of holding code: `driver "http-rpc"` and its settings. A
//! flow invokes it as it invokes VejasScript and gets what the system
//! answered, or its run fails with an error that names the service and the
//! cause, and says whether a later delivery of the message may try again.

use std::time::Duration;

use serde_json::{Map, Value};

use super::http_client::{http_call, urlencode, NoAnswer};
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
    /// A later delivery may get an answer: by default after a 429, a 5xx, a
    /// timeout or a network error. The message is then delivered again after
    /// `delay`, while it has had fewer deliveries than `attempts`; otherwise it
    /// is a dead letter at once.
    pub retry: bool,
    /// No answer came within the call's `TIMEOUT_MS`: an `api` flow answers 504.
    pub timed_out: bool,
    pub attempts: i64,
    pub delay: Duration,
}

impl Failure {
    /// A declaration whose settings could not be resolved: a secret store that
    /// did not answer, say. A later delivery may resolve them. Its own
    /// attempts and delay are among what is not known, so the defaults apply.
    pub fn unresolved(error: String) -> Self {
        Failure { error: shown(error), retry: true, timed_out: false, attempts: 3, delay: Duration::from_millis(1000) }
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
}

impl Why {
    /// A cause no later try can change.
    fn fixed(error: String) -> Self {
        Why { error, retry: false, timed_out: false }
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
        retry: why.retry,
        timed_out: why.timed_out,
        attempts: config.0.get("ATTEMPTS").and_then(Value::as_i64).unwrap_or(3).max(1),
        delay: Duration::from_millis(config.u64_or("DELAY_MS", 1000)).min(DELAY_MAX),
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
            NoAnswer::TimedOut => Why { error: format!("timeout after {timeout} ms"), retry: true, timed_out: true },
            NoAnswer::Network(e) => Why { error: format!("network: {e}"), retry: true, timed_out: false },
            NoAnswer::Broken(e) => Why::fixed(e),
        })?;
    let status = answer.status;
    if !listed(config, "ANSWERS", status).unwrap_or((200..300).contains(&status)) {
        return Err(Why {
            error: format!("HTTP {status}{}", excerpt(&answer.body)),
            retry: listed(config, "RETRY_ON", status).unwrap_or(status == 429 || status >= 500),
            timed_out: false,
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
/// the declaration's settings, as written. An argument may not be null, and
/// one in the path may not be empty, `.` or `..`, which would move the call to
/// another resource of the system.
fn filled(template: &str, args: &Map<String, Value>, config: &Config) -> Result<String, String> {
    let given: Vec<&str> = args.keys().map(String::as_str).collect();
    let settings: Vec<&str> = config.0.keys().map(String::as_str).collect();
    fits(template, &given, &settings)?;
    let mut url = String::with_capacity(template.len() + 16);
    for piece in pieces(template)? {
        match piece {
            Piece::Text(text) => url.push_str(text),
            Piece::Name(name) => match args.get(name) {
                Some(Value::Null) => return Err(format!("the argument {name} is null")),
                Some(value) => {
                    let value = text(value);
                    if !url.contains(['?', '#']) && matches!(value.as_str(), "" | "." | "..") {
                        return Err(format!("the argument {name} is {value:?}, which would move the call to another path"));
                    }
                    url.push_str(&urlencode(&value));
                }
                None => url.push_str(&config.0.get(name).map(text).unwrap_or_default()),
            },
        }
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

/// The start of a refused answer's body, for the error.
fn excerpt(body: &[u8]) -> String {
    let text = String::from_utf8_lossy(&body[..body.len().min(200)]);
    match text.trim() {
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
        assert_eq!(excerpt(b"  "), "");
        assert_eq!(excerpt(b"{\"error\":\"gone\"}"), ": {\"error\":\"gone\"}");
    }
}
