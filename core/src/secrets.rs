// Secret resolution (ADR-0008).
//
// Secrets are referenced, never inlined. `secret("path/key")` in VejasScript
// resolves at run time through a `SecretStore`; the value is used by the flow
// or connector but never written to a literal, the file, or the panel — which
// keeps "the whole script is editable and versionable" true without leaking
// credentials into git.
//
// Backends (chosen by env at startup):
//   - VaultStore  — HashiCorp Vault KV v2, when VAULT_ADDR is set.
//   - EnvStore    — dev default: secret("a/b") -> env VEJAS_SECRET_A_B.
//
// A reference is `path/key`: the last segment is the key. For Vault KV v2 the
// value is read from `{mount}/data/{path}` and the `{key}` field. For env it is
// the whole reference uppercased with separators turned into underscores.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use serde_json::Value;

/// The single definition of "credential-shaped key name" (case-insensitive
/// regex source). It has two enforcement points that MUST agree: the panel masks
/// any input whose key matches (so a secret is never shown in the clear), and the
/// connector admission lint (ADR-0017) fails a recipe that puts a matching key's
/// value in a literal instead of a `secret()`. To keep them one definition, the
/// panel HTML receives this pattern by serve-time substitution rather than
/// hard-coding its own copy, and `vejas-runtime secret-pattern` prints it for the
/// lint to read — no second source to drift.
///
/// This is profile **P4-dash**, chosen by Cyril on a labelled bench of 55 real
/// keys (repo + agent-generated + the Reglyze deployment + env conventions),
/// documented in ADR-0017. Measured subtleties, not guesses: `token` matches as a
/// SUFFIX only — real tokens end in it (GITHUB_TOKEN, ACCESS_TOKEN), configs
/// prefix (TOKEN_URL) or pluralise (MAX_TOKENS) it — and `_key$` is deliberately
/// absent (it would mask JIRA_PROJECT_KEY, FRAMEWORK_KEY). The `[_-]` classes let
/// it border on hyphens too, so HTTP header names match (X-Auth-Token, X-Api-Key,
/// X-Token) while Content-Type / Accept / X-Request-Id pass. It matches KEY names,
/// not values; the irreducible floor (PASSPHRASE, CREDENTIALS, BEARER,
/// DATABASE_URL, SERVICE_ACCOUNT_JSON) is covered by the generation contract, not
/// this regex. Keep it free of `"`/`\` so it injects verbatim into a JS
/// double-quoted string.
///
/// A key is matched as [`key_as_read`] reads it, camelCase as snake_case, so
/// that `accessToken` is masked as `ACCESS_TOKEN` is (decided on 2026-11-26 on
/// 43 labelled camelCase names: 18 credentials missed before, 3 after, against 9
/// false masks, the pagination tokens that `page_token` already gave; ADR-0017).
pub const SECRET_KEY_PATTERN: &str =
    "pass(wd|word)|secret|(^|[_-])token$|api[_-]?key|^auth$|[_-]auth$|^auth[_-]|authorization|webhook_url";

/// A key as [`SECRET_KEY_PATTERN`] reads it: an underscore where a lowercase
/// letter or a digit meets an uppercase one, so camelCase reads as snake_case
/// (`accessToken` as `access_Token`, `apiKey` as `api_Key`) and a name means the
/// same in either convention. Upper-case, snake_case and kebab-case names read
/// as they are. Every reader of the pattern applies this first, with the same
/// rule: the record here, the panel (`isSecretKey`), the admission lint and its
/// bench (`e2e/admission/`), each pinned by its own test.
pub fn key_as_read(key: &str) -> std::borrow::Cow<'_, str> {
    let bytes = key.as_bytes();
    let boundary = |i: usize| {
        i > 0
            && bytes[i].is_ascii_uppercase()
            && (bytes[i - 1].is_ascii_lowercase() || bytes[i - 1].is_ascii_digit())
    };
    if !(1..bytes.len()).any(boundary) {
        return std::borrow::Cow::Borrowed(key);
    }
    let mut read = String::with_capacity(key.len() + 4);
    for (i, c) in key.char_indices() {
        if boundary(i) {
            read.push('_');
        }
        read.push(c);
    }
    std::borrow::Cow::Owned(read)
}

pub trait SecretStore: Send + Sync {
    fn kind(&self) -> &'static str;
    fn get(&self, reference: &str) -> Result<String, String>;
    /// Write a secret (rotation included). Write-only: a value that enters
    /// here is never rendered back by any surface. Backends that cannot
    /// write stay read-only and say so.
    fn set(&self, _reference: &str, _value: &str) -> Result<(), String> {
        Err(format!("the {} secret backend is read-only", self.kind()))
    }
}

/// What every path that SHOWS a run instead of performing it resolves through:
/// preview, run on a fixture, flow-as-tool, shadow-replay, time-travel, canary,
/// golden capture. `secret("a/b")` stays fail-closed (a missing secret aborts as
/// in a real run) and evaluates to the placeholder `<secret:a/b>`, never to the
/// value. Those results go to an agent, into a model's context, or to an open
/// GET, and a time-travel candidate is arbitrary code that could emit the value
/// or transform it, so masking the output afterwards cannot work: the value must
/// never enter the run. The price is that a branch comparing with a secret takes
/// the not-equal path in shadow.
pub struct RedactedStore(Arc<dyn SecretStore>);

impl RedactedStore {
    pub fn new(inner: Arc<dyn SecretStore>) -> Self {
        RedactedStore(inner)
    }
}

impl SecretStore for RedactedStore {
    fn kind(&self) -> &'static str {
        "redacted"
    }
    fn get(&self, reference: &str) -> Result<String, String> {
        self.0.get(reference)?;
        Ok(format!("<secret:{reference}>"))
    }
}

/// The store the runtime uses, selected at startup: Vault if configured, else
/// the writable file backend if configured, else the dev env backend. Every
/// value it hands out or takes in is remembered, so the trace record can take
/// it back out of what it writes (ADR-0034).
pub fn default_store() -> Arc<dyn SecretStore> {
    let store: Arc<dyn SecretStore> =
        if std::env::var("VAULT_ADDR").map(|v| !v.is_empty()).unwrap_or(false) {
            Arc::new(VaultStore::from_env())
        } else if let Ok(path) = std::env::var("VEJAS_SECRETS_FILE") {
            if !path.is_empty() {
                Arc::new(FileStore::new(path.into()))
            } else {
                Arc::new(EnvStore)
            }
        } else {
            Arc::new(EnvStore)
        };
    Arc::new(Remembered(store))
}

/// The values the stores of this process have handed out, each with the last
/// time it was, on a counter. A rotation adds the new value and keeps the old
/// one. Past the cap, the value handed out least recently goes first, so a
/// credential that is read on every use stays however many others come and go.
static HANDED_OUT: OnceLock<Mutex<Handed>> = OnceLock::new();
/// Bumped each time a value joins: a reader learns of a change without a lock.
static HANDED_OUT_GENERATION: AtomicU64 = AtomicU64::new(0);
const HANDED_OUT_MAX: usize = 4096;
/// Shorter than this is no credential, and taking it out of every string a
/// record holds would leave nothing to read.
const HANDED_OUT_MIN: usize = 4;

#[derive(Default)]
struct Handed {
    last_use: HashMap<String, u64>,
    clock: u64,
}

impl Handed {
    /// Note a value handed out; true when it is new.
    fn note(&mut self, value: &str, cap: usize) -> bool {
        self.clock += 1;
        let now = self.clock;
        if let Some(last) = self.last_use.get_mut(value) {
            *last = now;
            return false;
        }
        if self.last_use.len() >= cap {
            let oldest = self
                .last_use
                .iter()
                .min_by_key(|(_, last)| **last)
                .map(|(v, _)| v.clone());
            if let Some(oldest) = oldest {
                self.last_use.remove(&oldest);
            }
        }
        self.last_use.insert(value.to_string(), now);
        true
    }
}

fn remember(value: &str) {
    if value.len() < HANDED_OUT_MIN {
        return;
    }
    let new = HANDED_OUT
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .note(value, HANDED_OUT_MAX);
    if new {
        HANDED_OUT_GENERATION.fetch_add(1, Ordering::Relaxed);
    }
}

/// Every value handed out and still remembered, longest first.
pub fn handed_out() -> Vec<String> {
    let Some(handed) = HANDED_OUT.get() else {
        return Vec::new();
    };
    let mut values: Vec<String> = handed
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .last_use
        .keys()
        .cloned()
        .collect();
    values.sort_by(|a, b| b.len().cmp(&a.len()).then_with(|| a.cmp(b)));
    values
}

/// A value as if a store had handed it out, for the tests of what reads them.
#[cfg(test)]
pub(crate) fn hand_out(value: &str) {
    remember(value);
}

/// A credential the runtime holds that no store handed out, a token a
/// connector fetched: masked wherever the values handed out are.
pub(crate) fn hold(value: &str) {
    remember(value);
}

/// Changes when a value joins `handed_out()`: the trace record rebuilds its
/// index of them only then.
pub fn handed_out_generation() -> u64 {
    HANDED_OUT_GENERATION.load(Ordering::Relaxed)
}

/// A store whose values are remembered on their way through.
struct Remembered(Arc<dyn SecretStore>);

impl SecretStore for Remembered {
    fn kind(&self) -> &'static str {
        self.0.kind()
    }
    fn get(&self, reference: &str) -> Result<String, String> {
        let value = self.0.get(reference)?;
        remember(&value);
        Ok(value)
    }
    fn set(&self, reference: &str, value: &str) -> Result<(), String> {
        self.0.set(reference, value)?;
        remember(value);
        Ok(())
    }
}

/// Writable secrets file (0600, JSON object {"path/key": "value"}), read on
/// every get so a rotation needs no restart of the runtime itself. The
/// on-prem collector's backend: the admin pastes values in the panel, they
/// land here, they never leave the machine.
pub struct FileStore {
    path: std::path::PathBuf,
}

impl FileStore {
    pub fn new(path: std::path::PathBuf) -> Self {
        FileStore { path }
    }
    fn load(&self) -> Result<serde_json::Map<String, Value>, String> {
        match std::fs::read_to_string(&self.path) {
            Ok(s) if s.trim().is_empty() => Ok(Default::default()),
            Ok(s) => serde_json::from_str::<Value>(&s)
                .ok()
                .and_then(|v| v.as_object().cloned())
                .ok_or_else(|| {
                    format!("secrets file {} is not a JSON object", self.path.display())
                }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Default::default()),
            Err(e) => Err(e.to_string()),
        }
    }
}

impl SecretStore for FileStore {
    fn kind(&self) -> &'static str {
        "file"
    }
    fn get(&self, reference: &str) -> Result<String, String> {
        self.load()?
            .get(reference)
            .and_then(|v| v.as_str().map(|s| s.to_string()))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| format!("secret {reference:?} not set (panel Secrets card, or {})", self.path.display()))
    }
    fn set(&self, reference: &str, value: &str) -> Result<(), String> {
        use std::io::Write as _;
        use std::os::unix::fs::OpenOptionsExt as _;
        let mut map = self.load()?;
        map.insert(reference.to_string(), Value::String(value.to_string()));
        let rendered =
            serde_json::to_string_pretty(&Value::Object(map)).map_err(|e| e.to_string())?;
        // atomic replace so a concurrent get never sees a torn file
        let tmp = self.path.with_extension("tmp");
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&tmp)
            .and_then(|mut f| f.write_all(rendered.as_bytes()))
            .map_err(|e| e.to_string())?;
        match std::fs::rename(&tmp, &self.path) {
            Ok(()) => Ok(()),
            // a single-file docker bind mount cannot be renamed over (EBUSY):
            // fall back to writing in place — mount a DIRECTORY to keep the
            // atomic path (the collector bundle does)
            Err(e) if e.raw_os_error() == Some(16) => {
                let _ = std::fs::remove_file(&tmp);
                std::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .mode(0o600)
                    .open(&self.path)
                    .and_then(|mut f| f.write_all(rendered.as_bytes()))
                    .map_err(|e| e.to_string())
            }
            Err(e) => {
                let _ = std::fs::remove_file(&tmp);
                Err(e.to_string())
            }
        }
    }
}

fn env_key(reference: &str) -> String {
    let norm: String = reference
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c.to_ascii_uppercase() } else { '_' })
        .collect();
    format!("VEJAS_SECRET_{norm}")
}

pub struct EnvStore;
impl SecretStore for EnvStore {
    fn kind(&self) -> &'static str {
        "env"
    }
    fn get(&self, reference: &str) -> Result<String, String> {
        let key = env_key(reference);
        std::env::var(&key)
            .map_err(|_| format!("secret {reference:?} not found (set env {key})"))
    }
}

pub struct VaultStore {
    addr: String,
    token: String,
    mount: String,
}

impl VaultStore {
    pub fn from_env() -> Self {
        VaultStore {
            addr: std::env::var("VAULT_ADDR")
                .unwrap_or_default()
                .trim_end_matches('/')
                .to_string(),
            token: std::env::var("VAULT_TOKEN").unwrap_or_default(),
            mount: std::env::var("VEJAS_VAULT_MOUNT").unwrap_or_else(|_| "secret".into()),
        }
    }
}

impl SecretStore for VaultStore {
    fn kind(&self) -> &'static str {
        "vault"
    }
    fn get(&self, reference: &str) -> Result<String, String> {
        let (path, key) = reference
            .rsplit_once('/')
            .ok_or_else(|| format!("secret ref {reference:?} must be path/key"))?;
        let url = format!("{}/v1/{}/data/{}", self.addr, self.mount, path);
        // argv-safe: the token rides in an in-memory ureq header, never on argv
        let headers = [("X-Vault-Token".to_string(), self.token.clone())];
        let (code, body) = crate::connectors::http_request("GET", &url, &headers, None)
            .map_err(|e| format!("vault: {e}"))?;
        if !(200..300).contains(&code) {
            return Err(vault_refused(code, "reading", path));
        }
        let v: Value =
            serde_json::from_slice(&body).map_err(|e| format!("vault: bad response: {e}"))?;
        v["data"]["data"][key]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| format!("secret key {key:?} not found at {path:?}"))
    }
    fn set(&self, reference: &str, value: &str) -> Result<(), String> {
        let (path, key) = reference
            .rsplit_once('/')
            .ok_or_else(|| format!("secret ref {reference:?} must be path/key"))?;
        let url = format!("{}/v1/{}/data/{}", self.addr, self.mount, path);
        let headers = [("X-Vault-Token".to_string(), self.token.clone())];
        // KV v2 writes replace the whole path: merge with the existing keys,
        // and write over the version read only (check-and-set), so that a
        // write in between is not dropped. A read that fails stops the write,
        // which would drop them.
        let (mut data, version) = match crate::connectors::http_request("GET", &url, &headers, None) {
            Ok((code, body)) if (200..300).contains(&code) => {
                let read = serde_json::from_slice::<Value>(&body).ok();
                let data = read.as_ref().and_then(|v| v["data"]["data"].as_object().cloned());
                match (data, read.as_ref().and_then(|v| v["data"]["metadata"]["version"].as_u64())) {
                    (Some(data), Some(version)) => (data, version),
                    _ => return Err(format!("vault: bad response reading {path:?}, so nothing was written")),
                }
            }
            // nothing at the path yet, or its last version deleted, as Vault
            // says it: another server's 404, a proxy's, says nothing of the path
            Ok((404, body)) => match serde_json::from_slice::<Value>(&body) {
                Ok(v) if v.get("errors").is_some_and(Value::is_array) || v["data"]["metadata"].is_object() => {
                    (Default::default(), v["data"]["metadata"]["version"].as_u64().unwrap_or(0))
                }
                _ => {
                    return Err(format!(
                        "vault: HTTP 404 reading {path:?} that is no answer of Vault's, so nothing was written"
                    ))
                }
            },
            Ok((code, _)) => return Err(format!("{}, so nothing was written", vault_refused(code, "reading", path))),
            Err(e) => return Err(format!("vault: {e}, so nothing was written")),
        };
        data.insert(key.to_string(), Value::String(value.to_string()));
        let body = serde_json::json!({ "data": data, "options": { "cas": version } }).to_string();
        let mut headers = headers.to_vec();
        headers.push(("Content-Type".to_string(), "application/json".to_string()));
        match crate::connectors::http_request("POST", &url, &headers, Some(body.as_bytes())) {
            Ok((code, _)) if (200..300).contains(&code) => Ok(()),
            Ok((400, answer)) if String::from_utf8_lossy(&answer).contains("check-and-set") => Err(format!(
                "vault: {path:?} changed between its read and the write, so nothing was written: write it again"
            )),
            Ok((code, _)) => Err(vault_refused(code, "writing", path)),
            Err(e) => Err(format!("vault: {e}")),
        }
    }
}

/// What a Vault answer outside 2xx says. A standby node answers with a
/// redirect to the active one, which is not followed: the token would go along.
fn vault_refused(code: u16, doing: &str, path: &str) -> String {
    match code {
        301 | 302 | 303 | 307 | 308 => format!(
            "vault: HTTP {code} {doing} {path:?}: a redirect, not followed, since the token would go along: set VAULT_ADDR to the active node"
        ),
        _ => format!("vault: HTTP {code} {doing} {path:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A Vault that answers each request in turn with the next of `answers`,
    /// and hands back the requests it got, each whole.
    fn fake_vault(answers: Vec<String>) -> (VaultStore, std::thread::JoinHandle<Vec<String>>) {
        use std::io::{Read as _, Write as _};
        let server = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = server.local_addr().unwrap().port();
        let serving = std::thread::spawn(move || {
            let mut got = Vec::new();
            for answer in answers {
                let (mut conn, _) = server.accept().unwrap();
                let mut request = Vec::new();
                let mut chunk = [0u8; 4096];
                // the head, then as much body as it announces
                loop {
                    let n = conn.read(&mut chunk).unwrap_or(0);
                    request.extend_from_slice(&chunk[..n]);
                    let text = String::from_utf8_lossy(&request);
                    let Some((head, body)) = text.split_once("\r\n\r\n") else {
                        if n == 0 { break } else { continue }
                    };
                    let length = head
                        .lines()
                        .find_map(|l| l.to_ascii_lowercase().strip_prefix("content-length:").map(|v| v.trim().parse().unwrap_or(0)))
                        .unwrap_or(0);
                    if body.len() >= length || n == 0 {
                        break;
                    }
                }
                got.push(String::from_utf8_lossy(&request).into_owned());
                conn.write_all(answer.as_bytes()).unwrap();
            }
            got
        });
        let store = VaultStore { addr: format!("http://127.0.0.1:{port}"), token: "t0k3n-vault".into(), mount: "secret".into() };
        (store, serving)
    }

    fn answer(status: &str, body: &str) -> String {
        format!("HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len())
    }

    #[test]
    fn a_standby_vault_is_named_and_not_followed() {
        let standby = "HTTP/1.1 307 Temporary Redirect\r\nLocation: http://127.0.0.1:1/v1/secret/data/app\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        let (store, serving) = fake_vault(vec![standby.to_string()]);
        let e = store.get("app/key").unwrap_err();
        assert!(e.starts_with("vault: HTTP 307 reading \"app\": a redirect, not followed") && e.contains("VAULT_ADDR"), "{e}");
        assert_eq!(serving.join().unwrap().len(), 1);
    }

    #[test]
    fn a_vault_write_never_drops_the_keys_it_could_not_read() {
        // the read fails: nothing is written
        let (store, serving) = fake_vault(vec![answer("500 Internal Server Error", r#"{"errors":["local node not active"]}"#)]);
        let e = store.set("app/new", "v").unwrap_err();
        assert_eq!(e, "vault: HTTP 500 reading \"app\", so nothing was written");
        assert_eq!(serving.join().unwrap().len(), 1, "a write followed the failed read");
        // nothing at the path yet: the write goes on, for a path still empty
        let (store, serving) = fake_vault(vec![answer("404 Not Found", r#"{"errors":[]}"#), answer("200 OK", "{}")]);
        store.set("app/new", "v").unwrap();
        let got = serving.join().unwrap();
        let lines: Vec<&str> = got.iter().map(|r| r.lines().next().unwrap_or_default()).collect();
        assert_eq!(lines, ["GET /v1/secret/data/app HTTP/1.1", "POST /v1/secret/data/app HTTP/1.1"]);
        assert!(got[1].ends_with(r#"{"data":{"new":"v"},"options":{"cas":0}}"#), "{}", got[1]);
        // its last version deleted: written over that version
        let deleted = r#"{"data":{"data":null,"metadata":{"deletion_time":"2026-11-27T00:00:00Z","version":4}}}"#;
        let (store, serving) = fake_vault(vec![answer("404 Not Found", deleted), answer("200 OK", "{}")]);
        store.set("app/new", "v").unwrap();
        assert!(serving.join().unwrap()[1].ends_with(r#""options":{"cas":4}}"#));
        // a 404 that is not Vault's, a proxy with no server behind it: nothing is written
        let (store, serving) = fake_vault(vec![answer("404 Not Found", "404 page not found")]);
        let e = store.set("app/new", "v").unwrap_err();
        assert!(e.contains("no answer of Vault's, so nothing was written"), "{e}");
        assert_eq!(serving.join().unwrap().len(), 1, "a write followed a proxy's 404");
        // the keys read are written back with the new one, over the version read
        let read = answer("200 OK", r#"{"data":{"data":{"old":"kept"},"metadata":{"version":7}}}"#);
        let (store, serving) = fake_vault(vec![read, answer("200 OK", "{}")]);
        store.set("app/new", "v").unwrap();
        let got = serving.join().unwrap();
        assert!(got[1].ends_with(r#"{"data":{"new":"v","old":"kept"},"options":{"cas":7}}"#), "{}", got[1]);
        // a write in between: Vault refuses ours, which says so
        let read = answer("200 OK", r#"{"data":{"data":{"old":"kept"},"metadata":{"version":7}}}"#);
        let refused = answer("400 Bad Request", r#"{"errors":["check-and-set parameter did not match the current version"]}"#);
        let (store, serving) = fake_vault(vec![read, refused]);
        let e = store.set("app/new", "v").unwrap_err();
        assert!(e.contains("changed between its read and the write"), "{e}");
        serving.join().unwrap();
    }

    #[test]
    fn camel_case_reads_as_snake_case() {
        for (key, read) in [
            ("accessToken", "access_Token"),
            ("XAuthToken", "XAuth_Token"),
            ("aB1C", "a_B1_C"),
            ("oauth2Token", "oauth2_Token"),
            ("ACCESS_TOKEN", "ACCESS_TOKEN"),
            ("x-api-key", "x-api-key"),
            ("ID", "ID"),
            ("", ""),
            ("éToken", "éToken"),
        ] {
            assert_eq!(key_as_read(key), read, "{key}");
        }
        let pattern = regex::RegexBuilder::new(SECRET_KEY_PATTERN).case_insensitive(true).build().unwrap();
        let masked = |key: &str| pattern.is_match(&key_as_read(key));
        for key in ["accessToken", "refreshToken", "idToken", "apiKey", "clientSecret", "xAuthToken"] {
            assert!(masked(key), "{key}");
        }
        for key in ["maxTokens", "tokenUrl", "author", "oauthScope", "keyboardLayout", "publicKey"] {
            assert!(!masked(key), "{key}");
        }
    }

    struct Fixed(&'static str);
    impl SecretStore for Fixed {
        fn kind(&self) -> &'static str {
            "fixed"
        }
        fn get(&self, _reference: &str) -> Result<String, String> {
            Ok(self.0.to_string())
        }
        fn set(&self, _reference: &str, _value: &str) -> Result<(), String> {
            Ok(())
        }
    }

    #[test]
    fn what_a_store_hands_out_or_takes_in_is_remembered_longest_first() {
        let store = Remembered(Arc::new(Fixed("remembered-value-one")));
        assert_eq!(store.get("a/b").unwrap(), "remembered-value-one");
        store.set("a/b", "remembered-rotation-two-longer").unwrap();
        // too short to be a credential: not taken out of anything
        Remembered(Arc::new(Fixed("abc"))).get("c/d").unwrap();
        let values = handed_out();
        let one = values.iter().position(|v| v == "remembered-value-one").unwrap();
        let two = values.iter().position(|v| v == "remembered-rotation-two-longer").unwrap();
        assert!(two < one, "longest first: {values:?}");
        assert!(!values.iter().any(|v| v == "abc"));
        assert_eq!(store.kind(), "fixed");
    }

    #[test]
    fn past_the_cap_the_value_handed_out_least_recently_goes() {
        let mut handed = Handed::default();
        assert!(handed.note("read-at-start", 3));
        assert!(handed.note("token-1", 3));
        assert!(handed.note("token-2", 3));
        // the credential read again is fresh; the first token is now the oldest
        assert!(!handed.note("read-at-start", 3));
        assert!(handed.note("token-3", 3));
        let mut kept: Vec<&str> = handed.last_use.keys().map(String::as_str).collect();
        kept.sort();
        assert_eq!(kept, ["read-at-start", "token-2", "token-3"]);
    }

    #[test]
    fn file_store_set_get_rotate_merge_and_mode() {
        let path = std::env::temp_dir().join(format!("vejas-secrets-{}.json", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let s = FileStore::new(path.clone());
        assert!(s.get("a/b").is_err(), "missing secret fails closed");
        s.set("a/b", "v1").unwrap();
        assert_eq!(s.get("a/b").unwrap(), "v1");
        s.set("a/b", "v2").unwrap(); // rotation
        assert_eq!(s.get("a/b").unwrap(), "v2");
        s.set("c/d", "x").unwrap(); // merge keeps other keys
        assert_eq!(s.get("a/b").unwrap(), "v2");
        assert_eq!(s.get("c/d").unwrap(), "x");
        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "secrets file must be private");
        // env backend refuses writes
        assert!(EnvStore.set("a/b", "x").is_err());
        let _ = std::fs::remove_file(&path);
    }
}
