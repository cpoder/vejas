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

use std::sync::Arc;

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
pub const SECRET_KEY_PATTERN: &str =
    "pass(wd|word)|secret|(^|[_-])token$|api[_-]?key|^auth$|[_-]auth$|^auth[_-]|authorization|webhook_url";

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

/// The store the runtime uses, selected at startup: Vault if configured, else
/// the writable file backend if configured, else the dev env backend.
pub fn default_store() -> Arc<dyn SecretStore> {
    if std::env::var("VAULT_ADDR").map(|v| !v.is_empty()).unwrap_or(false) {
        Arc::new(VaultStore::from_env())
    } else if let Ok(path) = std::env::var("VEJAS_SECRETS_FILE") {
        if !path.is_empty() {
            return Arc::new(FileStore::new(path.into()));
        }
        Arc::new(EnvStore)
    } else {
        Arc::new(EnvStore)
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
            return Err(format!("vault: HTTP {code} reading {path:?}"));
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
        // KV v2 writes replace the whole path: merge with the existing keys
        let mut data = match crate::connectors::http_request("GET", &url, &headers, None) {
            Ok((code, body)) if (200..300).contains(&code) => {
                serde_json::from_slice::<Value>(&body)
                    .ok()
                    .and_then(|v| v["data"]["data"].as_object().cloned())
                    .unwrap_or_default()
            }
            _ => Default::default(),
        };
        data.insert(key.to_string(), Value::String(value.to_string()));
        let body = serde_json::json!({ "data": data }).to_string();
        let mut headers = headers.to_vec();
        headers.push(("Content-Type".to_string(), "application/json".to_string()));
        match crate::connectors::http_request("POST", &url, &headers, Some(body.as_bytes())) {
            Ok((code, _)) if (200..300).contains(&code) => Ok(()),
            Ok((code, _)) => Err(format!("vault: HTTP {code} writing {path:?}")),
            Err(e) => Err(format!("vault: {e}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
