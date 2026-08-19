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

pub trait SecretStore: Send + Sync {
    fn kind(&self) -> &'static str;
    fn get(&self, reference: &str) -> Result<String, String>;
}

/// The store the runtime uses, selected at startup. Vault if configured, else
/// the dev env backend.
pub fn default_store() -> Arc<dyn SecretStore> {
    if std::env::var("VAULT_ADDR").map(|v| !v.is_empty()).unwrap_or(false) {
        Arc::new(VaultStore::from_env())
    } else {
        Arc::new(EnvStore)
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
        let out = std::process::Command::new("curl")
            .args(["-sS", "-m", "10", "-H", &format!("X-Vault-Token: {}", self.token), &url])
            .output()
            .map_err(|e| e.to_string())?;
        if !out.status.success() {
            return Err(format!("vault: {}", String::from_utf8_lossy(&out.stderr).trim()));
        }
        let v: Value = serde_json::from_slice(&out.stdout)
            .map_err(|e| format!("vault: bad response: {e}"))?;
        v["data"]["data"][key]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| format!("secret key {key:?} not found at {path:?}"))
    }
}
