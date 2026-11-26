// Connector SDK (ADR-0007).
//
// A connector is a native Rust **driver** + a declarative **instance manifest**
// (`connectors/<name>.vjs`: `driver "http-in"` plus UPPERCASE literal config).
// Drivers are compiled in; instances are data — hot-addable, editable in the
// panel like any other business surface. Two families:
//
//   Source  — pushes onto the bus. Trigger kinds: webhook, poll, interval,
//             (queue/stream = future drivers). Publishes vx.<...>.
//   Sink    — consumes from the bus (durable pull consumer) and does a side
//             effect. Ack after success, else nak (redelivery = retry).
//
// The subject convention (SUBJECTS.md) remains the whole interface, so an
// external connector in any language is still a first-class citizen over the
// bus. These native drivers are the batteries included.
//
// This file is the contract a driver is written against: its Config, its Ctx
// and the Driver trait. The services drivers share sit beside it in
// connectors/ (what a hop records, the sink loop, pull rounds, the DLQ,
// offsets, the HTTP client), and the bundled drivers in connectors/drivers/,
// one module per protocol.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde_json::{Map, Value};

use crate::bus;

mod audit;
mod clock;
mod dlq;
mod drivers;
mod hops;
mod http_client;
mod offsets;
mod process;
mod pull;
mod sink;
mod slot;
mod snapshots;

// What the rest of the runtime uses.
pub use audit::{audit_recent, to_audit};
pub use clock::iso8601_utc;
pub use dlq::{dlq_unit_token, ensure_dlq_stream, to_dlq, DLQ_STREAM};
pub use drivers::{catalog, driver_for};
pub use hops::trace_preview;
pub use http_client::http_request;
pub use process::without_runtime_credentials;
pub use pull::{ack_wait, fetch_round, hydrate_recent, subject_matches};
pub use slot::Slot;
pub use snapshots::{open_state_store, snapshot_get, snapshot_put, Snapshot};

/// A connector instance's config: the UPPERCASE literals of its manifest.
#[derive(Clone, Default)]
pub struct Config(pub Map<String, Value>);

impl Config {
    pub fn str(&self, key: &str) -> Option<String> {
        self.0.get(key).and_then(|v| v.as_str().map(|s| s.to_string()))
    }
    pub fn str_or(&self, key: &str, default: &str) -> String {
        self.str(key).unwrap_or_else(|| default.to_string())
    }
    pub fn u64_or(&self, key: &str, default: u64) -> u64 {
        self.0.get(key).and_then(|v| v.as_u64()).unwrap_or(default)
    }
    pub fn value(&self, key: &str) -> Option<Value> {
        self.0.get(key).cloned()
    }
    /// Optional HEADERS doc: `HEADERS = {"Authorization": f"Bearer {secret("…")}"}`.
    /// Manifests are evaluated, so secret()/f-strings resolve before this runs —
    /// and an expression is not a literal, so it never enters the business surface.
    pub fn headers(&self) -> Vec<(String, String)> {
        self.0
            .get("HEADERS")
            .and_then(|v| v.as_object())
            .map(|o| {
                o.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default()
    }
    /// Optional ENV doc: `ENV = {"SAP_PASSWD": secret("…"), "SAP_USER": "DEV"}`.
    /// Evaluated at manifest time, so secret()/f-strings resolve here; passed to
    /// an exec child's environment — never argv (`/proc/<pid>/environ` is
    /// owner-only, `cmdline` is world-readable). Non-string values are stringified.
    pub fn env_vars(&self) -> Vec<(String, String)> {
        self.0
            .get("ENV")
            .and_then(|v| v.as_object())
            .map(|o| {
                o.iter()
                    .map(|(k, v)| match v {
                        Value::String(s) => (k.clone(), s.clone()),
                        other => (k.clone(), other.to_string()),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

pub struct Ctx {
    pub name: String,
    pub nats_url: String,
    pub stream: String,
    pub subj_root: String,
    pub config: Config,
    pub running: Arc<AtomicBool>,
}

impl Ctx {
    fn alive(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
    fn jetstream(&self) -> Result<bus::JetStream, String> {
        let js = bus::connect(&self.nats_url)?.jetstream();
        let _ = js.add_stream(&bus::StreamConfig {
            name: self.stream.clone(),
            subjects: vec![format!("{}.>", self.subj_root)],
            ..Default::default()
        });
        Ok(js)
    }
    /// Full subject from a config value that may be a bare suffix or a vx.* path.
    fn subject(&self, raw: &str) -> String {
        if raw.starts_with(&format!("{}.", self.subj_root)) {
            raw.to_string()
        } else {
            format!("{}.{}", self.subj_root, raw)
        }
    }
}

pub trait Driver: Send + Sync {
    /// e.g. "source:webhook", "source:poll", "source:interval", "sink".
    fn kind(&self) -> &'static str;
    /// One-line description for the panel / MCP.
    fn about(&self) -> &'static str;
    /// Blocking; loops until `ctx.running` clears. Returning Err triggers a
    /// supervised restart.
    fn run(&self, ctx: &Ctx) -> Result<(), String>;
    /// One synchronous end-to-end check of THIS instance's config — reach the
    /// remote side with the real credentials, touch nothing (no publish, no
    /// data written). The admin's "does my secret and my network work?"
    /// button. Default: the driver has no probe.
    fn probe(&self, _ctx: &Ctx) -> Result<String, String> {
        Err("this driver has no test probe".into())
    }
}

/// Plain-words prefix for the errors an admin actually meets.
pub fn humanize(e: &str) -> String {
    if e.contains("HTTP 401") || e.contains("HTTP 403") {
        format!("authentication refused (invalid or expired secret?) — {e}")
    } else if e.contains("not set") || e.contains("not found (set env") || (e.contains("secret") && e.contains("not found")) {
        format!("missing secret — {e}")
    } else if e.starts_with("http:") || e.contains("Connection refused") || e.contains("Could not resolve") || e.contains("timed out") {
        format!("unreachable (network, URL, proxy?) — {e}")
    } else {
        e.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn config_headers_doc() {
        let mut m = Map::new();
        m.insert(
            "HEADERS".into(),
            json!({"Authorization": "Bearer x", "X-N": 42}),
        );
        let cfg = Config(m);
        // non-string values are ignored, string ones pass through
        assert_eq!(cfg.headers(), vec![("Authorization".to_string(), "Bearer x".to_string())]);
        assert!(Config::default().headers().is_empty());
    }
}
