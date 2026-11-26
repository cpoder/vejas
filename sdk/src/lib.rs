//! The contract a Vejas connector is written against (ADR-0038).
//!
//! A driver implements [`Driver`]. It is handed a [`Ctx`]: its name, its
//! [`Config`], the bus it runs on, and the [`Host`] that records each hop it
//! makes. The same driver code is packaged two ways: compiled into the
//! runtime, whose host writes a hop to its events ring, its trace record and
//! its metrics; or as a standalone binary the runtime starts, whose host hands
//! each hop back to the runtime to be recorded the same way.
//!
//! What a driver does on the bus goes through the services here, so that
//! every connector behaves alike: [`Ctx::publish`] opens the trace of what a
//! source lets in, or continues the caller's; [`Ctx::consume`] is the sink
//! loop (ack, retries, the dead-letter queue, one hop per delivery);
//! [`offsets`] keeps where a source stopped; [`dlq`] parks what cannot be
//! delivered.

pub mod bus;
pub mod clock;
pub mod dlq;
pub mod offsets;
pub mod pull;
pub mod span;
pub mod trace;

mod hop;
mod sink;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use serde_json::{Map, Value};

pub use hop::{answer_text, publish, summarize, trace_preview, HopEvent, Host};

/// Set while the process runs; cleared when it shuts down. The background
/// threads of this crate (the dead-letter announcer) stop on it.
pub static RUNNING: AtomicBool = AtomicBool::new(true);

/// After this many failed deliveries of the same message, a unit stops
/// retrying and parks it in the dead-letter queue (ADR-0015) — redelivery is
/// the retry mechanism, not an infinite loop for poison messages.
pub const MAX_DELIVERIES: i64 = 5;

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

/// What a running connector is handed: who it is, its config, the bus it runs
/// on, and the host that records its hops.
pub struct Ctx {
    pub name: String,
    pub nats_url: String,
    pub stream: String,
    pub subj_root: String,
    pub config: Config,
    pub running: Arc<AtomicBool>,
    pub host: Arc<dyn Host>,
}

impl Ctx {
    /// Should the driver keep running? It returns once this is false.
    pub fn alive(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
    /// The bus, with the stream that holds this instance's subjects.
    pub fn jetstream(&self) -> Result<bus::JetStream, String> {
        let js = bus::connect(&self.nats_url)?.jetstream();
        let _ = js.add_stream(&bus::StreamConfig {
            name: self.stream.clone(),
            subjects: vec![format!("{}.>", self.subj_root)],
            ..Default::default()
        });
        Ok(js)
    }
    /// Full subject from a config value that may be a bare suffix or a vx.* path.
    pub fn subject(&self, raw: &str) -> String {
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
