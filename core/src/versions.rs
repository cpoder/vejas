//! Cluster-wide version overlay (ADR-0021), increment 1.
//!
//! A live business-surface promote in a cluster cannot write one instance's local
//! file (ADR-0020 refuses it — split-brain). Instead it publishes a **version**: a
//! content snapshot of the flow's source to a JetStream KV bucket. Every instance
//! watches the bucket and hot-reloads the flow from the snapshot, so a promote
//! fans out atomically with no per-instance write.
//!
//! Git stays the source of truth for *deployed* versions; the bucket is the live
//! **overlay** between deploys. The reconciliation rule (the crux): an overlay is
//! valid only against the exact git baseline it forked from. If the baseline moved
//! (a deploy) or the flow was git-deleted, **git wins and the overlay is evicted
//! loudly** — never the inverse (a silent overlay masking a deploy, or a ghost
//! flow after `git rm`).

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::{Mutex, OnceLock};

const VERSIONS_BUCKET: &str = "VEJAS_VERSIONS";
const VERSIONS_HISTORY: i64 = 64; // bounded rollback/audit history per flow (ADR-0021)

/// Content hash of a flow's source — the version id and the baseline fingerprint.
/// A 64-bit non-cryptographic hash is enough to detect "did this content change".
pub fn hash_content(s: &str) -> String {
    let mut h = DefaultHasher::new();
    s.hash(&mut h);
    format!("{:016x}", h.finish())
}

/// A flow name as a KV key token (KV keys disallow the same characters as subjects).
fn key_of(flow: &str) -> String {
    flow.chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' })
        .collect()
}

/// Open (or create) the shared version bucket. None if JetStream KV is
/// unavailable — the caller then uses the baseline file (single-instance, no
/// overlay), no worse than before versioning existed.
pub fn open_store(js: &nats::jetstream::JetStream) -> Option<nats::kv::Store> {
    let cfg = nats::kv::Config {
        bucket: VERSIONS_BUCKET.to_string(),
        history: VERSIONS_HISTORY,
        ..Default::default()
    };
    js.create_key_value(&cfg)
        .or_else(|_| js.key_value(VERSIONS_BUCKET))
        .ok()
}

/// The current overlay for a flow, if any (the parsed snapshot value).
pub fn get_version(store: &nats::kv::Store, flow: &str) -> Option<serde_json::Value> {
    store
        .get(&key_of(flow))
        .ok()
        .flatten()
        .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok())
}

/// Publish a new version for a flow: the new source, the baseline it forked from,
/// its content hash, the actor, and a timestamp. `ts` is passed in (the language
/// and this layer have no clock of their own — the caller stamps it).
pub fn put_version(
    store: &nats::kv::Store,
    flow: &str,
    source: &str,
    parent_hash: &str,
    actor: &str,
    ts: u64,
) -> Result<u64, String> {
    let record = serde_json::json!({
        "flow": flow,
        "source": source,
        "parent": parent_hash,
        "hash": hash_content(source),
        "actor": actor,
        "ts": ts,
    });
    store
        .put(&key_of(flow), record.to_string())
        .map_err(|e| e.to_string())
}

/// What `resolve_source` did — so the caller can audit an eviction.
pub enum Outcome {
    Baseline,
    Overlay,
    Evicted { parent: String, baseline: String },
}

/// Recent overlay evictions, newest last, capped — the panel reads these to show
/// a banner ("your live promote was superseded by a deploy"). In memory: an
/// eviction is a transient operator signal, not durable state.
static EVICTIONS: OnceLock<Mutex<Vec<serde_json::Value>>> = OnceLock::new();

fn note_eviction(flow: &str, parent: &str, baseline: &str, ts: u64) {
    let mut v = EVICTIONS.get_or_init(|| Mutex::new(Vec::new())).lock().unwrap();
    v.push(serde_json::json!({
        "flow": flow, "parent": parent, "baseline": baseline, "ts": ts,
    }));
    while v.len() > 50 {
        v.remove(0);
    }
}

/// The recent evictions, for the panel banner.
pub fn recent_evictions() -> Vec<serde_json::Value> {
    EVICTIONS
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .clone()
}

/// The EFFECTIVE source for a flow: the overlay if it is valid against the current
/// baseline, else the baseline. A stale overlay (baseline advanced or flow gone)
/// is evicted here — git wins, loudly (logged + noted for the panel; the caller
/// also writes an ADR-0018 audit record on `Evicted`). Returns (source, outcome).
pub fn resolve_source(
    store: Option<&nats::kv::Store>,
    flow: &str,
    baseline: &str,
    ts: u64,
) -> (String, Outcome) {
    let Some(store) = store else {
        return (baseline.to_string(), Outcome::Baseline);
    };
    let Some(ov) = get_version(store, flow) else {
        return (baseline.to_string(), Outcome::Baseline);
    };
    let parent = ov["parent"].as_str().unwrap_or("").to_string();
    let base_hash = hash_content(baseline);
    if parent == base_hash {
        // overlay forked from exactly this baseline → it applies
        let src = ov["source"].as_str().unwrap_or(baseline).to_string();
        (src, Outcome::Overlay)
    } else {
        // baseline moved (a deploy) or the flow changed under it → git wins.
        // Evict the overlay loudly; the content stays in KV history, re-promotable.
        let _ = store.delete(&key_of(flow));
        eprintln!(
            "[vejas] overlay for {flow} EVICTED — superseded by a deploy \
             (overlay parent {parent}, current baseline {base_hash}); git wins, \
             content kept in VEJAS_VERSIONS history"
        );
        note_eviction(flow, &parent, &base_hash, ts);
        (
            baseline.to_string(),
            Outcome::Evicted { parent, baseline: base_hash },
        )
    }
}

/// A flow name as its version KV key token — for the version watch to map a KV
/// key back to a supervised unit.
pub fn key_token(flow: &str) -> String {
    key_of(flow)
}
