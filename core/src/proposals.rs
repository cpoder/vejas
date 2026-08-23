//! The proposal queue (ADR-0024): governed change from agents and the fleet.
//!
//! A proposal is a mutation an agent SUBMITS and a human APPROVES — the product
//! seam between "how" (the agent owns it) and "what it means" (the human owns it).
//! Proposals live in a JetStream KV bucket: the **live queue** — mutable, keyed by
//! id, cluster-visible like versions and leases. It is NOT the audit trail; the
//! durable record of who approved what is the VEJAS_AUDIT stream (a proposal aging
//! out of the bounded KV history never takes the proof of its approval with it).
//!
//! This module is pure KV CRUD; the runtime (main.rs) owns the policy — the
//! approval token, the baseline staleness check, execution on approve, and the
//! `vx.proposals.events` emission — because those need runtime helpers.

use std::sync::atomic::{AtomicU64, Ordering};

const PROPOSALS_BUCKET: &str = "VEJAS_PROPOSALS";
const PROPOSALS_HISTORY: i64 = 64; // bounded live queue; audit is the durable record

static SEQ: AtomicU64 = AtomicU64::new(1);

/// Open (or create) the shared proposal bucket. None if JetStream KV is
/// unavailable (proposals need the shared bus; a single dev instance without it
/// simply has no queue).
pub fn open_store(js: &nats::jetstream::JetStream) -> Option<nats::kv::Store> {
    let cfg = nats::kv::Config {
        bucket: PROPOSALS_BUCKET.to_string(),
        history: PROPOSALS_HISTORY,
        ..Default::default()
    };
    js.create_key_value(&cfg)
        .or_else(|_| js.key_value(PROPOSALS_BUCKET))
        .ok()
}

/// A fresh proposal id — `ts` is the wall-clock second (stamped by the caller;
/// this layer has no clock), a per-process counter makes it unique within it.
pub fn new_id(ts: u64) -> String {
    format!("p-{ts}-{}", SEQ.fetch_add(1, Ordering::Relaxed))
}

/// Submit a proposal (status "pending"). Returns the stored record.
#[allow(clippy::too_many_arguments)]
pub fn submit(
    store: &nats::kv::Store,
    id: &str,
    kind: &str,
    payload: serde_json::Value,
    author: &str,
    baseline: &str,
    evidence: serde_json::Value,
    ts: u64,
) -> Result<serde_json::Value, String> {
    let record = serde_json::json!({
        "id": id,
        "kind": kind,
        "payload": payload,
        "author": author,
        "created_at": ts,
        "baseline": baseline,
        "evidence": evidence,
        "status": "pending",
    });
    store
        .put(id, record.to_string())
        .map_err(|e| e.to_string())?;
    Ok(record)
}

pub fn get(store: &nats::kv::Store, id: &str) -> Option<serde_json::Value> {
    store
        .get(id)
        .ok()
        .flatten()
        .and_then(|b| serde_json::from_slice(&b).ok())
}

/// Every proposal, newest first (by created_at).
pub fn list(store: &nats::kv::Store) -> Vec<serde_json::Value> {
    let mut all: Vec<serde_json::Value> = store
        .keys()
        .map(|keys| keys.filter_map(|k| get(store, &k)).collect())
        .unwrap_or_default();
    all.sort_by_key(|p| std::cmp::Reverse(p["created_at"].as_u64().unwrap_or(0)));
    all
}

/// Transition a proposal's status (and stamp who/when for a human decision).
/// Returns the updated record, or an error if it is gone.
pub fn set_status(
    store: &nats::kv::Store,
    id: &str,
    status: &str,
    decided_by: Option<&str>,
    ts: u64,
) -> Result<serde_json::Value, String> {
    let mut rec = get(store, id).ok_or("proposal not found")?;
    if let Some(o) = rec.as_object_mut() {
        o.insert("status".into(), serde_json::json!(status));
        o.insert("decided_at".into(), serde_json::json!(ts));
        if let Some(by) = decided_by {
            o.insert("decided_by".into(), serde_json::json!(by));
        }
    }
    store.put(id, rec.to_string()).map_err(|e| e.to_string())?;
    Ok(rec)
}
