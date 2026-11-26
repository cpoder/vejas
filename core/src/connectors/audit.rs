//! The promote audit trail (ADR-0018).

use crate::bus;
use crate::connectors::dlq::dlq_unit_token;
use crate::connectors::pull::hydrate_recent;

pub const AUDIT_STREAM: &str = "VEJAS_AUDIT";
pub const AUDIT_ROOT: &str = "vxaudit";
// Generous: the audit trail must not evict on churn. The oldest-discard cap is a
// last-resort safety bound (a full audit stream is itself an operator signal),
// never a routine truncation — like the DLQ, on its own sibling root so no
// `max_age` on the hot stream can touch it.
pub const AUDIT_MAX_MSGS: i64 = 1_000_000;

pub fn ensure_audit_stream(js: &bus::JetStream) {
    let _ = js.add_stream(&bus::StreamConfig {
        name: AUDIT_STREAM.to_string(),
        subjects: vec![format!("{AUDIT_ROOT}.>")],
        max_msgs: AUDIT_MAX_MSGS,
        discard: bus::Discard::Old,
        ..Default::default()
    });
}

/// Append one promote record to the audit stream. `record` carries file, name,
/// key, before, after, actor, ts. Publish-confirmed (JetStream stores it) so the
/// caller can trust the trail; the live-promote path logs but does not fail the
/// promote if this errors (git remains the trail for committed changes).
pub fn to_audit(
    js: &bus::JetStream,
    unit: &str,
    record: &serde_json::Value,
) -> Result<(), String> {
    ensure_audit_stream(js);
    let body = serde_json::to_vec(record).map_err(|e| e.to_string())?;
    let subject = format!("{AUDIT_ROOT}.{}", dlq_unit_token(unit));
    js.publish(&subject, body).map(|_| ()).map_err(|e| e.to_string())
}

/// Recent audit records for a unit, oldest last (read-only, ephemeral consumer).
pub fn audit_recent(
    js: &bus::JetStream,
    unit: &str,
    n: usize,
) -> Result<Vec<serde_json::Value>, String> {
    let subject = format!("{AUDIT_ROOT}.{}", dlq_unit_token(unit));
    Ok(hydrate_recent(js, AUDIT_STREAM, &subject, n)?
        .into_iter()
        .map(|(_, _, v)| v)
        .collect())
}
