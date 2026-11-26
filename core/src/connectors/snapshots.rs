//! Detect snapshots: a detect unit's state, kept in an object store.

use serde_json::Value;

use crate::bus;

pub const DETECT_STATE_BUCKET: &str = "VEJAS_DETECT_STATE";

/// A detect unit's snapshot as stored: which program it belongs to (the hash
/// of its source), the stream sequence it stands for — everything up to it is
/// in the state, everything after it replays on restart — when it was taken,
/// and the engine's bytes.
pub struct Snapshot {
    pub version: String,
    pub seq: u64,
    pub taken_at: u64,
    pub state: Vec<u8>,
}

/// The object store the detect units keep their snapshots in: one object per
/// unit, the latest only. Create-or-open, like every bucket in this repo.
pub fn open_state_store(js: &bus::JetStream) -> Option<bus::Objects> {
    js.object_store(DETECT_STATE_BUCKET)
}

/// Store a snapshot: one JSON header line, then the engine's bytes as they are.
pub fn snapshot_put(
    store: &bus::Objects,
    unit: &str,
    snap: &Snapshot,
) -> Result<(), String> {
    let mut bytes = serde_json::json!({
        "version": snap.version,
        "seq": snap.seq,
        "taken_at": snap.taken_at,
        "bytes": snap.state.len(),
    })
    .to_string()
    .into_bytes();
    bytes.push(b'\n');
    bytes.extend_from_slice(&snap.state);
    store.put(unit, &bytes).map_err(|e| format!("snapshot put: {e}"))
}

/// The unit's snapshot, if it has one.
pub fn snapshot_get(store: &bus::Objects, unit: &str) -> Result<Option<Snapshot>, String> {
    let Some(bytes) = store.get(unit).map_err(|e| format!("snapshot get: {e}"))? else {
        return Ok(None);
    };
    let nl = bytes
        .iter()
        .position(|b| *b == b'\n')
        .ok_or("snapshot without a header")?;
    let header: Value = serde_json::from_slice(&bytes[..nl])
        .map_err(|e| format!("snapshot header: {e}"))?;
    Ok(Some(Snapshot {
        version: header["version"].as_str().unwrap_or("").to_string(),
        seq: header["seq"].as_u64().unwrap_or(0),
        taken_at: header["taken_at"].as_u64().unwrap_or(0),
        state: bytes[nl + 1..].to_vec(),
    }))
}
