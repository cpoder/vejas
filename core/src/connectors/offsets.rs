//! Offset resume for exec stream sources (ADR-0011/0021).

use crate::bus;

//
// A generic resumption seam for exec stream sources whose remote has an offset
// (Kafka via kcat, above all): the driver reads the last committed offset from a
// KV bucket at (re)start and hands it to the child as `$OFFSET`, and commits each
// record's offset AFTER publishing it to the bus (publish-before-commit, so a
// crash re-consumes from the last committed offset — at-least-once, our model,
// not the remote's opaque consumer-group rebalancing).

pub const OFFSET_BUCKET: &str = "VEJAS_OFFSETS";

pub fn open_offset_store(js: &bus::JetStream) -> Option<bus::Kv> {
    js.key_value(&bus::KvConfig {
        bucket: OFFSET_BUCKET.to_string(),
        history: 1,
        ..Default::default()
    })
}

pub fn offset_get(store: &bus::Kv, key: &str) -> Option<String> {
    store
        .get(key)
        .ok()
        .flatten()
        .map(|b| String::from_utf8_lossy(&b).trim().to_string())
        .filter(|s| !s.is_empty())
}

pub fn offset_put(store: &bus::Kv, key: &str, value: &str) {
    let _ = store.put(key, value.as_bytes());
}
