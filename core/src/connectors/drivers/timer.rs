//! timer: emits a payload on a subject every INTERVAL_SECS.

use std::thread;
use std::time::Duration;

use serde_json::{Map, Value};
use vejas_connector::clock::iso8601_utc;

use crate::connectors::{Ctx, Driver};
use crate::metrics::Kind;

/// A timer event carries its tick timestamp: an object payload without a `ts`
/// field gets one (ISO 8601 UTC). A dated event is what lets downstream flows
/// build idempotency keys — same reasoning as oauth-poll's `fetched_at` (the
/// language has no clock, on purpose).
fn stamp_ts(payload: &Value, secs: u64) -> Value {
    match payload {
        Value::Object(o) if !o.contains_key("ts") => {
            let mut o = o.clone();
            o.insert("ts".into(), Value::String(iso8601_utc(secs)));
            Value::Object(o)
        }
        other => other.clone(),
    }
}

pub(super) struct Timer;
impl Driver for Timer {
    fn kind(&self) -> &'static str {
        "source:interval"
    }
    fn about(&self) -> &'static str {
        "Emits PAYLOAD on SUBJECT every INTERVAL_SECS; an object payload gains a ts field (ISO 8601 UTC) when absent. Config: SUBJECT, INTERVAL_SECS, PAYLOAD."
    }
    fn run(&self, ctx: &Ctx) -> Result<(), String> {
        let subject = ctx.subject(&ctx.config.str("SUBJECT").ok_or("SUBJECT required")?);
        let interval = ctx.config.u64_or("INTERVAL_SECS", 60).max(1);
        let payload = ctx.config.value("PAYLOAD").unwrap_or(Value::Object(Map::new()));
        let js = ctx.jetstream()?;
        eprintln!("[{}] timer every {interval}s -> {subject}", ctx.name);
        let mut waited = 0;
        loop {
            if !ctx.alive() {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(250));
            waited += 250;
            if waited >= interval * 1000 {
                waited = 0;
                let bytes = serde_json::to_vec(&stamp_ts(&payload, crate::now_secs()))
                    .unwrap_or_default();
                if let (_, Err(e)) = ctx.publish(&js, &subject, &bytes, None, Kind::Producer) {
                    return Err(format!("publish: {e}"));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn timer_payload_gains_ts_once() {
        let stamped = stamp_ts(&json!({"origin": "collector"}), 1_000_000_000);
        assert_eq!(stamped, json!({"origin": "collector", "ts": "2001-09-09T01:46:40Z"}));
        // an explicit ts is preserved, non-objects pass through untouched
        assert_eq!(stamp_ts(&json!({"ts": "x"}), 0), json!({"ts": "x"}));
        assert_eq!(stamp_ts(&json!("ping"), 0), json!("ping"));
    }
}
