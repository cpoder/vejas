//! The runtime's host (ADR-0038): how the runtime records a hop a connector
//! reports. One path for every hop, so the events ring, the trace record with
//! its masking and its vault scrub, and the metrics stay in one place.

use std::sync::{Arc, OnceLock};

use vejas_connector::{HopEvent, Host};

use crate::metrics;
use crate::record::{self, Extras};

struct Runtime;

impl Host for Runtime {
    fn observing(&self) -> bool {
        metrics::observing()
    }

    fn hop(&self, hop: HopEvent<'_>) {
        let ok = hop.error.is_none();
        if let Some(latency) = hop.latency {
            metrics::observe(hop.unit, ok, 0, latency.as_secs_f64());
        }
        if hop.dead_lettered {
            metrics::inc_dead_letter(hop.unit);
        }
        if let Some(span) = hop.span {
            let mut extras = Extras::new().emits(hop.emits.clone()).answer(hop.answer);
            if let Some(input) = hop.input {
                extras = extras.input(input);
            }
            record::end(span, extras);
        }
        crate::record_trace_full(
            hop.unit, hop.subject, ok, hop.error, hop.emits, hop.preview, None, hop.summary, hop.trace, hop.delivery,
        );
    }
}

/// The host every connector of this runtime records its hops through.
pub fn host() -> Arc<dyn Host> {
    static HOST: OnceLock<Arc<dyn Host>> = OnceLock::new();
    HOST.get_or_init(|| Arc::new(Runtime)).clone()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::Value;
    use vejas_connector::{HopEvent, Host};

    use super::Runtime;
    use crate::metrics;

    fn ring(unit: &str) -> Vec<Value> {
        let out: Value = serde_json::from_str(&crate::events_json(Some(unit))).unwrap();
        out["events"].as_array().cloned().unwrap_or_default()
    }

    // Every connector's hops go through this one function: what it writes is
    // what the panel, the metrics and the trace record show of a connector.
    #[test]
    fn a_dead_lettered_delivery_reaches_the_ring_and_the_metrics() {
        let unit = "connector:host_probe_sink";
        Runtime.hop(HopEvent {
            unit,
            subject: "vx.hp.out",
            error: Some("refused — dead-lettered after 5 deliveries".into()),
            preview: "{\"a\":1}".into(),
            latency: Some(Duration::from_millis(3)),
            dead_lettered: true,
            ..HopEvent::default()
        });
        let events = ring(unit);
        assert_eq!(events.len(), 1);
        let e = &events[0];
        assert_eq!(e["ok"], false);
        assert_eq!(e["subject"], "vx.hp.out");
        assert_eq!(e["error"], "refused — dead-lettered after 5 deliveries");
        assert_eq!(e["preview"], "{\"a\":1}");
        assert_eq!(e["emits"], serde_json::json!([]));
        let text = metrics::render(&[]);
        assert!(text.contains("vejas_events_processed_total{unit=\"connector:host_probe_sink\",result=\"error\"} 1"));
        assert!(text.contains("vejas_dead_letters_total{unit=\"connector:host_probe_sink\"} 1"));
    }

    #[test]
    fn a_published_hop_lists_its_subject_and_counts_no_delivery() {
        let unit = "connector:host_probe_source";
        Runtime.hop(HopEvent {
            unit,
            subject: "vx.hp.in",
            emits: vec!["vx.hp.in".into()],
            preview: "ping".into(),
            summary: Some("accepted".into()),
            ..HopEvent::default()
        });
        let events = ring(unit);
        assert_eq!(events.len(), 1);
        let e = &events[0];
        assert_eq!(e["ok"], true);
        assert_eq!(e["error"], Value::Null);
        assert_eq!(e["emits"], serde_json::json!(["vx.hp.in"]));
        assert_eq!(e["response"], "accepted");
        // A source's publish is no delivery: no latency, so nothing observed.
        assert!(!metrics::render(&[]).contains("unit=\"connector:host_probe_source\""));
    }
}
