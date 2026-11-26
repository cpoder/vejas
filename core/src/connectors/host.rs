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
            hop.unit, hop.subject, ok, hop.error, hop.emits, hop.preview, None, hop.summary, hop.trace,
        );
    }
}

/// The host every connector of this runtime records its hops through.
pub fn host() -> Arc<dyn Host> {
    static HOST: OnceLock<Arc<dyn Host>> = OnceLock::new();
    HOST.get_or_init(|| Arc::new(Runtime)).clone()
}
