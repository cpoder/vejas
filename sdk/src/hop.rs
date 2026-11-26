//! What a connector reports of each hop, and who records it (ADR-0034,
//! ADR-0038): the events ring the panel reads, the span the trace record and
//! the exporter take, the metrics.

use std::time::Duration;

use crate::bus;
use crate::span::{now_nanos, Kind, Span};
use crate::trace;
use crate::Ctx;

/// What records the hops of a connector. The runtime's host writes them to
/// its events ring, its trace record and its metrics; a standalone
/// connector's host hands them to the runtime that runs it, which records
/// them through the same path.
pub trait Host: Send + Sync {
    /// Does anything take a hop's span: an exporter, or the trace record?
    fn observing(&self) -> bool;
    /// Is this hop's span worth building: something takes spans, and the
    /// sampler kept the trace or the hop failed (failures are always kept).
    fn wants(&self, sampled: bool, failed: bool) -> bool {
        (sampled || failed) && self.observing()
    }
    /// Record one hop.
    fn hop(&self, hop: HopEvent<'_>);
}

/// One hop a connector made: what is recorded of it.
#[derive(Default)]
pub struct HopEvent<'a> {
    /// The connector that made it.
    pub unit: &'a str,
    /// The subject it published to or consumed from.
    pub subject: &'a str,
    /// Its place in a trace. None for what happens outside one: a reply a
    /// request/reply connector sends, a failure before anything is published.
    pub trace: Option<&'a trace::Hop>,
    /// None when it worked; otherwise what went wrong, as the events ring
    /// shows it.
    pub error: Option<String>,
    /// The subjects it published to.
    pub emits: Vec<String>,
    /// The beginning of its input, for the events ring.
    pub preview: String,
    /// The beginning of what the downstream system answered, for the ring.
    pub summary: Option<String>,
    /// Its span, when something takes one (see [`Host::wants`]).
    pub span: Option<Span>,
    /// The payload it carried, which the trace record keeps with the span.
    pub input: Option<&'a [u8]>,
    /// What the downstream system answered, whole, for the trace record.
    pub answer: Option<&'a str>,
    /// How long a delivery took, for the metrics; sinks report it.
    pub latency: Option<Duration>,
    /// The delivery was parked in the dead-letter queue.
    pub dead_lettered: bool,
    /// The JetStream delivery a sink consumed: which message, which attempt.
    /// The ring tells a retry from another message of the same trace by it.
    pub delivery: Option<bus::Delivery>,
}

/// The beginning of a payload, as the events ring shows it.
pub fn trace_preview(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).chars().take(160).collect()
}

/// What a sink's downstream answered, whole. The trace record scrubs it before
/// it cuts it, so a JSON answer is still JSON when its credentials are masked;
/// the events ring keeps `summarize` of it.
pub fn answer_text(bytes: Vec<u8>) -> Option<String> {
    let text = String::from_utf8(bytes)
        .unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned());
    (!text.trim().is_empty()).then_some(text)
}

/// A sink's response-body summary (what the downstream API answered).
pub fn summarize(bytes: &[u8]) -> Option<String> {
    let s = String::from_utf8_lossy(bytes);
    let s = s.trim();
    if s.is_empty() {
        None
    } else {
        Some(s.chars().take(200).collect())
    }
}

/// Publish what a source let in, as the first hop of its trace (ADR-0034): a
/// new trace, or the caller's when the request carried one. The message
/// carries the trace on; the hop's span is a PRODUCER (a SERVER for http-in),
/// and the host records the publish or its failure. [`Ctx::publish`] is the
/// same for a driver's own thread; this form serves a thread that holds the
/// host and the name without the context.
pub fn publish(
    host: &dyn Host,
    unit: &str,
    js: &bus::JetStream,
    subject: &str,
    bytes: &[u8],
    incoming: Option<trace::Context>,
    kind: Kind,
) -> (trace::Hop, Result<u64, String>) {
    let hop = trace::Hop::begin(incoming);
    let start_nanos = now_nanos();
    let published = js.publish_traced(subject, bytes, &hop.context());
    let error = published.as_ref().err().map(|e| format!("publish: {e}"));
    let span = host.wants(hop.sampled, error.is_some()).then(|| {
        let mut span = Span::hop(&hop, kind, unit, start_nanos).messaging("send", subject);
        if kind == Kind::Server {
            span = span.attr("http.request.method", "POST");
        }
        match &error {
            None => span,
            Some(e) => span.failed(e.clone()),
        }
    });
    let sent = error.is_none();
    host.hop(HopEvent {
        unit,
        subject,
        trace: Some(&hop),
        error,
        emits: if sent { vec![subject.to_string()] } else { Vec::new() },
        preview: if sent { trace_preview(bytes) } else { String::new() },
        span,
        input: Some(bytes),
        ..HopEvent::default()
    });
    (hop, published)
}

impl Ctx {
    /// Publish what this source let in: see [`publish`].
    pub fn publish(
        &self,
        js: &bus::JetStream,
        subject: &str,
        bytes: &[u8],
        incoming: Option<trace::Context>,
        kind: Kind,
    ) -> (trace::Hop, Result<u64, String>) {
        publish(&*self.host, &self.name, js, subject, bytes, incoming, kind)
    }

    /// A source-side failure worth showing in the panel: the remote side
    /// refused, the command failed.
    pub fn failed(&self, subject: &str, err: String) {
        self.host.hop(HopEvent { unit: &self.name, subject, error: Some(err), ..HopEvent::default() });
    }

    /// A reply that went out, in the events ring (the panel's "is it flowing?").
    pub fn replied(&self, subject: &str, bytes: &[u8]) {
        self.host.hop(HopEvent {
            unit: &self.name,
            subject,
            emits: vec![subject.to_string()],
            preview: trace_preview(bytes),
            ..HopEvent::default()
        });
    }
}
