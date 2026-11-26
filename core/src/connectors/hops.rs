//! What a connector records of each hop: the trace ring the panel reads, and
//! the span the trace record is written from (ADR-0034).

use crate::bus;
use crate::metrics::{self, Kind, Span};
use crate::trace;

pub fn trace_preview(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).chars().take(160).collect()
}

/// A reply that went out, in the trace ring (the panel's "is it flowing?").
pub(super) fn trace_pub(name: &str, subject: &str, bytes: &[u8]) {
    crate::record_trace_full(
        name, subject, true, None,
        vec![subject.to_string()], trace_preview(bytes), None, None, None,
    );
}

/// A source-side failure worth showing in the panel.
pub(super) fn trace_fail(name: &str, subject: &str, err: String) {
    crate::record_trace_full(name, subject, false, Some(err), vec![], String::new(), None, None, None);
}

/// Publish what a source let in, as the first hop of its trace (ADR-0034): a
/// new trace, or the caller's when the request carried one. The message
/// carries the trace on; the hop's span is a PRODUCER (a SERVER for http-in),
/// and the trace ring records the publish or its failure.
pub(super) fn source_publish(
    name: &str,
    js: &bus::JetStream,
    subject: &str,
    bytes: &[u8],
    incoming: Option<trace::Context>,
    kind: Kind,
) -> (trace::Hop, Result<u64, String>) {
    let hop = trace::Hop::begin(incoming);
    let start_nanos = metrics::now_nanos();
    let published = js.publish_traced(subject, bytes, &hop.context());
    if metrics::wants(hop.sampled, published.is_err()) {
        let mut span = Span::hop(&hop, kind, name, start_nanos).messaging("send", subject);
        if kind == Kind::Server {
            span = span.attr("http.request.method", "POST");
        }
        crate::record::end(
            match &published {
                Ok(_) => span,
                Err(e) => span.failed(format!("publish: {e}")),
            },
            crate::record::Extras::new().emits(vec![subject.to_string()]).input(bytes),
        );
    }
    match &published {
        Ok(_) => crate::record_trace_full(
            name, subject, true, None,
            vec![subject.to_string()], trace_preview(bytes), None, None, Some(&hop),
        ),
        Err(e) => crate::record_trace_full(
            name, subject, false, Some(format!("publish: {e}")),
            vec![], String::new(), None, None, Some(&hop),
        ),
    }
    (hop, published)
}

/// What a sink's downstream answered, whole. The trace record scrubs it before
/// it cuts it, so a JSON answer is still JSON when its credentials are masked;
/// the events ring keeps `summarize` of it.
pub(super) fn answer_text(bytes: Vec<u8>) -> Option<String> {
    let text = String::from_utf8(bytes)
        .unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned());
    (!text.trim().is_empty()).then_some(text)
}

/// A sink's response-body summary (what the downstream API answered).
pub(super) fn summarize(bytes: &[u8]) -> Option<String> {
    let s = String::from_utf8_lossy(bytes);
    let s = s.trim();
    if s.is_empty() {
        None
    } else {
        Some(s.chars().take(200).collect())
    }
}
