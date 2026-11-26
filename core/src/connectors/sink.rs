//! The sink loop every sink driver runs.

use std::time::Instant;

use crate::bus;
use crate::connectors::Ctx;
use crate::connectors::dlq::to_dlq;
use crate::connectors::hops::{summarize, trace_preview};
use crate::connectors::pull::ack_wait;
use crate::metrics::{self, Kind, Span};
use crate::trace;

/// Shared sink loop: durable pull consumer -> handler; ack on Ok, nak on Err.
/// The handler may return a response summary, recorded in the trace ring so
/// the panel shows what the downstream answered (rejected facts, skips…).
/// The sink loop. `handler` does the side effect for one message and gets the
/// trace context to hand on (ADR-0034): an HTTP sink sends it as `traceparent`,
/// an exec sink as `TRACEPARENT` in the child's environment, so a downstream
/// system that traces hangs its spans under the sink's.
pub(super) fn run_sink(
    ctx: &Ctx,
    subject: &str,
    handler: impl Fn(&[u8], &trace::Context) -> Result<Option<String>, String>,
) -> Result<(), String> {
    let js = ctx.jetstream()?;
    let durable = ctx.name.replace([':', '.', '-'], "_");
    let filter = [subject.to_string()];
    let _ = js.create_pull_consumer(&ctx.stream, &durable, &filter, ack_wait(), None);
    let sub = js.pull_durable(&ctx.stream, &durable, Some(subject))?;
    eprintln!("[{}] consuming {subject}", ctx.name);
    // The span of one delivery: kind CLIENT, as the runtime is the client of
    // the system the sink delivers to.
    let sink_span = |hop: &trace::Hop, msg: &bus::Message, start_nanos: u128| {
        let mut span = Span::hop(hop, Kind::Client, &ctx.name, start_nanos).messaging("process", &msg.subject);
        if let Some(d) = msg.delivery() {
            span = span
                .attr("messaging.message.id", d.stream_seq.to_string())
                .attr("vejas.delivery.attempt", d.delivered);
        }
        span
    };
    loop {
        if !ctx.alive() {
            return Ok(());
        }
        for msg in sub.round()? {
            if !ctx.alive() {
                // stopped mid-batch: leave the message un-acked, it redelivers
                return Ok(());
            }
            let hop = trace::Hop::begin(trace::of(&msg));
            let carried = hop.context();
            let preview = trace_preview(&msg.data);
            let t0 = Instant::now();
            let start_nanos = metrics::now_nanos();
            match handler(&msg.data, &carried) {
                Ok(response) => {
                    metrics::observe(&ctx.name, true, 0, t0.elapsed().as_secs_f64());
                    if metrics::wants(hop.sampled, false) {
                        crate::record::end(
                            sink_span(&hop, &msg, start_nanos),
                            crate::record::Extras::new().input(&msg.data).answer(response.as_deref()),
                        );
                    }
                    let summary = response.as_deref().and_then(|r| summarize(r.as_bytes()));
                    crate::record_trace_full(
                        &ctx.name, subject, true, None, vec![], preview, None, summary, Some(&hop),
                    );
                    let _ = msg.ack();
                }
                Err(e) => {
                    metrics::observe(&ctx.name, false, 0, t0.elapsed().as_secs_f64());
                    // poison guard: give up after MAX_DELIVERIES attempts
                    let delivered =
                        msg.delivery().map(|i| i.delivered).unwrap_or(1);
                    let mut dead_lettered = false;
                    if delivered >= crate::MAX_DELIVERIES {
                        // Poison: park it in the DLQ, then ack — publish before ack
                        // so it is never lost (ADR-0015). On DLQ failure, nak.
                        match to_dlq(&js, &ctx.name, subject, delivered, &e, &msg.data, "", Some(&carried)) {
                            Ok(()) => {
                                dead_lettered = true;
                                metrics::inc_dead_letter(&ctx.name);
                                eprintln!("[{}] {e} -> DLQ after {delivered} deliveries", ctx.name);
                                crate::record_trace_full(
                                    &ctx.name, subject, false,
                                    Some(format!("{e} — dead-lettered after {delivered} deliveries")),
                                    vec![], preview, None, None, Some(&hop),
                                );
                                let _ = msg.ack();
                            }
                            Err(de) => {
                                eprintln!("[{}] {e} -> DLQ publish failed ({de}); nak", ctx.name);
                                crate::record_trace_full(
                                    &ctx.name, subject, false,
                                    Some(format!("{e} — DLQ publish failed: {de}")),
                                    vec![], preview, None, None, Some(&hop),
                                );
                                let _ = msg.nak();
                            }
                        }
                    } else {
                        eprintln!("[{}] {e} -> nak", ctx.name);
                        crate::record_trace_full(
                            &ctx.name, subject, false, Some(e.clone()), vec![], preview, None, None, Some(&hop),
                        );
                        let _ = msg.nak();
                    }
                    if metrics::observing() {
                        let span = sink_span(&hop, &msg, start_nanos);
                        let span = if dead_lettered { span.attr("vejas.dlq", true) } else { span };
                        crate::record::end(span.failed(e), crate::record::Extras::new().input(&msg.data));
                    }
                }
            }
        }
    }
}
