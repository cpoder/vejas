//! The sink loop every sink driver runs.

use std::time::Instant;

use crate::bus;
use crate::dlq::to_dlq;
use crate::hop::{summarize, trace_preview, HopEvent};
use crate::pull::ack_wait;
use crate::span::{now_nanos, Kind, Span};
use crate::trace;
use crate::{Ctx, MAX_DELIVERIES};

/// What a sink does with one message: see [`Ctx::consume`].
type Handler<'a> = dyn Fn(&[u8], &trace::Context) -> Result<Option<String>, String> + 'a;

impl Ctx {
    /// The sink loop: a durable pull consumer on `subject`, one call of
    /// `handler` per message; ack on Ok, nak on Err, and past
    /// `MAX_DELIVERIES` the message goes to the dead-letter queue.
    ///
    /// `handler` does the side effect for one message and gets the trace
    /// context to hand on (ADR-0034): an HTTP sink sends it as `traceparent`,
    /// an exec sink as `TRACEPARENT` in the child's environment, so a
    /// downstream system that traces hangs its spans under the sink's. It may
    /// return what the downstream answered: the events ring shows its
    /// beginning (rejected facts, skips…), the trace record keeps it whole.
    pub fn consume(
        &self,
        subject: &str,
        handler: impl Fn(&[u8], &trace::Context) -> Result<Option<String>, String>,
    ) -> Result<(), String> {
        self.consume_with(subject, &handler)
    }

    /// The loop itself, compiled once for every sink: a closure per driver
    /// would compile it again for each (see the note in `bus`).
    fn consume_with(
        &self,
        subject: &str,
        handler: &Handler,
    ) -> Result<(), String> {
        let ctx = self;
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
                if let Some(died) = d.replay_of {
                    span = span.attr("vejas.replay_of", died.to_string());
                }
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
                // the replay of another unit's dead letter (ADR-0015 Amendment C)
                if msg.replay_elsewhere(&ctx.name) {
                    let _ = msg.ack();
                    continue;
                }
                let hop = trace::Hop::begin(trace::of(&msg));
                let carried = hop.context();
                let preview = trace_preview(&msg.data);
                let t0 = Instant::now();
                let start_nanos = now_nanos();
                match handler(&msg.data, &carried) {
                    Ok(response) => {
                        let latency = t0.elapsed();
                        let span = ctx
                            .host
                            .wants(hop.sampled, false)
                            .then(|| sink_span(&hop, &msg, start_nanos));
                        ctx.host.hop(HopEvent {
                            unit: &ctx.name,
                            subject,
                            trace: Some(&hop),
                            preview,
                            summary: response.as_deref().and_then(|r| summarize(r.as_bytes())),
                            span,
                            input: Some(&msg.data),
                            answer: response.as_deref(),
                            latency: Some(latency),
                            delivery: msg.delivery(),
                            ..HopEvent::default()
                        });
                        let _ = msg.ack();
                    }
                    Err(e) => {
                        let latency = t0.elapsed();
                        // poison guard: give up after MAX_DELIVERIES attempts
                        let delivery = msg.delivery();
                        let delivered = delivery.as_ref().map(|i| i.delivered).unwrap_or(1);
                        // Poison: park it in the DLQ, then ack — publish before ack
                        // so it is never lost (ADR-0015). On DLQ failure, nak.
                        let parked = (delivered >= MAX_DELIVERIES)
                            .then(|| to_dlq(&js, &ctx.name, subject, delivered, &e, &msg.data, "", Some(&carried), delivery.as_ref().map(bus::Delivery::message)));
                        let (ring_error, dead_lettered) = match &parked {
                            Some(Ok(())) => {
                                eprintln!("[{}] {e} -> DLQ after {delivered} deliveries", ctx.name);
                                (format!("{e} — dead-lettered after {delivered} deliveries"), true)
                            }
                            Some(Err(de)) => {
                                eprintln!("[{}] {e} -> DLQ publish failed ({de}); nak", ctx.name);
                                (format!("{e} — DLQ publish failed: {de}"), false)
                            }
                            None => {
                                eprintln!("[{}] {e} -> nak", ctx.name);
                                (e.clone(), false)
                            }
                        };
                        let span = ctx.host.observing().then(|| {
                            let span = sink_span(&hop, &msg, start_nanos);
                            let span = if dead_lettered { span.attr("vejas.dlq", true) } else { span };
                            span.failed(e)
                        });
                        ctx.host.hop(HopEvent {
                            unit: &ctx.name,
                            subject,
                            trace: Some(&hop),
                            error: Some(ring_error),
                            preview,
                            span,
                            input: Some(&msg.data),
                            latency: Some(latency),
                            dead_lettered,
                            delivery,
                            ..HopEvent::default()
                        });
                        if dead_lettered {
                            let _ = msg.ack();
                        } else {
                            let _ = msg.nak();
                        }
                    }
                }
            }
        }
    }
}
