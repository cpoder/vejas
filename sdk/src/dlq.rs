//! The dead-letter queue (ADR-0015), and the events that announce a dead letter
//! (ADR-0015, Amendment A).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::Duration;

use serde_json::Value;

use crate::bus;
use crate::clock::epoch_secs;
use crate::trace;

// Poison messages are parked, not dropped, in a DEDICATED JetStream stream on a
// sibling root (vxdlq.<unit>, never vx.> — overlapping stream subjects are
// forbidden and the DLQ's retention must be independent of the hot path). The
// caller acks the original ONLY when to_dlq() returns Ok (publish-before-ack).

pub const DLQ_STREAM: &str = "VEJAS_DLQ";
pub const DLQ_ROOT: &str = "vxdlq";
pub const DLQ_MAX_MSGS: i64 = 100_000;

/// Ensure the dead-letter stream exists (idempotent). Bounded + discard-oldest:
/// a full DLQ is itself an operator signal, never a silent unbounded growth.
pub fn ensure_dlq_stream(js: &bus::JetStream) {
    let _ = js.add_stream(&bus::StreamConfig {
        name: DLQ_STREAM.to_string(),
        subjects: vec![format!("{DLQ_ROOT}.>")],
        max_msgs: DLQ_MAX_MSGS,
        discard: bus::Discard::Old,
        ..Default::default()
    });
}

/// A unit name (e.g. "connector:sap_out", "flow:orders") as a NATS subject token.
pub fn dlq_unit_token(unit: &str) -> String {
    unit.chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' })
        .collect()
}

/// Park one poison message in the DLQ as a death envelope. Returns Ok only when
/// JetStream has stored it — the caller must ack the original only on Ok.
/// `message`: the stream sequence of the message that died ([`bus::Delivery::message`]),
/// which its replay names. `consumer`: the durable consumer the unit read it
/// with, whose filter a replay checks still takes the subject.
#[allow(clippy::too_many_arguments)]
pub fn to_dlq(
    js: &bus::JetStream,
    unit: &str,
    subject: &str,
    attempts: i64,
    last_error: &str,
    payload: &[u8],
    version: &str,
    trace: Option<&trace::Context>,
    message: Option<u64>,
    consumer: &str,
) -> Result<(), String> {
    ensure_dlq_stream(js);
    let now = epoch_secs();
    let envelope = serde_json::json!({
        "original_subject": subject,
        "unit": unit,
        "attempts": attempts,
        "first_seen": now,
        "dead_at": now,
        "last_error": last_error,
        // The version (content hash) that failed this message (ADR-0021). Empty
        // for unversioned units. So a replay after a promote to v2 is legible:
        // "this died under v1 — does it still fail under the current version?"
        "version": version,
        // Raw text of the original message — replayed verbatim, so JSON and
        // non-JSON alike re-inject faithfully; the panel pretty-prints it if JSON.
        "payload": String::from_utf8_lossy(payload),
        // The message that died, which its replay names (bus::REPLAY_OF): the
        // events ring reads the letter and its second chance as one message.
        "original_seq": message,
        // The durable the unit read it with: a replay goes ahead only while its
        // filter still takes `original_subject` (ADR-0015 Amendment C).
        "consumer": consumer,
    });
    park(js, envelope, unit, subject, attempts, version, trace)
}

/// Park an emit that a stream refused, when no redelivery of the message
/// behind it could send it again: the alert of a detect unit, whose engine has
/// taken that message in. `subject` is where the emit was going, and its
/// replay publishes it there, for every reader, not for the unit alone.
pub fn emit_to_dlq(
    js: &bus::JetStream,
    unit: &str,
    subject: &str,
    last_error: &str,
    payload: &[u8],
    version: &str,
    trace: Option<&trace::Context>,
) -> Result<(), String> {
    ensure_dlq_stream(js);
    let now = epoch_secs();
    let envelope = serde_json::json!({
        "original_subject": subject,
        "unit": unit,
        "attempts": 1,
        "first_seen": now,
        "dead_at": now,
        "last_error": last_error,
        "version": version,
        "payload": String::from_utf8_lossy(payload),
        "emit": true,
    });
    park(js, envelope, unit, subject, 1, version, trace)
}

/// Store a letter in the DLQ and announce it. The trace the message died in
/// (ADR-0034): a replay continues it, so the death and the second chance read
/// as one story. Never at the cost of the letter: if its 70 bytes push the
/// envelope past the server's limit, the letter goes without it. Nor does a
/// payload near that limit cost the letter: it is cut until the letter fits,
/// and the letter says so, since what it holds can be read but not replayed.
fn park(
    js: &bus::JetStream,
    mut envelope: Value,
    unit: &str,
    subject: &str,
    attempts: i64,
    version: &str,
    trace: Option<&trace::Context>,
) -> Result<(), String> {
    let limit = js.max_payload();
    let mut body = serde_json::to_vec(&envelope).map_err(|e| e.to_string())?;
    while body.len() > limit {
        let text = envelope["payload"].as_str().unwrap_or_default();
        if text.is_empty() {
            return Err(format!("a letter of {} bytes without its payload is over the server's limit", body.len()));
        }
        // escaping may make the text take more room in the letter than its bytes
        let mut keep = text.len() * (limit - limit / 10) / body.len();
        while !text.is_char_boundary(keep) {
            keep -= 1;
        }
        let cut = text[..keep].to_string();
        envelope["payload"] = cut.into();
        envelope["cut"] = true.into();
        body = serde_json::to_vec(&envelope).map_err(|e| e.to_string())?;
    }
    if let Some(t) = trace {
        envelope["traceparent"] = t.traceparent().into();
        let traced = serde_json::to_vec(&envelope).map_err(|e| e.to_string())?;
        if traced.len() <= limit {
            body = traced;
        }
    }
    let dlq_subject = format!("{DLQ_ROOT}.{}", dlq_unit_token(unit));
    js.publish(&dlq_subject, body).map_err(|e| e.to_string())?;
    announce_dead_letter(js, unit, subject, attempts, version, trace);
    Ok(())
}

//
// Nothing used to tell anyone that a message died: the self-healing loop waited
// for someone to open a session. Each dead letter is now announced on
// `<root>.dlq.events`, an ordinary subject, so waking a person or an agent is a
// flow or a sink, like every other notification here (ADR-0024). The event holds
// only what the runtime wrote itself, never the payload or the error text: a dead
// letter is untrusted content, and this is what wakes things up. A reader that
// needs the letter fetches it through `vejas_dlq`.

/// Dead letters of one unit not announced yet, and the last one's coordinates.
#[derive(Default)]
struct DlqAnnouncement {
    last_sent: u64,
    count: u64,
    subject: String,
    attempts: i64,
    version: String,
    /// The trace of the last letter: the event continues it, so whatever it
    /// wakes shows up in that trace.
    trace: Option<trace::Context>,
}

static DLQ_ANNOUNCEMENTS: OnceLock<Mutex<HashMap<String, DlqAnnouncement>>> = OnceLock::new();
static DLQ_FLUSHER: AtomicBool = AtomicBool::new(false);

fn dlq_announcements() -> &'static Mutex<HashMap<String, DlqAnnouncement>> {
    DLQ_ANNOUNCEMENTS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn dlq_events_subject() -> String {
    let root = std::env::var("VEJAS_SUBJECT_ROOT").unwrap_or_else(|_| "vx".into());
    format!("{root}.dlq.events")
}

/// At most one event per unit per this many seconds, so that a burst of poison
/// wakes a reader once and not ten thousand times. 0 announces every letter.
fn dlq_event_gap() -> u64 {
    std::env::var("VEJAS_DLQ_EVENT_SECS").ok().and_then(|v| v.parse().ok()).unwrap_or(60)
}

/// The events due now. Each carries `count`, the dead letters it stands for, so
/// the window delays news without losing any.
fn take_due_dlq_events(
    pending: &mut HashMap<String, DlqAnnouncement>,
    now: u64,
    gap: u64,
) -> Vec<(Value, Option<trace::Context>)> {
    pending
        .iter_mut()
        .filter(|(_, a)| a.count > 0 && now.saturating_sub(a.last_sent) >= gap)
        .map(|(unit, a)| {
            let event = serde_json::json!({
                "transition": "dead-lettered", "ts": now, "unit": unit,
                "subject": a.subject, "attempts": a.attempts, "version": a.version,
                "count": a.count,
                "trace_id": a.trace.as_ref().map(|t| format!("{:032x}", t.trace_id)),
            });
            a.last_sent = now;
            a.count = 0;
            (event, a.trace.take())
        })
        .collect()
}

/// Best-effort, like the proposal events: the letter is stored and the caller
/// goes on to ack, whatever happens to its announcement.
fn publish_dlq_event(js: &bus::JetStream, (event, trace): &(Value, Option<trace::Context>)) {
    let body = serde_json::to_vec(event).unwrap_or_default();
    let published = match trace {
        Some(trace) => js.publish_traced(&dlq_events_subject(), body, trace),
        None => js.publish(&dlq_events_subject(), body),
    };
    if let Err(e) = published {
        eprintln!("[vejas] dead-letter event not published: {e}");
    }
}

/// What the announcement keeps of a letter's trace: the ids, not the caller's
/// `tracestate`. That is text from outside, and the event holds only what the
/// runtime wrote.
fn announced_trace(trace: Option<&trace::Context>) -> Option<trace::Context> {
    trace.map(|t| trace::Context { state: None, ..t.clone() })
}

fn announce_dead_letter(
    js: &bus::JetStream,
    unit: &str,
    subject: &str,
    attempts: i64,
    version: &str,
    trace: Option<&trace::Context>,
) {
    // A reader of these events that keeps failing dead-letters them in turn. If
    // that were announced too, the reader would feed itself forever.
    if subject == dlq_events_subject() {
        return;
    }
    let gap = dlq_event_gap();
    let due = {
        let mut pending = dlq_announcements().lock().unwrap();
        let a = pending.entry(unit.to_string()).or_default();
        a.count += 1;
        a.subject = subject.to_string();
        a.attempts = attempts;
        a.version = version.to_string();
        a.trace = announced_trace(trace);
        take_due_dlq_events(&mut pending, epoch_secs(), gap)
    };
    for event in &due {
        publish_dlq_event(js, event);
    }
    // What the window held back must still go out if no letter follows it.
    if gap > 0 && !DLQ_FLUSHER.swap(true, Ordering::SeqCst) {
        let js = js.clone();
        let flusher = thread::Builder::new().name("dlq-events".into()).spawn(move || {
            while crate::RUNNING.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_secs(1));
                let due = {
                    let mut pending = dlq_announcements().lock().unwrap();
                    take_due_dlq_events(&mut pending, epoch_secs(), gap)
                };
                for event in &due {
                    publish_dlq_event(&js, event);
                }
            }
        });
        if flusher.is_err() {
            DLQ_FLUSHER.store(false, Ordering::SeqCst);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn letter(pending: &mut HashMap<String, DlqAnnouncement>, unit: &str) {
        let a = pending.entry(unit.to_string()).or_default();
        a.count += 1;
        a.subject = "vx.in".into();
    }

    #[test]
    fn a_burst_of_dead_letters_is_one_event_that_counts_them() {
        let mut pending = HashMap::new();
        letter(&mut pending, "flow:a");
        let first = take_due_dlq_events(&mut pending, 1000, 60);
        assert_eq!(first.len(), 1, "the first letter is news at once");
        assert_eq!(first[0].0["count"], 1);
        for _ in 0..3 {
            letter(&mut pending, "flow:a");
        }
        assert!(take_due_dlq_events(&mut pending, 1030, 60).is_empty(), "inside the window");
        let held = take_due_dlq_events(&mut pending, 1060, 60);
        assert_eq!(held[0].0["count"], 3, "the window delays news, it loses none");
        assert!(take_due_dlq_events(&mut pending, 2000, 60).is_empty(), "nothing pending");
    }

    #[test]
    fn the_event_carries_the_trace_of_the_last_letter() {
        let mut pending = HashMap::new();
        letter(&mut pending, "flow:t");
        let tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let caller = trace::Context::parse(tp, Some("vendor=text from outside")).unwrap();
        let kept = announced_trace(Some(&caller)).unwrap();
        assert_eq!((kept.trace_id, kept.span_id), (caller.trace_id, caller.span_id));
        assert_eq!(kept.state, None, "the caller's tracestate is not announced");
        pending.get_mut("flow:t").unwrap().trace = Some(kept);
        let due = take_due_dlq_events(&mut pending, 1000, 0);
        assert_eq!(due[0].0["trace_id"], "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(due[0].1.as_ref().map(|t| t.traceparent()).as_deref(), Some(tp));
        letter(&mut pending, "flow:t");
        let next = take_due_dlq_events(&mut pending, 2000, 0);
        assert!(next[0].1.is_none() && next[0].0["trace_id"].is_null(), "taken once, not repeated");
    }

    #[test]
    fn units_are_windowed_apart_and_zero_means_every_letter() {
        let mut pending = HashMap::new();
        letter(&mut pending, "flow:a");
        take_due_dlq_events(&mut pending, 1000, 60);
        letter(&mut pending, "flow:a");
        letter(&mut pending, "flow:b");
        let due = take_due_dlq_events(&mut pending, 1001, 60);
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].0["unit"], "flow:b");
        assert_eq!(take_due_dlq_events(&mut pending, 1001, 0).len(), 1, "gap 0 lets flow:a through");
    }
}
