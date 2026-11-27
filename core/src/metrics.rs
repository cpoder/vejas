//! Observability: a Prometheus `/metrics` exposition and an OTLP/HTTP-JSON span
//! exporter — both hand-rolled so the runtime keeps its single-digit-MB RSS and
//! tiny binary (no `opentelemetry` crate tree). Metrics are pull-scraped; spans
//! are pushed to an OTLP collector ONLY when an endpoint is set
//! (`OTEL_EXPORTER_OTLP_ENDPOINT` or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`): no
//! exporter thread and no span built when it is not.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, RecvTimeoutError, SyncSender};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

// A hop's span is the connector contract's type (ADR-0038); the exporter
// and the trace record take it from there.
pub use vejas_connector::span::{now_nanos, Attr, Kind, Span};

// Histogram bucket upper bounds (seconds). A flow hop is ms-scale, so the
// buckets crowd the low end; the last (+Inf) is implicit in rendering.
const BUCKETS: [f64; 13] = [
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
];

#[derive(Default, Clone)]
struct Unit {
    ok: u64,
    err: u64,
    emits: u64,
    dead: u64,
    rejected: u64,
    /// Requests an http-in connector refused for their signature, by
    /// `signature::REASONS`.
    signature_failures: [u64; 4],
    /// Pull rounds the consumer loop made, and messages they returned.
    /// `messages / rounds` is the mean batch: the number that says whether a
    /// unit is paying one pull round-trip per message or per sixty-four.
    fetch_rounds: u64,
    fetch_messages: u64,
    /// Where a round's wall time goes: waiting for the pull, running the flow
    /// over the batch, and the publish-before-ack barrier (flush + acks).
    round_fetch_secs: f64,
    round_process_secs: f64,
    round_barrier_secs: f64,
    buckets: [u64; 13], // per-bucket (non-cumulative) counts
    sum: f64,
    count: u64,
    /// Detect units: snapshots taken, the stream sequence and size of the
    /// latest, the time they took, and restores (a restart that resumed from
    /// a snapshot, with the sequence it resumed at).
    snapshots: u64,
    snapshot_seq: u64,
    snapshot_bytes: u64,
    snapshot_secs: f64,
    restores: u64,
    restore_seq: u64,
}

static UNITS: OnceLock<Mutex<HashMap<String, Unit>>> = OnceLock::new();

fn units() -> &'static Mutex<HashMap<String, Unit>> {
    UNITS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// One processed event: bump the result counter, the emit counter, and the
/// latency histogram. `secs` is the in-process processing time (interpret + emit
/// buffering), not the batch flush wait — the "pure" per-event latency.
pub fn observe(unit: &str, ok: bool, emits: u64, secs: f64) {
    let mut map = units().lock().unwrap();
    let u = map.entry(unit.to_string()).or_default();
    if ok {
        u.ok += 1;
    } else {
        u.err += 1;
    }
    u.emits += emits;
    u.sum += secs;
    u.count += 1;
    for (i, bound) in BUCKETS.iter().enumerate() {
        if secs <= *bound {
            u.buckets[i] += 1;
        }
    }
}

/// Emits published by a detect unit's tick: windows that time closed while
/// nothing arrived. They count as emits, not as processed events.
pub fn observe_tick_emits(unit: &str, emits: u64) {
    let mut map = units().lock().unwrap();
    map.entry(unit.to_string()).or_default().emits += emits;
}

/// One pull round of the consumer loop and how many messages it returned.
/// Empty rounds count too: they are the idle long-poll cadence.
pub fn observe_fetch(unit: &str, messages: usize) {
    let mut map = units().lock().unwrap();
    let u = map.entry(unit.to_string()).or_default();
    u.fetch_rounds += 1;
    u.fetch_messages += messages as u64;
}

/// The wall-time breakdown of one round: pull wait, batch processing, barrier.
pub fn observe_round(unit: &str, fetch_secs: f64, process_secs: f64, barrier_secs: f64) {
    let mut map = units().lock().unwrap();
    let u = map.entry(unit.to_string()).or_default();
    u.round_fetch_secs += fetch_secs;
    u.round_process_secs += process_secs;
    u.round_barrier_secs += barrier_secs;
}

/// A detect unit took a snapshot standing for stream sequence `seq`.
pub fn observe_snapshot(unit: &str, seq: u64, bytes: usize, secs: f64) {
    let mut map = units().lock().unwrap();
    let u = map.entry(unit.to_string()).or_default();
    u.snapshots += 1;
    u.snapshot_seq = seq;
    u.snapshot_bytes = bytes as u64;
    u.snapshot_secs += secs;
}

/// A detect unit restored a snapshot and resumed its consumer at `seq`.
pub fn observe_restore(unit: &str, seq: u64) {
    let mut map = units().lock().unwrap();
    let u = map.entry(unit.to_string()).or_default();
    u.restores += 1;
    u.restore_seq = seq;
}

/// A message that was dead-lettered (ADR-0015). Kept separate from `observe`
/// because a poison event is already counted there as an error — this names the
/// subset that exhausted its deliveries and left the bus.
pub fn inc_dead_letter(unit: &str) {
    let mut map = units().lock().unwrap();
    map.entry(unit.to_string()).or_default().dead += 1;
}

/// A connection or request refused because a bound was reached, or because the
/// OS would not give a thread: the visible side of keeping the listener alive.
pub fn inc_rejected(unit: &str) {
    let mut map = units().lock().unwrap();
    map.entry(unit.to_string()).or_default().rejected += 1;
}

/// A request an http-in connector refused for its signature (ADR-0037), by its
/// reason, one of `signature::REASONS`.
pub fn inc_signature_failure(unit: &str, reason: &str) {
    let Some(at) = crate::signature::REASONS.iter().position(|r| *r == reason) else {
        return;
    };
    let mut map = units().lock().unwrap();
    map.entry(unit.to_string()).or_default().signature_failures[at] += 1;
}

fn esc(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n")
}

/// Render the full Prometheus text exposition. `gauges` are process-supervision
/// facts pulled live from the registry: (unit, kind, restarts).
pub fn render(gauges: &[(String, String, u64)]) -> String {
    let map = units().lock().unwrap();
    let mut out = String::with_capacity(4096);

    out.push_str("# HELP vejas_up 1 while the runtime is serving.\n");
    out.push_str("# TYPE vejas_up gauge\nvejas_up 1\n");

    let mut flows = 0u64;
    let mut connectors = 0u64;
    for (_, kind, _) in gauges {
        match kind.as_str() {
            "connector" => connectors += 1,
            _ => flows += 1,
        }
    }
    out.push_str("# HELP vejas_units Supervised units by kind.\n");
    out.push_str("# TYPE vejas_units gauge\n");
    out.push_str(&format!("vejas_units{{kind=\"flow\"}} {flows}\n"));
    out.push_str(&format!("vejas_units{{kind=\"connector\"}} {connectors}\n"));

    out.push_str("# HELP vejas_flow_restarts Supervisor restarts of a unit since it was (re)loaded.\n");
    out.push_str("# TYPE vejas_flow_restarts gauge\n");
    for (unit, _, restarts) in gauges {
        out.push_str(&format!(
            "vejas_flow_restarts{{unit=\"{}\"}} {restarts}\n",
            esc(unit)
        ));
    }

    out.push_str("# HELP vejas_events_processed_total Events a unit processed, by result.\n");
    out.push_str("# TYPE vejas_events_processed_total counter\n");
    for (unit, u) in map.iter() {
        let e = esc(unit);
        out.push_str(&format!(
            "vejas_events_processed_total{{unit=\"{e}\",result=\"ok\"}} {}\n",
            u.ok
        ));
        out.push_str(&format!(
            "vejas_events_processed_total{{unit=\"{e}\",result=\"error\"}} {}\n",
            u.err
        ));
    }

    out.push_str("# HELP vejas_fetch_rounds_total Pull rounds a unit's consumer loop made.\n");
    out.push_str("# TYPE vejas_fetch_rounds_total counter\n");
    for (unit, u) in map.iter() {
        out.push_str(&format!(
            "vejas_fetch_rounds_total{{unit=\"{}\"}} {}\n",
            esc(unit),
            u.fetch_rounds
        ));
    }
    out.push_str("# HELP vejas_fetch_messages_total Messages those pull rounds returned.\n");
    out.push_str("# TYPE vejas_fetch_messages_total counter\n");
    for (unit, u) in map.iter() {
        out.push_str(&format!(
            "vejas_fetch_messages_total{{unit=\"{}\"}} {}\n",
            esc(unit),
            u.fetch_messages
        ));
    }

    out.push_str("# HELP vejas_snapshots_total Snapshots a detect unit took of its engine state.\n");
    out.push_str("# TYPE vejas_snapshots_total counter\n");
    for (unit, u) in map.iter() {
        out.push_str(&format!("vejas_snapshots_total{{unit=\"{}\"}} {}\n", esc(unit), u.snapshots));
    }
    out.push_str("# HELP vejas_snapshot_seq Stream sequence the unit's latest snapshot stands for: everything up to it is in the state, everything after it replays on restart.\n");
    out.push_str("# TYPE vejas_snapshot_seq gauge\n");
    for (unit, u) in map.iter() {
        out.push_str(&format!("vejas_snapshot_seq{{unit=\"{}\"}} {}\n", esc(unit), u.snapshot_seq));
    }
    out.push_str("# HELP vejas_snapshot_bytes Size of the unit's latest snapshot.\n");
    out.push_str("# TYPE vejas_snapshot_bytes gauge\n");
    for (unit, u) in map.iter() {
        out.push_str(&format!("vejas_snapshot_bytes{{unit=\"{}\"}} {}\n", esc(unit), u.snapshot_bytes));
    }
    out.push_str("# HELP vejas_snapshot_seconds_sum Time spent taking and storing snapshots.\n");
    out.push_str("# TYPE vejas_snapshot_seconds_sum counter\n");
    for (unit, u) in map.iter() {
        out.push_str(&format!("vejas_snapshot_seconds_sum{{unit=\"{}\"}} {:.6}\n", esc(unit), u.snapshot_secs));
    }
    out.push_str("# HELP vejas_restores_total Restarts that restored a snapshot and resumed the consumer after it.\n");
    out.push_str("# TYPE vejas_restores_total counter\n");
    for (unit, u) in map.iter() {
        out.push_str(&format!("vejas_restores_total{{unit=\"{}\"}} {}\n", esc(unit), u.restores));
    }
    out.push_str("# HELP vejas_restore_seq Stream sequence the unit's last restore resumed at.\n");
    out.push_str("# TYPE vejas_restore_seq gauge\n");
    for (unit, u) in map.iter() {
        out.push_str(&format!("vejas_restore_seq{{unit=\"{}\"}} {}\n", esc(unit), u.restore_seq));
    }

    out.push_str("# HELP vejas_round_seconds_sum Wall time of the consumer loop by phase: fetch (wait for the first message of a round), process, barrier (flush + acks).\n");
    out.push_str("# TYPE vejas_round_seconds_sum counter\n");
    for (unit, u) in map.iter() {
        let e = esc(unit);
        out.push_str(&format!(
            "vejas_round_seconds_sum{{unit=\"{e}\",phase=\"fetch\"}} {}\n",
            u.round_fetch_secs
        ));
        out.push_str(&format!(
            "vejas_round_seconds_sum{{unit=\"{e}\",phase=\"process\"}} {}\n",
            u.round_process_secs
        ));
        out.push_str(&format!(
            "vejas_round_seconds_sum{{unit=\"{e}\",phase=\"barrier\"}} {}\n",
            u.round_barrier_secs
        ));
    }

    out.push_str("# HELP vejas_emits_published_total Messages a unit published to the bus.\n");
    out.push_str("# TYPE vejas_emits_published_total counter\n");
    for (unit, u) in map.iter() {
        out.push_str(&format!(
            "vejas_emits_published_total{{unit=\"{}\"}} {}\n",
            esc(unit),
            u.emits
        ));
    }

    out.push_str("# HELP vejas_http_rejected_total Connections or requests refused past a bound, or for want of a thread.\n");
    out.push_str("# TYPE vejas_http_rejected_total counter\n");
    for (unit, u) in map.iter().filter(|(_, u)| u.rejected > 0) {
        out.push_str(&format!(
            "vejas_http_rejected_total{{unit=\"{}\"}} {}\n",
            esc(unit),
            u.rejected
        ));
    }
    out.push_str("# HELP vejas_signature_failures_total Requests an http-in connector refused for their signature, by reason.\n");
    out.push_str("# TYPE vejas_signature_failures_total counter\n");
    for (unit, u) in map.iter() {
        for (reason, n) in crate::signature::REASONS.iter().zip(u.signature_failures) {
            if n > 0 {
                out.push_str(&format!(
                    "vejas_signature_failures_total{{unit=\"{}\",reason=\"{reason}\"}} {n}\n",
                    esc(unit)
                ));
            }
        }
    }
    out.push_str("# HELP vejas_dead_letters_total Messages a unit dead-lettered (ADR-0015).\n");
    out.push_str("# TYPE vejas_dead_letters_total counter\n");
    for (unit, u) in map.iter() {
        out.push_str(&format!(
            "vejas_dead_letters_total{{unit=\"{}\"}} {}\n",
            esc(unit),
            u.dead
        ));
    }

    out.push_str("# HELP vejas_spans_exported_total Spans the OTLP exporter delivered to the collector.\n");
    out.push_str("# TYPE vejas_spans_exported_total counter\n");
    out.push_str(&format!(
        "vejas_spans_exported_total {}\n",
        SPANS_EXPORTED.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP vejas_spans_dropped_total Spans lost before the collector had them: the export queue was full, or the export failed after its retries.\n");
    out.push_str("# TYPE vejas_spans_dropped_total counter\n");
    out.push_str(&format!(
        "vejas_spans_dropped_total{{reason=\"queue_full\"}} {}\n",
        SPANS_DROPPED_QUEUE.load(Ordering::Relaxed)
    ));
    out.push_str(&format!(
        "vejas_spans_dropped_total{{reason=\"export_failed\"}} {}\n",
        SPANS_DROPPED_EXPORT.load(Ordering::Relaxed)
    ));
    let (kept, queue_full, publish_failed) = crate::record::counters();
    out.push_str("# HELP vejas_trace_records_total Hop records the VEJAS_TRACES stream acknowledged (ADR-0034).\n");
    out.push_str("# TYPE vejas_trace_records_total counter\n");
    out.push_str(&format!("vejas_trace_records_total {kept}\n"));
    out.push_str("# HELP vejas_trace_records_dropped_total Hop records lost: the record's queue was full (by count or by bytes), or the stream did not take them.\n");
    out.push_str("# TYPE vejas_trace_records_dropped_total counter\n");
    out.push_str(&format!("vejas_trace_records_dropped_total{{reason=\"queue_full\"}} {queue_full}\n"));
    out.push_str(&format!("vejas_trace_records_dropped_total{{reason=\"publish_failed\"}} {publish_failed}\n"));

    out.push_str("# HELP vejas_event_duration_seconds In-process per-event processing time.\n");
    out.push_str("# TYPE vejas_event_duration_seconds histogram\n");
    for (unit, u) in map.iter() {
        let e = esc(unit);
        // `observe` already stores each bucket cumulatively (it bumps every bound
        // ≥ the sample), so buckets[i] is the Prometheus `le` count directly.
        for (i, bound) in BUCKETS.iter().enumerate() {
            out.push_str(&format!(
                "vejas_event_duration_seconds_bucket{{unit=\"{e}\",le=\"{bound}\"}} {}\n",
                u.buckets[i]
            ));
        }
        out.push_str(&format!(
            "vejas_event_duration_seconds_bucket{{unit=\"{e}\",le=\"+Inf\"}} {}\n",
            u.count
        ));
        out.push_str(&format!(
            "vejas_event_duration_seconds_sum{{unit=\"{e}\"}} {}\n",
            u.sum
        ));
        out.push_str(&format!(
            "vejas_event_duration_seconds_count{{unit=\"{e}\"}} {}\n",
            u.count
        ));
    }
    out
}

// ───────────────────────── OTLP/HTTP-JSON trace export ─────────────────────────
//
// One span per hop, with the parent the trace context names (ADR-0034): the
// source that let an event in, each flow it crossed, each `invoke`, each sink
// that delivered it, all under one trace id. Spans are built only while an
// exporter is configured (`tracing()`), so the hot path pays nothing for them
// otherwise; the context itself travels either way (see `trace.rs`).

static TX: OnceLock<Option<SyncSender<Span>>> = OnceLock::new();
static SPANS_EXPORTED: AtomicU64 = AtomicU64::new(0);
static SPANS_DROPPED_QUEUE: AtomicU64 = AtomicU64::new(0);
static SPANS_DROPPED_EXPORT: AtomicU64 = AtomicU64::new(0);

/// Is an exporter configured? The spans of `invoke`s go only there, so their
/// call sites test this; without one the hot path builds none (ADR-0016).
pub fn tracing() -> bool {
    matches!(TX.get(), Some(Some(_)))
}

/// Does anything take a hop's span: the exporter or the trace record
/// (ADR-0034)? Hop call sites test this before building one.
pub fn observing() -> bool {
    tracing() || crate::record::on()
}

/// Is this hop's span worth building: the exporter or the record takes it, and
/// the sampler kept the trace or the hop failed (failures are always kept).
pub fn wants(sampled: bool, failed: bool) -> bool {
    (sampled || failed) && observing()
}

/// Where and how spans go: the OTEL_* variables the specification defines for
/// an OTLP/HTTP exporter, the traces-specific ones first.
struct ExportConfig {
    url: String,
    headers: Vec<(String, String)>,
    timeout: Duration,
    resource: Vec<(String, String)>,
    /// The batch span processor's knobs, under the specification's names.
    delay: Duration,
    batch: usize,
    queue: usize,
}

fn export_config(env: impl Fn(&str) -> Option<String>) -> Option<ExportConfig> {
    let var = |k: &str| env(k).filter(|v| !v.trim().is_empty());
    // The traces endpoint is used as given; the generic one gets the signal path.
    let url = match var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") {
        Some(url) => url.trim().to_string(),
        None => format!(
            "{}/v1/traces",
            var("OTEL_EXPORTER_OTLP_ENDPOINT")?.trim().trim_end_matches('/')
        ),
    };
    let headers = ["OTEL_EXPORTER_OTLP_TRACES_HEADERS", "OTEL_EXPORTER_OTLP_HEADERS"]
        .into_iter()
        .find_map(|name| var(name).map(|h| sendable_headers(key_values(&h, name), name)))
        .unwrap_or_default();
    let timeout = var("OTEL_EXPORTER_OTLP_TRACES_TIMEOUT")
        .or_else(|| var("OTEL_EXPORTER_OTLP_TIMEOUT"))
        .and_then(|ms| ms.trim().parse::<u64>().ok())
        .unwrap_or(10_000);
    let mut resource: Vec<(String, String)> = var("OTEL_RESOURCE_ATTRIBUTES")
        .map(|r| key_values(&r, "OTEL_RESOURCE_ATTRIBUTES"))
        .unwrap_or_default();
    let service = var("OTEL_SERVICE_NAME")
        .or_else(|| resource.iter().find(|(k, _)| k == "service.name").map(|(_, v)| v.clone()))
        .unwrap_or_else(|| "vejas".to_string());
    resource.retain(|(k, _)| k != "service.name" && k != "service.version");
    resource.insert(0, ("service.name".into(), service));
    resource.insert(1, ("service.version".into(), env!("CARGO_PKG_VERSION").into()));
    let number = |k: &str, default: u64| var(k).and_then(|v| v.trim().parse::<u64>().ok()).unwrap_or(default);
    // 1 s rather than the specification's 5 s, so that a trace shows up while
    // its sender still looks; under load a full batch leaves at once anyway.
    let delay = Duration::from_millis(number("OTEL_BSP_SCHEDULE_DELAY", 1000));
    let queue = number("OTEL_BSP_MAX_QUEUE_SIZE", 4096).clamp(1, 1 << 20) as usize;
    let batch = (number("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", 512).max(1) as usize).min(queue);
    Some(ExportConfig {
        url,
        headers,
        timeout: Duration::from_millis(timeout.max(1)),
        resource,
        delay,
        batch,
        queue,
    })
}

/// `k=v,k2=v2` with percent-encoded values, the format of the OTEL_* lists. A
/// malformed entry is skipped with a warning that names the variable, never
/// the value: header values are credentials.
fn key_values(list: &str, variable: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for entry in list.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        match entry.split_once('=') {
            Some((k, v)) if !k.trim().is_empty() => {
                out.push((percent_decode(k.trim()), percent_decode(v.trim())))
            }
            _ => eprintln!("[vejas] {variable}: an entry without `key=` is skipped"),
        }
    }
    out
}

/// The headers an HTTP request can carry (a token for a name, visible ASCII
/// for a value). Any other is skipped here, once, with a warning that names
/// the variable and the entry's position only: sent, it would fail every
/// export, and the client's error quotes the whole header line.
fn sendable_headers(headers: Vec<(String, String)>, variable: &str) -> Vec<(String, String)> {
    let token = |b: u8| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b);
    let visible = |b: u8| b == b'\t' || (0x20..=0x7e).contains(&b);
    let mut kept = Vec::with_capacity(headers.len());
    for (n, (k, v)) in headers.into_iter().enumerate() {
        if !k.is_empty() && k.bytes().all(token) && v.bytes().all(visible) {
            kept.push((k, v));
        } else {
            eprintln!(
                "[vejas] {variable}: entry {} is not a valid HTTP header and is skipped (not shown: it may hold a credential)",
                n + 1
            );
        }
    }
    kept
}

/// Where spans go, for the log: scheme, host and path, without the user
/// info or query that may carry a credential.
fn shown(url: &str) -> String {
    let (scheme, rest) = url.split_once("://").map_or(("", url), |(s, r)| (s, r));
    let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (authority, path) = rest.split_at(end);
    let host = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    let path = path.split(['?', '#']).next().unwrap_or("");
    if scheme.is_empty() {
        format!("{host}{path}")
    } else {
        format!("{scheme}://{host}{path}")
    }
}

pub(crate) fn percent_decode(s: &str) -> String {
    let hex = |c: u8| (c as char).to_digit(16).map(|d| d as u8);
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let (Some(hi), Some(lo)) = (hex(b[i + 1]), hex(b[i + 2])) {
                out.push(hi << 4 | lo);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Spawn the exporter thread iff an OTLP endpoint is configured. Idempotent
/// (the OnceLock guards it). Without one, TX holds None, `tracing()` is false
/// and `span()` is a no-op.
pub fn otlp_init() {
    TX.get_or_init(|| {
        let config = export_config(|k| std::env::var(k).ok())?;
        // Bounded queue: if the collector stalls, spans are dropped (and
        // counted), never buffered without bound, and the hot loop never waits.
        let (tx, rx) = sync_channel::<Span>(config.queue);
        eprintln!("[vejas] OTLP traces -> {}", shown(&config.url));
        let spawned = std::thread::Builder::new().name("otlp-export".into()).spawn(move || {
            // no redirect: ureq would take OTEL_EXPORTER_OTLP_HEADERS, an API
            // key among them, to wherever the collector's answer points
            let agent = ureq::AgentBuilder::new().timeout(config.timeout).redirects(0).build();
            let mut batch: Vec<Span> = Vec::with_capacity(config.batch);
            let mut oldest: Option<Instant> = None;
            let mut last_report: Option<Instant> = None;
            loop {
                let flush = FLUSH_ASKED.load(Ordering::SeqCst);
                if batch.is_empty() && flush <= FLUSH_DONE.load(Ordering::SeqCst) {
                    // Nothing pending: wait on the queue itself, so an idle
                    // runtime leaves this thread asleep. Only the first span
                    // after a quiet spell pays for waking it.
                    match rx.recv_timeout(IDLE_WAIT) {
                        Ok(s) => {
                            batch.push(s);
                            oldest = Some(Instant::now());
                        }
                        Err(RecvTimeoutError::Timeout) => continue,
                        Err(RecvTimeoutError::Disconnected) => break,
                    }
                }
                // While spans are pending, take what is queued without waiting
                // on the queue: a producer
                // never has to wake this thread, so a span costs the hot path
                // one push. A full batch leaves at once.
                while let Ok(s) = rx.try_recv() {
                    batch.push(s);
                    oldest.get_or_insert_with(Instant::now);
                    if batch.len() >= config.batch {
                        ship(&agent, &config, &mut batch, &mut last_report);
                        oldest = None;
                    }
                }
                let flushing = flush > FLUSH_DONE.load(Ordering::SeqCst);
                let due = oldest.is_some_and(|t| t.elapsed() >= config.delay);
                if !batch.is_empty() && (flushing || due) {
                    ship(&agent, &config, &mut batch, &mut last_report);
                    oldest = None;
                }
                if flushing {
                    FLUSH_DONE.store(flush, Ordering::SeqCst);
                }
                std::thread::sleep(EXPORT_TICK);
            }
        });
        match spawned {
            Ok(_) => Some(tx),
            Err(e) => {
                eprintln!("[vejas] OTLP exporter not started: {e}");
                None
            }
        }
    });
}

/// How often the exporter looks at its queue while spans are pending. At
/// 25 ms a 4096-span queue holds 160 000 spans a second.
const EXPORT_TICK: Duration = Duration::from_millis(25);
/// How long an idle exporter sleeps on its queue before it looks for a flush
/// request: a graceful stop with nothing to export waits at most this long.
const IDLE_WAIT: Duration = Duration::from_millis(250);
static FLUSH_ASKED: AtomicU64 = AtomicU64::new(0);
static FLUSH_DONE: AtomicU64 = AtomicU64::new(0);

/// Export what is queued now and wait for it, at most `within`. Called on a
/// graceful stop: the spans of the last moments before a restart are the
/// ones an operator comes looking for.
pub fn otlp_flush(within: Duration) {
    if !tracing() {
        return;
    }
    let ticket = FLUSH_ASKED.fetch_add(1, Ordering::SeqCst) + 1;
    let start = Instant::now();
    while FLUSH_DONE.load(Ordering::SeqCst) < ticket && start.elapsed() < within {
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Export one batch, count the outcome, and say so in the log at most once a
/// minute: a dead collector must not flood it.
fn ship(agent: &ureq::Agent, config: &ExportConfig, batch: &mut Vec<Span>, last_report: &mut Option<Instant>) {
    let body = otlp_payload(&config.resource, batch);
    let n = batch.len() as u64;
    batch.clear();
    let failure = match export(agent, config, &body) {
        Ok(0) => {
            SPANS_EXPORTED.fetch_add(n, Ordering::Relaxed);
            None
        }
        Ok(rejected) => {
            let rejected = rejected.min(n);
            SPANS_EXPORTED.fetch_add(n - rejected, Ordering::Relaxed);
            SPANS_DROPPED_EXPORT.fetch_add(rejected, Ordering::Relaxed);
            Some(format!("the collector rejected {rejected} of {n} spans"))
        }
        Err(e) => {
            SPANS_DROPPED_EXPORT.fetch_add(n, Ordering::Relaxed);
            Some(format!("{e}; {n} spans dropped"))
        }
    };
    if let Some(why) = failure {
        if last_report.is_none_or(|t| t.elapsed() >= Duration::from_secs(60)) {
            eprintln!("[vejas] OTLP export: {why} (vejas_spans_dropped_total counts them all)");
            *last_report = Some(Instant::now());
        }
    }
}

/// One POST, retried with backoff on what the OTLP specification calls
/// retryable (connection errors, 429, 502, 503, 504) and honouring
/// `Retry-After`. Ok carries the spans the collector said it rejected.
fn export(agent: &ureq::Agent, config: &ExportConfig, body: &str) -> Result<u64, String> {
    const ATTEMPTS: u32 = 4;
    let mut backoff = Duration::from_millis(500);
    for attempt in 1..=ATTEMPTS {
        let mut request = agent.post(&config.url).set("content-type", "application/json");
        for (k, v) in &config.headers {
            request = request.set(k, v);
        }
        let wait = match request.send_string(body) {
            Ok(response) if (200..300).contains(&response.status()) => {
                return Ok(rejected_spans(&response.into_string().unwrap_or_default()))
            }
            // with no redirect followed, a 3xx comes back as an answer: the
            // spans were not taken
            Ok(response) => {
                let code = response.status();
                return Err(match code {
                    301 | 302 | 303 | 307 | 308 => format!(
                        "HTTP {code} from {}: a redirect, not followed, since the headers would go along: \
                         set OTEL_EXPORTER_OTLP_ENDPOINT to the address it points to",
                        crate::connectors::host_of(&config.url)
                    ),
                    _ => format!("HTTP {code}"),
                });
            }
            Err(ureq::Error::Status(code, response)) if matches!(code, 429 | 502 | 503 | 504) => {
                if attempt == ATTEMPTS {
                    return Err(format!("HTTP {code} after {ATTEMPTS} attempts"));
                }
                response
                    .header("retry-after")
                    .and_then(|s| s.trim().parse::<u64>().ok())
                    .map(|secs| Duration::from_secs(secs.min(30)))
                    .unwrap_or(backoff)
            }
            Err(ureq::Error::Status(code, _)) => return Err(format!("HTTP {code}")),
            Err(ureq::Error::Transport(t)) => {
                use ureq::ErrorKind::{ConnectionFailed, Dns, Io, ProxyConnect};
                // Only the network clears on its own; a bad header or URL does
                // not, so it is not retried.
                let transient = matches!(t.kind(), ConnectionFailed | Dns | Io | ProxyConnect);
                if !transient || attempt == ATTEMPTS {
                    // The kind, and the detail only where it is the network's:
                    // a bad header's detail is the whole header line, which may
                    // be a credential, and the Display prints the URL.
                    return Err(match (transient, t.message()) {
                        (true, Some(detail)) => format!("{:?}: {detail} after {ATTEMPTS} attempts", t.kind()),
                        (true, None) => format!("{:?} after {ATTEMPTS} attempts", t.kind()),
                        (false, _) => format!("{:?}", t.kind()),
                    });
                }
                backoff
            }
        };
        let jitter = Duration::from_millis(crate::trace::span_id() % 250);
        std::thread::sleep(wait + jitter);
        backoff *= 2;
    }
    unreachable!("the last attempt returns")
}

/// `partialSuccess.rejectedSpans` of an OTLP answer, 0 when absent.
fn rejected_spans(answer: &str) -> u64 {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(answer) else {
        return 0;
    };
    let r = &v["partialSuccess"]["rejectedSpans"];
    r.as_u64().or_else(|| r.as_str().and_then(|s| s.parse().ok())).unwrap_or(0)
}

/// Queue a finished span for export. A no-op without an exporter; never
/// blocks: a full queue drops the span (counted) rather than stall the caller.
/// A span the sampler left out is not exported, unless it failed: errors and
/// dead letters are always exported, whatever the ratio.
pub fn span(s: Span) {
    if let Some(Some(tx)) = TX.get() {
        if !s.sampled && s.error.is_none() {
            return;
        }
        if tx.try_send(s).is_err() {
            SPANS_DROPPED_QUEUE.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// The OTLP/HTTP JSON body for a batch, written straight into one string: the
/// exporter thread runs this for every span, and a `serde_json::Value` tree per
/// span cost several times the write.
fn otlp_payload(resource: &[(String, String)], spans: &[Span]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(256 + spans.len() * 640);
    out.push_str(r#"{"resourceSpans":[{"resource":{"attributes":["#);
    for (i, (k, v)) in resource.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        attribute(&mut out, k, &Attr::Str(v.clone()));
    }
    out.push_str(r#"]},"scopeSpans":[{"scope":{"name":"vejas.runtime","version":"#);
    json_string(&mut out, env!("CARGO_PKG_VERSION"));
    out.push_str(r#"},"spans":["#);
    for (i, s) in spans.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        let _ = write!(out, r#"{{"traceId":"{:032x}","spanId":"{:016x}""#, s.trace_id, s.span_id);
        if let Some(parent) = s.parent {
            let _ = write!(out, r#","parentSpanId":"{parent:016x}""#);
        }
        out.push_str(r#","name":"#);
        json_string(&mut out, &s.name);
        let _ = write!(
            out,
            r#","kind":{},"startTimeUnixNano":"{}","endTimeUnixNano":"{}","attributes":["#,
            s.kind as u8,
            s.start_nanos,
            s.end_nanos.max(s.start_nanos)
        );
        for (j, (k, v)) in s.attrs.iter().enumerate() {
            if j > 0 {
                out.push(',');
            }
            attribute(&mut out, k, v);
        }
        match &s.error {
            Some(e) => {
                let message: String = e.chars().take(1024).collect();
                if !s.attrs.is_empty() {
                    out.push(',');
                }
                attribute(&mut out, "error.type", &Attr::Static("_OTHER"));
                out.push(',');
                attribute(&mut out, "error.message", &Attr::Str(message.clone()));
                // status code 2 = ERROR; a success leaves it unset.
                out.push_str(r#"],"status":{"code":2,"message":"#);
                json_string(&mut out, &message);
                out.push_str("}}");
            }
            None => out.push_str("]}"),
        }
    }
    out.push_str("]}]}]}");
    out
}

fn attribute(out: &mut String, key: &str, value: &Attr) {
    out.push_str(r#"{"key":"#);
    json_string(out, key);
    out.push_str(r#","value":{"#);
    match value {
        Attr::Str(s) => {
            out.push_str(r#""stringValue":"#);
            json_string(out, s);
        }
        Attr::Static(s) => {
            out.push_str(r#""stringValue":"#);
            json_string(out, s);
        }
        Attr::Computed(s) => {
            out.push_str(r#""stringValue":"#);
            json_string(out, s);
        }
        // int64 travels as a decimal string in OTLP/JSON (proto3 mapping).
        Attr::Int(n) => {
            out.push_str(r#""intValue":""#);
            out.push_str(&n.to_string());
            out.push('"');
        }
        Attr::Bool(b) => out.push_str(if *b { r#""boolValue":true"# } else { r#""boolValue":false"# }),
    }
    out.push_str("}}");
}

/// A JSON string literal, escaped as RFC 8259 requires. The runs between two
/// escapes are copied whole: every byte it escapes is ASCII, so a cut there is
/// always on a character boundary.
pub(crate) fn json_string(out: &mut String, s: &str) {
    use std::fmt::Write as _;
    out.push('"');
    let mut from = 0;
    for (i, b) in s.bytes().enumerate() {
        let escaped = match b {
            b'"' => "\\\"",
            b'\\' => "\\\\",
            b'\n' => "\\n",
            b'\r' => "\\r",
            b'\t' => "\\t",
            0..=0x1f => "",
            _ => continue,
        };
        out.push_str(&s[from..i]);
        if escaped.is_empty() {
            let _ = write!(out, "\\u{b:04x}");
        } else {
            out.push_str(escaped);
        }
        from = i + 1;
    }
    out.push_str(&s[from..]);
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_emits_prometheus_shape() {
        observe("flow:t", true, 2, 0.003);
        observe("flow:t", false, 0, 0.02);
        inc_dead_letter("flow:t");
        inc_rejected("flow:t");
        let text = render(&[("flow:t".into(), "flow".into(), 1)]);
        assert!(text.contains("vejas_up 1"));
        assert!(text.contains("vejas_events_processed_total{unit=\"flow:t\",result=\"ok\"} 1"));
        assert!(text.contains("vejas_events_processed_total{unit=\"flow:t\",result=\"error\"} 1"));
        assert!(text.contains("vejas_emits_published_total{unit=\"flow:t\"} 2"));
        assert!(text.contains("vejas_dead_letters_total{unit=\"flow:t\"} 1"));
        assert!(text.contains("vejas_http_rejected_total{unit=\"flow:t\"} 1"));
        assert!(text.contains("vejas_event_duration_seconds_count{unit=\"flow:t\"} 2"));
        assert!(text.contains("le=\"+Inf\"} 2"));
        assert!(text.contains("vejas_flow_restarts{unit=\"flow:t\"} 1"));
        // A reason the counter does not know is not counted, not a panic.
        inc_signature_failure("connector:hook", "mismatch");
        inc_signature_failure("connector:hook", "mismatch");
        inc_signature_failure("connector:hook", "stale");
        inc_signature_failure("connector:hook", "bogus");
        let text = render(&[]);
        assert!(text.contains("vejas_signature_failures_total{unit=\"connector:hook\",reason=\"mismatch\"} 2"));
        assert!(text.contains("vejas_signature_failures_total{unit=\"connector:hook\",reason=\"stale\"} 1"));
        assert!(!text.contains("reason=\"missing\""), "a reason never seen is not rendered");
        assert!(!text.contains("bogus"));
        // Cumulative buckets must be monotone and top out at count (2), never
        // exceed it — guards the double-accumulation bug. 0.003 lands in ≤0.005,
        // 0.02 in ≤0.025: nothing ≤0.001, one ≤0.005, both ≤0.025.
        assert!(text.contains("le=\"0.001\"} 0"));
        assert!(text.contains("le=\"0.005\"} 1"));
        assert!(text.contains("le=\"0.025\"} 2"));
        assert!(text.contains("le=\"5\"} 2"));
    }

    #[test]
    fn otlp_payload_is_valid_otlp_json() {
        let incoming = crate::trace::Context::parse(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            None,
        );
        let hop = crate::trace::Hop::begin(incoming);
        let failed = Span::hop(&hop, Kind::Consumer, "flow:x", 1)
            .messaging("process", "vx.a.b")
            .attr("vejas.emits", 3u64)
            .failed("boom");
        let child = hop.child();
        let ok = Span::hop(&child, Kind::Internal, "invoke p:s", 2);
        let resource = vec![("service.name".to_string(), "vejas".to_string())];
        let body = otlp_payload(&resource, &[failed, ok]);
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        let spans = &v["resourceSpans"][0]["scopeSpans"][0]["spans"];
        let (first, second) = (&spans[0], &spans[1]);
        assert_eq!(first["name"], "flow:x");
        assert_eq!(first["kind"], 5);
        assert_eq!(first["status"]["code"], 2);
        assert_eq!(first["traceId"], "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(first["parentSpanId"], "00f067aa0ba902b7");
        assert_eq!(first["spanId"].as_str().unwrap().len(), 16);
        let attr = |span: &serde_json::Value, key: &str| {
            span["attributes"]
                .as_array()
                .unwrap()
                .iter()
                .find(|a| a["key"] == key)
                .map(|a| a["value"].clone())
        };
        assert_eq!(attr(first, "messaging.destination.name").unwrap()["stringValue"], "vx.a.b");
        assert_eq!(attr(first, "vejas.emits").unwrap()["intValue"], "3");
        assert_eq!(attr(first, "error.message").unwrap()["stringValue"], "boom");
        // The child hangs under the hop, in the same trace; a success leaves
        // the status unset.
        assert_eq!(second["traceId"], first["traceId"]);
        assert_eq!(second["parentSpanId"], first["spanId"]);
        assert!(second.get("status").is_none());
        assert_eq!(v["resourceSpans"][0]["resource"]["attributes"][0]["value"]["stringValue"], "vejas");
    }

    #[test]
    fn the_payload_stays_json_whatever_the_strings_hold() {
        let hop = crate::trace::Hop::begin(None);
        let nasty = "a \"quoted\" \\ back\nline\ttab\u{1}ctl é ✓";
        let span = Span::hop(&hop, Kind::Client, nasty, 5)
            .attr("vejas.note", nasty)
            .attr("vejas.ok", true)
            .attr("vejas.n", -3i64)
            .failed(nasty);
        let body = otlp_payload(&[("service.name".into(), nasty.into())], &[span]);
        let v: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");
        let s = &v["resourceSpans"][0]["scopeSpans"][0]["spans"][0];
        assert_eq!(s["name"], nasty);
        assert_eq!(s["status"]["message"], nasty);
        assert!(s.get("parentSpanId").is_none(), "a root has no parent");
        let attrs = s["attributes"].as_array().unwrap();
        let value = |k: &str| attrs.iter().find(|a| a["key"] == k).unwrap()["value"].clone();
        assert_eq!(value("vejas.note")["stringValue"], nasty);
        assert_eq!(value("vejas.ok")["boolValue"], true);
        assert_eq!(value("vejas.n")["intValue"], "-3");
        assert_eq!(value("error.type")["stringValue"], "_OTHER");
        assert_eq!(v["resourceSpans"][0]["resource"]["attributes"][0]["value"]["stringValue"], nasty);
        // An empty batch is still a valid request body.
        let empty: serde_json::Value = serde_json::from_str(&otlp_payload(&[], &[])).unwrap();
        assert_eq!(empty["resourceSpans"][0]["scopeSpans"][0]["spans"], serde_json::json!([]));
    }

    #[test]
    fn neither_a_bad_header_nor_a_credential_in_a_url_reaches_the_log() {
        let config = export_config(|k: &str| match k {
            "OTEL_EXPORTER_OTLP_ENDPOINT" => Some("http://c:4318".into()),
            // a colon where `=` belongs, a control byte, then a good one
            "OTEL_EXPORTER_OTLP_HEADERS" => {
                Some("Authorization: Basic dXNlcjpwYXNzd29yZA==,x-bad=a%0Ab,api-key=s%3Dcret".into())
            }
            _ => None,
        })
        .unwrap();
        assert_eq!(config.headers, vec![("api-key".to_string(), "s=cret".to_string())]);
        assert_eq!(shown("https://user:tok@collector:4318/v1/traces?key=abc"), "https://collector:4318/v1/traces");
        assert_eq!(shown("http://c:4318/v1/traces"), "http://c:4318/v1/traces");
        assert_eq!(shown("c:4318"), "c:4318");
    }

    #[test]
    fn the_exporter_reads_the_otel_variables() {
        let env = |pairs: &'static [(&'static str, &'static str)]| {
            move |k: &str| pairs.iter().find(|(n, _)| *n == k).map(|(_, v)| v.to_string())
        };
        assert!(export_config(env(&[])).is_none(), "no endpoint, no exporter");
        let generic = export_config(env(&[("OTEL_EXPORTER_OTLP_ENDPOINT", "http://c:4318/")])).unwrap();
        assert_eq!(generic.url, "http://c:4318/v1/traces");
        assert_eq!(generic.timeout, Duration::from_secs(10));
        assert_eq!((generic.delay, generic.batch, generic.queue), (Duration::from_secs(1), 512, 4096));
        let tuned = export_config(env(&[
            ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://c:4318"),
            ("OTEL_BSP_SCHEDULE_DELAY", "200"),
            ("OTEL_BSP_MAX_QUEUE_SIZE", "100"),
            ("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", "1000"),
        ]))
        .unwrap();
        assert_eq!(tuned.delay, Duration::from_millis(200));
        assert_eq!((tuned.batch, tuned.queue), (100, 100), "a batch never exceeds the queue");
        let specific = export_config(env(&[
            ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://c:4318"),
            ("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "https://t.example/otlp/traces"),
            ("OTEL_EXPORTER_OTLP_HEADERS", "x-generic=no"),
            ("OTEL_EXPORTER_OTLP_TRACES_HEADERS", "authorization=Basic%20abc%3D,x-team = ops ,broken"),
            ("OTEL_EXPORTER_OTLP_TIMEOUT", "2500"),
            ("OTEL_RESOURCE_ATTRIBUTES", "service.name=from-resource,deployment.environment=prod"),
            ("OTEL_SERVICE_NAME", "cell-a"),
        ]))
        .unwrap();
        assert_eq!(specific.url, "https://t.example/otlp/traces", "the traces endpoint is used as given");
        assert_eq!(
            specific.headers,
            vec![("authorization".to_string(), "Basic abc=".to_string()), ("x-team".to_string(), "ops".to_string())]
        );
        assert_eq!(specific.timeout, Duration::from_millis(2500));
        assert_eq!(specific.resource[0], ("service.name".to_string(), "cell-a".to_string()));
        assert_eq!(specific.resource[1].0, "service.version");
        assert!(specific.resource.contains(&("deployment.environment".to_string(), "prod".to_string())));
        assert_eq!(specific.resource.iter().filter(|(k, _)| k == "service.name").count(), 1);
        assert_eq!(rejected_spans(r#"{"partialSuccess":{"rejectedSpans":"3"}}"#), 3);
        assert_eq!(rejected_spans(r#"{"partialSuccess":{}}"#), 0);
        assert_eq!(rejected_spans(""), 0);
        assert_eq!(percent_decode("a%2Cb%zz%4"), "a,b%zz%4");
    }

    #[test]
    fn spans_are_neither_built_nor_counted_without_an_exporter() {
        // No endpoint in the test environment: span() is a no-op.
        assert!(!tracing());
        span(Span::hop(&crate::trace::Hop::begin(None), Kind::Producer, "connector:t", 0));
        let text = render(&[]);
        assert!(text.contains("vejas_spans_dropped_total{reason=\"queue_full\"} 0"));
        assert!(text.contains("vejas_spans_dropped_total{reason=\"export_failed\"} 0"));
    }
}
