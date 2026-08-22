//! Observability: a Prometheus `/metrics` exposition and an OTLP/HTTP-JSON span
//! exporter — both hand-rolled so the runtime keeps its single-digit-MB RSS and
//! tiny binary (no `opentelemetry` crate tree). Metrics are pull-scraped; traces
//! are pushed to an OTLP collector ONLY when `OTEL_EXPORTER_OTLP_ENDPOINT` is set
//! (zero cost, no exporter thread, when it is not).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, SyncSender};
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

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
    buckets: [u64; 13], // per-bucket (non-cumulative) counts
    sum: f64,
    count: u64,
}

static UNITS: OnceLock<Mutex<HashMap<String, Unit>>> = OnceLock::new();

fn units() -> &'static Mutex<HashMap<String, Unit>> {
    UNITS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn now_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
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

/// A message that was dead-lettered (ADR-0015). Kept separate from `observe`
/// because a poison event is already counted there as an error — this names the
/// subset that exhausted its deliveries and left the bus.
pub fn inc_dead_letter(unit: &str) {
    let mut map = units().lock().unwrap();
    map.entry(unit.to_string()).or_default().dead += 1;
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

    out.push_str("# HELP vejas_emits_published_total Messages a unit published to the bus.\n");
    out.push_str("# TYPE vejas_emits_published_total counter\n");
    for (unit, u) in map.iter() {
        out.push_str(&format!(
            "vejas_emits_published_total{{unit=\"{}\"}} {}\n",
            esc(unit),
            u.emits
        ));
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

/// One finished span, queued for the background exporter.
pub struct Span {
    pub unit: String,
    pub subject: String,
    pub ok: bool,
    pub error: Option<String>,
    pub start_nanos: u128,
    pub end_nanos: u128,
    pub emits: u64,
}

static TX: OnceLock<Option<SyncSender<Span>>> = OnceLock::new();
static SPAN_SEQ: AtomicU64 = AtomicU64::new(1);

fn id_hex(bytes: usize) -> String {
    let seq = SPAN_SEQ.fetch_add(1, Ordering::Relaxed);
    let n = now_nanos();
    if bytes == 16 {
        // 128-bit trace id: nanos in the high half, a mixed counter in the low.
        let lo = (seq).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        format!("{:016x}{:016x}", n as u64, lo)
    } else {
        // 64-bit span id.
        format!("{:016x}", (seq ^ (n as u64)).wrapping_mul(0xD6E8_FEB8_6659_FD93))
    }
}

/// Spawn the exporter thread iff `OTEL_EXPORTER_OTLP_ENDPOINT` is set. Idempotent
/// (the OnceLock guards it). When unset, TX holds None and `span()` is a no-op.
pub fn otlp_init() {
    TX.get_or_init(|| {
        let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok()?;
        let endpoint = endpoint.trim_end_matches('/').to_string();
        let url = format!("{endpoint}/v1/traces");
        let service =
            std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "vejas".to_string());
        // Bounded queue: if the collector stalls, spans are dropped (a metric
        // path must never block the flow hot loop), never buffered unboundedly.
        let (tx, rx) = sync_channel::<Span>(4096);
        eprintln!("[vejas] OTLP traces -> {url}");
        std::thread::spawn(move || {
            let agent = ureq::AgentBuilder::new()
                .timeout(std::time::Duration::from_secs(5))
                .build();
            let mut batch: Vec<Span> = Vec::with_capacity(512);
            loop {
                // Block for the first span, then drain whatever else is queued.
                match rx.recv() {
                    Ok(s) => batch.push(s),
                    Err(_) => break, // sender gone: shutdown
                }
                while batch.len() < 512 {
                    match rx.try_recv() {
                        Ok(s) => batch.push(s),
                        Err(_) => break,
                    }
                }
                let body = otlp_payload(&service, &batch);
                let _ = agent
                    .post(&url)
                    .set("content-type", "application/json")
                    .send_string(&body);
                batch.clear();
            }
        });
        Some(tx)
    });
}

/// Queue a finished span for export. No-op unless the exporter is enabled; never
/// blocks — a full queue drops the span rather than stalling the caller.
pub fn span(s: Span) {
    if let Some(Some(tx)) = TX.get() {
        let _ = tx.try_send(s);
    }
}

fn otlp_payload(service: &str, spans: &[Span]) -> String {
    let spans_json: Vec<serde_json::Value> = spans
        .iter()
        .map(|s| {
            let mut attrs = vec![
                serde_json::json!({"key":"vejas.unit","value":{"stringValue": s.unit}}),
                serde_json::json!({"key":"messaging.destination.name","value":{"stringValue": s.subject}}),
                serde_json::json!({"key":"vejas.emits","value":{"intValue": s.emits}}),
            ];
            if let Some(e) = &s.error {
                attrs.push(
                    serde_json::json!({"key":"error.message","value":{"stringValue": e}}),
                );
            }
            serde_json::json!({
                "traceId": id_hex(16),
                "spanId": id_hex(8),
                "name": s.unit,
                // SPAN_KIND_CONSUMER: this unit consumed a message off the bus.
                "kind": 5,
                "startTimeUnixNano": s.start_nanos.to_string(),
                "endTimeUnixNano": s.end_nanos.to_string(),
                "attributes": attrs,
                // status: 2 = ERROR, 1 = OK.
                "status": {"code": if s.ok { 1 } else { 2 }},
            })
        })
        .collect();
    serde_json::json!({
        "resourceSpans": [{
            "resource": {
                "attributes": [
                    {"key":"service.name","value":{"stringValue": service}}
                ]
            },
            "scopeSpans": [{
                "scope": {"name": "vejas.runtime"},
                "spans": spans_json
            }]
        }]
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_emits_prometheus_shape() {
        observe("flow:t", true, 2, 0.003);
        observe("flow:t", false, 0, 0.02);
        inc_dead_letter("flow:t");
        let text = render(&[("flow:t".into(), "flow".into(), 1)]);
        assert!(text.contains("vejas_up 1"));
        assert!(text.contains("vejas_events_processed_total{unit=\"flow:t\",result=\"ok\"} 1"));
        assert!(text.contains("vejas_events_processed_total{unit=\"flow:t\",result=\"error\"} 1"));
        assert!(text.contains("vejas_emits_published_total{unit=\"flow:t\"} 2"));
        assert!(text.contains("vejas_dead_letters_total{unit=\"flow:t\"} 1"));
        assert!(text.contains("vejas_event_duration_seconds_count{unit=\"flow:t\"} 2"));
        assert!(text.contains("le=\"+Inf\"} 2"));
        assert!(text.contains("vejas_flow_restarts{unit=\"flow:t\"} 1"));
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
        let s = Span {
            unit: "flow:x".into(),
            subject: "vx.a.b".into(),
            ok: false,
            error: Some("boom".into()),
            start_nanos: 1,
            end_nanos: 2,
            emits: 3,
        };
        let body = otlp_payload("vejas", &[s]);
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        let span = &v["resourceSpans"][0]["scopeSpans"][0]["spans"][0];
        assert_eq!(span["name"], "flow:x");
        assert_eq!(span["status"]["code"], 2);
        assert_eq!(span["traceId"].as_str().unwrap().len(), 32);
        assert_eq!(span["spanId"].as_str().unwrap().len(), 16);
    }
}
