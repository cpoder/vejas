// Vejas runtime — all Rust, no Python.
//
// Executes VejasScript flows natively, runs the bundled connectors as threads,
// and serves the panel. Layout (hot-addable packages):
//
//   flows/*.vjs                  the "default" package
//   services/*.vjs               composable services (invoke name(...))
//   packages/<pkg>/flows|services
//   packages/<pkg>/package.vjs   literal manifest: ENABLED, EXPORTS = [...]
//
// A flow is a NATS pull-consumer thread + the in-process interpreter. The
// bundled connectors (http-in, slack-out) are Rust threads. External
// connectors in any language remain possible over the bus (docs/SUBJECTS.md).
//
// HTTP surface (one request = one thread; /flows/new can take minutes):
//   GET  /            panel        GET /healthz        GET /topology
//   GET  /metrics     Prometheus exposition (OTLP traces: OTEL_EXPORTER_OTLP_ENDPOINT)
//   GET  /graph       pipeline     GET /surface        business surface
//   GET  /events?flow=             last processed events (in-memory ring)
//   GET  /preview?file=            fixture -> sample run
//   GET  /file?path=               read a script        POST /file/set
//   GET  /fixture?file=            read a fixture       POST /fixture/set
//   POST /surface/set              rewrite one literal in place
//   POST /flows/new                agent CLI writes a VejasScript flow
//   POST /reload                   rescan; restart changed files (mtime)

mod connectors;
mod control;
mod metrics;
mod secrets;
mod vjs;

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{env, fs, thread};

use serde_json::{json, Value};

#[derive(Clone, PartialEq)]
enum Kind {
    Flow,
    Connector,
}

#[derive(Clone)]
struct Spec {
    name: String,
    path: PathBuf,
    pkg: String,
    kind: Kind,
    mtime: u64,
}

struct ProcState {
    status: String,
    restarts: u64,
    started_at: Option<u64>,
    last_error: Option<String>,
}

struct Handle {
    spec: Spec,
    state: Mutex<ProcState>,
    stop: AtomicBool,
}

type Registry = Arc<Mutex<HashMap<String, Arc<Handle>>>>;

static RUNNING: AtomicBool = AtomicBool::new(true);

// ───────────────────────── event trace ─────────────────────────
//
// The last events each flow processed, in memory (ring of 50 per flow) — the
// minimal honest answer to "what just went through, and did it work?". Served
// by GET /events, the vejas_events MCP tool, and the panel's live strip.

static TRACES: OnceLock<Mutex<HashMap<String, VecDeque<Value>>>> = OnceLock::new();
static TRACE_SEQ: AtomicU64 = AtomicU64::new(0);

/// After this many failed deliveries of the same message, a unit stops
/// retrying and drops it (acked, visible in the trace) — redelivery is the
/// retry mechanism, not an infinite loop for poison messages.
pub const MAX_DELIVERIES: i64 = 5;

/// Secret references the operator asked to rotate (`rotate_requested` over
/// the control channel, CONTROL.md). A flag, nothing more: the value is typed
/// locally, and setting the secret clears it.
static ROTATIONS: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

pub fn flag_rotation(reference: &str) {
    ROTATIONS
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .unwrap()
        .insert(reference.to_string());
}

fn rotation_flagged(reference: &str) -> bool {
    ROTATIONS
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .unwrap()
        .contains(reference)
}

fn clear_rotation(reference: &str) {
    ROTATIONS
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .unwrap()
        .remove(reference);
}

pub fn record_trace(
    flow: &str,
    subject: &str,
    ok: bool,
    error: Option<String>,
    emits: Vec<String>,
    preview: String,
    event: Option<Value>,
) {
    record_trace_full(flow, subject, ok, error, emits, preview, event, None)
}

/// Full form: `response` carries a sink's response-body summary (what the
/// downstream API answered — rejected facts, scoring skips…), so mapping
/// problems are visible in the panel instead of dying in an acked 2xx.
#[allow(clippy::too_many_arguments)]
pub fn record_trace_full(
    unit: &str,
    subject: &str,
    ok: bool,
    error: Option<String>,
    emits: Vec<String>,
    preview: String,
    event: Option<Value>,
    response: Option<String>,
) {
    let seq = TRACE_SEQ.fetch_add(1, Ordering::SeqCst);
    let mut map = TRACES.get_or_init(|| Mutex::new(HashMap::new())).lock().unwrap();
    let ring = map.entry(unit.to_string()).or_default();
    ring.push_back(json!({
        "seq": seq, "ts": now_secs(), "flow": unit, "subject": subject,
        "ok": ok, "error": error, "emits": emits, "preview": preview,
        "response": response,
        // the full event, kept for shadow-replay; stripped from /events output
        "event": event,
    }));
    while ring.len() > 50 {
        ring.pop_front();
    }
}

/// Newest-first flat list of trace entries, optionally for one flow, capped.
/// The stored full event stays internal (replay fuel), only the preview goes out.
fn events_json(flow: Option<&str>) -> String {
    let map = TRACES.get_or_init(|| Mutex::new(HashMap::new())).lock().unwrap();
    let mut all: Vec<Value> = map
        .iter()
        .filter(|(name, _)| flow.map(|f| f == name.as_str()).unwrap_or(true))
        .flat_map(|(_, ring)| {
            ring.iter().map(|e| {
                let mut e = e.clone();
                if let Some(o) = e.as_object_mut() {
                    o.remove("event");
                }
                e
            })
        })
        .collect();
    all.sort_by_key(|e| std::cmp::Reverse(e["seq"].as_u64().unwrap_or(0)));
    all.truncate(100);
    json!({ "events": all }).to_string()
}

/// The supervised process name of a flow file (how the trace ring is keyed).
fn flow_proc_name(path: &Path) -> String {
    let stem = path
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();
    let pkg = pkg_of_path(path);
    if pkg == "default" {
        format!("flow:{stem}")
    } else {
        format!("flow:{pkg}:{stem}")
    }
}

/// Shadow-replay (ADR-0005): apply a literal change IN MEMORY, rerun the
/// flow's last real events against the current and the patched script, and
/// return the before/after emit diff. Nothing is written, nothing touches the
/// bus — emits are collected, not published. Approval = the ordinary
/// `set_literal` write afterwards.
fn replay_literal(
    root: &Path,
    file: &str,
    name: &str,
    key: &str,
    value: &Value,
    n: usize,
) -> Result<Value, String> {
    let path = guard_path(root, file).ok_or("path not allowed")?;
    let src = fs::read_to_string(&path).map_err(|e| e.to_string())?;
    let patched = vjs::set_literal(&src, name, key, value)?;
    let before = vjs::parse(&src)?;
    let after = vjs::parse(&patched)?;
    let proc = flow_proc_name(&path);
    // Shadow-replay fuel (ADR-0018): prefer REAL persisted traffic hydrated from
    // JetStream (read-only, the bus untouched) so replay reflects history that
    // survived restarts and reaches past the 50-entry ring. Fall back to the
    // in-memory trace ring when there is no bus source (api/tool flow), the
    // stream is empty, or NATS is unreachable — always with a `source` tag so the
    // operator knows what the diff was computed against.
    let (events, replay_source): (Vec<Value>, &str) = {
        let hydrated = before.source.as_deref().and_then(|subject| {
            let url = env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
            let stream = env::var("VEJAS_STREAM").unwrap_or_else(|_| "VEJAS".into());
            let nc = nats::connect(&url).ok()?;
            let js = nats::jetstream::new(nc);
            connectors::hydrate_recent(&js, &stream, subject, n).ok()
        });
        match hydrated {
            Some(rows) if !rows.is_empty() => (
                rows.into_iter()
                    .map(|(subject, event)| {
                        json!({
                            "ts": Value::Null,
                            "subject": subject,
                            "preview": preview_of(&event),
                            "event": event,
                        })
                    })
                    .collect(),
                "jetstream",
            ),
            _ => {
                let map = TRACES.get_or_init(|| Mutex::new(HashMap::new())).lock().unwrap();
                let ring = map
                    .get(&proc)
                    .map(|ring| ring.iter().rev().take(n.clamp(1, 50)).cloned().collect())
                    .unwrap_or_default();
                (ring, "trace-ring")
            }
        }
    };
    let pkg = pkg_of_path(&path);
    let run_one = |prog: &vjs::Program, ev: &Value| -> Value {
        let mut engine = vjs::Engine::new(root.to_path_buf(), pkg.clone());
        match vjs::run(prog, ev, &mut engine) {
            Ok(ctx) => json!({
                "emits": ctx.emits.iter().map(|(s, p)| json!({"subject": s, "payload": p})).collect::<Vec<_>>(),
                "error": Value::Null,
            }),
            Err(e) => json!({ "emits": [], "error": e }),
        }
    };
    let mut results = Vec::new();
    let mut changed_count = 0;
    for entry in &events {
        let ev = entry.get("event").cloned().unwrap_or(Value::Null);
        if ev.is_null() {
            continue; // unparseable event of a bad-json trace: nothing to replay
        }
        let b = run_one(&before, &ev);
        let a = run_one(&after, &ev);
        let changed = b != a;
        if changed {
            changed_count += 1;
        }
        results.push(json!({
            "ts": entry["ts"], "subject": entry["subject"], "preview": entry["preview"],
            "before": b, "after": a, "changed": changed,
        }));
    }
    Ok(json!({
        "file": file, "name": name, "key": key, "value": value,
        "source": replay_source,
        "events": results.len(), "changed": changed_count, "results": results,
    }))
}

fn preview_of(v: &Value) -> String {
    let s = v.to_string();
    if s.chars().count() > 160 {
        let cut: String = s.chars().take(159).collect();
        format!("{cut}…")
    } else {
        s
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn mtime_of(path: &Path) -> u64 {
    // Nanoseconds, not seconds: `vejas_set_literal` writes the file and calls
    // reload() in the same wall-clock second, so a second-granularity stamp
    // collides and reload's `old_mtime != spec.mtime` guard misses the change —
    // the live flow keeps running the pre-promote surface (ADR-0005 no-op).
    fs::metadata(path)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

// ───────────────────────── package scan ─────────────────────────

fn package_enabled(pkg_dir: &Path) -> bool {
    let manifest = pkg_dir.join("package.vjs");
    if let Ok(src) = fs::read_to_string(&manifest) {
        if let Ok(prog) = vjs::parse(&src) {
            if let Some(e) = prog.surface.iter().find(|e| e.name == "ENABLED") {
                return e.value.as_bool().unwrap_or(true);
            }
        }
    }
    true
}

fn scan_units(dir: &Path, pkg: &str, kind: Kind, out: &mut Vec<Spec>) {
    let Ok(entries) = fs::read_dir(dir) else { return };
    let prefix = match kind {
        Kind::Flow => "flow",
        Kind::Connector => "connector",
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(fname) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if fname.starts_with('_') || !fname.ends_with(".vjs") {
            continue;
        }
        let stem = fname.trim_end_matches(".vjs").to_string();
        let name = if pkg == "default" {
            format!("{prefix}:{stem}")
        } else {
            format!("{prefix}:{pkg}:{stem}")
        };
        out.push(Spec {
            name,
            mtime: mtime_of(&path),
            path,
            pkg: pkg.to_string(),
            kind: kind.clone(),
        });
    }
}

fn scan_all(root: &Path) -> Vec<Spec> {
    let mut out = Vec::new();
    scan_units(&root.join("flows"), "default", Kind::Flow, &mut out);
    scan_units(&root.join("connectors"), "default", Kind::Connector, &mut out);
    if let Ok(pkgs) = fs::read_dir(root.join("packages")) {
        for p in pkgs.flatten() {
            let dir = p.path();
            if !dir.is_dir() {
                continue;
            }
            let pkg = p.file_name().to_string_lossy().to_string();
            if !package_enabled(&dir) {
                continue;
            }
            scan_units(&dir.join("flows"), &pkg, Kind::Flow, &mut out);
            scan_units(&dir.join("connectors"), &pkg, Kind::Connector, &mut out);
        }
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

fn fixture_path_for(flow_path: &Path) -> PathBuf {
    let stem = flow_path
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();
    flow_path
        .parent()
        .unwrap_or(Path::new("."))
        .join("fixtures")
        .join(format!("{stem}.json"))
}

// ───────────────────────── supervision ─────────────────────────

fn set_state(handle: &Handle, f: impl FnOnce(&mut ProcState)) {
    let mut st = handle.state.lock().unwrap();
    f(&mut st);
}

/// Native VejasScript flow: NATS pull consumer + in-process interpreter.
fn supervise_vjs(handle: Arc<Handle>, root: PathBuf) {
    let url = env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let stream = env::var("VEJAS_STREAM").unwrap_or_else(|_| "VEJAS".into());
    let subj_root = env::var("VEJAS_SUBJECT_ROOT").unwrap_or_else(|_| "vx".into());
    let mut delay = Duration::from_secs(1);
    'outer: loop {
        if !RUNNING.load(Ordering::SeqCst) || handle.stop.load(Ordering::SeqCst) {
            break;
        }
        let attempt = (|| -> Result<(), String> {
            let src = fs::read_to_string(&handle.spec.path).map_err(|e| e.to_string())?;
            let prog = vjs::parse(&src)?;
            let Some(source) = prog.source.clone() else {
                // An api-only / tool-only flow has no bus source: it is served
                // synchronously by the HTTP/API and MCP routers, not consumed
                // here. Park the supervisor (no consumer) instead of crash-
                // looping on the missing `source` — only a flow that is neither
                // is genuinely malformed.
                if prog.api.is_some() || prog.tool.is_some() {
                    let how = if prog.api.is_some() { "api" } else { "tool" };
                    set_state(&handle, |st| {
                        st.status = format!("serving ({how})");
                        st.started_at = Some(now_secs());
                        st.last_error = None;
                    });
                    while RUNNING.load(Ordering::SeqCst) && !handle.stop.load(Ordering::SeqCst) {
                        thread::sleep(Duration::from_millis(200));
                    }
                    return Ok(());
                }
                return Err("no `source` declaration".into());
            };
            let nc = nats::connect(&url).map_err(|e| e.to_string())?;
            let js = nats::jetstream::new(nc.clone());
            let _ = js.add_stream(&nats::jetstream::StreamConfig {
                name: stream.clone(),
                subjects: vec![format!("{subj_root}.>")],
                ..Default::default()
            });
            let durable = handle.spec.name.replace([':', '.'], "_");
            // create the durable pull consumer (idempotent-ish), then bind
            let _ = js.add_consumer(
                &stream,
                nats::jetstream::ConsumerConfig {
                    durable_name: Some(durable.clone()),
                    deliver_subject: None,
                    filter_subject: source.clone(),
                    ..Default::default()
                },
            );
            let sub = js
                .pull_subscribe_with_options(
                    &source,
                    &nats::jetstream::PullSubscribeOptions::new()
                        .durable_name(durable),
                )
                .map_err(|e| e.to_string())?;
            set_state(&handle, |st| {
                st.status = "running".into();
                st.started_at = Some(now_secs());
                st.last_error = None;
            });
            eprintln!(
                "[vejas] {} (vjs, native) consuming {source}",
                handle.spec.name
            );
            let mut engine = vjs::Engine::new(root.clone(), handle.spec.pkg.clone());
            loop {
                if !RUNNING.load(Ordering::SeqCst) || handle.stop.load(Ordering::SeqCst) {
                    return Ok(());
                }
                engine.invalidate(); // pick up live service edits
                let batch = connectors::fetch_round(&sub)?;
                // Emits are buffered (core publish) and confirmed by ONE flush per
                // batch before their source messages are acked — one round-trip a
                // batch instead of one per emit, at-least-once preserved (ADR-0002).
                let mut to_ack: Vec<&nats::Message> = Vec::new();
                for msg in &batch {
                    if !RUNNING.load(Ordering::SeqCst) || handle.stop.load(Ordering::SeqCst) {
                        // stopped mid-batch: leave the message un-acked, it
                        // redelivers to whoever holds the durable next
                        return Ok(());
                    }
                    let event: Value = match serde_json::from_slice(&msg.data) {
                        Ok(v) => v,
                        Err(e) => {
                            // Not JSON: permanent poison. Dead-letter it (ADR-0015)
                            // — publish before ack, nak if the DLQ write fails.
                            let err = format!("bad json: {e}");
                            eprintln!("[vejas] {}: {err}", handle.spec.name);
                            let delivered =
                                msg.jetstream_message_info().map(|i| i.delivered).unwrap_or(1);
                            let prev: String =
                                String::from_utf8_lossy(&msg.data).chars().take(160).collect();
                            metrics::observe(&handle.spec.name, false, 0, 0.0);
                            match connectors::to_dlq(&js, &handle.spec.name, &msg.subject, delivered, &err, &msg.data) {
                                Ok(()) => {
                                    metrics::inc_dead_letter(&handle.spec.name);
                                    record_trace(&handle.spec.name, &msg.subject, false,
                                        Some(format!("{err} — dead-lettered")), vec![], prev, None);
                                    let _ = msg.ack();
                                }
                                Err(de) => {
                                    record_trace(&handle.spec.name, &msg.subject, false,
                                        Some(format!("{err} — DLQ publish failed: {de}")), vec![], prev, None);
                                    let _ = msg.ack_kind(nats::jetstream::AckKind::Nak);
                                }
                            }
                            continue;
                        }
                    };
                    let t0 = Instant::now();
                    let start_nanos = metrics::now_nanos();
                    match vjs::run(&prog, &event, &mut engine) {
                        Ok(ctx) => {
                            let mut ok = true;
                            for (subj, payload) in &ctx.emits {
                                let bytes = serde_json::to_vec(payload).unwrap_or_default();
                                // buffered core publish; flushed once per batch below
                                if let Err(e) = nc.publish(subj, bytes) {
                                    eprintln!(
                                        "[vejas] {}: publish {subj}: {e}",
                                        handle.spec.name
                                    );
                                    ok = false;
                                }
                            }
                            let emits = ctx.emits.len() as u64;
                            metrics::observe(&handle.spec.name, ok, emits, t0.elapsed().as_secs_f64());
                            metrics::span(metrics::Span {
                                unit: handle.spec.name.clone(),
                                subject: msg.subject.clone(),
                                ok,
                                error: None,
                                start_nanos,
                                end_nanos: metrics::now_nanos(),
                                emits,
                            });
                            record_trace(
                                &handle.spec.name,
                                &msg.subject,
                                ok,
                                (!ok).then(|| "publish failed (will redeliver)".to_string()),
                                ctx.emits.iter().map(|(s, _)| s.clone()).collect(),
                                preview_of(&event),
                                Some(event.clone()),
                            );
                            if ok {
                                to_ack.push(msg); // acked after the batch flush
                            }
                        }
                        Err(e) => {
                            eprintln!("[vejas] {}: {e}", handle.spec.name);
                            metrics::observe(&handle.spec.name, false, 0, t0.elapsed().as_secs_f64());
                            metrics::span(metrics::Span {
                                unit: handle.spec.name.clone(),
                                subject: msg.subject.clone(),
                                ok: false,
                                error: Some(e.clone()),
                                start_nanos,
                                end_nanos: metrics::now_nanos(),
                                emits: 0,
                            });
                            // poison guard: past MAX_DELIVERIES the message is
                            // dead-lettered (ADR-0015) rather than dropped; publish
                            // before ack, nak if the DLQ write fails.
                            let delivered =
                                msg.jetstream_message_info().map(|i| i.delivered).unwrap_or(1);
                            if delivered >= MAX_DELIVERIES {
                                match connectors::to_dlq(&js, &handle.spec.name, &msg.subject, delivered, &e, &msg.data) {
                                    Ok(()) => {
                                        metrics::inc_dead_letter(&handle.spec.name);
                                        record_trace(&handle.spec.name, &msg.subject, false,
                                            Some(format!("{e} — dead-lettered after {delivered} deliveries")),
                                            vec![], preview_of(&event), Some(event.clone()));
                                        let _ = msg.ack();
                                    }
                                    Err(de) => {
                                        record_trace(&handle.spec.name, &msg.subject, false,
                                            Some(format!("{e} — DLQ publish failed: {de}")),
                                            vec![], preview_of(&event), Some(event.clone()));
                                        let _ = msg.ack_kind(nats::jetstream::AckKind::Nak);
                                    }
                                }
                            } else {
                                record_trace(&handle.spec.name, &msg.subject, false, Some(e.clone()),
                                    vec![], preview_of(&event), Some(event.clone()));
                                // no ack -> redelivery after ack_wait
                            }
                            set_state(&handle, |st| st.last_error = Some(e));
                        }
                    }
                }
                // Publish-before-ack barrier for the batch: one flush confirms the
                // server received every buffered emit, THEN ack the fully-published
                // source messages. On flush failure, ack none — the batch
                // redelivers (nothing is lost; at-least-once, ADR-0002).
                if !to_ack.is_empty() {
                    match nc.flush() {
                        Ok(_) => {
                            for m in &to_ack {
                                let _ = m.ack();
                            }
                        }
                        Err(e) => eprintln!(
                            "[vejas] {}: flush before ack failed: {e} (batch redelivers)",
                            handle.spec.name
                        ),
                    }
                }
            }
        })();
        match attempt {
            Ok(()) => break 'outer,
            Err(e) => {
                eprintln!("[vejas] {}: {e}", handle.spec.name);
                set_state(&handle, |st| {
                    st.status = "restarting".into();
                    st.restarts += 1;
                    st.last_error = Some(e);
                });
                thread::sleep(delay);
                delay = (delay * 2).min(Duration::from_secs(30));
            }
        }
    }
    set_state(&handle, |st| {
        st.status = "stopped".into();
    });
}

fn pkg_of_path(path: &Path) -> String {
    let s = path.display().to_string();
    if let Some(rest) = s.split("packages/").nth(1) {
        if let Some(pkg) = rest.split('/').next() {
            return pkg.to_string();
        }
    }
    "default".to_string()
}

fn start_proc(registry: &Registry, spec: Spec, root: &Path) {
    let handle = Arc::new(Handle {
        spec: spec.clone(),
        state: Mutex::new(ProcState {
            status: "starting".into(),
            restarts: 0,
            started_at: None,
            last_error: None,
        }),
        stop: AtomicBool::new(false),
    });
    registry.lock().unwrap().insert(spec.name.clone(), handle.clone());
    let root = root.to_path_buf();
    match spec.kind {
        Kind::Flow => {
            thread::spawn(move || supervise_vjs(handle, root));
        }
        Kind::Connector => {
            thread::spawn(move || supervise_connector(handle, root));
        }
    }
}

/// Native connector instance: a manifest (`driver "..."` + literal config) run
/// by a compiled-in driver, restarted with backoff on error / file change.
fn supervise_connector(handle: Arc<Handle>, root: PathBuf) {
    let url = env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let stream = env::var("VEJAS_STREAM").unwrap_or_else(|_| "VEJAS".into());
    let subj_root = env::var("VEJAS_SUBJECT_ROOT").unwrap_or_else(|_| "vx".into());
    let mut delay = Duration::from_secs(1);
    let stop = Arc::new(AtomicBool::new(true));
    loop {
        if !RUNNING.load(Ordering::SeqCst) || handle.stop.load(Ordering::SeqCst) {
            break;
        }
        let attempt = (|| -> Result<(), String> {
            let src = fs::read_to_string(&handle.spec.path).map_err(|e| e.to_string())?;
            let prog = vjs::parse(&src)?;
            let driver_name = prog.driver.clone().ok_or("no `driver` declaration")?;
            let driver = connectors::driver_for(&driver_name)
                .ok_or_else(|| format!("unknown driver {driver_name:?}"))?;
            // Resolve config by EVALUATING the manifest (so secret("…") in a
            // config value resolves to the real value, fail-closed). The
            // manifest's UPPERCASE variables are the driver's config.
            let mut cfg_engine =
                vjs::Engine::new(root.clone(), handle.spec.pkg.clone());
            let cfg_ctx = vjs::run(&prog, &Value::Object(serde_json::Map::new()), &mut cfg_engine)?;
            let mut config = serde_json::Map::new();
            for (k, v) in cfg_ctx.vars {
                if k != "event"
                    && k.chars().all(|c| c.is_ascii_uppercase() || c == '_' || c.is_ascii_digit())
                    && k.chars().any(|c| c.is_ascii_uppercase())
                {
                    config.insert(k, v);
                }
            }
            set_state(&handle, |st| {
                st.status = format!("running ({})", driver.kind());
                st.started_at = Some(now_secs());
                st.last_error = None;
            });
            // wire this connector's stop flag to the shared running signal
            let running = stop.clone();
            running.store(true, Ordering::SeqCst);
            let ctx = connectors::Ctx {
                name: handle.spec.name.clone(),
                nats_url: url.clone(),
                stream: stream.clone(),
                subj_root: subj_root.clone(),
                config: connectors::Config(config),
                running: running.clone(),
            };
            // watch the handle's stop flag in a sidecar thread → clears ctx.running
            let h2 = handle.clone();
            let r2 = running.clone();
            thread::spawn(move || {
                while RUNNING.load(Ordering::SeqCst) && !h2.stop.load(Ordering::SeqCst) {
                    thread::sleep(Duration::from_millis(200));
                }
                r2.store(false, Ordering::SeqCst);
            });
            driver.run(&ctx)
        })();
        stop.store(false, Ordering::SeqCst);
        match attempt {
            Ok(()) => break,
            Err(e) => {
                eprintln!("[vejas] connector {}: {e}", handle.spec.name);
                set_state(&handle, |st| {
                    st.status = "restarting".into();
                    st.restarts += 1;
                    st.last_error = Some(e);
                });
                if !RUNNING.load(Ordering::SeqCst) || handle.stop.load(Ordering::SeqCst) {
                    break;
                }
                thread::sleep(delay);
                delay = (delay * 2).min(Duration::from_secs(30));
            }
        }
    }
    set_state(&handle, |st| st.status = "stopped".into());
}

/// Stop one unit and start it again from a fresh scan (secret rotation, etc.).
fn restart_unit(registry: &Registry, root: &Path, name: &str) -> bool {
    let Some(spec) = scan_all(root).into_iter().find(|s| s.name == name) else {
        return false;
    };
    if let Some(handle) = registry.lock().unwrap().get(name) {
        handle.stop.store(true, Ordering::SeqCst);
    }
    start_proc(registry, spec, root);
    true
}

/// Every secret reference declared anywhere, aggregated: ref -> using files.
fn secrets_inventory(root: &Path) -> Vec<(String, Vec<String>)> {
    let mut map: HashMap<String, Vec<String>> = HashMap::new();
    let mut scan = |files: Vec<(PathBuf, String)>| {
        for (path, _pkg) in files {
            let Ok(src) = fs::read_to_string(&path) else { continue };
            let Ok(prog) = vjs::parse(&src) else { continue };
            for (_var, r) in prog.secret_refs {
                map.entry(r).or_default().push(path.display().to_string());
            }
        }
    };
    scan(vjs_files(root));
    scan(connector_files(root));
    let mut out: Vec<(String, Vec<String>)> = map.into_iter().collect();
    out.sort();
    out
}

/// The Secrets surface: references, who uses them, and whether each one
/// RESOLVES (probed against the store, the value immediately discarded).
/// Values never appear anywhere.
fn secrets_json(root: &Path) -> String {
    let store = secrets::default_store();
    let list: Vec<Value> = secrets_inventory(root)
        .into_iter()
        .map(|(r, used_by)| {
            let status = match store.get(&r) {
                Ok(_) => "ok",
                Err(_) => "missing",
            };
            json!({"ref": r, "used_by": used_by, "status": status,
                   "rotation_requested": rotation_flagged(&r)})
        })
        .collect();
    json!({"backend": store.kind(), "writable": !matches!(store.kind(), "env"), "secrets": list})
        .to_string()
}

/// Write one secret, then restart every unit whose file references it (their
/// config was evaluated at start). Returns the restarted unit names.
fn set_secret(registry: &Registry, root: &Path, reference: &str, value: &str) -> Result<Vec<String>, String> {
    secrets::default_store().set(reference, value)?;
    clear_rotation(reference); // a set satisfies an operator rotation request
    let mut restarted = Vec::new();
    for spec in scan_all(root) {
        let Ok(src) = fs::read_to_string(&spec.path) else { continue };
        let Ok(prog) = vjs::parse(&src) else { continue };
        if prog.secret_refs.iter().any(|(_, r)| r == reference)
            && restart_unit(registry, root, &spec.name)
        {
            restarted.push(spec.name);
        }
    }
    Ok(restarted)
}

fn reload(registry: &Registry, root: &Path) -> (usize, usize, usize) {
    let mut wanted: HashMap<String, Spec> = HashMap::new();
    for spec in scan_all(root) {
        wanted.insert(spec.name.clone(), spec);
    }
    let mut started = 0;
    let mut stopped = 0;
    let current: HashMap<String, (bool, u64)> = {
        let reg = registry.lock().unwrap();
        for (name, handle) in reg.iter() {
            if !wanted.contains_key(name) && !handle.stop.load(Ordering::SeqCst) {
                handle.stop.store(true, Ordering::SeqCst);
                stopped += 1;
            }
        }
        reg.iter()
            .map(|(n, h)| (n.clone(), (h.stop.load(Ordering::SeqCst), h.spec.mtime)))
            .collect()
    };
    for (name, spec) in &wanted {
        let restart = match current.get(name) {
            None => true,
            Some((true, _)) => true,
            Some((false, old_mtime)) if *old_mtime != spec.mtime => {
                if let Some(handle) = registry.lock().unwrap().get(name) {
                    handle.stop.store(true, Ordering::SeqCst);
                }
                stopped += 1;
                true
            }
            _ => false,
        };
        if restart {
            start_proc(registry, spec.clone(), root);
            started += 1;
        }
    }
    (wanted.len(), started, stopped)
}

// ───────────────────────── introspection ─────────────────────────

fn topology_json(registry: &Registry) -> Value {
    let reg = registry.lock().unwrap();
    let mut flows = Vec::new();
    let mut connectors = Vec::new();
    for handle in reg.values() {
        let st = handle.state.lock().unwrap();
        let entry = json!({
            "name": handle.spec.name,
            "file": handle.spec.path.display().to_string(),
            "pkg": handle.spec.pkg,
            "lang": "vjs",
            "status": st.status,
            "restarts": st.restarts,
            "started_at": st.started_at,
            "last_error": st.last_error,
        });
        match handle.spec.kind {
            Kind::Flow => flows.push(entry),
            Kind::Connector => connectors.push(entry),
        }
    }
    json!({ "flows": flows, "connectors": connectors })
}

/// Connector instances (manifests), for the graph: name, driver, kind, in/out.
fn connector_graph(root: &Path) -> Vec<Value> {
    let mut out = Vec::new();
    let scan = |dir: PathBuf, list: &mut Vec<Value>| {
        let Ok(entries) = fs::read_dir(&dir) else { return };
        for e in entries.flatten() {
            let p = e.path();
            if !p.extension().map(|x| x == "vjs").unwrap_or(false) {
                continue;
            }
            let Ok(prog) = fs::read_to_string(&p).ok().map(|s| vjs::parse(&s)).unwrap_or(Err("".into()))
            else { continue };
            let Some(driver) = prog.driver.clone() else { continue };
            let cfg: std::collections::HashMap<String, Value> =
                prog.surface.iter().map(|s| (s.name.clone(), s.value.clone())).collect();
            let kind = connectors::driver_for(&driver).map(|d| d.kind()).unwrap_or("unknown");
            let name = p.file_stem().map(|s| s.to_string_lossy().to_string()).unwrap_or_default();
            let mut entry = json!({"name": name, "driver": driver, "kind": kind});
            if kind.starts_with("source") {
                let subj = cfg.get("SUBJECT").and_then(|v| v.as_str()).unwrap_or("vx.*");
                entry["subjects_out"] = json!([subj]);
            } else if let Some(s) = cfg.get("SUBJECT").and_then(|v| v.as_str()) {
                entry["subjects_in"] = json!([s]);
            }
            if !prog.secret_refs.is_empty() {
                entry["secret_refs"] = json!(prog
                    .secret_refs
                    .iter()
                    .map(|(n, r)| json!({"var": n, "ref": r}))
                    .collect::<Vec<_>>());
            }
            list.push(entry);
        }
    };
    scan(root.join("connectors"), &mut out);
    if let Ok(pkgs) = fs::read_dir(root.join("packages")) {
        for p in pkgs.flatten() {
            scan(p.path().join("connectors"), &mut out);
        }
    }
    out
}

fn vjs_kind_of(name: &str, value: &Value) -> &'static str {
    if name.starts_with("MAPPING") && value.is_object() {
        "mapping"
    } else if value.is_object() {
        "table"
    } else {
        "constant"
    }
}

fn vjs_files(root: &Path) -> Vec<(PathBuf, String)> {
    let mut out = Vec::new();
    let mut push_dir = |dir: PathBuf, pkg: &str| {
        if let Ok(entries) = fs::read_dir(&dir) {
            for e in entries.flatten() {
                let p = e.path();
                if p.extension().map(|x| x == "vjs").unwrap_or(false)
                    && p.file_name().map(|f| f != "package.vjs").unwrap_or(false)
                {
                    out.push((p, pkg.to_string()));
                }
            }
        }
    };
    push_dir(root.join("flows"), "default");
    push_dir(root.join("services"), "default");
    if let Ok(pkgs) = fs::read_dir(root.join("packages")) {
        for p in pkgs.flatten() {
            let pkg = p.file_name().to_string_lossy().to_string();
            push_dir(p.path().join("flows"), &pkg);
            push_dir(p.path().join("services"), &pkg);
        }
    }
    out.sort();
    out
}

/// Connector manifests (root connectors/ + packages/<pkg>/connectors/).
fn connector_files(root: &Path) -> Vec<(PathBuf, String)> {
    let mut out = Vec::new();
    fn push_dir(dir: PathBuf, pkg: &str, out: &mut Vec<(PathBuf, String)>) {
        if let Ok(entries) = fs::read_dir(&dir) {
            for e in entries.flatten() {
                let p = e.path();
                if p.extension().map(|x| x == "vjs").unwrap_or(false) {
                    out.push((p, pkg.to_string()));
                }
            }
        }
    }
    push_dir(root.join("connectors"), "default", &mut out);
    if let Ok(pkgs) = fs::read_dir(root.join("packages")) {
        for p in pkgs.flatten() {
            let pkg = p.file_name().to_string_lossy().to_string();
            push_dir(p.path().join("connectors"), &pkg, &mut out);
        }
    }
    out.sort();
    out
}

fn surface_json(root: &Path) -> Value {
    let mut entries: Vec<Value> = Vec::new();
    for (path, pkg) in vjs_files(root) {
        let Ok(src) = fs::read_to_string(&path) else { continue };
        let Ok(prog) = vjs::parse(&src) else { continue };
        let is_service = path
            .parent()
            .and_then(|p| p.file_name())
            .map(|f| f == "services")
            .unwrap_or(false);
        for e in prog.surface {
            entries.push(json!({
                "file": path.display().to_string(),
                "name": e.name,
                "kind": vjs_kind_of(&e.name, &e.value),
                "value": e.value,
                "list": e.value.is_array(),
                "lang": "vjs",
                "pkg": pkg,
                "service": is_service,
            }));
        }
        // ensure a card exists even without surface entries
        if entries
            .iter()
            .all(|v| v["file"].as_str() != Some(&path.display().to_string()))
        {
            entries.push(json!({
                "file": path.display().to_string(),
                "name": "",
                "kind": "none",
                "value": Value::Null,
                "lang": "vjs",
                "pkg": pkg,
                "service": is_service,
            }));
        }
    }
    // connector manifests get cards too: their literal config IS a business
    // surface (edited with the same set_literal machinery, applied directly)
    for (path, pkg) in connector_files(root) {
        let Ok(src) = fs::read_to_string(&path) else { continue };
        let Ok(prog) = vjs::parse(&src) else { continue };
        let Some(driver) = prog.driver.clone() else { continue };
        let driver_kind = connectors::driver_for(&driver)
            .map(|d| d.kind())
            .unwrap_or("unknown");
        let base = json!({
            "file": path.display().to_string(),
            "lang": "vjs",
            "pkg": pkg,
            "service": false,
            "connector": true,
            "driver": driver,
            "driver_kind": driver_kind,
        });
        let mut pushed = false;
        for e in &prog.surface {
            let mut v = base.clone();
            let o = v.as_object_mut().unwrap();
            o.insert("name".into(), json!(e.name));
            o.insert("kind".into(), json!(vjs_kind_of(&e.name, &e.value)));
            o.insert("value".into(), e.value.clone());
            o.insert("list".into(), json!(e.value.is_array()));
            entries.push(v);
            pushed = true;
        }
        if !pushed {
            let mut v = base.clone();
            let o = v.as_object_mut().unwrap();
            o.insert("name".into(), json!(""));
            o.insert("kind".into(), json!("none"));
            o.insert("value".into(), Value::Null);
            entries.push(v);
        }
    }
    Value::Array(entries)
}

fn graph_json(root: &Path) -> Value {
    let mut flows: Vec<Value> = Vec::new();
    let mut services: Vec<Value> = Vec::new();
    let connectors: Vec<Value> = connector_graph(root);
    for (path, pkg) in vjs_files(root) {
        let Ok(src) = fs::read_to_string(&path) else { continue };
        let Ok(prog) = vjs::parse(&src) else { continue };
        let stem = path
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default();
        let is_service = path
            .parent()
            .and_then(|p| p.file_name())
            .map(|f| f == "services")
            .unwrap_or(false);
        let entry = json!({
            "file": path.display().to_string(),
            "name": if pkg == "default" { stem.clone() } else { format!("{pkg}:{stem}") },
            "sources": prog.source.clone().map(|s| vec![s]).unwrap_or_default(),
            "emits": prog.emit_subjects,
            "invokes": prog.invokes,
            "lang": "vjs",
            "pkg": pkg,
        });
        if is_service {
            services.push(entry);
        } else {
            flows.push(entry);
        }
    }
    json!({ "flows": flows, "services": services, "connectors": connectors })
}

fn preview_json(root: &Path, file: &str) -> Result<String, String> {
    let path = guard_path(root, file).ok_or("path not allowed")?;
    let src = fs::read_to_string(&path).map_err(|e| e.to_string())?;
    let prog = vjs::parse(&src)?;
    let fixture = fixture_path_for(&path);
    if !fixture.exists() {
        return Ok(json!({"fixture": null, "entries": {}, "emits": [], "error": null}).to_string());
    }
    let event: Value = serde_json::from_str(
        &fs::read_to_string(&fixture).map_err(|e| e.to_string())?,
    )
    .map_err(|e| format!("fixture: {e}"))?;
    // package context: services resolve relative to the file's package
    let mut engine = vjs::Engine::new(root.to_path_buf(), pkg_of_path(&path));
    let (emits, error, pipeline) = match vjs::run(&prog, &event, &mut engine) {
        Ok(ctx) => {
            let emits: Vec<Value> = ctx
                .emits
                .iter()
                .map(|(s, p)| json!({"subject": s, "payload": p}))
                .collect();
            let mut vars = ctx.vars.clone();
            vars.remove("event");
            (emits, Value::Null, Value::Object(vars))
        }
        Err(e) => (vec![], Value::String(e), Value::Null),
    };
    Ok(json!({
        "fixture": fixture.display().to_string(),
        "input": event,
        "entries": {},
        "emits": emits,
        "pipeline": pipeline,
        "error": error,
        "invokes": prog.invokes,
    })
    .to_string())
}

// ───────────────────────── file access guard ─────────────────────────

fn guard_path(root: &Path, rel: &str) -> Option<PathBuf> {
    if rel.contains("..") {
        return None;
    }
    let mut rel = rel.trim_start_matches("./");
    // The introspection endpoints emit paths as the runtime sees them, which
    // are absolute when VEJAS_ROOT is (e.g. /app in the container). The panel
    // round-trips those, so accept an absolute path IF it lives under root.
    if let Ok(stripped) = Path::new(rel).strip_prefix(root) {
        rel = stripped.to_str()?;
    }
    let p = root.join(rel);
    let ok_dir = ["flows", "services", "connectors", "packages"]
        .iter()
        .any(|d| rel.starts_with(d));
    let ok_ext = rel.ends_with(".vjs") || rel.ends_with(".json");
    if ok_dir && ok_ext {
        Some(p)
    } else {
        None
    }
}

// ───────────────────────── agent generation ─────────────────────────

/// The single-source VejasScript reference: served to agents over MCP
/// (`vejas_language`) and embedded in the generation prompts below.
const LANGUAGE_VJS: &str = r#"VejasScript in 20 lines:
  # comment
  source "vx.domain.name"            <- the flow's input subject, REQUIRED, line 1
  SEVERITY_CODES = {"critique": "P1", "haute": "P2"}   <- UPPERCASE literal dicts are transcoding tables the business expert edits
  ALERT_LEVELS = ["P1", "P2"]        <- UPPERCASE literal lists/scalars are editable constants
  x = priority                       <- the incoming event's top-level fields are variables; `event` is the whole document
  code = SEVERITY_CODES[priority] ?? "P3"
  email = lower(requester?.email)    <- builtins: upper lower trim len str num split join replace round abs; ?. is null-safe
  ids = orders[].id                  <- array projection
  big = orders[total > 100]          <- array filtering
  out = out + [{sku: l.sku}]         <- array concatenation builds lists inside a for
  fact = {source: "graph", in: 2}    <- doc keys and .field names may be ANY word, keywords included
  invoke format_alert(sev: code)     <- compose a service from services/<name>.vjs; its outputs MERGE into this pipeline
  d = invoke format_alert(sev: code) <- or capture its whole pipeline as a document
  invoke pkg:svc(k: v)               <- cross-package composition (the target package must list svc in its EXPORTS)
  key = secret("slack/webhook")      <- credentials resolve from the Vault at run time; NEVER a literal
  if code in ALERT_LEVELS:
      emit "vx.slack.out", {text: f"[{code}] {subject}"}
  end                                <- every if/for closes with `end`

Exposing a flow (instead of, or besides, `source`):
  tool "what calling this flow does" <- exposes the flow as an MCP tool
  api "POST /orders"                 <- expose the flow as a SYNCHRONOUS HTTP endpoint under /api (POST /api/orders)
  api "GET /orders/{id}"             <- a REST resource = several flows, ONE per verb; {path params} become event variables (here `id`)
  API_REQUEST = {customer: "string", total: "number"}   <- optional: typed request schema for the generated OpenAPI
  API_RESPONSE = {id: "string", status: "string"}       <- optional: typed 200 response schema
  respond 201, {id: id, status: "created"}   <- the SYNCHRONOUS HTTP response (status code + JSON body); `emit` still fires bus side-effects

Rules:
- Known sinks: vx.slack.out (payload {text: "..."}). All subjects start with "vx.".
- Put every business-meaningful value (thresholds, tables, queue names) in UPPERCASE literals.
- A flow file's first line is `# flow: <snake_case_name>`; it lives under flows/ (or packages/<pkg>/flows/).
- Its sample input lives at flows/fixtures/<flow>.json (or packages/<pkg>/fixtures/) — one JSON event.
- A flow is triggered by ONE of: `source "vx…"` (bus), `tool "…"` (MCP), or `api "VERB /path"` (HTTP). An `api` flow answers with `respond <status>, {body}`; the request's JSON body, {path params} and `query` are all in the event. The whole API is described at GET /api/openapi.json.
- A connector manifest's first line is `# connector: <name>`, then `driver "<name>"` (catalog: vejas_drivers) and UPPERCASE literal config; any credential uses secret("path/key"), never a literal."#;

const CONTRACT_VJS: &str = r#"You write ONE VejasScript file for the Vejas integration platform. Reply with ONLY the file content (no markdown fences, no commentary).

{language}

Task rules:
- First line must be: # flow: <snake_case_name>
- Existing flows (do not reuse names): {existing}

Task: {task}"#;

/// Is the agent CLI reachable in THIS deployment? The stock container ships
/// no CLI, so the generation tools must not be advertised there — an external
/// agent writes the .vjs itself (vejas_language + vejas_write_flow).
fn agent_available() -> bool {
    let agent = env::var("VEJAS_AGENT_CMD").unwrap_or_else(|_| "claude".into());
    if agent.contains('/') {
        return Path::new(&agent).exists();
    }
    env::var("PATH")
        .unwrap_or_default()
        .split(':')
        .any(|dir| !dir.is_empty() && Path::new(dir).join(&agent).exists())
}

const NO_AGENT_CLI: &str = "this deployment has no agent CLI (VEJAS_AGENT_CMD): write the file yourself — vejas_language gives the syntax, vejas_write_flow deploys it";

/// Ask the agent CLI for a file and return its content (markdown fences stripped).
fn ask_agent(prompt: String) -> Result<String, String> {
    let agent = env::var("VEJAS_AGENT_CMD").unwrap_or_else(|_| "claude".into());
    let out = Command::new(agent)
        .arg("-p")
        .arg(prompt)
        .output()
        .map_err(|e| format!("agent spawn: {e}"))?;
    if !out.status.success() {
        return Err(format!("agent: {}", String::from_utf8_lossy(&out.stderr)));
    }
    let mut code = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if code.starts_with("```") {
        code = code
            .trim_start_matches(|c| c != '\n')
            .trim_start_matches('\n')
            .trim_end_matches("```")
            .trim()
            .to_string();
    }
    code.push('\n');
    Ok(code)
}

fn unique_target(dir: &Path, name: &str) -> PathBuf {
    let mut target = dir.join(format!("{name}.vjs"));
    let mut n = 2;
    while target.exists() {
        target = dir.join(format!("{name}_{n}.vjs"));
        n += 1;
    }
    target
}

fn first_line_name(code: &str, prefix: &str, fallback: &str) -> String {
    code.lines()
        .next()
        .and_then(|l| l.strip_prefix(prefix))
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'))
        .unwrap_or_else(|| format!("{fallback}_{}", now_secs() % 100000))
}

fn generate_flow(root: &Path, prompt: &str) -> Result<String, String> {
    if !agent_available() {
        return Err(NO_AGENT_CLI.into());
    }
    let existing: Vec<String> = scan_all(root).iter().map(|s| s.name.clone()).collect();
    let full = CONTRACT_VJS
        .replace("{language}", LANGUAGE_VJS)
        .replace("{existing}", &existing.join(", "))
        .replace("{task}", prompt);
    let code = ask_agent(full)?;
    let prog = vjs::parse(&code)?;
    if prog.source.is_none() {
        return Err("generated flow has no `source` declaration".into());
    }
    let name = first_line_name(&code, "# flow:", "flow");
    let target = unique_target(&root.join("flows"), &name);
    fs::write(&target, &code).map_err(|e| e.to_string())?;
    Ok(json!({"ok": true, "file": target.display().to_string(), "flow": name, "lang": "vjs"})
        .to_string())
}

const CONTRACT_CONNECTOR: &str = r#"You write ONE Vejas connector manifest (.vjs). Reply with ONLY the file content (no markdown fences, no commentary).

A connector manifest binds a driver to config. Structure:
  # connector: <snake_case_name>       <- REQUIRED first line
  driver "<driver-name>"               <- REQUIRED, exactly one of the catalog below
  UPPERCASE_KEY = <literal>            <- config values (strings, numbers, {docs})
  SOME_SECRET = secret("path/key")     <- credentials: NEVER a literal, ALWAYS secret("...")

Available drivers (pick ONE; its description lists the config keys it needs):
{drivers}

Rules:
- Every bus subject starts with "vx.". A sink reads a SUBJECT; a source publishes to a SUBJECT.
- Any credential (token, password, a webhook URL that is secret) MUST use secret("path/key"), never a literal.
- If no built-in driver fits, use `exec-source` (CMD prints one JSON object per line on stdout, any language) or `exec-sink` (CMD reads a JSON body on stdin). Keep CMD to a single shell command.
- First line MUST be: # connector: <snake_case_name>
- Existing connectors (do not reuse names): {existing}

Task: {task}"#;

fn generate_connector(root: &Path, prompt: &str) -> Result<String, String> {
    if !agent_available() {
        return Err(NO_AGENT_CLI.into());
    }
    let drivers = connectors::catalog()
        .into_iter()
        .map(|(n, k, a)| format!("- {n} ({k}): {a}"))
        .collect::<Vec<_>>()
        .join("\n");
    let existing: Vec<String> = connector_graph(root)
        .iter()
        .filter_map(|c| c["name"].as_str().map(|s| s.to_string()))
        .collect();
    let full = CONTRACT_CONNECTOR
        .replace("{drivers}", &drivers)
        .replace("{existing}", &existing.join(", "))
        .replace("{task}", prompt);
    let code = ask_agent(full)?;
    let prog = vjs::parse(&code)?;
    let driver = prog
        .driver
        .clone()
        .ok_or("generated connector has no `driver` declaration")?;
    if connectors::driver_for(&driver).is_none() {
        return Err(format!("generated connector uses unknown driver {driver:?}"));
    }
    let name = first_line_name(&code, "# connector:", "connector");
    let dir = root.join("connectors");
    let _ = fs::create_dir_all(&dir);
    let target = unique_target(&dir, &name);
    fs::write(&target, &code).map_err(|e| e.to_string())?;
    Ok(json!({"ok": true, "file": target.display().to_string(), "connector": name, "driver": driver})
        .to_string())
}

// ───────────────────────── provision (templates) ─────────────────────────
//
// Instantiate a tenant package from a template directory: files under
// templates/<name>/ are rendered with ${param} substitution, parse-checked,
// written under packages/<tenant_slug>/, and started. This is the machine
// behind an operator's "connect your account" button — the runtime stays
// declarative (files on disk), the operator's app calls one operation.

fn valid_slug(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.chars().next().map(|c| c.is_ascii_lowercase()).unwrap_or(false)
        && s.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

fn valid_template_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// Params land inside executed .vjs files: a hostile value must not be able
/// to break out of a string literal or smuggle statements (that path ends at
/// an exec-source manifest). Whitelist, not blacklist.
fn valid_param_value(s: &str) -> bool {
    s.len() <= 512
        && s.chars().all(|c| {
            c.is_ascii_alphanumeric() || "-_./:?=&%$@#+, ".contains(c)
        })
}

fn render_template(src: &str, slug: &str, params: &serde_json::Map<String, Value>) -> Result<String, String> {
    let mut out = src.replace("${tenant_slug}", slug);
    for (k, v) in params {
        let Some(v) = v.as_str() else {
            return Err(format!("param {k:?} must be a string"));
        };
        out = out.replace(&format!("${{{k}}}"), v);
    }
    // an unfilled ${placeholder} means a missing param — refuse loudly
    let mut rest = out.as_str();
    while let Some(i) = rest.find("${") {
        let tail = &rest[i + 2..];
        if let Some(j) = tail.find('}') {
            let name = &tail[..j];
            if !name.is_empty()
                && name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
            {
                return Err(format!("missing param {name:?}"));
            }
        }
        rest = &rest[i + 2..];
    }
    Ok(out)
}

fn template_files(dir: &Path, base: &Path, out: &mut Vec<PathBuf>) {
    if let Ok(entries) = fs::read_dir(dir) {
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                template_files(&p, base, out);
            } else if let Ok(rel) = p.strip_prefix(base) {
                out.push(rel.to_path_buf());
            }
        }
    }
}

fn provision(
    registry: &Registry,
    root: &Path,
    template: &str,
    slug: &str,
    params: &serde_json::Map<String, Value>,
    force: bool,
) -> Result<Value, String> {
    if !valid_template_name(template) {
        return Err("invalid template name".into());
    }
    if !valid_slug(slug) {
        return Err("invalid tenant_slug (lowercase [a-z0-9_], starts with a letter)".into());
    }
    for (k, v) in params {
        if !k.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_') {
            return Err(format!("invalid param name {k:?}"));
        }
        match v.as_str() {
            Some(s) if valid_param_value(s) => {}
            Some(_) => return Err(format!("param {k:?} contains forbidden characters")),
            None => return Err(format!("param {k:?} must be a string")),
        }
    }
    let tdir = root.join("templates").join(template);
    if !tdir.is_dir() {
        return Err(format!("unknown template {template:?} (no templates/{template}/)"));
    }
    let pkg_dir = root.join("packages").join(slug);
    if pkg_dir.exists() && !force {
        return Err(format!(
            "package {slug:?} already exists — pass force to overwrite the template-rendered files (local corrections in them will be lost)"
        ));
    }
    // two phases: render + validate EVERYTHING in memory, then write
    let mut rels = Vec::new();
    template_files(&tdir, &tdir, &mut rels);
    if rels.is_empty() {
        return Err(format!("template {template:?} is empty"));
    }
    let mut rendered: Vec<(PathBuf, String)> = Vec::new();
    for rel in &rels {
        let src = fs::read_to_string(tdir.join(rel)).map_err(|e| e.to_string())?;
        let body = render_template(&src, slug, params)
            .map_err(|e| format!("{}: {e}", rel.display()))?;
        let name = rel.to_string_lossy();
        if name.ends_with(".vjs") {
            vjs::parse(&body).map_err(|e| format!("{}: {e}", rel.display()))?;
        } else if name.ends_with(".json") {
            serde_json::from_str::<Value>(&body)
                .map_err(|e| format!("{}: invalid JSON: {e}", rel.display()))?;
        }
        rendered.push((rel.clone(), body));
    }
    let mut created = Vec::new();
    for (rel, body) in &rendered {
        let target = pkg_dir.join(rel);
        if let Some(dir) = target.parent() {
            fs::create_dir_all(dir).map_err(|e| e.to_string())?;
        }
        fs::write(&target, body).map_err(|e| e.to_string())?;
        created.push(format!("packages/{slug}/{}", rel.display()));
    }
    let (_, _, _) = reload(registry, root);
    let started: Vec<String> = scan_all(root)
        .into_iter()
        .map(|s| s.name)
        .filter(|n| n.starts_with(&format!("flow:{slug}:")) || n.starts_with(&format!("connector:{slug}:")))
        .collect();
    // the provisioner's next step is writing these — hand it the exact list
    let store = secrets::default_store();
    let mut secret_refs = Vec::new();
    for (rel, body) in &rendered {
        if rel.to_string_lossy().ends_with(".vjs") {
            if let Ok(prog) = vjs::parse(body) {
                for (_, r) in prog.secret_refs {
                    if !secret_refs.iter().any(|e: &Value| e["ref"] == r.as_str()) {
                        let status = match store.get(&r) {
                            Ok(_) => "ok",
                            Err(_) => "missing",
                        };
                        secret_refs.push(json!({"ref": r, "status": status}));
                    }
                }
            }
        }
    }
    Ok(json!({
        "ok": true, "tenant": slug, "template": template,
        "created": created, "started": started, "secret_refs": secret_refs,
    }))
}

/// One synchronous test of a connector instance: evaluate its manifest with
/// the REAL secrets (a missing one fails here, in words), then run the
/// driver's probe — reach the remote side, touch nothing.
fn probe_connector(root: &Path, file: &str) -> Value {
    let verdict = (|| -> Result<String, String> {
        let path = guard_path(root, file).ok_or("path not allowed")?;
        let src = fs::read_to_string(&path).map_err(|e| e.to_string())?;
        let prog = vjs::parse(&src)?;
        let driver_name = prog.driver.clone().ok_or("not a connector manifest (no `driver`)")?;
        let driver = connectors::driver_for(&driver_name)
            .ok_or_else(|| format!("unknown driver {driver_name:?}"))?;
        let mut engine = vjs::Engine::new(root.to_path_buf(), pkg_of_path(&path));
        let cfg_ctx = vjs::run(&prog, &Value::Object(serde_json::Map::new()), &mut engine)
            .map_err(|e| connectors::humanize(&e))?;
        let mut config = serde_json::Map::new();
        for (k, v) in cfg_ctx.vars {
            if k != "event"
                && k.chars().all(|c| c.is_ascii_uppercase() || c == '_' || c.is_ascii_digit())
                && k.chars().any(|c| c.is_ascii_uppercase())
            {
                config.insert(k, v);
            }
        }
        let stem = path
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default();
        let ctx = connectors::Ctx {
            name: format!("probe:{stem}"),
            nats_url: String::new(),
            stream: String::new(),
            subj_root: env::var("VEJAS_SUBJECT_ROOT").unwrap_or_else(|_| "vx".into()),
            config: connectors::Config(config),
            running: Arc::new(AtomicBool::new(true)),
        };
        driver.probe(&ctx)
    })();
    match verdict {
        Ok(detail) => json!({"ok": true, "detail": detail}),
        Err(detail) => json!({"ok": false, "detail": detail}),
    }
}

/// Connector manifest paths (root connectors/ + packages/<pkg>/connectors/).
fn connector_manifest_paths(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut scan = |dir: PathBuf| {
        if let Ok(entries) = fs::read_dir(&dir) {
            for e in entries.flatten() {
                let p = e.path();
                let named = p.file_name().map(|f| f.to_string_lossy().to_string()).unwrap_or_default();
                if p.extension().map(|x| x == "vjs").unwrap_or(false) && !named.starts_with('_') {
                    out.push(p);
                }
            }
        }
    };
    scan(root.join("connectors"));
    if let Ok(pkgs) = fs::read_dir(root.join("packages")) {
        for p in pkgs.flatten() {
            scan(p.path().join("connectors"));
        }
    }
    out
}

/// Drive the interactive SAP connector over the bus: send one JSON op to the
/// exec-rpc connector's REQUEST_SUBJECT (found by evaluating connector
/// manifests, secret()-resolved) via NATS request/reply, return its reply. This
/// is how the sap_* MCP tools reach the live SAP without holding any state here.
fn sap_rpc_request(root: &Path, op: &Value) -> Result<Value, String> {
    let mut subject = None;
    for path in connector_manifest_paths(root) {
        let Ok(src) = fs::read_to_string(&path) else { continue };
        let Ok(prog) = vjs::parse(&src) else { continue };
        if prog.driver.as_deref() != Some("exec-rpc") {
            continue;
        }
        let mut engine = vjs::Engine::new(root.to_path_buf(), pkg_of_path(&path));
        let cfg = vjs::run(&prog, &Value::Object(serde_json::Map::new()), &mut engine)
            .map_err(|e| connectors::humanize(&e))?;
        for (k, v) in cfg.vars {
            if k == "REQUEST_SUBJECT" {
                if let Value::String(s) = v {
                    subject = Some(s);
                }
            }
        }
        if subject.is_some() {
            break;
        }
    }
    let subject = subject.ok_or(
        "no exec-rpc SAP connector found — add a manifest with driver \"exec-rpc\" and REQUEST_SUBJECT (see docs/examples/sap_rpc.vjs.example)",
    )?;
    let url = env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let nc = nats::connect(&url).map_err(|e| e.to_string())?;
    let payload = serde_json::to_vec(op).map_err(|e| e.to_string())?;
    let msg = nc
        .request_timeout(&subject, payload, Duration::from_secs(30))
        .map_err(|e| format!("SAP connector did not answer on {subject}: {e}"))?;
    serde_json::from_slice::<Value>(&msg.data).map_err(|e| e.to_string())
}

// ───────────────────────── dead-letter queue (ADR-0015) ─────────────────────────
fn dlq_js() -> Result<nats::jetstream::JetStream, String> {
    let url = env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let nc = nats::connect(&url).map_err(|e| e.to_string())?;
    let js = nats::jetstream::new(nc);
    connectors::ensure_dlq_stream(&js);
    Ok(js)
}

/// List dead letters, newest first (each carries its stream `seq` for replay/purge).
fn dlq_list(limit: usize) -> Result<Vec<Value>, String> {
    let js = dlq_js()?;
    let info = js.stream_info(connectors::DLQ_STREAM).map_err(|e| e.to_string())?;
    let (first, last) = (info.state.first_seq, info.state.last_seq);
    let mut out = Vec::new();
    if last == 0 || info.state.messages == 0 {
        return Ok(out);
    }
    let mut seq = last;
    loop {
        if let Ok(m) = js.get_message(connectors::DLQ_STREAM, seq) {
            if let Ok(mut env) = serde_json::from_slice::<Value>(&m.data) {
                if let Some(o) = env.as_object_mut() {
                    o.insert("seq".into(), json!(m.sequence));
                }
                out.push(env);
            }
        }
        if seq <= first || out.len() >= limit {
            break;
        }
        seq -= 1;
    }
    Ok(out)
}

/// Replay dead letters: re-publish each to its ORIGINAL subject (where the
/// corrected flow reprocesses it), then remove it from the DLQ. Publish-before-
/// delete, so a failed re-inject keeps the dead letter. By `seq`, by `unit`, or
/// all (both None).
fn dlq_replay(seq: Option<u64>, unit: Option<&str>) -> Result<Value, String> {
    let js = dlq_js()?;
    let mut targets: Vec<(u64, String, Vec<u8>)> = Vec::new();
    let take = |env: &Value| -> (u64, String, Vec<u8>) {
        (
            env["seq"].as_u64().unwrap_or(0),
            env["original_subject"].as_str().unwrap_or("").to_string(),
            env["payload"].as_str().unwrap_or("").as_bytes().to_vec(),
        )
    };
    if let Some(s) = seq {
        let m = js.get_message(connectors::DLQ_STREAM, s).map_err(|e| e.to_string())?;
        let mut env: Value = serde_json::from_slice(&m.data).map_err(|e| e.to_string())?;
        if let Some(o) = env.as_object_mut() {
            o.insert("seq".into(), json!(m.sequence));
        }
        targets.push(take(&env));
    } else {
        for env in dlq_list(100_000)? {
            if unit.map(|u| env["unit"].as_str() == Some(u)).unwrap_or(true) {
                targets.push(take(&env));
            }
        }
    }
    let (mut n, mut errs) = (0u64, Vec::new());
    for (s, subj, payload) in targets {
        if subj.is_empty() || s == 0 {
            continue;
        }
        match js.publish(&subj, payload) {
            Ok(_) => {
                let _ = js.delete_message(connectors::DLQ_STREAM, s);
                n += 1;
            }
            Err(e) => errs.push(format!("seq {s}: {e}")),
        }
    }
    Ok(json!({"replayed": n, "errors": errs}))
}

/// Discard dead letters without replaying — by `seq`, by `unit`, or all.
fn dlq_purge(seq: Option<u64>, unit: Option<&str>) -> Result<Value, String> {
    let js = dlq_js()?;
    if seq.is_none() && unit.is_none() {
        let r = js.purge_stream(connectors::DLQ_STREAM).map_err(|e| e.to_string())?;
        return Ok(json!({"purged": r.purged}));
    }
    let mut n = 0u64;
    if let Some(s) = seq {
        js.delete_message(connectors::DLQ_STREAM, s).map_err(|e| e.to_string())?;
        n = 1;
    } else if let Some(u) = unit {
        for env in dlq_list(100_000)? {
            if env["unit"].as_str() == Some(u) {
                if let Some(s) = env["seq"].as_u64() {
                    if js.delete_message(connectors::DLQ_STREAM, s).is_ok() {
                        n += 1;
                    }
                }
            }
        }
    }
    Ok(json!({"purged": n}))
}

// ───────────────────────── http ─────────────────────────

// ───────────────────────── MCP server ─────────────────────────
//
// The runtime IS the MCP server: JSON-RPC 2.0 over POST /mcp. The whole
// platform is drivable by any agent — inspect, edit, generate, run — and a
// flow/service that declares `tool "..."` is exposed as a first-class MCP tool
// (call it, it runs on the arguments and returns its emits). New tools are
// added by writing new flows; the MCP surface grows with the platform.

fn run_flow_on_input(root: &Path, file: &str, input: &Value) -> Result<Vec<Value>, String> {
    let path = guard_path(root, file).ok_or("path not allowed")?;
    let src = fs::read_to_string(&path).map_err(|e| e.to_string())?;
    let prog = vjs::parse(&src)?;
    let mut engine = vjs::Engine::new(root.to_path_buf(), pkg_of_path(&path));
    let ctx = vjs::run(&prog, input, &mut engine)?;
    Ok(ctx
        .emits
        .iter()
        .map(|(s, p)| json!({"subject": s, "payload": p}))
        .collect())
}

// ───────────────────────── HTTP API (flow-as-endpoint) ─────────────────────────
// A flow declares `api "VERB /path"`; the runtime routes (method, path) to it,
// binds {path params} into the event, runs it, and returns its `respond`. Several
// flows (one per verb) compose a REST resource. GET /api/openapi.json describes
// the whole surface. All served under /api/…

enum Seg {
    Lit(String),
    Param(String),
}

struct ApiRoute {
    method: String,
    segs: Vec<Seg>,
    file: String,
    summary: String,
    op_id: String,
    /// Optional typed schemas (field -> type name) from API_REQUEST / API_RESPONSE.
    req_schema: Option<Value>,
    resp_schema: Option<Value>,
}

/// Parse an `api` spec ("POST /orders", "GET /orders/{id}") into (method, segs).
fn parse_api_spec(spec: &str) -> Option<(String, Vec<Seg>)> {
    let mut it = spec.split_whitespace();
    let method = it.next()?.to_ascii_uppercase();
    let path = it.next()?;
    let segs = path
        .trim_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .map(|s| {
            if s.starts_with('{') && s.ends_with('}') {
                Seg::Param(s[1..s.len() - 1].to_string())
            } else {
                Seg::Lit(s.to_string())
            }
        })
        .collect();
    Some((method, segs))
}

/// Every flow that declares `api "..."`, as routes.
fn api_routes(root: &Path) -> Vec<ApiRoute> {
    let mut out = Vec::new();
    for (path, _pkg) in vjs_files(root) {
        let Ok(src) = fs::read_to_string(&path) else { continue };
        let Ok(prog) = vjs::parse(&src) else { continue };
        let Some(spec) = prog.api.clone() else { continue };
        let Some((method, segs)) = parse_api_spec(&spec) else { continue };
        let stem = path.file_stem().map(|s| s.to_string_lossy().to_string()).unwrap_or_default();
        let literal = |name: &str| prog.surface.iter().find(|e| e.name == name).map(|e| e.value.clone());
        out.push(ApiRoute {
            method,
            segs,
            file: path.display().to_string(),
            summary: prog.tool.clone().unwrap_or_else(|| stem.clone()),
            op_id: stem,
            req_schema: literal("API_REQUEST"),
            resp_schema: literal("API_RESPONSE"),
        });
    }
    out
}

/// Turn a `{field: "type"}` literal into a JSON-Schema object. Unknown types → string.
fn json_schema_from_literal(v: &Value) -> Value {
    let map_type = |t: &str| match t.to_ascii_lowercase().as_str() {
        "number" | "float" | "double" | "decimal" => "number",
        "int" | "integer" => "integer",
        "bool" | "boolean" => "boolean",
        "array" | "list" => "array",
        "object" | "map" => "object",
        _ => "string",
    };
    match v {
        Value::Object(o) => {
            let mut props = serde_json::Map::new();
            for (k, val) in o {
                let t = match val {
                    Value::String(s) => map_type(s),
                    Value::Object(_) => "object",
                    Value::Array(_) => "array",
                    _ => "string",
                };
                props.insert(k.clone(), json!({"type": t}));
            }
            json!({"type": "object", "properties": Value::Object(props)})
        }
        _ => json!({"type": "object"}),
    }
}

/// Match a request (method, path under /api/) to a route; capture path params.
fn match_api_route(root: &Path, method: &str, rel: &str) -> Option<(String, Vec<(String, String)>)> {
    let parts: Vec<&str> = rel.trim_matches('/').split('/').filter(|s| !s.is_empty()).collect();
    for r in api_routes(root) {
        if r.method != method || r.segs.len() != parts.len() {
            continue;
        }
        let mut params = Vec::new();
        let mut ok = true;
        for (seg, part) in r.segs.iter().zip(parts.iter()) {
            match seg {
                Seg::Lit(l) => {
                    if l != part {
                        ok = false;
                        break;
                    }
                }
                Seg::Param(name) => params.push((name.clone(), (*part).to_string())),
            }
        }
        if ok {
            return Some((r.file, params));
        }
    }
    None
}

fn method_str(m: &tiny_http::Method) -> String {
    m.as_str().to_ascii_uppercase()
}

fn parse_query(url: &str) -> Value {
    let mut q = serde_json::Map::new();
    if let Some(qs) = url.split('?').nth(1) {
        for pair in qs.split('&') {
            if pair.is_empty() {
                continue;
            }
            let mut kv = pair.splitn(2, '=');
            let k = kv.next().unwrap_or("").to_string();
            let v = kv.next().unwrap_or("").to_string();
            if !k.is_empty() {
                q.insert(k, Value::String(v));
            }
        }
    }
    Value::Object(q)
}

/// Run a flow and return its full context (emits + response).
fn run_flow_ctx(root: &Path, file: &str, input: &Value) -> Result<vjs::Ctx, String> {
    let path = guard_path(root, file).ok_or("path not allowed")?;
    let src = fs::read_to_string(&path).map_err(|e| e.to_string())?;
    let prog = vjs::parse(&src)?;
    let mut engine = vjs::Engine::new(root.to_path_buf(), pkg_of_path(&path));
    vjs::run(&prog, input, &mut engine)
}

/// Publish an API flow's side-effect emits to the bus (best-effort).
fn publish_emits(emits: &[(String, Value)]) {
    if emits.is_empty() {
        return;
    }
    let url = env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    if let Ok(nc) = nats::connect(&url) {
        for (subj, payload) in emits {
            let _ = nc.publish(subj, serde_json::to_vec(payload).unwrap_or_default());
        }
        let _ = nc.flush();
    }
}

/// Run an API flow on the request event; map its `respond` to (status, body).
fn run_api_flow(root: &Path, file: &str, input: &Value) -> (u16, String) {
    match run_flow_ctx(root, file, input) {
        Ok(ctx) => {
            publish_emits(&ctx.emits);
            match ctx.response {
                Some((status, body)) => {
                    let code = status
                        .as_u64()
                        .or_else(|| status.as_str().and_then(|s| s.parse().ok()))
                        .unwrap_or(200) as u16;
                    (code, body.to_string())
                }
                None => (
                    200,
                    json!({"ok": true, "emits": ctx.emits.iter().map(|(s, p)| json!({"subject": s, "payload": p})).collect::<Vec<_>>()}).to_string(),
                ),
            }
        }
        Err(e) => (500, json!({"error": e}).to_string()),
    }
}

/// The OpenAPI 3.0 document for every `api` flow. Info configurable via env.
fn openapi_json(root: &Path) -> Value {
    let mut paths = serde_json::Map::new();
    for r in api_routes(root) {
        let path_str = format!(
            "/{}",
            r.segs
                .iter()
                .map(|s| match s {
                    Seg::Lit(l) => l.clone(),
                    Seg::Param(p) => format!("{{{p}}}"),
                })
                .collect::<Vec<_>>()
                .join("/")
        );
        let params: Vec<Value> = r
            .segs
            .iter()
            .filter_map(|s| match s {
                Seg::Param(p) => Some(json!({
                    "name": p, "in": "path", "required": true, "schema": {"type": "string"}
                })),
                _ => None,
            })
            .collect();
        // Tag operations by their resource (the first literal path segment).
        let tag = r
            .segs
            .iter()
            .find_map(|s| match s {
                Seg::Lit(l) => Some(l.clone()),
                _ => None,
            })
            .unwrap_or_else(|| "default".into());
        let ok_schema = r
            .resp_schema
            .as_ref()
            .map(json_schema_from_literal)
            .unwrap_or_else(|| json!({"type": "object"}));
        let mut op = json!({
            "summary": r.summary,
            "operationId": r.op_id,
            "tags": [tag],
            "responses": {
                "200": {"description": "OK", "content": {"application/json": {"schema": ok_schema}}},
                "500": {"description": "Flow error"}
            },
        });
        if !params.is_empty() {
            op["parameters"] = Value::Array(params);
        }
        if matches!(r.method.as_str(), "POST" | "PUT" | "PATCH") {
            let req_schema = r
                .req_schema
                .as_ref()
                .map(json_schema_from_literal)
                .unwrap_or_else(|| json!({"type": "object"}));
            op["requestBody"] = json!({
                "required": true,
                "content": {"application/json": {"schema": req_schema}}
            });
        }
        let entry = paths.entry(path_str).or_insert_with(|| json!({}));
        entry[r.method.to_ascii_lowercase()] = op;
    }
    json!({
        "openapi": "3.0.3",
        "info": {
            "title": env::var("VEJAS_API_TITLE").unwrap_or_else(|_| "Vejas API".into()),
            "version": env::var("VEJAS_API_VERSION").unwrap_or_else(|_| "1.0.0".into()),
            "description": env::var("VEJAS_API_DESCRIPTION").unwrap_or_default(),
        },
        "servers": [{"url": "/api"}],
        "paths": paths,
    })
}

/// Flows/services that declare `tool "..."`, as (mcp_tool_name, file, description).
fn tool_flows(root: &Path) -> Vec<(String, String, String)> {
    let mut out = Vec::new();
    for (path, pkg) in vjs_files(root) {
        let Ok(src) = fs::read_to_string(&path) else { continue };
        let Ok(prog) = vjs::parse(&src) else { continue };
        let Some(desc) = prog.tool else { continue };
        let stem = path.file_stem().map(|s| s.to_string_lossy().to_string()).unwrap_or_default();
        let name = if pkg == "default" {
            format!("flow_{stem}")
        } else {
            format!("flow_{pkg}_{stem}")
        };
        out.push((name, path.display().to_string(), desc));
    }
    out
}

fn mcp_tools(root: &Path) -> Value {
    let obj = |props: Value, req: Vec<&str>| {
        json!({"type": "object", "properties": props, "required": req})
    };
    let mut tools = vec![
        json!({"name": "vejas_topology", "description": "List running flows and connectors with their status.", "inputSchema": obj(json!({}), vec![])}),
        json!({"name": "vejas_graph", "description": "The pipeline graph: sources, flows, composed services, destinations, connectors.", "inputSchema": obj(json!({}), vec![])}),
        json!({"name": "vejas_surface", "description": "The business surface of every flow: mappings, transcoding tables, constants.", "inputSchema": obj(json!({}), vec![])}),
        json!({"name": "vejas_language", "description": "The VejasScript reference: grammar, builtins, and the rules for flow files and connector manifests. Read this before writing any .vjs.", "inputSchema": obj(json!({}), vec![])}),
        json!({"name": "vejas_read", "description": "Read a script file (.vjs) or fixture (.json).", "inputSchema": obj(json!({"path": {"type": "string"}}), vec!["path"])}),
        json!({"name": "vejas_write_flow", "description": "Create or overwrite a .vjs script (parse-validated, hot-reloaded) or a .json fixture. path under flows/, connectors/, or packages/<pkg>/flows|services|fixtures.", "inputSchema": obj(json!({"path": {"type": "string"}, "content": {"type": "string"}}), vec!["path", "content"])}),
        json!({"name": "vejas_set_literal", "description": "Rewrite one literal of the business surface in place (constant, or a table/mapping entry via key).", "inputSchema": obj(json!({"file": {"type": "string"}, "name": {"type": "string"}, "key": {"type": "string", "description": "entry key, or '-' for a whole constant"}, "value": {}}), vec!["file", "name", "value"])}),
        json!({"name": "vejas_replay_literal", "description": "Shadow-replay a proposed literal change against REAL persisted traffic: hydrate the flow's recent events from JetStream (read-only, the bus untouched — falls back to the in-memory trace ring when the stream is empty or the flow has no bus source), rerun them against the current AND the patched script, and return the before/after emit diff (with `source`: jetstream|trace-ring). Nothing is written — promote with vejas_set_literal.", "inputSchema": obj(json!({"file": {"type": "string"}, "name": {"type": "string"}, "key": {"type": "string", "description": "entry key, or '-' for a whole constant"}, "value": {}, "n": {"type": "integer", "description": "how many recent events to replay (default 20)"}}), vec!["file", "name", "value"])}),
        json!({"name": "vejas_preview", "description": "Run a flow on its fixture and return the emitted messages plus the final pipeline.", "inputSchema": obj(json!({"file": {"type": "string"}}), vec!["file"])}),
        json!({"name": "vejas_run_flow", "description": "Run any flow on a supplied input event and return its emits (does not touch the bus).", "inputSchema": obj(json!({"file": {"type": "string"}, "input": {"type": "object"}}), vec!["file", "input"])}),
        json!({"name": "vejas_events", "description": "The most recent events processed by the flows — subject, ok/error, emitted subjects, payload preview — newest first. Optional filter: flow (e.g. \"flow:stripe_alerts\").", "inputSchema": obj(json!({"flow": {"type": "string"}}), vec![])}),
        json!({"name": "vejas_reload", "description": "Rescan flows and packages; start new, stop removed, restart changed.", "inputSchema": obj(json!({}), vec![])}),
        json!({"name": "vejas_drivers", "description": "List the available connector drivers (name, kind, description) for writing connector manifests.", "inputSchema": obj(json!({}), vec![])}),
        json!({"name": "vejas_secrets", "description": "The secret references declared by flows and connectors, who uses each, and whether it RESOLVES against the store — references and statuses only, never values.", "inputSchema": obj(json!({}), vec![])}),
        json!({"name": "vejas_set_secret", "description": "Write one secret value into the store (rotation included) and restart the units that reference it. WRITE-ONLY: no surface ever returns the value.", "inputSchema": obj(json!({"ref": {"type": "string", "description": "the secret(\"path/key\") reference"}, "value": {"type": "string"}}), vec!["ref", "value"])}),
        json!({"name": "vejas_test_connector", "description": "Synchronously test one connector instance: evaluate its manifest with the real secrets, reach the remote side with the driver's probe, touch nothing. Returns {ok, detail} in plain words.", "inputSchema": obj(json!({"file": {"type": "string"}}), vec!["file"])}),
        json!({"name": "vejas_provision", "description": "Instantiate a tenant package from a template (templates/<name>/, ${param} substitution, every file parse-checked, hot-started). Returns created files, started units and the secret references left to write. Refuses an existing package unless force (which overwrites template-rendered files).", "inputSchema": obj(json!({"template": {"type": "string"}, "tenant_slug": {"type": "string"}, "params": {"type": "object"}, "force": {"type": "boolean"}}), vec!["template", "tenant_slug"])}),
        json!({"name": "sap_list", "description": "List SAP function modules (BAPIs/RFCs) on the live system whose name matches a pattern (SAP wildcards, e.g. \"BAPI_USER*\"). Needs an exec-rpc SAP connector running (driver \"exec-rpc\").", "inputSchema": obj(json!({"pattern": {"type": "string"}}), vec![])}),
        json!({"name": "sap_describe", "description": "Describe a SAP function module's interface: every parameter's name, direction (import/export/changing/tables), type and length. Read this before sap_call.", "inputSchema": obj(json!({"func": {"type": "string"}}), vec!["func"])}),
        json!({"name": "sap_call", "description": "Call a SAP function module (BAPI/RFC) on the live system and return its outputs. `import` values may be scalars, structures (a JSON object) or tables (an array of row objects); every EXPORT/CHANGING scalar & structure and every TABLES parameter comes back auto-marshalled from metadata. `max_rows` caps table output. E.g. func=\"RFC_READ_TABLE\", import={\"QUERY_TABLE\":\"T000\"}.", "inputSchema": obj(json!({"func": {"type": "string"}, "import": {"type": "object"}, "max_rows": {"type": "integer"}}), vec!["func"])}),
        json!({"name": "sap_send_idoc", "description": "Send an IDoc INTO SAP via transactional RFC (exactly-once). `control` is the EDI_DC40 control record (e.g. {TABNAM:\"EDI_DC40\", IDOCTYP:\"MATMAS05\", MESTYP:\"MATMAS\", SNDPRN, SNDPRT:\"LS\", RCVPRN, RCVPRT:\"LS\", DIRECT:\"2\"}); `data` is the array of EDI_DD40 segments ({SEGNAM, SDATA}). Pass a stable `tid` (24 chars) derived from an idempotency key for dedup across retries; omit for a fresh one. Returns the transaction id.", "inputSchema": obj(json!({"control": {"type": "object"}, "data": {"type": "array"}, "tid": {"type": "string"}}), vec!["control", "data"])}),
        json!({"name": "vejas_dlq", "description": "List dead letters — poison messages parked in the DLQ (ADR-0015) instead of dropped: unit, original subject, attempts, last error, payload, each with a `seq` for replay/purge. Newest first.", "inputSchema": obj(json!({"limit": {"type": "integer"}}), vec![])}),
        json!({"name": "vejas_dlq_replay", "description": "Replay dead letters — re-inject each to its ORIGINAL subject so the (now corrected) flow reprocesses it, then remove it from the DLQ. Target one by `seq`, a whole `unit`, or all (omit both). Do this AFTER fixing the cause (vejas_set_literal, previewed with vejas_replay_literal).", "inputSchema": obj(json!({"seq": {"type": "integer"}, "unit": {"type": "string"}}), vec![])}),
        json!({"name": "vejas_dlq_purge", "description": "Discard dead letters without replaying — by `seq`, by `unit`, or all (omit both).", "inputSchema": obj(json!({"seq": {"type": "integer"}, "unit": {"type": "string"}}), vec![])}),
    ];
    // generation-by-prompt shells out to the agent CLI: only advertised where
    // one exists (the stock container has none — external agents write .vjs)
    if agent_available() {
        tools.push(json!({"name": "vejas_new_flow", "description": "Ask the agent to write a new VejasScript flow from a natural-language request; it lands running.", "inputSchema": obj(json!({"prompt": {"type": "string"}}), vec!["prompt"])}));
        tools.push(json!({"name": "vejas_new_connector", "description": "Ask the agent to write a new connector manifest from a natural-language request (picks a driver, writes config, uses secret() for credentials); it lands running.", "inputSchema": obj(json!({"prompt": {"type": "string"}}), vec!["prompt"])}));
    }
    // flow-as-tool: dynamic tools declared by `tool "..."` in a flow/service
    for (name, _file, desc) in tool_flows(root) {
        tools.push(json!({
            "name": name,
            "description": desc,
            "inputSchema": {"type": "object", "description": "the input event for this flow"},
        }));
    }
    Value::Array(tools)
}

fn mcp_call(root: &Path, registry: &Registry, name: &str, args: &Value) -> Result<Value, String> {
    let text = |v: String| Ok(json!({"content": [{"type": "text", "text": v}]}));
    match name {
        "vejas_topology" => text(topology_json(registry).to_string()),
        "vejas_graph" => text(graph_json(root).to_string()),
        "vejas_surface" => text(surface_json(root).to_string()),
        "vejas_read" => {
            let p = args["path"].as_str().ok_or("path required")?;
            let path = guard_path(root, p).ok_or("path not allowed")?;
            text(fs::read_to_string(&path).map_err(|e| e.to_string())?)
        }
        "vejas_language" => text(LANGUAGE_VJS.to_string()),
        "vejas_events" => text(events_json(args["flow"].as_str())),
        "vejas_write_flow" => {
            let p = args["path"].as_str().ok_or("path required")?;
            let content = args["content"].as_str().ok_or("content required")?;
            let path = guard_path(root, p).ok_or("path not allowed")?;
            // validate before writing: .vjs must parse, .json (fixtures) must be JSON
            if p.ends_with(".vjs") {
                vjs::parse(content)?;
            } else if p.ends_with(".json") {
                serde_json::from_str::<Value>(content)
                    .map_err(|e| format!("fixture must be valid JSON: {e}"))?;
            } else {
                return Err("only .vjs or .json files".into());
            }
            fs::write(&path, content).map_err(|e| e.to_string())?;
            let (_, started, stopped) = reload(registry, root);
            text(json!({"ok": true, "started": started, "stopped": stopped}).to_string())
        }
        "vejas_set_literal" => {
            let file = args["file"].as_str().ok_or("file required")?;
            let lname = args["name"].as_str().ok_or("name required")?;
            let key = args["key"].as_str().unwrap_or("-");
            let path = guard_path(root, file).ok_or("path not allowed")?;
            let src = fs::read_to_string(&path).map_err(|e| e.to_string())?;
            let new_src = vjs::set_literal(&src, lname, key, &args["value"])?;
            fs::write(&path, new_src).map_err(|e| e.to_string())?;
            let _ = reload(registry, root);
            text(json!({"ok": true}).to_string())
        }
        "vejas_replay_literal" => {
            let file = args["file"].as_str().ok_or("file required")?;
            let lname = args["name"].as_str().ok_or("name required")?;
            let key = args["key"].as_str().unwrap_or("-");
            let n = args["n"].as_u64().unwrap_or(20) as usize;
            text(replay_literal(root, file, lname, key, &args["value"], n)?.to_string())
        }
        "vejas_preview" => {
            let file = args["file"].as_str().ok_or("file required")?;
            text(preview_json(root, file)?)
        }
        "vejas_run_flow" => {
            let file = args["file"].as_str().ok_or("file required")?;
            let emits = run_flow_on_input(root, file, &args["input"])?;
            text(json!({"emits": emits}).to_string())
        }
        "vejas_new_flow" => {
            let prompt = args["prompt"].as_str().ok_or("prompt required")?;
            let res = generate_flow(root, prompt)?;
            let _ = reload(registry, root);
            text(res)
        }
        "vejas_new_connector" => {
            let prompt = args["prompt"].as_str().ok_or("prompt required")?;
            let res = generate_connector(root, prompt)?;
            let _ = reload(registry, root);
            text(res)
        }
        "vejas_reload" => {
            let (total, started, stopped) = reload(registry, root);
            text(json!({"total": total, "started": started, "stopped": stopped}).to_string())
        }
        "vejas_drivers" => {
            let list: Vec<Value> = connectors::catalog()
                .into_iter()
                .map(|(n, k, a)| json!({"driver": n, "kind": k, "about": a}))
                .collect();
            text(Value::Array(list).to_string())
        }
        "vejas_secrets" => text(secrets_json(root)),
        "vejas_set_secret" => {
            let r = args["ref"].as_str().ok_or("ref required")?;
            let v = args["value"].as_str().ok_or("value required")?;
            if v.is_empty() {
                return Err("empty value".into());
            }
            let restarted = set_secret(registry, root, r, v)?;
            text(json!({"ok": true, "ref": r, "restarted": restarted}).to_string())
        }
        "vejas_test_connector" => {
            let file = args["file"].as_str().ok_or("file required")?;
            text(probe_connector(root, file).to_string())
        }
        "vejas_provision" => {
            let template = args["template"].as_str().ok_or("template required")?;
            let slug = args["tenant_slug"].as_str().ok_or("tenant_slug required")?;
            let params = args["params"].as_object().cloned().unwrap_or_default();
            let force = args["force"].as_bool().unwrap_or(false);
            text(provision(registry, root, template, slug, &params, force)?.to_string())
        }
        "sap_list" => {
            let pattern = args.get("pattern").and_then(|v| v.as_str()).unwrap_or("*");
            text(sap_rpc_request(root, &json!({"op": "list", "pattern": pattern}))?.to_string())
        }
        "sap_describe" => {
            let func = args["func"].as_str().ok_or("func required")?;
            text(sap_rpc_request(root, &json!({"op": "describe", "func": func}))?.to_string())
        }
        "sap_call" => {
            let func = args["func"].as_str().ok_or("func required")?;
            let mut op = json!({"op": "call", "func": func});
            if let Some(imp) = args.get("import") {
                op["import"] = imp.clone();
            }
            if let Some(mr) = args.get("max_rows") {
                op["max_rows"] = mr.clone();
            }
            text(sap_rpc_request(root, &op)?.to_string())
        }
        "sap_send_idoc" => {
            let mut op = json!({"op": "send_idoc"});
            op["control"] = args.get("control").cloned().unwrap_or(json!({}));
            op["data"] = args.get("data").cloned().unwrap_or(json!([]));
            if let Some(t) = args.get("tid") {
                op["tid"] = t.clone();
            }
            text(sap_rpc_request(root, &op)?.to_string())
        }
        "vejas_dlq" => {
            let limit = args.get("limit").and_then(|v| v.as_u64()).unwrap_or(100) as usize;
            text(json!({"dead_letters": dlq_list(limit)?}).to_string())
        }
        "vejas_dlq_replay" => {
            let seq = args.get("seq").and_then(|v| v.as_u64());
            let unit = args.get("unit").and_then(|v| v.as_str());
            text(dlq_replay(seq, unit)?.to_string())
        }
        "vejas_dlq_purge" => {
            let seq = args.get("seq").and_then(|v| v.as_u64());
            let unit = args.get("unit").and_then(|v| v.as_str());
            text(dlq_purge(seq, unit)?.to_string())
        }
        other => {
            // flow-as-tool: run the declaring flow on the arguments
            if let Some((_, file, _)) = tool_flows(root).into_iter().find(|(n, _, _)| n == other) {
                let emits = run_flow_on_input(root, &file, args)?;
                text(json!({"emits": emits}).to_string())
            } else {
                Err(format!("unknown tool {other:?}"))
            }
        }
    }
}

fn mcp_dispatch(root: &Path, registry: &Registry, req: &Value) -> Option<Value> {
    let id = req.get("id").cloned();
    let method = req["method"].as_str().unwrap_or("");
    let ok = |result: Value| {
        Some(json!({"jsonrpc": "2.0", "id": id, "result": result}))
    };
    let err = |code: i64, msg: String| {
        Some(json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": msg}}))
    };
    match method {
        "initialize" => ok(json!({
            "protocolVersion": "2024-11-05",
            "capabilities": {"tools": {"listChanged": true}},
            "serverInfo": {"name": "vejas", "version": env!("CARGO_PKG_VERSION")},
        })),
        "notifications/initialized" | "notifications/cancelled" => None, // notifications: no reply
        "ping" => ok(json!({})),
        "tools/list" => ok(json!({"tools": mcp_tools(root)})),
        "tools/call" => {
            let params = &req["params"];
            let name = params["name"].as_str().unwrap_or("");
            let args = params.get("arguments").cloned().unwrap_or(json!({}));
            match mcp_call(root, registry, name, &args) {
                Ok(result) => ok(result),
                Err(e) => ok(json!({"content": [{"type": "text", "text": format!("error: {e}")}], "isError": true})),
            }
        }
        _ => err(-32601, format!("method not found: {method}")),
    }
}

fn respond(request: tiny_http::Request, code: u16, body: String, ctype: &str) {
    let header = tiny_http::Header::from_bytes(&b"Content-Type"[..], ctype.as_bytes()).unwrap();
    let _ = request.respond(
        tiny_http::Response::from_string(body)
            .with_status_code(code)
            .with_header(header),
    );
}

fn read_body(request: &mut tiny_http::Request) -> Value {
    let mut body = String::new();
    let _ = request.as_reader().read_to_string(&mut body);
    serde_json::from_str(&body).unwrap_or(Value::Null)
}

fn qparam<'a>(url: &'a str, key: &str) -> Option<String> {
    let q = url.split_once('?')?.1;
    for pair in q.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            if k == key {
                return Some(v.replace("%2F", "/").replace("%2f", "/"));
            }
        }
    }
    None
}

fn handle_request(mut request: tiny_http::Request, registry: Registry, root: PathBuf) {
    let method = request.method().clone();
    let url = request.url().to_string();
    let path_only = url.split('?').next().unwrap_or("").to_string();
    // Optional write protection: when VEJAS_TOKEN is set, every POST (the
    // whole mutating surface, /mcp included) requires `Authorization: Bearer
    // <token>`. Reads stay open — they expose no secret values by design.
    if method == tiny_http::Method::Post {
        if let Ok(token) = env::var("VEJAS_TOKEN") {
            if !token.is_empty() {
                let authorized = request.headers().iter().any(|h| {
                    h.field.equiv("authorization")
                        && h.value
                            .as_str()
                            .strip_prefix("Bearer ")
                            .map(|t| t == token)
                            .unwrap_or(false)
                });
                if !authorized {
                    return respond(
                        request,
                        401,
                        "unauthorized: set Authorization: Bearer <VEJAS_TOKEN>".into(),
                        "text/plain",
                    );
                }
            }
        }
    }
    // ── HTTP API surface: flows declared `api "VERB /path"`, served under /api/ ──
    if path_only == "/api/openapi.json" {
        return respond(request, 200, openapi_json(&root).to_string(), "application/json");
    }
    if let Some(rel) = path_only.strip_prefix("/api/") {
        let m = method_str(&method);
        return match match_api_route(&root, &m, rel) {
            Some((file, params)) => {
                let body = read_body(&mut request);
                let mut ev = match body {
                    Value::Object(o) => o,
                    _ => serde_json::Map::new(),
                };
                for (k, v) in params {
                    ev.insert(k, Value::String(v));
                }
                ev.insert("query".into(), parse_query(&url));
                let (code, out) = run_api_flow(&root, &file, &Value::Object(ev));
                respond(request, code, out, "application/json")
            }
            None => respond(
                request,
                404,
                json!({"error": "no API route", "method": m, "path": format!("/{rel}")}).to_string(),
                "application/json",
            ),
        };
    }

    match (method, path_only.as_str()) {
        (tiny_http::Method::Get, "/") | (tiny_http::Method::Get, "/panel") => respond(
            request,
            200,
            include_str!("panel.html").to_string(),
            "text/html; charset=utf-8",
        ),
        (tiny_http::Method::Get, "/healthz") => respond(request, 200, "ok".into(), "text/plain"),
        (tiny_http::Method::Get, "/metrics") => {
            // Prometheus exposition. Gauges (unit/kind/restarts) come live from
            // the supervision registry; counters & histograms from the metrics
            // module accumulated on the flow hot path.
            let gauges: Vec<(String, String, u64)> = {
                let reg = registry.lock().unwrap();
                reg.values()
                    .map(|h| {
                        let kind = match h.spec.kind {
                            Kind::Connector => "connector",
                            Kind::Flow => "flow",
                        };
                        let restarts = h.state.lock().unwrap().restarts;
                        (h.spec.name.clone(), kind.to_string(), restarts)
                    })
                    .collect()
            };
            respond(
                request,
                200,
                metrics::render(&gauges),
                "text/plain; version=0.0.4",
            )
        }
        (tiny_http::Method::Get, "/topology") => respond(
            request,
            200,
            topology_json(&registry).to_string(),
            "application/json",
        ),
        (tiny_http::Method::Get, "/graph") => respond(
            request,
            200,
            graph_json(&root).to_string(),
            "application/json",
        ),
        (tiny_http::Method::Get, "/surface") => respond(
            request,
            200,
            surface_json(&root).to_string(),
            "application/json",
        ),
        (tiny_http::Method::Get, "/events") => {
            let flow = qparam(&url, "flow");
            respond(request, 200, events_json(flow.as_deref()), "application/json")
        }
        (tiny_http::Method::Get, "/dlq") => match dlq_list(200) {
            Ok(l) => respond(request, 200, json!({"dead_letters": l}).to_string(), "application/json"),
            Err(e) => respond(request, 500, e, "text/plain"),
        },
        (tiny_http::Method::Post, "/dlq/replay") => {
            let body = read_body(&mut request);
            let r = dlq_replay(body.get("seq").and_then(|v| v.as_u64()), body.get("unit").and_then(|v| v.as_str()));
            match r {
                Ok(v) => respond(request, 200, v.to_string(), "application/json"),
                Err(e) => respond(request, 422, e, "text/plain"),
            }
        }
        (tiny_http::Method::Post, "/dlq/purge") => {
            let body = read_body(&mut request);
            let r = dlq_purge(body.get("seq").and_then(|v| v.as_u64()), body.get("unit").and_then(|v| v.as_str()));
            match r {
                Ok(v) => respond(request, 200, v.to_string(), "application/json"),
                Err(e) => respond(request, 422, e, "text/plain"),
            }
        }
        (tiny_http::Method::Get, "/drivers") => {
            let list: Vec<Value> = connectors::catalog()
                .into_iter()
                .map(|(n, k, a)| json!({"driver": n, "kind": k, "about": a}))
                .collect();
            respond(request, 200, Value::Array(list).to_string(), "application/json")
        }
        (tiny_http::Method::Get, "/secrets") => {
            respond(request, 200, secrets_json(&root), "application/json")
        }
        (tiny_http::Method::Post, "/secrets/set") => {
            let body = read_body(&mut request);
            let (Some(r), Some(v)) = (body["ref"].as_str(), body["value"].as_str()) else {
                return respond(request, 400, "need ref+value".into(), "text/plain");
            };
            if v.is_empty() {
                return respond(request, 400, "empty value".into(), "text/plain");
            }
            match set_secret(&registry, &root, r, v) {
                Ok(restarted) => respond(
                    request,
                    200,
                    json!({"ok": true, "ref": r, "restarted": restarted}).to_string(),
                    "application/json",
                ),
                Err(e) => respond(request, 422, e, "text/plain"),
            }
        }
        (tiny_http::Method::Post, "/connectors/test") => {
            let body = read_body(&mut request);
            let Some(file) = body["file"].as_str() else {
                return respond(request, 400, "need file".into(), "text/plain");
            };
            respond(request, 200, probe_connector(&root, file).to_string(), "application/json")
        }
        (tiny_http::Method::Post, "/provision") => {
            let body = read_body(&mut request);
            let (Some(template), Some(slug)) =
                (body["template"].as_str(), body["tenant_slug"].as_str())
            else {
                return respond(request, 400, "need template+tenant_slug".into(), "text/plain");
            };
            let params = body["params"].as_object().cloned().unwrap_or_default();
            let force = body["force"].as_bool().unwrap_or(false);
            match provision(&registry, &root, template, slug, &params, force) {
                Ok(out) => respond(request, 200, out.to_string(), "application/json"),
                Err(e) => respond(request, 422, e, "text/plain"),
            }
        }
        (tiny_http::Method::Get, "/preview") => {
            let Some(file) = qparam(&url, "file") else {
                return respond(request, 400, "missing file".into(), "text/plain");
            };
            match preview_json(&root, &file) {
                Ok(json) => respond(request, 200, json, "application/json"),
                Err(err) => respond(request, 500, err, "text/plain"),
            }
        }
        (tiny_http::Method::Get, "/file") => {
            let Some(p) = qparam(&url, "path").and_then(|p| guard_path(&root, &p)) else {
                return respond(request, 400, "path not allowed".into(), "text/plain");
            };
            match fs::read_to_string(&p) {
                Ok(content) => respond(
                    request,
                    200,
                    json!({"path": p.display().to_string(), "content": content}).to_string(),
                    "application/json",
                ),
                Err(e) => respond(request, 404, e.to_string(), "text/plain"),
            }
        }
        (tiny_http::Method::Post, "/file/set") => {
            let body = read_body(&mut request);
            let (Some(pstr), Some(content)) =
                (body["path"].as_str(), body["content"].as_str())
            else {
                return respond(request, 400, "need path+content".into(), "text/plain");
            };
            let Some(p) = guard_path(&root, pstr) else {
                return respond(request, 400, "path not allowed".into(), "text/plain");
            };
            // validate (parse) before writing
            if pstr.ends_with(".vjs") {
                if let Err(e) = vjs::parse(content) {
                    return respond(request, 422, e, "text/plain");
                }
            }
            if let Err(e) = fs::write(&p, content) {
                return respond(request, 500, e.to_string(), "text/plain");
            }
            let (_, started, stopped) = reload(&registry, &root);
            respond(
                request,
                200,
                json!({"ok": true, "started": started, "stopped": stopped}).to_string(),
                "application/json",
            )
        }
        (tiny_http::Method::Get, "/fixture") => {
            let Some(file) = qparam(&url, "file") else {
                return respond(request, 400, "missing file".into(), "text/plain");
            };
            let Some(p) = guard_path(&root, &file) else {
                return respond(request, 400, "path not allowed".into(), "text/plain");
            };
            let fx = fixture_path_for(&p);
            let content = fs::read_to_string(&fx).unwrap_or_else(|_| "{}".into());
            respond(
                request,
                200,
                json!({"path": fx.display().to_string(), "content": content}).to_string(),
                "application/json",
            )
        }
        (tiny_http::Method::Post, "/fixture/set") => {
            let body = read_body(&mut request);
            let (Some(file), Some(content)) = (body["file"].as_str(), body["content"].as_str())
            else {
                return respond(request, 400, "need file+content".into(), "text/plain");
            };
            let Some(p) = guard_path(&root, file) else {
                return respond(request, 400, "path not allowed".into(), "text/plain");
            };
            if serde_json::from_str::<Value>(content).is_err() {
                return respond(request, 422, "fixture must be valid JSON".into(), "text/plain");
            }
            let fx = fixture_path_for(&p);
            if let Some(dir) = fx.parent() {
                let _ = fs::create_dir_all(dir);
            }
            match fs::write(&fx, content) {
                Ok(_) => respond(request, 200, json!({"ok": true}).to_string(), "application/json"),
                Err(e) => respond(request, 500, e.to_string(), "text/plain"),
            }
        }
        (tiny_http::Method::Post, "/surface/replay") => {
            let body = read_body(&mut request);
            let file = body["file"].as_str().unwrap_or("").to_string();
            let name = body["name"].as_str().unwrap_or("").to_string();
            let key = body["key"].as_str().unwrap_or("-").to_string();
            let n = body["n"].as_u64().unwrap_or(20) as usize;
            match replay_literal(&root, &file, &name, &key, &body["value"], n) {
                Ok(diff) => respond(request, 200, diff.to_string(), "application/json"),
                Err(e) => respond(request, 422, e, "text/plain"),
            }
        }
        (tiny_http::Method::Post, "/surface/set") => {
            let body = read_body(&mut request);
            let file = body["file"].as_str().unwrap_or("").to_string();
            let name = body["name"].as_str().unwrap_or("").to_string();
            let key = body["key"].as_str().unwrap_or("-").to_string();
            let value = body["value"].clone();
            let Some(p) = guard_path(&root, &file) else {
                return respond(request, 400, "path not allowed".into(), "text/plain");
            };
            let src = match fs::read_to_string(&p) {
                Ok(s) => s,
                Err(e) => return respond(request, 500, e.to_string(), "text/plain"),
            };
            match vjs::set_literal(&src, &name, &key, &value) {
                Ok(new_src) => {
                    if let Err(e) = fs::write(&p, new_src) {
                        return respond(request, 500, e.to_string(), "text/plain");
                    }
                    let _ = reload(&registry, &root);
                    respond(
                        request,
                        200,
                        json!({"ok": true, "file": file, "name": name, "key": key}).to_string(),
                        "application/json",
                    )
                }
                Err(e) => respond(request, 422, e, "text/plain"),
            }
        }
        (tiny_http::Method::Post, "/flows/new") => {
            let body = read_body(&mut request);
            let prompt = body["prompt"].as_str().unwrap_or("").to_string();
            if prompt.trim().is_empty() {
                return respond(request, 400, "missing prompt".into(), "text/plain");
            }
            eprintln!("[vejas] asking the agent for a new flow…");
            match generate_flow(&root, &prompt) {
                Ok(json) => {
                    let (_, started, _) = reload(&registry, &root);
                    eprintln!("[vejas] agent flow landed ({started} started)");
                    respond(request, 200, json, "application/json")
                }
                Err(err) => respond(request, 422, err, "text/plain"),
            }
        }
        (tiny_http::Method::Post, "/connectors/new") => {
            let body = read_body(&mut request);
            let prompt = body["prompt"].as_str().unwrap_or("").to_string();
            if prompt.trim().is_empty() {
                return respond(request, 400, "missing prompt".into(), "text/plain");
            }
            eprintln!("[vejas] asking the agent for a new connector…");
            match generate_connector(&root, &prompt) {
                Ok(json) => {
                    let (_, started, _) = reload(&registry, &root);
                    eprintln!("[vejas] agent connector landed ({started} started)");
                    respond(request, 200, json, "application/json")
                }
                Err(err) => respond(request, 422, err, "text/plain"),
            }
        }
        (tiny_http::Method::Post, "/reload") => {
            let (total, started, stopped) = reload(&registry, &root);
            respond(
                request,
                200,
                json!({"total": total, "started": started, "stopped": stopped}).to_string(),
                "application/json",
            )
        }
        (tiny_http::Method::Post, "/mcp") => {
            let body = read_body(&mut request);
            // support a JSON-RPC batch or a single request
            let out = if let Some(arr) = body.as_array() {
                let replies: Vec<Value> = arr
                    .iter()
                    .filter_map(|r| mcp_dispatch(&root, &registry, r))
                    .collect();
                if replies.is_empty() {
                    return respond(request, 202, String::new(), "application/json");
                }
                Value::Array(replies)
            } else {
                match mcp_dispatch(&root, &registry, &body) {
                    Some(reply) => reply,
                    None => return respond(request, 202, String::new(), "application/json"),
                }
            };
            respond(request, 200, out.to_string(), "application/json")
        }
        _ => respond(request, 404, "not found".into(), "text/plain"),
    }
}

fn serve_http(registry: Registry, root: PathBuf, addr: String) {
    let server = match tiny_http::Server::http(&addr) {
        Ok(server) => server,
        Err(err) => {
            eprintln!("[vejas] cannot bind {addr}: {err}");
            return;
        }
    };
    eprintln!("[vejas] panel + monitoring on http://{addr}");
    for request in server.incoming_requests() {
        let registry = registry.clone();
        let root = root.clone();
        thread::spawn(move || handle_request(request, registry, root));
        if !RUNNING.load(Ordering::SeqCst) {
            break;
        }
    }
}

fn main() {
    // vjs one-shot helpers for scripting/tests:
    //   vejas-runtime vjs-check <file>
    //   vejas-runtime vjs-run <file> <fixture.json>
    let args: Vec<String> = env::args().collect();
    if args.len() >= 3 && args[1] == "vjs-check" {
        match fs::read_to_string(&args[2]).map_err(|e| e.to_string()).and_then(|s| vjs::parse(&s).map(|_| ())) {
            Ok(()) => {
                println!("ok");
                return;
            }
            Err(e) => {
                eprintln!("{e}");
                std::process::exit(1);
            }
        }
    }
    if args.len() >= 4 && args[1] == "vjs-run" {
        let src = fs::read_to_string(&args[2]).expect("read flow");
        let prog = vjs::parse(&src).expect("parse");
        let event: Value =
            serde_json::from_str(&fs::read_to_string(&args[3]).expect("read fixture"))
                .expect("fixture json");
        let root = PathBuf::from(env::var("VEJAS_ROOT").unwrap_or_else(|_| ".".into()));
        let mut engine = vjs::Engine::new(root, pkg_of_path(&PathBuf::from(&args[2])));
        match vjs::run(&prog, &event, &mut engine) {
            Ok(ctx) => {
                for (s, p) in &ctx.emits {
                    println!("emit {s} {p}");
                }
                return;
            }
            Err(e) => {
                eprintln!("{e}");
                std::process::exit(1);
            }
        }
    }

    // Golden-test runner: vejas-runtime vjs-test <dir>
    // Each <dir>/*.json case: {"flow": path, "input": {...} | "input_file": path,
    //   "expect_emits": [{"subject": s, "payload": {...}}]} or {"expect_error": "substring"}.
    if args.len() >= 3 && args[1] == "vjs-test" {
        let root = PathBuf::from(env::var("VEJAS_ROOT").unwrap_or_else(|_| ".".into()));
        let dir = PathBuf::from(&args[2]);
        let mut files: Vec<PathBuf> = fs::read_dir(&dir)
            .expect("test dir")
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().map(|x| x == "json").unwrap_or(false))
            .collect();
        files.sort();
        let (mut pass, mut fail) = (0, 0);
        for case_path in files {
            let case: Value =
                serde_json::from_str(&fs::read_to_string(&case_path).unwrap()).unwrap();
            let name = case_path.file_stem().unwrap().to_string_lossy().to_string();
            let flow = case["flow"].as_str().expect("case.flow");
            let input: Value = if let Some(f) = case["input_file"].as_str() {
                serde_json::from_str(&fs::read_to_string(f).unwrap()).unwrap()
            } else {
                case["input"].clone()
            };
            let run = fs::read_to_string(flow)
                .map_err(|e| e.to_string())
                .and_then(|src| vjs::parse(&src))
                .and_then(|prog| {
                    let mut engine =
                        vjs::Engine::new(root.clone(), pkg_of_path(&PathBuf::from(flow)));
                    vjs::run(&prog, &input, &mut engine)
                        .map(|ctx| ctx.emits)
                });
            let verdict: Result<(), String> = match (&run, case["expect_error"].as_str()) {
                (Err(e), Some(want)) => {
                    if e.contains(want) {
                        Ok(())
                    } else {
                        Err(format!("error mismatch: got {e:?}, want substring {want:?}"))
                    }
                }
                (Err(e), None) => Err(format!("unexpected error: {e}")),
                (Ok(_), Some(want)) => Err(format!("expected error {want:?}, flow succeeded")),
                (Ok(emits), None) => {
                    let got: Value = emits
                        .iter()
                        .map(|(s, p)| json!({"subject": s, "payload": p}))
                        .collect::<Vec<_>>()
                        .into();
                    let want = case["expect_emits"].clone();
                    if got == want {
                        Ok(())
                    } else {
                        Err(format!(
                            "emits mismatch\n     got:  {got}\n     want: {want}"
                        ))
                    }
                }
            };
            match verdict {
                Ok(()) => {
                    pass += 1;
                    println!("  ✓ {name}");
                }
                Err(e) => {
                    fail += 1;
                    println!("  ✗ {name}: {e}");
                }
            }
        }
        println!("{pass} passed, {fail} failed");
        std::process::exit(if fail > 0 { 1 } else { 0 });
    }

    let root = PathBuf::from(env::var("VEJAS_ROOT").unwrap_or_else(|_| ".".into()));
    let addr = env::var("VEJAS_HTTP_ADDR").unwrap_or_else(|_| "0.0.0.0:8686".into());
    let registry: Registry = Arc::new(Mutex::new(HashMap::new()));

    // Traces to an OTLP collector iff OTEL_EXPORTER_OTLP_ENDPOINT is set; metrics
    // are always scrapeable at GET /metrics. No-op (no thread) when unset.
    metrics::otlp_init();

    ctrlc::set_handler(|| {
        eprintln!("[vejas] shutdown requested");
        RUNNING.store(false, Ordering::SeqCst);
    })
    .expect("signal handler");

    // flows AND declared connector instances start here (reload supervises both)
    let (total, started, _) = reload(&registry, &root);
    eprintln!("[vejas] supervising {total} units ({started} started)");

    // the control channel, only when this runtime names a tenant (ADR-0013)
    control::start(registry.clone(), root.clone());

    {
        let registry = registry.clone();
        let root = root.clone();
        thread::spawn(move || serve_http(registry, root, addr));
    }

    while RUNNING.load(Ordering::SeqCst) {
        thread::sleep(Duration::from_millis(300));
    }
    let reg = registry.lock().unwrap();
    for handle in reg.values() {
        handle.stop.store(true, Ordering::SeqCst);
    }
    drop(reg);
    thread::sleep(Duration::from_millis(800));
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // NOTE: TRACES is process-global and tests run in parallel — every test
    // must use its own unique flow name.

    #[test]
    fn replay_literal_diffs_on_traced_events() {
        let root = std::env::temp_dir().join(format!("vejas-replay-{}", std::process::id()));
        std::fs::create_dir_all(root.join("flows")).unwrap();
        std::fs::write(
            root.join("flows").join("replay_probe.vjs"),
            "source \"vx.rp.test\"\nTH = 500\nif amount > TH:\n  emit \"vx.out\", {a: amount}\nend\n",
        )
        .unwrap();
        record_trace("flow:replay_probe", "vx.rp.test", true, None, vec!["vx.out".into()], "{}".into(), Some(json!({"amount": 700})));
        record_trace("flow:replay_probe", "vx.rp.test", true, None, vec![], "{}".into(), Some(json!({"amount": 300})));
        let diff = replay_literal(&root, "flows/replay_probe.vjs", "TH", "-", &json!(200), 20).unwrap();
        assert_eq!(diff["events"], 2);
        // 700 emitted before and after; 300 emits only after the change
        assert_eq!(diff["changed"], 1);
        let changed: Vec<&Value> = diff["results"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|r| r["changed"] == true)
            .collect();
        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0]["before"]["emits"].as_array().unwrap().len(), 0);
        assert_eq!(changed[0]["after"]["emits"].as_array().unwrap().len(), 1);
        // nothing was written: the file still holds the original threshold
        let src = std::fs::read_to_string(root.join("flows").join("replay_probe.vjs")).unwrap();
        assert!(src.contains("TH = 500"));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn provision_renders_checks_and_guards() {
        let root = std::env::temp_dir().join(format!("vejas-prov-{}", std::process::id()));
        let t = root.join("templates").join("demo");
        std::fs::create_dir_all(t.join("connectors")).unwrap();
        std::fs::write(t.join("package.vjs"), "ENABLED = true\nEXPORTS = []\n").unwrap();
        std::fs::write(
            t.join("connectors").join("hb.vjs"),
            "# connector: hb\ndriver \"timer\"\nSUBJECT = \"vx.${tenant_slug}.ping\"\nINTERVAL_SECS = 300\nPAYLOAD = {origin: \"${origin}\"}\n",
        )
        .unwrap();
        let registry: Registry = Arc::new(Mutex::new(HashMap::new()));
        let mut params = serde_json::Map::new();
        params.insert("origin".into(), json!("acme"));
        let out = provision(&registry, &root, "demo", "acme_it", &params, false).unwrap();
        assert_eq!(out["ok"], true);
        let rendered =
            std::fs::read_to_string(root.join("packages/acme_it/connectors/hb.vjs")).unwrap();
        assert!(rendered.contains("vx.acme_it.ping"));
        assert!(rendered.contains("\"acme\""));
        // create-only by default; force overwrites
        assert!(provision(&registry, &root, "demo", "acme_it", &params, false)
            .unwrap_err()
            .contains("already exists"));
        assert!(provision(&registry, &root, "demo", "acme_it", &params, true).is_ok());
        // missing param -> loud refusal, nothing written
        let empty = serde_json::Map::new();
        assert!(provision(&registry, &root, "demo", "fresh_t", &empty, false)
            .unwrap_err()
            .contains("missing param"));
        assert!(!root.join("packages/fresh_t").exists());
        // injection guard: a value that could escape a string literal is refused
        let mut evil = serde_json::Map::new();
        evil.insert("origin".into(), json!("x\"\nCMD = \"rm -rf /\""));
        assert!(provision(&registry, &root, "demo", "evil_t", &evil, false)
            .unwrap_err()
            .contains("forbidden characters"));
        // slug and template names are pinned
        assert!(provision(&registry, &root, "demo", "../oops", &params, false).is_err());
        assert!(provision(&registry, &root, "../etc", "okslug", &params, false).is_err());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn guard_path_accepts_absolute_only_under_root() {
        let root = Path::new("/srv/vejas");
        assert!(guard_path(root, "flows/x.vjs").is_some());
        assert_eq!(
            guard_path(root, "/srv/vejas/flows/x.vjs"),
            guard_path(root, "flows/x.vjs"),
        );
        assert!(guard_path(root, "/etc/passwd").is_none());
        assert!(guard_path(root, "/srv/vejas/core/x.vjs").is_none());
        assert!(guard_path(root, "flows/../core/x.vjs").is_none());
    }

    #[test]
    fn surface_includes_connector_manifests() {
        let root = std::env::temp_dir().join(format!("vejas-surf-{}", std::process::id()));
        std::fs::create_dir_all(root.join("connectors")).unwrap();
        std::fs::write(
            root.join("connectors").join("surf_probe.vjs"),
            "# connector: surf_probe\ndriver \"timer\"\nSUBJECT = \"vx.surf.probe\"\nINTERVAL_SECS = 30\n",
        )
        .unwrap();
        let v = surface_json(&root);
        let entries: Vec<&Value> = v
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["file"].as_str().unwrap_or("").ends_with("surf_probe.vjs"))
            .collect();
        assert_eq!(entries.len(), 2, "SUBJECT + INTERVAL_SECS");
        for e in &entries {
            assert_eq!(e["connector"], true);
            assert_eq!(e["driver"], "timer");
            assert_eq!(e["driver_kind"], "source:interval");
        }
        assert!(entries.iter().any(|e| e["name"] == "SUBJECT"));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn events_json_strips_the_full_event() {
        record_trace("flow:strip_probe", "vx.sp.test", true, None, vec![], "{\"a\":1}".into(), Some(json!({"a": 1})));
        let out: Value = serde_json::from_str(&events_json(Some("flow:strip_probe"))).unwrap();
        let entry = &out["events"][0];
        assert_eq!(entry["flow"], "flow:strip_probe");
        assert!(entry.get("event").is_none(), "full event must not leave the runtime via /events");
        assert_eq!(entry["preview"], "{\"a\":1}");
    }
}
