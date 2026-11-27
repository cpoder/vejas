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

mod cluster;
mod connectors;
mod control;
mod metrics;
mod mqtt;
mod proposals;
mod record;
mod secrets;
mod signature;
mod versions;
mod vjs;

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{env, fs, thread};

use serde_json::{json, Value};

// The bus, the trace context and what a unit shares with every connector
// come from the connector crate (ADR-0038).
use vejas_connector::{bus, trace, MAX_DELIVERIES, RUNNING};

#[derive(Clone, PartialEq)]
enum Kind {
    Flow,
    Connector,
    /// A pattern-detection unit (ADR-0031): a VPL program under `detects/`,
    /// run by the embedded Varpulis engine.
    Detect,
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

// ───────────────────────── event trace ─────────────────────────
//
// The last events each flow processed, in memory (ring of 50 per flow) — the
// minimal honest answer to "what just went through, and did it work?". Served
// by GET /events, the vejas_events MCP tool, and the panel's live strip.

static TRACES: OnceLock<Mutex<HashMap<String, VecDeque<Value>>>> = OnceLock::new();
static TRACE_SEQ: AtomicU64 = AtomicU64::new(0);


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

#[allow(clippy::too_many_arguments)]
pub fn record_trace(
    flow: &str,
    subject: &str,
    ok: bool,
    error: Option<String>,
    emits: Vec<String>,
    preview: String,
    event: Option<Value>,
    trace: Option<&trace::Hop>,
    delivery: Option<bus::Delivery>,
) {
    record_trace_full(flow, subject, ok, error, emits, preview, event, None, trace, delivery)
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
    trace: Option<&trace::Hop>,
    delivery: Option<bus::Delivery>,
) {
    // The trace this hop belongs to (ADR-0034): what an operator copies into
    // the tracing backend to see the whole path of this event.
    let (trace_id, span_id) = match trace {
        Some(hop) => (Some(hop.trace_hex()), Some(format!("{:016x}", hop.span_id))),
        None => (None, None),
    };
    let seq = TRACE_SEQ.fetch_add(1, Ordering::SeqCst);
    let mut map = TRACES.get_or_init(|| Mutex::new(HashMap::new())).lock().unwrap();
    let ring = map.entry(unit.to_string()).or_default();
    ring.push_back(json!({
        "seq": seq, "ts": now_secs(), "flow": unit, "subject": subject,
        "ok": ok, "error": error, "emits": emits, "preview": preview,
        "response": response, "trace_id": trace_id, "span_id": span_id,
        // the JetStream message the hop consumed and which delivery of it this
        // was: a retry shares the message, another message of the trace does not
        "stream_seq": delivery.as_ref().map(|d| d.stream_seq),
        "attempt": delivery.as_ref().map(|d| d.delivered),
        // a dead letter's replay: the message that died, the same event
        "replay_of": delivery.as_ref().and_then(|d| d.replay_of),
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
    events_out(flow, false)
}

/// One unit's events, each with the other hops of its path (the entries that
/// share its trace id), for a page that follows events end to end. A unit that
/// sends often (a timer) never pushes a quiet flow's events out of it, as it
/// does out of the newest hundred of all units.
fn events_with_paths_json(unit: &str) -> String {
    events_out(Some(unit), true)
}

/// The other hops `/events?paths=1` gives with a unit's events, the newest.
const PATH_HOPS_MAX: usize = 200;

/// What a page or an agent is shown of the bus (ADR-0017), masked as the
/// trace record masks it: credential-named keys and pairs, and the values the
/// secret stores handed out. Only the fields that carry what went through the
/// bus, by their names in the runtime's answers; units, sequences, times and
/// trace ids stay as they are, since pages find events and letters by them.
fn as_shown(mut v: Value) -> Value {
    fn walk(v: &mut Value, scrub: &record::Shown) {
        match v {
            Value::Object(o) => {
                for (k, x) in o.iter_mut() {
                    match (k.as_str(), &mut *x) {
                        // the start of a longer text: a hop's payload, a sink's answer
                        ("preview" | "response", Value::String(s)) => *s = scrub.text(s, true),
                        // what failed, which may quote the start of an answer
                        ("error" | "last_error" | "detail", Value::String(s)) => *s = scrub.error(s),
                        // where it went: a subject can carry a value of the event
                        ("subject" | "original_subject", Value::String(s)) => *s = scrub.text(s, false),
                        // what went through: a dead letter's payload, as the
                        // record keeps a payload; an emitted event's, whole
                        ("payload", Value::String(s)) => *s = scrub.payload(s),
                        ("payload" | "event", x) => {
                            let shown = scrub.json(x);
                            *x = shown;
                        }
                        // what a hop or a run emitted, one by one: one event
                        // too deep to read stays an event among the others
                        ("emits", Value::Array(emits)) => {
                            for e in emits {
                                match e {
                                    Value::String(subject) => *subject = scrub.text(subject, false),
                                    e => walk(e, scrub),
                                }
                            }
                        }
                        (_, x) => walk(x, scrub),
                    }
                }
            }
            Value::Array(a) => a.iter_mut().for_each(|x| walk(x, scrub)),
            _ => {}
        }
    }
    walk(&mut v, &record::shown());
    v
}

/// Proposals as pages and agents are shown them: the change as it was
/// proposed, since it goes into the repository as it is and an agent may
/// revise it; the evidence, events of the bus, masked (ADR-0034).
fn proposals_shown(list: Vec<Value>) -> Vec<Value> {
    let scrub = record::shown();
    list.into_iter().map(|p| proposal_shown(p, &scrub)).collect()
}

/// One proposal, or a decision's answer that holds it under `proposal`.
fn proposal_shown(mut p: Value, scrub: &record::Shown) -> Value {
    if let Some(inner) = p.get_mut("proposal") {
        let shown = proposal_shown(inner.take(), scrub);
        *inner = shown;
    }
    if let Some(evidence) = p.get_mut("evidence") {
        let shown = scrub.json(evidence);
        *evidence = shown;
    }
    p
}

/// What a shadow run reads of a real event (time travel, the canary, a
/// replay, a golden case), and the fields it reads that it does not see
/// (ADR-0034). A candidate is any code, and a replay's literal can name the
/// field a flow reads (`event[FIELD]`), so a credential must not enter the
/// run: `{pw: event.password}` would carry it out under a name no mask reads.
/// As with `secret()`, its value never enters. Flags and nulls do
/// (`record::Shown::shadow_input`), and so does a number neither named as a
/// credential nor holding a value handed out. A branch on a masked field may
/// go another way than live, and the answer names that field. `reads` are what the run's code reads of the
/// event (`vjs::event_reads`): a field it does not read changes nothing, and
/// is not named.
fn shadow_input(event: &Value, scrub: &record::Shown, reads: &[String]) -> (Value, Vec<String>) {
    let (read, mut hidden) = scrub.shadow_input(event);
    if !reads.iter().any(|r| r == "event") {
        hidden.retain(|path| path == "event" || reads.iter().any(|r| path.split(['.', '[']).next() == Some(r.as_str())));
    }
    hidden.truncate(HIDDEN_MAX);
    (read, hidden)
}

/// The most fields a shadow run's result names as hidden.
const HIDDEN_MAX: usize = 20;

/// A shadow run's result, naming the fields it read hidden, when there are.
fn with_hidden(mut result: Value, hidden: Vec<String>) -> Value {
    if !hidden.is_empty() {
        result["hidden"] = json!(hidden);
    }
    result
}

fn events_out(flow: Option<&str>, paths: bool) -> String {
    let newest_first = |hops: &mut Vec<&Value>| {
        hops.sort_unstable_by_key(|e| std::cmp::Reverse(e["seq"].as_u64().unwrap_or(0)))
    };
    // Every message's hop waits on this lock: under it, only the choice, a copy
    // of what goes out and its preview (bounded); the serialising after.
    let events: Vec<Value> = {
        let map = TRACES.get_or_init(|| Mutex::new(HashMap::new())).lock().unwrap();
        let mut picked: Vec<&Value> = match flow {
            Some(f) => {
                // the unit's own ring (50) whole; the other hops capped, so a
                // busy path never pushes the unit's oldest events out
                let mut own: Vec<&Value> = map.get(f).map(|ring| ring.iter().collect()).unwrap_or_default();
                if paths {
                    let traces: HashSet<&str> = own.iter().filter_map(|e| e["trace_id"].as_str()).collect();
                    let mut others: Vec<&Value> = map
                        .iter()
                        .filter(|(name, _)| name.as_str() != f)
                        .flat_map(|(_, ring)| ring.iter())
                        .filter(|e| e["trace_id"].as_str().is_some_and(|t| traces.contains(t)))
                        .collect();
                    newest_first(&mut others);
                    others.truncate(PATH_HOPS_MAX);
                    own.extend(others);
                }
                own
            }
            None => map.values().flat_map(|ring| ring.iter()).collect(),
        };
        newest_first(&mut picked);
        if flow.is_none() {
            picked.truncate(100);
        }
        picked
            .into_iter()
            .map(|e| {
                let mut out = serde_json::Map::new();
                if let Some(o) = e.as_object() {
                    // the stored event stays in (replay fuel); a copy of the rest
                    for (k, v) in o.iter().filter(|(k, _)| k.as_str() != "event") {
                        out.insert(k.clone(), v.clone());
                    }
                    // Hot loops store the event and no preview (one JSON walk
                    // less per message); derive it here, on read.
                    if out.get("preview").and_then(Value::as_str).is_none_or(str::is_empty) {
                        let pv = o.get("event").map(preview_of).unwrap_or_default();
                        out.insert("preview".into(), Value::String(pv));
                    }
                }
                Value::Object(out)
            })
            .collect()
    };
    as_shown(json!({ "events": events })).to_string()
}

/// A unit's file as traces and the panel name it: relative to the project
/// root when it is under it.
fn relative_to(root: &Path, path: &Path) -> String {
    path.strip_prefix(root).unwrap_or(path).display().to_string()
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
            let js = bus::connect(&url).ok()?.jetstream();
            connectors::hydrate_recent(&js, &stream, subject, n).ok()
        });
        match hydrated {
            Some(rows) if !rows.is_empty() => (
                rows.into_iter()
                    .map(|(seq, subject, event)| {
                        json!({
                            "ts": Value::Null,
                            "seq": seq,
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
    let mut results = Vec::new();
    let (mut changed_count, mut not_recorded) = (0, 0);
    let scrub = record::shown();
    let reads = [vjs::event_reads(&before), vjs::event_reads(&after)].concat();
    for entry in &events {
        let Some(ev) = entry.get("event").filter(|ev| !ev.is_null()) else {
            continue; // unparseable event of a bad-json trace: nothing to replay
        };
        let (ev, hidden) = shadow_input(ev, &scrub, &reads);
        let b = shadow_emits(&before, &ev, root, &pkg);
        let a = shadow_emits(&after, &ev, root, &pkg);
        let changed = compared(&b, &a);
        match changed {
            Some(true) => changed_count += 1,
            None => not_recorded += 1,
            Some(false) => {}
        }
        let result = json!({
            "ts": entry["ts"], "subject": entry["subject"], "preview": trace_preview(entry),
            "before": b, "after": a, "changed": changed == Some(true),
        });
        results.push(with_hidden(result, hidden));
    }
    Ok(json!({
        "file": file, "name": name, "key": key, "value": value,
        "source": replay_source,
        "events": results.len(), "changed": changed_count, "not_recorded": not_recorded, "results": results,
    }))
}

/// Time-travel (ADR-0021): replay a window of REAL persisted traffic through both
/// the current effective version and a whole CANDIDATE version, and return the
/// side-by-side emit diff joined by the source message's stream sequence. This is
/// `replay_literal` generalised from a literal patch to an arbitrary new source.
/// The shadow invariant holds by construction — `vjs::run` collects emits, it
/// never publishes, so nothing a candidate emits reaches a real subject — and
/// `Engine::shadow` holds no secret value, so a candidate cannot read one back.
fn time_travel(root: &Path, file: &str, candidate_src: &str, n: usize) -> Result<Value, String> {
    let path = guard_path(root, file).ok_or("path not allowed")?;
    let baseline = fs::read_to_string(&path).map_err(|e| e.to_string())?;
    let unit = flow_proc_name(&path);
    let pkg = pkg_of_path(&path);
    let url = env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let stream = env::var("VEJAS_STREAM").unwrap_or_else(|_| "VEJAS".into());
    let js = bus::connect(&url).map_err(|e| format!("NATS unreachable: {e}"))?.jetstream();
    // diff against the CURRENT EFFECTIVE version (a live overlay if one is valid)
    let store = versions::open_store(&js);
    let (current_src, _) = versions::resolve_source(store.as_ref(), &unit, &baseline, now_secs());
    let current = vjs::parse(&current_src)?;
    let candidate = vjs::parse(candidate_src)?;
    let source_subj = current
        .source
        .clone()
        .ok_or("flow has no bus source to replay")?;
    let rows = connectors::hydrate_recent(&js, &stream, &source_subj, n)?;
    let mut results = Vec::new();
    let (mut changed, mut not_recorded) = (0, 0);
    let scrub = record::shown();
    let reads = [vjs::event_reads(&current), vjs::event_reads(&candidate)].concat();
    for (seq, subject, event) in &rows {
        let (event, hidden) = shadow_input(event, &scrub, &reads);
        let b = shadow_emits(&current, &event, root, &pkg);
        let a = shadow_emits(&candidate, &event, root, &pkg);
        let ch = compared(&b, &a);
        match ch {
            Some(true) => changed += 1,
            None => not_recorded += 1,
            Some(false) => {}
        }
        let result = json!({
            "seq": seq, "subject": subject, "preview": preview_of(&event),
            "before": b, "after": a, "changed": ch == Some(true),
        });
        results.push(with_hidden(result, hidden));
    }
    Ok(json!({
        "file": file, "flow": unit, "candidate_hash": versions::hash_content(candidate_src),
        "events": results.len(), "changed": changed, "not_recorded": not_recorded, "results": results,
    }))
}

// ───────────────────────── canary (ADR-0021, inc 3) ─────────────────────────
//
// A canary is time-travel on the TAIL of the live stream: a shadow consumer
// follows the live source subject (deliver-new, never acked), runs each new event
// through both the current and the candidate version in the shadow engine, and
// accumulates the diff. Zero real side effects — the same shadow invariant as
// time-travel. Recordings are an in-memory bounded ring for v1.

struct Canary {
    candidate_hash: String,
    started_at: u64,
    running: Arc<AtomicBool>,
    total: Arc<AtomicU64>,
    changed: Arc<AtomicU64>,
    /// Events left out: a run made a call whose answer is not recorded.
    not_recorded: Arc<AtomicU64>,
    ring: Arc<Mutex<VecDeque<Value>>>,
    // set when the canary auto-stops (the live version changed under it, so the
    // accumulated diff would compare against a stale baseline) — never a silently
    // wrong diff.
    stop_reason: Arc<Mutex<Option<String>>>,
}
static CANARIES: OnceLock<Mutex<HashMap<String, Canary>>> = OnceLock::new();
fn canaries() -> &'static Mutex<HashMap<String, Canary>> {
    CANARIES.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Run one event through a parsed version in shadow, returning its emits (never
/// published — vjs::run only collects them).
/// What a shown run of `prog` gives for one event: its emits, or its error. A
/// run that made a call whose answer is not recorded names it as
/// `not_recorded`: what it gave says nothing of the flow's outputs.
fn shadow_emits(prog: &vjs::Program, ev: &Value, root: &Path, pkg: &str) -> Value {
    let mut engine = vjs::Engine::shadow(root.to_path_buf(), pkg.to_string());
    let mut out = match vjs::run(prog, ev, &mut engine) {
        Ok(ctx) => json!({
            "emits": ctx.emits.iter().map(|(s, p)| json!({"subject": s, "payload": p})).collect::<Vec<_>>(),
            "error": Value::Null,
        }),
        Err(e) => json!({ "emits": [], "error": e }),
    };
    if let Some(call) = engine.take_unrecorded() {
        out["not_recorded"] = Value::String(call);
    }
    out
}

/// Whether one event's two outputs differ. None when both runs reached the
/// same call, whose answer is not recorded (ADR-0032, point 4): what follows
/// depends on that answer, so the event is left out of the comparison and
/// counted apart, and two refusals do not read as "unchanged". A run that
/// calls with other arguments, calls another service, or fails before its
/// call does something else with the event: that is a change.
fn compared(before: &Value, after: &Value) -> Option<bool> {
    match (before.get("not_recorded"), after.get("not_recorded")) {
        (None, None) => Some(before != after),
        (Some(b), Some(a)) if b == a => None,
        _ => Some(true),
    }
}

/// Start (or replace) a canary for a flow: shadow-follow the live source and diff
/// the candidate against the current effective version.
fn canary_start(root: &Path, file: &str, candidate_src: &str) -> Result<Value, String> {
    let path = guard_path(root, file).ok_or("path not allowed")?;
    let baseline = fs::read_to_string(&path).map_err(|e| e.to_string())?;
    let unit = flow_proc_name(&path);
    let pkg = pkg_of_path(&path);
    let url = env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let js = bus::connect(&url).map_err(|e| format!("NATS unreachable: {e}"))?.jetstream();
    let store = versions::open_store(&js);
    let (current_src, _) = versions::resolve_source(store.as_ref(), &unit, &baseline, now_secs());
    let current = vjs::parse(&current_src)?;
    let candidate = vjs::parse(candidate_src)?;
    let reads = [vjs::event_reads(&current), vjs::event_reads(&candidate)].concat();
    let subject = current.source.clone().ok_or("flow has no bus source to shadow")?;
    let candidate_hash = versions::hash_content(candidate_src);
    // the effective version this canary diffs against — snapshotted so we can
    // detect a live promote / eviction under it.
    let current_hash = versions::hash_content(&current_src);

    let running = Arc::new(AtomicBool::new(true));
    let total = Arc::new(AtomicU64::new(0));
    let changed = Arc::new(AtomicU64::new(0));
    let not_recorded = Arc::new(AtomicU64::new(0));
    let ring = Arc::new(Mutex::new(VecDeque::new()));
    let stop_reason = Arc::new(Mutex::new(None::<String>));
    {
        let mut map = canaries().lock().unwrap();
        // Refuse a double start: a running canary vanishing without trace would
        // surprise the operator watching it. Stop it first.
        if let Some(prev) = map.get(&unit) {
            if prev.running.load(Ordering::SeqCst) {
                return Err("canary already running for this flow — stop it first".into());
            }
        }
        map.insert(
            unit.clone(),
            Canary {
                candidate_hash: candidate_hash.clone(),
                started_at: now_secs(),
                running: running.clone(),
                total: total.clone(),
                changed: changed.clone(),
                not_recorded: not_recorded.clone(),
                ring: ring.clone(),
                stop_reason: stop_reason.clone(),
            },
        );
    }
    let (root, pkg, path, unit_t, store_t) =
        (root.to_path_buf(), pkg, path.clone(), unit.clone(), store);
    thread::spawn(move || {
        let stream = env::var("VEJAS_STREAM").unwrap_or_else(|_| "VEJAS".into());
        // Only the live tail, and never acked: read-only.
        let cfg = bus::EphemeralConfig {
            filter: &subject,
            start_seq: None,
            inactive_threshold: Duration::from_secs(30),
        };
        let sub = match js.pull_ephemeral(&stream, &cfg) {
            Ok(s) => s,
            Err(_) => return,
        };
        while running.load(Ordering::SeqCst) && RUNNING.load(Ordering::SeqCst) {
            // Auto-stop if the live version changed under us (a promote or an
            // overlay eviction): the accumulated diff would otherwise silently
            // compare the candidate against a stale baseline.
            if let Ok(baseline) = fs::read_to_string(&path) {
                let (eff, _) =
                    versions::resolve_source(store_t.as_ref(), &unit_t, &baseline, now_secs());
                if versions::hash_content(&eff) != current_hash {
                    *stop_reason.lock().unwrap() =
                        Some("baseline changed (live version promoted or overlay evicted)".into());
                    running.store(false, Ordering::SeqCst);
                    break;
                }
            }
            let batch = connectors::fetch_round(&sub).unwrap_or_default();
            let scrub = record::shown();
            for m in &batch {
                let Ok(event) = serde_json::from_slice::<Value>(&m.data) else {
                    continue;
                };
                let (event, hidden) = shadow_input(&event, &scrub, &reads);
                let seq = m.delivery().map(|i| i.stream_seq).unwrap_or(0);
                let b = shadow_emits(&current, &event, &root, &pkg);
                let a = shadow_emits(&candidate, &event, &root, &pkg);
                let ch = compared(&b, &a);
                total.fetch_add(1, Ordering::SeqCst);
                match ch {
                    Some(true) => _ = changed.fetch_add(1, Ordering::SeqCst),
                    None => _ = not_recorded.fetch_add(1, Ordering::SeqCst),
                    Some(false) => {}
                }
                let mut r = ring.lock().unwrap();
                r.push_back(with_hidden(json!({"seq": seq, "changed": ch == Some(true), "before": b, "after": a}), hidden));
                while r.len() > 500 {
                    r.pop_front();
                }
            }
        }
    });
    Ok(json!({"ok": true, "flow": unit, "candidate_hash": candidate_hash}))
}

/// The accumulated diff of a running (or last) canary for a flow.
fn canary_read(root: &Path, file: &str) -> Result<Value, String> {
    let path = guard_path(root, file).ok_or("path not allowed")?;
    let unit = flow_proc_name(&path);
    let map = canaries().lock().unwrap();
    let Some(c) = map.get(&unit) else {
        return Ok(json!({"running": false, "flow": unit, "events": 0}));
    };
    let results: Vec<Value> = {
        let r = c.ring.lock().unwrap();
        r.iter().rev().take(100).cloned().collect()
    };
    Ok(json!({
        "running": c.running.load(Ordering::SeqCst),
        "flow": unit,
        "candidate_hash": c.candidate_hash,
        "started_at": c.started_at,
        "events": c.total.load(Ordering::SeqCst),
        "changed": c.changed.load(Ordering::SeqCst),
        "not_recorded": c.not_recorded.load(Ordering::SeqCst),
        "stop_reason": *c.stop_reason.lock().unwrap(),
        "results": results,
    }))
}

/// Stop a running canary (the shadow consumer thread exits; state is kept for a
/// final read until replaced).
fn canary_stop(root: &Path, file: &str) -> Result<Value, String> {
    let path = guard_path(root, file).ok_or("path not allowed")?;
    let unit = flow_proc_name(&path);
    let map = canaries().lock().unwrap();
    match map.get(&unit) {
        Some(c) => {
            c.running.store(false, Ordering::SeqCst);
            Ok(json!({"ok": true, "flow": unit, "stopped": true}))
        }
        None => Ok(json!({"ok": true, "flow": unit, "stopped": false})),
    }
}

/// Promote a literal change as a cluster-wide VERSION (ADR-0021) instead of a
/// local file write: compute the new source by applying set_literal to the
/// current EFFECTIVE source (overlay-or-baseline), then publish it to
/// VEJAS_VERSIONS forked from the git baseline hash. Every instance's version
/// watch then hot-reloads the flow. Also records the ADR-0018 audit trail.
fn promote_version(
    root: &Path,
    file: &str,
    name: &str,
    key: &str,
    value: &Value,
    actor: &str,
) -> Result<Value, String> {
    let path = guard_path(root, file).ok_or("path not allowed")?;
    let baseline = fs::read_to_string(&path).map_err(|e| e.to_string())?;
    let unit = flow_proc_name(&path);
    let url = env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let js = bus::connect(&url).map_err(|e| format!("NATS unreachable: {e}"))?.jetstream();
    let store = versions::open_store(&js).ok_or("version store unavailable")?;
    // current effective source: a prior overlay if valid, else the baseline
    let (current, _) = versions::resolve_source(Some(&store), &unit, &baseline, now_secs());
    let before = vjs::get_literal(&current, name, key).unwrap_or(Value::Null);
    let new_src = vjs::set_literal(&current, name, key, value)?;
    // overlays always fork from the GIT baseline, so a deploy that moves the
    // baseline evicts the whole overlay chain (resolve_source).
    let parent = versions::hash_content(&baseline);
    versions::put_version(&store, &unit, &new_src, &parent, actor, now_secs())?;
    record_promote(root, file, name, key, &before, value, actor);
    Ok(json!({"ok": true, "versioned": true, "flow": unit, "parent": parent}))
}

// ───────────────────────── proposal queue (ADR-0024) ─────────────────────────

/// The current effective-source hash of a flow file — the baseline a proposal is
/// pinned to (the overlay-or-baseline the evidence saw, ADR-0021/0024).
fn effective_source_hash(
    js: &bus::JetStream,
    root: &Path,
    file: &str,
) -> Result<String, String> {
    let path = guard_path(root, file).ok_or("path not allowed")?;
    let baseline = fs::read_to_string(&path).map_err(|e| e.to_string())?;
    let unit = flow_proc_name(&path);
    let store = versions::open_store(js);
    let (src, _) = versions::resolve_source(store.as_ref(), &unit, &baseline, now_secs());
    Ok(versions::hash_content(&src))
}

/// Emit a proposal transition on the bus so a notification flow can route it
/// (Slack/PagerDuty/email) — the platform dogfoods its own connectors (ADR-0024).
fn emit_proposal_event(js: &bus::JetStream, transition: &str, proposal: &Value) {
    let subj_root = env::var("VEJAS_SUBJECT_ROOT").unwrap_or_else(|_| "vx".into());
    let subject = format!("{subj_root}.proposals.events");
    let ev = json!({"transition": transition, "ts": now_secs(), "proposal": proposal});
    let _ = js.publish(&subject, serde_json::to_vec(&ev).unwrap_or_default());
}

/// The distinct approval credential (ADR-0024). A shared VEJAS_TOKEN is what the
/// agent already holds for /mcp, so approve/reject require their OWN token —
/// otherwise governance is decorative.
/// The actor to attribute a mutation to in the audit trail (ADR-0030): the
/// `X-Vejas-Actor` header when present (the enterprise auth proxy sets it to the
/// authenticated user, so the single core token does not collapse every action
/// into one identity for SIEM export and the RBAC audit), else the default. Useful
/// even open — a solo operator can name themselves.
fn header_actor(request: &tiny_http::Request, default: &str) -> String {
    request
        .headers()
        .iter()
        .find(|h| h.field.equiv("x-vejas-actor"))
        .map(|h| h.value.as_str().trim().to_string())
        .filter(|s| !s.is_empty() && s.len() <= 128)
        .unwrap_or_else(|| default.to_string())
}

/// Constant-time string comparison for credentials — no early return on the first
/// differing byte, so a token/approval-token check leaks no timing side-channel
/// (ADR-0029 R7). Length mismatch folds into the accumulator, not a short-circuit.
fn ct_eq(a: &str, b: &str) -> bool {
    ct_eq_bytes(a.as_bytes(), b.as_bytes())
}

/// The same on bytes: a webhook's signature against the tag it should be.
fn ct_eq_bytes(a: &[u8], b: &[u8]) -> bool {
    let mut diff = (a.len() ^ b.len()) as u64;
    let n = a.len().max(b.len());
    for i in 0..n {
        let (x, y) = (a.get(i).copied().unwrap_or(0), b.get(i).copied().unwrap_or(0));
        diff |= (x ^ y) as u64;
    }
    diff == 0
}

fn approval_token() -> Option<String> {
    env::var("VEJAS_APPROVAL_TOKEN").ok().filter(|t| !t.is_empty())
}

/// The optional write credential. When set (and non-empty), the mutating surface
/// requires `Authorization: Bearer <it>`; None = writes are open (dev default).
/// Where the panel, the API and `/mcp` listen unless `VEJAS_HTTP_ADDR` says otherwise.
/// Loopback, because this port writes flows and runs commands and has no token until
/// one is set. The image sets `0.0.0.0:8686`: inside a container, publishing the port
/// is the deliberate act.
const DEFAULT_HTTP_ADDR: &str = "127.0.0.1:8686";

/// Whether `addr` takes connections from anywhere but this machine. An address that
/// does not resolve answers yes: all this feeds is a warning.
fn beyond_loopback(addr: &str) -> bool {
    use std::net::ToSocketAddrs;
    match addr.to_socket_addrs() {
        Ok(mut resolved) => resolved.any(|s| !s.ip().is_loopback()),
        Err(_) => true,
    }
}

fn write_token() -> Option<String> {
    env::var("VEJAS_TOKEN").ok().filter(|t| !t.is_empty())
}

/// Does the request carry the human approval credential? Constant-time.
fn approval_authorized(request: &tiny_http::Request) -> bool {
    approval_token().is_some_and(|tok| {
        request
            .headers()
            .iter()
            .any(|h| h.field.equiv("x-approval-token") && ct_eq(h.value.as_str(), &tok))
    })
}

/// The doors that belong to the human, not to the agent: deciding a proposal,
/// and replaying or discarding dead letters. The panel holds the approval token
/// and never the agent's `VEJAS_TOKEN`, so on these routes a valid approval
/// token answers the write gate too. Otherwise a deployment with both tokens
/// set, the documented production posture, leaves the human unable to approve
/// from the panel.
fn is_human_route(path: &str) -> bool {
    matches!(path, "/dlq/replay" | "/dlq/purge")
        || (path.starts_with("/proposals/")
            && (path.ends_with("/approve") || path.ends_with("/reject")))
}

/// Does the request carry a valid `Authorization: Bearer <token>`? Constant-time.
fn bearer_authorized(request: &tiny_http::Request, token: &str) -> bool {
    request.headers().iter().any(|h| {
        h.field.equiv("authorization")
            && h.value
                .as_str()
                .strip_prefix("Bearer ")
                .map(|t| ct_eq(t, token))
                .unwrap_or(false)
    })
}

/// GET/HEAD are reads (left open by the write gate); every other verb is a
/// mutation. The one exception — a read-method /api flow that emits — is a bus
/// write and is gated in the /api branch, not here (Finding A: gate on "writes",
/// not on the POST verb; `api "DELETE /x"` and an emitting `api "GET /x"` mutate).
fn is_read_method(m: &tiny_http::Method) -> bool {
    matches!(m, tiny_http::Method::Get | tiny_http::Method::Head)
}

/// Submit a proposal (ADR-0024): an agent proposes a mutation, pinned to the
/// baseline its evidence saw. `payload` must carry `file`.
fn submit_proposal(
    root: &Path,
    kind: &str,
    payload: Value,
    author: &str,
    evidence: Value,
) -> Result<Value, String> {
    if kind != "set_literal" && kind != "version" {
        return Err("kind must be set_literal or version".into());
    }
    let file = payload["file"].as_str().ok_or("payload.file required")?.to_string();
    let url = env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let js = bus::connect(&url).map_err(|e| format!("NATS unreachable: {e}"))?.jetstream();
    let baseline = effective_source_hash(&js, root, &file)?;
    let store = proposals::open_store(&js).ok_or("proposal store unavailable")?;
    let id = proposals::new_id(now_secs());
    let rec = proposals::submit(&store, &id, kind, payload, author, &baseline, evidence, now_secs())?;
    emit_proposal_event(&js, "submitted", &rec);
    Ok(rec)
}

/// List proposals, auto-expiring pending ones whose baseline has moved (a deploy
/// or another promote) — loudly (status expired, audited, an event) so the queue
/// never shows an approvable change the evidence never saw (ADR-0024).
fn reconcile_and_list(root: &Path) -> Result<Vec<Value>, String> {
    let url = env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let js = bus::connect(&url).map_err(|e| format!("NATS unreachable: {e}"))?.jetstream();
    let store = proposals::open_store(&js).ok_or("proposal store unavailable")?;
    let mut out = Vec::new();
    for p in proposals::list(&store) {
        let mut p = p;
        if p["status"] == "pending" {
            if let Some(file) = p["payload"]["file"].as_str() {
                let current = effective_source_hash(&js, root, file).unwrap_or_default();
                if !current.is_empty() && current != p["baseline"].as_str().unwrap_or("") {
                    let id = p["id"].as_str().unwrap_or("").to_string();
                    if let Ok(updated) = proposals::set_status(&store, &id, "expired", None, now_secs()) {
                        let _ = connectors::to_audit(&js, "proposals", &json!({
                            "ts": now_secs(), "actor": "expiry", "kind": "proposal-expired",
                            "proposal": id, "baseline_was": p["baseline"], "baseline_now": current,
                        }));
                        emit_proposal_event(&js, "expired", &updated);
                        p = updated;
                    }
                }
            }
        }
        out.push(p);
    }
    Ok(out)
}

/// Execute an approved proposal through the EXISTING paths (a set_literal or a
/// whole-version promote), cluster-aware. The caller has already re-verified the
/// baseline (TOCTOU).
fn apply_approved(
    registry: &Registry,
    root: &Path,
    proposal: &Value,
    actor: &str,
) -> Result<Value, String> {
    let kind = proposal["kind"].as_str().unwrap_or("");
    let payload = &proposal["payload"];
    let file = payload["file"].as_str().ok_or("payload.file")?;
    match kind {
        "set_literal" => {
            let name = payload["name"].as_str().ok_or("payload.name")?;
            let key = payload["key"].as_str().unwrap_or("-");
            let value = &payload["value"];
            if cluster::clustered() {
                promote_version(root, file, name, key, value, actor)
            } else {
                let path = guard_path(root, file).ok_or("path not allowed")?;
                let src = fs::read_to_string(&path).map_err(|e| e.to_string())?;
                let before = vjs::get_literal(&src, name, key).unwrap_or(Value::Null);
                let new_src = vjs::set_literal(&src, name, key, value)?;
                fs::write(&path, new_src).map_err(|e| e.to_string())?;
                record_promote(root, file, name, key, &before, value, actor);
                let _ = reload(registry, root);
                Ok(json!({"ok": true, "file": file, "name": name, "key": key}))
            }
        }
        "version" => {
            let candidate = payload["candidate"].as_str().ok_or("payload.candidate")?;
            vjs::parse(candidate)?; // reject a malformed candidate before writing
            let path = guard_path(root, file).ok_or("path not allowed")?;
            let baseline = fs::read_to_string(&path).map_err(|e| e.to_string())?;
            let unit = flow_proc_name(&path);
            let url = env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
            let js = bus::connect(&url)?.jetstream();
            if cluster::clustered() {
                let store = versions::open_store(&js).ok_or("version store unavailable")?;
                let parent = versions::hash_content(&baseline);
                versions::put_version(&store, &unit, candidate, &parent, actor, now_secs())?;
                Ok(json!({"ok": true, "versioned": true, "flow": unit}))
            } else {
                fs::write(&path, candidate).map_err(|e| e.to_string())?;
                let _ = reload(registry, root);
                Ok(json!({"ok": true, "file": file, "written": true}))
            }
        }
        other => Err(format!("unknown proposal kind {other:?}")),
    }
}

/// Approve or reject a proposal (ADR-0024). Approve re-verifies the baseline at
/// execution (TOCTOU) — a race with a deploy/promote resolves to `expired`, never
/// to landing a change the evidence never saw — then runs it through the existing
/// paths, audits with the proposal id, and records the transition. `decided_by`
/// is the approval channel.
fn decide_proposal(
    registry: &Registry,
    root: &Path,
    id: &str,
    approve: bool,
    decided_by: &str,
) -> Result<Value, String> {
    let url = env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let js = bus::connect(&url).map_err(|e| format!("NATS unreachable: {e}"))?.jetstream();
    let store = proposals::open_store(&js).ok_or("proposal store unavailable")?;
    let proposal = proposals::get(&store, id).ok_or("proposal not found")?;
    if proposal["status"] != "pending" {
        return Err(format!("proposal already {}", proposal["status"]));
    }
    if !approve {
        let updated = proposals::set_status(&store, id, "rejected", Some(decided_by), now_secs())?;
        let _ = connectors::to_audit(&js, "proposals", &json!({
            "ts": now_secs(), "actor": decided_by, "kind": "proposal-rejected", "proposal": id,
        }));
        emit_proposal_event(&js, "rejected", &updated);
        return Ok(updated);
    }
    // TOCTOU: the baseline must not have moved since the evidence was gathered.
    let file = proposal["payload"]["file"].as_str().ok_or("payload.file")?;
    let current = effective_source_hash(&js, root, file)?;
    if current != proposal["baseline"].as_str().unwrap_or("") {
        let updated = proposals::set_status(&store, id, "expired", None, now_secs())?;
        emit_proposal_event(&js, "expired", &updated);
        return Err("baseline changed since the proposal was made — expired, re-propose".into());
    }
    let result = apply_approved(registry, root, &proposal, decided_by)?;
    let _ = connectors::to_audit(&js, "proposals", &json!({
        "ts": now_secs(), "actor": decided_by, "kind": "proposal-approved",
        "proposal": id, "applied": result,
    }));
    let updated = proposals::set_status(&store, id, "approved", Some(decided_by), now_secs())?;
    emit_proposal_event(&js, "approved", &updated);
    Ok(json!({"ok": true, "proposal": updated, "applied": result}))
}

const DLQ_HUMAN_ACT: &str = "approval required: replaying or discarding dead letters is a human \
    action in governed mode: the panel, or X-Approval-Token (ADR-0024)";

/// Replaying or discarding dead letters in governed mode. No proposal carries
/// them, and an agent holds `VEJAS_TOKEN`, so they sit on the human credential:
/// a replay re-runs side effects, and a purge destroys what the agent would be
/// judged on. A dead letter is also untrusted content the agent reads, so these
/// are the last things it should be able to reach.
fn dlq_agent_gate() -> Result<(), String> {
    if require_approval() {
        Err(DLQ_HUMAN_ACT.into())
    } else {
        Ok(())
    }
}

/// The same rule on the HTTP door, where the human can present the credential.
fn dlq_http_gate(request: &tiny_http::Request) -> Result<(), (u16, String)> {
    if !require_approval() || approval_authorized(request) {
        return Ok(());
    }
    if request.headers().iter().any(|h| h.field.equiv("x-approval-token")) {
        return Err((401, "unauthorized: X-Approval-Token required".into()));
    }
    Err((409, DLQ_HUMAN_ACT.into()))
}

/// Governed-change mode (ADR-0024): when `VEJAS_REQUIRE_APPROVAL=1`, a direct
/// mutation is refused — the caller submits a proposal a human approves instead.
/// Gates EVERY mutating path (MCP tools AND raw HTTP), not just the MCP door: an
/// agent with HTTP access would otherwise walk through the other one.
fn require_approval() -> bool {
    env::var("VEJAS_REQUIRE_APPROVAL")
        .map(|v| v == "1" || v == "true")
        .unwrap_or(false)
}
fn approval_gate() -> Result<(), String> {
    if require_approval() {
        Err("approval required: submit a proposal (vejas_propose, or the panel) — \
             a human approves it, see ADR-0024"
            .into())
    } else {
        Ok(())
    }
}

/// In cluster mode (`VEJAS_CLUSTER=1`), refuse any mutation of this instance's
/// LOCAL files. A promote that lands on one instance while the others keep the old
/// rule is the worst failure mode for the business-surface thesis (ADR-0020) — the
/// expert believes they fixed the meaning while half the traffic disagrees. In a
/// cluster, changes flow through GitOps or a proposal. `/reload`, DLQ replay, and
/// Vault-backed secret writes stay allowed (they are per-instance-safe or shared).
fn cluster_write_guard() -> Result<(), String> {
    if cluster::clustered() {
        Err("clustered: this instance will not mutate its local files — \
             promote through git (GitOps), see ADR-0020"
            .into())
    } else {
        Ok(())
    }
}

/// Append a promote to the audit trail (ADR-0018). Best-effort for LIVE promotes:
/// a live promote that cannot reach VEJAS_AUDIT is logged but still applied — git
/// remains the durable trail for repo-deployed changes, and blocking a correction
/// because the audit stream is unreachable would be the wrong trade. Records
/// file, name, key, before → after, actor, ts.
fn record_promote(
    root: &Path,
    file: &str,
    name: &str,
    key: &str,
    before: &Value,
    after: &Value,
    actor: &str,
) {
    let Some(path) = guard_path(root, file) else { return };
    let unit = flow_proc_name(&path);
    let url = env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let Ok(bus) = bus::connect(&url) else {
        eprintln!(
            "[vejas] audit: NATS unreachable — live promote of {name} not recorded \
             (git remains the trail for committed changes)"
        );
        return;
    };
    let js = bus.jetstream();
    let record = json!({
        "ts": now_secs(), "actor": actor, "unit": unit,
        "file": file, "name": name, "key": key,
        "before": before, "after": after,
    });
    if let Err(e) = connectors::to_audit(&js, &unit, &record) {
        eprintln!("[vejas] audit: promote of {name} not recorded: {e}");
    }
}

/// Roll a literal back to the value it held before its most recent promote
/// (ADR-0018). Rollback is itself a promote — to the recorded previous value —
/// so it is forward-only (never rewrites the trail), reuses the same write +
/// reload + audit path, and can be previewed with the same shadow-replay rail.
/// The caller reloads after this returns. Writes the file; returns what it did.
fn rollback_literal(
    root: &Path,
    file: &str,
    name: &str,
    key: &str,
    actor: &str,
) -> Result<Value, String> {
    let path = guard_path(root, file).ok_or("path not allowed")?;
    let unit = flow_proc_name(&path);
    let url = env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let js = bus::connect(&url).map_err(|e| format!("NATS unreachable: {e}"))?.jetstream();
    let records = connectors::audit_recent(&js, &unit, 500)?;
    // newest first: the most recent promote of exactly this literal
    let target = records
        .iter()
        .rev()
        .find(|r| r["name"] == name && r["key"] == key)
        .ok_or("no audit record to roll back for this literal")?;
    let restore = target["before"].clone();
    let src = fs::read_to_string(&path).map_err(|e| e.to_string())?;
    let current = vjs::get_literal(&src, name, key).unwrap_or(Value::Null);
    let new_src = vjs::set_literal(&src, name, key, &restore)?;
    fs::write(&path, new_src).map_err(|e| e.to_string())?;
    record_promote(root, file, name, key, &current, &restore, actor);
    Ok(json!({
        "ok": true, "file": file, "name": name, "key": key,
        "restored": restore, "was": current,
        "rolled_back_promote_ts": target["ts"], "actor": actor,
    }))
}

/// The panel HTML with the credential-mask pattern injected from its single
/// definition (secrets::SECRET_KEY_PATTERN) — so the mask and the admission lint
/// (ADR-0017) share one source and cannot drift.
fn panel_html() -> String {
    include_str!("panel.html").replace("{{SECRET_PATTERN}}", secrets::SECRET_KEY_PATTERN)
}

/// A trace entry's preview: the stored one, or derived from the stored event
/// when the hot loop skipped serialising it.
fn trace_preview(entry: &Value) -> String {
    match entry.get("preview").and_then(Value::as_str) {
        Some(p) if !p.is_empty() => p.to_string(),
        _ => entry.get("event").map(preview_of).unwrap_or_default(),
    }
}

/// The beginning of an event as the ring shows it: 160 characters at most,
/// written only as far as that takes, however large the event (`/events`
/// derives it under the ring's lock).
fn preview_of(v: &Value) -> String {
    const CHARS: usize = 160;
    // four bytes at most a character: past this, more than CHARS are written
    const ENOUGH: usize = 4 * (CHARS + 1);
    struct Upto(Vec<u8>);
    impl std::io::Write for Upto {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            // a long string comes in one write: take only what is missing
            let room = ENOUGH.saturating_sub(self.0.len());
            if room == 0 {
                return Err(std::io::Error::other("enough for a preview"));
            }
            let n = buf.len().min(room);
            self.0.extend_from_slice(&buf[..n]);
            Ok(n)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut out = Upto(Vec::new());
    // stopped on purpose past ENOUGH: what was written is all the preview needs
    let _ = serde_json::to_writer(&mut out, v);
    let s = match std::str::from_utf8(&out.0) {
        Ok(s) => s,
        Err(e) => std::str::from_utf8(&out.0[..e.valid_up_to()]).unwrap_or_default(),
    };
    if s.chars().count() > CHARS {
        let cut: String = s.chars().take(CHARS - 1).collect();
        format!("{cut}…")
    } else {
        s.to_string()
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
    let (prefix, ext) = match kind {
        Kind::Flow => ("flow", ".vjs"),
        Kind::Connector => ("connector", ".vjs"),
        Kind::Detect => ("detect", ".vpl"),
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(fname) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if fname.starts_with('_') || !fname.ends_with(ext) {
            continue;
        }
        let stem = fname.trim_end_matches(ext).to_string();
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
    scan_units(&root.join("detects"), "default", Kind::Detect, &mut out);
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
            scan_units(&dir.join("detects"), &pkg, Kind::Detect, &mut out);
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

/// How soon a flow picks up an edit of a service it invokes, or of a secret a
/// declaration reads: its engine loads them again at most this often, and a
/// declaration's secrets are resolved at most once in that time.
const SERVICES_FRESH: Duration = Duration::from_secs(1);

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
        let attempt = contained(|| -> Result<(), String> {
            let baseline = fs::read_to_string(&handle.spec.path).map_err(|e| e.to_string())?;
            // Cluster-wide overlay (ADR-0021): a promoted version overrides the
            // file iff it forked from exactly this baseline; a stale overlay
            // (baseline advanced / flow git-deleted) is evicted here — git wins.
            // The git baseline's hash when a promoted version overrides it: the
            // spans carry both (ADR-0034), the version being the provenance key.
            let mut overlay_of: Option<Arc<str>> = None;
            let src = {
                let vconn = bus::connect(&url).ok().map(|bus| bus.jetstream());
                let vstore = vconn.as_ref().and_then(versions::open_store);
                let (s, outcome) = versions::resolve_source(
                    vstore.as_ref(),
                    &handle.spec.name,
                    &baseline,
                    now_secs(),
                );
                match outcome {
                    versions::Outcome::Overlay => {
                        overlay_of = Some(versions::hash_content(&baseline).into());
                        eprintln!("[vejas] {} on a promoted version (overlay)", handle.spec.name)
                    }
                    versions::Outcome::Evicted { parent, baseline: base_hash } => {
                        // audit the eviction (ADR-0018) so it is in the trail, not
                        // only the log — best-effort.
                        if let Some(js) = &vconn {
                            let record = json!({
                                "ts": now_secs(), "actor": "eviction",
                                "unit": handle.spec.name, "kind": "overlay-evicted",
                                "parent": parent, "baseline": base_hash,
                            });
                            let _ = connectors::to_audit(js, &handle.spec.name, &record);
                        }
                    }
                    versions::Outcome::Baseline => {}
                }
                s
            };
            let prog = vjs::parse(&src)?;
            // the version (content hash of the effective source) this flow runs —
            // tags any message it dead-letters (ADR-0021).
            let flow_version: Arc<str> = versions::hash_content(&src).into();
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
            let nc = bus::connect(&url)?;
            let js = nc.jetstream();
            let _ = js.add_stream(&bus::StreamConfig {
                name: stream.clone(),
                subjects: vec![format!("{subj_root}.>")],
                ..Default::default()
            });
            let durable = handle.spec.name.replace([':', '.'], "_");
            // create the durable pull consumer (idempotent-ish), then bind
            let _ = js.create_pull_consumer(
                &stream,
                &durable,
                std::slice::from_ref(&source),
                connectors::ack_wait(),
                None,
            );
            let sub = js.pull_durable(&stream, &durable, Some(&source))?;
            set_state(&handle, |st| {
                st.status = "running".into();
                st.started_at = Some(now_secs());
                st.last_error = None;
            });
            eprintln!(
                "[vejas] {} (vjs, native) consuming {source}",
                handle.spec.name
            );
            let mut engine = vjs::Engine::live(root.clone(), handle.spec.pkg.clone());
            // The flow's file as the record names it: a trace joins each
            // statement it ran with this file's code (ADR-0034, point 8).
            let code_file: Arc<str> = relative_to(&root, &handle.spec.path).into();
            // A flow that only computes keeps one pull request in flight ahead
            // of the round being processed. Without it every round began with ~2 ms of waiting for
            // its first message — request, round-trip, server delivery — while
            // this thread sat idle at ~50 % CPU; a Go client on the same
            // server pulls 72 k msgs/s with the same batch precisely because it
            // keeps the next request outstanding. The request for round N+1
            // leaves before round N is processed; its messages queue in the
            // subscription and are there when N is done. If the stream idles,
            // the outstanding request expires server-side like any other
            // (`PULL_EXPIRES_MS`) and its round comes back empty. A stop
            // leaves at most one request outstanding, whose messages redeliver
            // un-acked after `ack_wait` — the same as stopping mid-batch.
            //
            // A flow that calls a system takes its messages one by one, and asks
            // for the next one once it is done with this one (ADR-0032). Rounds
            // of 64, and the next asked for ahead, would hold up to 128 messages
            // in the client while each one's calls run, and ack a round only at
            // its end: past `ack_wait`, the server delivers them again, here or
            // to another instance. A service edited live to call a system turns
            // this on from the round after its first call.
            //
            // A message whose run outlived `ack_wait` can still come back here,
            // when the server takes the next pull before the late ack: it takes
            // acks and pull requests on queues of their own. The loop knows the
            // messages it settled, and acks such a copy without running it.
            let mut one_by_one = engine.reaches_system(&prog);
            let mut settled = Settled::default();
            let mut round = sub.round_of(if one_by_one { 1 } else { bus::PULL_BATCH })?;
            let mut batch = nc.batch_stored(&subj_root);
            loop {
                if !RUNNING.load(Ordering::SeqCst) || handle.stop.load(Ordering::SeqCst) {
                    return Ok(());
                }
                engine.refresh(SERVICES_FRESH); // pick up live service edits
                let round_t0 = Instant::now();
                let next = if one_by_one { None } else { Some(sub.round()?) };
                // Messages are processed as they arrive, not once the whole batch
                // has landed, so reading and working overlap instead of taking
                // turns.
                let fetch = &mut round;
                let mut fetched = 0usize;
                let mut first_at: Option<Instant> = None;
                // Emits are held and confirmed once a batch, before their source
                // messages are acked: the stream's answer to each one it stores,
                // all awaited together (ADR-0002). A run that succeeded is settled
                // then: acked and recorded, or failed if the stream refused one
                // of its emits.
                let mut ran: Vec<Ran> = Vec::new();
                for msg in fetch {
                    fetched += 1;
                    first_at.get_or_insert_with(Instant::now);
                    if !RUNNING.load(Ordering::SeqCst) || handle.stop.load(Ordering::SeqCst) {
                        // stopped mid-batch: leave the message un-acked, it
                        // redelivers to whoever holds the durable next
                        return Ok(());
                    }
                    // the replay of another unit's dead letter (ADR-0015 Amendment C)
                    if msg.replay_elsewhere(&handle.spec.name) {
                        let _ = msg.ack();
                        continue;
                    }
                    // a message this loop settled, delivered again (`Settled`)
                    if msg.delivery().is_some_and(|d| settled.holds(d.stream_seq)) {
                        let _ = msg.ack();
                        continue;
                    }
                    // The hop continues the trace the message carries, or starts
                    // one; its emits and its dead letter carry it on (ADR-0034).
                    let hop = trace::Hop::begin(trace::of(&msg));
                    let carried = hop.context();
                    // The hop's span goes to the exporter and the record; its
                    // invokes' spans to the exporter alone. A sampled-out hop
                    // builds no span, unless it fails.
                    let observing = metrics::observing();
                    let traced = metrics::tracing() && hop.sampled;
                    let start_nanos = metrics::now_nanos();
                    let event: Value = match serde_json::from_slice(&msg.data) {
                        Ok(v) => v,
                        Err(e) => {
                            // Not JSON: permanent poison. Dead-letter it (ADR-0015)
                            // — publish before ack, nak if the DLQ write fails.
                            let err = format!("bad json: {e}");
                            eprintln!("[vejas] {}: {err}", handle.spec.name);
                            let delivered =
                                msg.delivery().map(|i| i.delivered).unwrap_or(1);
                            let prev: String =
                                String::from_utf8_lossy(&msg.data).chars().take(160).collect();
                            metrics::observe(&handle.spec.name, false, 0, 0.0);
                            let dead = connectors::to_dlq(&js, &handle.spec.name, &msg.subject, delivered, &err, &msg.data, &flow_version, Some(&carried), msg.delivery().map(|d| d.message()), &durable);
                            match &dead {
                                Ok(()) => {
                                    metrics::inc_dead_letter(&handle.spec.name);
                                    record_trace(&handle.spec.name, &msg.subject, false,
                                        Some(format!("{err} — dead-lettered")), vec![], prev, None, Some(&hop), msg.delivery());
                                    let _ = msg.ack();
                                    settled.add(&msg);
                                }
                                Err(de) => {
                                    record_trace(&handle.spec.name, &msg.subject, false,
                                        Some(format!("{err} — DLQ publish failed: {de}")), vec![], prev, None, Some(&hop), msg.delivery());
                                    let _ = msg.nak();
                                }
                            }
                            if observing {
                                let span = hop_span(&hop, &handle.spec.name, &code_file, &msg, &flow_version, overlay_of.as_ref(), start_nanos);
                                record::end(
                                    if dead.is_ok() { span.attr("vejas.dlq", true) } else { span }.failed(err),
                                    record::Extras::new().input(&msg.data),
                                );
                            }
                            continue;
                        }
                    };
                    let t0 = Instant::now();
                    // Each `invoke` leaves a span under the hop, while spans are exported.
                    engine.trace_under(traced.then_some(&hop));
                    // A system the run calls continues the message's trace (ADR-0032).
                    engine.carry(Some(carried.clone()));
                    // The record says which statements the event went through.
                    engine.record_steps(record::on());
                    // Holding an emit is local, so a refusal is final for this message:
                    // it can never leave, however often it redelivers. That makes it
                    // a failed run, capped by MAX_DELIVERIES and dead-lettered, and
                    // the emits it held before the refusal go back out of the batch:
                    // a run's emits leave all together or not at all (ADR-0032).
                    let held = batch.held();
                    let outcome = vjs::run(&prog, &event, &mut engine).and_then(|ctx| {
                        for (subj, payload) in &ctx.emits {
                            let bytes = serde_json::to_vec(payload).unwrap_or_default();
                            // held; sent and confirmed once per batch below
                            batch
                                .publish_traced(subj, bytes, &carried)
                                .map_err(|e| format!("publish {subj}: {e}"))?;
                        }
                        Ok(ctx)
                    });
                    if outcome.is_err() {
                        batch.rollback(held);
                    }
                    if traced {
                        for invoked in engine.take_spans() {
                            metrics::span(invoke_span(&handle.spec.name, invoked));
                        }
                    }
                    let steps = engine.take_steps();
                    match outcome {
                        Ok(ctx) => ran.push(Ran {
                            emits: held..batch.held(),
                            subjects: ctx.emits.iter().map(|(s, _)| s.clone()).collect(),
                            event,
                            hop,
                            steps,
                            start_nanos,
                            secs: t0.elapsed().as_secs_f64(),
                            observing,
                            msg,
                        }),
                        Err(e) => {
                            eprintln!("[vejas] {}: {e}", handle.spec.name);
                            metrics::observe(&handle.spec.name, false, 0, t0.elapsed().as_secs_f64());
                            // poison guard: past MAX_DELIVERIES the message is
                            // dead-lettered (ADR-0015) rather than dropped; publish
                            // before ack, nak if the DLQ write fails.
                            let delivered =
                                msg.delivery().map(|i| i.delivered).unwrap_or(1);
                            // A call that got no answer (ADR-0032): the message tries
                            // again at its next delivery, after the service's delay,
                            // while the service allows it; a call no later try can
                            // change, or whose attempts are spent, is a dead letter now.
                            let failure = engine.take_failure();
                            let spent = failure.as_ref().is_some_and(|f| !f.retry || delivered >= f.attempts);
                            let mut dead_lettered = false;
                            if delivered >= MAX_DELIVERIES || spent {
                                match connectors::to_dlq(&js, &handle.spec.name, &msg.subject, delivered, &e, &msg.data, &flow_version, Some(&carried), msg.delivery().map(|d| d.message()), &durable) {
                                    Ok(()) => {
                                        dead_lettered = true;
                                        metrics::inc_dead_letter(&handle.spec.name);
                                        record_trace(&handle.spec.name, &msg.subject, false,
                                            Some(format!("{e} — dead-lettered after {delivered} deliveries")),
                                            vec![], String::new(), Some(event.clone()), Some(&hop), msg.delivery());
                                        let _ = msg.ack();
                                        settled.add(&msg);
                                    }
                                    Err(de) => {
                                        record_trace(&handle.spec.name, &msg.subject, false,
                                            Some(format!("{e} — DLQ publish failed: {de}")),
                                            vec![], String::new(), Some(event.clone()), Some(&hop), msg.delivery());
                                        let _ = msg.nak();
                                    }
                                }
                            } else {
                                record_trace(&handle.spec.name, &msg.subject, false, Some(e.clone()),
                                    vec![], String::new(), Some(event.clone()), Some(&hop), msg.delivery());
                                // no ack -> redelivery after ack_wait; after a call,
                                // after the service's delay
                                if let Some(failure) = &failure {
                                    let _ = msg.nak_after(failure.delay);
                                }
                            }
                            if observing {
                                let span = hop_span(&hop, &handle.spec.name, &code_file, &msg, &flow_version, overlay_of.as_ref(), start_nanos);
                                let span = if dead_lettered { span.attr("vejas.dlq", true) } else { span };
                                record::end(span.failed(e.clone()), record::Extras::new().input(&msg.data).steps(steps));
                            }
                            set_state(&handle, |st| st.last_error = Some(e));
                        }
                    }
                }
                // Publish-before-ack for the batch: the stream's answers confirm
                // every emit it stores (a round trip, any other), THEN the runs
                // are settled and their messages acked. A batch the bus does not
                // confirm acks none and redelivers; an emit the stream refused
                // fails its own run (nothing is lost; at-least-once, ADR-0002).
                metrics::observe_fetch(&handle.spec.name, fetched);
                let processed_at = Instant::now();
                let confirmed = batch.confirm_each();
                if let Err(e) = &confirmed {
                    eprintln!("[vejas] {}: flush before ack failed: {e} (batch redelivers)", handle.spec.name);
                }
                let unit = Unit {
                    handle: &handle,
                    file: &code_file,
                    version: &flow_version,
                    overlay_of: overlay_of.as_ref(),
                    js: &js,
                    durable: &durable,
                };
                let acked = settle(ran, &confirmed, &unit);
                // done, whether or not the acks get through
                for msg in &acked {
                    settled.add(msg);
                }
                if let Err(e) = nc.ack_all(&acked) {
                    eprintln!("[vejas] {}: ack: {e} (redelivers)", handle.spec.name);
                }
                if !one_by_one && engine.made_a_call() {
                    one_by_one = true;
                }
                round = match next {
                    Some(next) => next,
                    None => sub.round_of(1)?,
                };
                let first = first_at.unwrap_or(processed_at);
                metrics::observe_round(
                    &handle.spec.name,
                    (first - round_t0).as_secs_f64(),
                    (processed_at - first).as_secs_f64(),
                    processed_at.elapsed().as_secs_f64(),
                );
            }
        });
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

/// The messages a flow loop settled last, acked after a run or a dead letter,
/// by stream sequence. A message whose `ack_wait` ran out before its ack was
/// taken is delivered again although its run is done; the loop acks that copy
/// instead of running it a second time. The copy comes in the next round or
/// the one after: the last [`Settled::KEPT`] are remembered.
#[derive(Default)]
struct Settled {
    seqs: HashSet<u64>,
    order: VecDeque<u64>,
}

impl Settled {
    const KEPT: usize = 4 * bus::PULL_BATCH;

    fn add(&mut self, msg: &bus::Message) {
        let Some(seq) = msg.delivery().map(|d| d.stream_seq) else {
            return;
        };
        if self.seqs.insert(seq) {
            self.order.push_back(seq);
        }
        if self.order.len() > Self::KEPT {
            if let Some(oldest) = self.order.pop_front() {
                self.seqs.remove(&oldest);
            }
        }
    }

    fn holds(&self, seq: u64) -> bool {
        self.seqs.contains(&seq)
    }
}

/// A run of a flow that succeeded, held until its batch is confirmed.
struct Ran {
    msg: bus::Message,
    /// Where its emits are in the batch.
    emits: std::ops::Range<usize>,
    subjects: Vec<String>,
    event: Value,
    hop: trace::Hop,
    steps: Option<vjs::Steps>,
    start_nanos: u128,
    secs: f64,
    observing: bool,
}

/// What settling a run needs of the flow it ran in.
struct Unit<'a> {
    handle: &'a Handle,
    file: &'a Arc<str>,
    version: &'a Arc<str>,
    overlay_of: Option<&'a Arc<str>>,
    js: &'a bus::JetStream,
    durable: &'a str,
}

/// Settle the runs of a batch once it has been confirmed, and give back the
/// messages to ack: the runs that succeeded and the dead letters. A run whose
/// emits the stream stored succeeded, and is recorded so. A run with an emit the stream refused failed as a run that
/// errs does: its message comes back after ack_wait, and is a dead letter
/// once its deliveries are spent (ADR-0015). Its other emits may be stored
/// already, and are sent again with it (at least once). A batch that is not
/// confirmed acks none of its runs, and each redelivers.
fn settle(ran: Vec<Ran>, confirmed: &Result<Vec<bus::Refused>, String>, unit: &Unit) -> Vec<bus::Message> {
    let name = &unit.handle.spec.name;
    let mut acked = Vec::with_capacity(ran.len());
    for run in ran {
        let failed = match confirmed {
            Err(e) => Some(format!("flush before ack failed: {e} (redelivers)")),
            Ok(refused) => refused
                .iter()
                .find(|r| run.emits.contains(&r.index))
                .map(|r| format!("publish {}: {}", r.subject, r.error)),
        };
        let msg = &run.msg;
        let Some(error) = failed else {
            let emits = run.subjects.len() as u64;
            metrics::observe(name, true, emits, run.secs);
            if run.observing && run.hop.sampled {
                let span = hop_span(&run.hop, name, unit.file, msg, unit.version, unit.overlay_of, run.start_nanos)
                    .attr("vejas.emits", emits);
                let extras = record::Extras::new().emits(run.subjects.clone()).input(&msg.data).steps(run.steps);
                record::end(span, extras);
            }
            // The preview is derived from the stored event when /events is
            // read; serialising it here cost every message a second walk of
            // its JSON.
            record_trace(name, &msg.subject, true, None, run.subjects, String::new(), Some(run.event), Some(&run.hop), msg.delivery());
            acked.push(run.msg);
            continue;
        };
        metrics::observe(name, false, 0, run.secs);
        let mut said = error.clone();
        let mut dead_lettered = false;
        // An emit the stream refused fails the run, and its deliveries count;
        // a batch the bus did not confirm only redelivers.
        let delivered = msg.delivery().map(|i| i.delivered).unwrap_or(1);
        if confirmed.is_ok() {
            eprintln!("[vejas] {name}: {error}");
            set_state(unit.handle, |st| st.last_error = Some(error.clone()));
            if delivered >= MAX_DELIVERIES {
                let carried = run.hop.context();
                let message = msg.delivery().map(|d| d.message());
                match connectors::to_dlq(unit.js, name, &msg.subject, delivered, &error, &msg.data, unit.version, Some(&carried), message, unit.durable) {
                    Ok(()) => {
                        dead_lettered = true;
                        metrics::inc_dead_letter(name);
                        said = format!("{error} — dead-lettered after {delivered} deliveries");
                    }
                    Err(de) => {
                        said = format!("{error} — DLQ publish failed: {de}");
                        let _ = msg.nak();
                    }
                }
            }
        }
        if run.observing {
            let span = hop_span(&run.hop, name, unit.file, msg, unit.version, unit.overlay_of, run.start_nanos);
            let span = if dead_lettered { span.attr("vejas.dlq", true) } else { span };
            record::end(span.failed(error), record::Extras::new().input(&msg.data).steps(run.steps));
        }
        record_trace(name, &msg.subject, false, Some(said), vec![], String::new(), Some(run.event), Some(&run.hop), msg.delivery());
        if dead_lettered {
            acked.push(run.msg);
        }
    }
    acked
}

/// End the successful hops of a batch once its barrier has answered: a hop
/// whose emits did not leave is recorded as failed, and its message redelivers.
fn end_confirmed(done: Vec<(metrics::Span, record::Extras)>, confirmed: &Result<(), String>) {
    for (span, extras) in done {
        match confirmed {
            Ok(()) => record::end(span, extras),
            Err(e) => record::end(span.failed(format!("flush before ack failed: {e} (redelivers)")), extras),
        }
    }
}

/// The span of one hop of a flow or detect unit on the bus (ADR-0034): kind
/// CONSUMER, its parent the span that published the message.
fn hop_span(
    hop: &trace::Hop,
    unit: &str,
    file: &Arc<str>,
    msg: &bus::Message,
    version: &Arc<str>,
    baseline: Option<&Arc<str>>,
    start_nanos: u128,
) -> metrics::Span {
    let mut span = metrics::Span::hop(hop, metrics::Kind::Consumer, unit, start_nanos)
        .messaging("process", &msg.subject)
        .attr("code.file.path", metrics::Attr::Computed(file.clone()));
    if !version.is_empty() {
        span = span.attr("vejas.flow.version", metrics::Attr::Computed(version.clone()));
    }
    if let Some(baseline) = baseline {
        span = span.attr("vejas.flow.baseline", metrics::Attr::Computed(baseline.clone()));
    }
    if let Some(d) = msg.delivery() {
        span = span
            .attr("messaging.message.id", d.stream_seq.to_string())
            .attr("vejas.delivery.attempt", d.delivered);
        if let Some(died) = d.replay_of {
            span = span.attr("vejas.replay_of", died.to_string());
        }
    }
    span
}

/// An `invoke` a hop made, as a span under the hop (or under the invoke that
/// made it).
fn invoke_span(unit: &str, invoked: vjs::InvokeSpan) -> metrics::Span {
    let mut span = metrics::Span::hop(&invoked.hop, metrics::Kind::Internal, unit, invoked.start_nanos)
        .attr("code.function.name", invoked.service.clone())
        .attr("code.file.path", invoked.file);
    span.name = format!("invoke {}", invoked.service);
    span.end_nanos = invoked.end_nanos;
    match invoked.error {
        Some(e) => span.failed(e),
        None => span,
    }
}

/// Where a detect unit publishes an emit: the subject its `.to()` names, else
/// `<root>.detect.<unit>.<stream>`.
fn emit_subject(emit: &varpulis_engine::Emit, subj_root: &str, stem: &str) -> String {
    emit.sink
        .as_ref()
        .and_then(|s| s.topic.clone())
        .unwrap_or_else(|| format!("{subj_root}.detect.{stem}.{}", emit.stream))
}

/// A detect unit (ADR-0031): a VPL program run by the embedded Varpulis engine.
///
/// One durable pull consumer per `.from()` subject, each on a thread of its
/// own that feeds one channel; the engine runs on this thread, in event time,
/// over the merged arrival order. Emits are published before the messages
/// that caused them are acked (ADR-0002), exactly as a flow's are. An emit
/// whose stream has a `.to(<connector>, topic: "...")` goes to that subject;
/// one without goes to `<subject root>.detect.<unit>.<stream>`.
///
/// Payloads follow the Varpulis connectors' rules: a string `event_type`
/// names the event type, else the `.from()` binding's; `@timestamp` (RFC
/// 3339), else `ts` or `timestamp` (epoch milliseconds), is the event's time
/// — and every `.within()` and window is judged against it, not the clock.
///
/// State (windows, open sequences) lives in the engine and is lost on a
/// crash until the snapshot-and-resume of ADR-0031 points 4–5 lands: after
/// a `kill -9`, redelivered messages replay into an empty engine. A
/// stateless detection (a threshold, a filter) loses nothing today; a
/// sequence can miss the matches that straddle the crash.
fn supervise_detect(handle: Arc<Handle>, root: PathBuf) {
    let url = env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let stream = env::var("VEJAS_STREAM").unwrap_or_else(|_| "VEJAS".into());
    let subj_root = env::var("VEJAS_SUBJECT_ROOT").unwrap_or_else(|_| "vx".into());
    // Snapshot cadence (ADR-0031 point 4): whichever comes first, at a batch
    // boundary with nothing left for redelivery.
    let snapshot_every = Duration::from_secs(
        env::var("VEJAS_SNAPSHOT_SECS").ok().and_then(|v| v.parse::<u64>().ok()).unwrap_or(5).max(1),
    );
    let snapshot_every_acks: u64 =
        env::var("VEJAS_SNAPSHOT_ACKS").ok().and_then(|v| v.parse().ok()).unwrap_or(10_000).max(1);
    // A window whose sources go quiet closes about this long after its end:
    // once a source has sent nothing for the grace, its event time moves on
    // with the wall clock (varpulis `Program::set_idle_grace`), at every
    // batch and on the ticks of an idle unit. 0 turns it off: the unit is
    // then judged in event time alone, like a replay.
    let idle_grace = match env::var("VEJAS_IDLE_CLOSE_SECS").ok().and_then(|v| v.parse::<u64>().ok()).unwrap_or(60) {
        0 => None,
        secs => Some(Duration::from_secs(secs)),
    };
    let mut delay = Duration::from_secs(1);
    'outer: loop {
        if !RUNNING.load(Ordering::SeqCst) || handle.stop.load(Ordering::SeqCst) {
            break;
        }
        let attempt = contained(|| -> Result<(), String> {
            let src = fs::read_to_string(&handle.spec.path).map_err(|e| e.to_string())?;
            let mut program =
                varpulis_engine::Program::compile(&src).map_err(|e| e.to_string())?;
            let version: Arc<str> = versions::hash_content(&src).into();
            let code_file: Arc<str> = relative_to(&root, &handle.spec.path).into();
            let mut sources: Vec<(String, String)> = Vec::new();
            for b in program.sources() {
                let Some(subject) = b.topic_override.clone() else {
                    return Err(format!(
                        "`.from({}, ...)` names no topic: a detect unit consumes a bus subject, \
                         write `.from({}, topic: \"{subj_root}.some.subject\")`",
                        b.connector_name, b.connector_name
                    ));
                };
                sources.push((subject, b.event_type.clone()));
            }
            if sources.is_empty() {
                return Err("no `.from(<connector>, topic: \"...\")` declaration: nothing to consume".into());
            }
            let stem = handle
                .spec
                .name
                .rsplit(':')
                .next()
                .unwrap_or(&handle.spec.name)
                .to_string();
            let nc = bus::connect(&url)?;
            let js = nc.jetstream();
            let _ = js.add_stream(&bus::StreamConfig {
                name: stream.clone(),
                subjects: vec![format!("{subj_root}.>")],
                ..Default::default()
            });
            // One durable consumer per unit over every `.from()` subject, so the
            // engine sees the bus in stream order: a sequence across two subjects
            // is judged in the order the events were published, also when a
            // backlog is replayed after a restart. (One consumer per subject
            // delivered a backlog subject by subject, and a `->` across subjects
            // broke: e2e/detect D2, backlog round.) One reader thread pulls it
            // into one channel. Stop is shared: the unit's flag plus one for
            // this attempt, so a restart never leaves a stale reader behind.
            let attempt_stop = Arc::new(AtomicBool::new(false));
            let _stop_reader = StopOnDrop(attempt_stop.clone());
            let (tx, rx) = std::sync::mpsc::channel::<bus::Message>();
            let durable = handle.spec.name.replace([':', '.'], "_");
            let filters: Vec<String> = sources.iter().map(|(s, _)| s.clone()).collect();
            // Snapshot and resume (ADR-0031 points 4-5), for a program with state
            // to keep — sequences, windows, joins. Its latest snapshot, if it is
            // this program's, is restored and the consumer resumes at the sequence
            // after it: everything acked since then replays, and a sequence that
            // closed in that window emits again — at least once, like every unit.
            // A snapshot of another version of the program is ignored: the state
            // of one program is not the state of another. A stateless program
            // keeps no snapshot and resumes at its ack floor, as a flow does.
            let stateful = program_is_stateful(&program);
            let store = if stateful { connectors::open_state_store(&js) } else { None };
            if stateful && store.is_none() {
                eprintln!(
                    "[vejas] {}: no snapshot store: open sequences are lost on a crash",
                    handle.spec.name
                );
            }
            let mut resume_seq: Option<u64> = None;
            if let Some(store) = &store {
                match connectors::snapshot_get(store, &handle.spec.name)? {
                    Some(snap) if *snap.version == *version => {
                        program
                            .restore(&snap.state)
                            .map_err(|e| format!("restore snapshot: {e}"))?;
                        eprintln!(
                            "[vejas] {}: restored its snapshot ({} bytes, stream seq {}, taken {}s ago)",
                            handle.spec.name,
                            snap.state.len(),
                            snap.seq,
                            now_secs().saturating_sub(snap.taken_at)
                        );
                        resume_seq = Some(snap.seq + 1);
                    }
                    Some(snap) => eprintln!(
                        "[vejas] {}: its snapshot is of another version of the program ({} ≠ {}): ignored",
                        handle.spec.name, snap.version, version
                    ),
                    None => {}
                }
            }
            // After the restore: a restored window's sources start counting
            // their silence from now.
            program.set_idle_grace(idle_grace);
            match resume_seq {
                Some(seq) => {
                    // The server cannot move an existing consumer: re-create it after the snapshot.
                    let _ = js.delete_consumer(&stream, &durable);
                    js.create_pull_consumer(&stream, &durable, &filters, connectors::ack_wait(), Some(seq))?;
                    metrics::observe_restore(&handle.spec.name, seq);
                }
                None => js.create_pull_consumer(&stream, &durable, &filters, connectors::ack_wait(), None)?,
            }
            // A first snapshot at the consumer's position, so a crash before the
            // first cadence still replays from here rather than resuming past
            // events the state never saw.
            if let (Some(store), None) = (&store, resume_seq) {
                let floor = js.ack_floor(&stream, &durable).unwrap_or(0);
                if let Err(e) = snapshot_unit(store, &handle.spec.name, &version, &program, floor) {
                    eprintln!("[vejas] {}: first snapshot: {e}", handle.spec.name);
                }
            }
            let sub = js
                .pull_durable(&stream, &durable, None)
                .map_err(|e| format!("{durable}: {e}"))?;
            {
                let stop = attempt_stop.clone();
                let unit = handle.clone();
                let name = handle.spec.name.clone();
                thread::spawn(move || {
                    while RUNNING.load(Ordering::SeqCst)
                        && !unit.stop.load(Ordering::SeqCst)
                        && !stop.load(Ordering::SeqCst)
                    {
                        match sub.round() {
                            Ok(round) => {
                                let mut n = 0usize;
                                for msg in round {
                                    n += 1;
                                    // the replay of another unit's dead letter
                                    // (ADR-0015 Amendment C)
                                    if msg.replay_elsewhere(&name) {
                                        let _ = msg.ack();
                                        continue;
                                    }
                                    if tx.send(msg).is_err() {
                                        return;
                                    }
                                }
                                metrics::observe_fetch(&name, n);
                            }
                            Err(e) => {
                                eprintln!("[vejas] {name}: pull: {e}");
                                thread::sleep(Duration::from_secs(1));
                            }
                        }
                    }
                });
            }
            set_state(&handle, |st| {
                st.status = "running".into();
                st.started_at = Some(now_secs());
                st.last_error = None;
            });
            eprintln!(
                "[vejas] {} (vpl, embedded engine) consuming {}",
                handle.spec.name,
                sources.iter().map(|(s, _)| s.as_str()).collect::<Vec<_>>().join(", ")
            );
            // Snapshot bookkeeping: the highest acked stream sequence, the
            // sequences left for redelivery (a snapshot must not stand for a
            // sequence with an unprocessed message below it), acks since the
            // last snapshot and when it was taken.
            let mut acked_max: u64 = 0;
            let mut retrying: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
            let mut acks_since_snapshot: u64 = 0;
            let mut last_snapshot = Instant::now();
            // A refused emit fails the whole batch here, which redelivers: a
            // detect unit's engine has taken in every event of it.
            let mut outbox = nc.batch_stored(&subj_root);
            let result = (|| -> Result<(), String> {
                loop {
                    if !RUNNING.load(Ordering::SeqCst) || handle.stop.load(Ordering::SeqCst) {
                        // A clean stop: what is acked is in the snapshot, so a
                        // restart replays nothing.
                        if let Some(store) = &store {
                            if acks_since_snapshot > 0 && retrying.is_empty() {
                                if let Err(e) = snapshot_unit(store, &handle.spec.name, &version, &program, acked_max) {
                                    eprintln!("[vejas] {}: snapshot on stop: {e}", handle.spec.name);
                                }
                            }
                        }
                        return Ok(());
                    }
                    // A batch is whatever has arrived: wait for one message, take
                    // the rest without waiting, bounded.
                    let first = match rx.recv_timeout(Duration::from_millis(500)) {
                        Ok(m) => m,
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                            // Nothing to feed: close what time has closed on
                            // quiet sources. Those emits stand for no message,
                            // so there is nothing to ack; a crash before the
                            // next snapshot replays and closes them again.
                            if idle_grace.is_some() {
                                match program.tick() {
                                    Ok(emits) if !emits.is_empty() => {
                                        // No message stands behind a window that time
                                        // closed: each tick that emits starts a trace.
                                        let hop = trace::Hop::begin(None);
                                        let carried = hop.context();
                                        let start_nanos = metrics::now_nanos();
                                        let mut subjects = Vec::with_capacity(emits.len());
                                        let mut lost: Option<String> = None;
                                        for emit in &emits {
                                            let subject = emit_subject(emit, &subj_root, &stem);
                                            match nc.publish_traced(&subject, emit.to_payload(), &carried) {
                                                Ok(()) => subjects.push(subject),
                                                Err(e) => {
                                                    eprintln!("[vejas] {}: publish {subject}: {e}", handle.spec.name);
                                                    // nothing redelivers a tick: these emits are gone
                                                    lost.get_or_insert(format!("publish {subject}: {e}"));
                                                }
                                            }
                                        }
                                        metrics::observe_tick_emits(&handle.spec.name, emits.len() as u64);
                                        if metrics::wants(hop.sampled, lost.is_some()) {
                                            let span = metrics::Span::hop(&hop, metrics::Kind::Producer, &handle.spec.name, start_nanos)
                                                .attr("vejas.flow.version", metrics::Attr::Computed(version.clone()))
                                                .attr("vejas.emits", emits.len() as u64);
                                            record::end(
                                                match lost {
                                                    Some(e) => span.failed(e),
                                                    None => span,
                                                },
                                                record::Extras::new().emits(subjects),
                                            );
                                        }
                                    }
                                    Ok(_) => {}
                                    Err(e) => eprintln!("[vejas] {}: tick: {e}", handle.spec.name),
                                }
                            }
                            continue;
                        }
                        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                            return Err("the consumer reader stopped".into())
                        }
                    };
                    let round_t0 = Instant::now();
                    let mut batch = vec![first];
                    while batch.len() < 256 {
                        match rx.try_recv() {
                            Ok(m) => batch.push(m),
                            Err(_) => break,
                        }
                    }
                    let mut to_ack: Vec<(bus::Message, u64)> = Vec::new();
                    // A success is recorded once the barrier confirms its emits left.
                    let mut done: Vec<(metrics::Span, record::Extras)> = Vec::new();
                    for msg in batch {
                        if !RUNNING.load(Ordering::SeqCst) || handle.stop.load(Ordering::SeqCst) {
                            return Ok(()); // un-acked: redelivers to whoever holds the durable next
                        }
                        let seq = msg.delivery().map(|i| i.stream_seq).unwrap_or(0);
                        // The consumer's filters are the `.from()` subjects, so one
                        // matches; the first declared wins when two overlap.
                        let Some(event_type) = sources
                            .iter()
                            .find(|(topic, _)| connectors::subject_matches(topic, &msg.subject))
                            .map(|(_, event_type)| event_type.as_str())
                        else {
                            return Err(format!(
                                "received {} which no `.from()` subject matches: consumer `{durable}` \
                                 is not the one this unit created",
                                msg.subject
                            ));
                        };
                        let t0 = Instant::now();
                        // The hop continues the trace of the message that closed
                        // or fed a window; what it emits carries it on (ADR-0034).
                        let hop = trace::Hop::begin(trace::of(&msg));
                        let carried = hop.context();
                        let observing = metrics::observing();
                        let start_nanos = metrics::now_nanos();
                        // The program decodes the payload itself: its decoder
                        // interns field names and event types once per program,
                        // not once per message (21.6 → 5.4 µs an event).
                        match program.feed_json(event_type, &msg.data) {
                            Err(varpulis_engine::Error::Event(e)) => {
                                // Not an event: permanent poison. Dead-letter it
                                // (ADR-0015), publish before ack, nak if the DLQ
                                // write fails.
                                let err = format!("bad payload: {e}");
                                eprintln!("[vejas] {}: {err}", handle.spec.name);
                                let delivered =
                                    msg.delivery().map(|i| i.delivered).unwrap_or(1);
                                metrics::observe(&handle.spec.name, false, 0, 0.0);
                                let dead = connectors::to_dlq(&js, &handle.spec.name, &msg.subject, delivered, &err, &msg.data, &version, Some(&carried), msg.delivery().map(|d| d.message()), &durable);
                                match &dead {
                                    Ok(()) => {
                                        metrics::inc_dead_letter(&handle.spec.name);
                                        let _ = msg.ack();
                                        retrying.remove(&seq);
                                    }
                                    Err(_) => {
                                        let _ = msg.nak();
                                        retrying.insert(seq);
                                    }
                                }
                                if observing {
                                    let span = hop_span(&hop, &handle.spec.name, &code_file, &msg, &version, None, start_nanos);
                                    record::end(
                                        if dead.is_ok() { span.attr("vejas.dlq", true) } else { span }.failed(err),
                                        record::Extras::new().input(&msg.data),
                                    );
                                }
                            }
                            fed => {
                                // A run's emits leave all together or not at all
                                // (ADR-0032): one the batch refuses, a payload over
                                // the server's limit, takes the others back out of
                                // the batch, and the message fails as a run that
                                // errs does, dead-lettered once its deliveries are
                                // spent. Its emits used to leave at every delivery.
                                let mark = outbox.held();
                                let run = fed.map_err(|e| e.to_string()).and_then(|emits| {
                                    let mut subjects = Vec::with_capacity(emits.len());
                                    for emit in &emits {
                                        let subject = emit_subject(emit, &subj_root, &stem);
                                        outbox
                                            .publish_traced(&subject, emit.to_payload(), &carried)
                                            .map_err(|e| format!("publish {subject}: {e}"))?;
                                        subjects.push(subject);
                                    }
                                    Ok(subjects)
                                });
                                match run {
                                    Ok(subjects) => {
                                        let emits = subjects.len() as u64;
                                        metrics::observe(&handle.spec.name, true, emits, t0.elapsed().as_secs_f64());
                                        if observing && hop.sampled {
                                            let span = hop_span(&hop, &handle.spec.name, &code_file, &msg, &version, None, start_nanos)
                                                .attr("vejas.emits", emits);
                                            done.push((span, record::Extras::new().emits(subjects.clone()).input(&msg.data)));
                                        }
                                        record_trace(
                                            &handle.spec.name,
                                            &msg.subject,
                                            true,
                                            None,
                                            subjects,
                                            connectors::trace_preview(&msg.data),
                                            None,
                                            Some(&hop),
                                            msg.delivery(),
                                        );
                                        to_ack.push((msg, seq));
                                    }
                                    Err(e) => {
                                        outbox.rollback(mark);
                                        eprintln!("[vejas] {}: {e}", handle.spec.name);
                                        metrics::observe(&handle.spec.name, false, 0, t0.elapsed().as_secs_f64());
                                        let delivered =
                                            msg.delivery().map(|i| i.delivered).unwrap_or(1);
                                        let mut dead_lettered = false;
                                        if delivered >= MAX_DELIVERIES {
                                            match connectors::to_dlq(&js, &handle.spec.name, &msg.subject, delivered, &e, &msg.data, &version, Some(&carried), msg.delivery().map(|d| d.message()), &durable) {
                                                Ok(()) => {
                                                    dead_lettered = true;
                                                    metrics::inc_dead_letter(&handle.spec.name);
                                                    let _ = msg.ack();
                                                    retrying.remove(&seq);
                                                }
                                                Err(_) => {
                                                    let _ = msg.nak();
                                                    retrying.insert(seq);
                                                }
                                            }
                                        } else {
                                            retrying.insert(seq); // left un-acked: redelivers after ack_wait
                                        }
                                        if observing {
                                            let span = hop_span(&hop, &handle.spec.name, &code_file, &msg, &version, None, start_nanos);
                                            let span = if dead_lettered { span.attr("vejas.dlq", true) } else { span };
                                            record::end(span.failed(e.clone()), record::Extras::new().input(&msg.data));
                                        }
                                        record_trace(&handle.spec.name, &msg.subject, false, Some(e.clone()),
                                            vec![], connectors::trace_preview(&msg.data), None, Some(&hop), msg.delivery());
                                        set_state(&handle, |st| st.last_error = Some(e));
                                    }
                                }
                            }
                        }
                    }
                    let processed_at = Instant::now();
                    // Publish-before-ack barrier, one round-trip per batch (ADR-0002).
                    let confirmed = outbox.confirm();
                    match &confirmed {
                        Ok(()) => {
                            let _ = nc.ack_all(to_ack.iter().map(|(m, _)| m));
                            for (_, seq) in &to_ack {
                                retrying.remove(seq);
                                acked_max = acked_max.max(*seq);
                            }
                            acks_since_snapshot += to_ack.len() as u64;
                        }
                        Err(e) => eprintln!(
                            "[vejas] {}: flush before ack failed: {e} (batch redelivers)",
                            handle.spec.name
                        ),
                    }
                    end_confirmed(done, &confirmed);
                    metrics::observe_round(
                        &handle.spec.name,
                        0.0,
                        (processed_at - round_t0).as_secs_f64(),
                        processed_at.elapsed().as_secs_f64(),
                    );
                    if let Some(store) = &store {
                        if acks_since_snapshot > 0
                            && retrying.is_empty()
                            && (acks_since_snapshot >= snapshot_every_acks
                                || last_snapshot.elapsed() >= snapshot_every)
                        {
                            match snapshot_unit(store, &handle.spec.name, &version, &program, acked_max) {
                                Ok(()) => {
                                    acks_since_snapshot = 0;
                                    last_snapshot = Instant::now();
                                }
                                Err(e) => eprintln!(
                                    "[vejas] {}: snapshot: {e} (the next batch tries again)",
                                    handle.spec.name
                                ),
                            }
                        }
                    }
                }
            })();
            result
        });
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

/// Whether the program has state worth a snapshot: a sequence, a window or a
/// join. Read off a snapshot of the freshly compiled program: the engine lists
/// every such operator in it, with or without anything in flight. A snapshot
/// that is not JSON is taken as stateful.
fn program_is_stateful(program: &varpulis_engine::Program) -> bool {
    let Ok(bytes) = program.snapshot() else {
        return true;
    };
    let Ok(v) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
        return true;
    };
    ["sase_states", "window_states", "join_states"].iter().any(|k| {
        v.get(k)
            .and_then(|m| m.as_object())
            .is_some_and(|m| !m.is_empty())
    })
}

/// Take the unit's snapshot standing for stream sequence `seq` and store it.
fn snapshot_unit(
    store: &bus::Objects,
    unit: &str,
    version: &str,
    program: &varpulis_engine::Program,
    seq: u64,
) -> Result<(), String> {
    let t0 = Instant::now();
    let state = program.snapshot().map_err(|e| e.to_string())?;
    let bytes = state.len();
    let snap = connectors::Snapshot {
        version: version.to_string(),
        seq,
        taken_at: now_secs(),
        state,
    };
    connectors::snapshot_put(store, unit, &snap)?;
    metrics::observe_snapshot(unit, seq, bytes, t0.elapsed().as_secs_f64());
    Ok(())
}

/// The project a script is in: the folder that holds its `flows/` or
/// `services/`, or the `packages/` its package is in; else VEJAS_ROOT, else
/// the current folder.
fn project_of(path: &Path) -> PathBuf {
    let path = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    let scripts = path
        .ancestors()
        .skip(1)
        .find(|dir| matches!(dir.file_name().and_then(|n| n.to_str()), Some("flows" | "services")));
    match scripts.and_then(Path::parent) {
        Some(holder) => match holder.parent() {
            Some(packages) if packages.file_name().is_some_and(|n| n == "packages") => {
                packages.parent().unwrap_or(holder).to_path_buf()
            }
            _ => holder.to_path_buf(),
        },
        None => PathBuf::from(env::var("VEJAS_ROOT").unwrap_or_else(|_| ".".into())),
    }
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

/// Run one supervised attempt with its panics caught. Each unit's supervisor is
/// the thread that runs it, so a panic used to end the supervision too: the unit
/// stopped for good while its status stayed `running`, `restarts` 0 and
/// `last_error` null. Caught here, a panic takes the path of any other failure:
/// `restarting`, the error on record, backoff, another attempt.
fn contained(attempt: impl FnOnce() -> Result<(), String>) -> Result<(), String> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(attempt)).unwrap_or_else(|p| {
        let text = p
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| p.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "unknown panic".into());
        Err(format!("panic: {text}"))
    })
}

/// Start a background loop of the runtime itself, which no supervisor watches.
/// A panic used to end such a loop for good with nothing to show for it: in v0.4.0
/// a burst of connections during boot took `version_watch`, and a promoted version
/// no longer reached that instance. The thread carries a name, and a panic starts
/// the loop again.
fn background(name: &'static str, body: impl Fn() + Send + 'static) {
    let spawned = thread::Builder::new().name(name.into()).spawn(move || {
        while RUNNING.load(Ordering::SeqCst) {
            let run = contained(|| {
                body();
                Ok(())
            });
            match run {
                Ok(()) => break,
                Err(e) => {
                    eprintln!("[vejas] {name}: {e}; starting it again");
                    thread::sleep(Duration::from_secs(2));
                }
            }
        }
    });
    if let Err(e) = spawned {
        eprintln!("[vejas] {name}: cannot start its thread: {e}");
    }
}

/// Raises a flag when dropped, so that a helper thread stops on every way out of
/// an attempt, a panic included.
struct StopOnDrop(Arc<AtomicBool>);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
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
    let supervised = handle.clone();
    let spawned = thread::Builder::new().name(spec.name.clone()).spawn(move || match spec.kind {
        Kind::Flow => supervise_vjs(handle, root),
        Kind::Connector => supervise_connector(handle, root),
        Kind::Detect => supervise_detect(handle, root),
    });
    if let Err(e) = spawned {
        eprintln!("[vejas] {}: cannot start its thread: {e}", supervised.spec.name);
        set_state(&supervised, |st| {
            st.status = "failed".into();
            st.last_error = Some(format!("cannot start its thread: {e}"));
        });
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
        let attempt = contained(|| -> Result<(), String> {
            let src = fs::read_to_string(&handle.spec.path).map_err(|e| e.to_string())?;
            let prog = vjs::parse(&src)?;
            let driver_name = prog.driver.clone().ok_or("no `driver` declaration")?;
            let driver = connectors::driver_for(&driver_name)
                .ok_or_else(|| unknown_driver(&driver_name))?;
            // Resolve config by EVALUATING the manifest (so secret("…") in a
            // config value resolves to the real value, fail-closed). The
            // manifest's UPPERCASE variables are the driver's config.
            let mut cfg_engine =
                vjs::Engine::live(root.clone(), handle.spec.pkg.clone());
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
                host: connectors::host(),
            };
            // watch the handle's stop flag in a sidecar thread → clears ctx.running.
            // Also exits if `running` was cleared elsewhere (a lost singleton lease),
            // so it doesn't outlive its attempt.
            let h2 = handle.clone();
            let r2 = running.clone();
            thread::spawn(move || {
                while RUNNING.load(Ordering::SeqCst)
                    && !h2.stop.load(Ordering::SeqCst)
                    && r2.load(Ordering::SeqCst)
                {
                    thread::sleep(Duration::from_millis(200));
                }
                r2.store(false, Ordering::SeqCst);
            });
            // Singleton sources run on exactly one instance (ADR-0020): stand by
            // until this instance holds the lease, then run. The guard releases the
            // lease when it drops (after driver.run returns) — instant handoff on a
            // graceful stop. If the lease is lost mid-run, the renewal thread clears
            // `running`, the driver returns, and the supervisor re-enters standby.
            let _lease = if cluster::is_singleton(driver.kind()) {
                let js = bus::connect(&url)?.jetstream();
                match cluster::open_lease_store(&js) {
                    Some(store) => {
                        let key = connectors::dlq_unit_token(&handle.spec.name);
                        set_state(&handle, |st| {
                            st.status = format!("standby ({})", driver.kind())
                        });
                        let h3 = handle.clone();
                        let alive = move || {
                            RUNNING.load(Ordering::SeqCst) && !h3.stop.load(Ordering::SeqCst)
                        };
                        match cluster::acquire_blocking(&store, &key, running.clone(), alive) {
                            Some(guard) => {
                                set_state(&handle, |st| {
                                    st.status = format!("running ({}, leader)", driver.kind())
                                });
                                Some(guard)
                            }
                            None => return Ok(()), // asked to stop while standing by
                        }
                    }
                    // No JetStream KV: run unleased — single-instance behaviour.
                    None => None,
                }
            } else {
                None
            };
            driver.run(&ctx)
        });
        stop.store(false, Ordering::SeqCst);
        match attempt {
            Ok(()) => {
                // Driver stopped. On shutdown/removal, finish. Otherwise a
                // singleton lost its lease mid-run — loop back to standby and
                // re-acquire rather than exiting the connector (ADR-0020).
                if !RUNNING.load(Ordering::SeqCst) || handle.stop.load(Ordering::SeqCst) {
                    break;
                }
                delay = Duration::from_secs(1);
                thread::sleep(Duration::from_millis(300));
            }
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

/// Watch VEJAS_VERSIONS for promotes/evictions and hot-reload the affected flow
/// on every instance (ADR-0021). The KV value is the authority; this watch is the
/// notification — a missed event self-heals because the restarted flow re-reads
/// the current value via `resolve_source`. Reconnects on a dropped watch.
fn version_watch(registry: Registry, root: PathBuf) {
    let url = env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    while RUNNING.load(Ordering::SeqCst) {
        let store = bus::connect(&url)
            .ok()
            .and_then(|bus| versions::open_store(&bus.jetstream()));
        let Some(store) = store else {
            thread::sleep(Duration::from_secs(2));
            continue;
        };
        let mut watch = match store.watch_all() {
            Ok(w) => w,
            Err(_) => {
                thread::sleep(Duration::from_secs(2));
                continue;
            }
        };
        while RUNNING.load(Ordering::SeqCst) {
            let key = match watch.next_timeout(Duration::from_millis(500)) {
                Ok(Some(key)) => key,
                Ok(None) => continue,
                Err(_) => break,
            };
            // map the KV key token back to a supervised flow unit and restart it
            // (it then re-resolves the overlay: apply on a promote, baseline on
            // an eviction/delete).
            let target = {
                let reg = registry.lock().unwrap();
                reg.keys().find(|n| versions::key_token(n) == key).cloned()
            };
            if let Some(name) = target {
                eprintln!("[vejas] version change for {name} → hot-reloading");
                restart_unit(&registry, &root, &name);
            }
        }
        thread::sleep(Duration::from_secs(1)); // watch ended: reconnect
    }
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
    let mut detects = Vec::new();
    for handle in reg.values() {
        let st = handle.state.lock().unwrap();
        let entry = json!({
            "name": handle.spec.name,
            "file": handle.spec.path.display().to_string(),
            "pkg": handle.spec.pkg,
            "lang": if matches!(handle.spec.kind, Kind::Detect) { "vpl" } else { "vjs" },
            "status": st.status,
            "restarts": st.restarts,
            "started_at": st.started_at,
            "last_error": st.last_error,
        });
        match handle.spec.kind {
            Kind::Flow => flows.push(entry),
            Kind::Connector => connectors.push(entry),
            Kind::Detect => detects.push(entry),
        }
    }
    json!({ "flows": flows, "connectors": connectors, "detects": detects })
}

/// Connector instances (manifests), for the graph: name, driver, kind, in/out.
fn connector_graph(root: &Path) -> Vec<Value> {
    let mut out = Vec::new();
    let subj_root = env::var("VEJAS_SUBJECT_ROOT").unwrap_or_else(|_| "vx".into());
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
                // NATS patterns: `*` is one token, `>` the rest. A webhook
                // publishes on <root>.<what the caller asked for>, within its
                // ALLOW list when it has one (an entry allows itself and below).
                let allow: Vec<&str> = cfg
                    .get("ALLOW")
                    .and_then(|v| v.as_array())
                    .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
                    .unwrap_or_default();
                let subjects: Vec<String> = match cfg.get("SUBJECT").and_then(|v| v.as_str()) {
                    Some(s) => vec![s.to_string()],
                    None if driver == "http-in" && !allow.is_empty() => {
                        allow.iter().flat_map(|a| [format!("{subj_root}.{a}"), format!("{subj_root}.{a}.>")]).collect()
                    }
                    None => vec![format!("{subj_root}.>")],
                };
                entry["subjects_out"] = json!(subjects);
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
                "doc": vjs::literal_doc(&src, e.value_span.0),
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
            o.insert("doc".into(), json!(vjs::literal_doc(&src, e.value_span.0)));
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

/// The rules-view projection of a flow (ADR-0019): its top-level if/elif/else
/// arms as faithful sentences or verbatim-raw blocks, plus the source file (so
/// the panel can edit the inline literals through the existing surface loop).
fn rules_json(root: &Path, file: &str) -> Result<String, String> {
    let path = guard_path(root, file).ok_or("path not allowed")?;
    let src = fs::read_to_string(&path).map_err(|e| e.to_string())?;
    let prog = vjs::parse(&src)?;
    let rules = vjs::flow_rules(&prog, &src, &prog.surface);
    Ok(json!({"file": file, "rules": rules}).to_string())
}

/// A flow as its page shows it: what it reads from its event, each value it
/// computes and from what, what it checks (the rules-view), where it sends,
/// and the comment each business literal carries. Derived from the code,
/// read-only: the panel changes literals through the surface.
fn shape_json(root: &Path, file: &str) -> Result<String, String> {
    let path = guard_path(root, file).ok_or("path not allowed")?;
    let src = fs::read_to_string(&path).map_err(|e| e.to_string())?;
    let prog = vjs::parse(&src)?;
    // what each invoked service makes, resolved as a run would resolve it
    let mut engine = vjs::Engine::shadow(root.to_path_buf(), pkg_of_path(&path));
    let services: Vec<(String, Vec<String>)> = prog
        .invokes
        .iter()
        .filter_map(|name| engine.service_makes(name).ok().map(|made| (name.clone(), made)))
        .collect();
    let mut shape = vjs::flow_shape(&prog, &src, &services);
    shape["file"] = json!(file);
    shape["source"] = json!(prog.source);
    shape["rules"] = Value::Array(vjs::flow_rules(&prog, &src, &prog.surface));
    Ok(shape.to_string())
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
    let mut engine = vjs::Engine::shadow(root.to_path_buf(), pkg_of_path(&path));
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
    // the first component whole, and a file in it: `flows_old/a.vjs` or a
    // `services.json` at the root is in none of these
    let mut parts = Path::new(rel).components();
    let ok_dir = matches!(parts.next(), Some(std::path::Component::Normal(d))
        if ["flows", "services", "connectors", "packages"].iter().any(|ok| d == std::ffi::OsStr::new(ok)))
        && parts.next().is_some();
    let ok_ext = rel.ends_with(".vjs") || rel.ends_with(".json");
    if !(ok_dir && ok_ext) {
        return None;
    }
    let p = root.join(rel);
    // Symlink defense: the string checks above stop `..`, absolute escapes and
    // wrong extensions, but a symlink whose NAME ends `.vjs`/`.json` and sits in
    // an allowed dir would still pass them, and fs::read/write would follow it —
    // an arbitrary host-file read/write (e.g. flows/x.vjs → /etc/passwd or the
    // secrets file). So resolve symlinks and require the real target to stay under
    // the real root; reject anything that escapes. An existing path is
    // canonicalized whole (catches a symlinked FILE); a not-yet-existing path (a
    // new file being written) has its PARENT canonicalized (catches a symlinked
    // directory in the chain). We return the original `p` — callers only fs-read/
    // write it — so no caller's path form changes; the check is the guard.
    let canon_root = root.canonicalize().ok()?;
    let resolved = match p.canonicalize() {
        Ok(c) => c,
        Err(_) => p.parent()?.canonicalize().ok()?.join(p.file_name()?),
    };
    if resolved.starts_with(&canon_root) {
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
- A string is in double quotes and reads JSON's escapes: `\n`, `\t`, `\r`, `\"`, `\\`, `\/`, `\b`, `\f`, and `\u` with four hex digits. In an f-string, `\{` and `\}` give a brace, as `{{` and `}}` do. Any other escape is an error.
- An `emit` subject must be a valid subject: no whitespace, no control character. A subject built from an event field is checked when the flow runs, and a value that breaks the rule fails the run, so the message reaches the DLQ after its deliveries. Map such a field through a table rather than trusting it.
- `secret()` never yields the value where a run is SHOWN instead of performed (preview, vejas_run_flow, flow-as-tool, shadow-replay, time-travel, canary, golden capture): it stays fail-closed and evaluates to the placeholder `<secret:path/key>`, so a branch comparing with a secret takes the not-equal path there.
- A service in `services/` is VejasScript, or the declaration of a system that answers: `driver "http-rpc"`, `URL = "{BASE_URL}/orders/{id}"` and its settings (see Composing services). `o = invoke get_order(id: x)` then gives `{status, body, headers}` with the arguments, and a call that gets no answer fails the run with the service's name and the cause. Every argument but `body` fills a `{name}` of the URL, and none takes the name of a setting.
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
    let mut command = Command::new(agent);
    connectors::without_runtime_credentials(&mut command);
    let out = command
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

fn stem_of(path: &Path) -> Option<String> {
    path.file_stem().map(|s| s.to_string_lossy().into_owned())
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
    // the name it runs under: the file's, which a taken name made order_sync_2
    let name = stem_of(&target).unwrap_or(name);
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
    let name = stem_of(&target).unwrap_or(name);
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

/// Why a manifest's driver runs no connector: a driver a service declares
/// answers a flow's invoke from `services/` (ADR-0032).
fn unknown_driver(driver: &str) -> String {
    if connectors::answer::DRIVERS.contains(&driver) {
        format!("driver {driver:?} answers a flow's invoke: declare it in services/, and invoke the service")
    } else {
        format!("unknown driver {driver:?}")
    }
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
            .ok_or_else(|| unknown_driver(&driver_name))?;
        let mut engine = vjs::Engine::live(root.to_path_buf(), pkg_of_path(&path));
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
            host: connectors::host(),
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
        let mut engine = vjs::Engine::live(root.to_path_buf(), pkg_of_path(&path));
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
    let nc = bus::connect(&url)?;
    let payload = serde_json::to_vec(op).map_err(|e| e.to_string())?;
    let msg = nc
        .request(&subject, payload, Duration::from_secs(30))
        .map_err(|e| format!("SAP connector did not answer on {subject}: {e}"))?;
    serde_json::from_slice::<Value>(&msg.data).map_err(|e| e.to_string())
}

// ───────────────────────── dead-letter queue (ADR-0015) ─────────────────────────
fn dlq_js() -> Result<bus::JetStream, String> {
    let url = env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let js = bus::connect(&url)?.jetstream();
    connectors::ensure_dlq_stream(&js);
    Ok(js)
}

/// The trace record's stream, read on the runtime's shared connection: an
/// operator's or an agent's question, a few requests. Writing it has a
/// connection of its own (record.rs).
fn record_js() -> Result<bus::JetStream, String> {
    let url = env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    Ok(bus::connect(&url)?.jetstream())
}

/// What the record says when it has nothing to say.
fn record_absent(e: &str) -> bool {
    e.contains("stream not found")
}

/// One trace from the record (ADR-0034): every hop kept, in the order they
/// started. A 404 says why a trace can be missing.
fn trace_view(root: &Path, id: &str) -> Result<Value, (u16, String)> {
    let Some(key) = record::trace_key(id) else {
        return Err((400, "a trace id is 32 hexadecimal digits".into()));
    };
    let js = record_js().map_err(|e| (503, e))?;
    match record::read(&js, &key) {
        Ok(hops) if hops.is_empty() => Err((
            404,
            format!("no hop of trace {key} is in the record: sampled out, past the retention limits, or never seen by this cell"),
        )),
        Ok(mut hops) => {
            let mut sources = Sources::new(Some(js));
            for hop in &mut hops {
                join_code(root, hop, &mut sources);
            }
            Ok(json!({"trace_id": key, "hops": hops}))
        }
        Err(e) if record_absent(&e) => Err((
            404,
            "the trace record holds nothing: VEJAS_TRACES is off, or no hop has ended yet".into(),
        )),
        Err(e) => Err((500, e)),
    }
}

/// A file read for a trace view: the version it hashes to, and its lines.
struct Source {
    version: String,
    lines: Arc<Vec<String>>,
}

impl Source {
    fn of(src: &str) -> Source {
        Source {
            version: versions::hash_content(src),
            lines: Arc::new(src.lines().map(|l| l.trim().chars().take(200).collect()).collect()),
        }
    }
}

/// One trace view's reads: each file once, whatever version a hop names, and
/// the version store's overlays the same way.
struct Sources {
    js: Option<bus::JetStream>,
    store: Option<Option<bus::Kv>>,
    files: HashMap<String, Option<Source>>,
    overlays: HashMap<String, Option<Source>>,
    /// Bytes of code given so far.
    given: usize,
}

/// What one trace view reads and gives out, at most. Anyone who can publish
/// on the bus can write a record: these keep one from making a view slow or
/// huge.
const JOIN_FILES_MAX: usize = 64;
const JOIN_FILE_BYTES_MAX: u64 = 1 << 20;
const JOIN_CODE_MAX: usize = 1 << 20;
const JOIN_STEPS_MAX: usize = vjs::STEPS_HEAD + vjs::STEPS_TAIL + 1;

impl Sources {
    fn new(js: Option<bus::JetStream>) -> Sources {
        Sources { js, store: None, files: HashMap::new(), overlays: HashMap::new(), given: 0 }
    }

    /// The lines of `file` if it still hashes to `version`.
    fn file(&mut self, root: &Path, file: &str, version: &str) -> Option<Arc<Vec<String>>> {
        if !self.files.contains_key(file) {
            if self.files.len() >= JOIN_FILES_MAX {
                return None;
            }
            let read = || -> Option<Source> {
                let path = guard_path(root, file)?;
                if fs::metadata(&path).ok()?.len() > JOIN_FILE_BYTES_MAX {
                    return None;
                }
                Some(Source::of(&fs::read_to_string(&path).ok()?))
            };
            self.files.insert(file.to_string(), read());
        }
        let source = self.files.get(file)?.as_ref()?;
        (source.version == version).then(|| source.lines.clone())
    }

    /// The lines of the version promoted over a flow's file in a cluster
    /// (ADR-0024), if the version store still holds it as `version`.
    fn overlay(&mut self, flow: &str, version: &str) -> Option<Arc<Vec<String>>> {
        if !self.overlays.contains_key(flow) {
            if self.overlays.len() >= JOIN_FILES_MAX {
                return None;
            }
            let js = &self.js;
            let store = self.store.get_or_insert_with(|| js.as_ref().and_then(versions::open_store));
            let source = store
                .as_ref()
                .and_then(|store| versions::get_version(store, flow))
                .and_then(|overlay| overlay["source"].as_str().map(Source::of));
            self.overlays.insert(flow.to_string(), source);
        }
        let source = self.overlays.get(flow)?.as_ref()?;
        (source.version == version).then(|| source.lines.clone())
    }
}

/// A service's file, from its name as steps give it.
fn service_file(service: &str) -> Option<String> {
    match service.split_once(':')? {
        ("default", svc) => Some(format!("services/{svc}.vjs")),
        (pkg, svc) => Some(format!("packages/{pkg}/services/{svc}.vjs")),
    }
}

/// Give each step of a hop the line of code it ran (ADR-0034, point 8): the
/// flow's from the file the record names, or from the version promoted over
/// it, when one of them is the version that ran (`vejas.flow.version`); a
/// service's from its file, when it is the version the record's `services`
/// gives. A step whose code is in neither keeps its line number only. The
/// record itself holds no code: any it carries is taken out first.
fn join_code(root: &Path, hop: &mut Value, sources: &mut Sources) {
    let Some(steps) = hop.get_mut("steps").and_then(Value::as_array_mut) else {
        return;
    };
    for step in steps.iter_mut() {
        if let Some(step) = step.as_object_mut() {
            step.remove("code");
        }
    }
    let attrs = &hop["attributes"];
    let flow = match (attrs["code.file.path"].as_str(), attrs["vejas.flow.version"].as_str()) {
        (Some(file), Some(version)) => sources.file(root, file, version).or_else(|| {
            // promoted in a cluster: the file is still the version it forked from
            let promoted = attrs["vejas.flow.baseline"].is_string();
            let name = hop["unit"].as_str().and_then(|unit| unit.strip_prefix("flow:"));
            name.filter(|_| promoted).and_then(|name| sources.overlay(name, version))
        }),
        _ => None,
    };
    let services: HashMap<String, String> = hop["services"]
        .as_object()
        .map(|services| {
            let named = services.iter().filter_map(|(name, version)| Some((name.clone(), version.as_str()?.to_string())));
            named.take(vjs::SERVICES_MAX).collect()
        })
        .unwrap_or_default();
    let mut of_service: HashMap<String, Option<Arc<Vec<String>>>> = HashMap::new();
    let Some(steps) = hop.get_mut("steps").and_then(Value::as_array_mut) else {
        return;
    };
    for step in steps.iter_mut().take(JOIN_STEPS_MAX) {
        let Some(step) = step.as_object_mut() else { continue };
        let Some(line) = step.get("line").and_then(Value::as_u64) else { continue };
        let lines = match step.get("service").and_then(Value::as_str) {
            None => flow.clone(),
            Some(service) => match of_service.get(service) {
                Some(lines) => lines.clone(),
                None => {
                    let lines = services
                        .get(service)
                        .and_then(|version| sources.file(root, &service_file(service)?, version));
                    of_service.insert(service.to_string(), lines.clone());
                    lines
                }
            },
        };
        let Some(code) = lines.as_ref().and_then(|l| l.get((line as usize).checked_sub(1)?)) else { continue };
        if sources.given + code.len() > JOIN_CODE_MAX {
            return;
        }
        sources.given += code.len();
        step.insert("code".into(), Value::String(code.clone()));
    }
}

/// The latest traces whose recorded hops mention `text`, newest first.
fn trace_search(text: &str, limit: usize) -> Result<Value, (u16, String)> {
    if text.trim().is_empty() {
        return Err((400, "nothing to search for".into()));
    }
    let js = record_js().map_err(|e| (503, e))?;
    let (traces, searched) = match record::search(&js, text, limit.clamp(1, 100), record::SEARCH_WINDOW) {
        Ok(found) => found,
        Err(e) if record_absent(&e) => (Vec::new(), 0),
        Err(e) => return Err((500, e)),
    };
    Ok(json!({"search": text.trim(), "searched_hops": searched, "traces": traces}))
}

/// List dead letters, newest first (each carries its stream `seq` for replay/purge).
fn dlq_list(limit: usize) -> Result<Vec<Value>, String> {
    let js = dlq_js()?;
    let state = js.stream_state(connectors::DLQ_STREAM)?;
    let (first, last) = (state.first_seq, state.last_seq);
    let mut out = Vec::new();
    if last == 0 || state.messages == 0 {
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

/// Record a replay or a purge: one summary per call, best-effort like the
/// live-promote path. A purge in particular leaves no other trace.
fn dlq_audit(
    js: &bus::JetStream,
    kind: &str,
    actor: &str,
    seq: Option<u64>,
    unit: Option<&str>,
    count: u64,
) {
    let record = json!({
        "ts": now_secs(), "actor": actor, "kind": kind,
        "unit": unit, "seq": seq, "count": count,
    });
    if let Err(e) = connectors::to_audit(js, unit.unwrap_or("dlq"), &record) {
        eprintln!("[vejas] {kind}: audit not written: {e}");
    }
}

/// Replay dead letters: re-publish each to its ORIGINAL subject (where the
/// corrected flow reprocesses it), then remove it from the DLQ. Publish-before-
/// delete, so a failed re-inject keeps the dead letter. By `seq`, by `unit`, or
/// all (both None). A replay goes to the unit whose letter it is, and to no
/// other consumer of the subject (ADR-0015 Amendment C): a letter whose unit
/// is no longer declared stays, as nothing would take its replay.
fn dlq_replay(root: &Path, seq: Option<u64>, unit: Option<&str>, actor: &str) -> Result<Value, String> {
    let js = dlq_js()?;
    struct Letter {
        seq: u64,
        subject: String,
        payload: Vec<u8>,
        // The trace the letter died in (ADR-0034): the replay continues it.
        died_in: Option<trace::Context>,
        // The message that died: the replay names it, the same event.
        original: Option<u64>,
        // The unit it died in: the replay is for it alone.
        unit: String,
        // The durable it read the message with; none in a letter written before.
        consumer: Option<String>,
    }
    let mut targets: Vec<Letter> = Vec::new();
    let take = |env: &Value| Letter {
        seq: env["seq"].as_u64().unwrap_or(0),
        subject: env["original_subject"].as_str().unwrap_or("").to_string(),
        payload: env["payload"].as_str().unwrap_or("").as_bytes().to_vec(),
        died_in: env["traceparent"].as_str().and_then(|tp| trace::Context::parse(tp, None)),
        original: env["original_seq"].as_u64(),
        unit: env["unit"].as_str().unwrap_or("").to_string(),
        consumer: env["consumer"].as_str().map(str::to_string),
    };
    if let Some(s) = seq {
        let m = js.get_message(connectors::DLQ_STREAM, s)?;
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
    // the units declared now: a removed one stays in the registry, stopped
    let units: HashSet<String> = scan_all(root).into_iter().map(|spec| spec.name).collect();
    let stream = env::var("VEJAS_STREAM").unwrap_or_else(|_| "VEJAS".into());
    for letter in targets {
        let Letter { seq: s, subject: subj, payload, died_in, original, unit: of_unit, consumer } = letter;
        if subj.is_empty() || s == 0 {
            errs.push(format!("seq {s}: the letter names no subject. Discard it."));
            continue;
        }
        if !units.contains(&of_unit) {
            errs.push(if of_unit.is_empty() {
                format!("seq {s}: the letter names no unit. Discard it, or publish its payload by hand.")
            } else {
                format!("seq {s}: no unit {of_unit} to take it. Restore the unit, or discard the letter.")
            });
            continue;
        }
        // A fix that moved the unit to another subject would leave the replay to
        // no one: the other consumers of this one ack it unread.
        if let Some(consumer) = consumer {
            match js.consumer_filters(&stream, &consumer) {
                Ok(f) if f.is_empty() || f.iter().any(|p| connectors::subject_matches(p, &subj)) => {}
                Ok(_) => {
                    errs.push(format!("seq {s}: {of_unit} no longer reads {subj}. Discard the letter, or publish its payload where it reads now."));
                    continue;
                }
                Err(e) => {
                    errs.push(format!("seq {s}: {of_unit} has no consumer to take it ({e}). Start the unit, then replay."));
                    continue;
                }
            }
        }
        let hop = trace::Hop::begin(died_in);
        let start_nanos = metrics::now_nanos();
        let published = js.publish_replay(&subj, &payload, &hop.context(), &of_unit, original);
        if metrics::wants(hop.sampled, published.is_err()) {
            let span = metrics::Span::hop(&hop, metrics::Kind::Producer, "dlq:replay", start_nanos)
                .messaging("send", &subj)
                .attr("vejas.dlq.seq", s);
            record::end(
                match &published {
                    Ok(_) => span,
                    Err(e) => span.failed(e.clone()),
                },
                record::Extras::new().emits(vec![subj.clone()]).input(&payload),
            );
        }
        match published {
            Ok(_) => {
                let _ = js.delete_message(connectors::DLQ_STREAM, s);
                n += 1;
            }
            Err(e) => errs.push(format!("seq {s}: {e}")),
        }
    }
    dlq_audit(&js, "dlq-replay", actor, seq, unit, n);
    Ok(json!({"replayed": n, "errors": errs}))
}

/// Discard dead letters without replaying — by `seq`, by `unit`, or all.
fn dlq_purge(seq: Option<u64>, unit: Option<&str>, actor: &str) -> Result<Value, String> {
    let js = dlq_js()?;
    if seq.is_none() && unit.is_none() {
        let purged = js.purge_stream(connectors::DLQ_STREAM)?;
        dlq_audit(&js, "dlq-purge", actor, None, None, purged);
        return Ok(json!({"purged": purged}));
    }
    let mut n = 0u64;
    if let Some(s) = seq {
        js.delete_message(connectors::DLQ_STREAM, s)?;
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
    dlq_audit(&js, "dlq-purge", actor, seq, unit, n);
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

/// Curate a golden test from a real processed event (candidate 4.4): take the
/// event a flow actually handled (from the trace ring), re-run the flow on it to
/// capture what it ACTUALLY emitted — that is the contract, "real traffic becomes
/// the test" — and write a golden case under tests/vjs/. The generated name is
/// prefixed `curated_` so it never collides with the hand-written NN_ cases.
fn golden_from_event(root: &Path, flow_unit: &str, seq: u64) -> Result<Value, String> {
    // the stored input for this flow+seq (the ring keeps the full event as replay
    // fuel; /events strips it, but we read TRACES directly here)
    let event = {
        let map = TRACES.get_or_init(|| Mutex::new(HashMap::new())).lock().unwrap();
        map.get(flow_unit)
            .and_then(|ring| ring.iter().find(|e| e["seq"].as_u64() == Some(seq)))
            .and_then(|e| e.get("event").cloned())
            .filter(|v| !v.is_null())
            .ok_or("no stored event for that flow/seq (ring rotated, or a non-JSON event)")?
    };
    // map the unit back to its flow file (repo-relative)
    let file = vjs_files(root)
        .into_iter()
        .map(|(p, _)| p)
        .find(|p| flow_proc_name(p) == flow_unit)
        .ok_or("flow not found for that unit")?;
    let rel = file
        .strip_prefix(root)
        .unwrap_or(&file)
        .to_string_lossy()
        .to_string();
    // the case goes into the repository: a credential of the event does not,
    // and the run it records reads the event as every shadow run does
    let prog = vjs::parse(&fs::read_to_string(&file).map_err(|e| e.to_string())?)?;
    let (event, hidden) = shadow_input(&event, &record::shown(), &vjs::event_reads(&prog));
    // re-run to capture the real emits (deterministic — the flow is pure)
    let expect_emits = run_flow_on_input(root, &rel, &event)?;
    let stem = file
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();
    let case = json!({ "flow": rel, "input": event, "expect_emits": expect_emits });
    let dir = root.join("tests").join("vjs");
    fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    let name = format!("curated_{stem}_{seq}.json");
    fs::write(
        dir.join(&name),
        serde_json::to_string_pretty(&case).unwrap_or_default(),
    )
    .map_err(|e| e.to_string())?;
    Ok(with_hidden(json!({"ok": true, "case": name, "flow": rel, "expect_emits": expect_emits.len()}), hidden))
}

fn run_flow_on_input(root: &Path, file: &str, input: &Value) -> Result<Vec<Value>, String> {
    let path = guard_path(root, file).ok_or("path not allowed")?;
    let src = fs::read_to_string(&path).map_err(|e| e.to_string())?;
    let prog = vjs::parse(&src)?;
    let mut engine = vjs::Engine::shadow(root.to_path_buf(), pkg_of_path(&path));
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

/// Publish an API flow's emits to the bus, carrying the request's trace. An
/// error means the bus did not confirm them all, so the caller must not take
/// the request as done: it is answered 503 and sends it again (ADR-0002).
fn publish_emits(emits: &[(String, Value)], carried: &trace::Context) -> Result<(), Unsent> {
    if emits.is_empty() {
        return Ok(());
    }
    let url = env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let nc = bus::connect(&url).map_err(|e| Unsent::Unconfirmed(format!("NATS unreachable: {e}")))?;
    let root = env::var("VEJAS_SUBJECT_ROOT").unwrap_or_else(|_| "vx".into());
    ensure_stream(&nc, &root);
    let mut batch = nc.batch_stored(&root);
    for (subj, payload) in emits {
        batch
            .publish_traced(subj, serde_json::to_vec(payload).unwrap_or_default(), carried)
            .map_err(|e| Unsent::Refused(format!("publish {subj}: {e}")))?;
    }
    // One the stream refused may be stored later: the request is sent again.
    match batch.confirm_each().map_err(Unsent::Unconfirmed)?.into_iter().next() {
        None => Ok(()),
        Some(refused) => Err(Unsent::Unconfirmed(format!("publish {}: {}", refused.subject, refused.error))),
    }
}

/// Make the stream an api flow's emits go to, once, if no unit has made it: a
/// runtime that only answers HTTP runs no unit on the bus that would. Without
/// it, the stream would refuse each emit for want of a stream (503).
fn ensure_stream(nc: &bus::Bus, root: &str) {
    static MADE: AtomicBool = AtomicBool::new(false);
    if MADE.load(Ordering::Relaxed) {
        return;
    }
    let made = nc.jetstream().add_stream(&bus::StreamConfig {
        name: env::var("VEJAS_STREAM").unwrap_or_else(|_| "VEJAS".into()),
        subjects: vec![format!("{root}.>")],
        ..Default::default()
    });
    if made.is_ok() {
        MADE.store(true, Ordering::Relaxed);
    }
}

/// Why an api flow's emits did not all leave.
enum Unsent {
    /// The batch refused one, a payload over the server's limit or a subject
    /// it cannot carry: none left, and sending the request again changes nothing.
    Refused(String),
    /// The bus did not confirm them: some may be missing, and the request can
    /// be sent again.
    Unconfirmed(String),
}

impl Unsent {
    fn error(&self) -> &str {
        match self {
            Unsent::Refused(e) | Unsent::Unconfirmed(e) => e,
        }
    }
}

/// The trace context an HTTP request carries, if any (ADR-0034).
fn request_trace(request: &tiny_http::Request) -> Option<trace::Context> {
    let header = |name: &'static str| {
        request
            .headers()
            .iter()
            .find(|h| h.field.equiv(name))
            .map(|h| h.value.as_str())
    };
    trace::Context::parse(header(trace::TRACEPARENT)?, header(trace::TRACESTATE))
}

/// Run an API flow on the request event; map its `respond` to (status, body).
/// Does the /api flow at `file` write to the bus (an `emit`, or an `invoke` of a
/// service that may emit)? Used to gate read-method routes that write (Finding A,
/// volet-2). Uses the full AST visitor `Program::writes_bus` — NOT `emit_subjects`
/// or `invokes`, which list only statically resolvable targets and so miss a
/// dynamic emit subject (A') or an `invoke` nested in a subexpression (A''); either
/// gap let a read-method flow publish to the bus unauthenticated.
fn flow_writes_bus(root: &Path, file: &str) -> bool {
    guard_path(root, file)
        .and_then(|p| fs::read_to_string(p).ok())
        .and_then(|src| vjs::parse(&src).ok())
        .map(|prog| prog.writes_bus())
        .unwrap_or(false)
}

fn run_api_flow(
    root: &Path,
    file: &str,
    input: &Value,
    method: &str,
    incoming: Option<trace::Context>,
) -> (u16, String) {
    // Instrument the synchronous /api path like the bus hot path (ADR-0016
    // follow-up): a run-time error is `ok:false`; a `respond 4xx/5xx` is a valid
    // business outcome, so the flow still counts as processed.
    // resolved once: each resolution canonicalizes twice
    let path = guard_path(root, file);
    let unit = path.as_deref().map(flow_proc_name).unwrap_or_else(|| file.to_string());
    // The request's trace, honoured when it carries one (ADR-0034): the flow's
    // span hangs under the caller's, and its emits carry the trace on.
    let hop = trace::Hop::begin(incoming);
    let traced = metrics::tracing() && hop.sampled;
    let t0 = Instant::now();
    let start_nanos = metrics::now_nanos();
    let mut version = String::new();
    let mut route = String::new();
    let mut invoked = Vec::new();
    let mut steps = None;
    let mut failure = None;
    let result = (|| -> Result<vjs::Ctx, String> {
        let path = path.as_deref().ok_or("path not allowed")?;
        let src = fs::read_to_string(path).map_err(|e| e.to_string())?;
        version = versions::hash_content(&src);
        let prog = vjs::parse(&src)?;
        route = prog
            .api
            .as_deref()
            .and_then(|spec| spec.split_whitespace().nth(1))
            .unwrap_or_default()
            .to_string();
        let mut engine = vjs::Engine::live(root.to_path_buf(), pkg_of_path(path));
        engine.trace_under(traced.then_some(&hop));
        engine.carry(Some(hop.context()));
        engine.record_steps(record::on());
        let run = vjs::run(&prog, input, &mut engine);
        invoked = engine.take_spans();
        steps = engine.take_steps();
        failure = engine.take_failure();
        run
    })();
    // The emits leave before the answer: a caller told 2xx can count on them.
    let unsent = result.as_ref().ok().and_then(|ctx| publish_emits(&ctx.emits, &hop.context()).err());
    let failed = result.is_err() || unsent.is_some();
    // A failed request counts no emit, as a failed run of the flow loop.
    let emits = match &result {
        Ok(ctx) if !failed => ctx.emits.len() as u64,
        _ => 0,
    };
    metrics::observe(&unit, !failed, emits, t0.elapsed().as_secs_f64());
    let (code, body) = match (&result, &unsent) {
        (Ok(_), Some(Unsent::Refused(e))) => (
            500,
            json!({"error": format!("{e}, so none of this request's events left")}).to_string(),
        ),
        (Ok(_), Some(Unsent::Unconfirmed(e))) => (
            503,
            json!({"error": format!("the bus did not confirm this request's events ({e}), so some may be missing: send the request again")}).to_string(),
        ),
        (Ok(ctx), None) => match &ctx.response {
            Some((status, body)) => (vjs::http_status(status), body.to_string()),
            None => (
                200,
                json!({"ok": true, "emits": ctx.emits.iter().map(|(s, p)| json!({"subject": s, "payload": p})).collect::<Vec<_>>()}).to_string(),
            ),
        },
        // A service that answered nothing (ADR-0032): the caller may try again.
        (Err(e), _) => match &failure {
            Some(f) if f.timed_out => (504, json!({"error": e}).to_string()),
            Some(_) => (502, json!({"error": e}).to_string()),
            None => (500, json!({"error": e}).to_string()),
        },
    };
    if traced || (metrics::tracing() && failed) {
        for span in invoked {
            metrics::span(invoke_span(&unit, span));
        }
    }
    if metrics::wants(hop.sampled, failed) {
        let subjects: Vec<String> = match &result {
            Ok(ctx) => ctx.emits.iter().map(|(s, _)| s.clone()).collect(),
            Err(_) => Vec::new(),
        };
        let mut span = metrics::Span::hop(&hop, metrics::Kind::Server, &unit, start_nanos)
            .attr("http.request.method", method)
            .attr("http.response.status_code", code as i64)
            .attr("vejas.emits", emits);
        if !route.is_empty() {
            span = span.attr("http.route", route);
        }
        if !version.is_empty() {
            span = span.attr("vejas.flow.version", metrics::Attr::Computed(version.into()));
        }
        if let Some(path) = &path {
            span = span.attr("code.file.path", metrics::Attr::Computed(relative_to(root, path).into()));
        }
        if let Some(e) = result.err().or(unsent.map(|u| u.error().to_string())) {
            span = span.failed(e);
        }
        record::end(
            span,
            record::Extras::new().emits(subjects).input_value(input).answer(Some(&body)).steps(steps),
        );
    }
    (code, body)
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
        json!({"name": "vejas_vpl_check", "description": "Check a detect unit (a VPL program under detects/, run by the embedded Varpulis engine — ADR-0031): the engine's own verdict, the same refusal a running unit would give. Returns {ok} or {ok:false, error}.", "inputSchema": obj(json!({"file": {"type": "string", "description": "path of the .vpl file"}}), vec!["file"])}),
        json!({"name": "vejas_read", "description": "Read a script file (.vjs) or fixture (.json).", "inputSchema": obj(json!({"path": {"type": "string"}}), vec!["path"])}),
        json!({"name": "vejas_write_flow", "description": "Create or overwrite a .vjs script (parse-validated, hot-reloaded) or a .json fixture. path under flows/, connectors/, or packages/<pkg>/flows|services|fixtures.", "inputSchema": obj(json!({"path": {"type": "string"}, "content": {"type": "string"}}), vec!["path", "content"])}),
        json!({"name": "vejas_set_literal", "description": "Rewrite one literal of the business surface in place (constant, or a table/mapping entry via key).", "inputSchema": obj(json!({"file": {"type": "string"}, "name": {"type": "string"}, "key": {"type": "string", "description": "entry key, or '-' for a whole constant"}, "value": {}}), vec!["file", "name", "value"])}),
        json!({"name": "vejas_rollback_literal", "description": "Roll a business-surface literal back to the value it held before its most recent promote (from the VEJAS_AUDIT trail). Rollback is itself an audited promote to the recorded previous value — forward-only, hot-reloaded, previewable first with vejas_replay_literal. Returns {restored, was, rolled_back_promote_ts}.", "inputSchema": obj(json!({"file": {"type": "string"}, "name": {"type": "string"}, "key": {"type": "string", "description": "entry key, or '-' for a whole constant"}}), vec!["file", "name"])}),
        json!({"name": "vejas_time_travel", "description": "Time-travel (ADR-0021): replay a window of REAL persisted traffic through a whole CANDIDATE version of a flow and diff its emissions against the current effective version, joined by stream sequence. Read-only, the bus untouched, a candidate's emits never reach a real subject. Use to preview an arbitrary rewrite (not just one literal) before promoting it. Returns {events, changed, not_recorded, results:[{seq, before, after, changed, hidden?}]}: `not_recorded` counts the events left out of the comparison because a run called a system whose answer is not recorded, named in that run's `not_recorded`. The replayed events are masked as the record masks them, flags and nulls kept: `hidden` names a field the code reads that the run did not see, and a rule on it may go another way live.", "inputSchema": obj(json!({"file": {"type": "string"}, "candidate": {"type": "string", "description": "the whole candidate VejasScript source"}, "n": {"type": "integer", "description": "how many recent events to replay (default 20)"}}), vec!["file", "candidate"])}),
        json!({"name": "vejas_canary_start", "description": "Start a canary (ADR-0021): shadow-follow a flow's LIVE traffic and diff a candidate version against the current effective version as events arrive, accumulating a diff. Read-only (shadow — no real emit). Refuses if a canary is already running for the flow. Auto-stops if the live version changes under it (reason in status).", "inputSchema": obj(json!({"file": {"type": "string"}, "candidate": {"type": "string", "description": "the whole candidate VejasScript source"}}), vec!["file", "candidate"])}),
        json!({"name": "vejas_canary_status", "description": "Read a canary's accumulating diff: {running, events, changed, not_recorded, stop_reason, results:[{seq, before, after, changed, hidden?}]}. `not_recorded` counts the events left out of the comparison because a run called a system whose answer is not recorded. The replayed events are masked as the record masks them, flags and nulls kept: `hidden` names a field the code reads that the run did not see, and a rule on it may go another way live.", "inputSchema": obj(json!({"file": {"type": "string"}}), vec!["file"])}),
        json!({"name": "vejas_canary_stop", "description": "Stop a running canary for a flow (its shadow consumer exits; the last diff stays readable).", "inputSchema": obj(json!({"file": {"type": "string"}}), vec!["file"])}),
        json!({"name": "vejas_propose", "description": "Submit a governed change PROPOSAL for a human to approve in the panel (ADR-0024) — you can propose but never approve. kind='set_literal' (payload {file, name, key, value}) or kind='version' (payload {file, candidate: whole source}). Attach `evidence` you gathered (vejas_time_travel results, vejas_canary stats) — the panel shows it next to Approve, and flags 'no evidence' loudly. The proposal is pinned to the current baseline and auto-expires if a deploy/promote moves it. Returns the stored proposal (id, status:pending).", "inputSchema": obj(json!({"kind": {"type": "string"}, "payload": {"type": "object"}, "evidence": {"type": "object"}}), vec!["kind", "payload"])}),
        json!({"name": "vejas_proposals", "description": "List the proposal queue with status (pending/approved/rejected/expired) and evidence. Read-only. Approve/reject are human panel actions, not tools.", "inputSchema": obj(json!({}), vec![])}),
        json!({"name": "vejas_replay_literal", "description": "Shadow-replay a proposed literal change against REAL persisted traffic: hydrate the flow's recent events from JetStream (read-only, the bus untouched — falls back to the in-memory trace ring when the stream is empty or the flow has no bus source), rerun them against the current AND the patched script, and return the before/after emit diff (with `source`: jetstream|trace-ring). `not_recorded` counts the events left out of the comparison because a run called a system whose answer is not recorded. The replayed events are masked as the record masks them, flags and nulls kept: `hidden` names a field the code reads that the run did not see, and a rule on it may go another way live. Nothing is written — promote with vejas_set_literal.", "inputSchema": obj(json!({"file": {"type": "string"}, "name": {"type": "string"}, "key": {"type": "string", "description": "entry key, or '-' for a whole constant"}, "value": {}, "n": {"type": "integer", "description": "how many recent events to replay (default 20)"}}), vec!["file", "name", "value"])}),
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
        json!({"name": "vejas_trace", "description": "What happened to one business event, end to end, from the trace record (ADR-0034). With `trace_id` (32 hex digits, as /events, vejas_events and every dead letter give it): every hop the event went through, in the order they started: unit, subject, delivery attempt, flow version, the subjects it emitted, the payload it received (credential-named keys masked, vault values taken out), the statements each flow ran with their line of code, a sink's downstream answer, the error and whether it was dead-lettered. With `search` instead (an order number, a customer id, any text): the latest traces whose payloads, subjects, units or errors mention it, newest first; read one of them by its trace_id. Answer from the record, not from a guess.", "inputSchema": obj(json!({"trace_id": {"type": "string"}, "search": {"type": "string"}, "limit": {"type": "integer"}}), vec![])}),
        json!({"name": "vejas_dlq", "description": "List dead letters — poison messages parked in the DLQ (ADR-0015) instead of dropped: unit, original subject, attempts, last error, payload, each with a `seq` for replay/purge. Newest first, at most 200; a payload past 16 KB is cut there.", "inputSchema": obj(json!({"limit": {"type": "integer"}}), vec![])}),
        json!({"name": "vejas_dlq_replay", "description": "Replay dead letters — re-inject each to its ORIGINAL subject, for the unit it died in alone (no other consumer of the subject runs it), so the (now corrected) flow reprocesses it, then remove it from the DLQ. A letter that no unit would take (its unit removed, or reading another subject now) stays, and `errors` says why. Target one by `seq`, a whole `unit`, or all (omit both). Do this AFTER fixing the cause (vejas_set_literal, previewed with vejas_replay_literal). Refused in governed mode: a human replays them in the panel, after approving the fix.", "inputSchema": obj(json!({"seq": {"type": "integer"}, "unit": {"type": "string"}}), vec![])}),
        json!({"name": "vejas_dlq_purge", "description": "Discard dead letters without replaying — by `seq`, by `unit`, or all (omit both). Refused in governed mode: a human discards them in the panel.", "inputSchema": obj(json!({"seq": {"type": "integer"}, "unit": {"type": "string"}}), vec![])}),
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
        "vejas_topology" => text(as_shown(topology_json(registry)).to_string()),
        "vejas_graph" => text(graph_json(root).to_string()),
        "vejas_surface" => text(surface_json(root).to_string()),
        "vejas_read" => {
            let p = args["path"].as_str().ok_or("path required")?;
            let path = guard_path(root, p).ok_or("path not allowed")?;
            text(fs::read_to_string(&path).map_err(|e| e.to_string())?)
        }
        "vejas_language" => text(LANGUAGE_VJS.to_string()),
        "vejas_vpl_check" => {
            let file = args["file"].as_str().ok_or("file required")?;
            let src = fs::read_to_string(root.join(file))
                .or_else(|_| fs::read_to_string(file))
                .map_err(|e| e.to_string())?;
            match varpulis_engine::Program::check(&src) {
                Ok(()) => text(json!({"ok": true, "file": file}).to_string()),
                Err(e) => text(json!({"ok": false, "file": file, "error": e.to_string()}).to_string()),
            }
        }
        "vejas_events" => text(events_json(args["flow"].as_str())),
        "vejas_write_flow" => {
            approval_gate()?;
            cluster_write_guard()?;
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
            approval_gate()?;
            let file = args["file"].as_str().ok_or("file required")?;
            let lname = args["name"].as_str().ok_or("name required")?;
            let key = args["key"].as_str().unwrap_or("-");
            // In a cluster a promote is a version (fans out), not a local write.
            if cluster::clustered() {
                return text(promote_version(root, file, lname, key, &args["value"], "mcp")?.to_string());
            }
            let path = guard_path(root, file).ok_or("path not allowed")?;
            let src = fs::read_to_string(&path).map_err(|e| e.to_string())?;
            let before = vjs::get_literal(&src, lname, key).unwrap_or(Value::Null);
            let new_src = vjs::set_literal(&src, lname, key, &args["value"])?;
            fs::write(&path, new_src).map_err(|e| e.to_string())?;
            record_promote(root, file, lname, key, &before, &args["value"], "mcp");
            let _ = reload(registry, root);
            text(json!({"ok": true}).to_string())
        }
        "vejas_rollback_literal" => {
            approval_gate()?;
            cluster_write_guard()?;
            let file = args["file"].as_str().ok_or("file required")?;
            let lname = args["name"].as_str().ok_or("name required")?;
            let key = args["key"].as_str().unwrap_or("-");
            let out = rollback_literal(root, file, lname, key, "rollback:mcp")?;
            let _ = reload(registry, root);
            text(out.to_string())
        }
        "vejas_replay_literal" => {
            let file = args["file"].as_str().ok_or("file required")?;
            let lname = args["name"].as_str().ok_or("name required")?;
            let key = args["key"].as_str().unwrap_or("-");
            let n = args["n"].as_u64().unwrap_or(20) as usize;
            text(as_shown(replay_literal(root, file, lname, key, &args["value"], n)?).to_string())
        }
        "vejas_time_travel" => {
            let file = args["file"].as_str().ok_or("file required")?;
            let candidate = args["candidate"].as_str().ok_or("candidate required")?;
            let n = args["n"].as_u64().unwrap_or(20) as usize;
            text(as_shown(time_travel(root, file, candidate, n)?).to_string())
        }
        "vejas_canary_start" => {
            let file = args["file"].as_str().ok_or("file required")?;
            let candidate = args["candidate"].as_str().ok_or("candidate required")?;
            text(as_shown(canary_start(root, file, candidate)?).to_string())
        }
        "vejas_canary_status" => {
            let file = args["file"].as_str().ok_or("file required")?;
            text(as_shown(canary_read(root, file)?).to_string())
        }
        "vejas_canary_stop" => {
            let file = args["file"].as_str().ok_or("file required")?;
            text(as_shown(canary_stop(root, file)?).to_string())
        }
        "vejas_propose" => {
            let kind = args["kind"].as_str().ok_or("kind required")?;
            let payload = args["payload"].clone();
            let evidence = args.get("evidence").cloned().unwrap_or(Value::Null);
            text(submit_proposal(root, kind, payload, "mcp", evidence)?.to_string())
        }
        "vejas_proposals" => text(json!({"proposals": proposals_shown(reconcile_and_list(root)?)}).to_string()),
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
            approval_gate()?;
            cluster_write_guard()?;
            let prompt = args["prompt"].as_str().ok_or("prompt required")?;
            let res = generate_flow(root, prompt)?;
            let _ = reload(registry, root);
            text(res)
        }
        "vejas_new_connector" => {
            approval_gate()?;
            cluster_write_guard()?;
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
            approval_gate()?;
            // A shared Vault backend is cluster-safe; a local FileStore is not
            // (one instance's file only). Vault stays allowed, local refuses.
            if cluster::clustered() && secrets::default_store().kind() != "vault" {
                return Err("clustered: a local-file secret store would be written on \
                    one instance only — use a shared Vault backend, see ADR-0020"
                    .into());
            }
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
            text(as_shown(probe_connector(root, file)).to_string())
        }
        "vejas_provision" => {
            approval_gate()?;
            cluster_write_guard()?;
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
            let limit = args.get("limit").and_then(|v| v.as_u64()).unwrap_or(100).min(200) as usize;
            text(as_shown(json!({"dead_letters": dlq_list(limit)?})).to_string())
        }
        "vejas_trace" => {
            let found = match (args["trace_id"].as_str(), args["search"].as_str()) {
                (Some(id), _) => trace_view(root, id),
                (None, Some(search)) => {
                    trace_search(search, args["limit"].as_u64().unwrap_or(20) as usize)
                }
                (None, None) => Err((400, "give a trace_id, or a search".into())),
            };
            text(found.map_err(|(_, e)| e)?.to_string())
        }
        "vejas_dlq_replay" => {
            dlq_agent_gate()?;
            let seq = args.get("seq").and_then(|v| v.as_u64());
            let unit = args.get("unit").and_then(|v| v.as_str());
            text(dlq_replay(root, seq, unit, "mcp")?.to_string())
        }
        "vejas_dlq_purge" => {
            dlq_agent_gate()?;
            let seq = args.get("seq").and_then(|v| v.as_u64());
            let unit = args.get("unit").and_then(|v| v.as_str());
            text(dlq_purge(seq, unit, "mcp")?.to_string())
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
    // Optional write protection: when VEJAS_TOKEN is set, every MUTATING request
    // requires `Authorization: Bearer <token>`. A mutation is any non-read method
    // — POST (the whole built-in surface, /mcp included), and PUT/PATCH/DELETE on
    // the /api flow surface. GET/HEAD are reads and stay open (they expose no
    // secret values by design) — with ONE exception handled in the /api branch: a
    // read-method flow that emits to the bus is itself a write, and is gated there.
    // (Finding A: the earlier gate keyed on the POST verb, so `api "DELETE /x"` and
    // an emitting `api "GET /x"` slipped past — a bus write with no token.)
    let wtoken = write_token();
    if let Some(tok) = &wtoken {
        if !is_read_method(&method)
            && !bearer_authorized(&request, tok)
            && !(is_human_route(&path_only) && approval_authorized(&request))
        {
            return respond(
                request,
                401,
                "unauthorized: set Authorization: Bearer <VEJAS_TOKEN>".into(),
                "text/plain",
            );
        }
    }
    // Who to attribute a mutation to in the audit (ADR-0030) — the proxy-supplied
    // X-Vejas-Actor, else "panel". Extracted once; the immutable borrow ends here.
    let actor = header_actor(&request, "panel");
    // ── HTTP API surface: flows declared `api "VERB /path"`, served under /api/ ──
    if path_only == "/api/openapi.json" {
        return respond(request, 200, openapi_json(&root).to_string(), "application/json");
    }
    if let Some(rel) = path_only.strip_prefix("/api/") {
        let m = method_str(&method);
        return match match_api_route(&root, &m, rel) {
            Some((file, params)) => {
                // Finding A (volet-2, +A'/A''): a read-method route that writes the
                // bus — via emit OR a service invoke — is a mutation; gate it. A
                // pure read-only flow (no emit, no invoke) stays open, as does a
                // writing read once authorized.
                if let Some(tok) = &wtoken {
                    if is_read_method(&method)
                        && flow_writes_bus(&root, &file)
                        && !bearer_authorized(&request, tok)
                    {
                        return respond(
                            request,
                            401,
                            "unauthorized: this endpoint writes to the bus; set Authorization: Bearer <VEJAS_TOKEN>".into(),
                            "text/plain",
                        );
                    }
                }
                let body = read_body(&mut request);
                let mut ev = match body {
                    Value::Object(o) => o,
                    _ => serde_json::Map::new(),
                };
                for (k, v) in params {
                    ev.insert(k, Value::String(v));
                }
                ev.insert("query".into(), parse_query(&url));
                let incoming = request_trace(&request);
                let (code, out) = run_api_flow(&root, &file, &Value::Object(ev), &m, incoming);
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

    // The trace record (ADR-0034, lot B): one trace by its id, or the traces a
    // search finds. Reads, open like /events and /dlq.
    if method == tiny_http::Method::Get {
        let found = if path_only == "/traces" {
            Some(match qparam(&url, "search") {
                Some(text) => trace_search(
                    &metrics::percent_decode(&text.replace('+', " ")),
                    qparam(&url, "limit").and_then(|l| l.parse().ok()).unwrap_or(20),
                ),
                None => Err((400, "GET /traces/{trace id}, or GET /traces?search=<text>".into())),
            })
        } else {
            path_only.strip_prefix("/traces/").map(|id| trace_view(&root, id))
        };
        if let Some(found) = found {
            return match found {
                Ok(v) => respond(request, 200, v.to_string(), "application/json"),
                Err((code, e)) => respond(request, code, e, "text/plain"),
            };
        }
    }

    // Proposal approve/reject (ADR-0024): dynamic /proposals/{id}/{approve|reject},
    // gated by the DISTINCT approval credential the agent does not hold.
    if method == tiny_http::Method::Post {
        if let Some(rest) = path_only.strip_prefix("/proposals/") {
            let Some((id, action)) = rest.rsplit_once('/') else {
                return respond(request, 400, "use /proposals/{id}/approve|reject".into(), "text/plain");
            };
            let approve = match action {
                "approve" => true,
                "reject" => false,
                _ => return respond(request, 400, "action must be approve or reject".into(), "text/plain"),
            };
            match approval_token() {
                None => {
                    return respond(request, 403,
                        "approval requires VEJAS_APPROVAL_TOKEN (a credential distinct from the agent's VEJAS_TOKEN)".into(),
                        "text/plain");
                }
                Some(_) if !approval_authorized(&request) => {
                    return respond(request, 401, "unauthorized: X-Approval-Token required".into(), "text/plain");
                }
                Some(_) => {}
            }
            return match decide_proposal(&registry, &root, id, approve, &actor) {
                Ok(v) => respond(request, 200, proposal_shown(v, &record::shown()).to_string(), "application/json"),
                Err(e) => respond(request, 422, e, "text/plain"),
            };
        }
    }

    match (method, path_only.as_str()) {
        (tiny_http::Method::Get, "/proposals") => match reconcile_and_list(&root) {
            Ok(list) => respond(request, 200, json!({"proposals": proposals_shown(list)}).to_string(), "application/json"),
            Err(e) => respond(request, 500, e, "text/plain"),
        },
        (tiny_http::Method::Get, "/") | (tiny_http::Method::Get, "/panel") => respond(
            request,
            200,
            panel_html(),
            "text/html; charset=utf-8",
        ),
        (tiny_http::Method::Get, "/healthz") => respond(request, 200, "ok".into(), "text/plain"),
        (tiny_http::Method::Get, "/evictions") => respond(
            request,
            200,
            json!({ "evictions": versions::recent_evictions() }).to_string(),
            "application/json",
        ),
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
                            Kind::Detect => "detect",
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
            as_shown(topology_json(&registry)).to_string(),
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
            // `flow%3Aorder_sync`, as a browser encodes it, is `flow:order_sync`
            let flow = qparam(&url, "flow").map(|f| metrics::percent_decode(&f));
            let body = match flow.as_deref() {
                Some(unit) if qparam(&url, "paths").as_deref() == Some("1") => events_with_paths_json(unit),
                unit => events_json(unit),
            };
            respond(request, 200, body, "application/json")
        }
        (tiny_http::Method::Post, "/events/golden") => {
            // Curate a golden test from a real event (candidate 4.4). Writes a
            // repo file, so cluster mode refuses it (golden tests go via git).
            if let Err(e) = approval_gate() {
                return respond(request, 409, e, "text/plain");
            }
            if let Err(e) = cluster_write_guard() {
                return respond(request, 409, e, "text/plain");
            }
            let body = read_body(&mut request);
            let flow = body["flow"].as_str().unwrap_or("");
            let seq = body["seq"].as_u64().unwrap_or(u64::MAX);
            match golden_from_event(&root, flow, seq) {
                Ok(out) => respond(request, 200, out.to_string(), "application/json"),
                Err(e) => respond(request, 422, e, "text/plain"),
            }
        }
        (tiny_http::Method::Get, "/dlq") => match dlq_list(200) {
            Ok(l) => respond(request, 200, as_shown(json!({"dead_letters": l})).to_string(), "application/json"),
            Err(e) => respond(request, 500, e, "text/plain"),
        },
        (tiny_http::Method::Post, "/dlq/replay") => {
            if let Err((code, e)) = dlq_http_gate(&request) {
                return respond(request, code, e, "text/plain");
            }
            let body = read_body(&mut request);
            let r = dlq_replay(&root, body.get("seq").and_then(|v| v.as_u64()), body.get("unit").and_then(|v| v.as_str()), &actor);
            match r {
                Ok(v) => respond(request, 200, v.to_string(), "application/json"),
                Err(e) => respond(request, 422, e, "text/plain"),
            }
        }
        (tiny_http::Method::Post, "/dlq/purge") => {
            if let Err((code, e)) = dlq_http_gate(&request) {
                return respond(request, code, e, "text/plain");
            }
            let body = read_body(&mut request);
            let r = dlq_purge(body.get("seq").and_then(|v| v.as_u64()), body.get("unit").and_then(|v| v.as_str()), &actor);
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
            if let Err(e) = approval_gate() {
                return respond(request, 409, e, "text/plain");
            }
            // Vault (shared) is cluster-safe; a local FileStore is not.
            if cluster::clustered() && secrets::default_store().kind() != "vault" {
                return respond(
                    request,
                    409,
                    "clustered: a local-file secret store would be written on one \
                     instance only — use a shared Vault backend, see ADR-0020"
                        .into(),
                    "text/plain",
                );
            }
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
            respond(request, 200, as_shown(probe_connector(&root, file)).to_string(), "application/json")
        }
        (tiny_http::Method::Post, "/provision") => {
            if let Err(e) = approval_gate() {
                return respond(request, 409, e, "text/plain");
            }
            if let Err(e) = cluster_write_guard() {
                return respond(request, 409, e, "text/plain");
            }
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
        (tiny_http::Method::Get, "/rules") => {
            let Some(file) = qparam(&url, "file") else {
                return respond(request, 400, "missing file".into(), "text/plain");
            };
            match rules_json(&root, &file) {
                Ok(json) => respond(request, 200, json, "application/json"),
                Err(err) => respond(request, 500, err, "text/plain"),
            }
        }
        (tiny_http::Method::Get, "/shape") => {
            let Some(file) = qparam(&url, "file") else {
                return respond(request, 400, "missing file".into(), "text/plain");
            };
            match shape_json(&root, &file) {
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
            if let Err(e) = approval_gate() {
                return respond(request, 409, e, "text/plain");
            }
            if let Err(e) = cluster_write_guard() {
                return respond(request, 409, e, "text/plain");
            }
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
            if let Err(e) = approval_gate() {
                return respond(request, 409, e, "text/plain");
            }
            if let Err(e) = cluster_write_guard() {
                return respond(request, 409, e, "text/plain");
            }
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
                Ok(diff) => respond(request, 200, as_shown(diff).to_string(), "application/json"),
                Err(e) => respond(request, 422, e, "text/plain"),
            }
        }
        (tiny_http::Method::Post, "/surface/timetravel") => {
            // ADR-0021: replay a window of real traffic through a whole candidate
            // version and diff it against the current effective version.
            let body = read_body(&mut request);
            let file = body["file"].as_str().unwrap_or("").to_string();
            let candidate = body["candidate"].as_str().unwrap_or("").to_string();
            let n = body["n"].as_u64().unwrap_or(20) as usize;
            match time_travel(&root, &file, &candidate, n) {
                Ok(diff) => respond(request, 200, as_shown(diff).to_string(), "application/json"),
                Err(e) => respond(request, 422, e, "text/plain"),
            }
        }
        (tiny_http::Method::Post, "/surface/canary/start") => {
            let body = read_body(&mut request);
            let file = body["file"].as_str().unwrap_or("").to_string();
            let candidate = body["candidate"].as_str().unwrap_or("").to_string();
            match canary_start(&root, &file, &candidate) {
                Ok(out) => respond(request, 200, as_shown(out).to_string(), "application/json"),
                // a double-start is refused, not silently replaced
                Err(e) if e.contains("already running") => respond(request, 409, e, "text/plain"),
                Err(e) => respond(request, 422, e, "text/plain"),
            }
        }
        (tiny_http::Method::Get, "/surface/canary") => {
            let Some(file) = qparam(&url, "file") else {
                return respond(request, 400, "missing file".into(), "text/plain");
            };
            match canary_read(&root, &file) {
                Ok(out) => respond(request, 200, as_shown(out).to_string(), "application/json"),
                Err(e) => respond(request, 422, e, "text/plain"),
            }
        }
        (tiny_http::Method::Post, "/surface/canary/stop") => {
            let body = read_body(&mut request);
            let file = body["file"].as_str().unwrap_or("").to_string();
            match canary_stop(&root, &file) {
                Ok(out) => respond(request, 200, as_shown(out).to_string(), "application/json"),
                Err(e) => respond(request, 422, e, "text/plain"),
            }
        }
        (tiny_http::Method::Post, "/surface/set") => {
            if let Err(e) = approval_gate() {
                return respond(request, 409, e, "text/plain");
            }
            let body = read_body(&mut request);
            let file = body["file"].as_str().unwrap_or("").to_string();
            let name = body["name"].as_str().unwrap_or("").to_string();
            let key = body["key"].as_str().unwrap_or("-").to_string();
            let value = body["value"].clone();
            // In a cluster, a promote is a cluster-wide VERSION, not a local write.
            if cluster::clustered() {
                return match promote_version(&root, &file, &name, &key, &value, &actor) {
                    Ok(out) => respond(request, 200, out.to_string(), "application/json"),
                    Err(e) => respond(request, 422, e, "text/plain"),
                };
            }
            let Some(p) = guard_path(&root, &file) else {
                return respond(request, 400, "path not allowed".into(), "text/plain");
            };
            let src = match fs::read_to_string(&p) {
                Ok(s) => s,
                Err(e) => return respond(request, 500, e.to_string(), "text/plain"),
            };
            let before = vjs::get_literal(&src, &name, &key).unwrap_or(Value::Null);
            match vjs::set_literal(&src, &name, &key, &value) {
                Ok(new_src) => {
                    if let Err(e) = fs::write(&p, new_src) {
                        return respond(request, 500, e.to_string(), "text/plain");
                    }
                    record_promote(&root, &file, &name, &key, &before, &value, &actor);
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
        (tiny_http::Method::Post, "/surface/rollback") => {
            if let Err(e) = approval_gate() {
                return respond(request, 409, e, "text/plain");
            }
            if let Err(e) = cluster_write_guard() {
                return respond(request, 409, e, "text/plain");
            }
            let body = read_body(&mut request);
            let file = body["file"].as_str().unwrap_or("").to_string();
            let name = body["name"].as_str().unwrap_or("").to_string();
            let key = body["key"].as_str().unwrap_or("-").to_string();
            match rollback_literal(&root, &file, &name, &key, &format!("rollback:{actor}")) {
                Ok(out) => {
                    let _ = reload(&registry, &root);
                    respond(request, 200, out.to_string(), "application/json")
                }
                Err(e) => respond(request, 422, e, "text/plain"),
            }
        }
        (tiny_http::Method::Post, "/flows/new") => {
            if let Err(e) = approval_gate() {
                return respond(request, 409, e, "text/plain");
            }
            if let Err(e) = cluster_write_guard() {
                return respond(request, 409, e, "text/plain");
            }
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
            // Governed mode + cluster guard, exactly like /flows/new: a connector
            // is generated and HOT-STARTED (and can be an exec driver — arbitrary
            // CMD), so a direct create must be refused and routed to a proposal.
            // (Finding B: this path was missing both guards while its twin had them,
            // giving an agent a governance bypass and an RCE with no human approval.)
            if let Err(e) = approval_gate() {
                return respond(request, 409, e, "text/plain");
            }
            if let Err(e) = cluster_write_guard() {
                return respond(request, 409, e, "text/plain");
            }
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
    if write_token().is_none() && beyond_loopback(&addr) {
        eprintln!(
            "[vejas] WARNING: {addr} with no VEJAS_TOKEN: whatever reaches this port can \
             write flows and run commands (exec connectors). Set VEJAS_TOKEN, or publish \
             the port on localhost only."
        );
    }
    eprintln!("[vejas] panel + monitoring on http://{addr}");
    // One thread per request, so a slow /flows/new never blocks monitoring, and a
    // bound on them, so a burst costs a 503 instead of this loop.
    let max_inflight = env::var("VEJAS_HTTP_MAX_INFLIGHT")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(256)
        .max(1);
    let inflight = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    for request in server.incoming_requests() {
        let Some(slot) = connectors::Slot::try_take(&inflight, max_inflight) else {
            metrics::inc_rejected("panel");
            respond(request, 503, "too many requests in flight".into(), "text/plain");
            continue;
        };
        let registry = registry.clone();
        let root = root.clone();
        // On failure the request drops with the closure and tiny_http answers 500.
        let spawned = thread::Builder::new().name("http-request".into()).spawn(move || {
            let _slot = slot;
            handle_request(request, registry, root)
        });
        if let Err(e) = spawned {
            metrics::inc_rejected("panel");
            eprintln!("[vejas] no thread for a request: {e}");
        }
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
    // `--version` answers and exits. It used to start a runtime, like any
    // argument the binary does not know: `docker run IMAGE --version` stayed
    // up for a day.
    if args.len() == 2 && matches!(args[1].as_str(), "--version" | "-V") {
        println!("vejas-runtime {}", env!("CARGO_PKG_VERSION"));
        return;
    }
    // Print the single credential-mask pattern (secrets::SECRET_KEY_PATTERN) so
    // the admission lint (ADR-0017) reads it without parsing source — one
    // definition, shared by the panel mask and the lint.
    if args.len() >= 2 && args[1] == "secret-pattern" {
        println!("{}", secrets::SECRET_KEY_PATTERN);
        return;
    }
    if args.len() >= 3 && args[1] == "vjs-check" {
        let path = PathBuf::from(&args[2]);
        let checked = fs::read_to_string(&path).map_err(|e| e.to_string()).and_then(|s| vjs::parse(&s)).and_then(|prog| {
            // the arguments of its calls, against the services of its project
            let mut engine = vjs::Engine::shadow(project_of(&path), pkg_of_path(&path));
            match engine.check_invokes(&prog) {
                problems if problems.is_empty() => Ok(()),
                problems => Err(problems.join("\n")),
            }
        });
        match checked {
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
    if args.len() >= 3 && args[1] == "vpl-check" {
        // The embedded engine's own verdict (ADR-0031, point 8): the same
        // refusal a running detect unit would give.
        match fs::read_to_string(&args[2])
            .map_err(|e| e.to_string())
            .and_then(|s| varpulis_engine::Program::check(&s).map_err(|e| e.to_string()))
        {
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
        let mut engine = vjs::Engine::live(root, pkg_of_path(&PathBuf::from(&args[2])));
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
                        vjs::Engine::shadow(root.clone(), pkg_of_path(&PathBuf::from(flow)));
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
    let addr = env::var("VEJAS_HTTP_ADDR").unwrap_or_else(|_| DEFAULT_HTTP_ADDR.into());
    let registry: Registry = Arc::new(Mutex::new(HashMap::new()));

    // Governed-change mode (ADR-0024): refuse to boot without the DISTINCT approval
    // credential. A governance mode whose approve door shares the agent's VEJAS_TOKEN
    // is a governance mode in name only — the agent that proposes could also approve.
    if require_approval() && approval_token().is_none() {
        eprintln!(
            "[vejas] VEJAS_REQUIRE_APPROVAL is set but VEJAS_APPROVAL_TOKEN is not — \
             refusing to start: the approval credential MUST be distinct from the \
             agent's VEJAS_TOKEN (ADR-0024). Set VEJAS_APPROVAL_TOKEN and restart."
        );
        std::process::exit(1);
    }
    if require_approval() {
        eprintln!(
            "[vejas] governed-change mode — direct mutations refused; agents submit \
             proposals, a human approves with VEJAS_APPROVAL_TOKEN (ADR-0024)"
        );
    }

    // Traces to an OTLP collector iff OTEL_EXPORTER_OTLP_ENDPOINT is set; metrics
    // are always scrapeable at GET /metrics. No-op (no thread) when unset.
    metrics::otlp_init();
    // The trace record, on unless VEJAS_TRACES=off: each hop kept in the
    // VEJAS_TRACES stream for the panel and `vejas_trace` (ADR-0034, lot B).
    record::init(&env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into()));

    // Clustering (ADR-0020): singleton sources take a KV lease so exactly one
    // instance runs them; announce this instance's identity for the operator.
    if cluster::clustered() {
        eprintln!(
            "[vejas] cluster mode — instance {} (singletons run under a lease)",
            cluster::instance_id()
        );
    }

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

    // watch VEJAS_VERSIONS: a promote on any instance fans out here (ADR-0021)
    {
        let registry = registry.clone();
        let root = root.clone();
        background("version-watch", move || version_watch(registry.clone(), root.clone()));
    }

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
    // The units have stopped: what they traced last leaves before the process.
    record::flush(Duration::from_secs(3));
    metrics::otlp_flush(Duration::from_secs(3));
}

#[cfg(test)]
mod supervision_tests {
    use super::*;

    #[test]
    fn a_panic_in_an_attempt_comes_back_as_an_error() {
        assert_eq!(contained(|| Ok(())), Ok(()));
        assert_eq!(contained(|| Err("plain".into())), Err("plain".to_string()));
        let err = contained(|| panic!("boom {}", 7)).unwrap_err();
        assert_eq!(err, "panic: boom 7");
    }

    #[test]
    fn a_background_loop_starts_again_after_a_panic() {
        let (tx, rx) = std::sync::mpsc::channel();
        let runs = AtomicU64::new(0);
        background("test-loop", move || {
            let n = runs.fetch_add(1, Ordering::SeqCst);
            tx.send(n).unwrap();
            if n == 0 {
                panic!("no thread to give");
            }
        });
        let timeout = Duration::from_secs(10);
        assert_eq!(rx.recv_timeout(timeout), Ok(0));
        assert_eq!(rx.recv_timeout(timeout), Ok(1), "the loop came back");
    }

    #[test]
    fn a_slot_comes_back_when_its_thread_panics() {
        let open = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let slot = connectors::Slot::try_take(&open, 1).unwrap();
        assert!(connectors::Slot::try_take(&open, 1).is_none(), "the bound holds");
        let _ = thread::spawn(move || {
            let _slot = slot;
            panic!("in the handler");
        })
        .join();
        assert!(connectors::Slot::try_take(&open, 1).is_some(), "the slot came back");
    }
}

#[cfg(test)]
mod language_tests {
    use super::LANGUAGE_VJS;

    /// The book's language reference is the text `vejas_language` serves every
    /// agent, word for word: a rule added to one only is a rule half the
    /// readers never see.
    #[test]
    fn the_book_shows_the_language_agents_read() {
        let book = include_str!("../../docs/book/src/reference/vejascript.md");
        let shown = book.split("```text\n").nth(1).and_then(|b| b.split("```").next());
        assert_eq!(
            shown.map(str::trim_end),
            Some(LANGUAGE_VJS.trim_end()),
            "docs/book/src/reference/vejascript.md and LANGUAGE_VJS differ"
        );
    }
}

#[cfg(test)]
mod bind_tests {
    use super::{beyond_loopback, DEFAULT_HTTP_ADDR};

    #[test]
    fn the_default_address_stays_on_this_machine() {
        assert!(!beyond_loopback(DEFAULT_HTTP_ADDR));
        assert!(!beyond_loopback("[::1]:8686"));
    }

    #[test]
    fn any_other_address_counts_as_reachable() {
        assert!(beyond_loopback("0.0.0.0:8686"));
        assert!(beyond_loopback("[::]:8686"));
        assert!(beyond_loopback("192.0.2.1:8686"));
        assert!(beyond_loopback("not an address"));
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_program_is_stateful_when_it_has_a_sequence_window_or_join() {
        let threshold = "event Order:\n    id: int\n    total: float\n\nstream Big = Order\n    .where(total > 100.0)\n    .emit(id: id)\n";
        let program = varpulis_engine::Program::compile(threshold).unwrap();
        assert!(!program_is_stateful(&program), "a filter and an emit keep nothing");
        let lateral = "event SmbConnect:\n    host: str\n    target: str\n\nevent ServiceStart:\n    host: str\n\nstream Lateral = SmbConnect as smb\n    -> ServiceStart where host == smb.target as svc\n    .within(2m)\n    .emit(to: svc.host)\n";
        let program = varpulis_engine::Program::compile(lateral).unwrap();
        assert!(program_is_stateful(&program), "an open sequence is state");
    }

    use super::*;
    use serde_json::json;

    #[test]
    fn a_source_lists_the_subjects_it_can_publish_as_nats_patterns() {
        let root = std::env::temp_dir().join(format!("vejas-cgraph-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("connectors")).unwrap();
        let write = |name: &str, src: &str| fs::write(root.join("connectors").join(name), src).unwrap();
        write("open_hook.vjs", "driver \"http-in\"\nPORT = 8787\n");
        write("shop_hook.vjs", "driver \"http-in\"\nPORT = 8788\nALLOW = [\"shop\"]\n");
        write("tick.vjs", "driver \"timer\"\nSUBJECT = \"vx.ops.ping\"\nINTERVAL_SECS = 30\n");
        let graph = connector_graph(&root);
        let out = |name: &str| graph.iter().find(|c| c["name"] == name).unwrap()["subjects_out"].clone();
        assert_eq!(out("open_hook"), json!(["vx.>"]), "without ALLOW a webhook reaches any vx subject");
        assert_eq!(out("shop_hook"), json!(["vx.shop", "vx.shop.>"]), "an ALLOW entry allows itself and below");
        assert_eq!(out("tick"), json!(["vx.ops.ping"]));
        let _ = fs::remove_dir_all(&root);
    }

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
        record_trace("flow:replay_probe", "vx.rp.test", true, None, vec!["vx.out".into()], "{}".into(), Some(json!({"amount": 700})), None, None);
        record_trace("flow:replay_probe", "vx.rp.test", true, None, vec![], "{}".into(), Some(json!({"amount": 300})), None, None);
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
    fn a_replay_leaves_out_the_events_whose_calls_are_not_recorded() {
        let root = std::env::temp_dir().join(format!("vejas-replay-calls-{}", std::process::id()));
        std::fs::create_dir_all(root.join("flows")).unwrap();
        std::fs::create_dir_all(root.join("services")).unwrap();
        std::fs::write(
            root.join("services").join("get_limit.vjs"),
            "driver \"http-rpc\"\nURL = \"http://127.0.0.1:9/limits/{id}\"\n",
        )
        .unwrap();
        std::fs::write(
            root.join("flows").join("replay_calls.vjs"),
            "source \"vx.rc.test\"\nLIMIT = 100\no = invoke get_limit(id: event.id)\nif o.body.n > LIMIT:\n  emit \"vx.out\", {n: o.body.n}\nend\n",
        )
        .unwrap();
        record_trace("flow:replay_calls", "vx.rc.test", true, None, vec![], "{}".into(), Some(json!({"id": "A"})), None, None);
        record_trace("flow:replay_calls", "vx.rc.test", true, None, vec![], "{}".into(), Some(json!({"id": "B"})), None, None);
        let diff = replay_literal(&root, "flows/replay_calls.vjs", "LIMIT", "-", &json!(10), 20).unwrap();
        // two refusals of the same call do not read as "unchanged"
        assert_eq!((&diff["events"], &diff["changed"], &diff["not_recorded"]), (&json!(2), &json!(0), &json!(2)), "{diff}");
        assert_eq!(diff["results"][0]["before"]["not_recorded"], "get_limit(id: \"B\")");
        // a candidate that calls with other arguments changes what the event does
        std::fs::write(
            root.join("flows").join("replay_args.vjs"),
            "source \"vx.ra.test\"\nPREFIX = \"\"\no = invoke get_limit(id: f\"{PREFIX}{event.id}\")\nemit \"vx.out\", {n: o.body.n}\n",
        )
        .unwrap();
        record_trace("flow:replay_args", "vx.ra.test", true, None, vec![], "{}".into(), Some(json!({"id": "A"})), None, None);
        let diff = replay_literal(&root, "flows/replay_args.vjs", "PREFIX", "-", &json!("X-"), 20).unwrap();
        assert_eq!((&diff["events"], &diff["changed"], &diff["not_recorded"]), (&json!(1), &json!(1), &json!(0)), "{diff}");
        // and one that fails before its call, or calls another service
        let call = |c: &str| json!({"emits": [], "error": "e", "not_recorded": c});
        let failed = json!({"emits": [], "error": "get_limit: the argument idd appears nowhere in its URL"});
        assert_eq!(compared(&call("get_limit(id: \"A\")"), &call("get_limit(id: \"A\")")), None);
        assert_eq!(compared(&call("get_limit(id: \"A\")"), &failed), Some(true));
        assert_eq!(compared(&failed, &call("get_limit(id: \"A\")")), Some(true));
        assert_eq!(compared(&call("get_limit(id: \"A\")"), &call("get_cap(id: \"A\")")), Some(true));
        // runs that call nothing are compared as before
        assert_eq!(compared(&json!({"emits": []}), &json!({"emits": [1]})), Some(true));
        assert_eq!(compared(&json!({"emits": []}), &json!({"emits": []})), Some(false));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn a_script_belongs_to_the_project_that_holds_its_folder() {
        let root = std::env::temp_dir().join(format!("vejas-project-{}", std::process::id()));
        for dir in ["flows", "services", "packages/billing/flows"] {
            std::fs::create_dir_all(root.join(dir)).unwrap();
        }
        let root = root.canonicalize().unwrap();
        for script in ["flows/a.vjs", "services/b.vjs", "packages/billing/flows/c.vjs"] {
            std::fs::write(root.join(script), "").unwrap();
            assert_eq!(project_of(&root.join(script)), root, "{script}");
        }
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
        // Real temp root — the symlink defense resolves paths, so it needs real
        // files, not a string-only fake root.
        let root = std::env::temp_dir().join(format!("vejas-gp-{}", std::process::id()));
        std::fs::create_dir_all(root.join("flows")).unwrap();
        std::fs::write(root.join("flows").join("x.vjs"), "source \"vx.a\"\n").unwrap();

        // legit file, relative and absolute-under-root (panel round-trip)
        assert!(guard_path(&root, "flows/x.vjs").is_some());
        assert!(guard_path(&root, root.join("flows/x.vjs").to_str().unwrap()).is_some());
        // a not-yet-existing file under an allowed dir (a write) is allowed
        assert!(guard_path(&root, "flows/new.vjs").is_some());
        // string escapes still rejected
        assert!(guard_path(&root, "/etc/passwd").is_none());
        // a folder's name is a whole component, not a prefix
        for beside in ["flows_old/a.vjs", "services.json", "connectors-prod.json", "packagesx/p/package.vjs", "flows"] {
            assert!(guard_path(&root, beside).is_none(), "{beside}");
        }
        std::fs::create_dir_all(root.join("packages/p/services")).unwrap();
        assert!(guard_path(&root, "packages/p/services/s.vjs").is_some());
        assert!(guard_path(&root, "flows/../secrets.json").is_none());

        // F1: a symlink that ESCAPES root is rejected (arbitrary read/write hole)
        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            let outside = std::env::temp_dir().join(format!("vejas-gp-out-{}", std::process::id()));
            std::fs::write(&outside, "SECRET").unwrap();
            symlink(&outside, root.join("flows").join("evil.vjs")).unwrap();
            assert!(
                guard_path(&root, "flows/evil.vjs").is_none(),
                "a symlink escaping root must be refused (F1)"
            );
            // a symlink that stays WITHIN root is harmless and allowed
            symlink(root.join("flows").join("x.vjs"), root.join("flows").join("inner.vjs")).unwrap();
            assert!(guard_path(&root, "flows/inner.vjs").is_some());
            let _ = std::fs::remove_file(&outside);
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn write_gate_classifies_methods() {
        use tiny_http::Method;
        // reads stay open …
        assert!(is_read_method(&Method::Get));
        assert!(is_read_method(&Method::Head));
        // … every other verb is a mutation → gated (Finding A: not just POST).
        assert!(!is_read_method(&Method::Post));
        assert!(!is_read_method(&Method::Put));
        assert!(!is_read_method(&Method::Patch));
        assert!(!is_read_method(&Method::Delete));
    }

    #[test]
    fn flow_emits_detects_bus_writers() {
        // Finding A (volet-2) + A'/A'': a read-method /api flow that writes the bus
        // — directly (emit) or via a service invoke — must be gated; a pure
        // respond-only read is not.
        let root = std::env::temp_dir().join(format!("vejas-emits-{}", std::process::id()));
        std::fs::create_dir_all(root.join("flows")).unwrap();
        let w = |name: &str, body: &str| std::fs::write(root.join("flows").join(name), body).unwrap();
        w("audit.vjs", "api \"GET /thing\"\nemit \"vx.audit.read\", {who: \"x\"}\nrespond 200, {ok: true}\n");
        w("read.vjs", "api \"GET /thing\"\nrespond 200, {ok: true}\n");
        // A': a DYNAMIC subject (lowercase local, invisible to emit_subjects).
        w("dyn.vjs", "api \"GET /thing\"\ns = \"vx.leak\"\nemit s, {x: 1}\nrespond 200, {ok: true}\n");
        // A'': invoke of a service (which may emit) — statement-level, in a respond
        // arg, and nested in a dict value. All are invisible to emit_subjects and
        // (for the nested one) to prog.invokes, yet all can write the bus.
        w("inv_stmt.vjs", "api \"GET /thing\"\ninvoke notify(m: 1)\nrespond 200, {ok: true}\n");
        w("inv_respond.vjs", "api \"GET /thing\"\nrespond 200, invoke notify(m: 1)\n");
        w("inv_nested.vjs", "api \"GET /thing\"\nx = {a: invoke notify(m: 1)}\nrespond 200, {ok: true}\n");

        assert!(flow_writes_bus(&root, "flows/audit.vjs"), "a literal emit writes the bus");
        assert!(!flow_writes_bus(&root, "flows/read.vjs"), "a respond-only flow is a pure read");
        // the dynamic-subject emitter has an EMPTY emit_subjects yet MUST gate (A')
        let dyn_prog = vjs::parse(&std::fs::read_to_string(root.join("flows").join("dyn.vjs")).unwrap()).unwrap();
        assert!(dyn_prog.emit_subjects.is_empty(), "dynamic subject is invisible to emit_subjects");
        assert!(flow_writes_bus(&root, "flows/dyn.vjs"), "a dynamic-subject emit still writes the bus (A')");
        // invoke in any position writes the bus (A''); the nested case is invisible
        // even to prog.invokes (statement-level static list).
        assert!(flow_writes_bus(&root, "flows/inv_stmt.vjs"), "a statement invoke can emit (A'')");
        assert!(flow_writes_bus(&root, "flows/inv_respond.vjs"), "an invoke in a respond arg can emit (A'')");
        let nested = vjs::parse(&std::fs::read_to_string(root.join("flows").join("inv_nested.vjs")).unwrap()).unwrap();
        assert!(nested.invokes.is_empty(), "a nested invoke is invisible to prog.invokes");
        assert!(flow_writes_bus(&root, "flows/inv_nested.vjs"), "an invoke nested in a value can emit (A'')");
        // a path escaping root resolves to no flow → treated as non-emitting (the
        // top-level method gate and guard_path already bar it from running).
        assert!(!flow_writes_bus(&root, "/etc/passwd"));
        let _ = std::fs::remove_dir_all(&root);
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
    fn what_pages_and_agents_are_shown_of_the_bus_is_masked() {
        crate::secrets::hand_out("hooks-s3cr3t-T0K3N-7f1");
        record_trace_full(
            "connector:mask_probe",
            "vx.mask.out",
            false,
            Some("POST https://hooks.example/hooks-s3cr3t-T0K3N-7f1 failed: 500".into()),
            vec![],
            r#"{"id":7,"password":"hunter2","header":{"name":"Authorization","value":"Bearer abc"}}"#.into(),
            Some(json!({"id": 7, "password": "hunter2"})),
            Some(r#"{"echo":"hooks-s3cr3t-T0K3N-7f1"}"#.into()),
            None,
            Some(bus::Delivery { delivered: 2, stream_seq: 41, replay_of: None }),
        );
        let out = events_json(Some("connector:mask_probe"));
        for leak in ["hunter2", "Bearer abc", "hooks-s3cr3t-T0K3N-7f1"] {
            assert!(!out.contains(leak), "{leak} in {out}");
        }
        let v: Value = serde_json::from_str(&out).unwrap();
        let hop = &v["events"][0];
        assert!(hop["preview"].as_str().unwrap().contains(r#""id":7"#), "{hop}");
        assert!(hop["error"].as_str().unwrap().starts_with("POST https://hooks.example/"), "{hop}");
        // what a page finds an event by stays as it was
        assert_eq!(hop["flow"], "connector:mask_probe");
        assert_eq!(hop["subject"], "vx.mask.out");
        assert_eq!((hop["stream_seq"].as_u64(), hop["attempt"].as_u64()), (Some(41), Some(2)));

        // a dead letter: its payload and its error; its sequence and unit stay
        let letters = as_shown(json!({"dead_letters": [{
            "seq": 12, "unit": "flow:orders", "original_seq": 40, "attempts": 5,
            "original_subject": "vx.orders",
            "payload": r#"{"order":1,"api_key":"k-123456","note":"hooks-s3cr3t-T0K3N-7f1"}"#,
            "last_error": "refused: hooks-s3cr3t-T0K3N-7f1",
        }]}));
        let shown = letters.to_string();
        for leak in ["k-123456", "hooks-s3cr3t-T0K3N-7f1"] {
            assert!(!shown.contains(leak), "{leak} in {shown}");
        }
        let letter = &letters["dead_letters"][0];
        assert_eq!((letter["seq"].as_u64(), letter["original_seq"].as_u64()), (Some(12), Some(40)));
        assert_eq!(letter["unit"], "flow:orders");
        assert!(letter["payload"].as_str().unwrap().contains(r#""order":1"#), "{letter}");

        // a shadow replay's results, and a proposal that changes a credential
        let replay = as_shown(json!({"results": [{"seq": 3, "preview": "{\"token\":\"t-998877\"}",
            "before": {"emits": [{"subject": "vx.a", "payload": {"secret": "s-1"}}], "error": null}}]}));
        let shown = replay.to_string();
        assert!(!shown.contains("t-998877") && !shown.contains("s-1"), "{shown}");
        assert_eq!(replay["results"][0]["before"]["emits"][0]["subject"], "vx.a");
        // a proposal: its evidence is masked, the change it proposes is not
        let candidate = "password = event.password\nemit \"vx.a\" { password }\n";
        let proposals = proposals_shown(vec![json!({"id": "p1", "kind": "version",
            "payload": {"file": "flows/a.vjs", "candidate": candidate},
            "evidence": {"results": [{"seq": 3, "after": {"emits": [{"subject": "vx.a", "payload": {"password": "pw-4455"}}]}}]}})]);
        assert!(!proposals[0]["evidence"].to_string().contains("pw-4455"), "{}", proposals[0]);
        assert_eq!(proposals[0]["payload"]["candidate"], candidate);
        assert_eq!(proposals[0]["id"], "p1");

        // a letter past 16 KB is shown as its start, as the record keeps one
        let big = format!(r#"{{"api_key":"k-998877","blob":"{}"}}"#, "x".repeat(40_000));
        let letters = as_shown(json!({"dead_letters": [{"seq": 13, "payload": big}]}));
        let payload = letters["dead_letters"][0]["payload"].as_str().unwrap();
        assert!(payload.len() < 17_000 && payload.ends_with('…'), "{}", payload.len());
        assert!(!payload.contains("k-998877") && payload.contains("<masked>"), "{}", &payload[..60]);
    }

    #[test]
    fn a_shadow_run_never_reads_a_credential_of_a_real_event() {
        let root = std::env::temp_dir().join(format!("vejas-shadow-mask-{}", std::process::id()));
        std::fs::create_dir_all(root.join("flows")).unwrap();
        std::fs::write(
            root.join("flows").join("rename_probe.vjs"),
            "source \"vx.rn.test\"\nTH = 1\nif n > TH:\n  emit \"vx.out\", {pw: password, h: headers}\nend\n",
        )
        .unwrap();
        let event = json!({"n": 5, "password": "pw-rename-7731", "headers": [{"name": "Authorization", "value": "Bearer rn-55"}]});
        record_trace("flow:rename_probe", "vx.rn.test", true, None, vec![], "{}".into(), Some(event), None, None);
        let diff = as_shown(replay_literal(&root, "flows/rename_probe.vjs", "TH", "-", &json!(2), 20).unwrap());
        let shown = diff.to_string();
        // a name no mask reads would carry it out: it never enters the run
        assert!(!shown.contains("pw-rename-7731") && !shown.contains("rn-55"), "{shown}");
        assert_eq!(diff["results"][0]["after"]["emits"][0]["payload"]["pw"], "<masked>", "{shown}");
        // and the result names what the run read hidden
        assert_eq!(diff["results"][0]["hidden"], json!(["headers[0].value", "password"]), "{shown}");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn a_replay_reads_flags_and_numbers_and_names_what_it_did_not_see() {
        let root = std::env::temp_dir().join(format!("vejas-shadow-branch-{}", std::process::id()));
        std::fs::create_dir_all(root.join("flows")).unwrap();
        let flow = |name: &str, cond: &str| {
            let src = format!("source \"vx.{name}.test\"\nTH = 10\nif {cond}:\n  emit \"vx.out\", {{id: id}}\nend\n");
            std::fs::write(root.join("flows").join(format!("{name}.vjs")), src).unwrap();
            let event = json!({"id": 4, "amount": 30, "password_expired": true, "auth_level": 30, "auth_type": "sso"});
            record_trace(&format!("flow:{name}"), &format!("vx.{name}.test"), true, None, vec![], "{}".into(), Some(event), None, None);
            replay_literal(&root, &format!("flows/{name}.vjs"), "TH", "-", &json!(50), 20).unwrap()
        };
        // a flag named like a credential and a plain number: read as live, so
        // event 4, sent with TH = 10, would not be with TH = 50
        let diff = flow("shadow_flags", "amount > TH and password_expired");
        assert_eq!(diff["changed"], 1, "{diff}");
        // the fields it hides are ones this flow does not read: none named
        assert!(diff["results"][0].get("hidden").is_none(), "{diff}");
        // a number named as a credential is not read, and the answer says so
        let diff = flow("shadow_level", "auth_level > TH");
        let _ = std::fs::remove_dir_all(&root);
        assert_eq!((&diff["changed"], &diff["results"][0]["hidden"]), (&json!(0), &json!(["auth_level"])), "{diff}");
    }

    #[test]
    fn a_golden_case_keeps_no_credential_of_its_event() {
        let root = std::env::temp_dir().join(format!("vejas-golden-mask-{}", std::process::id()));
        std::fs::create_dir_all(root.join("flows")).unwrap();
        std::fs::write(
            root.join("flows").join("golden_probe.vjs"),
            "source \"vx.gp.test\"\nemit \"vx.out\", {id: id, pw: password}\n",
        )
        .unwrap();
        let event = json!({"id": 3, "password": "pw-golden-4417"});
        record_trace("flow:golden_probe", "vx.gp.test", true, None, vec![], "{}".into(), Some(event), None, None);
        let events: Value = serde_json::from_str(&events_json(Some("flow:golden_probe"))).unwrap();
        let seq = events["events"][0]["seq"].as_u64().unwrap();
        let out = golden_from_event(&root, "flow:golden_probe", seq).unwrap();
        let case = std::fs::read_to_string(root.join("tests").join("vjs").join(out["case"].as_str().unwrap())).unwrap();
        assert!(!case.contains("pw-golden-4417") && case.contains("<masked>"), "{case}");
        assert_eq!((&out["expect_emits"], &out["hidden"]), (&json!(1), &json!(["password"])), "{out}");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn an_event_too_deep_to_read_leaves_the_others_their_shape() {
        let mut deep = json!("leaf");
        for _ in 0..120 {
            deep = json!({ "d": deep });
        }
        let shown = as_shown(json!({"results": [{"after": {"error": null, "emits": [
            {"subject": "vx.a", "payload": deep}, {"subject": "vx.b", "payload": {"id": 1}}]}}]}));
        let emits = &shown["results"][0]["after"]["emits"];
        assert!(emits.is_array(), "{emits}");
        assert_eq!((emits[0]["subject"].as_str(), emits[0]["payload"].as_str()), (Some("vx.a"), Some("<secret>")));
        assert_eq!(emits[1]["payload"], json!({"id": 1}));
    }

    #[test]
    fn events_json_strips_the_full_event() {
        record_trace("flow:strip_probe", "vx.sp.test", true, None, vec![], "{\"a\":1}".into(), Some(json!({"a": 1})), None, None);
        let out: Value = serde_json::from_str(&events_json(Some("flow:strip_probe"))).unwrap();
        let entry = &out["events"][0];
        assert_eq!(entry["flow"], "flow:strip_probe");
        assert!(entry.get("event").is_none(), "full event must not leave the runtime via /events");
        assert_eq!(entry["preview"], "{\"a\":1}");
    }

    #[test]
    fn a_units_events_come_with_the_other_hops_of_their_path() {
        let hook = trace::Hop::begin(None);
        let flow = hook.child();
        record_trace("connector:paths_hook", "vx.paths.in", true, None, vec![], "{}".into(), None, Some(&hook), None);
        record_trace("flow:paths_probe", "vx.paths.in", true, None, vec![], "{}".into(), None, Some(&flow), None);
        let elsewhere = trace::Hop::begin(None);
        record_trace("connector:paths_hook", "vx.paths.in", true, None, vec![], "{}".into(), None, Some(&elsewhere), None);
        record_trace("connector:paths_hook", "vx.paths.in", true, None, vec![], "{}".into(), None, None, None);
        let out: Value = serde_json::from_str(&events_with_paths_json("flow:paths_probe")).unwrap();
        let units: Vec<&str> = out["events"].as_array().unwrap().iter().filter_map(|e| e["flow"].as_str()).collect();
        assert_eq!(units, ["flow:paths_probe", "connector:paths_hook"],
                   "the flow's event and the webhook's hop of it, not the webhook's other events");
        let alone: Value = serde_json::from_str(&events_json(Some("flow:paths_probe"))).unwrap();
        assert_eq!(alone["events"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn a_preview_is_the_same_cut_however_large_the_event() {
        let whole = |v: &Value| {
            let s = v.to_string();
            if s.chars().count() > 160 {
                format!("{}…", s.chars().take(159).collect::<String>())
            } else {
                s
            }
        };
        let values = [
            json!({}),
            json!({"a": "x".repeat(152)}),
            json!({"a": "x".repeat(153)}),
            json!({"t": "é€😀a".repeat(5000)}),
            json!("ü".repeat(161)),
            json!({"n": (0..10_000).collect::<Vec<u32>>()}),
        ];
        for v in &values {
            assert_eq!(preview_of(v), whole(v));
        }
        assert_eq!(preview_of(&values[1]).chars().count(), 160, "exactly 160: whole");
    }

    #[test]
    fn a_busy_path_never_pushes_the_units_own_events_out() {
        let hook = trace::Hop::begin(None);
        let flow = hook.child();
        record_trace("flow:cap_probe", "vx.cap.in", true, None, vec![], "{}".into(), None, Some(&flow), None);
        // newer hops of the same trace, more than the cap, over five rings of 50
        for i in 0..PATH_HOPS_MAX + 10 {
            let unit = format!("connector:cap_hook_{}", i % 5);
            record_trace(&unit, "vx.cap.in", true, None, vec![], "{}".into(), None, Some(&hook), None);
        }
        let out: Value = serde_json::from_str(&events_with_paths_json("flow:cap_probe")).unwrap();
        let events = out["events"].as_array().unwrap();
        assert_eq!(events.len(), PATH_HOPS_MAX + 1);
        assert_eq!(events.last().unwrap()["flow"], "flow:cap_probe", "the oldest, and still there");
    }

    #[test]
    fn a_retry_and_another_message_of_one_trace_read_apart() {
        let hop = trace::Hop::begin(None);
        let delivery = |delivered, stream_seq| Some(bus::Delivery { delivered, stream_seq, replay_of: None });
        record_trace("flow:retry_probe", "vx.rt.in", false, Some("refused".into()), vec![], "{}".into(), None, Some(&hop), delivery(1, 41));
        record_trace("flow:retry_probe", "vx.rt.in", true, None, vec![], "{}".into(), None, Some(&hop), delivery(2, 41));
        record_trace("flow:retry_probe", "vx.rt.in", true, None, vec![], "{}".into(), None, Some(&hop), delivery(1, 42));
        let out: Value = serde_json::from_str(&events_json(Some("flow:retry_probe"))).unwrap();
        let read: Vec<(u64, i64)> = out["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| (e["stream_seq"].as_u64().unwrap(), e["attempt"].as_i64().unwrap()))
            .collect();
        assert_eq!(read, [(42, 1), (41, 2), (41, 1)], "the retry shares its message; the other message has its own");
        // the replay of a dead letter is a new message that names the one that died
        let replay = Some(bus::Delivery { delivered: 1, stream_seq: 97, replay_of: Some(41) });
        record_trace("flow:retry_probe", "vx.rt.in", true, None, vec![], "{}".into(), None, Some(&hop), replay);
        let out: Value = serde_json::from_str(&events_json(Some("flow:retry_probe"))).unwrap();
        assert_eq!((out["events"][0]["stream_seq"].as_u64(), out["events"][0]["replay_of"].as_u64()), (Some(97), Some(41)));
        assert_eq!(out["events"][1]["replay_of"], Value::Null);
        let source = "connector:retry_probe_source";
        record_trace(source, "vx.rt.in", true, None, vec![], "{}".into(), None, Some(&hop), None);
        let out: Value = serde_json::from_str(&events_json(Some(source))).unwrap();
        assert_eq!(out["events"][0]["stream_seq"], Value::Null, "a hop that consumed nothing names no message");
    }

    #[test]
    fn a_trace_shows_the_code_its_steps_ran_while_the_file_is_unchanged() {
        let root = std::env::temp_dir().join(format!("vejas-join-{}", std::process::id()));
        std::fs::create_dir_all(root.join("flows")).unwrap();
        std::fs::create_dir_all(root.join("packages/crm/services")).unwrap();
        let flow = "source \"vx.in\"\n  total = amount * 2\nemit \"vx.out\", {t: total}\n";
        let service = "found = true\n";
        std::fs::write(root.join("flows/join_probe.vjs"), flow).unwrap();
        std::fs::write(root.join("packages/crm/services/lookup.vjs"), service).unwrap();
        let hop = |flow_version: &str, service_version: &str| {
            json!({
                "unit": "flow:join_probe",
                "attributes": {"code.file.path": "flows/join_probe.vjs", "vejas.flow.version": flow_version},
                "steps": [
                    {"line": 2},
                    {"service": "crm:lookup", "version": service_version},
                    {"service": "crm:lookup", "line": 1},
                    {"line": 3, "emit": "vx.out"},
                    {"line": 99},
                    {"skipped": 4},
                ],
                "services": {"crm:lookup": service_version},
            })
        };
        let mut sources = Sources::new(None);
        let mut same = hop(&versions::hash_content(flow), &versions::hash_content(service));
        join_code(&root, &mut same, &mut sources);
        let code: Vec<Value> = same["steps"].as_array().unwrap().iter().map(|s| s["code"].clone()).collect();
        assert_eq!(
            code,
            [json!("total = amount * 2"), Value::Null, json!("found = true"), json!("emit \"vx.out\", {t: total}"), Value::Null, Value::Null]
        );
        // a file changed since: its steps keep their numbers, without code
        let mut changed = hop("0000000000000000", &versions::hash_content(service));
        join_code(&root, &mut changed, &mut sources);
        let code: Vec<Value> = changed["steps"].as_array().unwrap().iter().map(|s| s["code"].clone()).collect();
        assert_eq!(code, [Value::Null, Value::Null, json!("found = true"), Value::Null, Value::Null, Value::Null]);
        // a file the record names outside the project is not read
        let mut outside = json!({
            "attributes": {"code.file.path": "../../etc/passwd", "vejas.flow.version": "x"},
            "steps": [{"line": 1}],
        });
        join_code(&root, &mut outside, &mut sources);
        assert!(outside["steps"][0].get("code").is_none());

        // a service's code comes from the record's `services`, even when the
        // step where it started was left out
        let mut skipped = hop(&versions::hash_content(flow), &versions::hash_content(service));
        skipped["steps"].as_array_mut().unwrap().remove(1);
        join_code(&root, &mut skipped, &mut sources);
        assert_eq!(skipped["steps"][1]["code"], "found = true");

        // a flow promoted over its file in a cluster: the overlay that ran
        let promoted = "source \"vx.in\"\nshipped = true\n";
        let mut overlaid = hop(&versions::hash_content(promoted), &versions::hash_content(service));
        overlaid["attributes"]["vejas.flow.baseline"] = json!(versions::hash_content(flow));
        sources.overlays.insert("join_probe".into(), Some(Source::of(promoted)));
        join_code(&root, &mut overlaid, &mut sources);
        assert_eq!(overlaid["steps"][0]["code"], "shipped = true");
        // without the baseline, a hop ran no overlay: no code from one
        let mut plain = hop(&versions::hash_content(promoted), &versions::hash_content(service));
        join_code(&root, &mut plain, &mut sources);
        assert!(plain["steps"][0].get("code").is_none());

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn a_forged_record_cannot_make_a_trace_view_slow_or_huge() {
        let root = std::env::temp_dir().join(format!("vejas-forged-{}", std::process::id()));
        std::fs::create_dir_all(root.join("flows")).unwrap();
        let flow = format!("source \"vx.in\"\nx = \"{}\"\n", "y".repeat(150));
        std::fs::write(root.join("flows/f.vjs"), &flow).unwrap();
        let version = versions::hash_content(&flow);
        let mut sources = Sources::new(None);
        // code a record carries is never shown: the runtime writes none
        let mut forged = json!({
            "attributes": {"code.file.path": "flows/f.vjs", "vejas.flow.version": "0000000000000000"},
            "steps": [{"line": 2, "code": "rm -rf /"}],
        });
        join_code(&root, &mut forged, &mut sources);
        assert!(forged["steps"][0].get("code").is_none());
        // a thousand versions of one file: read once
        for i in 0..1000 {
            let mut hop = json!({
                "attributes": {"code.file.path": "flows/f.vjs", "vejas.flow.version": format!("{i:016x}")},
                "steps": [{"line": 2}],
                "services": {"default:s": format!("{i:016x}")},
            });
            join_code(&root, &mut hop, &mut sources);
        }
        assert_eq!(sources.files.len(), 1);
        // a thousand files: JOIN_FILES_MAX read at most
        for i in 0..1000 {
            let mut hop = json!({
                "attributes": {"code.file.path": format!("flows/f{i}.vjs"), "vejas.flow.version": "x"},
                "steps": [{"line": 1}],
            });
            join_code(&root, &mut hop, &mut sources);
        }
        assert_eq!(sources.files.len(), JOIN_FILES_MAX);
        // steps past what a run keeps get no code, nor bytes past JOIN_CODE_MAX
        let mut sources = Sources::new(None);
        let mut given = 0;
        for _ in 0..100 {
            let mut hop = json!({
                "attributes": {"code.file.path": "flows/f.vjs", "vejas.flow.version": version},
                "steps": vec![json!({"line": 2}); 5000],
            });
            join_code(&root, &mut hop, &mut sources);
            let with_code = hop["steps"].as_array().unwrap().iter().filter(|s| s.get("code").is_some()).count();
            assert!(with_code <= JOIN_STEPS_MAX, "{with_code}");
            given += with_code;
        }
        assert!(sources.given <= JOIN_CODE_MAX && given < 100 * JOIN_STEPS_MAX, "{given} {}", sources.given);
        let _ = std::fs::remove_dir_all(&root);
    }
}
