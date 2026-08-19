// Vejas runtime — all Rust, no Python.
//
// Executes VejasScript flows natively, runs the bundled connectors as threads,
// and serves the panel. Layout (webMethods-style packages, hot-addable):
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
//   GET  /graph       pipeline     GET /surface        business surface
//   GET  /events?flow=             last processed events (in-memory ring)
//   GET  /preview?file=            fixture -> sample run
//   GET  /file?path=               read a script        POST /file/set
//   GET  /fixture?file=            read a fixture       POST /fixture/set
//   POST /surface/set              rewrite one literal in place
//   POST /flows/new                agent CLI writes a VejasScript flow
//   POST /reload                   rescan; restart changed files (mtime)

mod connectors;
mod secrets;
mod vjs;

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
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

fn record_trace(
    flow: &str,
    subject: &str,
    ok: bool,
    error: Option<String>,
    emits: Vec<String>,
    preview: String,
) {
    let seq = TRACE_SEQ.fetch_add(1, Ordering::SeqCst);
    let mut map = TRACES.get_or_init(|| Mutex::new(HashMap::new())).lock().unwrap();
    let ring = map.entry(flow.to_string()).or_default();
    ring.push_back(json!({
        "seq": seq, "ts": now_secs(), "flow": flow, "subject": subject,
        "ok": ok, "error": error, "emits": emits, "preview": preview,
    }));
    while ring.len() > 50 {
        ring.pop_front();
    }
}

/// Newest-first flat list of trace entries, optionally for one flow, capped.
fn events_json(flow: Option<&str>) -> String {
    let map = TRACES.get_or_init(|| Mutex::new(HashMap::new())).lock().unwrap();
    let mut all: Vec<Value> = map
        .iter()
        .filter(|(name, _)| flow.map(|f| f == name.as_str()).unwrap_or(true))
        .flat_map(|(_, ring)| ring.iter().cloned())
        .collect();
    all.sort_by_key(|e| std::cmp::Reverse(e["seq"].as_u64().unwrap_or(0)));
    all.truncate(100);
    json!({ "events": all }).to_string()
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
    fs::metadata(path)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
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
                return Err("no `source` declaration".into());
            };
            let nc = nats::connect(&url).map_err(|e| e.to_string())?;
            let js = nats::jetstream::new(nc);
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
                for msg in connectors::fetch_round(&sub)? {
                    if !RUNNING.load(Ordering::SeqCst) || handle.stop.load(Ordering::SeqCst) {
                        // stopped mid-batch: leave the message un-acked, it
                        // redelivers to whoever holds the durable next
                        return Ok(());
                    }
                    let event: Value = match serde_json::from_slice(&msg.data) {
                        Ok(v) => v,
                        Err(e) => {
                            eprintln!("[vejas] {}: bad json: {e}", handle.spec.name);
                            record_trace(
                                &handle.spec.name,
                                &msg.subject,
                                false,
                                Some(format!("bad json: {e}")),
                                vec![],
                                String::from_utf8_lossy(&msg.data).chars().take(160).collect(),
                            );
                            let _ = msg.ack();
                            continue;
                        }
                    };
                    match vjs::run(&prog, &event, &mut engine) {
                        Ok(ctx) => {
                            let mut ok = true;
                            for (subj, payload) in &ctx.emits {
                                let bytes = serde_json::to_vec(payload).unwrap_or_default();
                                if let Err(e) = js.publish(subj, bytes) {
                                    eprintln!(
                                        "[vejas] {}: publish {subj}: {e}",
                                        handle.spec.name
                                    );
                                    ok = false;
                                }
                            }
                            record_trace(
                                &handle.spec.name,
                                &msg.subject,
                                ok,
                                (!ok).then(|| "publish failed (will redeliver)".to_string()),
                                ctx.emits.iter().map(|(s, _)| s.clone()).collect(),
                                preview_of(&event),
                            );
                            if ok {
                                let _ = msg.ack();
                            }
                        }
                        Err(e) => {
                            eprintln!("[vejas] {}: {e}", handle.spec.name);
                            record_trace(
                                &handle.spec.name,
                                &msg.subject,
                                false,
                                Some(e.clone()),
                                vec![],
                                preview_of(&event),
                            );
                            set_state(&handle, |st| st.last_error = Some(e));
                            // no ack -> redelivery after ack_wait
                        }
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
    let rel = rel.trim_start_matches("./");
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
  invoke format_alert(sev: code)     <- compose a service from services/<name>.vjs; its outputs MERGE into this pipeline
  d = invoke format_alert(sev: code) <- or capture its whole pipeline as a document
  invoke pkg:svc(k: v)               <- cross-package composition (the target package must list svc in its EXPORTS)
  key = secret("slack/webhook")      <- credentials resolve from the Vault at run time; NEVER a literal
  if code in ALERT_LEVELS:
      emit "vx.slack.out", {text: f"[{code}] {subject}"}
  end                                <- every if/for closes with `end`

Rules:
- Known sinks: vx.slack.out (payload {text: "..."}). All subjects start with "vx.".
- Put every business-meaningful value (thresholds, tables, queue names) in UPPERCASE literals.
- A flow file's first line is `# flow: <snake_case_name>`; it lives under flows/ (or packages/<pkg>/flows/).
- Its sample input lives at flows/fixtures/<flow>.json (or packages/<pkg>/fixtures/) — one JSON event.
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
        json!({"name": "vejas_preview", "description": "Run a flow on its fixture and return the emitted messages plus the final pipeline.", "inputSchema": obj(json!({"file": {"type": "string"}}), vec!["file"])}),
        json!({"name": "vejas_run_flow", "description": "Run any flow on a supplied input event and return its emits (does not touch the bus).", "inputSchema": obj(json!({"file": {"type": "string"}, "input": {"type": "object"}}), vec!["file", "input"])}),
        json!({"name": "vejas_events", "description": "The most recent events processed by the flows — subject, ok/error, emitted subjects, payload preview — newest first. Optional filter: flow (e.g. \"flow:stripe_alerts\").", "inputSchema": obj(json!({"flow": {"type": "string"}}), vec![])}),
        json!({"name": "vejas_reload", "description": "Rescan flows and packages; start new, stop removed, restart changed.", "inputSchema": obj(json!({}), vec![])}),
        json!({"name": "vejas_drivers", "description": "List the available connector drivers (name, kind, description) for writing connector manifests.", "inputSchema": obj(json!({}), vec![])}),
        json!({"name": "vejas_secrets", "description": "List the secret references (var, path) declared by flows and connectors — references only, never values.", "inputSchema": obj(json!({}), vec![])}),
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
        "vejas_secrets" => {
            let mut refs = Vec::new();
            for (path, _pkg) in vjs_files(root) {
                if let Ok(prog) = fs::read_to_string(&path).ok().map(|s| vjs::parse(&s)).unwrap_or(Err(String::new())) {
                    for (var, r) in prog.secret_refs {
                        refs.push(json!({"file": path.display().to_string(), "var": var, "ref": r}));
                    }
                }
            }
            for c in connector_graph(root) {
                if let Some(sr) = c.get("secret_refs").and_then(|v| v.as_array()) {
                    for s in sr {
                        refs.push(json!({"connector": c["name"], "var": s["var"], "ref": s["ref"]}));
                    }
                }
            }
            text(Value::Array(refs).to_string())
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
    match (method, path_only.as_str()) {
        (tiny_http::Method::Get, "/") | (tiny_http::Method::Get, "/panel") => respond(
            request,
            200,
            include_str!("panel.html").to_string(),
            "text/html; charset=utf-8",
        ),
        (tiny_http::Method::Get, "/healthz") => respond(request, 200, "ok".into(), "text/plain"),
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

    ctrlc::set_handler(|| {
        eprintln!("[vejas] shutdown requested");
        RUNNING.store(false, Ordering::SeqCst);
    })
    .expect("signal handler");

    // flows AND declared connector instances start here (reload supervises both)
    let (total, started, _) = reload(&registry, &root);
    eprintln!("[vejas] supervising {total} units ({started} started)");

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
