// Vejas runtime.
//
// Supervises flows and connectors, executes VejasScript natively, and serves
// the panel. Layout (webMethods-style packages, hot-addable):
//
//   flows/*.vjs|*.py          the "default" package
//   services/*.vjs            composable services (invoke name(...))
//   connectors/*.py           bus adapters (any language; Python bundled)
//   packages/<pkg>/flows|services|connectors|fixtures
//   packages/<pkg>/package.vjs   literal manifest: ENABLED = true/false
//
// VejasScript flows run IN-PROCESS (interpreter thread + NATS pull consumer);
// Python flows/connectors run as supervised subprocesses via the SDK.
//
// HTTP surface (one request = one thread; /flows/new can take minutes):
//   GET  /            panel        GET /healthz        GET /topology
//   GET  /graph       pipeline     GET /surface        business surface
//   GET  /preview?file=            fixture -> sample run (+ per-rule for .py)
//   GET  /file?path=               read a script        POST /file/set
//   GET  /fixture?file=            read a fixture       POST /fixture/set
//   POST /surface/set              rewrite one literal in place
//   POST /flows/new                agent CLI writes a VejasScript flow
//   POST /reload                   rescan; restart changed files (mtime)

mod vjs;

use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{env, fs, thread};

use serde_json::{json, Value};

#[derive(Clone, PartialEq)]
enum Kind {
    Flow,
    Connector,
}

#[derive(Clone, PartialEq)]
enum Lang {
    Py,
    Vjs,
}

#[derive(Clone)]
struct Spec {
    name: String,
    path: PathBuf,
    kind: Kind,
    lang: Lang,
    pkg: String,
    mtime: u64,
}

struct ProcState {
    status: String,
    pid: Option<u32>,
    restarts: u64,
    started_at: Option<u64>,
    last_exit: Option<i32>,
    last_error: Option<String>,
}

struct Handle {
    spec: Spec,
    state: Mutex<ProcState>,
    stop: AtomicBool,
}

type Registry = Arc<Mutex<HashMap<String, Arc<Handle>>>>;

static RUNNING: AtomicBool = AtomicBool::new(true);

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

fn scan_unit(dir: &Path, kind: Kind, pkg: &str, out: &mut Vec<Spec>) {
    let Ok(entries) = fs::read_dir(dir) else { return };
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(fname) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if fname.starts_with('_') {
            continue;
        }
        let lang = if fname.ends_with(".py") {
            Lang::Py
        } else if fname.ends_with(".vjs") {
            Lang::Vjs
        } else {
            continue;
        };
        if kind == Kind::Connector && lang == Lang::Vjs {
            continue; // connectors are external processes; vjs is for flows/services
        }
        let stem = fname.trim_end_matches(".py").trim_end_matches(".vjs").to_string();
        let kind_s = match kind {
            Kind::Flow => "flow",
            Kind::Connector => "connector",
        };
        let name = if pkg == "default" {
            format!("{kind_s}:{stem}")
        } else {
            format!("{kind_s}:{pkg}:{stem}")
        };
        out.push(Spec {
            name,
            mtime: mtime_of(&path),
            path,
            kind: kind.clone(),
            lang,
            pkg: pkg.to_string(),
        });
    }
}

fn scan_all(root: &Path) -> Vec<Spec> {
    let mut out = Vec::new();
    scan_unit(&root.join("flows"), Kind::Flow, "default", &mut out);
    scan_unit(&root.join("connectors"), Kind::Connector, "default", &mut out);
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
            scan_unit(&dir.join("flows"), Kind::Flow, &pkg, &mut out);
            scan_unit(&dir.join("connectors"), Kind::Connector, &pkg, &mut out);
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

fn build_py_command(spec: &Spec, root: &Path) -> Command {
    let mut cmd = Command::new("python3");
    match spec.kind {
        Kind::Flow => {
            cmd.arg("-m").arg("vejas").arg("run").arg(&spec.path);
        }
        Kind::Connector => {
            cmd.arg(&spec.path);
        }
    }
    let sdk = root.join("sdk").join("python");
    let py_path = match env::var("PYTHONPATH") {
        Ok(existing) if !existing.is_empty() => format!("{}:{}", sdk.display(), existing),
        _ => sdk.display().to_string(),
    };
    cmd.env("PYTHONPATH", py_path);
    cmd.env("VEJAS_PROC", &spec.name);
    cmd
}

fn set_state(handle: &Handle, f: impl FnOnce(&mut ProcState)) {
    let mut st = handle.state.lock().unwrap();
    f(&mut st);
}

fn supervise_py(handle: Arc<Handle>, root: PathBuf) {
    let mut delay = Duration::from_secs(1);
    loop {
        if !RUNNING.load(Ordering::SeqCst) || handle.stop.load(Ordering::SeqCst) {
            break;
        }
        let started = Instant::now();
        let mut child = match build_py_command(&handle.spec, &root).spawn() {
            Ok(c) => c,
            Err(err) => {
                set_state(&handle, |st| {
                    st.status = "spawn-error".into();
                    st.last_error = Some(err.to_string());
                });
                thread::sleep(delay);
                delay = (delay * 2).min(Duration::from_secs(30));
                continue;
            }
        };
        set_state(&handle, |st| {
            st.status = "running".into();
            st.pid = Some(child.id());
            st.started_at = Some(now_secs());
        });
        eprintln!("[vejas] {} started (pid {})", handle.spec.name, child.id());
        let exit = loop {
            if !RUNNING.load(Ordering::SeqCst) || handle.stop.load(Ordering::SeqCst) {
                let _ = child.kill();
                let _ = child.wait();
                break None;
            }
            match child.try_wait() {
                Ok(Some(status)) => break Some(status),
                Ok(None) => thread::sleep(Duration::from_millis(200)),
                Err(_) => break None,
            }
        };
        set_state(&handle, |st| st.pid = None);
        match exit {
            Some(status) => {
                set_state(&handle, |st| {
                    st.last_exit = status.code();
                    st.restarts += 1;
                    st.status = "restarting".into();
                });
                eprintln!("[vejas] {} exited, restarting", handle.spec.name);
                if started.elapsed() > Duration::from_secs(30) {
                    delay = Duration::from_secs(1);
                }
                thread::sleep(delay);
                delay = (delay * 2).min(Duration::from_secs(30));
            }
            None => break,
        }
    }
    set_state(&handle, |st| {
        st.status = "stopped".into();
        st.pid = None;
    });
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
            let mut engine = vjs::Engine::new(pkg_root(&root, &handle.spec.pkg));
            loop {
                if !RUNNING.load(Ordering::SeqCst) || handle.stop.load(Ordering::SeqCst) {
                    return Ok(());
                }
                engine.invalidate(); // pick up live service edits
                let batch = sub.fetch(10).map_err(|e| e.to_string())?;
                for msg in batch {
                    let event: Value = match serde_json::from_slice(&msg.data) {
                        Ok(v) => v,
                        Err(e) => {
                            eprintln!("[vejas] {}: bad json: {e}", handle.spec.name);
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
                            if ok {
                                let _ = msg.ack();
                            }
                        }
                        Err(e) => {
                            eprintln!("[vejas] {}: {e}", handle.spec.name);
                            set_state(&handle, |st| st.last_error = Some(e));
                            // no ack -> redelivery after ack_wait
                        }
                    }
                }
                thread::sleep(Duration::from_millis(150));
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

fn pkg_root(root: &Path, pkg: &str) -> PathBuf {
    if pkg == "default" {
        root.to_path_buf()
    } else {
        root.join("packages").join(pkg)
    }
}

fn start_proc(registry: &Registry, spec: Spec, root: &Path) {
    let handle = Arc::new(Handle {
        spec: spec.clone(),
        state: Mutex::new(ProcState {
            status: "starting".into(),
            pid: None,
            restarts: 0,
            started_at: None,
            last_exit: None,
            last_error: None,
        }),
        stop: AtomicBool::new(false),
    });
    registry.lock().unwrap().insert(spec.name.clone(), handle.clone());
    let root = root.to_path_buf();
    match spec.lang {
        Lang::Py => {
            thread::spawn(move || supervise_py(handle, root));
        }
        Lang::Vjs => {
            thread::spawn(move || supervise_vjs(handle, root));
        }
    }
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
            "lang": if handle.spec.lang == Lang::Vjs { "vjs" } else { "py" },
            "status": st.status,
            "pid": st.pid,
            "restarts": st.restarts,
            "started_at": st.started_at,
            "last_exit": st.last_exit,
            "last_error": st.last_error,
        });
        match handle.spec.kind {
            Kind::Flow => flows.push(entry),
            Kind::Connector => connectors.push(entry),
        }
    }
    json!({ "flows": flows, "connectors": connectors })
}

fn vejas_py(root: &Path, args: &[&str]) -> Result<String, String> {
    let mut cmd = Command::new("python3");
    cmd.arg("-m").arg("vejas");
    for a in args {
        cmd.arg(a);
    }
    cmd.env("PYTHONPATH", root.join("sdk").join("python"));
    match cmd.output() {
        Ok(o) if o.status.success() => Ok(String::from_utf8_lossy(&o.stdout).into_owned()),
        Ok(o) => Err(format!(
            "{}{}",
            String::from_utf8_lossy(&o.stdout),
            String::from_utf8_lossy(&o.stderr)
        )),
        Err(e) => Err(e.to_string()),
    }
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
    if let Ok(py) = vejas_py(root, &["surface", &root.join("flows").display().to_string()]) {
        if let Ok(Value::Array(a)) = serde_json::from_str(&py) {
            entries.extend(a);
        }
    }
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
    let mut connectors: Vec<Value> = Vec::new();
    if let Ok(py) = vejas_py(root, &["graph", &root.display().to_string()]) {
        if let Ok(v) = serde_json::from_str::<Value>(&py) {
            if let Some(a) = v["flows"].as_array() {
                flows.extend(a.clone());
            }
            if let Some(a) = v["connectors"].as_array() {
                connectors.extend(a.clone());
            }
        }
    }
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
    if file.ends_with(".py") {
        return vejas_py(root, &["preview", file]);
    }
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
    let pkg_dir = path
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| root.to_path_buf());
    let mut engine = vjs::Engine::new(pkg_dir);
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
    let ok_ext = rel.ends_with(".py") || rel.ends_with(".vjs") || rel.ends_with(".json");
    if ok_dir && ok_ext {
        Some(p)
    } else {
        None
    }
}

// ───────────────────────── agent generation ─────────────────────────

const CONTRACT_VJS: &str = r#"You write ONE VejasScript file for the Vejas integration platform. Reply with ONLY the file content (no markdown fences, no commentary).

VejasScript in 20 lines:
  # comment
  source "vx.domain.name"            <- the flow's input subject, REQUIRED, line 1
  SEVERITY_CODES = {"critique": "P1", "haute": "P2"}   <- UPPERCASE literal dicts are transcoding tables the business expert edits
  ALERT_LEVELS = ["P1", "P2"]        <- UPPERCASE literal lists/scalars are editable constants
  x = priority                       <- the incoming event's top-level fields are variables; `event` is the whole document
  code = SEVERITY_CODES[priority] ?? "P3"
  email = lower(requester?.email)    <- builtins: upper lower trim len str num split join replace round abs; ?. is null-safe
  ids = orders[].id                  <- array projection
  big = orders[total > 100]          <- array filtering
  invoke format_alert(sev: code)     <- compose a service from services/<name>.vjs; its outputs MERGE into this pipeline
  d = invoke format_alert(sev: code) <- or capture its whole pipeline as a document
  if code in ALERT_LEVELS:
      emit "vx.slack.out", {text: f"[{code}] {subject}"}
  end                                <- every if/for closes with `end`

Rules:
- Known sinks: vx.slack.out (payload {text: "..."}). All subjects start with "vx.".
- Put every business-meaningful value (thresholds, tables, queue names) in UPPERCASE literals.
- First line must be: # flow: <snake_case_name>
- Existing flows (do not reuse names): {existing}

Task: {task}"#;

fn generate_flow(root: &Path, prompt: &str) -> Result<String, String> {
    let existing: Vec<String> = scan_all(root)
        .iter()
        .filter(|s| s.kind == Kind::Flow)
        .map(|s| s.name.clone())
        .collect();
    let full = CONTRACT_VJS
        .replace("{existing}", &existing.join(", "))
        .replace("{task}", prompt);
    let agent = env::var("VEJAS_AGENT_CMD").unwrap_or_else(|_| "claude".into());
    let out = Command::new(agent)
        .arg("-p")
        .arg(full)
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
    let prog = vjs::parse(&code)?;
    if prog.source.is_none() {
        return Err("generated flow has no `source` declaration".into());
    }
    let name = code
        .lines()
        .next()
        .and_then(|l| l.strip_prefix("# flow:"))
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'))
        .unwrap_or_else(|| format!("flow_{}", now_secs() % 100000));
    let mut target = root.join("flows").join(format!("{name}.vjs"));
    let mut n = 2;
    while target.exists() {
        target = root.join("flows").join(format!("{name}_{n}.vjs"));
        n += 1;
    }
    fs::write(&target, &code).map_err(|e| e.to_string())?;
    Ok(json!({"ok": true, "file": target.display().to_string(), "flow": name, "lang": "vjs"})
        .to_string())
}

// ───────────────────────── http ─────────────────────────

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
            // validate before writing
            if pstr.ends_with(".vjs") {
                if let Err(e) = vjs::parse(content) {
                    return respond(request, 422, e, "text/plain");
                }
            } else if pstr.ends_with(".py") {
                let check = Command::new("python3")
                    .arg("-c")
                    .arg("import ast,sys; ast.parse(sys.stdin.read())")
                    .stdin(std::process::Stdio::piped())
                    .stderr(std::process::Stdio::piped())
                    .spawn()
                    .and_then(|mut ch| {
                        use std::io::Write;
                        ch.stdin.take().unwrap().write_all(content.as_bytes())?;
                        ch.wait_with_output()
                    });
                match check {
                    Ok(o) if o.status.success() => {}
                    Ok(o) => {
                        return respond(
                            request,
                            422,
                            String::from_utf8_lossy(&o.stderr).into_owned(),
                            "text/plain",
                        )
                    }
                    Err(e) => return respond(request, 500, e.to_string(), "text/plain"),
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
            if file.ends_with(".vjs") {
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
                            json!({"ok": true, "file": file, "name": name, "key": key})
                                .to_string(),
                            "application/json",
                        )
                    }
                    Err(e) => respond(request, 422, e, "text/plain"),
                }
            } else {
                match vejas_py(&root, &["set", &file, &name, &key, &value.to_string()]) {
                    Ok(json) => {
                        let _ = reload(&registry, &root);
                        respond(request, 200, json, "application/json")
                    }
                    Err(err) => respond(request, 422, err, "text/plain"),
                }
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
        (tiny_http::Method::Post, "/reload") => {
            let (total, started, stopped) = reload(&registry, &root);
            respond(
                request,
                200,
                json!({"total": total, "started": started, "stopped": stopped}).to_string(),
                "application/json",
            )
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
        let pkg_dir = PathBuf::from(&args[2])
            .parent()
            .and_then(|p| p.parent())
            .map(|p| p.to_path_buf())
            .unwrap_or(root);
        let mut engine = vjs::Engine::new(pkg_dir);
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

    let root = PathBuf::from(env::var("VEJAS_ROOT").unwrap_or_else(|_| ".".into()));
    let addr = env::var("VEJAS_HTTP_ADDR").unwrap_or_else(|_| "0.0.0.0:8686".into());
    let registry: Registry = Arc::new(Mutex::new(HashMap::new()));

    ctrlc::set_handler(|| {
        eprintln!("[vejas] shutdown requested");
        RUNNING.store(false, Ordering::SeqCst);
    })
    .expect("signal handler");

    let (total, started, _) = reload(&registry, &root);
    eprintln!("[vejas] supervising {total} processes ({started} started)");

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
