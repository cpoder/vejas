// Vejas runtime.
//
// Supervises two kinds of processes and exposes a monitoring surface:
//   - flows:      plain Python files in $VEJAS_ROOT/flows, run as `python3 -m vejas run <file>`
//   - connectors: freestanding programs in $VEJAS_ROOT/connectors, run as `python3 <file>`
//     (a connector is just a NATS service; Python is the bundled default, not a requirement)
//
// The runtime itself never touches the bus. Transport semantics (JetStream
// consumers, ack/nak) live in the SDK, so this binary stays a boring,
// dependency-light supervisor: spawn, watch, restart with backoff, report.
//
// HTTP surface (one request = one thread; /flows/new can take minutes):
//   GET  /            -> the panel (also /panel)
//   GET  /healthz     -> "ok"
//   GET  /topology    -> JSON: every supervised process, status, pid, restarts
//   GET  /graph       -> JSON: static pipeline graph (AST-derived)
//   GET  /surface     -> JSON: business surface (mappings, tables, constants)
//   GET  /preview?file=<flow.py> -> mapping preview + sample run on the fixture
//   POST /surface/set -> rewrite one literal in place, JSON {file,name,key,value}
//   POST /flows/new   -> ask the agent CLI for a new flow, JSON {prompt}
//   POST /reload      -> rescan; start new, stop removed, restart changed files

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::io::Read;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{env, fs, thread};

use serde_json::json;

#[derive(Clone, PartialEq, Eq, Hash)]
enum Kind {
    Flow,
    Connector,
}

impl Kind {
    fn as_str(&self) -> &'static str {
        match self {
            Kind::Flow => "flow",
            Kind::Connector => "connector",
        }
    }
}

#[derive(Clone)]
struct Spec {
    name: String,
    path: PathBuf,
    kind: Kind,
    mtime: u64,
}

struct ProcState {
    status: String,
    pid: Option<u32>,
    restarts: u64,
    started_at: Option<u64>,
    last_exit: Option<i32>,
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

fn scan_dir(dir: &Path, kind: Kind) -> Vec<Spec> {
    let mut out = Vec::new();
    let Ok(entries) = fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(fname) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !fname.ends_with(".py") || fname.starts_with('_') {
            continue;
        }
        let stem = fname.trim_end_matches(".py").to_string();
        let mtime = fs::metadata(&path)
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_secs())
            .unwrap_or(0);
        out.push(Spec {
            name: format!("{}:{}", kind.as_str(), stem),
            path,
            kind: kind.clone(),
            mtime,
        });
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

fn build_command(spec: &Spec, root: &Path) -> Command {
    let mut cmd = match spec.kind {
        Kind::Flow => {
            let mut c = Command::new("python3");
            c.arg("-m").arg("vejas").arg("run").arg(&spec.path);
            c
        }
        Kind::Connector => {
            let mut c = Command::new("python3");
            c.arg(&spec.path);
            c
        }
    };
    let sdk = root.join("sdk").join("python");
    let py_path = match env::var("PYTHONPATH") {
        Ok(existing) if !existing.is_empty() => format!("{}:{}", sdk.display(), existing),
        _ => sdk.display().to_string(),
    };
    cmd.env("PYTHONPATH", py_path);
    cmd.env("VEJAS_PROC", &spec.name);
    cmd
}

fn supervise(handle: Arc<Handle>, root: PathBuf) {
    let mut delay = Duration::from_secs(1);
    loop {
        if !RUNNING.load(Ordering::SeqCst) || handle.stop.load(Ordering::SeqCst) {
            break;
        }
        let started = Instant::now();
        let spawned = build_command(&handle.spec, &root).spawn();
        let mut child = match spawned {
            Ok(child) => child,
            Err(err) => {
                eprintln!("[vejas] {}: spawn failed: {err}", handle.spec.name);
                let mut st = handle.state.lock().unwrap();
                st.status = "spawn-error".into();
                st.pid = None;
                drop(st);
                thread::sleep(delay);
                delay = (delay * 2).min(Duration::from_secs(30));
                continue;
            }
        };
        {
            let mut st = handle.state.lock().unwrap();
            st.status = "running".into();
            st.pid = Some(child.id());
            st.started_at = Some(now_secs());
        }
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
                Err(err) => {
                    eprintln!("[vejas] {}: wait error: {err}", handle.spec.name);
                    break None;
                }
            }
        };

        let mut st = handle.state.lock().unwrap();
        st.pid = None;
        match exit {
            Some(status) => {
                st.last_exit = status.code();
                st.restarts += 1;
                st.status = "restarting".into();
                drop(st);
                eprintln!(
                    "[vejas] {} exited ({:?}), restarting",
                    handle.spec.name,
                    exit.and_then(|s| s.code())
                );
                if started.elapsed() > Duration::from_secs(30) {
                    delay = Duration::from_secs(1);
                }
                thread::sleep(delay);
                delay = (delay * 2).min(Duration::from_secs(30));
            }
            None => {
                st.status = "stopped".into();
                break;
            }
        }
    }
    let mut st = handle.state.lock().unwrap();
    st.status = "stopped".into();
    st.pid = None;
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
        }),
        stop: AtomicBool::new(false),
    });
    registry.lock().unwrap().insert(spec.name.clone(), handle.clone());
    let root = root.to_path_buf();
    thread::spawn(move || supervise(handle, root));
}

fn reload(registry: &Registry, root: &Path) -> (usize, usize, usize) {
    let mut wanted: HashMap<String, Spec> = HashMap::new();
    for spec in scan_dir(&root.join("connectors"), Kind::Connector)
        .into_iter()
        .chain(scan_dir(&root.join("flows"), Kind::Flow))
    {
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

fn topology_json(registry: &Registry) -> serde_json::Value {
    let reg = registry.lock().unwrap();
    let mut flows = Vec::new();
    let mut connectors = Vec::new();
    for handle in reg.values() {
        let st = handle.state.lock().unwrap();
        let entry = json!({
            "name": handle.spec.name,
            "file": handle.spec.path.display().to_string(),
            "status": st.status,
            "pid": st.pid,
            "restarts": st.restarts,
            "started_at": st.started_at,
            "last_exit": st.last_exit,
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

fn respond(request: tiny_http::Request, code: u16, body: String, ctype: &str) {
    let header = tiny_http::Header::from_bytes(&b"Content-Type"[..], ctype.as_bytes()).unwrap();
    let _ = request.respond(
        tiny_http::Response::from_string(body)
            .with_status_code(code)
            .with_header(header),
    );
}

fn handle_request(mut request: tiny_http::Request, registry: Registry, root: PathBuf) {
    let method = request.method().clone();
    let url = request.url().to_string();
    match (method, url.as_str()) {
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
        (tiny_http::Method::Get, "/graph") => {
            let root_s = root.display().to_string();
            match vejas_py(&root, &["graph", &root_s]) {
                Ok(json) => respond(request, 200, json, "application/json"),
                Err(err) => respond(request, 500, err, "text/plain"),
            }
        }
        (tiny_http::Method::Get, "/surface") => {
            let flows = root.join("flows").display().to_string();
            match vejas_py(&root, &["surface", &flows]) {
                Ok(json) => respond(request, 200, json, "application/json"),
                Err(err) => respond(request, 500, err, "text/plain"),
            }
        }
        (tiny_http::Method::Get, u) if u.starts_with("/preview?file=") => {
            let file = u.trim_start_matches("/preview?file=").replace("%2F", "/");
            match vejas_py(&root, &["preview", &file]) {
                Ok(json) => respond(request, 200, json, "application/json"),
                Err(err) => respond(request, 500, err, "text/plain"),
            }
        }
        (tiny_http::Method::Post, "/surface/set") => {
            let mut body = String::new();
            let _ = request.as_reader().read_to_string(&mut body);
            match serde_json::from_str::<serde_json::Value>(&body) {
                Ok(v) => {
                    let file = v["file"].as_str().unwrap_or("").to_string();
                    let name = v["name"].as_str().unwrap_or("").to_string();
                    let key = v["key"].as_str().unwrap_or("-").to_string();
                    let value = v["value"].to_string();
                    match vejas_py(&root, &["set", &file, &name, &key, &value]) {
                        Ok(json) => respond(request, 200, json, "application/json"),
                        Err(err) => respond(request, 422, err, "text/plain"),
                    }
                }
                Err(err) => respond(request, 400, format!("bad json: {err}"), "text/plain"),
            }
        }
        (tiny_http::Method::Post, "/flows/new") => {
            let mut body = String::new();
            let _ = request.as_reader().read_to_string(&mut body);
            let prompt = serde_json::from_str::<serde_json::Value>(&body)
                .ok()
                .and_then(|v| v["prompt"].as_str().map(|s| s.to_string()))
                .unwrap_or_default();
            if prompt.trim().is_empty() {
                return respond(request, 400, "missing prompt".into(), "text/plain");
            }
            eprintln!("[vejas] asking the agent for a new flow…");
            let root_s = root.display().to_string();
            match vejas_py(&root, &["new", &prompt, &root_s]) {
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
