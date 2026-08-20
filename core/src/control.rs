// The control channel (ADR-0013, docs/CONTROL.md) — collector side, v1.
//
// When VEJAS_TENANT is set, the runtime serves a CLOSED command allowlist on
// `vx.<tenant>.ctl.cmd` (core NATS request/reply — interactive, fail-fast),
// pushes periodic state on `…ctl.status`, and echoes every command on
// `…ctl.audit`. The uplink itself is the NATS leaf node configured next to
// the runtime; this module neither knows nor cares — it talks to the local
// bus, subject interest propagates.
//
// v1 is telemetry and operations only. Content changes (proposals) are v2.
// Security invariants live in docs/CONTROL.md §normative; the ones enforced
// here: no secret value in any payload, unknown commands are errors, and the
// channel cannot reconfigure itself.

use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use std::{env, thread};

use serde_json::{json, Value};

use crate::connectors::iso8601_utc;
use crate::Registry;

/// Start the control thread if (and only if) this runtime names a tenant.
pub fn start(registry: Registry, root: PathBuf) {
    let Some(tenant) = env::var("VEJAS_TENANT").ok().filter(|t| !t.is_empty()) else {
        return;
    };
    thread::spawn(move || supervise(registry, root, tenant));
}

fn supervise(registry: Registry, root: PathBuf, tenant: String) {
    let url = env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let subj_root = env::var("VEJAS_SUBJECT_ROOT").unwrap_or_else(|_| "vx".into());
    // Control subjects live under their OWN root, outside the persisted data
    // root: a JetStream stream capturing the command subject answers requests
    // with its pub-ack before the runtime can — found live. Never bind a
    // stream to this root (CONTROL.md, normative).
    let ctl_root =
        env::var("VEJAS_CONTROL_ROOT").unwrap_or_else(|_| format!("{subj_root}c"));
    let status_every = Duration::from_secs(
        env::var("VEJAS_STATUS_SECS").ok().and_then(|v| v.parse().ok()).unwrap_or(60).max(5),
    );
    let cmd_subj = format!("{ctl_root}.{tenant}.cmd");
    let status_subj = format!("{ctl_root}.{tenant}.status");
    let audit_subj = format!("{ctl_root}.{tenant}.audit");
    let mut delay = Duration::from_secs(1);
    loop {
        if !crate::RUNNING.load(Ordering::SeqCst) {
            return;
        }
        let attempt = (|| -> Result<(), String> {
            let nc = nats::connect(&url).map_err(|e| e.to_string())?;
            let sub = nc.subscribe(&cmd_subj).map_err(|e| e.to_string())?;
            eprintln!("[control] tenant {tenant}: serving {cmd_subj}, status every {}s", status_every.as_secs());
            // first status leaves immediately: the console sees us on connect
            let mut last_status = Instant::now() - status_every;
            loop {
                if !crate::RUNNING.load(Ordering::SeqCst) {
                    return Ok(());
                }
                if last_status.elapsed() >= status_every {
                    let _ = nc.publish(
                        &status_subj,
                        serde_json::to_vec(&status_payload(&registry, &tenant)).unwrap_or_default(),
                    );
                    last_status = Instant::now();
                }
                match sub.next_timeout(Duration::from_millis(500)) {
                    Ok(msg) => {
                        let req: Value = serde_json::from_slice(&msg.data).unwrap_or(Value::Null);
                        let (reply, summary) = dispatch(&registry, &root, &req);
                        let ok = reply["ok"].as_bool().unwrap_or(false);
                        let id = req["id"].as_str().unwrap_or("");
                        let cmd = req["cmd"].as_str().unwrap_or("?");
                        // audit twice: the local ring (panel) and the hub
                        crate::record_trace_full(
                            "control", &cmd_subj, ok,
                            (!ok).then(|| reply["error"].as_str().unwrap_or("error").to_string()),
                            vec![], format!("{cmd} {summary}"), None, None,
                        );
                        let _ = nc.publish(
                            &audit_subj,
                            serde_json::to_vec(&json!({
                                "ts": iso8601_utc(crate::now_secs()),
                                "id": id, "cmd": cmd, "ok": ok, "summary": summary,
                            }))
                            .unwrap_or_default(),
                        );
                        if msg.reply.is_some() {
                            let _ = msg.respond(serde_json::to_vec(&reply).unwrap_or_default());
                        }
                    }
                    Err(_) => {} // timeout: loop to re-check RUNNING / status schedule
                }
            }
        })();
        if let Err(e) = attempt {
            eprintln!("[control] {tenant}: {e}");
            thread::sleep(delay);
            delay = (delay * 2).min(Duration::from_secs(30));
        }
    }
}

fn status_payload(registry: &Registry, tenant: &str) -> Value {
    let topo = crate::topology_json(registry);
    let mut units = Vec::new();
    for pool in ["flows", "connectors"] {
        for u in topo[pool].as_array().cloned().unwrap_or_default() {
            units.push(json!({
                "name": u["name"],
                "status": u["status"],
                "restarts": u["restarts"],
                "last_error": u["last_error"].as_str().map(|e| e.chars().take(200).collect::<String>()),
            }));
        }
    }
    json!({
        "ts": iso8601_utc(crate::now_secs()),
        "tenant": tenant,
        "bundle": env::var("VEJAS_BUNDLE").ok(),
        "runtime_version": env!("CARGO_PKG_VERSION"),
        "units": units,
        "pending_proposals": 0, // proposals are v2; the field is contractual
    })
}

/// Execute one request against the CLOSED v1 allowlist. Returns the reply
/// envelope and a one-line audit summary. Pure with respect to the wire:
/// testable without NATS.
pub fn dispatch(registry: &Registry, root: &std::path::Path, req: &Value) -> (Value, String) {
    let id = req["id"].as_str().unwrap_or("");
    let cmd = req["cmd"].as_str().unwrap_or("");
    let args = &req["args"];
    let done = |result: Value, summary: String| {
        (json!({"id": id, "ok": true, "result": result}), summary)
    };
    let fail = |error: String| {
        let summary = error.chars().take(120).collect::<String>();
        (json!({"id": id, "ok": false, "error": error}), summary)
    };
    match cmd {
        "ping" => done(
            json!({
                "pong": true,
                "ts": iso8601_utc(crate::now_secs()),
                "runtime_version": env!("CARGO_PKG_VERSION"),
                "bundle": env::var("VEJAS_BUNDLE").ok(),
            }),
            "pong".into(),
        ),
        "status" => {
            let tenant = env::var("VEJAS_TENANT").unwrap_or_default();
            let mut payload = status_payload(registry, &tenant);
            // secret REFERENCES with resolve-status — statuses only, never values
            let secrets: Value =
                serde_json::from_str(&crate::secrets_json(root)).unwrap_or(Value::Null);
            payload["secrets"] = secrets["secrets"].clone();
            let n = payload["units"].as_array().map(|a| a.len()).unwrap_or(0);
            done(payload, format!("{n} units"))
        }
        "events" => {
            let unit = args["unit"].as_str();
            let n = args["n"].as_u64().unwrap_or(20) as usize;
            let mut v: Value =
                serde_json::from_str(&crate::events_json(unit)).unwrap_or(json!({"events": []}));
            if let Some(list) = v["events"].as_array_mut() {
                list.truncate(n);
            }
            let n = v["events"].as_array().map(|a| a.len()).unwrap_or(0);
            done(v, format!("{n} events"))
        }
        "probe" => match args["file"].as_str() {
            Some(file) => {
                let verdict = crate::probe_connector(root, file);
                let s = format!(
                    "{} {}",
                    file,
                    if verdict["ok"].as_bool().unwrap_or(false) { "ok" } else { "failed" }
                );
                done(verdict, s)
            }
            None => fail("probe: file required".into()),
        },
        "reload" => {
            let (total, started, stopped) = crate::reload(registry, root);
            done(
                json!({"total": total, "started": started, "stopped": stopped}),
                format!("{total} units, {started} started, {stopped} stopped"),
            )
        }
        "restart" => match args["unit"].as_str() {
            Some(unit) if crate::restart_unit(registry, root, unit) => {
                done(json!({"ok": true}), format!("restarted {unit}"))
            }
            Some(unit) => fail(format!("unknown unit {unit:?}")),
            None => fail("restart: unit required".into()),
        },
        "rotate_requested" => match args["ref"].as_str() {
            Some(r) => {
                crate::flag_rotation(r);
                done(json!({"ok": true}), format!("rotation flagged for {r}"))
            }
            None => fail("rotate_requested: ref required".into()),
        },
        // the allowlist is CLOSED: everything else is an error, including any
        // future-looking guess — extensions go through a spec revision
        other => fail(format!("unknown command {other:?} (closed allowlist, see CONTROL.md)")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    fn empty_registry() -> Registry {
        Arc::new(Mutex::new(HashMap::new()))
    }

    #[test]
    fn dispatch_ping_unknown_and_args_errors() {
        let reg = empty_registry();
        let root = std::env::temp_dir();
        let (r, s) = dispatch(&reg, &root, &json!({"id": "1", "cmd": "ping"}));
        assert_eq!(r["ok"], true);
        assert_eq!(r["result"]["pong"], true);
        assert_eq!(r["id"], "1");
        assert_eq!(s, "pong");
        // the allowlist is closed
        let (r, _) = dispatch(&reg, &root, &json!({"id": "2", "cmd": "set_secret"}));
        assert_eq!(r["ok"], false);
        assert!(r["error"].as_str().unwrap().contains("closed allowlist"));
        // missing args fail in words
        let (r, _) = dispatch(&reg, &root, &json!({"id": "3", "cmd": "restart"}));
        assert_eq!(r["ok"], false);
        let (r, _) = dispatch(&reg, &root, &json!({"id": "4", "cmd": "restart", "args": {"unit": "flow:nope"}}));
        assert_eq!(r["ok"], false);
    }

    #[test]
    fn dispatch_rotate_flags_and_events_bounded() {
        let reg = empty_registry();
        let root = std::env::temp_dir();
        let (r, s) = dispatch(&reg, &root, &json!({"id": "5", "cmd": "rotate_requested", "args": {"ref": "ctl/test_rotate"}}));
        assert_eq!(r["ok"], true);
        assert!(s.contains("ctl/test_rotate"));
        assert!(crate::rotation_flagged("ctl/test_rotate"));
        crate::clear_rotation("ctl/test_rotate");
        assert!(!crate::rotation_flagged("ctl/test_rotate"));
        let (r, _) = dispatch(&reg, &root, &json!({"id": "6", "cmd": "events", "args": {"n": 3}}));
        assert_eq!(r["ok"], true);
        assert!(r["result"]["events"].as_array().unwrap().len() <= 3);
    }
}
