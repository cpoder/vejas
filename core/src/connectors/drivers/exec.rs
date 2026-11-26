//! The exec drivers: exec-source, exec-sink, exec-stream-source and exec-rpc.

use std::io::Write;
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::bus;
use crate::connectors::hops::{answer_text, source_publish, trace_fail, trace_pub};
use crate::connectors::offsets::{offset_get, offset_put, open_offset_store};
use crate::connectors::process::shell;
use crate::connectors::sink::run_sink;
use crate::connectors::{Ctx, Driver};
use crate::metrics::Kind;

//
// The hot-add extension path for NEW connector types without recompiling the
// core and without loading native libraries (no .so/.dll, no unstable ABI, no
// in-process arbitrary code). A connector is an external program in ANY
// language; the runtime bridges it to the bus over stdio, so the program needs
// no NATS client. Isolation is by process. See ADR-0011.

pub(super) struct ExecSource;
impl Driver for ExecSource {
    fn kind(&self) -> &'static str {
        "source:exec"
    }
    fn about(&self) -> &'static str {
        "Runs CMD every INTERVAL_SECS; each JSON line it prints on stdout is published to SUBJECT. Any language, no NATS client. Config: CMD, SUBJECT, INTERVAL_SECS."
    }
    fn run(&self, ctx: &Ctx) -> Result<(), String> {
        let cmd = ctx.config.str("CMD").ok_or("CMD required")?;
        let subject = ctx.subject(&ctx.config.str("SUBJECT").ok_or("SUBJECT required")?);
        let interval = ctx.config.u64_or("INTERVAL_SECS", 10).max(1);
        let js = ctx.jetstream()?;
        eprintln!("[{}] exec-source: `{cmd}` every {interval}s -> {subject}", ctx.name);
        let mut waited = interval * 1000; // fire immediately on start
        loop {
            if !ctx.alive() {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(250));
            waited += 250;
            if waited >= interval * 1000 {
                waited = 0;
                let out = shell(&cmd)
                    .output()
                    .map_err(|e| e.to_string())?;
                if !out.status.success() {
                    let err = String::from_utf8_lossy(&out.stderr).trim().to_string();
                    eprintln!("[{}] exec: {err}", ctx.name);
                    trace_fail(&ctx.name, &subject, format!("exec: {err}"));
                }
                for line in out.stdout.split(|&b| b == b'\n') {
                    let line = line.strip_suffix(b"\r").unwrap_or(line);
                    if line.is_empty() {
                        continue;
                    }
                    if serde_json::from_slice::<Value>(line).is_ok() {
                        let _ = source_publish(&ctx.name, &js, &subject, line, None, Kind::Producer);
                    } else {
                        eprintln!("[{}] exec: non-JSON line skipped", ctx.name);
                    }
                }
            }
        }
    }
}

pub(super) struct ExecSink;
impl Driver for ExecSink {
    fn kind(&self) -> &'static str {
        "sink:exec"
    }
    fn about(&self) -> &'static str {
        "Consumes SUBJECT; pipes each message body to CMD's stdin. Any language, no NATS client. ENV = {\"KEY\": secret(\"…\")} is handed to the child environment (secrets never touch argv). Config: CMD, SUBJECT, ENV (optional)."
    }
    fn run(&self, ctx: &Ctx) -> Result<(), String> {
        let cmd = ctx.config.str("CMD").ok_or("CMD required")?;
        let subject = ctx.subject(&ctx.config.str("SUBJECT").ok_or("SUBJECT required")?);
        let name = ctx.name.clone();
        let env = ctx.config.env_vars();
        run_sink(ctx, &subject, move |data, trace| {
            let mut child = shell(&cmd)
                .envs(env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
                // The OpenTelemetry carrier for a child process: a program that
                // traces picks its parent up from here (ADR-0034).
                .env("TRACEPARENT", trace.traceparent())
                .env("TRACESTATE", trace.state.as_deref().unwrap_or(""))
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .map_err(|e| e.to_string())?;
            child
                .stdin
                .take()
                .ok_or("no stdin")?
                .write_all(data)
                .map_err(|e| e.to_string())?;
            let out = child.wait_with_output().map_err(|e| e.to_string())?;
            if out.status.success() {
                Ok(answer_text(out.stdout))
            } else {
                Err(format!("{name}: exit {:?}: {}", out.status.code(), String::from_utf8_lossy(&out.stderr).trim()))
            }
        })
    }
}

// ───────────────────────── source: streaming exec ─────────────────────────

pub(super) struct ExecStreamSource;
impl Driver for ExecStreamSource {
    fn kind(&self) -> &'static str {
        "source:stream"
    }
    fn about(&self) -> &'static str {
        "Runs CMD as a long-running process and publishes each JSON line it streams on stdout to SUBJECT — line by line, with back-pressure (a bounded internal buffer; when the bus is slow the child's stdout blocks, so a burst can't overrun memory). For push sources like SAP IDoc inbound, Salesforce Bulk 2.0, and Kafka (wrap kcat). ENV = {\"KEY\": secret(\"…\")} is handed to the child's environment (secrets never touch argv). On child exit it restarts after RESTART_SECS. Optional offset RESUME (Kafka etc.): set OFFSET_KV to a key and the driver hands the child the last committed offset as $OFFSET at (re)start and commits each record's OFFSET_FIELD (default \"offset\") after publishing it — publish-before-commit, at-least-once, resume in OUR JetStream KV; OFFSET_START (default \"end\") is used on the first run. Config: CMD, SUBJECT, ENV (optional), RESTART_SECS (optional, default 2), OFFSET_KV/OFFSET_FIELD/OFFSET_START (optional)."
    }
    fn run(&self, ctx: &Ctx) -> Result<(), String> {
        use std::io::{BufRead, BufReader};
        use std::process::Stdio;
        use std::sync::mpsc::{sync_channel, RecvTimeoutError};

        let cmd = ctx.config.str("CMD").ok_or("CMD required")?;
        let subject = ctx.subject(&ctx.config.str("SUBJECT").ok_or("SUBJECT required")?);
        let env = ctx.config.env_vars();
        let restart = ctx.config.u64_or("RESTART_SECS", 2).max(1);
        let js = ctx.jetstream()?;
        // Optional offset resume: when OFFSET_KV is set, hand the child the last
        // committed offset as $OFFSET at (re)start (e.g. `kcat -o $OFFSET`) and
        // commit each record's OFFSET_FIELD after publishing it.
        let offset_kv = ctx.config.str("OFFSET_KV");
        let offset_field = ctx.config.str("OFFSET_FIELD").unwrap_or_else(|| "offset".into());
        let offset_start = ctx.config.str("OFFSET_START").unwrap_or_else(|| "end".into());
        let offset_store = offset_kv.as_ref().and_then(|_| open_offset_store(&js));
        // Offset commit CADENCE (keeps the KV put out of the per-record hot path).
        // A KV put is itself a request/reply, and committing per record would
        // add a round-trip to every record. Commit
        // every OFFSET_COMMIT_MS (default 100ms) and once more when the child
        // drains/exits: publish-before-commit still holds (an offset is committed
        // only for a record already on the bus), the crash duplicate window just
        // widens to one interval — exactly a Kafka consumer's auto-commit-interval.
        let commit_every =
            Duration::from_millis(ctx.config.u64_or("OFFSET_COMMIT_MS", 100).max(1));
        eprintln!("[{}] exec-stream-source: `{cmd}` -> {subject} (streaming)", ctx.name);

        while ctx.alive() {
            // resume from the last committed offset (or OFFSET_START on first run)
            let mut run_env = env.clone();
            if let (Some(store), Some(key)) = (&offset_store, &offset_kv) {
                let start = offset_get(store, key).unwrap_or_else(|| offset_start.clone());
                eprintln!("[{}] resuming at OFFSET={start}", ctx.name);
                run_env.push(("OFFSET".to_string(), start));
            }
            let mut child = shell(&cmd)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit()) // child status/errors flow to our logs
                .envs(run_env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
                .spawn()
                .map_err(|e| e.to_string())?;
            let stdout = child.stdout.take().ok_or("no child stdout")?;

            // Bounded channel = back-pressure: when we stop consuming (slow bus),
            // the reader blocks on send, the OS pipe fills, the child blocks on
            // write. Memory stays bounded no matter how fast SAP pushes.
            let (tx, rx) = sync_channel::<String>(64);
            let reader = thread::spawn(move || {
                for line in BufReader::new(stdout).lines() {
                    match line {
                        Ok(l) => {
                            if tx.send(l).is_err() {
                                break; // consumer gone
                            }
                        }
                        Err(_) => break,
                    }
                }
            });

            // per-run offset state: advanced in memory per record, flushed to KV on
            // the cadence below and at drain (never per record).
            let mut pending: Option<String> = None;
            let mut committed: Option<String> = None;
            let mut last_commit = Instant::now();
            loop {
                if !ctx.alive() {
                    let _ = child.kill();
                    break;
                }
                // cadence-based offset commit — keeps the KV put out of the per-record path
                if last_commit.elapsed() >= commit_every {
                    if let (Some(store), Some(key)) = (&offset_store, &offset_kv) {
                        if pending.is_some() && pending != committed {
                            offset_put(store, key, pending.as_ref().unwrap());
                            committed = pending.clone();
                        }
                    }
                    last_commit = Instant::now();
                }
                match rx.recv_timeout(Duration::from_millis(250)) {
                    Ok(line) => {
                        let line = line.trim();
                        if line.is_empty() {
                            continue;
                        }
                        if let Ok(rec) = serde_json::from_str::<Value>(line) {
                            match source_publish(&ctx.name, &js, &subject, line.as_bytes(), None, Kind::Producer).1 {
                                Ok(_) => {
                                    // publish-before-commit: advance the offset in
                                    // MEMORY only after the record is on the bus; the
                                    // cadence loop (above) writes it to KV. No KV put
                                    // in the per-record path.
                                    if offset_store.is_some() {
                                        if let Some(off) =
                                            rec.get(&offset_field).and_then(|x| x.as_i64())
                                        {
                                            pending = Some((off + 1).to_string());
                                        }
                                    }
                                }
                                // recorded in the ring by source_publish
                                Err(_) => {}
                            }
                        } else {
                            eprintln!("[{}] stream: non-JSON line skipped", ctx.name);
                        }
                    }
                    Err(RecvTimeoutError::Timeout) => continue,
                    Err(RecvTimeoutError::Disconnected) => break, // child stdout closed
                }
            }
            // drain/exit: flush the last published offset so a restart resumes
            // correctly — never lose acked progress.
            if let (Some(store), Some(key)) = (&offset_store, &offset_kv) {
                if pending.is_some() && pending != committed {
                    offset_put(store, key, pending.as_ref().unwrap());
                }
            }

            let _ = reader.join();
            let status = child.wait();
            if !ctx.alive() {
                return Ok(());
            }
            eprintln!(
                "[{}] stream process ended ({:?}); restart in {restart}s",
                ctx.name,
                status.ok().and_then(|s| s.code())
            );
            let mut slept = 0;
            while slept < restart * 1000 && ctx.alive() {
                thread::sleep(Duration::from_millis(200));
                slept += 200;
            }
        }
        Ok(())
    }
}

// ───────────────────────── rpc: request/reply exec ─────────────────────────

pub(super) struct ExecRpc;
impl Driver for ExecRpc {
    fn kind(&self) -> &'static str {
        "rpc:exec"
    }
    fn about(&self) -> &'static str {
        "Runs CMD as a long-running request/reply process (one JSON request per line on stdin -> one JSON reply per line on stdout) and serves it over NATS request/reply on REQUEST_SUBJECT — so MCP tools (and flows) can drive an interactive connector like SAP (sap_list / sap_describe / sap_call). Keep REQUEST_SUBJECT OUTSIDE the vx.* JetStream subjects (e.g. \"vxrpc.sap\"). ENV = {\"KEY\": secret(\"…\")} is handed to the child environment (secrets never touch argv). One SAP logon is held open and requests are serialized. Config: CMD, REQUEST_SUBJECT, ENV (optional)."
    }
    fn run(&self, ctx: &Ctx) -> Result<(), String> {
        use std::io::{BufRead, BufReader, Write};
        use std::process::Stdio;

        let cmd = ctx.config.str("CMD").ok_or("CMD required")?;
        let subject = ctx
            .config
            .str("REQUEST_SUBJECT")
            .ok_or("REQUEST_SUBJECT required")?;
        if subject.starts_with(&format!("{}.", ctx.subj_root)) {
            return Err(format!(
                "REQUEST_SUBJECT must not be under {}.* (JetStream-captured); use e.g. vxrpc.sap",
                ctx.subj_root
            ));
        }
        let env = ctx.config.env_vars();
        let nc = bus::connect(&ctx.nats_url)?;

        let mut child = shell(&cmd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .envs(env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
            .spawn()
            .map_err(|e| e.to_string())?;
        let mut stdin = child.stdin.take().ok_or("no child stdin")?;
        let mut stdout = BufReader::new(child.stdout.take().ok_or("no child stdout")?);
        // The process announces readiness with a first stdout line — consume it
        // so it isn't mistaken for a reply.
        let mut ready = String::new();
        stdout.read_line(&mut ready).map_err(|e| e.to_string())?;
        eprintln!("[{}] exec-rpc: `{cmd}` serving {subject}", ctx.name);

        // Queue group (not a plain subscribe) so N clustered instances share the
        // requests — exactly one answers each, load-balanced (ADR-0020). Each
        // instance keeps its own child process (e.g. its own SAP logon).
        let mut sub = nc.queue_subscribe(&subject, "vejas")?;
        loop {
            if !ctx.alive() {
                let _ = child.kill();
                return Ok(());
            }
            match sub.next_timeout(Duration::from_millis(300)) {
                Ok(msg) => {
                    // One request line in, one reply line out (serialized).
                    let mut req = msg.data.to_vec();
                    req.push(b'\n');
                    if stdin.write_all(&req).is_err() {
                        return Err("child stdin closed".into());
                    }
                    let _ = stdin.flush();
                    let mut line = String::new();
                    match stdout.read_line(&mut line) {
                        Ok(0) => return Err("child stdout closed".into()),
                        Ok(_) => {
                            let _ = msg.respond(line.trim().as_bytes());
                            trace_pub(&ctx.name, &subject, line.trim().as_bytes());
                        }
                        Err(e) => return Err(e.to_string()),
                    }
                }
                Err(_) => {
                    // Idle timeout: notice if the child died so we restart.
                    if let Ok(Some(status)) = child.try_wait() {
                        return Err(format!("child exited: {:?}", status.code()));
                    }
                }
            }
        }
    }
    fn probe(&self, ctx: &Ctx) -> Result<String, String> {
        use std::io::{BufRead, BufReader, Write};
        use std::process::Stdio;
        let cmd = ctx.config.str("CMD").ok_or("CMD required")?;
        let env = ctx.config.env_vars();
        let mut child = shell(&cmd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .envs(env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
            .spawn()
            .map_err(|e| e.to_string())?;
        let mut stdin = child.stdin.take().ok_or("no child stdin")?;
        let mut stdout = BufReader::new(child.stdout.take().ok_or("no child stdout")?);
        let mut ready = String::new();
        stdout.read_line(&mut ready).map_err(|e| e.to_string())?; // ready line
        stdin
            .write_all(b"{\"op\":\"ping\"}\n")
            .map_err(|e| e.to_string())?;
        let _ = stdin.flush();
        let mut line = String::new();
        stdout.read_line(&mut line).map_err(|e| e.to_string())?;
        let _ = child.kill();
        let v: Value = serde_json::from_str(line.trim()).map_err(|e| e.to_string())?;
        if v.get("ok").and_then(|b| b.as_bool()).unwrap_or(false) {
            Ok(format!("ping ok — {}", line.trim()))
        } else {
            Err(format!("ping failed: {}", line.trim()))
        }
    }
}
