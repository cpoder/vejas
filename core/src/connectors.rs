// Native connectors, in-process.
//
// Bundled connectors are Rust threads inside the runtime — no subprocess, no
// Python. The subject convention (docs/SUBJECTS.md) is still the whole plugin
// interface, so an external connector in any language can talk to the same bus;
// these are just the batteries included.
//
//   http-in   : POST /ingest/<suffix>  -> publish vx.<suffix>
//   slack-out : consume vx.slack.out   -> Slack webhook (or DRY-RUN log)

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use serde_json::Value;

fn ensure_stream(js: &nats::jetstream::JetStream, stream: &str, subj_root: &str) {
    let _ = js.add_stream(&nats::jetstream::StreamConfig {
        name: stream.to_string(),
        subjects: vec![format!("{subj_root}.>")],
        ..Default::default()
    });
}

/// http-in: a tiny threaded HTTP server that publishes JSON bodies to the bus.
pub fn run_http_in(
    running: Arc<AtomicBool>,
    url: String,
    stream: String,
    subj_root: String,
    port: u16,
) {
    let nc = match nats::connect(&url) {
        Ok(nc) => nc,
        Err(e) => {
            eprintln!("[http-in] cannot connect NATS: {e}");
            return;
        }
    };
    let js = nats::jetstream::new(nc);
    ensure_stream(&js, &stream, &subj_root);
    let listener = match TcpListener::bind(("0.0.0.0", port)) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("[http-in] cannot bind :{port}: {e}");
            return;
        }
    };
    listener.set_nonblocking(true).ok();
    eprintln!("[http-in] listening on :{port}, publishing under {subj_root}.*");
    for stream_res in listener.incoming() {
        if !running.load(Ordering::SeqCst) {
            break;
        }
        match stream_res {
            Ok(mut sock) => {
                let js = js.clone();
                let subj_root = subj_root.clone();
                thread::spawn(move || handle_http(&mut sock, &js, &subj_root));
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(100));
            }
            Err(e) => eprintln!("[http-in] accept: {e}"),
        }
    }
}

fn handle_http(sock: &mut std::net::TcpStream, js: &nats::jetstream::JetStream, subj_root: &str) {
    sock.set_read_timeout(Some(Duration::from_secs(5))).ok();
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    // read headers + body (small requests; read until we have Content-Length bytes)
    let mut header_end = None;
    loop {
        match sock.read(&mut tmp) {
            Ok(0) => break,
            Ok(n) => {
                buf.extend_from_slice(&tmp[..n]);
                if header_end.is_none() {
                    if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
                        header_end = Some(pos + 4);
                    }
                }
                if let Some(he) = header_end {
                    let head = String::from_utf8_lossy(&buf[..he]);
                    let clen = content_length(&head);
                    if buf.len() >= he + clen {
                        break;
                    }
                }
                if buf.len() > 1_048_576 {
                    break;
                }
            }
            Err(_) => break,
        }
    }
    let text = String::from_utf8_lossy(&buf);
    let mut lines = text.split("\r\n");
    let request_line = lines.next().unwrap_or("");
    let mut parts = request_line.split(' ');
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("");
    let body = header_end
        .map(|he| &buf[he.min(buf.len())..])
        .unwrap_or(&[]);

    let reply = |sock: &mut std::net::TcpStream, code: &str, json: &str| {
        let _ = write!(
            sock,
            "HTTP/1.1 {code}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{json}",
            json.len()
        );
    };

    if method == "GET" && path == "/healthz" {
        return reply(sock, "200 OK", "{\"ok\":true}");
    }
    if method != "POST" || !path.starts_with("/ingest/") {
        return reply(sock, "404 Not Found", "{\"error\":\"POST /ingest/<subject-suffix>\"}");
    }
    let suffix = path.trim_start_matches("/ingest/").trim_matches('/');
    if suffix.is_empty() {
        return reply(sock, "400 Bad Request", "{\"error\":\"missing subject suffix\"}");
    }
    if serde_json::from_slice::<Value>(body).is_err() {
        return reply(sock, "400 Bad Request", "{\"error\":\"body must be JSON\"}");
    }
    let subject = format!("{subj_root}.{suffix}");
    match js.publish(&subject, body) {
        Ok(_) => reply(
            sock,
            "202 Accepted",
            &format!("{{\"published\":\"{subject}\"}}"),
        ),
        Err(e) => reply(sock, "502 Bad Gateway", &format!("{{\"error\":\"{e}\"}}")),
    }
}

fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

fn content_length(head: &str) -> usize {
    for line in head.lines() {
        if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
            return v.trim().parse().unwrap_or(0);
        }
    }
    0
}

/// slack-out: durable pull consumer on vx.slack.out -> webhook, or DRY-RUN log.
pub fn run_slack_out(running: Arc<AtomicBool>, url: String, stream: String, subj_root: String) {
    let subject = format!("{subj_root}.slack.out");
    let webhook = std::env::var("SLACK_WEBHOOK_URL").unwrap_or_default();
    loop {
        if !running.load(Ordering::SeqCst) {
            return;
        }
        let attempt = (|| -> Result<(), String> {
            let nc = nats::connect(&url).map_err(|e| e.to_string())?;
            let js = nats::jetstream::new(nc);
            ensure_stream(&js, &stream, &subj_root);
            let _ = js.add_consumer(
                &stream,
                nats::jetstream::ConsumerConfig {
                    durable_name: Some("slack_out".into()),
                    filter_subject: subject.clone(),
                    ..Default::default()
                },
            );
            let sub = js
                .pull_subscribe_with_options(
                    &subject,
                    &nats::jetstream::PullSubscribeOptions::new().durable_name("slack_out".into()),
                )
                .map_err(|e| e.to_string())?;
            let mode = if webhook.is_empty() {
                "DRY-RUN (set SLACK_WEBHOOK_URL to post)"
            } else {
                "webhook"
            };
            eprintln!("[slack-out] consuming {subject} -> {mode}");
            loop {
                if !running.load(Ordering::SeqCst) {
                    return Ok(());
                }
                let batch = sub.fetch(10).map_err(|e| e.to_string())?;
                for msg in batch {
                    let text = serde_json::from_slice::<Value>(&msg.data)
                        .ok()
                        .and_then(|v| v["text"].as_str().map(|s| s.to_string()))
                        .unwrap_or_else(|| String::from_utf8_lossy(&msg.data).into_owned());
                    if webhook.is_empty() {
                        eprintln!("[slack-out] DRY-RUN would post: {text}");
                        let _ = msg.ack();
                    } else {
                        match post_webhook(&webhook, &text) {
                            Ok(()) => {
                                eprintln!("[slack-out] posted: {text}");
                                let _ = msg.ack();
                            }
                            Err(e) => eprintln!("[slack-out] post failed ({e}), will retry"),
                        }
                    }
                }
                thread::sleep(Duration::from_millis(150));
            }
        })();
        if let Err(e) = attempt {
            eprintln!("[slack-out] {e}; reconnecting");
            thread::sleep(Duration::from_secs(2));
        }
    }
}

/// Minimal HTTPS POST via the system curl (no TLS crate pulled in for v0).
fn post_webhook(url: &str, text: &str) -> Result<(), String> {
    let payload = serde_json::json!({ "text": text }).to_string();
    let out = std::process::Command::new("curl")
        .args(["-sS", "-m", "10", "-X", "POST", "-H", "Content-Type: application/json", "-d", &payload, url])
        .output()
        .map_err(|e| e.to_string())?;
    if out.status.success() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&out.stderr).into_owned())
    }
}
