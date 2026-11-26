//! http-poll: GETs a URL every INTERVAL_SECS and publishes the body.

use std::thread;
use std::time::Duration;

use serde_json::Value;

use crate::connectors::clock::iso8601_utc;
use crate::connectors::hops::{source_publish, trace_fail};
use crate::connectors::http_client::http_get;
use crate::connectors::{Ctx, Driver, humanize};
use crate::metrics::Kind;

pub(super) struct HttpPoll;
impl Driver for HttpPoll {
    fn kind(&self) -> &'static str {
        "source:poll"
    }
    fn about(&self) -> &'static str {
        "GETs a URL every INTERVAL_SECS and publishes the JSON body. Config: URL, SUBJECT, INTERVAL_SECS, HEADERS (optional doc, e.g. {\"Authorization\": …}), ENVELOPE (optional bool: when true, publish {endpoint, fetched_at, body} like oauth-poll so a stateless flow gets a collected_at — the language has no clock)."
    }
    fn probe(&self, ctx: &Ctx) -> Result<String, String> {
        let url = ctx.config.str("URL").ok_or("URL required")?;
        match http_get(&url, &ctx.config.headers()) {
            Ok(body) => Ok(format!("GET OK ({} bytes)", body.len())),
            Err(e) => Err(humanize(&e)),
        }
    }
    fn run(&self, ctx: &Ctx) -> Result<(), String> {
        let url = ctx.config.str("URL").ok_or("URL required")?;
        let subject = ctx.subject(&ctx.config.str("SUBJECT").ok_or("SUBJECT required")?);
        let interval = ctx.config.u64_or("INTERVAL_SECS", 60).max(1);
        let headers = ctx.config.headers();
        let envelope = ctx.config.0.get("ENVELOPE").and_then(|v| v.as_bool()).unwrap_or(false);
        let js = ctx.jetstream()?;
        eprintln!("[{}] polling {url} every {interval}s -> {subject}", ctx.name);
        let mut waited = interval * 1000; // fire immediately on start
        loop {
            if !ctx.alive() {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(250));
            waited += 250;
            if waited >= interval * 1000 {
                waited = 0;
                match http_get(&url, &headers) {
                    Ok(raw) => match serde_json::from_slice::<Value>(&raw) {
                        Ok(parsed) => {
                            // ENVELOPE=true: {endpoint, fetched_at, body} (stateless
                            // flow gets collected_at); else the raw body (compat)
                            let bytes = if envelope {
                                serde_json::to_vec(&serde_json::json!({
                                    "endpoint": url,
                                    "fetched_at": iso8601_utc(crate::now_secs()),
                                    "body": parsed,
                                }))
                                .unwrap_or_default()
                            } else {
                                raw.clone()
                            };
                            let _ = source_publish(&ctx.name, &js, &subject, &bytes, None, Kind::Producer);
                        }
                        Err(_) => eprintln!("[{}] poll: non-JSON body, skipped", ctx.name),
                    },
                    Err(e) => {
                        eprintln!("[{}] poll: {e}", ctx.name);
                        trace_fail(&ctx.name, &subject, e);
                    }
                }
            }
        }
    }
}
