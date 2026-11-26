//! slack-out: posts each message of vx.slack.out to a Slack webhook.

use serde_json::Value;

use crate::connectors::hops::answer_text;
use crate::connectors::http_client::http_post;
use crate::connectors::sink::run_sink;
use crate::connectors::{Ctx, Driver};

pub(super) struct SlackOut;
impl Driver for SlackOut {
    fn kind(&self) -> &'static str {
        "sink"
    }
    fn about(&self) -> &'static str {
        "Consumes vx.slack.out and posts {text} to a Slack webhook (DRY-RUN if unset). Config: SUBJECT, WEBHOOK_URL."
    }
    fn run(&self, ctx: &Ctx) -> Result<(), String> {
        let subject = ctx.subject(&ctx.config.str_or("SUBJECT", "slack.out"));
        let webhook = ctx
            .config
            .str("WEBHOOK_URL")
            .or_else(|| std::env::var("SLACK_WEBHOOK_URL").ok())
            .unwrap_or_default();
        let name = ctx.name.clone();
        run_sink(ctx, &subject, move |data, trace| {
            let text = serde_json::from_slice::<Value>(data)
                .ok()
                .and_then(|v| v["text"].as_str().map(|s| s.to_string()))
                .unwrap_or_else(|| String::from_utf8_lossy(data).into_owned());
            if webhook.is_empty() {
                eprintln!("[{name}] DRY-RUN would post: {text}");
                Ok(Some(format!("DRY-RUN: {}", text.chars().take(160).collect::<String>())))
            } else {
                let payload = serde_json::json!({ "text": text }).to_string();
                let headers = [("Content-Type".to_string(), "application/json".to_string())];
                http_post(&webhook, &headers, payload.as_bytes(), Some(trace)).map(answer_text)
            }
        })
    }
}
