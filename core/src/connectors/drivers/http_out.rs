//! http-out: POSTs each message of a subject to a URL.

use vejas_connector::{answer_text, summarize};

use crate::connectors::http_client::{http_post, http_request};
use crate::connectors::{Ctx, Driver, humanize};

pub(super) struct HttpOut;
impl Driver for HttpOut {
    fn kind(&self) -> &'static str {
        "sink"
    }
    fn about(&self) -> &'static str {
        "Consumes a subject and POSTs each message body to a URL. Config: SUBJECT, URL, HEADERS (optional doc — put credentials there via secret(), e.g. {\"Authorization\": …}), TEST_BODY (optional harmless payload enabling the test probe)."
    }
    fn probe(&self, ctx: &Ctx) -> Result<String, String> {
        let url = ctx.config.str("URL").ok_or("URL required")?;
        let body = ctx.config.value("TEST_BODY").ok_or(
            "no TEST_BODY in the manifest: add a harmless payload the target accepts (e.g. an empty batch) to enable this probe",
        )?;
        let mut headers = ctx.config.headers();
        if !headers.iter().any(|(k, _)| k.eq_ignore_ascii_case("content-type")) {
            headers.push(("Content-Type".into(), "application/json".into()));
        }
        let bytes = serde_json::to_vec(&body).map_err(|e| e.to_string())?;
        match http_request("POST", &url, &headers, Some(&bytes)) {
            Ok((code, resp)) if (200..300).contains(&code) => Ok(format!(
                "POST OK · answered: {}",
                summarize(&resp).unwrap_or_else(|| "(empty)".into())
            )),
            Ok((401 | 403, _)) => Err(humanize(&format!("HTTP 401: authentication rejected by {url}"))),
            // any other answer past auth still proves network + credentials;
            // only the probe payload was refused
            Ok((code, resp)) => Ok(format!(
                "reachable and authenticated · but TEST_BODY was refused (HTTP {code}: {})",
                summarize(&resp).unwrap_or_default()
            )),
            Err(e) => Err(humanize(&e)),
        }
    }
    fn run(&self, ctx: &Ctx) -> Result<(), String> {
        let subject = ctx.subject(&ctx.config.str("SUBJECT").ok_or("SUBJECT required")?);
        let url = ctx.config.str("URL").ok_or("URL required")?;
        let mut headers = ctx.config.headers();
        if !headers.iter().any(|(k, _)| k.eq_ignore_ascii_case("content-type")) {
            headers.push(("Content-Type".into(), "application/json".into()));
        }
        ctx.consume(&subject, move |data, trace| {
            http_post(&url, &headers, data, Some(trace)).map(answer_text)
        })
    }
}
