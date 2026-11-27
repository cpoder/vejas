//! http-out: POSTs each message of a subject to a URL.

use vejas_connector::{answer_text, summarize};

use crate::connectors::http_client::{host_of, http_post, http_request, refused, Echoes};
use crate::connectors::{Ctx, Driver, humanize};

pub(super) struct HttpOut;
impl Driver for HttpOut {
    fn kind(&self) -> &'static str {
        "sink"
    }
    fn about(&self) -> &'static str {
        "Consumes a subject and POSTs each message body to a URL. Config: SUBJECT, URL, HEADERS (optional doc — put credentials there via secret(), e.g. {\"Authorization\": …}), TEST_BODY (optional harmless payload enabling the test probe). A redirect is not followed."
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
            // the URL's values masked in the whole answer, then its start: one
            // that the summary's cut would split is found all the same
            Ok((code, resp)) if (200..300).contains(&code) => Ok(crate::record::shown().error(&format!(
                "POST OK · answered: {}",
                answer_text(resp)
                    .and_then(|answer| summarize(Echoes::of(&url).masked(answer).as_bytes()))
                    .unwrap_or_else(|| "(empty)".into())
            ))),
            Ok((401 | 403, _)) => Err(humanize(&format!("HTTP 401: authentication rejected by {}", host_of(&url)))),
            Ok((code, resp)) if (300..400).contains(&code) => Err(refused(code, &url, &resp, 300, &[])),
            // any other answer past auth still proves network + credentials;
            // only the probe payload was refused
            Ok((code, resp)) => Ok(format!(
                "reachable and authenticated · but TEST_BODY was refused ({})",
                refused(code, &url, &resp, 300, &[])
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
        // an answer may echo the URL, a 2xx as well as an error, and the
        // record and the events keep it
        let echoes = Echoes::of(&url);
        ctx.consume(&subject, move |data, trace| {
            http_post(&url, &headers, data, Some(trace)).map(|body| answer_text(body).map(|text| echoes.masked(text)))
        })
    }
}
