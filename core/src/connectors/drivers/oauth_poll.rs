//! oauth-poll: the OAuth2 client-credentials REST poller.

use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;
use vejas_connector::clock::iso8601_utc;

use crate::bus;
use crate::connectors::http_client::{host_of, http_request, join_url, next_url, refused, urlencode};
use crate::connectors::{Config, Ctx, Driver, humanize};
use crate::metrics::Kind;

//
// The generic OAuth2 client-credentials REST poller — the driver that stands
// in for the overwhelming majority of "400 connectors": one token endpoint,
// a Bearer, N REST endpoints, cursor pagination. Publishes one message per
// page, enveloped as {endpoint, fetched_at, body}: `endpoint` is the logical
// route for the mapping flow, `fetched_at` (ISO 8601 UTC, stamped here — the
// language has no clock) is what the flow copies into collected_at, so a
// JetStream redelivery reproduces byte-identical facts and idempotency keys.

pub(super) struct OAuthPoll;

/// Client-credentials token cache: refresh a minute early, invalidate on 401.
struct TokenCache {
    token_url: String,
    client_id: String,
    client_secret: String,
    scope: String,
    cached: Option<(String, Instant)>,
}

impl TokenCache {
    fn bearer(&mut self) -> Result<String, String> {
        if let Some((t, deadline)) = &self.cached {
            if Instant::now() < *deadline {
                return Ok(t.clone());
            }
        }
        let (t, ttl) =
            OAuthPoll::token(&self.token_url, &self.client_id, &self.client_secret, &self.scope)?;
        self.cached = Some((t.clone(), Instant::now() + ttl));
        Ok(t)
    }
    fn invalidate(&mut self) {
        self.cached = None;
    }
}

/// GET with the cached bearer; ONE fresh-token retry on 401.
fn authed_get(tc: &mut TokenCache, url: &str) -> Result<(u16, Vec<u8>), String> {
    let tok = tc.bearer()?;
    let headers = [("Authorization".to_string(), format!("Bearer {tok}"))];
    match http_request("GET", url, &headers, None)? {
        (401, _) => {
            tc.invalidate();
            let tok = tc.bearer()?;
            let headers = [("Authorization".to_string(), format!("Bearer {tok}"))];
            http_request("GET", url, &headers, None)
        }
        ok => Ok(ok),
    }
}

/// `{key}` placeholder substitution for EXPAND detail templates.
fn fill_template(template: &str, key: &str, value: &str) -> String {
    template.replace(&format!("{{{key}}}"), value)
}

/// A client-side $expand: list endpoint + per-item detail endpoint, joined by
/// the driver into ONE envelope per page — so flows stay stateless (no
/// cross-message correlation) exactly as with a server-side $expand.
struct Expand {
    name: String,
    list: String,
    detail: String,
    key: String,
    as_field: String,
    /// The array field in the list response (default "value" à la Graph;
    /// "resources" for CrowdStrike, "data" for many others).
    list_field: String,
}

fn parse_expands(cfg: &Config) -> Result<Vec<Expand>, String> {
    let Some(raw) = cfg.value("EXPAND") else { return Ok(Vec::new()) };
    let entries = raw
        .as_array()
        .cloned()
        .ok_or("EXPAND must be a list of {name, list, detail, key, as} docs")?;
    let mut out = Vec::new();
    for e in &entries {
        let field = |k: &str| e.get(k).and_then(|v| v.as_str()).map(|s| s.to_string());
        match (field("name"), field("list"), field("detail"), field("key"), field("as")) {
            (Some(name), Some(list), Some(detail), Some(key), Some(as_field)) => out.push(Expand {
                name,
                list,
                detail,
                key,
                as_field,
                list_field: field("list_field").unwrap_or_else(|| "value".into()),
            }),
            _ => return Err("every EXPAND entry needs name, list, detail, key and as".into()),
        }
    }
    Ok(out)
}

impl OAuthPoll {
    fn token(
        token_url: &str,
        client_id: &str,
        client_secret: &str,
        scope: &str,
    ) -> Result<(String, Duration), String> {
        // scope is optional: CrowdStrike (and other client-credentials APIs)
        // reject a scope param they don't define — omit it entirely when empty
        let mut form = format!(
            "grant_type=client_credentials&client_id={}&client_secret={}",
            urlencode(client_id),
            urlencode(client_secret)
        );
        if !scope.is_empty() {
            form.push_str(&format!("&scope={}", urlencode(scope)));
        }
        let headers = [(
            "Content-Type".to_string(),
            "application/x-www-form-urlencoded".to_string(),
        )];
        let (code, body) = http_request("POST", token_url, &headers, Some(form.as_bytes()))?;
        if !(200..300).contains(&code) {
            // an endpoint may echo the form, the client secret in it
            let mut refusal = refused(code, token_url, &body, 300);
            if !client_secret.is_empty() {
                refusal = refusal.replace(&urlencode(client_secret), "<masked>").replace(client_secret, "<masked>");
            }
            return Err(format!("token endpoint {refusal}"));
        }
        let v: Value =
            serde_json::from_slice(&body).map_err(|e| format!("token endpoint: bad JSON: {e}"))?;
        let tok = v["access_token"]
            .as_str()
            .ok_or("token endpoint: no access_token in response")?
            .to_string();
        // refresh a minute early; floor for endpoints that answer tiny lifetimes
        let ttl = v["expires_in"].as_u64().unwrap_or(300).saturating_sub(60).max(30);
        Ok((tok, Duration::from_secs(ttl)))
    }
}

impl Driver for OAuthPoll {
    fn kind(&self) -> &'static str {
        "source:poll"
    }
    fn about(&self) -> &'static str {
        "OAuth2 client-credentials REST poller: takes a token from TOKEN_URL, GETs each of ENDPOINTS with the Bearer (pagination via NEXT_LINK_FIELD, default \"@odata.nextLink\", capped by MAX_PAGES), publishes one {endpoint, fetched_at, body} message per page on SUBJECT every INTERVAL_SECS. EXPAND = [{name, list, detail, key, as, list_field?}] does a client-side $expand: each item of the list response's array (list_field, default \"value\"; use \"resources\"/\"data\" for other APIs; a bare-string item becomes {key: id}) is enriched with its per-item detail call ({key} substituted in the detail path) and the page ships as ONE envelope under endpoint=name — sized for list APIs without a server-side expand (sequential detail calls, keep pages small). Config: TOKEN_URL, CLIENT_ID, CLIENT_SECRET (use secret()), SCOPE (optional — omitted when empty, e.g. CrowdStrike), BASE_URL, ENDPOINTS and/or EXPAND, SUBJECT, INTERVAL_SECS, MAX_PAGES."
    }
    fn probe(&self, ctx: &Ctx) -> Result<String, String> {
        let token_url = ctx.config.str("TOKEN_URL").ok_or("TOKEN_URL required")?;
        let client_id = ctx.config.str("CLIENT_ID").ok_or("CLIENT_ID required")?;
        let client_secret = ctx.config.str("CLIENT_SECRET").ok_or("CLIENT_SECRET required")?;
        let scope = ctx.config.str_or("SCOPE", ""); // optional (CrowdStrike: none)
        let base = ctx.config.str("BASE_URL").ok_or("BASE_URL required")?;
        let ep = ctx
            .config
            .value("ENDPOINTS")
            .and_then(|v| v.as_array().and_then(|a| a.first().and_then(|e| e.as_str().map(|s| s.to_string()))))
            .or_else(|| {
                parse_expands(&ctx.config).ok().and_then(|ex| ex.first().map(|s| s.list.clone()))
            })
            .ok_or("ENDPOINTS or EXPAND required")?;
        let (tok, _) = Self::token(&token_url, &client_id, &client_secret, &scope)
            .map_err(|e| humanize(&e))?;
        let url = join_url(&base, &ep);
        let headers = [("Authorization".to_string(), format!("Bearer {tok}"))];
        match http_request("GET", &url, &headers, None) {
            Ok((code, body)) if (200..300).contains(&code) => {
                let items = serde_json::from_slice::<Value>(&body)
                    .ok()
                    .and_then(|v| v["value"].as_array().map(|a| a.len()));
                Ok(match items {
                    Some(n) => format!("token OK · GET {ep} OK ({n} items)"),
                    None => format!("token OK · GET {ep} OK ({} bytes)", body.len()),
                })
            }
            Ok((code, _)) => Err(humanize(&format!(
                "token OK, but GET {ep} answered HTTP {code} (missing permission / admin consent?)"
            ))),
            Err(e) => Err(humanize(&format!("GET {ep}: {e}"))),
        }
    }
    fn run(&self, ctx: &Ctx) -> Result<(), String> {
        let token_url = ctx.config.str("TOKEN_URL").ok_or("TOKEN_URL required")?;
        let client_id = ctx.config.str("CLIENT_ID").ok_or("CLIENT_ID required")?;
        let client_secret = ctx.config.str("CLIENT_SECRET").ok_or("CLIENT_SECRET required")?;
        let scope = ctx.config.str_or("SCOPE", ""); // optional: omitted from the token form when empty
        let base = ctx.config.str("BASE_URL").ok_or("BASE_URL required")?;
        let endpoints: Vec<String> = ctx
            .config
            .value("ENDPOINTS")
            .and_then(|v| {
                v.as_array().map(|a| {
                    a.iter()
                        .filter_map(|e| e.as_str().map(|s| s.to_string()))
                        .collect::<Vec<_>>()
                })
            })
            .unwrap_or_default();
        let expands = parse_expands(&ctx.config)?;
        if endpoints.is_empty() && expands.is_empty() {
            return Err("ENDPOINTS or EXPAND required".into());
        }
        let subject = ctx.subject(&ctx.config.str("SUBJECT").ok_or("SUBJECT required")?);
        let interval = ctx.config.u64_or("INTERVAL_SECS", 300).max(1);
        let max_pages = ctx.config.u64_or("MAX_PAGES", 10).max(1);
        let next_field = ctx.config.str_or("NEXT_LINK_FIELD", "@odata.nextLink");
        let js = ctx.jetstream()?;
        eprintln!(
            "[{}] oauth-poll {} endpoint(s) + {} expand(s) on {} every {interval}s -> {subject}",
            ctx.name,
            endpoints.len(),
            expands.len(),
            host_of(&base)
        );
        let mut tc = TokenCache {
            token_url,
            client_id,
            client_secret,
            scope,
            cached: None,
        };
        let mut waited = interval * 1000; // fire immediately on start
        loop {
            if !ctx.alive() {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(250));
            waited += 250;
            if waited < interval * 1000 {
                continue;
            }
            waited = 0;
            // a broken token config never comes back on its own: validate it
            // once per tick, and fail the run (supervised backoff) if not
            tc.bearer().map_err(|e| {
                ctx.failed(&subject, e.clone());
                e
            })?;
            let publish_page =
                |parsed: &Value, logical: &str, js: &bus::JetStream| -> Result<(), String> {
                    let envelope = serde_json::json!({
                        "endpoint": logical,
                        "fetched_at": iso8601_utc(crate::now_secs()),
                        "body": parsed,
                    });
                    let bytes = serde_json::to_vec(&envelope).unwrap_or_default();
                    if let (_, Err(e)) = ctx.publish(js, &subject, &bytes, None, Kind::Producer) {
                        return Err(format!("publish: {e}"));
                    }
                    Ok(())
                };
            for ep in &endpoints {
                let mut url = join_url(&base, ep);
                let mut pages = 0;
                while pages < max_pages {
                    if !ctx.alive() {
                        return Ok(());
                    }
                    match authed_get(&mut tc, &url) {
                        Ok((code, body)) if (200..300).contains(&code) => {
                            let parsed: Value = match serde_json::from_slice(&body) {
                                Ok(v) => v,
                                Err(e) => {
                                    eprintln!("[{}] {ep}: non-JSON page skipped: {e}", ctx.name);
                                    break;
                                }
                            };
                            publish_page(&parsed, ep, &js)?;
                            pages += 1;
                            match next_url(&base, &parsed, &next_field) {
                                Some(next) => url = next,
                                None => break,
                            }
                        }
                        Ok((code, body)) => {
                            let msg = format!("{ep}: {}", refused(code, &url, &body, 200));
                            eprintln!("[{}] {msg} — endpoint skipped this tick", ctx.name);
                            ctx.failed(&subject, msg);
                            break;
                        }
                        Err(e) => {
                            eprintln!("[{}] {ep}: {e} — endpoint skipped this tick", ctx.name);
                            ctx.failed(&subject, format!("{ep}: {e}"));
                            break;
                        }
                    }
                }
            }
            // client-side $expand: list page fetched, every item enriched with
            // its detail call, ONE envelope per page — flows stay stateless
            for spec in &expands {
                let mut url = join_url(&base, &spec.list);
                let mut pages = 0;
                while pages < max_pages {
                    if !ctx.alive() {
                        return Ok(());
                    }
                    match authed_get(&mut tc, &url) {
                        Ok((code, body)) if (200..300).contains(&code) => {
                            let mut parsed: Value = match serde_json::from_slice(&body) {
                                Ok(v) => v,
                                Err(e) => {
                                    eprintln!("[{}] {}: non-JSON page skipped: {e}", ctx.name, spec.name);
                                    break;
                                }
                            };
                            if let Some(items) =
                                parsed.get_mut(&spec.list_field).and_then(|v| v.as_array_mut())
                            {
                                for item in items.iter_mut() {
                                    if !ctx.alive() {
                                        return Ok(());
                                    }
                                    // an item may be an OBJECT with a {key} field, or a
                                    // bare STRING id (CrowdStrike queries → [ids]); a
                                    // scalar becomes {key: id} so detail + as land on it
                                    if item.is_string() {
                                        let id = item.as_str().unwrap().to_string();
                                        let mut o = serde_json::Map::new();
                                        o.insert(spec.key.clone(), Value::String(id));
                                        *item = Value::Object(o);
                                    }
                                    let Some(id) = item
                                        .get(&spec.key)
                                        .and_then(|v| v.as_str())
                                        .map(|s| s.to_string())
                                    else {
                                        continue;
                                    };
                                    let durl = join_url(
                                        &base,
                                        &fill_template(&spec.detail, &spec.key, &id),
                                    );
                                    // a failing detail leaves null on THAT item;
                                    // the page still ships (per-item resilience)
                                    let detail = match authed_get(&mut tc, &durl) {
                                        Ok((c, b)) if (200..300).contains(&c) => {
                                            serde_json::from_slice::<Value>(&b)
                                                .unwrap_or(Value::Null)
                                        }
                                        Ok((c, _)) => {
                                            eprintln!(
                                                "[{}] {}: detail for {id} -> HTTP {c}",
                                                ctx.name, spec.name
                                            );
                                            Value::Null
                                        }
                                        Err(e) => {
                                            eprintln!(
                                                "[{}] {}: detail for {id}: {e}",
                                                ctx.name, spec.name
                                            );
                                            Value::Null
                                        }
                                    };
                                    if let Some(o) = item.as_object_mut() {
                                        o.insert(spec.as_field.clone(), detail);
                                    }
                                }
                            }
                            publish_page(&parsed, &spec.name, &js)?;
                            pages += 1;
                            match next_url(&base, &parsed, &next_field) {
                                Some(next) => url = next,
                                None => break,
                            }
                        }
                        Ok((code, body)) => {
                            let msg = format!("{}: {}", spec.name, refused(code, &url, &body, 200));
                            eprintln!("[{}] {msg} — expand skipped this tick", ctx.name);
                            ctx.failed(&subject, msg);
                            break;
                        }
                        Err(e) => {
                            eprintln!("[{}] {}: {e} — expand skipped this tick", ctx.name, spec.name);
                            ctx.failed(&subject, format!("{}: {e}", spec.name));
                            break;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use serde_json::Map;

    #[test]
    fn expand_template_and_config() {
        assert_eq!(
            fill_template("/users/{id}/authentication/methods", "id", "u-42"),
            "/users/u-42/authentication/methods"
        );
        let mut m = Map::new();
        m.insert(
            "EXPAND".into(),
            json!([{"name": "ua", "list": "/users", "detail": "/users/{id}/x", "key": "id", "as": "x"}]),
        );
        let ex = parse_expands(&Config(m)).unwrap();
        assert_eq!(ex.len(), 1);
        assert_eq!(ex[0].name, "ua");
        assert_eq!(ex[0].as_field, "x");
        assert_eq!(ex[0].list_field, "value"); // default à la Graph
        // list_field is configurable (CrowdStrike: "resources")
        let mut cs = Map::new();
        cs.insert("EXPAND".into(), json!([{"name":"d","list":"/q","detail":"/e?ids={id}","key":"id","as":"device","list_field":"resources"}]));
        let cex = parse_expands(&Config(cs)).unwrap();
        assert_eq!(cex[0].list_field, "resources");
        // malformed entries are an error, not a silent drop
        let mut bad = Map::new();
        bad.insert("EXPAND".into(), json!([{"name": "ua", "list": "/users"}]));
        assert!(parse_expands(&Config(bad)).is_err());
        assert!(parse_expands(&Config::default()).unwrap().is_empty());
    }
}
