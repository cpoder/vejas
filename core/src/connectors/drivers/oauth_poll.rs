//! oauth-poll: the OAuth2 client-credentials REST poller.

use std::collections::HashSet;
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;
use vejas_connector::clock::iso8601_utc;

use crate::bus;
use crate::connectors::http_client::{
    host_of, http_request, join_url, link_url, next_url, path_segment, path_shown, refused, urlencode,
};
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

/// Client-credentials token cache: refreshed before it runs out, and on a 401
/// of a route that has not refused a token fetched for it (`authed_get`).
struct TokenCache {
    /// The connector: its tokens are masked as its own (`secrets::hold`).
    holder: String,
    token_url: String,
    client_id: String,
    client_secret: String,
    scope: String,
    /// The token in use, and when it is fetched again, before the token
    /// endpoint said it runs out.
    cached: Option<(String, Instant)>,
    /// The routes, an endpoint or an EXPAND's list, that answered 401 to a
    /// token fetched for their 401: the client may not read them (a scope, a
    /// consent), which another token does not change.
    refused: HashSet<String>,
    /// The same for an EXPAND's details, by their template, while the token
    /// in use lives: they answer by item, so an item read says nothing of the
    /// next one, and only another token is taken for a change.
    items_refused: HashSet<String>,
}

impl TokenCache {
    fn bearer(&mut self) -> Result<String, String> {
        if let Some((token, _)) = self.cached.as_ref().filter(|(_, deadline)| Instant::now() < *deadline) {
            return Ok(token.clone());
        }
        let (token, life) = OAuthPoll::token(&self.holder, &self.token_url, &self.client_id, &self.client_secret, &self.scope)?;
        self.cached = Some((token.clone(), Instant::now() + life));
        self.items_refused.clear();
        Ok(token)
    }

    fn refusals(&mut self, item: bool) -> &mut HashSet<String> {
        if item { &mut self.items_refused } else { &mut self.refused }
    }
}

/// GET with the cached bearer. On a 401, one retry with another token, as a
/// revoked one must be replaced, unless `route` refused one fetched for it
/// already: one token per route and per tick would hammer the token
/// endpoint, each one more value for the masks. A route that answers
/// otherwise is taken for refused no more. An `item`'s detail answers for
/// its item alone: its route stays refused while the token lives, and a
/// token revoked meanwhile is replaced at the next list.
fn authed_get(tc: &mut TokenCache, url: &str, route: &str, item: bool) -> Result<(u16, Vec<u8>), String> {
    let get = |tok: &str| http_request("GET", url, &[("Authorization".to_string(), format!("Bearer {tok}"))], None);
    let answer = get(&tc.bearer()?)?;
    if answer.0 != 401 {
        if !item {
            tc.refused.remove(route);
        }
        return Ok(answer);
    }
    if tc.refusals(item).contains(route) {
        return Ok(answer);
    }
    tc.cached = None;
    let answer = get(&tc.bearer()?)?;
    if answer.0 == 401 {
        tc.refusals(item).insert(route.to_string());
    }
    Ok(answer)
}

/// How long a token is used: its lifetime less a tenth of it, a minute at
/// most, so that it is fetched again before it runs out; 5 minutes when the
/// token endpoint does not say. 5 s at least, so that a lifetime of 0 does
/// not fetch a token a request, a 401 past it drawing another; a day at most.
fn token_life(expires_in: Option<u64>) -> Duration {
    const LEAST: u64 = 5;
    const MOST: u64 = 86_400;
    let life = expires_in.unwrap_or(300).min(MOST);
    Duration::from_secs((life - (life / 10).min(60)).max(LEAST))
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
        holder: &str,
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
            return Err(format!("token endpoint {}", refused(code, token_url, &body, 300, &[client_secret])));
        }
        let v: Value =
            serde_json::from_slice(&body).map_err(|e| format!("token endpoint: bad JSON: {e}"))?;
        let tok = v["access_token"]
            .as_str()
            .ok_or("token endpoint: no access_token in response")?
            .to_string();
        // a credential no store handed out: masked wherever theirs are, in an
        // answer that quotes it as in the record (ADR-0017)
        crate::secrets::hold(holder, &tok);
        Ok((tok, token_life(v["expires_in"].as_u64())))
    }
}

impl Driver for OAuthPoll {
    fn kind(&self) -> &'static str {
        "source:poll"
    }
    fn about(&self) -> &'static str {
        "OAuth2 client-credentials REST poller: takes a token from TOKEN_URL, GETs each of ENDPOINTS with the Bearer (pagination via NEXT_LINK_FIELD, default \"@odata.nextLink\", capped by MAX_PAGES; a next page on another scheme, host or port than its page is not followed, since the token would go along), publishes one {endpoint, fetched_at, body} message per page on SUBJECT every INTERVAL_SECS (endpoint: as ENDPOINTS names it, without its query). EXPAND = [{name, list, detail, key, as, list_field?}] does a client-side $expand: each item of the list response's array (list_field, default \"value\"; use \"resources\"/\"data\" for other APIs; a bare-string item becomes {key: id}) is enriched with its per-item detail call ({key} substituted in the detail path, percent-encoded as one segment of it; a detail that is \"{key}\" alone is the item's link, followed on the list page's origin only) and the page ships as ONE envelope under endpoint=name — sized for list APIs without a server-side expand (sequential detail calls, keep pages small). Config: TOKEN_URL, CLIENT_ID, CLIENT_SECRET (use secret()), SCOPE (optional — omitted when empty, e.g. CrowdStrike), BASE_URL, ENDPOINTS and/or EXPAND, SUBJECT, INTERVAL_SECS, MAX_PAGES."
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
        // a holder of its own, so that a test never lets go of the token the
        // running connector still sends
        let (tok, _) = Self::token(&format!("{}:test", ctx.name), &token_url, &client_id, &client_secret, &scope)
            .map_err(|e| humanize(&e))?;
        let url = join_url(&base, &ep);
        let headers = [("Authorization".to_string(), format!("Bearer {tok}"))];
        match http_request("GET", &url, &headers, None) {
            Ok((code, body)) if (200..300).contains(&code) => {
                let items = serde_json::from_slice::<Value>(&body)
                    .ok()
                    .and_then(|v| v["value"].as_array().map(|a| a.len()));
                Ok(match items {
                    Some(n) => format!("token OK · GET {} OK ({n} items)", path_shown(&ep)),
                    None => format!("token OK · GET {} OK ({} bytes)", path_shown(&ep), body.len()),
                })
            }
            Ok((code, _)) => Err(humanize(&format!(
                "token OK, but GET {} answered HTTP {code} (missing permission / admin consent?)",
                path_shown(&ep)
            ))),
            Err(e) => Err(humanize(&format!("GET {}: {e}", path_shown(&ep)))),
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
            holder: ctx.name.clone(),
            token_url,
            client_id,
            client_secret,
            scope,
            cached: None,
            refused: HashSet::new(),
            items_refused: HashSet::new(),
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
                    match authed_get(&mut tc, &url, ep, false) {
                        Ok((code, body)) if (200..300).contains(&code) => {
                            let parsed: Value = match serde_json::from_slice(&body) {
                                Ok(v) => v,
                                Err(e) => {
                                    eprintln!("[{}] {}: non-JSON page skipped: {e}", ctx.name, path_shown(ep));
                                    break;
                                }
                            };
                            // the endpoint names the page without its query, where a
                            // credential may ride (an Azure Functions `code`)
                            publish_page(&parsed, &path_shown(ep), &js)?;
                            pages += 1;
                            match next_url(&base, &url, &parsed, &next_field) {
                                Ok(Some(next)) => url = next,
                                Ok(None) => break,
                                Err(e) => {
                                    let msg = format!("{}: {e}", path_shown(ep));
                                    eprintln!("[{}] {msg} — endpoint skipped this tick", ctx.name);
                                    ctx.failed(&subject, msg);
                                    break;
                                }
                            }
                        }
                        Ok((code, body)) => {
                            let msg = format!("{}: {}", path_shown(ep), refused(code, &url, &body, 200, &[]));
                            eprintln!("[{}] {msg} — endpoint skipped this tick", ctx.name);
                            ctx.failed(&subject, msg);
                            break;
                        }
                        Err(e) => {
                            let msg = format!("{}: {e}", path_shown(ep));
                            eprintln!("[{}] {msg} — endpoint skipped this tick", ctx.name);
                            ctx.failed(&subject, msg);
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
                    match authed_get(&mut tc, &url, &spec.list, false) {
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
                                    // a detail that is the item's field alone is its
                                    // link, followed on the list page's origin only;
                                    // an id in a path is one segment of it
                                    let durl = if spec.detail == format!("{{{}}}", spec.key) {
                                        link_url(&base, &url, &id, "its detail")
                                    } else {
                                        path_segment(&id)
                                            .map(|segment| join_url(&base, &fill_template(&spec.detail, &spec.key, &segment)))
                                            .ok_or_else(|| "an id that names no segment of a path".to_string())
                                    };
                                    // a failing detail leaves null on THAT item;
                                    // the page still ships (per-item resilience)
                                    let detail = match durl.and_then(|durl| authed_get(&mut tc, &durl, &spec.detail, true)) {
                                        Ok((c, b)) if (200..300).contains(&c) => {
                                            serde_json::from_slice::<Value>(&b)
                                                .unwrap_or(Value::Null)
                                        }
                                        Ok((c, _)) => {
                                            eprintln!(
                                                "[{}] {}: detail for {} -> HTTP {c}",
                                                ctx.name,
                                                spec.name,
                                                path_shown(&id)
                                            );
                                            Value::Null
                                        }
                                        Err(e) => {
                                            eprintln!(
                                                "[{}] {}: detail for {}: {e}",
                                                ctx.name,
                                                spec.name,
                                                path_shown(&id)
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
                            match next_url(&base, &url, &parsed, &next_field) {
                                Ok(Some(next)) => url = next,
                                Ok(None) => break,
                                Err(e) => {
                                    let msg = format!("{}: {e}", spec.name);
                                    eprintln!("[{}] {msg} — expand skipped this tick", ctx.name);
                                    ctx.failed(&subject, msg);
                                    break;
                                }
                            }
                        }
                        Ok((code, body)) => {
                            let msg = format!("{}: {}", spec.name, refused(code, &url, &body, 200, &[]));
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
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    /// A token endpoint that numbers its tokens, `prefix` first, and an API
    /// that answers what `api` says to each path and token: the base URL, and
    /// how many tokens were fetched.
    fn token_server(prefix: &'static str, api: fn(&str, &str) -> u16) -> (String, Arc<AtomicU32>) {
        use std::io::{BufRead as _, BufReader, Read as _, Write as _};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let fetched = Arc::new(AtomicU32::new(0));
        let count = Arc::clone(&fetched);
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                let Ok(read) = stream.try_clone() else { return };
                let mut reader = BufReader::new(read);
                let (mut first, mut length, mut bearer) = (String::new(), 0, String::new());
                let _ = reader.read_line(&mut first);
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                        break;
                    }
                    let lower = line.to_ascii_lowercase();
                    if let Some(n) = lower.strip_prefix("content-length:") {
                        length = n.trim().parse().unwrap_or(0);
                    }
                    if lower.starts_with("authorization:") {
                        bearer = line.trim_end().rsplit(' ').next().unwrap_or_default().to_string();
                    }
                }
                let _ = reader.read_exact(&mut vec![0; length]);
                let (code, answer) = if first.starts_with("POST") {
                    let n = count.fetch_add(1, Ordering::SeqCst) + 1;
                    (200, format!(r#"{{"access_token":"{prefix}{n:04}","expires_in":3600}}"#))
                } else {
                    (api(first.split(' ').nth(1).unwrap_or_default(), &bearer), "{}".to_string())
                };
                let head = format!("HTTP/1.1 {code} X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", answer.len());
                let _ = stream.write_all(format!("{head}{answer}").as_bytes());
            }
        });
        (base, fetched)
    }

    #[test]
    fn a_401_draws_another_token_once_a_token() {
        let cache = |holder: &str, base: &str| TokenCache {
            holder: holder.into(),
            token_url: format!("{base}/token"),
            client_id: "c".into(),
            client_secret: "s".into(),
            scope: String::new(),
            cached: None,
            refused: HashSet::new(),
            items_refused: HashSet::new(),
        };
        // a route the client may not read: one more token, not one a call
        let (base, fetched) = token_server("Rt7-oauth-loop-", |_, _| 401);
        let mut tc = cache("test:oauth-401-loop", &base);
        for _ in 0..5 {
            assert_eq!(authed_get(&mut tc, &format!("{base}/api"), "/api", false).unwrap().0, 401);
        }
        assert_eq!(fetched.load(Ordering::SeqCst), 2);
        // a token revoked, or past its life: replaced at once
        let (base, fetched) = token_server("Rt7-oauth-revoked-", |_, token| if token.ends_with("-0001") { 401 } else { 200 });
        let mut tc = cache("test:oauth-401-revoked", &base);
        for _ in 0..3 {
            assert_eq!(authed_get(&mut tc, &format!("{base}/api"), "/api", false).unwrap().0, 200);
        }
        assert_eq!(fetched.load(Ordering::SeqCst), 2);
        // a route refused does not keep another from drawing a new token
        let (base, fetched) = token_server("Rt7-oauth-mixed-", |_, token| if token.ends_with("-0002") { 401 } else { 200 });
        let mut tc = cache("test:oauth-401-mixed", &base);
        let refused = |tc: &mut TokenCache| authed_get(tc, &format!("{base}/api"), "/never", false).map(|answer| answer.0);
        tc.refused.insert("/never".into());
        assert_eq!(refused(&mut tc).unwrap(), 200, "token 1 is good");
        assert_eq!(fetched.load(Ordering::SeqCst), 1);
        assert!(tc.refused.is_empty(), "a route that answers otherwise is refused no more");
        tc.cached = None;
        tc.refused.insert("/never".into());
        // token 2 is revoked: /never keeps it, /other replaces it
        assert_eq!(refused(&mut tc).unwrap(), 401);
        assert_eq!(authed_get(&mut tc, &format!("{base}/api"), "/other", false).unwrap().0, 200);
        assert_eq!(fetched.load(Ordering::SeqCst), 3);
        // details answer by item: an item read clears no refusal, and while
        // the token lives no 401 of theirs draws another
        let (base, fetched) = token_server("Rt7-oauth-items-", |path, _| if path.ends_with("/2") || path.ends_with("/4") { 401 } else { 200 });
        let mut tc = cache("test:oauth-401-items", &base);
        for _ in 0..3 {
            for n in 1..=4 {
                let want = if n % 2 == 0 { 401 } else { 200 };
                assert_eq!(authed_get(&mut tc, &format!("{base}/det/{n}"), "/det/{id}", true).unwrap().0, want);
            }
        }
        assert_eq!(fetched.load(Ordering::SeqCst), 2, "the token, and one more for the first 401");
        // another token, as one runs out: their 401 draws one more
        tc.cached = None;
        assert_eq!(authed_get(&mut tc, &format!("{base}/det/2"), "/det/{id}", true).unwrap().0, 401);
        assert_eq!(authed_get(&mut tc, &format!("{base}/det/4"), "/det/{id}", true).unwrap().0, 401);
        assert_eq!(fetched.load(Ordering::SeqCst), 4);
    }

    #[test]
    fn a_token_is_used_no_longer_than_it_lives() {
        assert_eq!(token_life(Some(3600)), Duration::from_secs(3540));
        assert_eq!(token_life(Some(20)), Duration::from_secs(18));
        assert_eq!(token_life(Some(5)), Duration::from_secs(5));
        // a lifetime of 0, or past a day
        assert_eq!(token_life(Some(0)), Duration::from_secs(5));
        assert_eq!(token_life(Some(u64::MAX)), Duration::from_secs(86_340));
        assert_eq!(token_life(None), Duration::from_secs(270));
    }

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
