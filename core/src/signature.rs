//! Signed webhooks on `http-in` (ADR-0037, the first driver it calls for).
//!
//! An `http-in` manifest that sets `SIGNATURE` makes each request to its port
//! prove who sent it, before the body is parsed and before anything reaches the
//! bus. The sender signs with a secret the two share: an HMAC-SHA256 over the
//! raw body and, for the schemes that sign one, over a timestamp that must fall
//! within `SIGNATURE_TOLERANCE_SECS` (300) of this server's clock, which bounds
//! how long a captured request can be played again. A request that fails gets
//! 401, is counted on `/metrics`, and is not published.
//!
//! `SIGNATURE` names the sender's scheme, so a manifest does not spell out the
//! header and the encoding:
//!
//! - `github`: `X-Hub-Signature-256: sha256=<hex>` over the body;
//! - `stripe`: `Stripe-Signature: t=<ts>,v1=<hex>` over `<ts>.<body>`, any `v1`
//!   matching (there are two while a secret is rolled);
//! - `slack`: `X-Slack-Signature: v0=<hex>` over `v0:<ts>:<body>`, the
//!   timestamp in `X-Slack-Request-Timestamp`;
//! - `shopify`: `X-Shopify-Hmac-Sha256: <base64>` over the body;
//! - `standard-webhooks`, for Svix and the senders that follow the Standard
//!   Webhooks specification: `webhook-signature: v1,<base64> …` over
//!   `<id>.<ts>.<body>`, from `webhook-id` and `webhook-timestamp` (or their
//!   `svix-` names), with a secret written `whsec_<base64 key>`;
//! - `gitlab`: the secret itself in `X-Gitlab-Token`;
//! - `hmac-sha256`: any other sender that signs the body, its header in
//!   `SIGNATURE_HEADER`, an optional `SIGNATURE_PREFIX` before the signature,
//!   and `SIGNATURE_ENCODING` `hex` (the default) or `base64`;
//! - `token`: the secret itself in `SIGNATURE_HEADER`.
//!
//! The secret is `SIGNING_SECRET`, normally `secret("…")` so that it stays in the
//! secret store. The HMAC of a request is computed once and compared, in
//! constant time, with at most eight candidates of the right length, so a
//! header full of candidates costs no more than one; a token goes through the
//! runtime's constant-time comparison. A manifest that names a key of this
//! module without a scheme the runtime can check does not start: a port meant
//! to be signed never opens without its check.
//!
//! Slack checks a Request URL before it sends events to it, with a signed
//! `url_verification` request whose challenge must come back: [`Verifier::handshake`]
//! answers it, and that request is not published.

use std::sync::atomic::{AtomicU64, Ordering};

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use ring::hmac;
use serde_json::Value;

use crate::connectors::Config;

/// How far a signed timestamp may sit from this server's clock, by default.
const TOLERANCE_SECS: u64 = 300;

/// An HMAC-SHA256 is 32 bytes: a candidate of another length cannot match.
const TAG_LEN: usize = 32;

/// A sender sends one signature, two while a secret is rolled. Past eight, the
/// others are not looked at.
const MAX_CANDIDATES: usize = 8;

/// The shortest `whsec_` key taken, in bytes once decoded.
const MIN_KEY_BYTES: usize = 16;

const SCHEMES: &str =
    "github, stripe, slack, shopify, standard-webhooks, gitlab, hmac-sha256 or token";

/// The `reason` label of `vejas_signature_failures_total`, one per [`Refusal`].
pub const REASONS: [&str; 4] = ["missing", "malformed", "stale", "mismatch"];

enum How {
    GitHub(hmac::Key),
    Stripe(hmac::Key),
    Slack(hmac::Key),
    Shopify(hmac::Key),
    StandardWebhooks(hmac::Key),
    /// HMAC-SHA256 over the body, in a header the manifest names.
    Hmac { key: hmac::Key, header: String, prefix: String, base64: bool },
    /// The shared secret itself, in a header (`gitlab`, `token`).
    Token { header: String, secret: String },
}

/// The signature check of one `http-in` instance, shared by its connections.
pub struct Verifier {
    scheme: &'static str,
    how: How,
    tolerance: u64,
    refused: AtomicU64,
}

/// Why a request was refused. The text goes back to the caller and into the
/// log, so it says what was wrong and never shows a value.
#[derive(Debug, PartialEq, Eq)]
pub enum Refusal {
    /// The header the scheme reads is absent.
    Missing(String),
    /// The header is there and holds nothing that could be a signature.
    Malformed(String),
    /// The signed timestamp is further from this server's clock than the
    /// tolerance, in seconds.
    Stale(u64),
    /// Well formed, in time, and not signed with this secret.
    Mismatch,
}

impl Refusal {
    /// The `reason` label on `/metrics`, one of [`REASONS`].
    pub fn reason(&self) -> &'static str {
        match self {
            Refusal::Missing(_) => REASONS[0],
            Refusal::Malformed(_) => REASONS[1],
            Refusal::Stale(_) => REASONS[2],
            Refusal::Mismatch => REASONS[3],
        }
    }
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Refusal::Missing(header) => write!(f, "no {header} header"),
            Refusal::Malformed(header) => write!(f, "the {header} header does not parse"),
            Refusal::Stale(secs) => write!(
                f,
                "the signed timestamp is more than {secs} s away from this server's clock"
            ),
            Refusal::Mismatch => f.write_str("the signature does not match"),
        }
    }
}

fn malformed(header: &str) -> Refusal {
    Refusal::Malformed(header.to_string())
}

impl Verifier {
    /// The check a manifest asks for, or `None` when it names no key of this
    /// module and the port stays as open as before. Anything the runtime cannot
    /// honour is an error, so the connector does not start rather than take the
    /// unsigned requests it was meant to refuse: a `SIGNATURE` that is not a
    /// known scheme's name, a `SIGNING_SECRET` or `SIGNATURE_…` key without
    /// `SIGNATURE` (a typo leaves the port open otherwise), no secret, a
    /// `whsec_` key too short to be one, a generic scheme without its header.
    pub fn from_config(config: &Config) -> Result<Option<Verifier>, String> {
        let name = match config.value("SIGNATURE") {
            Some(Value::String(name)) => name,
            Some(other) => {
                return Err(format!(
                    "SIGNATURE is a scheme's name in quotes ({SCHEMES}), not {other}"
                ));
            }
            None => {
                if let Some(key) =
                    config.0.keys().find(|k| *k == "SIGNING_SECRET" || k.starts_with("SIGNATURE"))
                {
                    return Err(format!(
                        "{key} is set without SIGNATURE: name the sender's scheme ({SCHEMES})"
                    ));
                }
                return Ok(None);
            }
        };
        let Some(secret) = config.str("SIGNING_SECRET").filter(|s| !s.is_empty()) else {
            return Err(format!(
                "SIGNATURE {name:?} needs SIGNING_SECRET, normally secret(\"…\")"
            ));
        };
        let key = |bytes: &[u8]| hmac::Key::new(hmac::HMAC_SHA256, bytes);
        let header = |scheme: &str| {
            config
                .str("SIGNATURE_HEADER")
                .filter(|h| !h.trim().is_empty())
                .ok_or_else(|| format!("SIGNATURE {scheme:?} needs SIGNATURE_HEADER"))
        };
        let (scheme, how) = match name.to_ascii_lowercase().as_str() {
            "github" => ("github", How::GitHub(key(secret.as_bytes()))),
            "stripe" => ("stripe", How::Stripe(key(secret.as_bytes()))),
            "slack" => ("slack", How::Slack(key(secret.as_bytes()))),
            "shopify" => ("shopify", How::Shopify(key(secret.as_bytes()))),
            "standard-webhooks" | "svix" => {
                let encoded = secret.strip_prefix("whsec_").unwrap_or(&secret);
                let bytes = match STANDARD.decode(encoded) {
                    Ok(bytes) if bytes.len() >= MIN_KEY_BYTES => bytes,
                    Ok(bytes) => {
                        return Err(format!(
                            "SIGNATURE \"standard-webhooks\": the key in SIGNING_SECRET is {} bytes, \
                             and a key is at least {MIN_KEY_BYTES}",
                            bytes.len()
                        ));
                    }
                    Err(_) => {
                        return Err("SIGNATURE \"standard-webhooks\" takes a SIGNING_SECRET written \
                                    whsec_ then base64, and this one does not decode"
                            .into());
                    }
                };
                ("standard-webhooks", How::StandardWebhooks(key(&bytes)))
            }
            "gitlab" => ("gitlab", How::Token { header: "X-Gitlab-Token".into(), secret }),
            "token" => ("token", How::Token { header: header("token")?, secret }),
            "hmac-sha256" => {
                let base64 = match config.str_or("SIGNATURE_ENCODING", "hex").to_ascii_lowercase().as_str() {
                    "hex" => false,
                    "base64" => true,
                    other => {
                        return Err(format!("SIGNATURE_ENCODING {other:?}: hex or base64"));
                    }
                };
                let how = How::Hmac {
                    key: key(secret.as_bytes()),
                    header: header("hmac-sha256")?,
                    prefix: config.str_or("SIGNATURE_PREFIX", ""),
                    base64,
                };
                ("hmac-sha256", how)
            }
            _ => {
                return Err(format!(
                    "SIGNATURE {name:?} is not a scheme this runtime knows: {SCHEMES}"
                ));
            }
        };
        Ok(Some(Verifier {
            scheme,
            how,
            tolerance: config.u64_or("SIGNATURE_TOLERANCE_SECS", TOLERANCE_SECS),
            refused: AtomicU64::new(0),
        }))
    }

    /// The scheme's name, for the log.
    pub fn scheme(&self) -> &'static str {
        self.scheme
    }

    /// Counts one more refused request, and says how many so far.
    pub fn note_refusal(&self) -> u64 {
        self.refused.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Checks one request: its headers through `header` (a name matched without
    /// regard to case), its raw body, and the clock in Unix seconds.
    pub fn check<'h>(
        &self,
        header: impl Fn(&str) -> Option<&'h str>,
        body: &[u8],
        now: u64,
    ) -> Result<(), Refusal> {
        let read = |name: &str| header(name).ok_or_else(|| Refusal::Missing(name.to_string()));
        match &self.how {
            How::GitHub(key) => {
                const H: &str = "X-Hub-Signature-256";
                let found: Vec<_> = read(H)?.strip_prefix("sha256=").and_then(hex_tag).into_iter().collect();
                verify(key, body, &found, H)
            }
            How::Stripe(key) => {
                const H: &str = "Stripe-Signature";
                let value = read(H)?;
                let mut stamp = None;
                let mut found = Vec::new();
                for item in value.split(',') {
                    match item.trim().split_once('=') {
                        Some(("t", t)) => stamp = Some(t),
                        Some(("v1", hex)) if found.len() < MAX_CANDIDATES => found.extend(hex_tag(hex)),
                        _ => {}
                    }
                }
                let Some(stamp) = stamp.filter(|_| !found.is_empty()) else {
                    return Err(malformed(H));
                };
                self.in_time(stamp, now, H)?;
                verify(key, &[stamp.as_bytes(), b".", body].concat(), &found, H)
            }
            How::Slack(key) => {
                const STAMP: &str = "X-Slack-Request-Timestamp";
                const H: &str = "X-Slack-Signature";
                let stamp = read(STAMP)?;
                let found: Vec<_> = read(H)?.strip_prefix("v0=").and_then(hex_tag).into_iter().collect();
                if found.is_empty() {
                    return Err(malformed(H));
                }
                self.in_time(stamp, now, STAMP)?;
                verify(key, &[b"v0:", stamp.as_bytes(), b":", body].concat(), &found, H)
            }
            How::Shopify(key) => {
                const H: &str = "X-Shopify-Hmac-Sha256";
                let found: Vec<_> = base64_tag(read(H)?).into_iter().collect();
                verify(key, body, &found, H)
            }
            How::StandardWebhooks(key) => {
                const H: &str = "webhook-signature";
                // The specification's names, or Svix's for the senders that use them.
                let either = |name: &'static str, svix: &'static str| {
                    header(name).or_else(|| header(svix)).ok_or_else(|| Refusal::Missing(name.into()))
                };
                let id = either("webhook-id", "svix-id")?;
                let stamp = either("webhook-timestamp", "svix-timestamp")?;
                let found: Vec<_> = either(H, "svix-signature")?
                    .split(' ')
                    .filter_map(|s| s.strip_prefix("v1,"))
                    .filter_map(base64_tag)
                    .take(MAX_CANDIDATES)
                    .collect();
                if found.is_empty() {
                    return Err(malformed(H));
                }
                self.in_time(stamp, now, "webhook-timestamp")?;
                let content = [id.as_bytes(), b".", stamp.as_bytes(), b".", body].concat();
                verify(key, &content, &found, H)
            }
            How::Hmac { key, header: name, prefix, base64 } => {
                let encoded = read(name)?.strip_prefix(prefix.as_str()).ok_or_else(|| malformed(name))?;
                let tag = if *base64 { base64_tag(encoded) } else { hex_tag(encoded) };
                verify(key, body, &tag.into_iter().collect::<Vec<_>>(), name)
            }
            How::Token { header: name, secret } => {
                if crate::ct_eq(read(name)?, secret) {
                    Ok(())
                } else {
                    Err(Refusal::Mismatch)
                }
            }
        }
    }

    /// The answer to a sender checking the URL before it sends events, when the
    /// event is that check. Slack posts `{"type":"url_verification","challenge":…}`,
    /// signed like an event, when a Request URL is saved, and wants the challenge
    /// back; such a request is answered and not published. Only asked once the
    /// request's signature has passed and its subject is allowed.
    pub fn handshake(&self, event: &Value) -> Option<String> {
        if !matches!(self.how, How::Slack(_)) {
            return None;
        }
        if event.get("type")?.as_str()? != "url_verification" {
            return None;
        }
        let challenge = event.get("challenge")?.as_str()?;
        Some(serde_json::json!({ "challenge": challenge }).to_string())
    }

    /// Is a signed timestamp (Unix seconds) within the tolerance of `now`, on
    /// either side?
    fn in_time(&self, stamp: &str, now: u64, header: &str) -> Result<(), Refusal> {
        let Ok(stamp) = stamp.parse::<u64>() else {
            return Err(malformed(header));
        };
        if now.abs_diff(stamp) > self.tolerance {
            return Err(Refusal::Stale(self.tolerance));
        }
        Ok(())
    }
}

/// Is one of the candidates the HMAC of `content`? The HMAC is computed once and
/// each candidate compared whole, in constant time; no candidate at all means
/// the header held nothing that could be a signature.
fn verify(key: &hmac::Key, content: &[u8], candidates: &[Vec<u8>], header: &str) -> Result<(), Refusal> {
    if candidates.is_empty() {
        return Err(malformed(header));
    }
    let tag = hmac::sign(key, content);
    let found = candidates
        .iter()
        .fold(false, |found, candidate| crate::ct_eq_bytes(tag.as_ref(), candidate) | found);
    if found {
        Ok(())
    } else {
        Err(Refusal::Mismatch)
    }
}

/// A tag in hex: 64 digits, or nothing (the length is checked before decoding).
fn hex_tag(text: &str) -> Option<Vec<u8>> {
    if text.len() != 2 * TAG_LEN {
        return None;
    }
    unhex(text)
}

/// A tag in base64: 44 characters with their padding, or nothing.
fn base64_tag(text: &str) -> Option<Vec<u8>> {
    if text.len() != TAG_LEN.div_ceil(3) * 4 {
        return None;
    }
    STANDARD.decode(text).ok().filter(|tag| tag.len() == TAG_LEN)
}

/// Hex, either case, two digits a byte and nothing else (no sign, no space).
fn unhex(text: &str) -> Option<Vec<u8>> {
    let bytes = text.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return None;
    }
    bytes
        .chunks(2)
        .map(|pair| {
            let high = char::from(pair[0]).to_digit(16)?;
            let low = char::from(pair[1]).to_digit(16)?;
            u8::try_from(high * 16 + low).ok()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Map};

    fn config(value: Value) -> Config {
        let Value::Object(map) = value else { panic!("a test config is an object") };
        Config(map)
    }

    fn verifier(value: Value) -> Verifier {
        match Verifier::from_config(&config(value)) {
            Ok(Some(v)) => v,
            Ok(None) => panic!("no SIGNATURE in the test config"),
            Err(e) => panic!("{e}"),
        }
    }

    fn check(v: &Verifier, headers: &[(&str, &str)], body: &str, now: u64) -> Result<(), Refusal> {
        v.check(
            |name| headers.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)).map(|(_, value)| *value),
            body.as_bytes(),
            now,
        )
    }

    // The vectors below were computed with Python's `hmac`, apart from the two
    // that come from the senders' own documentation.
    const BODY: &str = r#"{"id":"evt_1","type":"charge.succeeded"}"#;
    const STRIPE_GOOD: &str = "c0475716eb2200f1fe97fcf35a3c05d1fd4f8ba9a07a596730c8d18bb87c0672";

    #[test]
    fn github_the_example_of_its_documentation() {
        let v = verifier(json!({"SIGNATURE": "github", "SIGNING_SECRET": "It's a Secret to Everybody"}));
        let good = "sha256=757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17";
        assert_eq!(check(&v, &[("x-hub-signature-256", good)], "Hello, World!", 0), Ok(()));
        assert_eq!(check(&v, &[("X-Hub-Signature-256", good)], "Hello, World?", 0), Err(Refusal::Mismatch));
        assert_eq!(
            check(&v, &[], "Hello, World!", 0),
            Err(Refusal::Missing("X-Hub-Signature-256".into()))
        );
        let sha1 = "sha1=757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17";
        assert_eq!(check(&v, &[("X-Hub-Signature-256", sha1)], "Hello, World!", 0).map_err(|r| r.reason()), Err("malformed"));
        assert_eq!(check(&v, &[("X-Hub-Signature-256", "sha256=00")], "Hello, World!", 0).map_err(|r| r.reason()), Err("malformed"));
    }

    #[test]
    fn stripe_signs_the_timestamp_and_any_v1_may_match() {
        let v = verifier(json!({"SIGNATURE": "stripe", "SIGNING_SECRET": "whsec_test_secret"}));
        let header = format!("t=1700000000,v1={STRIPE_GOOD},v0=6ffbb59b2300aae63f272406069a9788598b792a944a07aba816edb039989a39");
        assert_eq!(check(&v, &[("Stripe-Signature", &header)], BODY, 1700000100), Ok(()));
        // A secret being rolled: two v1, the first from the old secret.
        let rolled = format!("t=1700000000,v1={},v1={STRIPE_GOOD}", "00".repeat(32));
        assert_eq!(check(&v, &[("Stripe-Signature", &rolled)], BODY, 1700000000), Ok(()));
        // The same signature, the timestamp moved: the timestamp is signed.
        let moved = format!("t=1700000001,v1={STRIPE_GOOD}");
        assert_eq!(check(&v, &[("Stripe-Signature", &moved)], BODY, 1700000000), Err(Refusal::Mismatch));
        assert_eq!(check(&v, &[("Stripe-Signature", &header)], BODY, 1700000301), Err(Refusal::Stale(300)));
        assert_eq!(check(&v, &[("Stripe-Signature", &header)], BODY, 1699999699), Err(Refusal::Stale(300)));
        assert_eq!(
            check(&v, &[("Stripe-Signature", &format!("v1={STRIPE_GOOD}"))], BODY, 1700000000),
            Err(Refusal::Malformed("Stripe-Signature".into()))
        );
        assert_eq!(
            check(&v, &[("Stripe-Signature", "t=1700000000")], BODY, 1700000000).map_err(|r| r.reason()),
            Err("malformed")
        );
    }

    #[test]
    fn a_header_full_of_candidates_costs_one_hmac_and_is_cut_at_eight() {
        let v = verifier(json!({"SIGNATURE": "stripe", "SIGNING_SECRET": "whsec_test_secret"}));
        // Empty and short candidates are not candidates: the good one still counts.
        let padded = format!("t=1700000000,{}v1={STRIPE_GOOD}", "v1=,v1=ab,".repeat(50_000));
        assert_eq!(check(&v, &[("Stripe-Signature", &padded)], BODY, 1700000000), Ok(()));
        // Eight wrong ones of the right length come first: the ninth is not looked at.
        let crowded = format!("t=1700000000,{}v1={STRIPE_GOOD}", format!("v1={},", "00".repeat(32)).repeat(8));
        assert_eq!(check(&v, &[("Stripe-Signature", &crowded)], BODY, 1700000000), Err(Refusal::Mismatch));
        let empty_only = format!("t=1700000000,{}", "v1=,".repeat(50_000));
        assert_eq!(check(&v, &[("Stripe-Signature", &empty_only)], BODY, 1700000000).map_err(|r| r.reason()), Err("malformed"));
        let sw = verifier(json!({"SIGNATURE": "standard-webhooks", "SIGNING_SECRET": "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"}));
        let blanks = "v1, ".repeat(50_000);
        let headers = [("webhook-id", "msg_p5jXN8AQM9LWM0D4loKWxJek"), ("webhook-timestamp", "1614265330"), ("webhook-signature", blanks.as_str())];
        assert_eq!(check(&sw, &headers, "{}", 1614265330).map_err(|r| r.reason()), Err("malformed"));
    }

    #[test]
    fn slack_signs_its_version_and_timestamp() {
        let v = verifier(json!({"SIGNATURE": "slack", "SIGNING_SECRET": "8f742231b10e8888abcd99yyyzzz85a5"}));
        let good = "v0=113d0ad1213849ac6ca740d444007f661b28e56772997c714e30e2e2ba5b6e22";
        let headers = [("X-Slack-Request-Timestamp", "1700000000"), ("X-Slack-Signature", good)];
        assert_eq!(check(&v, &headers, BODY, 1700000000), Ok(()));
        assert_eq!(
            check(&v, &[("X-Slack-Signature", good)], BODY, 1700000000),
            Err(Refusal::Missing("X-Slack-Request-Timestamp".into()))
        );
        let late = [("X-Slack-Request-Timestamp", "1699990000"), ("X-Slack-Signature", good)];
        assert_eq!(check(&v, &late, BODY, 1700000000), Err(Refusal::Stale(300)));
        let words = [("X-Slack-Request-Timestamp", "yesterday"), ("X-Slack-Signature", good)];
        assert_eq!(check(&v, &words, BODY, 1700000000).map_err(|r| r.reason()), Err("malformed"));
    }

    #[test]
    fn slack_gets_its_challenge_back_and_only_slack() {
        let v = verifier(json!({"SIGNATURE": "slack", "SIGNING_SECRET": "8f742231b10e8888abcd99yyyzzz85a5"}));
        let check_url = json!({"token": "t", "challenge": "3eZbrw1aBm2rZgRNFdxV2595E9CY3gmdALWMmHkvFXO7tYXAYM8P",
            "type": "url_verification"});
        assert_eq!(
            v.handshake(&check_url).as_deref(),
            Some(r#"{"challenge":"3eZbrw1aBm2rZgRNFdxV2595E9CY3gmdALWMmHkvFXO7tYXAYM8P"}"#)
        );
        assert_eq!(v.handshake(&json!({"type": "event_callback", "challenge": "x"})), None);
        assert_eq!(v.handshake(&json!({"type": "url_verification"})), None);
        assert_eq!(v.handshake(&json!(["url_verification"])), None);
        let g = verifier(json!({"SIGNATURE": "github", "SIGNING_SECRET": "s"}));
        assert_eq!(g.handshake(&check_url), None);
    }

    #[test]
    fn shopify_sends_base64() {
        let v = verifier(json!({"SIGNATURE": "shopify", "SIGNING_SECRET": "shpss_test_secret"}));
        let good = "RDguc1lakwHUcZUEZGUOS2rRCk2tLneH3KquLdc/QU4=";
        assert_eq!(check(&v, &[("X-Shopify-Hmac-Sha256", good)], BODY, 0), Ok(()));
        assert_eq!(check(&v, &[("X-Shopify-Hmac-Sha256", "not base64!")], BODY, 0).map_err(|r| r.reason()), Err("malformed"));
        assert_eq!(check(&v, &[("X-Shopify-Hmac-Sha256", good)], "{}", 0), Err(Refusal::Mismatch));
    }

    #[test]
    fn standard_webhooks_the_example_of_the_specification() {
        let v = verifier(json!({"SIGNATURE": "standard-webhooks", "SIGNING_SECRET": "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"}));
        let body = r#"{"test": 2432232314}"#;
        let good = "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE=";
        let headers = |signature| {
            [("webhook-id", "msg_p5jXN8AQM9LWM0D4loKWxJek"), ("webhook-timestamp", "1614265330"), ("webhook-signature", signature)]
        };
        assert_eq!(check(&v, &headers(good), body, 1614265330), Ok(()));
        // Several signatures, of other versions too: one v1 that matches is enough.
        let several = format!("v1a,c2lnbmVk v1,AAAA {good}");
        assert_eq!(check(&v, &headers(&several), body, 1614265330), Ok(()));
        let svix = [("svix-id", "msg_p5jXN8AQM9LWM0D4loKWxJek"), ("svix-timestamp", "1614265330"), ("svix-signature", good)];
        assert_eq!(check(&v, &svix, body, 1614265330), Ok(()));
        assert_eq!(check(&v, &headers(good), body, 1614265631), Err(Refusal::Stale(300)));
        assert_eq!(check(&v, &headers("v1a,c2lnbmVk"), body, 1614265330).map_err(|r| r.reason()), Err("malformed"));
        let other_id = [("webhook-id", "msg_other"), ("webhook-timestamp", "1614265330"), ("webhook-signature", good)];
        assert_eq!(check(&v, &other_id, body, 1614265330), Err(Refusal::Mismatch));
    }

    #[test]
    fn tokens_are_compared_as_they_are() {
        let v = verifier(json!({"SIGNATURE": "gitlab", "SIGNING_SECRET": "glpat-hook"}));
        assert_eq!(check(&v, &[("X-Gitlab-Token", "glpat-hook")], BODY, 0), Ok(()));
        assert_eq!(check(&v, &[("X-Gitlab-Token", "glpat-hoo")], BODY, 0), Err(Refusal::Mismatch));
        assert_eq!(check(&v, &[], BODY, 0), Err(Refusal::Missing("X-Gitlab-Token".into())));
        let v = verifier(json!({"SIGNATURE": "token", "SIGNING_SECRET": "s3", "SIGNATURE_HEADER": "X-Hook-Token"}));
        assert_eq!(check(&v, &[("x-hook-token", "s3")], BODY, 0), Ok(()));
    }

    #[test]
    fn hmac_sha256_takes_its_header_prefix_and_encoding_from_the_manifest() {
        let hex = verifier(json!({"SIGNATURE": "hmac-sha256", "SIGNING_SECRET": "generic-secret",
            "SIGNATURE_HEADER": "X-Signature", "SIGNATURE_PREFIX": "sha256="}));
        let good = "sha256=AEB7CABF8DEFDFC35586D1E50444C80121CC792D1BC15CFE273ECCEB83DACE28";
        assert_eq!(check(&hex, &[("X-Signature", good)], BODY, 0), Ok(()));
        assert_eq!(check(&hex, &[("X-Signature", &good[7..])], BODY, 0).map_err(|r| r.reason()), Err("malformed"));
        let b64 = verifier(json!({"SIGNATURE": "hmac-sha256", "SIGNING_SECRET": "generic-secret",
            "SIGNATURE_HEADER": "X-Signature", "SIGNATURE_ENCODING": "base64"}));
        assert_eq!(check(&b64, &[("X-Signature", "rrfKv43v38NVhtHlBETIASHMeS0bwVz+Jz7M64Pazig=")], BODY, 0), Ok(()));
    }

    #[test]
    fn a_manifest_the_runtime_cannot_honour_does_not_start() {
        let err = |value: Value| match Verifier::from_config(&config(value)) {
            Err(e) => e,
            Ok(_) => panic!("expected an error"),
        };
        assert!(err(json!({"SIGNATURE": "github"})).contains("SIGNING_SECRET"));
        assert!(err(json!({"SIGNATURE": "github", "SIGNING_SECRET": ""})).contains("SIGNING_SECRET"));
        assert!(err(json!({"SIGNATURE": "twilio", "SIGNING_SECRET": "x"})).contains("github, stripe"));
        assert!(err(json!({"SIGNATURE": "", "SIGNING_SECRET": "x"})).contains("not a scheme"));
        assert!(err(json!({"SIGNATURE": "hmac-sha256", "SIGNING_SECRET": "x"})).contains("SIGNATURE_HEADER"));
        assert!(err(json!({"SIGNATURE": "token", "SIGNING_SECRET": "x"})).contains("SIGNATURE_HEADER"));
        assert!(err(json!({"SIGNATURE": "hmac-sha256", "SIGNING_SECRET": "x", "SIGNATURE_HEADER": "X-S",
            "SIGNATURE_ENCODING": "base32"})).contains("hex or base64"));
        assert!(err(json!({"SIGNATURE": "standard-webhooks", "SIGNING_SECRET": "whsec_%%%"})).contains("does not decode"));
        // A key that decodes to nothing, or to four bytes, is no key.
        assert!(err(json!({"SIGNATURE": "standard-webhooks", "SIGNING_SECRET": "whsec_"})).contains("0 bytes"));
        assert!(err(json!({"SIGNATURE": "standard-webhooks", "SIGNING_SECRET": "whsec_dGVzdA=="})).contains("4 bytes"));
        // SIGNATURE that is not a name, and the keys that only mean something with it.
        assert!(err(json!({"SIGNATURE": true, "SIGNING_SECRET": "x"})).contains("not true"));
        assert!(err(json!({"SIGNATURE": ["github"], "SIGNING_SECRET": "x"})).contains("in quotes"));
        assert!(err(json!({"SIGNING_SECRET": "x"})).starts_with("SIGNING_SECRET is set without SIGNATURE"));
        assert!(err(json!({"SIGNATURES": "github", "SIGNING_SECRET": "x"})).contains("without SIGNATURE"));
        assert!(err(json!({"SIGNATURE_HEADER": "X-S"})).starts_with("SIGNATURE_HEADER is set without SIGNATURE"));
        assert!(matches!(Verifier::from_config(&Config(Map::new())), Ok(None)));
        assert!(matches!(Verifier::from_config(&config(json!({"PORT": 8787, "ALLOW": ["a"]}))), Ok(None)));
    }

    #[test]
    fn a_refusal_never_shows_the_secret() {
        let secret = "It's a Secret to Everybody";
        let v = verifier(json!({"SIGNATURE": "github", "SIGNING_SECRET": secret}));
        let wrong = format!("sha256={}", "00".repeat(32));
        for headers in [vec![], vec![("X-Hub-Signature-256", "sha256=zz")], vec![("X-Hub-Signature-256", wrong.as_str())]] {
            let Err(refusal) = check(&v, &headers, "Hello, World!", 0) else { panic!("refused") };
            assert!(!refusal.to_string().contains(secret));
        }
        assert_eq!(v.note_refusal(), 1);
        assert_eq!(v.note_refusal(), 2);
    }

    #[test]
    fn tags_have_their_length_before_they_are_decoded() {
        assert_eq!(unhex("00ff7A"), Some(vec![0, 255, 122]));
        assert_eq!(unhex(""), Some(vec![]));
        assert_eq!(unhex("0"), None);
        assert_eq!(unhex("+f"), None);
        assert_eq!(unhex(" f"), None);
        assert_eq!(unhex("gg"), None);
        assert_eq!(hex_tag(""), None);
        assert_eq!(hex_tag(&"ab".repeat(31)), None);
        assert_eq!(hex_tag(&"ab".repeat(32)).map(|t| t.len()), Some(32));
        assert_eq!(base64_tag(""), None);
        assert_eq!(base64_tag("AAAA"), None);
        assert_eq!(base64_tag("RDguc1lakwHUcZUEZGUOS2rRCk2tLneH3KquLdc/QU4=").map(|t| t.len()), Some(32));
    }
}
