//! Trace context (ADR-0034, lot A): one business event is one trace, from the
//! source that let it in to the last sink that delivered it.
//!
//! The context is the W3C `traceparent`, with `tracestate` passed on as it was
//! received. It rides every message the runtime publishes as a NATS header
//! (written and read in `bus.rs` only), every outgoing HTTP call of a connector
//! as an HTTP header, and the environment of an exec sink as `TRACEPARENT`. A
//! message that arrives without one starts a new trace, so a foreign producer
//! keeps working as it did and a foreign consumer sees a header it can ignore.
//!
//! Ids come from a splitmix64 sequence seeded once from the OS: unique within
//! a process, unpredictable across processes, and they say nothing about when
//! or where they were made. They are not secrets either: whoever reads the bus
//! reads them.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

/// The header names, as W3C Trace Context spells them. NATS keeps the case a
/// header was written with, so a reader matches them without regard to case.
pub const TRACEPARENT: &str = "traceparent";
pub const TRACESTATE: &str = "tracestate";

/// What one hop hands the next: the trace, the span to hang under, and the
/// sampling decision the entry hop took.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Context {
    pub trace_id: u128,
    pub span_id: u64,
    pub sampled: bool,
    /// `tracestate` as received: a participant passes it on unchanged.
    pub state: Option<String>,
}

impl Context {
    /// Read a `traceparent` (and its `tracestate`). Version 00 exactly; a later
    /// version is read for its first four fields, as the specification asks of
    /// a version-00 reader. Anything else, an all-zero id included, is no
    /// context at all: the hop starts a new trace.
    pub fn parse(traceparent: &str, tracestate: Option<&str>) -> Option<Context> {
        let s = traceparent.trim();
        if !s.is_ascii() || s.len() < 55 {
            return None;
        }
        let b = s.as_bytes();
        let version = &s[0..2];
        if !lower_hex(version) || version == "ff" {
            return None;
        }
        if version == "00" && s.len() != 55 {
            return None;
        }
        if s.len() > 55 && b[55] != b'-' {
            return None;
        }
        if b[2] != b'-' || b[35] != b'-' || b[52] != b'-' {
            return None;
        }
        let (trace, span, flags) = (&s[3..35], &s[36..52], &s[53..55]);
        if !lower_hex(trace) || !lower_hex(span) || !lower_hex(flags) {
            return None;
        }
        let trace_id = u128::from_str_radix(trace, 16).ok()?;
        let span_id = u64::from_str_radix(span, 16).ok()?;
        if trace_id == 0 || span_id == 0 {
            return None;
        }
        let flags = u8::from_str_radix(flags, 16).ok()?;
        // A list over 512 characters may be cut (W3C); dropping it whole is the
        // bluntest cut. The list goes on into NATS and HTTP headers and an exec
        // sink's environment, so it keeps to what the W3C grammar allows there:
        // printable ASCII and tabs. A line break would split a header in two, a
        // NUL would make the exec sink fail to start.
        let state = tracestate
            .map(str::trim)
            .filter(|v| {
                !v.is_empty() && v.len() <= 512 && v.bytes().all(|b| b == b'\t' || (0x20..=0x7e).contains(&b))
            })
            .map(str::to_string);
        Some(Context { trace_id, span_id, sampled: flags & 1 == 1, state })
    }

    pub fn traceparent(&self) -> String {
        format!("00-{:032x}-{:016x}-{:02x}", self.trace_id, self.span_id, self.sampled as u8)
    }
}

fn lower_hex(s: &str) -> bool {
    s.bytes().all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}

/// The span of one hop: its id, the trace it belongs to, and the span it hangs
/// under (none on the hop that starts the trace).
#[derive(Clone, Debug)]
pub struct Hop {
    pub trace_id: u128,
    pub span_id: u64,
    pub parent: Option<u64>,
    pub sampled: bool,
    pub state: Option<String>,
}

impl Hop {
    /// Continue the trace `incoming` names, or start one. Sampling is decided
    /// here: at the entry hop for a new trace, from the parent's flag for one
    /// that continues (with the default, parent-based sampler).
    pub fn begin(incoming: Option<Context>) -> Hop {
        let sampler = sampler();
        match incoming {
            Some(c) => Hop {
                trace_id: c.trace_id,
                span_id: span_id(),
                parent: Some(c.span_id),
                sampled: sampler.continues(c.trace_id, c.sampled),
                state: c.state,
            },
            None => {
                let trace_id = trace_id();
                Hop { trace_id, span_id: span_id(), parent: None, sampled: sampler.starts(trace_id), state: None }
            }
        }
    }

    /// A span under this one, in the same trace (an `invoke`).
    pub fn child(&self) -> Hop {
        Hop {
            trace_id: self.trace_id,
            span_id: span_id(),
            parent: Some(self.span_id),
            sampled: self.sampled,
            state: self.state.clone(),
        }
    }

    /// What this hop hands on, to what it publishes or calls.
    pub fn context(&self) -> Context {
        Context {
            trace_id: self.trace_id,
            span_id: self.span_id,
            sampled: self.sampled,
            state: self.state.clone(),
        }
    }

    pub fn trace_hex(&self) -> String {
        format!("{:032x}", self.trace_id)
    }
}

// ───────────────────────── ids ─────────────────────────

static STATE: OnceLock<AtomicU64> = OnceLock::new();
const GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;

/// splitmix64 over a shared counter: distinct states give distinct outputs, so
/// no id repeats within a process before 2^64 draws.
fn next() -> u64 {
    let state = STATE.get_or_init(|| AtomicU64::new(seed()));
    let mut z = state.fetch_add(GAMMA, Ordering::Relaxed).wrapping_add(GAMMA);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn seed() -> u64 {
    let mut bytes = [0u8; 8];
    if getrandom::getrandom(&mut bytes).is_ok() {
        return u64::from_le_bytes(bytes);
    }
    // No OS randomness: the clock and the pid still keep two runtimes apart.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    nanos ^ ((std::process::id() as u64) << 32)
}

/// A span id, never zero (W3C: an all-zero id is invalid).
pub fn span_id() -> u64 {
    loop {
        let id = next();
        if id != 0 {
            return id;
        }
    }
}

/// A trace id, never zero: its low half is a span id.
pub fn trace_id() -> u128 {
    ((next() as u128) << 64) | span_id() as u128
}

// ───────────────────────── sampling ─────────────────────────

/// `OTEL_TRACES_SAMPLER` and `OTEL_TRACES_SAMPLER_ARG`, read once. The default
/// is the specification's, `parentbased_always_on`. The decision rides the
/// `sampled` flag, so every hop of a trace agrees with the entry hop; errors and
/// dead letters are exported whatever it says (see `metrics::wants`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Sampler {
    /// Follow the parent's flag when there is a parent.
    parent_based: bool,
    /// The share of new traces kept, 0.0 to 1.0.
    ratio: f64,
}

impl Sampler {
    pub fn from_env(name: Option<&str>, arg: Option<&str>) -> Sampler {
        let ratio = || {
            arg.and_then(|a| a.trim().parse::<f64>().ok())
                .filter(|r| (0.0..=1.0).contains(r))
                .unwrap_or(1.0)
        };
        match name.map(str::trim).unwrap_or("parentbased_always_on") {
            "always_on" => Sampler { parent_based: false, ratio: 1.0 },
            "always_off" => Sampler { parent_based: false, ratio: 0.0 },
            "traceidratio" => Sampler { parent_based: false, ratio: ratio() },
            "parentbased_always_off" => Sampler { parent_based: true, ratio: 0.0 },
            "parentbased_traceidratio" => Sampler { parent_based: true, ratio: ratio() },
            "parentbased_always_on" | "" => Sampler { parent_based: true, ratio: 1.0 },
            other => {
                eprintln!("[vejas] OTEL_TRACES_SAMPLER={other:?} is not supported; using parentbased_always_on");
                Sampler { parent_based: true, ratio: 1.0 }
            }
        }
    }

    /// A new trace. The test is the one the OpenTelemetry SDKs make (the low
    /// 64 bits of the trace id against the ratio), so a service downstream with
    /// the same ratio keeps the same traces.
    pub fn starts(&self, trace_id: u128) -> bool {
        if self.ratio >= 1.0 {
            return true;
        }
        if self.ratio <= 0.0 {
            return false;
        }
        let bound = (self.ratio * (1u64 << 63) as f64) as u64;
        ((trace_id as u64) >> 1) < bound
    }

    pub fn continues(&self, trace_id: u128, parent_sampled: bool) -> bool {
        if self.parent_based {
            parent_sampled
        } else {
            self.starts(trace_id)
        }
    }
}

fn sampler() -> Sampler {
    static SAMPLER: OnceLock<Sampler> = OnceLock::new();
    *SAMPLER.get_or_init(|| {
        Sampler::from_env(
            std::env::var("OTEL_TRACES_SAMPLER").ok().as_deref(),
            std::env::var("OTEL_TRACES_SAMPLER_ARG").ok().as_deref(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const TP: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    #[test]
    fn traceparent_round_trips() {
        let c = Context::parse(TP, Some("congo=t61rcWkgMzE")).expect("valid");
        assert_eq!(c.trace_id, 0x4bf92f3577b34da6a3ce929d0e0e4736);
        assert_eq!(c.span_id, 0x00f067aa0ba902b7);
        assert!(c.sampled);
        assert_eq!(c.state.as_deref(), Some("congo=t61rcWkgMzE"));
        assert_eq!(c.traceparent(), TP);
        let unsampled = Context::parse(&TP.replace("-01", "-00"), None).unwrap();
        assert!(!unsampled.sampled);
        assert!(unsampled.traceparent().ends_with("-00"));
    }

    #[test]
    fn a_bad_traceparent_is_no_context() {
        for bad in [
            "",
            "garbage",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-0",
            "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01", // upper case
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01", // zero trace
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01", // zero span
            "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", // forbidden version
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra", // 00 has no tail
            "00_4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-0é", // not ASCII
        ] {
            assert!(Context::parse(bad, None).is_none(), "{bad:?} must not parse");
        }
        // A later version is read for its first four fields.
        let later = "cc-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-what-the-future-holds";
        assert_eq!(Context::parse(later, None).unwrap().span_id, 0x00f067aa0ba902b7);
        // A tracestate that would break the header line is dropped, not passed on.
        assert_eq!(Context::parse(TP, Some("a=1\r\nb: 2")).unwrap().state, None);
        assert_eq!(Context::parse(TP, Some("a=1\0")).unwrap().state, None, "a NUL never reaches an env");
        assert_eq!(Context::parse(TP, Some("a=é")).unwrap().state, None);
        assert_eq!(Context::parse(TP, Some("a=1,\tb=2")).unwrap().state.as_deref(), Some("a=1,\tb=2"));
    }

    #[test]
    fn a_hop_continues_or_starts_a_trace() {
        let incoming = Context::parse(TP, None).unwrap();
        let hop = Hop::begin(Some(incoming.clone()));
        assert_eq!(hop.trace_id, incoming.trace_id);
        assert_eq!(hop.parent, Some(incoming.span_id));
        assert_ne!(hop.span_id, incoming.span_id);
        let child = hop.child();
        assert_eq!((child.trace_id, child.parent), (hop.trace_id, Some(hop.span_id)));
        let root = Hop::begin(None);
        assert_eq!(root.parent, None);
        assert_ne!(root.trace_id, 0);
        assert_ne!(root.trace_id, hop.trace_id);
        assert_eq!(Context::parse(&root.context().traceparent(), None), Some(root.context()));
    }

    #[test]
    fn ids_do_not_repeat() {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..10_000 {
            assert!(seen.insert(span_id()));
        }
    }

    #[test]
    fn the_sampler_follows_the_specification() {
        let default = Sampler::from_env(None, None);
        assert!(default.starts(trace_id()));
        assert!(!default.continues(trace_id(), false), "parent-based: an unsampled parent wins");
        let off = Sampler::from_env(Some("always_off"), None);
        assert!(!off.starts(trace_id()) && !off.continues(trace_id(), true));
        let half = Sampler::from_env(Some("parentbased_traceidratio"), Some("0.5"));
        let kept = (0..10_000).filter(|_| half.starts(trace_id())).count();
        assert!((4_000..6_000).contains(&kept), "kept {kept} of 10000 at 0.5");
        assert!(half.continues(trace_id(), true));
        // Out of range falls back to 1.0, like an unset argument.
        assert_eq!(Sampler::from_env(Some("traceidratio"), Some("7")).ratio, 1.0);
    }
}
