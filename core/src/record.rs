//! The trace record (ADR-0034, lot B): each hop the sampler keeps, and each hop
//! that fails, written to the JetStream stream `VEJAS_TRACES` under the subject
//! `vxtrace.<trace id>`. One filtered read returns a whole trace with no tracing
//! backend and no index outside NATS; the panel, `GET /traces/<id>` and the
//! `vejas_trace` MCP tool all read it there.
//!
//! A record is the hop's span, the very one the OTLP exporter sends, plus what a
//! span has no room for: the subjects the hop published to, the payload it
//! received, and what a sink's downstream or an API flow answered.
//!
//! What a record must not carry is scrubbed on this module's thread, never on
//! the hot path. The values of credential-named keys are masked (the panel's
//! mask, `secrets::SECRET_KEY_PATTERN`): in JSON, JSON held in a string read as
//! JSON too, and in text that names a key and gives its value (`key=value`,
//! `"key": value` escaped or not, `'key': value`, `Key: value`, `<key>value`,
//! `--key value`, a URL's userinfo), as it is and once its escapes are read
//! (JSON's, `%XX`, XML's entities). Every value a secret store handed out in
//! this process is taken out of any string, key or number, as it is or once the
//! text's escapes are read. Where a value ends is found on the original text
//! and every cut is made in one pass, so one never hides another from the
//! scrub.
//!
//! The hot path hands the span over a queue bounded in count and in bytes, and
//! goes on. This thread writes each record straight to JSON text, publishes
//! what has gathered once per tick on a NATS connection of its own, and has the
//! stream acknowledge the last record of each tick, which confirms the ones
//! before it. A full queue or a record the stream did not take is lost and
//! counted: a trace never costs a message.
//!
//! What it costs the NATS server is one subject per trace kept, measured at 200
//! to 450 bytes of memory (bench/README.md); the retention limits bound it.

use crate::bus;
use crate::metrics::{self, json_string, Attr, Span};
use crate::vjs::{Step, Steps, What};
use aho_corasick::AhoCorasick;
use serde::de::{self, DeserializeSeed, Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde_json::{json, Value};
use std::borrow::Cow;
use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Write as _};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender};
use std::sync::{Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::{Duration, Instant};

pub const STREAM: &str = "VEJAS_TRACES";
pub const ROOT: &str = "vxtrace";

const QUEUE: usize = 4096;
/// The bytes the queue may hold, whatever its count: a NATS that stalls costs
/// this much memory at most, then hops are dropped and counted.
const QUEUE_BYTES: usize = 32 * 1024 * 1024;
/// While records are waiting, the thread publishes them once per tick; idle, it
/// blocks for the next one. Waking it per record cost the exporter ~20 µs an
/// event (bench/README.md, finding #9), so the hot path never does.
const TICK: Duration = Duration::from_millis(25);
const IDLE_WAIT: Duration = Duration::from_millis(250);
/// How long a stream that is not there waits before it is asked for again.
const RETRY: Duration = Duration::from_secs(5);
/// A payload or an answer over this size is kept as its size and the start of
/// its scrubbed text; the hot path copies no more of it than this.
const INPUT_MAX: usize = 16 * 1024;
const PREVIEW_MAX: usize = 2048;
/// Errors and answers longer than this are cut, once scrubbed.
const TEXT_MAX: usize = 2048;
/// A subject as a record writes it, at most, once scrubbed: one built from an
/// event can be long, and a run's steps can name 256 of them.
const SUBJECT_MAX: usize = 256;


/// The subjects a record names, each scrubbed once and copied from where it
/// was first written: a hop names its own subject twice, and a loop emits to
/// the same one over and over. The first 16 distinct subjects are kept.
/// `out` only grows while they are written, so a place in it stays good.
#[derive(Default)]
struct Subjects<'a> {
    written: Vec<(&'a str, std::ops::Range<usize>)>,
}

impl<'a> Subjects<'a> {
    fn write(&mut self, out: &mut String, subject: &'a str, scrub: &Scrub) {
        if let Some((_, at)) = self.written.iter().find(|(s, _)| *s == subject) {
            out.extend_from_within(at.clone());
            return;
        }
        let start = out.len();
        scrub.write_text(out, subject, false, SUBJECT_MAX);
        if self.written.len() < 16 {
            self.written.push((subject, start..out.len()));
        }
    }

    /// A subject held in an attribute.
    fn write_attr(&mut self, out: &mut String, value: &'a Attr, scrub: &Scrub) {
        match value {
            Attr::Str(s) => self.write(out, s, scrub),
            other => scrub.write_attr(out, other),
        }
    }
}
/// The distinct subjects a record lists, at most; `vejas.emits` keeps the count.
/// A fan-out of thousands would otherwise make a record too big to publish.
const EMITS_MAX: usize = 100;
/// The most hops one read returns: a guard against a runaway trace.
const READ_MAX: usize = 10_000;
/// How many of the latest hops a search looks through, at most.
pub const SEARCH_WINDOW: u64 = 50_000;
const MASKED: &str = "<masked>";
const TAKEN_OUT: &str = "<secret>";

const DEFAULT_MAX_HOPS: i64 = 100_000;
const DEFAULT_MAX_BYTES: i64 = 256 * 1024 * 1024;
const DEFAULT_MAX_AGE_SECS: i64 = 7 * 24 * 3600;

static TX: OnceLock<Option<SyncSender<(Span, Extras)>>> = OnceLock::new();
static PAYLOADS: AtomicBool = AtomicBool::new(true);
static QUEUED: AtomicUsize = AtomicUsize::new(0);
static KEPT: AtomicU64 = AtomicU64::new(0);
static DROPPED_QUEUE: AtomicU64 = AtomicU64::new(0);
static DROPPED_PUBLISH: AtomicU64 = AtomicU64::new(0);
static FLUSH_ASKED: AtomicU64 = AtomicU64::new(0);
static FLUSH_DONE: AtomicU64 = AtomicU64::new(0);

/// What a hop adds to its span in the record.
#[derive(Default)]
pub struct Extras {
    emits: Vec<String>,
    input: Option<Kept>,
    answer: Option<Kept>,
    steps: Option<Steps>,
}

/// A payload or an answer as the hot path hands it over: its first
/// `INPUT_MAX` bytes, and how long it was.
struct Kept {
    bytes: Vec<u8>,
    len: usize,
}

impl Kept {
    fn copy(bytes: &[u8]) -> Kept {
        Kept {
            bytes: bytes[..bytes.len().min(INPUT_MAX)].to_vec(),
            len: bytes.len(),
        }
    }

    fn owned(mut bytes: Vec<u8>) -> Kept {
        let len = bytes.len();
        if len > INPUT_MAX {
            bytes.truncate(INPUT_MAX);
            bytes.shrink_to_fit();
        }
        Kept { bytes, len }
    }

    /// Is this all of it?
    fn whole(&self) -> bool {
        self.bytes.len() == self.len
    }
}

impl Extras {
    pub fn new() -> Extras {
        Extras::default()
    }

    /// The subjects the hop published to.
    pub fn emits(mut self, subjects: Vec<String>) -> Extras {
        self.emits = subjects;
        self
    }

    /// The payload the hop received or let in. Copied, `INPUT_MAX` bytes at
    /// most, only when the record keeps payloads.
    pub fn input(mut self, bytes: &[u8]) -> Extras {
        if payloads() {
            self.input = Some(Kept::copy(bytes));
        }
        self
    }

    /// The same, for a payload the hop holds already decoded.
    pub fn input_value(mut self, value: &Value) -> Extras {
        if payloads() {
            self.input = serde_json::to_vec(value).ok().map(Kept::owned);
        }
        self
    }

    /// What a sink's downstream or an API flow answered, whole: the record
    /// scrubs it like a payload, then keeps `TEXT_MAX` bytes of it.
    pub fn answer(mut self, answer: Option<&str>) -> Extras {
        if payloads() {
            self.answer = answer.map(|a| Kept::copy(a.as_bytes()));
        }
        self
    }

    /// The statements the hop's run went through (ADR-0034, point 8). They
    /// say which lines ran, never the data, so they stay when payloads are off.
    pub fn steps(mut self, steps: Option<Steps>) -> Extras {
        self.steps = steps.filter(|s| !s.is_empty());
        self
    }

    /// What it weighs in the queue, near enough.
    fn weight(&self) -> usize {
        let kept = |k: &Option<Kept>| k.as_ref().map_or(0, |k| k.bytes.len());
        let steps = self.steps.as_ref().map_or(0, |s| {
            let emitted = |step: &Step| match &step.what {
                What::Emit(_, subject) => subject.len(),
                _ => 0,
            };
            (s.len() + s.services().len()) * 64 + s.head().iter().chain(s.tail()).map(emitted).sum::<usize>()
        });
        256 + self.emits.iter().map(|s| s.len() + 24).sum::<usize>() + kept(&self.input) + kept(&self.answer) + steps
    }
}

/// What a hop weighs in the queue: its error waits there with it, and an
/// `exec` sink's is the child's whole stderr.
fn queued_weight(span: &Span, extras: &Extras) -> usize {
    extras.weight() + span.error.as_ref().map_or(0, String::len)
}

/// What the environment asks of the record.
#[derive(Debug, PartialEq)]
struct Config {
    payloads: bool,
    max_hops: i64,
    max_bytes: i64,
    max_age: Duration,
}

/// On unless told otherwise, and said so when the value is neither.
fn switch(name: &str, value: Option<String>) -> bool {
    let Some(value) = value else { return true };
    match value.trim().to_ascii_lowercase().as_str() {
        "" | "on" | "1" | "true" | "yes" => true,
        "off" | "0" | "false" | "no" => false,
        _ => {
            eprintln!("[vejas] {name}={value:?} is neither on nor off; on");
            true
        }
    }
}

fn config(get: impl Fn(&str) -> Option<String>) -> Option<Config> {
    if !switch("VEJAS_TRACES", get("VEJAS_TRACES")) {
        return None;
    }
    let number = |name: &str, default: i64| match get(name) {
        None => default,
        Some(v) => match v.trim().parse::<i64>() {
            Ok(n) if n > 0 => n,
            _ => {
                eprintln!("[vejas] {name}={v:?} is not a positive number; {default}");
                default
            }
        },
    };
    Some(Config {
        payloads: switch("VEJAS_TRACES_PAYLOADS", get("VEJAS_TRACES_PAYLOADS")),
        max_hops: number("VEJAS_TRACES_MAX_HOPS", DEFAULT_MAX_HOPS),
        max_bytes: number("VEJAS_TRACES_MAX_BYTES", DEFAULT_MAX_BYTES),
        max_age: Duration::from_secs(number("VEJAS_TRACES_MAX_AGE_SECS", DEFAULT_MAX_AGE_SECS) as u64),
    })
}

fn stream(config: &Config) -> bus::StreamConfig {
    bus::StreamConfig {
        name: STREAM.to_string(),
        subjects: vec![format!("{ROOT}.>")],
        max_msgs: config.max_hops,
        max_bytes: config.max_bytes,
        max_age: config.max_age,
        discard: bus::Discard::Old,
        ..Default::default()
    }
}

/// Start the record, unless `VEJAS_TRACES=off`. Called once, before any unit
/// starts. Its thread opens a NATS connection of its own and retries until
/// NATS answers; the hops that end meanwhile wait in the queue.
pub fn init(url: &str) {
    let Some(config) = config(|name| std::env::var(name).ok()) else {
        let _ = TX.set(None);
        eprintln!("[vejas] trace record off (VEJAS_TRACES=off)");
        return;
    };
    PAYLOADS.store(config.payloads, Ordering::Relaxed);
    let said = format!(
        "[vejas] trace record -> stream {STREAM}: the last {} hops, {} MB, {} h at most{}",
        config.max_hops,
        config.max_bytes / (1024 * 1024),
        config.max_age.as_secs() / 3600,
        if config.payloads { "" } else { ", without payloads" },
    );
    let (tx, rx) = sync_channel(QUEUE);
    let url = url.to_string();
    let started = std::thread::Builder::new()
        .name("vejas-record".into())
        .spawn(move || run(&url, &config, rx));
    match started {
        Ok(_) => {
            let _ = TX.set(Some(tx));
            eprintln!("{said}");
        }
        Err(e) => {
            let _ = TX.set(None);
            eprintln!("[vejas] trace record off: no thread for it ({e})");
        }
    }
}

/// Is the record on? Call sites test this, with the exporter, before building
/// a hop's span.
pub fn on() -> bool {
    matches!(TX.get(), Some(Some(_)))
}

/// Does the record keep payloads? With `VEJAS_TRACES_PAYLOADS=off` it keeps
/// what each hop did, and its error, but not the data it did it to.
pub fn payloads() -> bool {
    on() && PAYLOADS.load(Ordering::Relaxed)
}

/// The end of a hop: its span to the OTLP exporter and its record to the
/// stream, each when configured. A hop the sampler dropped goes to neither,
/// unless it failed.
pub fn end(span: Span, extras: Extras) {
    if !span.sampled && span.error.is_none() {
        return;
    }
    let Some(Some(tx)) = TX.get() else {
        metrics::span(span);
        return;
    };
    let kept = if metrics::tracing() {
        let copy = span.clone();
        metrics::span(span);
        copy
    } else {
        span
    };
    let weight = queued_weight(&kept, &extras);
    if QUEUED.fetch_add(weight, Ordering::Relaxed) + weight > QUEUE_BYTES {
        QUEUED.fetch_sub(weight, Ordering::Relaxed);
        DROPPED_QUEUE.fetch_add(1, Ordering::Relaxed);
        return;
    }
    if tx.try_send((kept, extras)).is_err() {
        QUEUED.fetch_sub(weight, Ordering::Relaxed);
        DROPPED_QUEUE.fetch_add(1, Ordering::Relaxed);
    }
}

/// Publish what the queue holds, waiting at most `within`. The runtime calls it
/// once its units have stopped, so the last hops of a graceful stop are kept
/// like the others.
pub fn flush(within: Duration) {
    if !on() {
        return;
    }
    let asked = FLUSH_ASKED.fetch_add(1, Ordering::SeqCst) + 1;
    let deadline = Instant::now() + within;
    while FLUSH_DONE.load(Ordering::SeqCst) < asked && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Records the stream acknowledged, records lost because the queue was full,
/// records the stream did not take: for `GET /metrics`.
pub fn counters() -> (u64, u64, u64) {
    (
        KEPT.load(Ordering::Relaxed),
        DROPPED_QUEUE.load(Ordering::Relaxed),
        DROPPED_PUBLISH.load(Ordering::Relaxed),
    )
}

fn run(url: &str, config: &Config, rx: Receiver<(Span, Extras)>) {
    let mut said = false;
    let nc = loop {
        match bus::connect_own(url) {
            Ok(nc) => break nc,
            Err(e) => {
                if !said {
                    eprintln!("[vejas] trace record: NATS not reachable yet ({e}), retrying");
                    said = true;
                }
                std::thread::sleep(Duration::from_secs(2));
            }
        }
    };
    let js = nc.jetstream();
    let wanted = stream(config);
    let mut ready = ensure(&js, &wanted, true);
    let mut retry_at = Instant::now() + RETRY;
    let mut losing = false;
    let mut scrub = Scrub::new();
    let mut batch = nc.batch();
    let mut pending: Vec<(Span, Extras)> = Vec::new();
    loop {
        let flush = FLUSH_ASKED.load(Ordering::SeqCst);
        if pending.is_empty() && flush <= FLUSH_DONE.load(Ordering::SeqCst) {
            match rx.recv_timeout(IDLE_WAIT) {
                Ok(r) => pending.push(r),
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
        while let Ok(r) = rx.try_recv() {
            pending.push(r);
        }
        if !ready && Instant::now() >= retry_at {
            ready = ensure(&js, &wanted, false);
            retry_at = Instant::now() + RETRY;
        }
        if !pending.is_empty() {
            if ready {
                scrub.refresh();
                match publish(&js, &mut batch, &scrub, &mut pending) {
                    Ok(()) => losing = false,
                    Err(e) => {
                        if !losing {
                            eprintln!("[vejas] trace record: the stream did not take the last records ({e})");
                            losing = true;
                        }
                        // gone (a NATS restarted without its store, a stream
                        // removed by hand) or down: ask for it now, then every RETRY
                        ready = ensure(&js, &wanted, false);
                        retry_at = Instant::now() + RETRY;
                    }
                }
            } else {
                // No stream to take them: a publish would vanish unseen.
                for (span, extras) in pending.drain(..) {
                    QUEUED.fetch_sub(queued_weight(&span, &extras), Ordering::Relaxed);
                    DROPPED_PUBLISH.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        if flush > FLUSH_DONE.load(Ordering::SeqCst) {
            FLUSH_DONE.store(flush, Ordering::SeqCst);
        }
        std::thread::sleep(TICK);
    }
}

/// Publish the records of one tick: core publishes behind one barrier, then
/// the last one, which the stream acknowledges. Sent after the others on the
/// same connection, its ack confirms them, and that the stream is there at all.
fn publish(
    js: &bus::JetStream,
    batch: &mut bus::Batch,
    scrub: &Scrub,
    pending: &mut Vec<(Span, Extras)>,
) -> Result<(), String> {
    let mut held = 0u64;
    let mut last: Option<(String, Vec<u8>)> = None;
    for (span, extras) in pending.drain(..) {
        QUEUED.fetch_sub(queued_weight(&span, &extras), Ordering::Relaxed);
        let record = (
            format!("{ROOT}.{:032x}", span.trace_id),
            render(&span, &extras, scrub).into_bytes(),
        );
        if let Some((subject, bytes)) = last.replace(record) {
            match batch.publish(&subject, bytes) {
                Ok(()) => held += 1,
                Err(_) => {
                    DROPPED_PUBLISH.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
    let Some((subject, bytes)) = last else { return Ok(()) };
    let sent = batch.confirm();
    match js.publish(&subject, &bytes) {
        Ok(_) => {
            let before = if sent.is_ok() { held } else { 0 };
            KEPT.fetch_add(before + 1, Ordering::Relaxed);
            DROPPED_PUBLISH.fetch_add(held - before, Ordering::Relaxed);
            Ok(())
        }
        Err(e) => {
            DROPPED_PUBLISH.fetch_add(held + 1, Ordering::Relaxed);
            Err(e)
        }
    }
}

/// Is the stream there, with the limits asked for? Says why not when asked to.
fn ensure(js: &bus::JetStream, wanted: &bus::StreamConfig, say: bool) -> bool {
    match js.put_stream(wanted) {
        Ok(()) => true,
        Err(e) => {
            if say {
                eprintln!("[vejas] trace record: stream {STREAM}: {e}");
            }
            js.stream_state(STREAM).is_ok()
        }
    }
}

/// A key and what separates it from its value, in the shapes a text gives
/// them. Where the value ends is read in code, from where the match ends, so
/// a long one is never half masked. Leftmost first: a URL's scheme before the
/// `Key:` it also looks like.
const PAIRS: &str = concat!(
    // scheme://, then the userinfo when there is one, read in code
    r"(?P<url>[A-Za-z][A-Za-z0-9+.\-]*://)",
    // "key": value, "key" => value or "key" = value: JSON that was cut or does
    // not parse, or JSON escaped once or more inside a string of it; a Ruby
    // hash; HCL
    r#"|(?P<jq>\\*)"(?P<json>(?:[^"\\\r\n]|\\u[[:xdigit:]]{4})+)(?P<jq2>\\*)"\s*(?::|=>|=)\s*"#,
    // 'key': value, a Python dict
    r#"|'(?P<sq>[^'\\\r\n]+)'\s*(?::|=>)\s*"#,
    // ["key"]=> value: PHP's var_dump, a property's visibility after the key
    r#"|\["(?P<vd>[^"\\\r\n]+)"(?::[^\]\r\n]*)?\]=>\s*"#,
    // key=value or key => value: a query, a form, an XML attribute, a PHP array
    // --key value: a command line
    r"|--(?P<flag>[A-Za-z0-9][A-Za-z0-9_\-]*)[ \t]+",
    r"|(?P<eq>[A-Za-z0-9_.%\-\[\]]+)[ \t]*(?:=>|=)[ \t]*",
    // Key: value: a header, YAML
    r"|(?P<colon>[A-Za-z0-9_\-]+)[ \t]*:[ \t]*",
    // <key>value, a namespace prefix allowed
    r"|<(?P<xp>[A-Za-z0-9_.\-]+:)?(?P<xml>[A-Za-z0-9_.\-]+)(?:\s[^<>]*)?>",
);

#[derive(Clone, Copy, PartialEq)]
enum Form {
    Url,
    Json,
    Single,
    VarDump,
    Flag,
    Eq,
    Colon,
    Xml,
}

/// JSON held in a string is read as JSON this many strings deep, and a text's
/// escapes this many levels deep.
const NEST_MAX: u8 = 6;
/// Arrays and objects deeper than this are kept as text: a record holds the
/// payload one level down, and every reader stops at 128.
const DEPTH_MAX: u16 = 100;
/// A text cut before the record saw it loses a value handed out that it ends
/// in the start of, from this many bytes of it.
const PREFIX_MIN: usize = 4;
/// Past this many credentials in one text, the rest of it is masked.
const CREDENTIALS_MAX: usize = 64;

/// What a record must not carry, and how it is taken out.
struct Scrub {
    /// Credential-named keys: the panel's mask (ADR-0017).
    keys: regex::Regex,
    pairs: regex::Regex,
    vault: Vault,
    generation: Option<u64>,
}

/// The values the secret stores handed out, found in one pass however many,
/// in a text as it is and as it reads once its escapes are read.
enum Vault {
    Empty,
    Values {
        finder: AhoCorasick,
        /// The finder's patterns, by index.
        values: Vec<String>,
        longest: usize,
        /// The values by their first bytes: does a text end with the start of one?
        starts: HashMap<[u8; PREFIX_MIN], Vec<usize>>,
        /// The values that read as numbers, by the bits of the float JSON reads
        /// them as: a number too long for an integer arrives as one.
        numbers: HashSet<u64>,
        /// Which of them are JSON documents themselves (an object or an array),
        /// by index: a record must not keep one forwarded as JSON.
        documents: Vec<bool>,
    },
    /// They could not be indexed: every string is kept out, whole.
    Unusable,
}

impl Vault {
    fn of(values: &[String]) -> Vault {
        if values.is_empty() {
            return Vault::Empty;
        }
        let mut seen = HashSet::new();
        let values: Vec<String> = values.iter().filter(|v| seen.insert(v.as_str())).cloned().collect();
        let mut starts: HashMap<[u8; PREFIX_MIN], Vec<usize>> = HashMap::new();
        for (i, value) in values.iter().enumerate() {
            if let Some(head) = value.as_bytes().first_chunk::<PREFIX_MIN>() {
                starts.entry(*head).or_default().push(i);
            }
        }
        // read as JSON reads it: its float parsing is not Rust's, to the last bit
        let numbers = values
            .iter()
            .filter(|v| v.bytes().all(|b| b.is_ascii_digit() || matches!(b, b'-' | b'+' | b'.' | b'e' | b'E')))
            .flat_map(|v| [serde_json::from_str::<f64>(v).ok(), v.parse::<f64>().ok()])
            .flatten()
            .map(f64::to_bits)
            .collect();
        match AhoCorasick::new(&values) {
            Ok(finder) => Vault::Values {
                finder,
                longest: values.iter().map(String::len).max().unwrap_or(0),
                documents: values.iter().map(|v| is_document(v)).collect(),
                values,
                starts,
                numbers,
            },
            Err(e) => {
                eprintln!(
                    "[vejas] trace record: cannot index the values the secret stores handed out ({e}); \
                     records keep every string out"
                );
                Vault::Unusable
            }
        }
    }
}

/// A text as it reads once one level of its escapes is read.
struct View {
    bytes: Vec<u8>,
    /// Where what each byte was read from starts and ends in the original text.
    spans: Vec<(usize, usize)>,
}

/// What starts at a byte that can start an escape.
enum Escape {
    /// No escape: the byte is kept as it is.
    Not,
    /// An escape `len` bytes long, that reads as the first `n` of `reads`.
    Reads { len: usize, reads: [u8; 4], n: usize },
    /// An escape the text was cut inside of.
    Cut,
}

impl Escape {
    fn byte(len: usize, b: u8) -> Escape {
        Escape::Reads { len, reads: [b, 0, 0, 0], n: 1 }
    }

    fn char(len: usize, code: u32) -> Escape {
        match char::from_u32(code) {
            Some(c) => {
                let mut reads = [0; 4];
                let n = c.encode_utf8(&mut reads).len();
                Escape::Reads { len, reads, n }
            }
            None => Escape::Not,
        }
    }
}

enum Hex {
    Value(u32),
    /// The text ends before the digits do, with digits up to its end.
    Short,
    Not,
}

/// The value of the `n` hex digits at `from` in `s`, in either case.
fn hex_at(s: &[u8], from: usize, n: usize) -> Hex {
    let digits = s.get(from..).unwrap_or(&[]);
    let have = digits.iter().take(n).take_while(|b| b.is_ascii_hexdigit()).count();
    if have == n {
        Hex::Value(digits[..n].iter().fold(0, |value, &d| value * 16 + (d as char).to_digit(16).unwrap_or(0)))
    } else if have == digits.len() {
        Hex::Short
    } else {
        Hex::Not
    }
}

/// The escape at `i`, where a `\`, a `%` or a `&` is: JSON's (`\"`, `\\`,
/// `\/`, `\b`, `\f`, `\n`, `\r`, `\t`, and a `\u` with four hex digits, a
/// surrogate pair read as one character) and `\'`, a URL's `%` with two hex
/// digits, or an entity of XML or HTML.
fn escape_at(bytes: &[u8], i: usize) -> Escape {
    let rest = &bytes[i + 1..];
    match bytes[i] {
        b'\\' => match rest.first() {
            None => Escape::Cut,
            // and \' as JavaScript, Python and PHP write it
            Some(&b @ (b'"' | b'\\' | b'/' | b'\'')) => Escape::byte(2, b),
            Some(b'b') => Escape::byte(2, 8),
            Some(b'f') => Escape::byte(2, 12),
            Some(b'n') => Escape::byte(2, b'\n'),
            Some(b'r') => Escape::byte(2, b'\r'),
            Some(b't') => Escape::byte(2, b'\t'),
            Some(b'u') => match hex_at(rest, 1, 4) {
                Hex::Short => Escape::Cut,
                Hex::Not => Escape::Not,
                Hex::Value(high @ 0xD800..=0xDBFF) => {
                    // the low half of the pair must follow
                    let low = &rest[5..];
                    match (low.first(), low.get(1), hex_at(low, 2, 4)) {
                        (None, ..) | (Some(b'\\'), None, _) | (Some(b'\\'), Some(b'u'), Hex::Short) => Escape::Cut,
                        (Some(b'\\'), Some(b'u'), Hex::Value(low @ 0xDC00..=0xDFFF)) => {
                            Escape::char(12, 0x10000 + ((high - 0xD800) << 10) + (low - 0xDC00))
                        }
                        _ => Escape::Not,
                    }
                }
                // a lone low half is no character: kept as it is
                Hex::Value(code) => Escape::char(6, code),
            },
            Some(_) => Escape::Not,
        },
        b'%' => match hex_at(rest, 0, 2) {
            Hex::Value(b) => Escape::byte(3, b as u8),
            Hex::Short => Escape::Cut,
            Hex::Not => Escape::Not,
        },
        b'&' => entity(rest),
        _ => Escape::Not,
    }
}

/// An entity of XML or HTML, from right after its `&`: a named one XML
/// defines, or a character's number.
fn entity(rest: &[u8]) -> Escape {
    const NAMED: [(&[u8], u8); 5] = [(b"amp;", b'&'), (b"lt;", b'<'), (b"gt;", b'>'), (b"quot;", b'"'), (b"apos;", b'\'')];
    for (name, b) in NAMED {
        if rest.starts_with(name) {
            return Escape::byte(1 + name.len(), b);
        }
        if name.starts_with(rest) {
            return Escape::Cut;
        }
    }
    if rest.first() != Some(&b'#') {
        return Escape::Not;
    }
    let (radix, from) = match rest.get(1) {
        Some(b'x' | b'X') => (16, 2),
        _ => (10, 1),
    };
    let digits = &rest[from..];
    let count = digits.iter().take_while(|b| (**b as char).is_digit(radix)).count();
    match digits.get(count) {
        None => Escape::Cut,
        Some(b';') if (1..=7).contains(&count) => {
            let code = digits[..count].iter().fold(0, |value, &d| value * radix + (d as char).to_digit(radix).unwrap_or(0));
            Escape::char(1 + from + count + 1, code)
        }
        Some(_) => Escape::Not,
    }
}

/// `bytes` with one level of escapes read, the bytes around them kept, and an
/// escape the text was cut inside of left out. `spans` places `bytes` in the
/// original text: none for the text itself. None when there is nothing to read.
fn read_escapes(bytes: &[u8], spans: Option<&[(usize, usize)]>) -> Option<View> {
    let escape = |i: usize| match bytes[i] {
        b'\\' | b'%' | b'&' => escape_at(bytes, i),
        _ => Escape::Not,
    };
    let first = (0..bytes.len()).find(|&i| !matches!(escape(i), Escape::Not))?;
    let place = |from: usize, to: usize| match spans {
        None => (from, to),
        Some(s) => (s[from].0, s[to - 1].1),
    };
    let mut view = View { bytes: Vec::with_capacity(bytes.len()), spans: Vec::with_capacity(bytes.len()) };
    view.bytes.extend_from_slice(&bytes[..first]);
    view.spans.extend((0..first).map(|i| place(i, i + 1)));
    let mut i = first;
    while i < bytes.len() {
        match escape(i) {
            Escape::Not => {
                view.bytes.push(bytes[i]);
                view.spans.push(place(i, i + 1));
                i += 1;
            }
            Escape::Reads { len, reads, n } => {
                let span = place(i, i + len);
                view.bytes.extend_from_slice(&reads[..n]);
                view.spans.extend(std::iter::repeat_n(span, n));
                i += len;
            }
            Escape::Cut => break,
        }
    }
    Some(view)
}

/// A reading's bytes as a text of the same length: a byte that is no part of
/// a character reads as `?`, so the places in it stay the reading's.
fn same_length_text(mut bytes: Vec<u8>) -> String {
    let mut from = 0;
    while let Err(e) = std::str::from_utf8(&bytes[from..]) {
        let bad = from + e.valid_up_to();
        let n = e.error_len().unwrap_or(bytes.len() - bad);
        bytes[bad..bad + n].fill(b'?');
        from = bad + n;
    }
    String::from_utf8(bytes).unwrap_or_default()
}

/// How a text reads once its escapes are read, one level after the other,
/// down to `NEST_MAX`: JSON escaped in a string of JSON, a URL percent-encoded
/// in a query. The text itself is not among them.
fn readings(text: &[u8]) -> Vec<View> {
    let mut views: Vec<View> = Vec::new();
    while views.len() < usize::from(NEST_MAX) {
        let next = match views.last() {
            None => read_escapes(text, None),
            Some(view) => read_escapes(&view.bytes, Some(&view.spans)),
        };
        let Some(next) = next else { break };
        views.push(next);
    }
    views
}

/// A form key as it reads, its `%XX` decoded.
fn percent_decoded(key: &str) -> Cow<'_, str> {
    if !key.contains('%') {
        return Cow::Borrowed(key);
    }
    let hex = |b: Option<&u8>| b.and_then(|&b| (b as char).to_digit(16));
    let bytes = key.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match (bytes[i], hex(bytes.get(i + 1)), hex(bytes.get(i + 2))) {
            (b'%', Some(high), Some(low)) => {
                out.push((high * 16 + low) as u8);
                i += 3;
            }
            (b, _, _) => {
                out.push(b);
                i += 1;
            }
        }
    }
    Cow::Owned(String::from_utf8_lossy(&out).into_owned())
}

/// A JSON key as it reads, its escapes decoded.
fn unescaped(key: &str) -> Cow<'_, str> {
    if !key.contains('\\') {
        return Cow::Borrowed(key);
    }
    serde_json::from_str::<String>(&format!("\"{key}\"")).map_or(Cow::Borrowed(key), Cow::Owned)
}

/// Where a text is cut, and what goes in its place.
#[derive(Clone, Copy)]
struct Cut {
    start: usize,
    end: usize,
    with: With,
}

#[derive(Clone, Copy, PartialEq)]
enum With {
    /// A value handed out.
    Secret,
    /// A credential's value.
    Masked,
    /// A credential's value that was a quoted string: masked inside the same
    /// quote, with as many backslashes before each as it had.
    Quoted(usize, u8),
}

/// `text` with each cut replaced, overlapping cuts as one: masked when a
/// credential's value is among them, taken out otherwise.
fn apply(text: &str, mut cuts: Vec<Cut>) -> String {
    for c in &mut cuts {
        c.start = floor_boundary(text, c.start);
        c.end = ceil_boundary(text, c.end);
    }
    cuts.sort_unstable_by_key(|c| (c.start, Reverse(c.end)));
    let mut out = String::with_capacity(text.len());
    let (mut from, mut i) = (0, 0);
    while i < cuts.len() {
        let (first, from_here) = (cuts[i], i);
        let mut end = first.end;
        i += 1;
        while i < cuts.len() && cuts[i].start < end {
            end = end.max(cuts[i].end);
            i += 1;
        }
        let group = &cuts[from_here..i];
        out.push_str(&text[from..first.start]);
        let quoted = group.iter().find_map(|c| match c.with {
            With::Quoted(slashes, quote) if c.start == first.start && c.end == end => Some((slashes, quote)),
            _ => None,
        });
        match quoted {
            Some((slashes, quote)) => {
                let mark = |out: &mut String| {
                    out.extend(std::iter::repeat_n('\\', slashes));
                    out.push(quote as char);
                };
                mark(&mut out);
                out.push_str(MASKED);
                mark(&mut out);
            }
            None if group.iter().any(|c| c.with != With::Secret) => out.push_str(MASKED),
            None => out.push_str(TAKEN_OUT),
        }
        from = end;
    }
    out.push_str(&text[from..]);
    out
}

impl Scrub {
    fn new() -> Scrub {
        Scrub {
            keys: regex::RegexBuilder::new(crate::secrets::SECRET_KEY_PATTERN)
                .case_insensitive(true)
                .build()
                .expect("SECRET_KEY_PATTERN is a valid pattern"),
            pairs: regex::Regex::new(PAIRS).expect("PAIRS is a valid pattern"),
            vault: Vault::Empty,
            generation: None,
        }
    }

    /// Learn the values the stores handed out since the last call. Called
    /// after the queue is drained: a value handed out before a hop ended is
    /// known before its record is written.
    fn refresh(&mut self) {
        let generation = crate::secrets::handed_out_generation();
        if self.generation != Some(generation) {
            self.generation = Some(generation);
            self.vault = Vault::of(&crate::secrets::handed_out());
        }
    }

    fn hides(&self, key: &str) -> bool {
        self.keys.is_match(&crate::secrets::key_as_read(key))
    }

    /// Does the value of a pair's name, `"Authorization"` in
    /// `{"name":"Authorization","value":"Bearer …"}`, name a credential? Only
    /// a name shaped like a key counts: one word, no space, as a header or an
    /// environment variable is, so a product called "Secret Santa" keeps its
    /// price.
    fn names_credential(&self, name: &str) -> bool {
        name.len() <= PAIR_NAME_MAX && !name.contains(char::is_whitespace) && self.hides(name)
    }

    /// Does a JSON text hold, as it is, a value handed out that is a JSON
    /// document itself? Read as JSON, the text would give it in pieces, none of
    /// them a value handed out: it is scrubbed as a text instead.
    fn spans_json(&self, json: &[u8]) -> bool {
        let Vault::Values { finder, documents, .. } = &self.vault else { return false };
        if !documents.contains(&true) {
            return false;
        }
        finder
            .try_find_overlapping_iter(json)
            .map_or(true, |mut found| found.any(|m| documents[m.pattern().as_usize()]))
    }

    /// Does `text` hold a value handed out?
    fn holds_secret(&self, text: &str) -> bool {
        match &self.vault {
            Vault::Empty => false,
            Vault::Unusable => true,
            Vault::Values { finder, .. } => finder.is_match(text),
        }
    }

    /// Where the values handed out are in `text`, as it is and as it reads
    /// once its escapes are read, overlapping ones included. Values nested in
    /// one another (aaaa, aaaaa, ...) over a run of the same byte would match
    /// at every position many times: past four matches a byte, the whole text
    /// goes, which is about what they cover anyway.
    fn vault_cuts(&self, text: &str, views: &[View], cuts: &mut Vec<Cut>) {
        let Vault::Values { finder, .. } = &self.vault else { return };
        let whole = Cut { start: 0, end: text.len(), with: With::Secret };
        let before = cuts.len();
        let read = std::iter::once((text.as_bytes(), None)).chain(views.iter().map(|v| (&v.bytes[..], Some(&v.spans[..]))));
        for (bytes, spans) in read {
            let Ok(found) = finder.try_find_overlapping_iter(bytes) else {
                cuts.truncate(before);
                cuts.push(whole);
                return;
            };
            let (mark, most) = (cuts.len(), 4 * bytes.len() + 64);
            for m in found {
                if cuts.len() - mark >= most {
                    cuts.truncate(before);
                    cuts.push(whole);
                    return;
                }
                let (start, end) = match spans {
                    None => (m.start(), m.end()),
                    Some(s) => (s[m.start()].0, s[m.end() - 1].1),
                };
                cuts.push(Cut { start, end, with: With::Secret });
            }
        }
    }

    /// Where a text that ends in the start of a value handed out, `PREFIX_MIN`
    /// bytes of it or more, as it is or once its escapes are read, starts it;
    /// the text's length when it does not.
    fn vault_start_at_end(&self, text: &str) -> usize {
        let Vault::Values { values, starts, longest, .. } = &self.vault else { return text.len() };
        let start_in = |bytes: &[u8]| {
            (bytes.len().saturating_sub(longest.saturating_sub(1))..bytes.len()).find(|&i| {
                let Some(ids) = bytes.get(i..i + PREFIX_MIN).and_then(|head| starts.get(head)) else { return false };
                let tail = &bytes[i..];
                ids.iter().any(|&id| values[id].len() > tail.len() && values[id].as_bytes().starts_with(tail))
            })
        };
        // a cut inside a character left U+FFFD: what is before it may start a value
        let probe = text.trim_end_matches('\u{FFFD}');
        let mut at = start_in(probe.as_bytes()).unwrap_or(text.len());
        for view in readings(probe.as_bytes()) {
            if let Some(i) = start_in(&view.bytes) {
                at = at.min(view.spans[i].0);
            }
        }
        floor_boundary(text, at)
    }

    /// Where the values of the credential-named keys `text` gives are, and
    /// what replaces each. After a key, the search goes on into its value: a
    /// URL's query, a header line, an XML tag's attributes, and inside a
    /// value already found, since where a header's value ends is a guess and
    /// may run into the next credential's. Overlapping cuts are made as one.
    /// A `cut` text may stop inside a value.
    fn pair_cuts(&self, text: &str, cut: bool, cuts: &mut Vec<Cut>) {
        let bytes = text.as_bytes();
        if !bytes.iter().any(|b| matches!(b, b':' | b'=' | b'<')) {
            return;
        }
        let (mut at, mut found_so_far) = (0, 0);
        while at < bytes.len() {
            let Some(caps) = self.pairs.captures_at(text, at) else { break };
            let whole = caps.get(0).expect("group 0 is the match");
            let (form, key) = [
                ("url", Form::Url),
                ("json", Form::Json),
                ("sq", Form::Single),
                ("vd", Form::VarDump),
                ("flag", Form::Flag),
                ("eq", Form::Eq),
                ("colon", Form::Colon),
                ("xml", Form::Xml),
            ]
            .into_iter()
            .find_map(|(name, form)| caps.name(name).map(|m| (form, m)))
            .expect("one alternative matched");
            let value = whole.end();
            let found = match form {
                Form::Url => userinfo(bytes, value).map(|(start, end)| Cut { start, end, with: With::Masked }),
                Form::Json => {
                    let open = caps.name("jq").map_or(0, |m| m.len());
                    let close = caps.name("jq2").map_or(0, |m| m.len());
                    // as many backslashes on each side, as many as escaping gives
                    let level = (open == close && (open + 1).is_power_of_two()).then(|| (open + 1).trailing_zeros());
                    level
                        .filter(|_| self.hides(&unescaped(key.as_str())))
                        .map(|level| Cut { start: value, end: json_end(bytes, value, level), with: With::Quoted(open, b'"') })
                        .filter(|c| &bytes[c.start..c.end] != b"null")
                }
                Form::Single => self.hides(key.as_str()).then(|| {
                    let with = match bytes.get(value) {
                        Some(&quote @ (b'"' | b'\'')) => With::Quoted(0, quote),
                        _ => With::Masked,
                    };
                    let end = var_dump_end(bytes, value).unwrap_or_else(|| loose_end(bytes, value));
                    Cut { start: value, end, with }
                }),
                Form::VarDump => self.hides(key.as_str()).then(|| Cut {
                    start: value,
                    end: var_dump_end(bytes, value).unwrap_or_else(|| loose_end(bytes, value)),
                    with: With::Masked,
                }),
                Form::Flag => self.hides(key.as_str()).then(|| {
                    let end = match bytes.get(value) {
                        Some(b'"' | b'\'') => quoted_end(bytes, value),
                        _ => value + bytes[value..].iter().take_while(|b| !b.is_ascii_whitespace()).count(),
                    };
                    Cut { start: value, end, with: With::Masked }
                }),
                Form::Eq => self
                    .hides(&percent_decoded(key.as_str()))
                    .then(|| Cut { start: value, end: eq_end(bytes, value), with: With::Masked }),
                Form::Colon => self
                    .hides(key.as_str())
                    .then(|| colon_value(bytes, value))
                    .flatten()
                    .map(|(start, end)| Cut { start, end, with: With::Masked }),
                Form::Xml => {
                    let empty = text[..value].ends_with("/>");
                    (!empty && self.hides(key.as_str())).then(|| {
                        let name = &text[caps.name("xp").map_or(key.start(), |p| p.start())..key.end()];
                        Cut { start: value, end: xml_end(text, value, name, cut), with: With::Masked }
                    })
                }
            };
            if let Some(c) = found {
                // where each value ends is read from its start, empty or not:
                // so many would cost the square of the text
                found_so_far += 1;
                if found_so_far > CREDENTIALS_MAX {
                    cuts.push(Cut { start: c.start, end: bytes.len(), with: With::Masked });
                    return;
                }
                if c.end > c.start {
                    cuts.push(c);
                }
            }
            at = key.end().max(whole.start() + 1);
        }
    }

    /// `text` scrubbed: the values handed out taken out, the values of the
    /// credential-named keys it gives masked. A `cut` text is the start of a
    /// longer one: when it ends in the start of a value handed out, that goes.
    fn scrubbed<'t>(&self, text: &'t str, cut: bool) -> Cow<'t, str> {
        let text = if cut { &text[..self.vault_start_at_end(text)] } else { text };
        let views = readings(text.as_bytes());
        let mut cuts = Vec::new();
        self.vault_cuts(text, &views, &mut cuts);
        self.pair_cuts(text, cut, &mut cuts);
        // a key escaped (an entity, %XX, a JSON string) is a key all the same:
        // what a reading masks covers the escapes it was read from
        for view in &views {
            let read = same_length_text(view.bytes.clone());
            let mut found = Vec::new();
            self.pair_cuts(&read, cut, &mut found);
            for c in found {
                let end = if c.end >= view.spans.len() { text.len() } else { view.spans[c.end - 1].1 };
                cuts.push(Cut { start: view.spans[c.start].0, end, with: With::Masked });
            }
        }
        if cuts.is_empty() {
            Cow::Borrowed(text)
        } else {
            Cow::Owned(apply(text, cuts))
        }
    }

    /// A text as a JSON string, at most `max` bytes once scrubbed.
    fn write_text(&self, out: &mut String, text: &str, cut: bool, max: usize) {
        self.write_text_in(out, text, cut, max, 0);
    }

    /// The same, for a text `nest` JSON documents deep: one that is JSON
    /// itself is read and scrubbed as JSON, until `NEST_MAX`.
    fn write_text_in(&self, out: &mut String, text: &str, cut: bool, max: usize, nest: u8) {
        if matches!(self.vault, Vault::Unusable) {
            json_string(out, TAKEN_OUT);
            return;
        }
        let first = text.trim_start().as_bytes().first();
        if nest < NEST_MAX && matches!(first, Some(b'{' | b'[' | b'"')) && !self.spans_json(text.as_bytes()) {
            let mut json = String::with_capacity(text.len() + 16);
            if !cut && scrubbed_json(text.as_bytes(), &mut json, self, nest + 1) {
                json_string(out, &clip(&json, max));
                return;
            }
            // JSON that stops before its end, cut here or before: read as JSON
            // as far as it goes, so that a pair is masked in it as in the whole
            let text = if cut { &text[..self.vault_start_at_end(text)] } else { text };
            if let Some((keep, closers)) = made_whole(text) {
                let whole = [&text[..keep], closers.as_str()].concat();
                if !self.spans_json(whole.as_bytes()) && scrubbed_json(whole.as_bytes(), &mut json, self, nest + 1) {
                    // what closed it is not shown, unless a mask took it in; a
                    // string masked where it stopped keeps its quotes
                    if let Some(body) = json.strip_suffix(closers.as_str()) {
                        let quoted = closers.starts_with('"')
                            && [MASKED, TAKEN_OUT].iter().any(|m| body.strip_suffix(m).is_some_and(|b| b.ends_with('"')));
                        json.truncate(body.len() + usize::from(quoted));
                    }
                    json_string(out, &clip(&json, max));
                    return;
                }
            }
        }
        json_string(out, &clip(&self.scrubbed(text, cut), max));
    }

    /// An error, as text. One past `INPUT_MAX` is cut there. One that ends in
    /// the start of a value handed out loses it: a connector quotes the start
    /// of a downstream's answer in its errors, cut wherever it fell.
    fn write_error(&self, out: &mut String, error: &str) {
        if error.len() > INPUT_MAX {
            let cut = &error[..floor_boundary(error, INPUT_MAX)];
            return self.write_text_in(out, cut, true, TEXT_MAX, 0);
        }
        self.write_text_in(out, &error[..self.vault_start_at_end(error)], false, TEXT_MAX, 0);
    }

    /// An object key: scrubbed as a text, never read as JSON.
    fn write_key(&self, out: &mut String, key: &str) {
        self.write_text_in(out, key, false, usize::MAX, NEST_MAX);
    }

    /// A number, unless it holds a value handed out: then TAKEN_OUT, as a string.
    fn write_number(&self, out: &mut String, number: fmt::Arguments) {
        let mark = out.len();
        let _ = out.write_fmt(number);
        if self.holds_secret(&out[mark..]) {
            out.truncate(mark);
            json_string(out, TAKEN_OUT);
        }
    }

    /// A float, which a number too long for an integer is read as: its digits
    /// change on the way, so it is compared as a number too.
    fn write_float(&self, out: &mut String, v: f64) {
        if let Vault::Values { numbers, .. } = &self.vault {
            if numbers.contains(&v.to_bits()) {
                json_string(out, TAKEN_OUT);
                return;
            }
        }
        match serde_json::Number::from_f64(v) {
            Some(n) => self.write_number(out, format_args!("{n}")),
            None => out.push_str("null"),
        }
    }

    fn write_attr(&self, out: &mut String, value: &Attr) {
        match value {
            Attr::Str(s) => self.write_text(out, s, false, usize::MAX),
            Attr::Static(s) => json_string(out, s),
            // the runtime's own: a file under the root, a content hash
            Attr::Computed(s) => json_string(out, s),
            Attr::Int(i) => {
                let _ = write!(out, "{i}");
            }
            Attr::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        }
    }
}

/// The password of a URL's userinfo, from right after `scheme://`, or the
/// whole userinfo when it has no password (a token often stands there alone).
/// The userinfo ends at the authority's last `@`, since a password may hold
/// `@`, `#` or `'` written as they are.
fn userinfo(bytes: &[u8], from: usize) -> Option<(usize, usize)> {
    let stop = |b: u8| {
        b.is_ascii_whitespace()
            || matches!(b, b'/' | b'?' | b'"' | b'<' | b'>' | b'\\' | b'{' | b'}' | b'[' | b']' | b'`' | b'|' | b'^')
    };
    let end = bytes[from..].iter().position(|&b| stop(b)).map_or(bytes.len(), |i| from + i);
    let Some(at) = bytes[from..end].iter().rposition(|&b| b == b'@').map(|i| from + i) else {
        // a text cut inside a password, `postgres://app:pa`, and not a port
        let colon = from + bytes[from..end].iter().position(|&b| b == b':')?;
        let rest = &bytes[colon + 1..end];
        let password = end == bytes.len() && !rest.is_empty() && !rest.iter().all(u8::is_ascii_digit);
        return password.then_some((colon + 1, end));
    };
    match bytes[from..at].iter().position(|&b| b == b':') {
        Some(colon) => (from + colon + 1 < at).then_some((from + colon + 1, at)),
        None => (at > from).then_some((from, at)),
    }
}

/// The backslashes right before `i`.
fn backslashes_before(bytes: &[u8], i: usize) -> usize {
    bytes[..i].iter().rev().take_while(|&&b| b == b'\\').count()
}

/// How many times the quote at `i` was escaped as JSON: 0 for a quote of the
/// text itself, 1 for one inside a string of it, and so on. Escaping a text
/// puts a backslash before each quote and doubles the backslashes already
/// there, so a quote escaped n times has 2^n - 1 backslashes before it, plus
/// an even number from the text inside.
fn quote_level(bytes: &[u8], i: usize) -> u32 {
    (backslashes_before(bytes, i) as u64).trailing_ones()
}

/// A JSON text that may stop anywhere, made whole to be read as JSON: how
/// much of it is kept, and what closes it. It closes after a value, after the
/// `[` or `{` that opens one, and inside a string that is a value; a key, a
/// number or a literal it stops in goes, with what led to it since the last
/// value. None when it is not an array or an object as far as it goes, or
/// holds more than one.
fn made_whole(text: &str) -> Option<(usize, String)> {
    let bytes = text.as_bytes();
    let mut at = bytes.iter().take_while(|b| b.is_ascii_whitespace()).count();
    if !matches!(bytes.get(at), Some(b'{' | b'[')) {
        return None;
    }
    // the closer of each array and object open, the outermost first
    let mut open: Vec<u8> = Vec::new();
    let mut key_next = false;
    // where the text can stop and be closed, and how many are open there
    let mut safe = (at, 0);
    while at < bytes.len() {
        match bytes[at] {
            b'{' | b'[' => {
                key_next = bytes[at] == b'{';
                open.push(if key_next { b'}' } else { b']' });
                at += 1;
                safe = (at, open.len());
            }
            close @ (b'}' | b']') => {
                if open.pop() != Some(close) {
                    return None;
                }
                at += 1;
                if open.is_empty() {
                    return bytes[at..].iter().all(u8::is_ascii_whitespace).then(|| (bytes.len(), String::new()));
                }
                safe = (at, open.len());
            }
            b',' => {
                key_next = open.last() == Some(&b'}');
                at += 1;
            }
            b':' => {
                key_next = false;
                at += 1;
            }
            b'"' => match string_at(bytes, at) {
                Ok(end) => {
                    at = end;
                    if !key_next {
                        safe = (at, open.len());
                    }
                }
                Err(keep) if !key_next => {
                    let closers = std::iter::once(b'"').chain(open.iter().rev().copied()).map(char::from).collect();
                    return Some((keep, closers));
                }
                Err(_) => break,
            },
            b if b.is_ascii_whitespace() => at += 1,
            _ => {
                // a number or a literal, which may be cut short however it reads
                let len = bytes[at..]
                    .iter()
                    .take_while(|b| !matches!(b, b',' | b'}' | b']' | b':' | b'"') && !b.is_ascii_whitespace())
                    .count();
                if at + len == bytes.len() {
                    break;
                }
                at += len;
                safe = (at, open.len());
            }
        }
    }
    let (keep, depth) = safe;
    Some((keep, open[..depth].iter().rev().copied().map(char::from).collect()))
}

/// The JSON string that opens at `at`: Ok(where it ends, past its quote), or,
/// when the text stops in it, Err(where it can be closed): before an escape
/// left unfinished, or the first half of a character a pair of escapes gives.
fn string_at(bytes: &[u8], at: usize) -> Result<usize, usize> {
    let mut i = at + 1;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => return Ok(i + 1),
            b'\\' => {
                let len = if bytes.get(i + 1) == Some(&b'u') { 6 } else { 2 };
                let high = len == 6
                    && matches!(bytes.get(i + 2), Some(b'd' | b'D'))
                    && matches!(bytes.get(i + 3), Some(b'8' | b'9' | b'a' | b'b' | b'A' | b'B'));
                if i + len > bytes.len() || (high && i + 2 * len > bytes.len()) {
                    return Err(i);
                }
                i += len;
            }
            _ => i += 1,
        }
    }
    Err(bytes.len())
}

/// Where the JSON value at `at` ends, in JSON escaped `level` times, in a text
/// that may stop before it does. A quote of a lower level ends it too: the
/// string around it closed first.
fn json_end(bytes: &[u8], at: usize, level: u32) -> usize {
    let lead = (1usize << level.min(16)) - 1;
    // from the quote that opens a string, past the one that closes it
    let string_end = |open: usize| -> usize {
        let mut i = open + 1;
        while let Some(off) = bytes[i..].iter().position(|&b| b == b'"') {
            let q = i + off;
            let found = quote_level(bytes, q);
            if found == level {
                return q + 1;
            }
            if found < level {
                return q - backslashes_before(bytes, q);
            }
            i = q + 1;
        }
        bytes.len()
    };
    let opens_string = bytes.get(at + lead) == Some(&b'"') && bytes[at..at + lead].iter().all(|&b| b == b'\\');
    if opens_string {
        return string_end(at + lead);
    }
    match bytes.get(at) {
        None => at,
        Some(b'{' | b'[') => {
            let (mut depth, mut i) = (0usize, at);
            while i < bytes.len() {
                match bytes[i] {
                    b'"' => {
                        let found = quote_level(bytes, i);
                        if found == level {
                            i = string_end(i);
                            continue;
                        }
                        if found < level {
                            return i - backslashes_before(bytes, i);
                        }
                    }
                    b'{' | b'[' => depth += 1,
                    b'}' | b']' => {
                        depth -= 1;
                        if depth == 0 {
                            return i + 1;
                        }
                    }
                    _ => {}
                }
                i += 1;
            }
            bytes.len()
        }
        Some(_) => bytes[at..]
            .iter()
            .position(|&b| matches!(b, b',' | b'}' | b']' | b'"' | b'\\') || b.is_ascii_whitespace())
            .map_or(bytes.len(), |i| at + i),
    }
}

/// The end of a string opened by the quote at `open`, a backslash escaping
/// the byte after it; the text's end if it does not close.
fn quoted_end(bytes: &[u8], open: usize) -> usize {
    let quote = bytes[open];
    let mut i = open + 1;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => i += 2,
            b if b == quote => return i + 1,
            _ => i += 1,
        }
    }
    bytes.len()
}

/// The end of the value at `at` after a Python or a Ruby key: a quoted string,
/// a bracketed value with its strings skipped, or a word.
fn loose_end(bytes: &[u8], at: usize) -> usize {
    // a Python string with a prefix: b'…', r"…", rb'…'
    let prefix = bytes[at.min(bytes.len())..]
        .iter()
        .take(2)
        .take_while(|b| matches!(b, b'b' | b'B' | b'r' | b'R' | b'u' | b'U' | b'f' | b'F'))
        .count();
    if prefix > 0 && matches!(bytes.get(at + prefix), Some(b'"' | b'\'')) {
        return quoted_end(bytes, at + prefix);
    }
    match bytes.get(at) {
        None => at,
        Some(b'"' | b'\'') => quoted_end(bytes, at),
        Some(b'{' | b'[' | b'(') => {
            let (mut depth, mut i) = (0usize, at);
            while i < bytes.len() {
                match bytes[i] {
                    b'"' | b'\'' => {
                        i = quoted_end(bytes, i);
                        continue;
                    }
                    b'{' | b'[' | b'(' => depth += 1,
                    b'}' | b']' | b')' => {
                        depth -= 1;
                        if depth == 0 {
                            return i + 1;
                        }
                    }
                    _ => {}
                }
                i += 1;
            }
            bytes.len()
        }
        Some(_) => bytes[at..]
            .iter()
            .position(|&b| matches!(b, b',' | b'}' | b']' | b')') || b.is_ascii_whitespace())
            .map_or(bytes.len(), |i| at + i),
    }
}

/// The end of a value after `key=`: a quoted string, or words up to a
/// separator (`&`, `;`, `,`, a quote, an angle bracket, the line's end). A
/// space ends it only before the next key (`word=` or `word:`) or a closing
/// bracket: a connection string gives `Password=my pass;`, a header
/// `Authorization=Bearer abc`, PHP's print_r `[password] => abc )`.
fn eq_end(bytes: &[u8], at: usize) -> usize {
    if matches!(bytes.get(at), Some(b'"' | b'\'')) {
        return quoted_end(bytes, at);
    }
    let stop = |b: u8| matches!(b, b'&' | b';' | b',' | b'"' | b'\'' | b'<' | b'>' | b'\r' | b'\n');
    let blank = |b: u8| matches!(b, b' ' | b'\t');
    let mut end = at;
    loop {
        end += bytes[end..].iter().take_while(|&&b| !stop(b) && !blank(b)).count();
        let next = end + bytes[end..].iter().take_while(|&&b| blank(b)).count();
        if next == end || next == bytes.len() || stop(bytes[next]) || matches!(bytes[next], b')' | b']' | b'}') {
            return end;
        }
        let word = next + bytes[next..].iter().take_while(|&&b| !stop(b) && !blank(b) && !matches!(b, b'=' | b':')).count();
        if word == next || matches!(bytes.get(word), Some(b'=' | b':')) {
            return end;
        }
        end = word;
    }
}

/// Where the value after `Key:` starts and ends: the line, a quoted string, or
/// the next line when this one is left empty and that one is indented (YAML).
/// A quote that closes a JSON string around the text ends the line: one
/// followed by `}`, `]`, the end, or a comma and the next key's quote. None
/// when there is no value.
fn colon_value(bytes: &[u8], at: usize) -> Option<(usize, usize)> {
    let line_end = |from: usize| {
        bytes[from..].iter().position(|&b| matches!(b, b'\r' | b'\n')).map_or(bytes.len(), |i| from + i)
    };
    match bytes.get(at)? {
        b'"' | b'\'' => Some((at, quoted_end(bytes, at))),
        b'\r' | b'\n' => {
            let next = at + bytes[at..].iter().take_while(|&&b| matches!(b, b'\r' | b'\n')).count();
            let start = next + bytes[next..].iter().take_while(|&&b| matches!(b, b' ' | b'\t')).count();
            (start > next && start < bytes.len()).then(|| (start, line_end(start)))
        }
        b'|' | b'>' if block_header(&bytes[at..line_end(at)]) => {
            // a YAML block scalar: the lines after it, blank or indented deeper than the key's
            let line_start = bytes[..at].iter().rposition(|&b| b == b'\n').map_or(0, |i| i + 1);
            let indent = |i: usize| bytes[i..].iter().take_while(|&&b| matches!(b, b' ' | b'\t')).count();
            let key_indent = indent(line_start);
            let (mut end, mut i) = (line_end(at), line_end(at));
            while i < bytes.len() {
                let next = i + if bytes[i] == b'\r' && bytes.get(i + 1) == Some(&b'\n') { 2 } else { 1 };
                let e = line_end(next);
                let blank = next + indent(next) == e;
                if !blank && indent(next) <= key_indent {
                    break;
                }
                end = e;
                i = e;
            }
            Some((at, end))
        }
        _ => {
            let end = line_end(at);
            let after = |i: usize| bytes[i..].iter().position(|&b| !matches!(b, b' ' | b'\t')).map(|n| i + n);
            let closes = |q: usize| match after(q + 1) {
                None => true,
                Some(n) => match bytes[n] {
                    b'}' | b']' => true,
                    b',' => after(n + 1).is_some_and(|k| {
                        let quote = k + bytes[k..].iter().take_while(|&&b| b == b'\\').count();
                        bytes.get(quote) == Some(&b'"')
                    }),
                    _ => false,
                },
            };
            Some((
                at,
                match (at..end).find(|&i| bytes[i] == b'"' && closes(i)) {
                    Some(q) => q - backslashes_before(bytes, q),
                    None => end,
                },
            ))
        }
    }
}

/// The end of a value as PHP's var_dump writes it, at `at`: `string(7) "…"`
/// (seven bytes, whatever they are), `int(5)`, or an `array(1) {` or an
/// `object(C)#1 (1) {` up to the `}` indented as the line it starts on.
fn var_dump_end(bytes: &[u8], at: usize) -> Option<usize> {
    let rest = &bytes[at..];
    let open = rest.iter().take(8).position(|&b| b == b'(')?;
    let kind = &rest[..open];
    if !matches!(kind, b"string" | b"int" | b"float" | b"bool" | b"array" | b"object" | b"enum") {
        return None;
    }
    let close = open + rest[open..].iter().position(|&b| b == b')')?;
    let line = rest.iter().position(|&b| b == b'\n').unwrap_or(rest.len());
    if kind == b"string" {
        let n: usize = std::str::from_utf8(&rest[open + 1..close]).ok()?.parse().ok()?;
        // `) "`, n bytes, `"`
        return Some(at + close.saturating_add(3).saturating_add(n).saturating_add(1).min(rest.len()));
    }
    if !rest[..line].trim_ascii_end().ends_with(b"{") {
        return Some(at + line);
    }
    let line_start = bytes[..at].iter().rposition(|&b| b == b'\n').map_or(0, |i| i + 1);
    let indent = bytes[line_start..].iter().take_while(|&&b| b == b' ').count();
    let mut closing = vec![b'\n'];
    closing.extend(std::iter::repeat_n(b' ', indent));
    closing.push(b'}');
    Some(rest.windows(closing.len()).position(|w| w == &closing[..]).map_or(bytes.len(), |i| at + i + closing.len()))
}

/// Is `value` a JSON object or array, such as a service account's key file?
fn is_document(value: &str) -> bool {
    matches!(value.trim_start().as_bytes().first(), Some(b'{' | b'['))
        && serde_json::from_str::<IgnoredAny>(value).is_ok()
}

/// `|`, `>`, `|-`, `>+2`… and nothing else on the line but a comment.
fn block_header(line: &[u8]) -> bool {
    let rest = &line[1..];
    let marks = rest.iter().take_while(|b| matches!(b, b'-' | b'+' | b'0'..=b'9')).count();
    let rest = &rest[marks..];
    let rest = &rest[rest.iter().take_while(|&&b| matches!(b, b' ' | b'\t')).count()..];
    marks <= 2 && (rest.is_empty() || rest[0] == b'#')
}

/// The end of the value of the XML element `name` whose start tag ends at
/// `at`: up to its end tag, CDATA and child elements included. Without one,
/// the rest of a text that was cut, or up to the next tag.
fn xml_end(text: &str, at: usize, name: &str, cut: bool) -> usize {
    let bytes = text.as_bytes();
    let mut i = at;
    while let Some(off) = text[i..].find("</") {
        let tag = i + off + 2;
        if text[tag..].starts_with(name)
            && matches!(bytes.get(tag + name.len()), Some(b'>' | b' ' | b'\t' | b'\r' | b'\n'))
        {
            return i + off;
        }
        i = tag;
    }
    if cut {
        return bytes.len();
    }
    if text[at..].starts_with("<![CDATA[") {
        return text[at..].find("]]>").map_or(bytes.len(), |n| at + n + 3);
    }
    text[at..].find('<').map_or(bytes.len(), |n| at + n)
}

fn floor_boundary(text: &str, mut i: usize) -> usize {
    i = i.min(text.len());
    while !text.is_char_boundary(i) {
        i -= 1;
    }
    i
}

fn ceil_boundary(text: &str, mut i: usize) -> usize {
    i = i.min(text.len());
    while !text.is_char_boundary(i) {
        i += 1;
    }
    i
}

/// At most `max` bytes of `text`, cut on a character boundary.
fn clip(text: &str, max: usize) -> Cow<'_, str> {
    if text.len() <= max {
        return Cow::Borrowed(text);
    }
    Cow::Owned(format!("{}…", &text[..floor_boundary(text, max)]))
}

/// A JSON value read and written again, scrubbed on the way, with no tree in
/// between. False, and `out` as it was, if `bytes` are not one JSON value or
/// nest deeper than `DEPTH_MAX`. `nest` counts the JSON documents it is
/// inside, as a string of each.
fn scrubbed_json(bytes: &[u8], out: &mut String, scrub: &Scrub, nest: u8) -> bool {
    let mark = out.len();
    let mut de = serde_json::Deserializer::from_slice(bytes);
    let ok = Json { out: &mut *out, scrub, depth: 0, nest }.deserialize(&mut de).is_ok() && de.end().is_ok();
    if !ok {
        out.truncate(mark);
    }
    ok
}

struct Json<'a> {
    out: &'a mut String,
    scrub: &'a Scrub,
    depth: u16,
    nest: u8,
}

impl Json<'_> {
    /// For a value inside this array or object.
    fn inner(&mut self) -> Json<'_> {
        Json { out: &mut *self.out, scrub: self.scrub, depth: self.depth + 1, nest: self.nest }
    }
}

impl<'de> DeserializeSeed<'de> for Json<'_> {
    type Value = ();
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<(), D::Error> {
        d.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for Json<'_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a JSON value")
    }
    fn visit_unit<E: de::Error>(self) -> Result<(), E> {
        self.out.push_str("null");
        Ok(())
    }
    fn visit_bool<E: de::Error>(self, v: bool) -> Result<(), E> {
        self.out.push_str(if v { "true" } else { "false" });
        Ok(())
    }
    fn visit_i64<E: de::Error>(self, v: i64) -> Result<(), E> {
        self.scrub.write_number(self.out, format_args!("{v}"));
        Ok(())
    }
    fn visit_u64<E: de::Error>(self, v: u64) -> Result<(), E> {
        self.scrub.write_number(self.out, format_args!("{v}"));
        Ok(())
    }
    fn visit_f64<E: de::Error>(self, v: f64) -> Result<(), E> {
        self.scrub.write_float(self.out, v);
        Ok(())
    }
    fn visit_str<E: de::Error>(self, v: &str) -> Result<(), E> {
        self.scrub.write_text_in(self.out, v, false, usize::MAX, self.nest);
        Ok(())
    }
    fn visit_seq<A: SeqAccess<'de>>(mut self, mut seq: A) -> Result<(), A::Error> {
        if self.depth >= DEPTH_MAX {
            return Err(de::Error::custom("nested too deep for a record"));
        }
        self.out.push('[');
        let mut comma = false;
        while seq.next_element_seed(Element { to: self.inner(), comma })?.is_some() {
            comma = true;
        }
        self.out.push(']');
        Ok(())
    }
    fn visit_map<A: MapAccess<'de>>(mut self, mut map: A) -> Result<(), A::Error> {
        if self.depth >= DEPTH_MAX {
            return Err(de::Error::custom("nested too deep for a record"));
        }
        self.out.push('{');
        let mut comma = false;
        // A pair, {"name":"Authorization","value":"Bearer …"}, names its
        // credential in a value, not in a key: a header list, an environment
        // list, tags. A `value` is written as it comes, and masked when the
        // object is read whole if its `name` named a credential, in whichever
        // order the two came.
        let mut named = false;
        let mut values = Vec::new();
        while let Some(key) = map.next_key_seed(KeySeed)? {
            if comma {
                self.out.push(',');
            }
            comma = true;
            let key = key.as_str();
            self.scrub.write_key(self.out, key);
            self.out.push(':');
            if self.scrub.hides(key) {
                map.next_value_seed(Hidden(&mut *self.out))?;
            } else if PAIR_NAMES.iter().any(|name| key.eq_ignore_ascii_case(name)) {
                named |= map.next_value_seed(Name(self.inner()))?;
            } else if PAIR_VALUES.iter().any(|value| key.eq_ignore_ascii_case(value)) {
                let from = self.out.len();
                map.next_value_seed(self.inner())?;
                values.push(from..self.out.len());
            } else {
                map.next_value_seed(self.inner())?;
            }
        }
        if named {
            let mut masked = String::new();
            json_string(&mut masked, MASKED);
            // From the last, so that the earlier places stay where they are.
            for place in values.into_iter().rev() {
                if &self.out[place.clone()] != "null" {
                    self.out.replace_range(place, &masked);
                }
            }
        }
        self.out.push('}');
        Ok(())
    }
}

/// The keys that name the other half of a pair, and the keys of that half
/// (HAR, Kubernetes and ECS environments, AWS tags and parameters, Postman).
const PAIR_NAMES: [&str; 6] = ["name", "key", "header", "field", "parameterkey", "parametername"];
const PAIR_VALUES: [&str; 2] = ["value", "parametervalue"];

/// The longest pair name read as a key: past it, a name is a text.
const PAIR_NAME_MAX: usize = 128;

/// The value of a key that names the other half of a pair: written as any value
/// is, and read for whether it names a credential.
struct Name<'a>(Json<'a>);

impl<'de> DeserializeSeed<'de> for Name<'_> {
    type Value = bool;
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<bool, D::Error> {
        d.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for Name<'_> {
    type Value = bool;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a JSON value")
    }
    fn visit_unit<E: de::Error>(self) -> Result<bool, E> {
        self.0.visit_unit().map(|()| false)
    }
    fn visit_bool<E: de::Error>(self, v: bool) -> Result<bool, E> {
        self.0.visit_bool(v).map(|()| false)
    }
    fn visit_i64<E: de::Error>(self, v: i64) -> Result<bool, E> {
        self.0.visit_i64(v).map(|()| false)
    }
    fn visit_u64<E: de::Error>(self, v: u64) -> Result<bool, E> {
        self.0.visit_u64(v).map(|()| false)
    }
    fn visit_f64<E: de::Error>(self, v: f64) -> Result<bool, E> {
        self.0.visit_f64(v).map(|()| false)
    }
    fn visit_str<E: de::Error>(self, v: &str) -> Result<bool, E> {
        let named = self.0.scrub.names_credential(v);
        self.0.visit_str(v).map(|()| named)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, seq: A) -> Result<bool, A::Error> {
        self.0.visit_seq(seq).map(|()| false)
    }
    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<bool, A::Error> {
        self.0.visit_map(map).map(|()| false)
    }
}

/// An element of an array, after a comma unless it is the first.
struct Element<'a> {
    to: Json<'a>,
    comma: bool,
}

impl<'de> DeserializeSeed<'de> for Element<'_> {
    type Value = ();
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<(), D::Error> {
        if self.comma {
            self.to.out.push(',');
        }
        d.deserialize_any(self.to)
    }
}

/// An object key, borrowed from the input unless it held an escape.
enum Key<'de> {
    Borrowed(&'de str),
    Owned(String),
}

impl Key<'_> {
    fn as_str(&self) -> &str {
        match self {
            Key::Borrowed(s) => s,
            Key::Owned(s) => s,
        }
    }
}

struct KeySeed;

impl<'de> DeserializeSeed<'de> for KeySeed {
    type Value = Key<'de>;
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Key<'de>, D::Error> {
        d.deserialize_str(KeySeed)
    }
}

impl<'de> Visitor<'de> for KeySeed {
    type Value = Key<'de>;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("an object key")
    }
    fn visit_borrowed_str<E: de::Error>(self, v: &'de str) -> Result<Key<'de>, E> {
        Ok(Key::Borrowed(v))
    }
    fn visit_str<E: de::Error>(self, v: &str) -> Result<Key<'de>, E> {
        Ok(Key::Owned(v.to_string()))
    }
}

/// The value of a credential-named key: read through, written as MASKED. A
/// null stays null: there is nothing to hide in it.
struct Hidden<'a>(&'a mut String);

impl Hidden<'_> {
    fn masked<E>(self) -> Result<(), E> {
        json_string(self.0, MASKED);
        Ok(())
    }
}

impl<'de> DeserializeSeed<'de> for Hidden<'_> {
    type Value = ();
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<(), D::Error> {
        d.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for Hidden<'_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a JSON value")
    }
    fn visit_unit<E: de::Error>(self) -> Result<(), E> {
        self.0.push_str("null");
        Ok(())
    }
    fn visit_bool<E: de::Error>(self, _: bool) -> Result<(), E> {
        self.masked()
    }
    fn visit_i64<E: de::Error>(self, _: i64) -> Result<(), E> {
        self.masked()
    }
    fn visit_u64<E: de::Error>(self, _: u64) -> Result<(), E> {
        self.masked()
    }
    fn visit_f64<E: de::Error>(self, _: f64) -> Result<(), E> {
        self.masked()
    }
    fn visit_str<E: de::Error>(self, _: &str) -> Result<(), E> {
        self.masked()
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        while seq.next_element::<IgnoredAny>()?.is_some() {}
        self.masked()
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
        self.masked()
    }
}

/// A payload as the record keeps it, as the value of "input": JSON scrubbed in
/// place, anything else as scrubbed text, and past `INPUT_MAX` only its size
/// and the start of its scrubbed text.
fn payload(out: &mut String, input: &Kept, scrub: &Scrub) {
    if input.whole() {
        if scrub.spans_json(&input.bytes) || !scrubbed_json(&input.bytes, out, scrub, 0) {
            // not JSON, or too deep to keep as JSON: a text
            scrub.write_text_in(out, &String::from_utf8_lossy(&input.bytes), false, usize::MAX, NEST_MAX);
        }
        return;
    }
    let _ = write!(out, r#"{{"truncated":true,"bytes":{},"preview":"#, input.len);
    scrub.write_text(out, &String::from_utf8_lossy(&input.bytes), true, PREVIEW_MAX);
    out.push('}');
}

/// What a sink's downstream or an API flow answered, as a text: JSON scrubbed
/// as JSON, at most `TEXT_MAX` bytes once scrubbed.
fn answer(out: &mut String, answer: &Kept, scrub: &Scrub) {
    scrub.write_text(out, &String::from_utf8_lossy(&answer.bytes), !answer.whole(), TEXT_MAX);
}

// ───────────────────────── what other surfaces show ─────────────────────────

/// The record's masks, for what pages and agents are shown of the bus outside
/// it (ADR-0017): the events ring, the dead letters, a unit's last error,
/// shadow replays, the evidence of proposals. An instance of its own,
/// refreshed as the stores hand values out.
static SHOWN: OnceLock<Mutex<Scrub>> = OnceLock::new();

/// The masks, ready for one answer: one lock however many texts it holds.
pub struct Shown(MutexGuard<'static, Scrub>);

/// The masks as they stand: the values the stores handed out so far included.
pub fn shown() -> Shown {
    let mut scrub = SHOWN.get_or_init(|| Mutex::new(Scrub::new())).lock().unwrap_or_else(PoisonError::into_inner);
    scrub.refresh();
    Shown(scrub)
}

impl Shown {
    /// A text: JSON scrubbed as JSON, anything else as text. A `cut` one is
    /// the start of a longer text (a preview, the summary of an answer): it
    /// loses the start of a value handed out that it ends in.
    pub fn text(&self, text: &str, cut: bool) -> String {
        shown_text(&self.0, text, cut)
    }

    /// An error, as the record keeps one: it may quote the start of an answer.
    pub fn error(&self, error: &str) -> String {
        shown_error(&self.0, error)
    }

    /// A JSON value, scrubbed as the record scrubs a payload.
    pub fn json(&self, v: &Value) -> Value {
        shown_json(&self.0, v)
    }
}

fn shown_text(scrub: &Scrub, text: &str, cut: bool) -> String {
    // a preview marks where it was cut: the text ends before the mark
    let (text, mark) = match text.strip_suffix('…') {
        Some(before) if cut => (before, "…"),
        _ => (text, ""),
    };
    let mut out = String::with_capacity(text.len() + 16);
    scrub.write_text(&mut out, text, cut, usize::MAX);
    let mut shown: String = serde_json::from_str(&out).unwrap_or_else(|_| TAKEN_OUT.to_string());
    shown.push_str(mark);
    shown
}

fn shown_error(scrub: &Scrub, error: &str) -> String {
    let mut out = String::with_capacity(error.len().min(TEXT_MAX) + 16);
    scrub.write_error(&mut out, error);
    serde_json::from_str(&out).unwrap_or_else(|_| TAKEN_OUT.to_string())
}

/// Whole, or nothing of it: when a value handed out is a JSON document that
/// spans it, or it nests too deep to read, `v` is not shown at all.
fn shown_json(scrub: &Scrub, v: &Value) -> Value {
    let bytes = serde_json::to_vec(v).unwrap_or_default();
    let mut out = String::with_capacity(bytes.len() + 64);
    if !scrub.spans_json(&bytes) && scrubbed_json(&bytes, &mut out, scrub, 0) {
        if let Ok(shown) = serde_json::from_str(&out) {
            return shown;
        }
    }
    Value::String(TAKEN_OUT.to_string())
}

/// One record, written straight to JSON text: the span's ids, unit, kind,
/// timing and attributes, then what the hop adds.
fn render(span: &Span, extras: &Extras, scrub: &Scrub) -> String {
    let mut out = String::with_capacity(512 + extras.weight());
    let mut subjects = Subjects::default();
    let _ = write!(
        out,
        r#"{{"trace_id":"{:032x}","span_id":"{:016x}","parent_id":"#,
        span.trace_id, span.span_id
    );
    match span.parent {
        Some(p) => {
            let _ = write!(out, r#""{p:016x}""#);
        }
        None => out.push_str("null"),
    }
    out.push_str(r#","unit":"#);
    json_string(&mut out, &span.name);
    out.push_str(r#","kind":"#);
    json_string(&mut out, span.kind.label());
    let _ = write!(
        out,
        r#","start_us":{},"duration_us":{},"ok":{}"#,
        (span.start_nanos / 1_000) as u64,
        (span.end_nanos.saturating_sub(span.start_nanos) / 1_000) as u64,
        span.error.is_none()
    );
    out.push_str(r#","error":"#);
    match &span.error {
        Some(e) => scrub.write_error(&mut out, e),
        None => out.push_str("null"),
    }
    out.push_str(r#","subject":"#);
    match span.attrs.iter().find(|(key, _)| *key == "messaging.destination.name") {
        Some((_, value)) => subjects.write_attr(&mut out, value, scrub),
        None => out.push_str("null"),
    }
    out.push_str(r#","emits":["#);
    for (i, subject) in distinct(&extras.emits).enumerate() {
        if i > 0 {
            out.push(',');
        }
        subjects.write(&mut out, subject, scrub);
    }
    out.push_str(r#"],"answer":"#);
    match &extras.answer {
        Some(a) => answer(&mut out, a, scrub),
        None => out.push_str("null"),
    }
    out.push_str(r#","attributes":{"#);
    for (i, (key, value)) in span.attrs.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        json_string(&mut out, key);
        out.push(':');
        match value {
            _ if *key == "messaging.destination.name" => subjects.write_attr(&mut out, value, scrub),
            _ => scrub.write_attr(&mut out, value),
        }
    }
    out.push('}');
    if let Some(steps) = &extras.steps {
        write_steps(&mut out, steps, scrub, &mut subjects);
        write_services(&mut out, steps.services());
    }
    if let Some(input) = &extras.input {
        out.push_str(r#","input":"#);
        payload(&mut out, input, scrub);
    }
    out.push('}');
    out
}

/// The statements a run went through, in order: `{"line":n}` for each, with
/// the service it is in when it is not the flow's own, the subject an `emit`
/// sent to, the status a flow's `respond` set, and an invoked service's version
/// where it starts. `{"skipped":n}` stands for the statements that ran
/// between the first ones kept and the last. The statement a failed run
/// ended on, the last step, says `"failed":true`.
fn write_steps<'a>(out: &mut String, steps: &'a Steps, scrub: &Scrub, subjects: &mut Subjects<'a>) {
    let failed_at = if steps.failed() { steps.len().checked_sub(1) } else { None };
    let mut one = |out: &mut String, step: &'a Step, at: usize| {
        out.push('{');
        // checked identifiers, `package:service`: no event data reaches them
        if let Some(service) = &step.service {
            out.push_str(r#""service":"#);
            json_string(out, service);
            out.push(',');
        }
        match &step.what {
            What::Line(line) => {
                out.push_str(r#""line":"#);
                push_u64(out, u64::from(*line));
            }
            What::Emit(line, subject) => {
                out.push_str(r#""line":"#);
                push_u64(out, u64::from(*line));
                out.push_str(r#","emit":"#);
                subjects.write(out, subject, scrub);
            }
            What::Respond(line, status) => {
                out.push_str(r#""line":"#);
                push_u64(out, u64::from(*line));
                out.push_str(r#","respond":"#);
                scrub.write_number(out, format_args!("{status}"));
            }
            What::Enter(version) => {
                // a hash of the service's source, computed here
                out.push_str(r#""version":"#);
                json_string(out, version);
            }
        }
        if failed_at == Some(at) && !matches!(step.what, What::Enter(_)) {
            out.push_str(r#","failed":true"#);
        }
        out.push('}');
    };
    out.push_str(r#","steps":["#);
    let head = steps.head().len();
    for (i, step) in steps.head().iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        one(out, step, i);
    }
    if steps.skipped() > 0 {
        let _ = write!(out, r#",{{"skipped":{}}}"#, steps.skipped());
    }
    for (i, step) in steps.tail().enumerate() {
        out.push(',');
        one(out, step, head + i);
    }
    out.push(']');
}

/// The services a run invoked, each with the version of its source: names
/// the runtime checked and hashes it computed, written as they are.
fn write_services(out: &mut String, services: &[(std::sync::Arc<str>, std::sync::Arc<str>)]) {
    if services.is_empty() {
        return;
    }
    out.push_str(r#","services":{"#);
    for (i, (name, version)) in services.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        json_string(out, name);
        out.push(':');
        json_string(out, version);
    }
    out.push('}');
}

/// A number, with none of the formatting machinery: a record writes one for
/// each step.
fn push_u64(out: &mut String, mut n: u64) {
    let mut digits = [0u8; 20];
    let mut at = digits.len();
    loop {
        at -= 1;
        digits[at] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    out.push_str(std::str::from_utf8(&digits[at..]).unwrap_or("0"));
}

/// The subjects in the order first emitted, each once, `EMITS_MAX` at most.
fn distinct(subjects: &[String]) -> impl Iterator<Item = &String> {
    let small = subjects.len() <= 16;
    let mut seen = HashSet::new();
    subjects
        .iter()
        .enumerate()
        .filter(move |(i, s)| if small { !subjects[..*i].contains(s) } else { seen.insert(s.as_str()) })
        .map(|(_, s)| s)
        .take(EMITS_MAX)
}

/// A trace id as the record's subjects spell it, 32 lowercase hex digits, or
/// None for anything else (the all-zero id included, which no trace has).
pub fn trace_key(id: &str) -> Option<String> {
    let id = id.trim().to_ascii_lowercase();
    let hex = id.len() == 32 && id.bytes().all(|b| b.is_ascii_hexdigit());
    (hex && id.bytes().any(|b| b != b'0')).then_some(id)
}

/// Every recorded hop of one trace, in the order they started. An unknown id
/// gives an empty list.
pub fn read(js: &bus::JetStream, trace_id: &str) -> Result<Vec<Value>, String> {
    let key = trace_key(trace_id).ok_or("a trace id is 32 hexadecimal digits")?;
    let filter = format!("{ROOT}.{key}");
    let sub = js.pull_ephemeral(
        STREAM,
        &bus::EphemeralConfig {
            filter: &filter,
            start_seq: Some(1),
            inactive_threshold: Duration::from_secs(30),
        },
    )?;
    let mut hops = Vec::new();
    loop {
        let round: Vec<bus::Message> = sub.round()?.collect();
        if round.is_empty() {
            break;
        }
        hops.extend(round.iter().filter_map(|m| serde_json::from_slice::<Value>(&m.data).ok()));
        if hops.len() >= READ_MAX {
            break;
        }
    }
    hops.sort_by_key(|h| h["start_us"].as_u64().unwrap_or(0));
    Ok(hops)
}

/// The traces with a hop that mentions `needle` in its payload, subject, unit,
/// emits, error or answer, newest first, at most `limit`. A scan of the last
/// `window` hops of the stream: an operator's or an agent's question, never the
/// hot path. The ids and the timings of a record are not searched, so a number
/// finds the order that carries it and not a timestamp that happens to.
/// Also says how many hops it read, which is the window only when the stream
/// holds that many.
pub fn search(js: &bus::JetStream, needle: &str, limit: usize, window: u64) -> Result<(Vec<Value>, u64), String> {
    let needle = needle.trim();
    if needle.is_empty() {
        return Err("nothing to search for".into());
    }
    let state = js.stream_state(STREAM)?;
    if state.messages == 0 {
        return Ok((Vec::new(), 0));
    }
    let window = window.clamp(1, SEARCH_WINDOW);
    let start = state.last_seq.saturating_sub(window - 1).max(state.first_seq).max(1);
    let filter = format!("{ROOT}.>");
    let sub = js.pull_ephemeral(
        STREAM,
        &bus::EphemeralConfig {
            filter: &filter,
            start_seq: Some(start),
            inactive_threshold: Duration::from_secs(30),
        },
    )?;
    // JSON escapes a quote or a backslash, so the bytes cannot show those.
    let quick = !needle.contains(['"', '\\']);
    let mut found: HashMap<String, (u64, Value)> = HashMap::new();
    let mut scanned = 0u64;
    loop {
        let round: Vec<bus::Message> = sub.round()?.collect();
        if round.is_empty() {
            break;
        }
        for m in &round {
            scanned += 1;
            if quick && !contains(&m.data, needle.as_bytes()) {
                continue;
            }
            let Ok(hop) = serde_json::from_slice::<Value>(&m.data) else { continue };
            if !mentions(&hop, needle) {
                continue;
            }
            let seq = m.delivery().map(|d| d.stream_seq).unwrap_or(0);
            let trace = hop["trace_id"].as_str().unwrap_or_default().to_string();
            // forward through the stream: the later match is the newer one
            found.insert(trace, (seq, hop));
        }
        if scanned >= window {
            break;
        }
    }
    let mut hits: Vec<(u64, Value)> = found.into_values().collect();
    hits.sort_by_key(|(seq, _)| Reverse(*seq));
    Ok((hits.into_iter().take(limit).map(|(_, hop)| summary(&hop)).collect(), scanned))
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    needle.is_empty() || haystack.windows(needle.len()).any(|w| w == needle)
}

fn mentions(hop: &Value, needle: &str) -> bool {
    let text = |v: &Value| v.as_str().is_some_and(|s| s.contains(needle));
    ["unit", "subject", "error", "answer"].iter().any(|k| text(&hop[*k]))
        || hop["emits"].as_array().is_some_and(|a| a.iter().any(text))
        || match &hop["input"] {
            Value::Null => false,
            Value::String(s) => s.contains(needle),
            other => other.to_string().contains(needle),
        }
}

/// The hop a search found, for a list of traces.
fn summary(hop: &Value) -> Value {
    json!({
        "trace_id": hop["trace_id"],
        "span_id": hop["span_id"],
        "unit": hop["unit"],
        "subject": hop["subject"],
        "ok": hop["ok"],
        "error": hop["error"],
        "start_us": hop["start_us"],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::Kind;

    fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> =
            pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        move |k: &str| map.get(k).cloned()
    }

    #[test]
    fn the_record_is_on_by_default_and_bounded() {
        let c = config(env(&[])).unwrap();
        assert_eq!(
            c,
            Config {
                payloads: true,
                max_hops: 100_000,
                max_bytes: 256 * 1024 * 1024,
                max_age: Duration::from_secs(7 * 24 * 3600),
            }
        );
        let s = stream(&c);
        assert_eq!(s.subjects, vec!["vxtrace.>".to_string()]);
        assert_eq!((s.max_msgs, s.max_bytes), (100_000, 256 * 1024 * 1024));
        assert_eq!(s.discard, bus::Discard::Old);
    }

    #[test]
    fn the_environment_turns_it_off_or_tunes_it() {
        assert_eq!(config(env(&[("VEJAS_TRACES", "off")])), None);
        assert_eq!(config(env(&[("VEJAS_TRACES", " FALSE ")])), None);
        let c = config(env(&[
            ("VEJAS_TRACES", "on"),
            ("VEJAS_TRACES_PAYLOADS", "off"),
            ("VEJAS_TRACES_MAX_HOPS", "5000"),
            ("VEJAS_TRACES_MAX_BYTES", "-3"),
            ("VEJAS_TRACES_MAX_AGE_SECS", "3600"),
        ]))
        .unwrap();
        assert!(!c.payloads);
        assert_eq!(c.max_hops, 5000);
        // not a positive number: the default stands
        assert_eq!(c.max_bytes, DEFAULT_MAX_BYTES);
        assert_eq!(c.max_age, Duration::from_secs(3600));
        // a value that is neither on nor off leaves it on
        assert!(config(env(&[("VEJAS_TRACES", "maybe")])).is_some());
    }

    fn span() -> Span {
        Span {
            name: "flow:orders".into(),
            kind: Kind::Consumer,
            trace_id: 0x4bf92f3577b34da6a3ce929d0e0e4736,
            span_id: 0x00f067aa0ba902b7,
            parent: Some(0x1111111111111111),
            sampled: true,
            start_nanos: 1_764_118_847_112_345_000,
            end_nanos: 1_764_118_847_112_845_000,
            error: None,
            attrs: vec![
                ("vejas.unit", Attr::Str("flow:orders".into())),
                ("messaging.destination.name", Attr::Str("vx.orders.in".into())),
                ("vejas.delivery.attempt", Attr::Int(2)),
                ("vejas.dlq", Attr::Bool(false)),
                ("messaging.system", Attr::Static("nats")),
            ],
        }
    }

    fn scrub(secrets: &[&str]) -> Scrub {
        let mut s = Scrub::new();
        s.vault = Vault::of(&secrets.iter().map(|v| v.to_string()).collect::<Vec<_>>());
        s
    }

    fn rendered(span: &Span, extras: Extras, secrets: &[&str]) -> Value {
        let text = render(span, &extras, &scrub(secrets));
        serde_json::from_str(&text).unwrap_or_else(|e| panic!("{e}: {text}"))
    }

    fn with_input(bytes: &[u8]) -> Extras {
        Extras { input: Some(Kept::copy(bytes)), ..Extras::default() }
    }

    fn input_of(bytes: &[u8], secrets: &[&str]) -> Value {
        rendered(&span(), with_input(bytes), secrets)["input"].clone()
    }

    fn text(t: &str, secrets: &[&str], cut: bool) -> String {
        let mut out = String::new();
        scrub(secrets).write_text(&mut out, t, cut, usize::MAX);
        serde_json::from_str(&out).unwrap()
    }

    #[test]
    fn what_other_surfaces_show_is_masked_as_the_record_is() {
        let s = scrub(&["s3cr3t-handed-out"]);
        let letter = json!({"dead_letters": [{
            "unit": "flow:orders", "original_subject": "vx.orders", "attempts": 5,
            "payload": r#"{"id":7,"password":"hunter2","header":{"name":"Authorization","value":"Bearer abc"},"note":"s3cr3t-handed-out"}"#,
            "last_error": "POST failed: token s3cr3t-handed-out refused",
        }]});
        let shown = shown_json(&s, &letter).to_string();
        for leak in ["hunter2", "Bearer abc", "s3cr3t-handed-out"] {
            assert!(!shown.contains(leak), "{leak} in {shown}");
        }
        assert!(shown.contains(r#"\"id\":7"#) && shown.contains("vx.orders"), "the rest stays: {shown}");
        // the shape of the answer is kept: what the panel reads is still there
        let back = shown_json(&s, &letter);
        assert_eq!(back["dead_letters"][0]["attempts"], 5);
        assert_eq!(back["dead_letters"][0]["unit"], "flow:orders");
        // a preview cut in a value handed out loses its start
        let cut = shown_text(&s, r#"{"id":7,"note":"s3cr3t-han"#, true);
        assert!(!cut.contains("s3cr3t-han"), "{cut}");
        assert!(cut.contains(r#""id":7"#), "{cut}");
        // an error that ends in the start of one, too
        let error = shown_error(&s, "downstream said: s3cr3t-hand");
        assert!(!error.contains("s3cr3t-hand"), "{error}");
        // a credential named in a cut preview
        let cut = shown_text(&s, r#"{"user":"bob","password":"hunt"#, true);
        assert!(!cut.contains("hunt") && cut.contains("bob"), "{cut}");
        // a preview marks its cut with an ellipsis: the text ends before it
        let cut = shown_text(&s, r#"{"id":7,"note":"s3cr3t-han…"#, true);
        assert!(!cut.contains("s3cr3t-han") && cut.ends_with('…'), "{cut}");
        // a pair, in a preview whole and in one cut
        for preview in [
            r#"{"header":{"name":"Authorization","value":"Bearer abc"}}"#,
            r#"{"headers":[{"name":"Authorization","value":"Bearer abc"},{"name":"X-Trace","value":"t1"}],"body":"lo…"#,
        ] {
            let shown = shown_text(&s, preview, true);
            assert!(!shown.contains("Bearer abc"), "{shown}");
        }
    }

    #[test]
    fn a_record_is_the_span_and_what_the_hop_adds() {
        let extras = Extras {
            emits: vec!["vx.orders.validated".into()],
            input: Some(Kept::copy(br#"{"order":4711}"#)),
            answer: Some(Kept::copy(b"202 accepted")),
            steps: None,
        };
        let r = rendered(&span(), extras, &[]);
        assert_eq!(r["trace_id"], "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(r["span_id"], "00f067aa0ba902b7");
        assert_eq!(r["parent_id"], "1111111111111111");
        assert_eq!(r["unit"], "flow:orders");
        assert_eq!(r["kind"], "consumer");
        assert_eq!(r["start_us"], 1_764_118_847_112_345u64);
        assert_eq!(r["duration_us"], 500);
        assert_eq!(r["ok"], true);
        assert_eq!(r["error"], Value::Null);
        assert_eq!(r["subject"], "vx.orders.in");
        assert_eq!(r["emits"], json!(["vx.orders.validated"]));
        assert_eq!(r["answer"], "202 accepted");
        assert_eq!(r["input"], json!({"order": 4711}));
        assert_eq!(r["attributes"]["vejas.delivery.attempt"], 2);
        assert_eq!(r["attributes"]["vejas.dlq"], false);
        assert_eq!(r["attributes"]["messaging.system"], "nats");
        // a hop that received nothing has no input at all
        assert!(rendered(&span(), Extras::new(), &[]).get("input").is_none());
        // a root hop has no parent
        let mut root = span();
        root.parent = None;
        assert_eq!(rendered(&root, Extras::new(), &[])["parent_id"], Value::Null);
    }

    /// A JSON escape spelled out: a backslash, a u, four hex digits.
    fn esc(hex: &str) -> String {
        format!("{}u{hex}", '\\')
    }

    #[test]
    fn the_json_written_back_is_the_json_read() {
        let input = format!(
            r#" {{"s":"quote \" backslash \\ tab \t nl \n ctl {} {} {}{} {}",
            "n":[0,-1,18446744073709551615,-9223372036854775808,1.5,-0.0,1e300,2.5E-7,123456789012345678901234567890],
            "b":[true,false,null],"e":{{}},"a":[],"nested":{{"x":[{{"y":[[]]}}]}},"k{}y":"v"}} "#,
            esc("0001"),
            esc("00e9"),
            esc("d83d"),
            esc("de00"),
            esc("2028"),
            esc("00e9")
        );
        let expected: Value = serde_json::from_str(&input).unwrap();
        assert_eq!(input_of(input.as_bytes(), &[]), expected);
    }

    #[test]
    fn credentials_are_masked_at_any_depth_and_in_any_shape() {
        let input = format!(
            r#"{{"user":"ana","password":"hunter2","nested":{{"API_KEY":"k-1","list":[{{"x-auth-token":"t"}}]}},
            "secret":{{"a":[1,{{"b":"x"}}]}},"client_secret":[1,2],"token_url":"https://idp","refresh_token":null,
            "pass{}ord":"escaped","after":[true,null,-1.5e3]}}"#,
            esc("0077")
        );
        let i = input_of(input.as_bytes(), &[]);
        assert_eq!(i["user"], "ana");
        assert_eq!(i["password"], MASKED);
        assert_eq!(i["nested"]["API_KEY"], MASKED);
        assert_eq!(i["nested"]["list"][0]["x-auth-token"], MASKED);
        // an object or an array goes whole, and what follows it is read right
        assert_eq!(i["secret"], MASKED);
        assert_eq!(i["client_secret"], MASKED);
        assert_eq!(i["after"], json!([true, null, -1500.0]));
        // config-shaped names pass, as in the panel (ADR-0017)
        assert_eq!(i["token_url"], "https://idp");
        // nothing to hide in a null
        assert_eq!(i["refresh_token"], Value::Null);
        // a key spelled with an escape is read as the name it spells
        assert!(!serde_json::to_string(&i).unwrap().contains("escaped"));
    }

    #[test]
    fn camel_case_names_are_masked_as_snake_case_ones_are() {
        let i = input_of(
            br#"{"accessToken":"at-1","refreshToken":"rt-1","idToken":"jwt","apiKey":"k","XAuthToken":"x",
               "nextPageToken":"p2","maxTokens":512,"tokenUrl":"https://idp","author":"ana","publicKey":"pk"}"#,
            &[],
        );
        for key in ["accessToken", "refreshToken", "idToken", "apiKey", "XAuthToken"] {
            assert_eq!(i[key], MASKED, "{key}");
        }
        // the trade-off decided on 2026-11-26: a pagination token is masked,
        // as next_page_token already was
        assert_eq!(i["nextPageToken"], MASKED);
        assert_eq!(i["maxTokens"], 512);
        assert_eq!(i["tokenUrl"], "https://idp");
        assert_eq!(i["author"], "ana");
        assert_eq!(i["publicKey"], "pk");
        // in a text too: a query string
        let t = input_of(br#"{"url":"https://h/cb?accessToken=at-2&state=s"}"#, &[]);
        assert_eq!(t["url"], "https://h/cb?accessToken=<masked>&state=s");
    }

    #[test]
    fn a_pair_that_names_a_credential_hides_its_value() {
        let i = input_of(
            br#"{"headers":[{"name":"Authorization","value":"Bearer abc"},{"name":"Accept","value":"*/*"},
               {"value":"sk_live_1","name":"x-api-key"},{"name":"Cookie","value":"sid=1"}],
               "env":[{"name":"DB_PASSWORD","value":{"from":"vault","v":"pw"}},{"name":"DB_HOST","value":"db"},
                      {"name":"API_TOKEN","value":null}],
               "Tags":[{"Key":"clientSecret","Value":"cs"},{"Key":"team","Value":"ops"}],
               "params":[{"ParameterKey":"MasterPassword","ParameterValue":"mp"}],
               "products":[{"name":"Secret Santa kit","value":12.5},{"name":"secret","value":"x","note":"kept"}]}"#,
            &[],
        );
        assert_eq!(i["headers"][0], json!({"name": "Authorization", "value": MASKED}));
        assert_eq!(i["headers"][1], json!({"name": "Accept", "value": "*/*"}));
        // the value first, the name after: masked all the same
        assert_eq!(i["headers"][2], json!({"value": MASKED, "name": "x-api-key"}));
        assert_eq!(i["headers"][3]["value"], "sid=1");
        // an object or an array goes whole, and nothing hides in a null
        assert_eq!(i["env"][0]["value"], MASKED);
        assert_eq!(i["env"][1]["value"], "db");
        assert_eq!(i["env"][2]["value"], Value::Null);
        assert_eq!(i["Tags"][0], json!({"Key": "clientSecret", "Value": MASKED}));
        assert_eq!(i["Tags"][1]["Value"], "ops");
        assert_eq!(i["params"][0]["ParameterValue"], MASKED);
        // a name with a space is a text, not a key
        assert_eq!(i["products"][0]["value"], 12.5);
        assert_eq!(i["products"][1], json!({"name": "secret", "value": MASKED, "note": "kept"}));
    }

    #[test]
    fn a_value_from_the_vault_never_reaches_a_record() {
        let secrets = ["s3cr3t-value-long", "s3cr3t", "98765432"];
        let mut s = span();
        s.error = Some("401 for s3cr3t-value-long".into());
        s.attrs.push(("http.url", Attr::Str("https://h/?k=s3cr3t".into())));
        s.attrs[1].1 = Attr::Str("vx.in.s3cr3t".into());
        s.attrs.push(("code.file.path", Attr::Str("flows/orders.vjs".into())));
        let extras = Extras {
            emits: vec!["vx.out.s3cr3t".into()],
            input: Some(Kept::copy(
                br#"{"note":"forwarding s3cr3t-value-long","n":["s3cr3t"],"pin":98765432,"s3cr3t":1}"#,
            )),
            answer: Some(Kept::copy(br#"{"echo":"s3cr3t","api_key":"abc"}"#)),
            steps: Some(steps_of(vec![
                (None, What::Line(1)),
                (None, What::Emit(2, "vx.out.s3cr3t".into())),
                (None, What::Emit(2, "vx.out.s3cr3t".into())),
                (Some("default:auth"), What::Enter("0123456789abcdef".into())),
                (Some("default:auth"), What::Respond(3, 401)),
            ])),
        };
        let out = render(&s, &extras, &scrub(&secrets));
        assert!(!out.contains("s3cr3t") && !out.contains("98765432"), "{out}");
        let r: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(r["input"]["note"], "forwarding <secret>");
        assert_eq!(r["input"]["n"][0], "<secret>");
        // a number and a key lose it too
        assert_eq!(r["input"]["pin"], "<secret>");
        assert_eq!(r["input"]["<secret>"], 1);
        assert_eq!(r["error"], "401 for <secret>");
        assert_eq!(r["attributes"]["http.url"], "https://h/?k=<secret>");
        assert_eq!(r["emits"][0], "vx.out.<secret>");
        // the hop's own subject, scrubbed once, written twice
        assert_eq!(r["subject"], "vx.in.<secret>");
        assert_eq!(r["attributes"]["messaging.destination.name"], "vx.in.<secret>");
        assert_eq!(r["attributes"]["code.file.path"], "flows/orders.vjs");
        // a JSON answer is scrubbed like a payload
        let answer: Value = serde_json::from_str(r["answer"].as_str().unwrap()).unwrap();
        assert_eq!(answer, json!({"echo": "<secret>", "api_key": MASKED}));
        // and so do the subjects and statuses the steps name
        assert_eq!(
            r["steps"],
            json!([
                {"line": 1},
                {"line": 2, "emit": "vx.out.<secret>"},
                {"line": 2, "emit": "vx.out.<secret>"},
                {"service": "default:auth", "version": "0123456789abcdef"},
                {"service": "default:auth", "line": 3, "respond": 401},
            ])
        );
    }

    fn steps_of(steps: Vec<(Option<&str>, What)>) -> Steps {
        let mut out = Steps::new();
        for (service, what) in steps {
            out.push(Step { service: service.map(Into::into), what });
        }
        out
    }

    #[test]
    fn a_record_says_which_lines_a_hop_ran() {
        let n = crate::vjs::STEPS_HEAD + crate::vjs::STEPS_TAIL + 10;
        let mut steps: Vec<(Option<&str>, What)> = (1..=n).map(|l| (None, What::Line(l as u32))).collect();
        steps.push((None, What::Emit(n as u32 + 1, format!("vx.{}", "k".repeat(1000)))));
        steps.push((None, What::Respond(n as u32 + 2, 201)));
        let r = rendered(&span(), Extras::new().steps(Some(steps_of(steps))), &[]);
        let kept = r["steps"].as_array().unwrap();
        assert_eq!(kept.len(), crate::vjs::STEPS_HEAD + 1 + crate::vjs::STEPS_TAIL);
        assert_eq!(kept[0], json!({"line": 1}));
        assert_eq!(kept[crate::vjs::STEPS_HEAD], json!({"skipped": 12}));
        assert_eq!(kept.last().unwrap(), &json!({"line": n + 2, "respond": 201}));
        // a long subject is cut once scrubbed
        let long = kept[kept.len() - 2]["emit"].as_str().unwrap();
        assert!(long.starts_with("vx.kkk") && long.ends_with('…'), "{long}");
        assert_eq!(long.len(), SUBJECT_MAX + '…'.len_utf8());
        // the services a run invoked, by name, with their versions
        let mut steps = steps_of(vec![
            (None, What::Line(u32::MAX)),
            (Some("default:auth"), What::Enter("0123456789abcdef".into())),
        ]);
        steps.entered(&"default:auth".into(), &"0123456789abcdef".into());
        steps.entered(&"billing:fmt".into(), &"fedcba9876543210".into());
        let r = rendered(&span(), Extras::new().steps(Some(steps)), &[]);
        assert_eq!(r["steps"][0], json!({"line": u32::MAX}));
        assert_eq!(r["services"], json!({"default:auth": "0123456789abcdef", "billing:fmt": "fedcba9876543210"}));
        for n in [0, 9, 10, 99, 100, u64::from(u32::MAX), u64::MAX] {
            let mut out = String::new();
            push_u64(&mut out, n);
            assert_eq!(out, n.to_string());
        }
        // the statement a failed run ended on, its last step, says so: in the
        // head of a short run or the tail of a long one, never on a start
        let mut steps = steps_of(vec![(None, What::Line(4)), (Some("default:crm"), What::Line(2)), (None, What::Line(6))]);
        steps.set_failed();
        let r = rendered(&span(), Extras::new().steps(Some(steps)), &[]);
        assert_eq!(
            r["steps"],
            json!([{"line": 4}, {"service": "default:crm", "line": 2}, {"line": 6, "failed": true}])
        );
        let n = crate::vjs::STEPS_HEAD + crate::vjs::STEPS_TAIL + 3;
        let mut steps = steps_of((1..=n).map(|l| (None, What::Line(l as u32))).collect());
        steps.set_failed();
        let r = rendered(&span(), Extras::new().steps(Some(steps)), &[]);
        let kept = r["steps"].as_array().unwrap();
        assert_eq!(kept.last().unwrap(), &json!({"line": n, "failed": true}));
        assert_eq!(kept.iter().filter(|st| st.get("failed").is_some()).count(), 1);
        let mut steps = steps_of(vec![(Some("default:crm"), What::Enter("0123456789abcdef".into()))]);
        steps.set_failed();
        let r = rendered(&span(), Extras::new().steps(Some(steps)), &[]);
        assert_eq!(r["steps"], json!([{"service": "default:crm", "version": "0123456789abcdef"}]));
        // no step, no field; steps carry no payload, so payloads off keeps them
        assert!(rendered(&span(), Extras::new().steps(Some(Steps::new())), &[]).get("steps").is_none());
        assert!(rendered(&span(), Extras::new(), &[]).get("steps").is_none());
    }

    #[test]
    fn overlapping_vault_values_go_as_one() {
        // the short one starts first: replaced alone, it would leave "ef"
        assert_eq!(text("xabcdefy", &["bcdef", "abcd"], false), "x<secret>y");
        assert_eq!(text("one s3cr3t, two s3cr3t", &["s3cr3t"], false), "one <secret>, two <secret>");
        assert_eq!(text("nothing here", &["s3cr3t"], false), "nothing here");
    }

    #[test]
    fn a_cut_text_loses_what_could_start_a_vault_value() {
        let cut = text("the key is sk_live_ab", &["sk_live_abcdef"], true);
        assert!(!cut.contains("sk_live"), "{cut}");
        // whole, the same text keeps its end: no value is there
        assert_eq!(text("the key is sk_live_ab", &["sk_live_abcdef"], false), "the key is sk_live_ab");
    }

    #[test]
    fn values_that_cannot_be_indexed_keep_every_string_out() {
        let mut s = scrub(&[]);
        s.vault = Vault::Unusable;
        let mut out = String::new();
        s.write_text(&mut out, "anything at all", false, usize::MAX);
        assert_eq!(out, r#""<secret>""#);
    }

    #[test]
    fn a_text_that_names_a_key_loses_its_value() {
        let t = |s: &str| text(s, &[], false);
        assert_eq!(t("POST https://h/x?api_key=abc123&x=1 failed"), "POST https://h/x?api_key=<masked>&x=1 failed");
        assert_eq!(
            t("grant_type=client_credentials&client_secret=s3&scope=a"),
            "grant_type=client_credentials&client_secret=<masked>&scope=a"
        );
        assert_eq!(t("https://user:pa55word@host/path"), "https://user:<masked>@host/path");
        assert_eq!(t("http://host:8080/path"), "http://host:8080/path");
        assert_eq!(t("Authorization: Bearer abc.def\nnext: line"), "Authorization: <masked>\nnext: line");
        // a value runs over its spaces up to the next key
        assert_eq!(t("Authorization=Bearer abc.def x"), "Authorization=<masked>");
        assert_eq!(t("Authorization=Bearer abc.def user=ana"), "Authorization=<masked> user=ana");
        assert_eq!(t("<wsse:Password>hunter2</wsse:Password>"), "<wsse:Password><masked></wsse:Password>");
        assert_eq!(t(r#"<login password="hunter2" user="ana"/>"#), r#"<login password=<masked> user="ana"/>"#);
        // JSON that was cut: the open value goes to the end
        assert_eq!(t(r#"{"user":"ana","password":"hun"#), r#"{"user":"ana","password":"<masked>""#);
        assert_eq!(t(r#"{"secret":{"a":"b"},"n":1}"#), r#"{"secret":"<masked>","n":1}"#);
        // a key that is no credential is searched inside
        assert_eq!(
            t("token_url=https://idp/?client_secret=x&y=1"),
            "token_url=https://idp/?client_secret=<masked>&y=1"
        );
        assert_eq!(t("no pairs in this text"), "no pairs in this text");
    }

    #[test]
    fn a_string_inside_json_is_scrubbed_as_text() {
        let i = input_of(br#"{"url":"https://h/?api_key=k-1","body":"{\"password\":\"x\"}"}"#, &[]);
        assert_eq!(i["url"], "https://h/?api_key=<masked>");
        assert_eq!(i["body"], r#"{"password":"<masked>"}"#);
    }

    #[test]
    fn a_big_payload_keeps_its_size_and_a_masked_preview() {
        let big = format!(r#"{{"api_key":"hunter2","blob":"{}"}}"#, "x".repeat(40_000));
        let kept = Kept::copy(big.as_bytes());
        // the hot path copies no more than it can show
        assert_eq!((kept.bytes.len(), kept.len, kept.whole()), (INPUT_MAX, big.len(), false));
        let i = &rendered(&span(), Extras { input: Some(kept), ..Extras::default() }, &[])["input"];
        assert_eq!(i["truncated"], true);
        assert_eq!(i["bytes"], big.len());
        let preview = i["preview"].as_str().unwrap();
        assert!(preview.len() <= PREVIEW_MAX + "…".len());
        assert!(!preview.contains("hunter2"));
        assert!(preview.contains(MASKED));
    }

    #[test]
    fn a_big_payload_masks_a_pair_in_its_preview() {
        let big = format!(
            r#"{{"headers":[{{"name":"X-Id","value":"7"}},{{"value":"Bearer abc","name":"Authorization"}}],"blob":"{}"}}"#,
            "x".repeat(40_000)
        );
        let i = &rendered(&span(), with_input(big.as_bytes()), &[])["input"];
        let preview = i["preview"].as_str().unwrap();
        assert!(!preview.contains("Bearer abc") && preview.contains(r#""value":"7""#), "{preview}");
    }

    #[test]
    fn a_cut_json_text_is_made_whole_to_be_read_as_json() {
        let whole = |t: &str| made_whole(t).map(|(keep, closers)| format!("{}{closers}", &t[..keep]));
        for (cut, made) in [
            (r#"{"a":1,"b":"x"#, r#"{"a":1,"b":"x"}"#),
            (r#"{"a":[1,2,"#, r#"{"a":[1,2]}"#),
            (r#"{"a":1,"pass"#, r#"{"a":1}"#),
            (r#"{"a":1,"b":"#, r#"{"a":1}"#),
            (r#"{"a":1,"b":""#, r#"{"a":1,"b":""}"#),
            (r#"{"a":{"b":tru"#, r#"{"a":{}}"#),
            (r#"{"a":12"#, "{}"),
            (r#" [{"x":"a\"#, r#" [{"x":"a"}]"#),
            (r#"{"x":"a\u00"#, r#"{"x":"a"}"#),
            (r#"{"x":"\ud83d\ude"#, r#"{"x":""}"#),
            (r#"{"x":"\ud83d\ude00"#, r#"{"x":"\ud83d\ude00"}"#),
            (r#"{"a":1}"#, r#"{"a":1}"#),
        ] {
            assert_eq!(whole(cut).as_deref(), Some(made), "{cut}");
        }
        for not_one in [r#"{"a":1} {"b":2}"#, r#"{"a":1]"#, "a=1", r#""text"#, ""] {
            assert_eq!(whole(not_one), None, "{not_one}");
        }
    }

    #[test]
    fn a_long_answer_is_scrubbed_before_it_is_cut() {
        let long = format!(r#"{{"id":"{}","api_key":"sk_live_1","more":"{}"}}"#, "i".repeat(300), "m".repeat(5000));
        let r = rendered(&span(), Extras { answer: Some(Kept::copy(long.as_bytes())), ..Extras::default() }, &[]);
        let answer = r["answer"].as_str().unwrap();
        assert!(answer.contains(MASKED) && !answer.contains("sk_live_1"), "{answer}");
        assert!(answer.len() <= TEXT_MAX + "…".len());
        // past what the hot path copies, it is scrubbed as text
        let huge = format!(r#"{{"api_key":"sk_live_2","blob":"{}"}}"#, "b".repeat(INPUT_MAX * 2));
        let r = rendered(&span(), Extras { answer: Some(Kept::copy(huge.as_bytes())), ..Extras::default() }, &[]);
        assert!(!r["answer"].as_str().unwrap().contains("sk_live_2"));
    }

    #[test]
    fn a_payload_that_is_not_json_is_kept_as_scrubbed_text() {
        assert_eq!(input_of(b"plain s3cr3t text \xff", &["s3cr3t"]), "plain <secret> text \u{fffd}");
        assert_eq!(input_of(b"user=ana&password=hunter2", &[]), "user=ana&password=<masked>");
        // trailing bytes after a JSON value, or too deep to read: text
        assert_eq!(input_of(br#"{"a":1} x"#, &[]), r#"{"a":1} x"#);
        let deep = format!("{}{}", "[".repeat(200), "]".repeat(200));
        assert_eq!(input_of(deep.as_bytes(), &[]), deep.as_str());
    }

    #[test]
    fn a_fan_out_lists_each_subject_once_and_stays_small() {
        let mut emits: Vec<String> = (0..500).map(|i| format!("vx.fan.{i}")).collect();
        emits.insert(1, "vx.fan.0".into());
        let r = rendered(&span(), Extras { emits, ..Extras::default() }, &[]);
        let listed = r["emits"].as_array().unwrap();
        assert_eq!(listed.len(), EMITS_MAX);
        assert_eq!(listed[0], "vx.fan.0");
        assert_eq!(listed[1], "vx.fan.1");
        let few = vec!["vx.a".to_string(), "vx.b".into(), "vx.a".into()];
        let r = rendered(&span(), Extras { emits: few, ..Extras::default() }, &[]);
        assert_eq!(r["emits"], json!(["vx.a", "vx.b"]));
    }

    #[test]
    fn long_texts_are_cut_on_a_character_boundary() {
        let text = "é".repeat(TEXT_MAX);
        let cut = clip(&text, TEXT_MAX);
        assert!(cut.ends_with('…'));
        assert!(cut.len() <= TEXT_MAX + "…".len());
        assert_eq!(clip("short", TEXT_MAX), "short");
    }

    #[test]
    fn only_a_trace_id_reads_a_trace() {
        assert_eq!(
            trace_key(" 4BF92F3577B34DA6A3CE929D0E0E4736 ").as_deref(),
            Some("4bf92f3577b34da6a3ce929d0e0e4736")
        );
        assert_eq!(trace_key("00000000000000000000000000000000"), None);
        assert_eq!(trace_key("4bf92f3577b34da6"), None);
        // a filter wildcard is no trace id
        assert_eq!(trace_key("4bf92f3577b34da6a3ce929d0e0e47>*"), None);
    }

    #[test]
    fn a_search_reads_payloads_and_names_not_ids_or_timings() {
        let hop: Value = rendered(&span(), with_input(br#"{"order":4711}"#), &[]);
        assert!(mentions(&hop, "4711"));
        assert!(mentions(&hop, "orders.in"));
        assert!(mentions(&hop, "flow:orders"));
        // the start time, 1764118847112345, holds "4711" too: not a match
        let mut quiet = hop.clone();
        quiet["input"] = json!({"order": 1});
        assert!(!mentions(&quiet, "4711"));
        assert!(!mentions(&quiet, "4bf92f35"));
        assert!(contains(b"abc", b"") && contains(b"abc", b"bc") && !contains(b"abc", b"cd"));
    }

    // ── what a second review found the scrub let through (2026-11-26) ──

    fn with_error(e: &str) -> Span {
        let mut s = span();
        s.error = Some(e.to_string());
        s
    }

    fn with_answer(bytes: &[u8]) -> Extras {
        Extras { answer: Some(Kept::copy(bytes)), ..Extras::default() }
    }

    /// A record, which must be JSON.
    fn record_of(span: &Span, extras: Extras, secrets: &[&str]) -> String {
        let out = render(span, &extras, &scrub(secrets));
        serde_json::from_str::<Value>(&out).unwrap_or_else(|e| panic!("not JSON ({e}): {out}"));
        out
    }

    #[test]
    fn a_url_gives_its_password_and_leaves_the_rest_to_be_read() {
        let t = |s: &str| text(s, &[], false);
        // a URL with no userinfo no longer swallows the key after it
        assert_eq!(
            t("endpoint=https://h&password=K1&next=https://x"),
            "endpoint=https://h&password=<masked>&next=https://x"
        );
        assert_eq!(
            t(r#"{"url":"https://api.example.com","api_key":"K2"#),
            r#"{"url":"https://api.example.com","api_key":"<masked>""#
        );
        let out = record_of(&with_error(r#"HTTP 401: {"issuer":"https://idp.example.com","client_secret":"K3"}"#), Extras::new(), &[]);
        assert!(!out.contains("K3"), "{out}");
        let body = input_of(br#"{"body":"{\"url\":\"https://a.example\",\"api_key\":\"K4\"}"}"#, &[]);
        assert!(!body.to_string().contains("K4"), "{body}");
        // the userinfo ends at the authority's last @, # and @ in a password included
        assert_eq!(t("redis://default:K5#x@redis:6379"), "redis://default:<masked>@redis:6379");
        assert_eq!(t("amqp://user:p@K6@host/vh"), "amqp://user:<masked>@host/vh");
        assert_eq!(
            t("smtp://alice@example.com:K7@smtp.example.com:587"),
            "smtp://alice@example.com:<masked>@smtp.example.com:587"
        );
        // a userinfo with no password is often a token
        assert_eq!(t("git clone https://K8@github.com/o/r.git"), "git clone https://<masked>@github.com/o/r.git");
        assert_eq!(t("http://host:8080/path"), "http://host:8080/path");
    }

    #[test]
    fn json_held_in_a_string_is_read_at_any_depth() {
        // an answer that is a JSON string holding JSON
        let r = rendered(&span(), with_answer(br#""{\"password\":\"K1\"}""#), &[]);
        let answer: String = serde_json::from_str(r["answer"].as_str().unwrap()).unwrap();
        assert_eq!(answer, r#"{"password":"<masked>"}"#);
        // SNS in SQS: JSON in a string in a string
        let i = input_of(br#"{"Body":"{\"Type\":\"Notification\",\"Message\":\"{\\\"password\\\":\\\"K2\\\"}\"}"}"#, &[]);
        assert!(!i.to_string().contains("K2"), "{i}");
        let inner: Value = serde_json::from_str(i["Body"].as_str().unwrap()).unwrap();
        let message: Value = serde_json::from_str(inner["Message"].as_str().unwrap()).unwrap();
        assert_eq!(message["password"], MASKED);
        // in a text, escaped once or twice
        assert_eq!(
            text(r#"HTTP 400: {"echo":"{\"password\":\"K3\"}"}"#, &[], false),
            r#"HTTP 400: {"echo":"{\"password\":\"<masked>\"}"}"#
        );
        let twice = r#"x {"Body":"{\"Message\":\"{\\\"api_key\\\":\\\"K4\\\",\\\"n\\\":1}\"}"}"#;
        let out = text(twice, &[], false);
        assert!(!out.contains("K4") && out.contains(r#"\\\"n\\\":1"#), "{out}");
        // an object under the key, escaped, goes whole
        let out = text(r#"x {"s":"{\"secret\":{\"a\":\"K5\",\"b\":[1]},\"keep\":2}"}"#, &[], false);
        assert_eq!(out, r#"x {"s":"{\"secret\":\"<masked>\",\"keep\":2}"}"#);
    }

    #[test]
    fn other_languages_give_keys_and_values_too() {
        let t = |s: &str| text(s, &[], false);
        assert_eq!(t("{'user': 'ana', 'password': 'K1'}"), "{'user': 'ana', 'password': '<masked>'}");
        let out = record_of(&with_error("exit 1: {'User-Agent': 'x', 'Authorization': 'Bearer K2'}"), Extras::new(), &[]);
        assert!(!out.contains("K2"), "{out}");
        assert_eq!(t(r#"{"password"=>"K3"}"#), r#"{"password"=>"<masked>"}"#);
        assert_eq!(t("Array ( [password] => K4 )"), "Array ( [password] => <masked> )");
        assert_eq!(t("user%5Bpassword%5D=K5&commit=Log+in"), "user%5Bpassword%5D=<masked>&commit=Log+in");
        assert_eq!(t(r#"level=error password="pa\"K6" user=ana"#), "level=error password=<masked> user=ana");
        assert_eq!(t("password = K7"), "password = <masked>");
        assert_eq!(t("authorization=bearer K8"), "authorization=<masked>");
        assert_eq!(t("<login password='K9' user='ana'/>"), "<login password=<masked> user='ana'/>");
    }

    #[test]
    fn an_xml_value_goes_to_its_end_tag() {
        let t = |s: &str| text(s, &[], false);
        assert_eq!(t("<Password><![CDATA[K1]]></Password>"), "<Password><masked></Password>");
        assert_eq!(t("<ClientSecret><Value>K2</Value></ClientSecret>"), "<ClientSecret><masked></ClientSecret>");
        assert_eq!(t(r#"<a:Password xmlns:a="x">K3</a:Password>"#), r#"<a:Password xmlns:a="x"><masked></a:Password>"#);
        // cut before its end tag: the rest goes
        assert_eq!(text("<x><Password>K4 and more", &[], true), "<x><Password><masked>");
    }

    #[test]
    fn a_header_or_a_yaml_key_masks_its_line() {
        let t = |s: &str| text(s, &[], false);
        assert_eq!(t("password:\n  K1\nnext: v"), "password:\n  <masked>\nnext: v");
        assert_eq!(t("Authorization:Bearer K2"), "Authorization:<masked>");
        // Digest: quoted parameters, commas between them
        let out = t(r#"Authorization: Digest username="a", response="K3""#);
        assert!(!out.contains("K3"), "{out}");
        // a header inside a JSON string: its closing quote ends the line
        assert_eq!(
            t(r#"{"note":"Authorization: Bearer K4","x":"keep"#),
            r#"{"note":"Authorization: <masked>","x":"keep"#
        );
        assert_eq!(t("{\"password\":\n  \"K5\"} x"), "{\"password\":\n  \"<masked>\"} x");
    }

    #[test]
    fn a_header_value_that_runs_on_still_leaves_the_next_credential_masked() {
        // the header's value is guessed to end at the quote before "n", inside
        // the value of x-auth: what follows must not escape
        let t = r#"x {"a":"["client_secret: Bearer K1",{"x-auth":{"inner":"K2","n":["K3",1]}}]"}"#;
        let out = text(t, &[], false);
        assert!(!out.contains("K1") && !out.contains("K2") && !out.contains("K3"), "{out}");
    }

    #[test]
    fn a_vault_value_is_found_as_a_text_escapes_it() {
        // escaped by the JSON around it, in a cut preview
        let big = format!(r#"{{"note":"pa\"ss-Word-K1x","blob":"{}"}}"#, "x".repeat(20_000));
        let i = input_of(big.as_bytes(), &["pa\"ss-Word-K1x"]);
        assert!(!i.to_string().contains("ss-Word-K1x"), "{i}");
        let big = format!(r#"{{"cb":"wJalrXUtnFEMI\/K7MDENG\/K2x","blob":"{}"}}"#, "x".repeat(20_000));
        let i = input_of(big.as_bytes(), &["wJalrXUtnFEMI/K7MDENG/K2x"]);
        assert!(!i.to_string().contains("K7MDENG"), "{i}");
        // Go escapes & < >
        let go = format!(r#"x "a{}b{}c-K3x""#, esc("0026"), esc("003c"));
        assert_eq!(text(&go, &["a&b<c-K3x"], false), r#"x "<secret>""#);
        // percent-encoded in a URL
        assert_eq!(text("https://h/?sig=abc%2Bdef%3D-K4x", &["abc+def=-K4x"], false), "https://h/?sig=<secret>");
    }

    #[test]
    fn a_vault_value_is_found_however_a_text_escapes_it() {
        // .NET escapes + and what is not ASCII, in upper case
        let dotnet = format!(r#"x "abc{}def/ghi=K5x""#, esc("002B"));
        assert_eq!(text(&dotnet, &["abc+def/ghi=K5x"], false), r#"x "<secret>""#);
        // Python and PHP escape what is not ASCII
        let python = format!(r#"x "mot-de-pass{}-K6x""#, esc("00e9"));
        assert_eq!(text(&python, &["mot-de-passé-K6x"], false), r#"x "<secret>""#);
        // a character past the first plane, as a surrogate pair
        let pair = format!(r#"x "key-{}{}-K7x""#, esc("d83d"), esc("de00"));
        assert_eq!(text(&pair, &["key-😀-K7x"], false), r#"x "<secret>""#);
        // encodeURIComponent leaves ! as it is
        assert_eq!(text("https://h/?p=pw!K8%2Bx&n=1", &["pw!K8+x"], false), "https://h/?p=<secret>&n=1");
        // a URL in a query, encoded twice
        assert_eq!(
            text("https://h/?next=%2F%3Fsig%3Dabc%252Bdef-K9&n=1", &["abc+def-K9"], false),
            "https://h/?next=%2F%3Fsig%3D<secret>&n=1"
        );
        // entities in XML
        assert_eq!(text("<p>tok&amp;en-K10x</p>", &["tok&en-K10x"], false), "<p><secret></p>");
        assert_eq!(text("<p>tok&#38;en-K11x</p>", &["tok&en-K11x"], false), "<p><secret></p>");
        // escaped three times: JSON in a string in a string, as a text
        let thrice = r#"x {"a":"{\"b\":\"{\\\"c\\\":\\\"pa\\\\\\\"ss-K12x\\\"}\"}"}"#;
        let out = text(thrice, &["pa\"ss-K12x"], false);
        assert!(!out.contains("K12x"), "{out}");
        // a text with nothing to read stays as it is
        let plain = r"C:\Users\new\table AT&T 100% R&D a\";
        assert_eq!(text(plain, &["nothing-here"], false), plain);
    }

    #[test]
    fn a_text_cut_inside_an_escape_loses_the_start_of_a_vault_value() {
        let value = "abcdef+ghijK13";
        for error in [
            format!(r#"HTTP 400: {{"echo":"abcdef{}"#, &esc("002B")[..4]),
            "HTTP 400: sig=abcdef%2".to_string(),
            "HTTP 400: <p>abcdef&#4".to_string(),
        ] {
            let r = rendered(&with_error(&error), Extras::new(), &[value]);
            assert!(!r["error"].as_str().unwrap().contains("abcdef"), "{error}: {r}");
        }
        let r = rendered(&with_error("HTTP 400: abcde"), Extras::new(), &["xyzwvu-K14"]);
        assert_eq!(r["error"], "HTTP 400: abcde");
    }

    /// A shape down every path a record has: whole, cut, as an error, as an
    /// answer whole and cut, and held in a JSON string.
    fn every_path(shape: &str, secrets: &[&str]) -> Vec<(&'static str, String)> {
        let filler = "f".repeat(INPUT_MAX + 100);
        let in_string = json!({ "body": shape }).to_string();
        let in_string_cut = format!(r#"{{"body":{},"blob":"{filler}"}}"#, Value::String(shape.to_string()));
        vec![
            ("payload", span(), with_input(shape.as_bytes())),
            ("payload cut", span(), with_input(format!("{shape}{filler}").as_bytes())),
            ("error", with_error(&format!("HTTP 500: {shape}")), Extras::new()),
            ("answer", span(), with_answer(shape.as_bytes())),
            ("answer cut", span(), with_answer(format!("{shape}{filler}").as_bytes())),
            ("in a string", span(), with_input(in_string.as_bytes())),
            ("in a string, cut", span(), with_input(in_string_cut.as_bytes())),
            ("in a string, error", with_error(&format!("HTTP 500: {in_string}")), Extras::new()),
        ]
        .into_iter()
        .map(|(path, s, extras)| (path, record_of(&s, extras, secrets)))
        .collect()
    }

    #[test]
    fn a_key_escaped_or_spelled_another_way_still_hides_its_value() {
        let (lt, gt) = (esc("003c"), esc("003e"));
        let go = format!(r#"{{"echo":"{lt}auth{gt}{lt}Password{gt}K04{lt}/Password{gt}{lt}/auth{gt}"}}"#);
        let shapes: Vec<(&str, String, &str)> = vec![
            ("XML in SOAP, as entities", "<ns:request>&lt;Login&gt;&lt;Password&gt;K01&lt;/Password&gt;&lt;/Login&gt;</ns:request>".into(), "K01"),
            ("JSON in an XML attribute", r#"<event body="{&quot;user&quot;:&quot;ana&quot;,&quot;password&quot;:&quot;K02&quot;}"/>"#.into(), "K02"),
            ("a URL escaped by PHP", r#"{"error":"cannot connect","dsn":"mysql:\/\/app:K03@db:3306\/shop"}"#.into(), "K03"),
            ("XML escaped by Go", go, "K04"),
            ("a query in a query", "GET https://app.example/login?return_to=%2Fcb%3Faccess_token%3DK05%26state%3Dx".into(), "K05"),
            ("JSON in a form field", "payload=%7B%22user%22%3A%22ana%22%2C%22password%22%3A%22K06%22%7D".into(), "K06"),
            ("key=\"value\" escaped in JSON", r#"{"message":"PlainLoginModule required username=\"ana\" password=\"K07\";"}"#.into(), "K07"),
            ("container log lines", "{\"log\":\"level=info msg=start\\n\"}\n{\"log\":\"level=error password=\\\"K08\\\" user=ana\\n\"}\n".into(), "K08"),
            ("an XML attribute in a JSON string", r#"{"request":"<login user=\"ana\" password=\"K09\"/>"}"#.into(), "K09"),
            ("a YAML literal block", "db:\n  user: ana\n  password: |\n    K10\n  host: db\n".into(), "K10"),
            ("a YAML folded block", "client_secret: >-\n  K11\nscope: read\n".into(), "K11"),
            ("a Java map", "{user=ana, password=correct horse K12, host=db}".into(), "K12"),
            ("a connection string", "Server=db;User Id=ana;Password=my pass K13;Encrypt=true".into(), "K13"),
            ("a Python bytes repr", "{'user': 'ana', 'password': b'x K14'}".into(), "K14"),
            ("HCL", "environment = {\n  \"DB_PASSWORD\" = \"K15\"\n}".into(), "K15"),
            ("a ' in a URL's password", "postgres://app:x'K16@db:5432/app".into(), "K16"),
            ("a command line", "mysqldump --user ana --password K17 shop: exit 2".into(), "K17"),
            ("PHP var_dump", "array(2) {\n  [\"user\"]=>\n  string(3) \"ana\"\n  [\"password\"]=>\n  string(3) \"K18\"\n}\n".into(), "K18"),
            ("PHP var_dump, a property", "object(Db)#1 (1) {\n  [\"password\":protected]=>\n  string(3) \"K19\"\n}\n".into(), "K19"),
        ];
        for (name, shape, planted) in &shapes {
            for (path, out) in every_path(shape, &[]) {
                assert!(!out.contains(planted), "{name}, {path}: {out}");
            }
        }
        // what is not a credential is left alone
        let r = rendered(&with_error("HTTP 500: mysqldump --user ana shop: exit 2"), Extras::new(), &[]);
        assert_eq!(r["error"], "HTTP 500: mysqldump --user ana shop: exit 2");
    }

    #[test]
    fn a_vault_value_forwarded_whole_or_cut_inside_a_character_is_taken_out() {
        // a service account's key file, one secret, forwarded as JSON
        let sa = r#"{"type":"service_account","project_id":"p1","private_key":"-----BEGIN PRIVATE KEY-----\nMIIEvK31xyz\n-----END PRIVATE KEY-----\n","client_email":"x@p1.iam.gserviceaccount.com"}"#;
        for (path, out) in every_path(&format!(r#"{{"creds":{sa},"n":1}}"#), &[sa]) {
            assert!(!out.contains("MIIEvK31"), "{path}: {out}");
        }
        // \' as JavaScript and Python write it
        for (path, out) in every_path(r#"{'note': 'it\'s K32', "q": "\""}"#, &["it's K32"]) {
            assert!(!out.contains("K32"), "{path}: {out}");
        }
        // a connector's 300-byte cut falls inside a character of the value
        let value = "sk_live_K33é0123456789abcdef";
        let body = format!(r#"{{"echo":"{}{value}"}}"#, "y".repeat(277));
        let cut = String::from_utf8_lossy(&body.as_bytes()[..300]).into_owned();
        let out = record_of(&with_error(&format!("HTTP 400: {cut}")), Extras::new(), &[value]);
        assert!(!out.contains("sk_live_K33"), "{out}");
        // a vault value that is a document does not turn every JSON into text
        let r = rendered(&span(), with_input(br#"{"a":1}"#), &[sa]);
        assert_eq!(r["input"], json!({"a": 1}));
    }

    #[test]
    fn many_credentials_in_one_text_cost_bounded_time() {
        for unit in ["password: ", "password=", r#""secret":{"#, "<Password>", "<Password></x>", "--password "] {
            let shape = unit.repeat(INPUT_MAX / unit.len());
            let started = Instant::now();
            let out = record_of(&with_error(&shape), with_input(shape.as_bytes()), &[]);
            assert!(started.elapsed() < Duration::from_secs(2), "{unit}: {:?}", started.elapsed());
            assert!(out.len() < 4 * INPUT_MAX, "{unit}");
        }
    }

    #[test]
    fn a_vault_value_never_hides_a_key_from_the_mask() {
        assert_eq!(text("client_secret=K1", &["secret"], false), "client_<secret>=<masked>");
        assert_eq!(text("apikey=AC81f2:K2", &["AC81f2"], false), "apikey=<masked>");
        assert_eq!(text("<Password>hunter-0001K3</Password>", &["hunter-0001"], false), "<Password><masked></Password>");
        assert_eq!(text("api_key=sk_live_0001-K4", &["sk_live_0001"], false), "api_key=<masked>");
    }

    #[test]
    fn an_error_cut_upstream_loses_the_start_of_a_vault_value() {
        let value = "sk_live_51HxK5abcdefghijklmnop";
        let body = format!(r#"{{"error":"bad request","echo":"{}{value}"}}"#, "y".repeat(254));
        // http_post keeps the first 300 bytes of a failing answer
        let error = format!("HTTP 400: {}", String::from_utf8_lossy(&body.as_bytes()[..300]));
        let out = record_of(&with_error(&error), Extras::new(), &[value]);
        assert!(!out.contains("sk_live_51Hx"), "{out}");
        // three bytes are no start worth taking out
        let r = rendered(&with_error("ends with abc"), Extras::new(), &["abcdef"]);
        assert_eq!(r["error"], "ends with abc");
    }

    #[test]
    fn keys_and_long_numbers_lose_what_they_carry() {
        let i = input_of(br#"{"amqp://u:K1@broker:5672":"down"}"#, &[]);
        assert_eq!(i, json!({"amqp://u:<masked>@broker:5672": "down"}));
        let i = input_of(br#"{"acct":123456789012345678901234}"#, &["123456789012345678901234"]);
        assert_eq!(i, json!({"acct": "<secret>"}));
    }

    #[test]
    fn every_record_is_json_however_deep_its_payload() {
        for depth in [99usize, 100, 101, 126, 127, 128, 5000] {
            let arrays = format!("{}{}", "[".repeat(depth), "]".repeat(depth));
            let objects = format!(r#"{}{{"password":"K1"}}{}"#, r#"{"a":"#.repeat(depth), "}".repeat(depth));
            for input in [arrays, objects] {
                let out = record_of(&span(), with_input(input.as_bytes()), &[]);
                assert!(!out.contains("K1"), "depth {depth}");
            }
        }
    }

    #[test]
    fn vault_values_nested_in_one_another_cost_bounded_time() {
        let values: Vec<String> = (4..404).map(|k| "a".repeat(k)).collect();
        let refs: Vec<&str> = values.iter().map(String::as_str).collect();
        let started = Instant::now();
        assert_eq!(text(&format!("x{}", "a".repeat(INPUT_MAX)), &refs, false), "<secret>");
        assert!(started.elapsed() < Duration::from_secs(2), "{:?}", started.elapsed());
        // a few, as a vault holds them, still go one by one
        assert_eq!(text("x aaaa y aaaaa z", &refs[..2], false), "x <secret> y <secret> z");
    }

    #[test]
    fn a_long_error_is_scrubbed_in_bounded_time() {
        let long = "abcd a=".repeat(150_000);
        let started = Instant::now();
        let out = record_of(&with_error(&long), Extras::new(), &["abcd"]);
        assert!(started.elapsed() < Duration::from_secs(2), "{:?}", started.elapsed());
        assert!(!out.contains("abcd"));
        // and it weighs in the queue what it holds
        assert!(queued_weight(&with_error(&long), &Extras::new()) > long.len());
    }

    /// xorshift: the same shapes on every run.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            self.0 = x;
            x.wrapping_mul(0x2545F4914F6CDD1D)
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
        fn chance(&mut self, percent: u64) -> bool {
            self.below(100) < percent
        }
        fn pick<'a>(&mut self, xs: &[&'a str]) -> &'a str {
            xs[self.below(xs.len() as u64) as usize]
        }
    }

    const CREDENTIAL_KEYS: &[&str] = &[
        "password", "passwd", "client_secret", "secret", "api_key", "apiKey", "X-Api-Key", "access_token",
        "refresh_token", "token", "auth", "x-auth", "auth_header", "Authorization", "webhook_url", "DB_PASSWORD",
        "x-auth-token", "Secret",
    ];
    /// Credential names that every text form can carry.
    const CREDENTIAL_WORDS: &[&str] =
        &["password", "client_secret", "api_key", "X-Api-Key", "access_token", "Authorization", "DB_PASSWORD"];
    const PLAIN_KEYS: &[&str] = &[
        "a", "base", "base_url", "body", "cb", "data", "endpoint", "id", "items", "message", "name", "note", "status",
        "token_url", "max_tokens", "url", "user", "zz",
    ];
    const WORDS: &[&str] = &["order", "ok", "alpha", "beta", "x1", "42", "done", "queued", "lorem", "ipsum"];

    /// Payloads with credentials and vault values planted in every shape the
    /// record claims to scrub.
    struct Planter {
        rng: Rng,
        credentials: Vec<String>,
        vault: Vec<String>,
        n: u64,
    }

    impl Planter {
        fn token(&mut self, prefix: &str) -> String {
            self.n += 1;
            format!("{prefix}{}Q{:x}", self.n, self.rng.next() & 0xfffff)
        }
        fn credential(&mut self) -> String {
            let t = self.token("Cr");
            self.credentials.push(t.clone());
            t
        }
        fn vault_text(&mut self) -> String {
            let t = self.token("Vt");
            self.vault.push(t.clone());
            t
        }
        fn vault_number(&mut self) -> u64 {
            let v = 7_000_000_000 + self.rng.below(1_000_000_000);
            self.vault.push(v.to_string());
            v
        }
        fn words(&mut self) -> String {
            (0..1 + self.rng.below(3)).map(|_| self.rng.pick(WORDS)).collect::<Vec<_>>().join(" ")
        }
        fn text_value(&mut self) -> String {
            let mut parts = Vec::new();
            for _ in 0..1 + self.rng.below(3) {
                let k = self.rng.pick(CREDENTIAL_WORDS);
                parts.push(match self.rng.below(12) {
                    0 => format!("{k}={}", self.credential()),
                    1 => format!("{k}: Bearer {}", self.credential()),
                    2 => format!("<{k}>{}</{k}>", self.credential()),
                    3 => format!("https://user:{}@host.example/p", self.credential()),
                    4 => format!("https://h.example/p?{k}={}&x=1", self.credential()),
                    5 | 6 => self.vault_text(),
                    7 => "https://origin.example".to_string(),
                    _ => self.words(),
                });
            }
            parts.join(" ")
        }
        fn secret_value(&mut self) -> Value {
            match self.rng.below(5) {
                0 => json!({"inner": self.credential(), "n": [self.credential(), 1]}),
                1 => json!([self.credential(), 5]),
                2 => Value::String(format!("Bearer {}", self.credential())),
                _ => Value::String(self.credential()),
            }
        }
        fn value(&mut self, depth: u32) -> Value {
            match self.rng.below(if depth > 3 { 5 } else { 9 }) {
                0 | 1 => Value::String(self.text_value()),
                2 => Value::String(if self.rng.chance(50) { "https://origin.example".into() } else { self.words() }),
                3 if self.rng.chance(30) => json!(self.vault_number()),
                3 => json!(self.rng.below(100_000)),
                4 => json!(self.rng.chance(50)),
                5 | 6 => self.object(depth + 1),
                7 => Value::Array((0..self.rng.below(4)).map(|_| self.value(depth + 1)).collect()),
                _ => Value::String(self.object(depth + 1).to_string()),
            }
        }
        fn object(&mut self, depth: u32) -> Value {
            let mut m = serde_json::Map::new();
            for _ in 0..1 + self.rng.below(6) {
                if self.rng.chance(30) {
                    let k = self.rng.pick(CREDENTIAL_KEYS);
                    let v = self.secret_value();
                    m.insert(k.to_string(), v);
                } else if self.rng.chance(8) {
                    let k = self.vault_text();
                    let v = self.value(depth);
                    m.insert(k, v);
                } else {
                    let k = self.rng.pick(PLAIN_KEYS);
                    let v = self.value(depth);
                    m.insert(k.to_string(), v);
                }
            }
            Value::Object(m)
        }
    }

    #[test]
    fn no_planted_credential_or_vault_value_survives_any_path() {
        // RECORD_SCRUB_ROUNDS=5000 for a longer run
        let rounds: u64 = std::env::var("RECORD_SCRUB_ROUNDS").ok().and_then(|v| v.parse().ok()).unwrap_or(120);
        let filler = "f".repeat(INPUT_MAX + 100);
        for seed in 1..=rounds {
            let mut p = Planter { rng: Rng(seed.wrapping_mul(0x9E3779B97F4A7C15) | 1), credentials: vec![], vault: vec![], n: 0 };
            let doc = p.object(0);
            let compact = doc.to_string();
            let secrets: Vec<&str> = p.vault.iter().map(String::as_str).collect();
            let paths: Vec<(&str, Span, Extras)> = vec![
                ("payload", span(), with_input(compact.as_bytes())),
                ("payload, pretty", span(), with_input(serde_json::to_string_pretty(&doc).unwrap().as_bytes())),
                ("payload, not JSON", span(), with_input(format!("{compact} x").as_bytes())),
                ("payload, cut", span(), with_input(format!("{compact}{filler}").as_bytes())),
                ("payload, in a string", span(), with_input(json!({"body": compact}).to_string().as_bytes())),
                ("error", with_error(&format!("HTTP 500: {compact}")), Extras::new()),
                ("answer", span(), with_answer(compact.as_bytes())),
                ("answer, a JSON string", span(), with_answer(Value::String(compact.clone()).to_string().as_bytes())),
            ];
            for (path, s, extras) in paths {
                let out = record_of(&s, extras, &secrets);
                for planted in p.credentials.iter().chain(&p.vault) {
                    assert!(!out.contains(planted.as_str()), "seed {seed}, {path}: {planted} in {out}");
                }
            }
        }
    }

    #[test]
    fn any_bytes_give_a_record_that_is_json() {
        let mut rng = Rng(0xDEADBEEF);
        let pieces: &[&[u8]] = &[
            b"{", b"}", b"[", b"]", b"\"", b":", b",", b"\\", b"\\\"", b"password", b"api_key", b"=", b"<", b">",
            b"</", b"Password", b"://", b"@", b"\n", b"\r", b"\t", b"\x00", b"\x01", b"\x1f", b"\x7f", b"\xff", b"\xc3",
            b"\xe2\x82", b"\xf0\x9f\x98", b"\xed\xa0\x80", b"\xc3\xa9", b"1e999", b"-0", b"123456789012345678901234567890",
            b"null", b"true", b"s3cr3t-v", b"alue", b" ", b"<![CDATA[", b"]]>", b"Bearer ", b"%5B", b"=>", b"'",
        ];
        let secrets = ["s3cr3t-value", "abcd", "\"quoted\"", "é-secret"];
        let sc = scrub(&secrets);
        for round in 0..300u64 {
            let target = match round % 4 {
                0 => rng.below(64) as usize,
                1 => rng.below(2048) as usize,
                2 => INPUT_MAX - 8 + rng.below(16) as usize,
                _ => INPUT_MAX + rng.below(4096) as usize,
            };
            let mut bytes = Vec::new();
            while bytes.len() < target {
                bytes.extend_from_slice(pieces[rng.below(pieces.len() as u64) as usize]);
            }
            let mut s = span();
            s.error = Some(String::from_utf8_lossy(&bytes).into_owned());
            let lossy = String::from_utf8_lossy(&bytes).into_owned();
            let extras = Extras {
                emits: vec![String::from_utf8_lossy(&bytes[..bytes.len().min(40)]).into_owned()],
                input: Some(Kept::copy(&bytes)),
                answer: Some(Kept::copy(&bytes)),
                steps: Some(steps_of(vec![
                    (None, What::Emit(1, lossy.clone())),
                    (Some("default:x"), What::Emit(2, lossy)),
                    (None, What::Respond(3, 500)),
                ])),
            };
            let out = render(&s, &extras, &sc);
            serde_json::from_str::<Value>(&out).unwrap_or_else(|e| panic!("round {round}, not JSON ({e}): {out}"));
            assert!(!out.contains("s3cr3t-value"), "round {round}: {out}");
        }
    }
}
