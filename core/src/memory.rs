//! What `seen` reads and `remember` writes (ADR-0039, point 4): a key-value
//! bucket of each flow's on the bus, `VEJAS_SEEN_` and the flow's name. A key
//! is stored as its SHA-256, which any text a key holds fits in, and its value
//! is when its window passes, in milliseconds since the epoch: a bucket keeps
//! each key as long as the longest window its flow gave, and `seen` reads the
//! window of each.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use vejas_connector::bus;

/// A flow's memory. Its bucket is opened at the first `seen` that finds one,
/// or created at the first mark.
pub struct Bucket {
    js: bus::JetStream,
    /// The bucket's name, in the characters a bucket's name takes.
    name: String,
    state: Mutex<State>,
}

/// How long a bucket that is not there is taken as absent, before `seen`
/// asks again: a flow that reads keys before it marks any asks once a second
/// at most, not at each message.
const ABSENT_FOR: Duration = Duration::from_secs(1);

/// How often a mark reads again how long the bucket keeps its keys: another
/// instance, running another version of the flow, may have set it in the same
/// instant to a shorter window.
const RECHECK: Duration = Duration::from_secs(60);

#[derive(Default)]
struct State {
    kv: Option<bus::Kv>,
    /// How long the bucket keeps a key, as last read or set: zero for no limit.
    max_age: Duration,
    /// When `max_age` was read.
    checked: Option<Instant>,
    /// Until when the bucket is taken as absent.
    absent_until: Option<Instant>,
    /// The marks of the runs whose emits wait for the stream's answer, each
    /// with when its window passes: a copy of their message in the same round
    /// reads them before they are written.
    held: HashMap<String, u64>,
}

/// The name of a flow's bucket: `VEJAS_SEEN_` and the flow's name, each
/// character a bucket's name does not take written `_`, then the start of a
/// hash of the name, so that `flow:a:b` and `flow:a_b` have one each.
pub fn bucket_name(unit: &str) -> String {
    let readable: String = unit
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect();
    let digest = ring::digest::digest(&ring::digest::SHA256, unit.as_bytes());
    let short: String = digest.as_ref()[..4].iter().map(|b| format!("{b:02x}")).collect();
    format!("VEJAS_SEEN_{readable}_{short}")
}

/// A key as the bucket stores it: its SHA-256, in hexadecimal.
fn hashed(key: &str) -> String {
    let digest = ring::digest::digest(&ring::digest::SHA256, key.as_bytes());
    digest.as_ref().iter().map(|b| format!("{b:02x}")).collect()
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

/// When a window that starts now passes.
fn until(window: Duration) -> u64 {
    now_ms().saturating_add(window.as_millis() as u64)
}

impl Bucket {
    pub fn of(js: &bus::JetStream, unit: &str) -> Arc<Bucket> {
        Arc::new(Bucket { js: js.clone(), name: bucket_name(unit), state: Mutex::default() })
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The bucket. A read opens it when it exists: none, and no run has
    /// remembered anything. A mark creates it, or makes it keep a key at
    /// least `window`: a bucket keeps its keys as long as the longest window
    /// a run gave, and never less once it did.
    fn open(&self, window: Option<Duration>) -> Result<Option<bus::Kv>, String> {
        let mut state = self.state();
        let now = Instant::now();
        if let Some(kv) = &state.kv {
            let stale = state.checked.is_none_or(|at| now.duration_since(at) > RECHECK);
            let longer = window.is_some_and(|w| !state.max_age.is_zero() && (w > state.max_age || stale));
            if !longer {
                return Ok(Some(kv.clone()));
            }
        } else if window.is_none() && state.absent_until.is_some_and(|until| now < until) {
            return Ok(None);
        }
        let stream = format!("KV_{}", self.name);
        // A bus that does not answer is no absent bucket: `seen` would read
        // a duplicate as new.
        let kept = self
            .js
            .stream_state_if_any(&stream)
            .map_err(|e| format!("the bucket {} did not answer: {e}", self.name))?
            .map(|s| s.max_age);
        let max_age = match (kept, window) {
            (None, None) => {
                state.absent_until = Some(now + ABSENT_FOR);
                return Ok(None);
            }
            (None, Some(window)) => window,
            (Some(kept), Some(window)) if !kept.is_zero() && window > kept => {
                self.js.put_stream(&bus::StreamConfig {
                    name: stream.clone(),
                    subjects: vec![format!("$KV.{}.>", self.name)],
                    max_msgs_per_subject: 1,
                    max_age: window,
                    discard: bus::Discard::New,
                    allow_rollup: true,
                    deny_delete: true,
                    ..Default::default()
                })?;
                window
            }
            (Some(kept), _) => kept,
        };
        let kv = self
            .js
            .key_value(&bus::KvConfig { bucket: self.name.clone(), history: 1, max_age })
            .ok_or_else(|| format!("the bucket {} could not be opened", self.name))?;
        state.max_age = max_age;
        state.checked = Some(now);
        state.absent_until = None;
        state.kv = Some(kv.clone());
        Ok(Some(kv))
    }

    /// The marks of a run whose emits wait for the stream's answer: `seen`
    /// reads them at once.
    pub fn hold(&self, marks: &[(String, Duration)]) {
        if marks.is_empty() {
            return;
        }
        let mut state = self.state();
        for (key, window) in marks {
            state.held.insert(key.clone(), until(*window));
        }
    }

    /// The stream answered for the run's emits, which it stored or refused:
    /// its marks are held no longer.
    pub fn release(&self, marks: &[(String, Duration)]) {
        if marks.is_empty() {
            return;
        }
        let mut state = self.state();
        for (key, _) in marks {
            state.held.remove(key);
        }
    }

    /// Mark each key, once the run's emits are stored: `seen` reads it until
    /// its window passes.
    pub fn write(&self, marks: &[(String, Duration)]) -> Result<(), String> {
        let Some(longest) = marks.iter().map(|(_, window)| *window).max() else {
            return Ok(());
        };
        let kv = self.open(Some(longest))?.ok_or("no bucket")?;
        for (key, window) in marks {
            kv.put(&hashed(key), until(*window).to_string())
                .map_err(|e| format!("the bucket {} did not store a mark: {e}", self.name))?;
        }
        Ok(())
    }
}

impl crate::vjs::Memory for Bucket {
    fn seen(&self, key: &str) -> Result<bool, String> {
        let now = now_ms();
        if self.state().held.get(key).is_some_and(|&until| until > now) {
            return Ok(true);
        }
        let Some(kv) = self.open(None)? else {
            return Ok(false);
        };
        let value = kv.get(&hashed(key)).map_err(|e| format!("the bucket {} did not answer: {e}", self.name))?;
        Ok(value
            .as_deref()
            .and_then(|v| std::str::from_utf8(v).ok())
            .and_then(|v| v.parse::<u64>().ok())
            .is_some_and(|until| until > now))
    }
}

/// The memory of an `api` flow, kept across its requests: the marks one
/// request holds, another reads. It is opened on the bus's shared connection
/// the first time a request needs it.
pub fn of_api(unit: &str) -> Result<Arc<Bucket>, String> {
    static BUCKETS: OnceLock<Mutex<HashMap<String, Arc<Bucket>>>> = OnceLock::new();
    // the shared connection, which says at once when the bus is down
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let bus = bus::connect(&url).map_err(|e| format!("NATS unreachable: {e}"))?;
    let mut buckets = BUCKETS.get_or_init(Default::default).lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    Ok(buckets.entry(unit.to_string()).or_insert_with(|| Bucket::of(&bus.jetstream(), unit)).clone())
}

/// What an `api` flow's run reads `seen` from: its memory, opened when the
/// run first reads it, so that a flow that never does opens none.
pub struct OfApi(pub String);

impl crate::vjs::Memory for OfApi {
    fn seen(&self, key: &str) -> Result<bool, String> {
        of_api(&self.0)?.seen(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bucket_name_takes_only_what_names_take_and_two_flows_never_share_one() {
        let name = bucket_name("flow:quote_refine");
        assert!(name.starts_with("VEJAS_SEEN_flow_quote_refine_"), "{name}");
        assert!(name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-'), "{name}");
        assert_ne!(bucket_name("flow:a:b"), bucket_name("flow:a_b"));
        assert_eq!(hashed("quote:ABC").len(), 64);
        assert_ne!(hashed("quote:ABC"), hashed("quote:ABD"));
    }
}
