//! Singleton lease for the MQ source (ADR-0020, ADR-0023 "singleton by default
//! BECAUSE order"). RabbitMQ round-robins N consumers, so AMQP is competing-*safe* — two getters never
//! receive the same message — so the source could scale with no lease at all. The
//! one thing N getters lose is ORDER: the queue's FIFO does not survive onto the
//! bus (transport test T1 asserts per-subject FIFO). So the default source takes a
//! lease and exactly one instance gets; `VEJAS_MQ_COMPETING=1` skips it (throughput
//! over order, chosen on purpose).
//!
//! Same JetStream KV mechanism as the core runtime (create = acquire, update-CAS =
//! renew/fence, delete = release, bucket max_age = crash failover) — reimplemented
//! here because the connector is a standalone binary with its own NATS client, not
//! linked to the core. The bucket is shared (VEJAS_LEASES), keys namespaced, so it
//! coordinates with the fleet when they run on the same bus.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

const LEASE_BUCKET: &str = "VEJAS_LEASES";

/// Lease TTL (bucket `max_age`); `VEJAS_LEASE_TTL_SECS` tunes it, default 10s.
pub fn lease_ttl() -> Duration {
    let secs = std::env::var("VEJAS_LEASE_TTL_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&s| s > 0)
        .unwrap_or(10);
    Duration::from_secs(secs)
}

/// This process's identity, stable for its lifetime — `VEJAS_INSTANCE` overrides,
/// else host + pid (unique enough to fence a lease).
pub fn instance_id() -> String {
    if let Ok(v) = std::env::var("VEJAS_INSTANCE") {
        if !v.is_empty() {
            return v;
        }
    }
    let host = std::env::var("HOSTNAME")
        .ok()
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| "host".into());
    format!("{host}-{}", std::process::id())
}

/// The lease key for an AMQP source on `queue` — namespaced so it never collides with
/// the core runtime's own connector/flow lease keys.
pub fn source_key(queue: &str) -> String {
    let q: String = queue
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    format!("amqp_source_{q}")
}

/// Open (or create) the shared lease bucket. None if JetStream KV is unavailable —
/// the caller then runs unleased (single-instance behaviour, no worse than today).
pub fn open_store(js: &nats::jetstream::JetStream) -> Option<nats::kv::Store> {
    let cfg = nats::kv::Config {
        bucket: LEASE_BUCKET.to_string(),
        max_age: lease_ttl(),
        history: 1,
        ..Default::default()
    };
    js.create_key_value(&cfg)
        .or_else(|_| js.key_value(LEASE_BUCKET))
        .ok()
}

/// Held while this instance owns the lease. Dropping it stops renewal and releases
/// the lease (instant handoff on shutdown). `lost()` goes true if the renewal
/// thread finds the lease fenced or aged out — the caller stops getting.
pub struct Lease {
    store: nats::kv::Store,
    key: String,
    stop_renew: Arc<AtomicBool>,
    lost: Arc<AtomicBool>,
}

impl Lease {
    /// True once the lease can no longer be held (fenced by another instance, or
    /// aged out under a stall). The source loop checks this each round.
    pub fn lost(&self) -> bool {
        self.lost.load(Ordering::SeqCst)
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.stop_renew.store(true, Ordering::SeqCst);
        match self.store.delete(&self.key) {
            Ok(()) => eprintln!("[vejas-amqp] lease released: {}", self.key),
            Err(e) => eprintln!("[vejas-amqp] lease release failed for {}: {e}", self.key),
        }
    }
}

/// Block until this instance holds `key`, then return a guard and spawn a renewal
/// thread. Returns None if `alive()` goes false before acquisition (asked to stop
/// while standing by).
pub fn acquire_blocking(
    store: &nats::kv::Store,
    key: &str,
    alive: impl Fn() -> bool,
) -> Option<Lease> {
    let id = instance_id();
    let rev = loop {
        if !alive() {
            return None;
        }
        match store.create(key, id.as_bytes()) {
            Ok(r) => {
                eprintln!("[vejas-amqp] lease acquired: {key} ({id})");
                break r;
            }
            Err(_) => thread::sleep(Duration::from_millis(1000)), // held elsewhere
        }
    };
    let stop_renew = Arc::new(AtomicBool::new(false));
    let lost = Arc::new(AtomicBool::new(false));
    {
        let (store, key, stop, lost) =
            (store.clone(), key.to_string(), stop_renew.clone(), lost.clone());
        let renew_every = (lease_ttl() / 3).max(Duration::from_secs(1));
        thread::spawn(move || {
            let id = instance_id();
            let mut rev = rev;
            let tick = Duration::from_millis(200);
            loop {
                let mut waited = Duration::ZERO;
                while waited < renew_every {
                    thread::sleep(tick);
                    if stop.load(Ordering::SeqCst) {
                        return;
                    }
                    waited += tick;
                }
                match store.update(&key, id.as_bytes(), rev) {
                    Ok(r) => rev = r,
                    Err(_) => {
                        // fenced or aged out — tell the source to stop getting.
                        lost.store(true, Ordering::SeqCst);
                        return;
                    }
                }
            }
        });
    }
    Some(Lease { store: store.clone(), key: key.to_string(), stop_renew, lost })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_key_is_namespaced_and_sanitised() {
        assert_eq!(source_key("DEV.QUEUE.1"), "amqp_source_DEV_QUEUE_1");
        assert_eq!(source_key("orders"), "amqp_source_orders");
    }

    // Mutual exclusion against real NATS KV: one acquires, a second is blocked
    // until the first releases. Needs a live NATS on 127.0.0.1:4222; excluded from
    // CI. Run: cargo test lease_is_mutually_exclusive -- --ignored --nocapture
    #[test]
    #[ignore]
    fn lease_is_mutually_exclusive() {
        let url = std::env::var("NATS_URL").unwrap_or_else(|_| "127.0.0.1:4222".into());
        let nc = nats::connect(&url).expect("connect");
        let js = nats::jetstream::new(nc);
        let store = open_store(&js).expect("lease store");
        let key = format!("amqp_test_{}", std::process::id());
        let _ = store.delete(&key);

        // first holder acquires immediately
        let held = acquire_blocking(&store, &key, || true).expect("first acquires");
        // a second create must fail while the first holds it
        assert!(store.create(&key, b"other").is_err(), "second cannot acquire while held");
        // release → a create now succeeds
        drop(held);
        // Drop deletes the key; give the delete a moment to propagate.
        thread::sleep(Duration::from_millis(200));
        let rev = store.create(&key, b"other");
        assert!(rev.is_ok(), "after release a new holder acquires");
        let _ = store.delete(&key);
    }

    // CAS fencing: an update with a stale revision must fail (so a paused leader
    // that wakes cannot renew over a new one).
    #[test]
    #[ignore]
    fn stale_revision_is_fenced() {
        let url = std::env::var("NATS_URL").unwrap_or_else(|_| "127.0.0.1:4222".into());
        let nc = nats::connect(&url).expect("connect");
        let js = nats::jetstream::new(nc);
        let store = open_store(&js).expect("lease store");
        let key = format!("amqp_fence_{}", std::process::id());
        let _ = store.delete(&key);

        let r1 = store.create(&key, b"a").expect("create");
        let r2 = store.update(&key, b"a", r1).expect("first renew");
        assert!(store.update(&key, b"a", r1).is_err(), "stale revision fenced");
        assert!(store.update(&key, b"a", r2).is_ok(), "current revision renews");
        let _ = store.delete(&key);
    }
}
