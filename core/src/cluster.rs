//! Singleton leader-election for clustered deployments (ADR-0020).
//!
//! Flows and sinks are competing-consumer safe (a shared JetStream durable
//! load-balances across N instances) and need nothing here. A *singleton* source
//! — a timer, a poller, an inbound registration — must run on exactly one
//! instance, or N instances produce N× the events. Such a driver acquires a
//! **lease** in a JetStream KV bucket before it runs:
//!
//!   - acquire  = `Store::create` (atomic create-if-absent — exactly one wins)
//!   - renew    = `Store::update(key, id, revision)` (compare-and-set; a paused
//!                leader that wakes with a stale revision fails and stands down —
//!                fencing, so two instances never *keep* running the same unit)
//!   - release  = `Store::delete` on graceful shutdown (instant handoff)
//!   - failover = the bucket's `max_age` ages the value out after a crash, so a
//!                stand-by's `create` then succeeds (bounded by the TTL)
//!
//! No sticky coordinator, no external quorum: the bus is the coordination.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::Duration;

const LEASE_BUCKET: &str = "VEJAS_LEASES";

/// This process's identity, stable for its lifetime. `VEJAS_INSTANCE` overrides
/// (tests, explicit naming); otherwise host + pid, unique enough to fence a lease.
pub fn instance_id() -> &'static str {
    static ID: OnceLock<String> = OnceLock::new();
    ID.get_or_init(|| {
        if let Ok(v) = std::env::var("VEJAS_INSTANCE") {
            if !v.is_empty() {
                return v;
            }
        }
        let host = std::env::var("HOSTNAME")
            .ok()
            .filter(|h| !h.is_empty())
            .or_else(|| std::fs::read_to_string("/etc/hostname").ok())
            .map(|h| h.trim().to_string())
            .unwrap_or_else(|| "host".into());
        format!("{host}-{}", std::process::id())
    })
}

/// True when clustering is declared (`VEJAS_CLUSTER=1`). Governs the local-file
/// write refusal (ADR-0020); the lease itself runs whenever a singleton driver
/// is supervised, so a single instance still just acquires and runs as before.
pub fn clustered() -> bool {
    std::env::var("VEJAS_CLUSTER").map(|v| v == "1" || v == "true").unwrap_or(false)
}

/// A driver kind that must run on exactly one instance. Sinks and the RPC
/// responder are competing-safe (durable / queue group); `source:webhook` is
/// LB-safe (its own listener per node). Everything else that *produces* from a
/// non-bus trigger is a singleton.
pub fn is_singleton(kind: &str) -> bool {
    matches!(
        kind,
        "source:interval" | "source:poll" | "source:exec" | "source:stream"
    )
}

/// Lease TTL (bucket `max_age`). `VEJAS_LEASE_TTL_SECS` tunes it; default 10s,
/// which matches the real pollers' `INTERVAL_SECS ≥ 60`.
pub fn lease_ttl() -> Duration {
    let secs = std::env::var("VEJAS_LEASE_TTL_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&s| s > 0)
        .unwrap_or(10);
    Duration::from_secs(secs)
}

/// Open (or create) the shared lease bucket. Its own sibling root, like the DLQ
/// and audit streams. Returns None if JetStream KV is unavailable — the caller
/// then runs the driver unleased (single-instance behaviour, no worse than today).
pub fn open_lease_store(js: &nats::jetstream::JetStream) -> Option<nats::kv::Store> {
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

/// Held while this instance owns a singleton's lease. Dropping it stops the
/// renewal thread and releases the lease (instant handoff on shutdown).
pub struct LeaseGuard {
    store: nats::kv::Store,
    key: String,
    stop_renew: Arc<AtomicBool>,
}

impl Drop for LeaseGuard {
    fn drop(&mut self) {
        self.stop_renew.store(true, Ordering::SeqCst);
        // release → a stand-by acquires now (instant handoff, no TTL wait)
        match self.store.delete(&self.key) {
            Ok(()) => eprintln!("[vejas] lease released: {}", self.key),
            Err(e) => eprintln!("[vejas] lease release failed for {}: {e}", self.key),
        }
    }
}

/// Block until this instance holds the lease for `key`, then return a guard and
/// spawn a renewal thread. If the lease is later lost (fenced, or the bucket
/// aged it out under a stall), the thread clears `running` so the driver stops
/// and the supervisor re-enters standby. Returns None if `alive()` goes false
/// before the lease is acquired (asked to stop while standing by).
pub fn acquire_blocking(
    store: &nats::kv::Store,
    key: &str,
    running: Arc<AtomicBool>,
    alive: impl Fn() -> bool,
) -> Option<LeaseGuard> {
    let id = instance_id();
    let rev = loop {
        if !alive() {
            return None;
        }
        match store.create(key, id.as_bytes()) {
            Ok(r) => {
                eprintln!("[vejas] lease acquired: {key} ({id})");
                break r;
            }
            Err(_) => thread::sleep(Duration::from_millis(1000)), // held elsewhere
        }
    };
    let stop_renew = Arc::new(AtomicBool::new(false));
    // renew well inside the TTL (a third of it) so a slow round never lets the
    // value age out under a live leader.
    let renew_every = (lease_ttl() / 3).max(Duration::from_secs(1));
    {
        let (store, key, stop, running) =
            (store.clone(), key.to_string(), stop_renew.clone(), running);
        thread::spawn(move || {
            let id = instance_id();
            let mut rev = rev;
            let tick = Duration::from_millis(200);
            loop {
                // wait one renewal cadence, but in short ticks so a stop is
                // noticed promptly (and the lease is released promptly on drop).
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
                        // lost the lease (fenced, or aged out under a stall):
                        // stop the driver so the supervisor re-enters standby.
                        running.store(false, Ordering::SeqCst);
                        return;
                    }
                }
            }
        });
    }
    Some(LeaseGuard {
        store: store.clone(),
        key: key.to_string(),
        stop_renew,
    })
}
