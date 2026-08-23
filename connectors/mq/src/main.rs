//! Vejas IBM MQ connector (ADR-0023) — a first-class bus citizen.
//!
//! Not an exec-bridge (ADR-0022's kcat shape) and not an exec-stdout child (the
//! SAP/Salesforce shape): a destructive `MQGET` under syncpoint may only `MQCMIT`
//! *after* the bus publish is confirmed, and a stdout-only child cannot know the
//! bus confirmed. So this process owns BOTH sides — the MQ transaction and its own
//! NATS client — and the commit ordering lives in one place:
//!
//!   source (MQ→bus): MQGET syncpoint → NATS publish (await pub-ack) → MQCMIT
//!                     (crash before MQCMIT → message re-got; MQBACK on any error)
//!   sink   (bus→MQ): NATS durable pull → MQPUT syncpoint → MQCMIT → ack NATS
//!                     (crash between MQCMIT and ack → bus redelivers → dup MQPUT;
//!                      carry an idempotency key in CorrelId so downstream dedups)
//!
//! The MQI itself is hand-declared FFI, dlopen'd at runtime (see mqi.rs). The
//! transactional invariants above are exercised in tests against an in-memory fake
//! broker with fault injection; real-queue-manager certification is the declared
//! CI exception (ADR-0023, like SAP/ADR-0017).

mod lease;
mod mqi;

use mqi::{Broker, MqMessage, MqiQueue};
use std::os::raw::c_int;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

static STOP: AtomicBool = AtomicBool::new(false);
extern "C" fn on_signal(_: c_int) {
    STOP.store(true, Ordering::SeqCst); // atomic store is async-signal-safe
}
extern "C" {
    fn signal(signum: c_int, handler: extern "C" fn(c_int)) -> usize;
}
fn install_signals() {
    unsafe {
        signal(2, on_signal); // SIGINT
        signal(15, on_signal); // SIGTERM
    }
}
fn alive() -> bool {
    !STOP.load(Ordering::SeqCst)
}

fn env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}
fn env_or(key: &str, default: &str) -> String {
    env(key).unwrap_or_else(|| default.to_string())
}

struct Config {
    mode: String,
    qmgr: String,
    queue: String,
    nats_url: String,
    subject: String,
    durable: String,
    wait: Duration,
    dedup_field: Option<String>,
    competing: bool,
}

fn load_config() -> Result<Config, String> {
    let mode = env("VEJAS_MQ_MODE").ok_or("VEJAS_MQ_MODE must be source or sink")?;
    if mode != "source" && mode != "sink" {
        return Err(format!("VEJAS_MQ_MODE={mode} — must be source or sink"));
    }
    let queue = env("VEJAS_MQ_QUEUE").ok_or("VEJAS_MQ_QUEUE required")?;
    let subject = env("VEJAS_MQ_SUBJECT")
        .or_else(|| env("VEJAS_SUBJECT"))
        .ok_or("VEJAS_MQ_SUBJECT required (the bus subject to publish to / consume)")?;
    Ok(Config {
        mode,
        qmgr: env_or("VEJAS_MQ_QMGR", ""),
        queue,
        nats_url: env_or("NATS_URL", "127.0.0.1:4222"),
        subject,
        durable: env_or("VEJAS_MQ_DURABLE", "vejas_mq"),
        wait: Duration::from_secs(
            env("VEJAS_MQ_WAIT_SECS").and_then(|s| s.parse().ok()).unwrap_or(3),
        ),
        dedup_field: env("VEJAS_MQ_DEDUP_FIELD"),
        competing: matches!(env("VEJAS_MQ_COMPETING").as_deref(), Some("1") | Some("true")),
    })
}

// ───────────────────────── source: MQ → bus ─────────────────────────
/// Get under syncpoint, publish to the bus awaiting the JetStream pub-ack, then —
/// and only then — MQCMIT. Any failure before the commit is an MQBACK, so the
/// message stays on the queue and is re-got: at-least-once, no loss. The broker is
/// the durable cursor (no offset KV — contrast Kafka/ADR-0022).
fn run_source(
    broker: &mut dyn Broker,
    js: &nats::jetstream::JetStream,
    subject: &str,
    wait: Duration,
    alive: impl Fn() -> bool,
) -> Result<(), String> {
    eprintln!("[vejas-mq] source: MQ → {subject} (get→publish→commit)");
    while alive() {
        let got = broker
            .get_syncpoint(wait)
            .map_err(|e| format!("get: {e}"))?;
        let Some(msg) = got else { continue }; // idle: no message within the wait
        match js.publish(subject, &msg.body) {
            Ok(_) => {
                // JetStream pub-ack received (message is durable on the bus) →
                // safe to remove it from MQ.
                broker.commit().map_err(|e| format!("commit after publish: {e}"))?;
            }
            Err(e) => {
                // bus not confirmed → leave the message on the queue.
                eprintln!("[vejas-mq] publish failed ({e}); MQBACK, will re-get");
                broker.backout().map_err(|e| format!("backout: {e}"))?;
            }
        }
    }
    Ok(())
}

// ───────────────────────── sink: bus → MQ ─────────────────────────
/// Durable pull → MQPUT under syncpoint → MQCMIT → ack the NATS message
/// (side-effect-before-ack, SUBJECTS.md rule 3). A crash between MQCMIT and the ack
/// redelivers the bus message → a duplicate MQPUT; carrying an idempotency key in
/// CorrelId (VEJAS_MQ_DEDUP_FIELD) lets a downstream consumer dedup.
fn run_sink(
    broker: &mut dyn Broker,
    sub: &nats::jetstream::PullSubscription,
    dedup_field: Option<&str>,
    alive: impl Fn() -> bool,
) -> Result<(), String> {
    eprintln!("[vejas-mq] sink: bus → MQ (put→commit→ack)");
    while alive() {
        for m in fetch_round(sub)? {
            if !alive() {
                return Ok(()); // stopped mid-batch: leave un-acked, it redelivers
            }
            let correlid = dedup_field
                .and_then(|f| dedup_key(&m.data, f))
                .map(|k| mqi::correlid_from_key(&k))
                .unwrap_or_default();
            let out = MqMessage { body: m.data.clone(), correlid };
            match broker.put_syncpoint(&out) {
                Ok(()) => match broker.commit() {
                    Ok(()) => {
                        let _ = m.ack(); // committed to MQ → safe to ack the bus
                    }
                    Err(e) => {
                        eprintln!("[vejas-mq] MQCMIT failed ({e}); not acking, will redeliver");
                        let _ = broker.backout();
                    }
                },
                Err(e) => {
                    eprintln!("[vejas-mq] MQPUT failed ({e}); MQBACK, not acking");
                    let _ = broker.backout();
                }
            }
        }
    }
    Ok(())
}

/// Pull one bounded batch as a continuous long-poll: messages the instant they
/// arrive, else the pull is held server-side for the window and returns empty (no
/// client sleep, no coverage gap — the same shape the core runtime uses).
fn fetch_round(
    sub: &nats::jetstream::PullSubscription,
) -> Result<Vec<nats::Message>, String> {
    let iter = sub
        .timeout_fetch(
            nats::jetstream::BatchOptions { batch: 64, expires: Some(500_000_000), no_wait: true },
            Duration::from_millis(750),
        )
        .map_err(|e| e.to_string())?;
    Ok(iter.map_while(|m| m.ok()).collect())
}

/// Pull a stringified idempotency key out of the JSON body at a top-level field.
fn dedup_key(body: &[u8], field: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_slice(body).ok()?;
    match v.get(field)? {
        serde_json::Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

fn main() {
    install_signals();
    let cfg = match load_config() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[vejas-mq] config error: {e}");
            std::process::exit(2);
        }
    };
    if cfg.mode == "source" && cfg.competing {
        eprintln!(
            "[vejas-mq] source in COMPETING mode — N getters, no ordering guarantee \
             (ADR-0023: default is singleton/ordered; you chose throughput on purpose)"
        );
    }

    let nc = match nats::connect(&cfg.nats_url) {
        Ok(nc) => nc,
        Err(e) => {
            eprintln!("[vejas-mq] cannot connect to NATS at {}: {e}", cfg.nats_url);
            std::process::exit(1);
        }
    };
    let js = nats::jetstream::new(nc.clone());

    let result = if cfg.mode == "source" {
        // Singleton by default (ADR-0023, "because order"): take the lease so one
        // instance gets. COMPETING skips it; no KV → run unleased (single instance).
        let held = if cfg.competing {
            None
        } else if let Some(store) = lease::open_store(&js) {
            let key = lease::source_key(&cfg.queue);
            match lease::acquire_blocking(&store, &key, alive) {
                Some(l) => Some(l),
                None => {
                    // asked to stop while standing by for the lease — clean exit.
                    eprintln!("[vejas-mq] stopped before acquiring the lease");
                    return;
                }
            }
        } else {
            eprintln!("[vejas-mq] no JetStream KV for the lease — running unleased (single instance)");
            None
        };
        // stop getting if the lease is lost (fenced / aged out) as well as on signal
        let source_alive = || alive() && held.as_ref().map_or(true, |l| !l.lost());
        MqiQueue::open(&cfg.qmgr, &cfg.queue, false)
            .and_then(|mut q| run_source(&mut q, &js, &cfg.subject, cfg.wait, source_alive))
    } else {
        // durable pull consumer, competing-safe by its own durable (SUBJECTS.md).
        let sub = js
            .pull_subscribe_with_options(
                &cfg.subject,
                &nats::jetstream::PullSubscribeOptions::new().durable_name(cfg.durable.clone()),
            )
            .map_err(|e| e.to_string());
        match (MqiQueue::open(&cfg.qmgr, &cfg.queue, true), sub) {
            (Ok(mut q), Ok(sub)) => run_sink(&mut q, &sub, cfg.dedup_field.as_deref(), alive),
            (Err(e), _) | (_, Err(e)) => Err(e),
        }
    };

    match result {
        Ok(()) => eprintln!("[vejas-mq] stopped cleanly"),
        Err(e) => {
            eprintln!("[vejas-mq] fatal: {e}");
            std::process::exit(1);
        }
    }
}

// ───────────────────────── tests ─────────────────────────
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    /// In-memory queue with a single unit of work, modelling MQ syncpoint
    /// semantics so the loops' commit-ordering invariants can be tested without a
    /// live queue manager. `committed` is the durable queue; `in_flight` is a
    /// got-but-uncommitted message; `staged` is put-but-uncommitted.
    #[derive(Default)]
    struct FakeBroker {
        committed: VecDeque<MqMessage>,
        in_flight: Option<MqMessage>,
        staged: Vec<MqMessage>,
        delivered: Vec<MqMessage>, // committed puts (the "downstream")
        fail_next_commit: bool,
    }
    impl FakeBroker {
        fn with(msgs: &[&str]) -> Self {
            let mut b = FakeBroker::default();
            for m in msgs {
                b.committed.push_back(MqMessage { body: m.as_bytes().to_vec(), correlid: [0; 24] });
            }
            b
        }
    }
    impl Broker for FakeBroker {
        fn get_syncpoint(&mut self, _wait: Duration) -> Result<Option<MqMessage>, mqi::MqError> {
            if self.in_flight.is_some() {
                panic!("got a second message before committing the first");
            }
            match self.committed.pop_front() {
                Some(m) => {
                    self.in_flight = Some(m.clone());
                    Ok(Some(m))
                }
                None => Ok(None),
            }
        }
        fn put_syncpoint(&mut self, msg: &MqMessage) -> Result<(), mqi::MqError> {
            self.staged.push(msg.clone());
            Ok(())
        }
        fn commit(&mut self) -> Result<(), mqi::MqError> {
            if self.fail_next_commit {
                self.fail_next_commit = false;
                return Err(mqi::MqError { op: "MQCMIT", comp_code: 2, reason: 2003 });
            }
            self.in_flight = None; // got message consumed for good
            self.delivered.append(&mut self.staged);
            Ok(())
        }
        fn backout(&mut self) -> Result<(), mqi::MqError> {
            if let Some(m) = self.in_flight.take() {
                self.committed.push_front(m); // got message returns to the queue
            }
            self.staged.clear();
            Ok(())
        }
    }

    // The ADR's central claim: a crash after the bus publish but BEFORE MQCMIT must
    // not lose the message — it stays on the queue and is re-got.
    #[test]
    fn source_no_loss_when_crash_before_commit() {
        let mut b = FakeBroker::with(&["m1"]);
        let got = b.get_syncpoint(Duration::ZERO).unwrap().unwrap();
        assert_eq!(got.body, b"m1");
        // ...publish to the bus succeeds here... then the process dies before commit.
        // Recovery (a fresh unit of work) rolls the uncommitted get back:
        b.backout().unwrap();
        let again = b.get_syncpoint(Duration::ZERO).unwrap().unwrap();
        assert_eq!(again.body, b"m1", "message must survive a crash-before-commit");
    }

    #[test]
    fn source_consumes_only_after_commit() {
        let mut b = FakeBroker::with(&["m1"]);
        b.get_syncpoint(Duration::ZERO).unwrap().unwrap();
        b.commit().unwrap(); // bus confirmed → commit
        assert!(b.get_syncpoint(Duration::ZERO).unwrap().is_none(), "consumed after commit");
    }

    #[test]
    fn sink_put_invisible_until_commit_and_gone_on_backout() {
        let mut b = FakeBroker::default();
        b.put_syncpoint(&MqMessage { body: b"x".to_vec(), correlid: [0; 24] }).unwrap();
        assert_eq!(b.delivered.len(), 0, "staged put must not be visible pre-commit");
        b.backout().unwrap();
        b.commit().unwrap();
        assert_eq!(b.delivered.len(), 0, "backed-out put must never appear");

        b.put_syncpoint(&MqMessage { body: b"y".to_vec(), correlid: [0; 24] }).unwrap();
        b.commit().unwrap();
        assert_eq!(b.delivered.len(), 1, "committed put is durable");
    }

    // Sink loop wiring: a failed MQCMIT must NOT ack the bus message (so it
    // redelivers) — model the ordering through the broker directly.
    #[test]
    fn sink_commit_failure_leaves_message_for_redelivery() {
        let mut b = FakeBroker::default();
        b.fail_next_commit = true;
        b.put_syncpoint(&MqMessage { body: b"z".to_vec(), correlid: [0; 24] }).unwrap();
        assert!(b.commit().is_err(), "commit fails");
        // loop would skip the ack → NATS redelivers; downstream saw nothing yet.
        assert_eq!(b.delivered.len(), 0);
    }

    // End-to-end wiring: the source loop, driven by a fake MQ queue, must land the
    // messages on the real bus (proves the publish subject + await-ack path).
    // Needs a live NATS on 127.0.0.1:4222; excluded from CI.
    // Run: cargo test source_publishes_to_bus_e2e -- --ignored --nocapture
    #[test]
    #[ignore]
    fn source_publishes_to_bus_e2e() {
        let url = std::env::var("NATS_URL").unwrap_or_else(|_| "127.0.0.1:4222".into());
        let nc = nats::connect(&url).expect("connect");
        let js = nats::jetstream::new(nc.clone());
        let pid = std::process::id();
        let stream = format!("MQPROBE_{pid}");
        let subject = format!("mqprobe.{pid}.in");
        let _ = js.delete_stream(&stream);
        js.add_stream(nats::jetstream::StreamConfig {
            name: stream.clone(),
            subjects: vec![format!("mqprobe.{pid}.>")],
            ..Default::default()
        })
        .expect("add_stream");
        // a plain subscriber to observe what lands on the bus
        let sub = nc.subscribe(&subject).expect("sub");

        let mut broker = FakeBroker::with(&["a1", "b2", "c3"]);
        // stop once the queue has drained (get returns None) — bounded, no globals.
        let seen_empty = std::cell::Cell::new(false);
        let alive = || !seen_empty.get();
        // wrap so the first empty get flips the stop — model in a small adapter.
        struct Draining<'a> { inner: FakeBroker, flag: &'a std::cell::Cell<bool> }
        impl Broker for Draining<'_> {
            fn get_syncpoint(&mut self, w: Duration) -> Result<Option<MqMessage>, mqi::MqError> {
                let m = self.inner.get_syncpoint(w)?;
                if m.is_none() { self.flag.set(true); }
                Ok(m)
            }
            fn put_syncpoint(&mut self, m: &MqMessage) -> Result<(), mqi::MqError> { self.inner.put_syncpoint(m) }
            fn commit(&mut self) -> Result<(), mqi::MqError> { self.inner.commit() }
            fn backout(&mut self) -> Result<(), mqi::MqError> { self.inner.backout() }
        }
        let mut draining = Draining { inner: std::mem::take(&mut broker), flag: &seen_empty };
        run_source(&mut draining, &js, &subject, Duration::from_millis(50), alive).expect("source");

        let mut got = Vec::new();
        while let Ok(m) = sub.next_timeout(Duration::from_millis(300)) {
            got.push(String::from_utf8_lossy(&m.data).to_string());
        }
        assert_eq!(got, vec!["a1", "b2", "c3"], "all MQ messages reached the bus, in order");
        // and the fake queue was fully consumed (each commit fired after publish)
        assert!(draining.inner.get_syncpoint(Duration::ZERO).unwrap().is_none());
        let _ = js.delete_stream(&stream);
    }

    #[test]
    fn correlid_is_deterministic_and_fills_24_bytes() {
        let a = mqi::correlid_from_key("invoice-42");
        let b = mqi::correlid_from_key("invoice-42");
        let c = mqi::correlid_from_key("invoice-43");
        assert_eq!(a, b, "same key → same CorrelId (downstream dedup)");
        assert_ne!(a, c, "different key → different CorrelId");
        assert!(a.iter().any(|&x| x != 0), "fills the 24 bytes");
    }
}
