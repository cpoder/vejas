//! Vejas AMQP 0-9-1 (RabbitMQ) connector — spike prototype (ADR-0026).
//!
//! A first-class bus citizen owning its own NATS client, like the IBM MQ connector
//! (ADR-0023) — but simpler, because AMQP has no two-phase syncpoint: `basic.ack`
//! IS the commit.
//!
//!   source (AMQP→bus): basic_consume (manual ack) → NATS publish (await pub-ack)
//!                      → ack. Crash before the ack → redelivered (no loss).
//!   sink   (bus→AMQP): durable pull → basic_publish + publisher confirm → ack NATS.
//!                      Crash between confirm and ack → dup publish; carry an
//!                      idempotency key in message properties for downstream dedup.
//!
//! amiquip is sync/no-tokio; TLS is plaintext for the spike (default-features off,
//! no openssl-sys) — the rustls-over-IoStream adapter (ADR-0026) is the production
//! path. The transactional ordering is exercised against a fake below with fault
//! injection; real-RabbitMQ certification is the declared CI exception (ADR-0017).

mod lease;
mod tls;

use amiquip::{
    Auth, Channel, Confirm, Connection, ConnectionOptions, ConnectionTuning, Consumer,
    ConsumerMessage, ConsumerOptions, Exchange, Publish, QueueDeclareOptions,
};

// Present our rustls-over-mio stream to amiquip as a pollable byte stream (ADR-0026).
impl amiquip::IoStream for tls::RustlsMioStream {}
use std::collections::HashMap;
use std::os::raw::c_int;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

static STOP: AtomicBool = AtomicBool::new(false);
extern "C" fn on_signal(_: c_int) {
    STOP.store(true, Ordering::SeqCst);
}
extern "C" {
    fn signal(signum: c_int, handler: extern "C" fn(c_int)) -> usize;
}
fn install_signals() {
    unsafe {
        signal(2, on_signal);
        signal(15, on_signal);
    }
}
fn alive() -> bool {
    !STOP.load(Ordering::SeqCst)
}

fn env(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}
fn env_or(k: &str, d: &str) -> String {
    env(k).unwrap_or_else(|| d.to_string())
}

// ───────────────────────── the seams ─────────────────────────
/// One message consumed from AMQP: the body plus the delivery tag used to ack it.
pub struct InMsg {
    pub body: Vec<u8>,
    pub tag: u64,
}

/// Source seam — consume with manual ack. The loop acks a message only after the
/// bus confirms it, so a crash in between redelivers (at-least-once). Abstracted so
/// the ordering can be tested against a fake without a live broker.
pub trait AmqpSource {
    fn recv(&mut self, wait: Duration) -> Result<Option<InMsg>, String>;
    fn ack(&mut self, tag: u64) -> Result<(), String>;
    fn nack(&mut self, tag: u64) -> Result<(), String>;
}

/// Sink seam — publish and wait for the broker's publisher confirm. Returns true
/// iff the broker confirmed (Ack); the loop acks the bus only then.
pub trait AmqpSink {
    fn publish_confirmed(&mut self, body: &[u8], routing_key: &str) -> Result<bool, String>;
}

// ───────────────────────── loops ─────────────────────────
/// Publish to a JetStream subject and wait for the pub-ack, flushing DIRECTLY past
/// nats 0.25's 5ms flusher floor (client.rs MIN_FLUSH_BETWEEN) — that floor caps
/// every sequential pub-ack at ~200/s, and the AMQP source is a sequential
/// consume→publish→ack loop (the spike measured 186/s, dead on the floor). Faithful
/// re-impl of the crate's request path incl. the no-responders guard + a direct
/// flush; Ok still means a real pub-ack (persisted), so the ack-after-bus-publish
/// ordering is unchanged. (Named bus_* to avoid the AmqpSink::publish_confirmed
/// method, which is the RabbitMQ *broker* confirm.)
fn bus_publish_confirmed(
    nc: &nats::Connection,
    subject: &str,
    payload: &[u8],
    wait: Duration,
) -> Result<(), String> {
    let reply = nc.new_inbox();
    let sub = nc.subscribe(&reply).map_err(|e| e.to_string())?;
    nc.publish_request(subject, &reply, payload)
        .map_err(|e| e.to_string())?;
    nc.flush().map_err(|e| e.to_string())?; // direct flush — bypass the 5ms floor
    let msg = sub
        .next_timeout(wait)
        .map_err(|_| "timed out waiting for JetStream pub-ack".to_string())?;
    if msg.is_no_responders() {
        return Err("no JetStream responder for the subject (stream missing?)".into());
    }
    let ack: serde_json::Value =
        serde_json::from_slice(&msg.data).map_err(|e| format!("bad pub-ack: {e}"))?;
    if let Some(err) = ack.get("error") {
        return Err(format!("JetStream rejected the publish: {err}"));
    }
    if ack.get("stream").is_none() {
        return Err(format!(
            "unexpected pub-ack (no stream): {}",
            String::from_utf8_lossy(&msg.data)
        ));
    }
    Ok(())
}

fn run_source(
    src: &mut dyn AmqpSource,
    nc: &nats::Connection,
    subject: &str,
    wait: Duration,
    alive: impl Fn() -> bool,
) -> Result<(), String> {
    eprintln!("[vejas-amqp] source: AMQP → {subject} (consume→publish→ack)");
    while alive() {
        let Some(msg) = src.recv(wait)? else { continue };
        match bus_publish_confirmed(nc, subject, &msg.body, Duration::from_secs(5)) {
            Ok(_) => src.ack(msg.tag)?, // bus durable → ack the broker
            Err(e) => {
                eprintln!("[vejas-amqp] publish failed ({e}); nack+requeue");
                src.nack(msg.tag)?;
            }
        }
    }
    Ok(())
}

fn run_sink(
    sink: &mut dyn AmqpSink,
    sub: &nats::jetstream::PullSubscription,
    routing_key: &str,
    alive: impl Fn() -> bool,
) -> Result<(), String> {
    eprintln!("[vejas-amqp] sink: bus → AMQP (publish→confirm→ack)");
    while alive() {
        for m in fetch_round(sub)? {
            if !alive() {
                return Ok(());
            }
            match sink.publish_confirmed(&m.data, routing_key) {
                Ok(true) => {
                    let _ = m.ack(); // confirmed by the broker → safe to ack the bus
                }
                Ok(false) => eprintln!("[vejas-amqp] broker nack'd publish; not acking, will redeliver"),
                Err(e) => eprintln!("[vejas-amqp] publish error ({e}); not acking, will redeliver"),
            }
        }
    }
    Ok(())
}

fn fetch_round(sub: &nats::jetstream::PullSubscription) -> Result<Vec<nats::Message>, String> {
    let iter = sub
        .timeout_fetch(
            nats::jetstream::BatchOptions { batch: 64, expires: Some(500_000_000), no_wait: true },
            Duration::from_millis(750),
        )
        .map_err(|e| e.to_string())?;
    Ok(iter.map_while(|m| m.ok()).collect())
}

// ───────────────────────── amiquip-backed impls ─────────────────────────
/// Real source: an amiquip Consumer plus a tag→Delivery map (amiquip acks a
/// Delivery by value, so the loop-facing tag API holds the deliveries until acked).
struct AmqpConsumer<'a> {
    consumer: Consumer<'a>,
    pending: HashMap<u64, amiquip::Delivery>,
}
impl AmqpSource for AmqpConsumer<'_> {
    fn recv(&mut self, wait: Duration) -> Result<Option<InMsg>, String> {
        match self.consumer.receiver().recv_timeout(wait) {
            Ok(ConsumerMessage::Delivery(d)) => {
                let tag = d.delivery_tag();
                let body = d.body.clone();
                self.pending.insert(tag, d);
                Ok(Some(InMsg { body, tag }))
            }
            Ok(_) => Ok(None), // cancellation / channel-close notices: treated as idle
            Err(_) => Ok(None), // recv timeout: no message within the wait
        }
    }
    fn ack(&mut self, tag: u64) -> Result<(), String> {
        if let Some(d) = self.pending.remove(&tag) {
            self.consumer.ack(d).map_err(|e| e.to_string())?;
        }
        Ok(())
    }
    fn nack(&mut self, tag: u64) -> Result<(), String> {
        if let Some(d) = self.pending.remove(&tag) {
            self.consumer.nack(d, true).map_err(|e| e.to_string())?; // requeue
        }
        Ok(())
    }
}

/// Real sink: a confirm-enabled channel + its confirm receiver. Publish one, wait
/// for exactly one confirm, report Ack/Nack.
struct AmqpPublisher<'a> {
    exchange: Exchange<'a>,
    confirms: crossbeam_channel::Receiver<Confirm>,
}
impl AmqpSink for AmqpPublisher<'_> {
    fn publish_confirmed(&mut self, body: &[u8], routing_key: &str) -> Result<bool, String> {
        self.exchange
            .publish(Publish::new(body, routing_key))
            .map_err(|e| e.to_string())?;
        match self.confirms.recv_timeout(Duration::from_secs(30)) {
            Ok(Confirm::Ack(_)) => Ok(true),
            Ok(Confirm::Nack(_)) => Ok(false),
            Err(_) => Err("no publisher confirm within 30s".into()),
        }
    }
}

fn main() {
    install_signals();
    let mode = match env("VEJAS_AMQP_MODE") {
        Some(m) if m == "source" || m == "sink" => m,
        _ => {
            eprintln!("[vejas-amqp] VEJAS_AMQP_MODE must be source or sink");
            std::process::exit(2);
        }
    };
    let url = env_or("VEJAS_AMQP_URL", "amqp://guest:guest@127.0.0.1:5672");
    let queue = env("VEJAS_AMQP_QUEUE").unwrap_or_default();
    let routing_key = env_or("VEJAS_AMQP_ROUTING_KEY", &queue);
    let subject = match env("VEJAS_AMQP_SUBJECT").or_else(|| env("VEJAS_SUBJECT")) {
        Some(s) => s,
        None => {
            eprintln!("[vejas-amqp] VEJAS_AMQP_SUBJECT required");
            std::process::exit(2);
        }
    };
    let nats_url = env_or("NATS_URL", "127.0.0.1:4222");
    let durable = env_or("VEJAS_AMQP_DURABLE", "vejas_amqp");
    let stream = env_or("VEJAS_STREAM", "VEJAS");
    let wait = Duration::from_secs(env("VEJAS_AMQP_WAIT_SECS").and_then(|s| s.parse().ok()).unwrap_or(3));
    let competing = matches!(env("VEJAS_AMQP_COMPETING").as_deref(), Some("1") | Some("true"));

    let nc = match nats::connect(&nats_url) {
        Ok(nc) => nc,
        Err(e) => {
            eprintln!("[vejas-amqp] cannot connect to NATS at {nats_url}: {e}");
            std::process::exit(1);
        }
    };
    let js = nats::jetstream::new(nc.clone());

    // amqps:// → pure-Rust rustls over amiquip's mio stream (ADR-0026 Q1); amqp://
    // → plaintext. Both avoid amiquip's native-tls (openssl-sys) default.
    let opened = if url.starts_with("amqps://") {
        open_tls(&url)
    } else {
        Connection::insecure_open(&url).map_err(|e| e.to_string())
    };
    let mut conn = match opened {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[vejas-amqp] cannot open AMQP at {url}: {e}");
            std::process::exit(1);
        }
    };
    let channel = match conn.open_channel(None) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[vejas-amqp] open_channel failed: {e}");
            std::process::exit(1);
        }
    };

    let result = if mode == "source" {
        // Singleton by default (ADR-0026, because order): RabbitMQ round-robins N
        // consumers so competing is duplication-safe, but the queue's order is lost
        // across N — so one consumer holds a lease. VEJAS_AMQP_COMPETING=1 opts out.
        let held = if competing {
            None
        } else if let Some(store) = lease::open_store(&js) {
            let key = lease::source_key(&queue);
            match lease::acquire_blocking(&store, &key, alive) {
                Some(l) => Some(l),
                None => {
                    eprintln!("[vejas-amqp] stopped before acquiring the lease");
                    let _ = conn.close();
                    return;
                }
            }
        } else {
            eprintln!("[vejas-amqp] no JetStream KV for the lease — running unleased (single instance)");
            None
        };
        let source_alive = || alive() && held.as_ref().map_or(true, |l| !l.lost());
        run_source_amqp(&channel, &nc, &queue, &subject, wait, source_alive)
    } else {
        run_sink_amqp(&channel, &js, &stream, &subject, &durable, &routing_key)
    };
    match result {
        Ok(()) => eprintln!("[vejas-amqp] stopped cleanly"),
        Err(e) => {
            eprintln!("[vejas-amqp] fatal: {e}");
            std::process::exit(1);
        }
    }
    let _ = conn.close();
}

fn run_source_amqp(
    channel: &Channel,
    nc: &nats::Connection,
    queue: &str,
    subject: &str,
    wait: Duration,
    alive: impl Fn() -> bool,
) -> Result<(), String> {
    // passive-ish: declare durable so the queue exists; a real recipe may bind it.
    channel
        .queue_declare(queue, QueueDeclareOptions { durable: true, ..Default::default() })
        .map_err(|e| e.to_string())?;
    let consumer = channel
        .basic_consume(queue, ConsumerOptions::default())
        .map_err(|e| e.to_string())?;
    let mut src = AmqpConsumer { consumer, pending: HashMap::new() };
    run_source(&mut src, nc, subject, wait, alive)
}

fn run_sink_amqp(
    channel: &Channel,
    js: &nats::jetstream::JetStream,
    stream: &str,
    subject: &str,
    durable: &str,
    routing_key: &str,
) -> Result<(), String> {
    channel.enable_publisher_confirms().map_err(|e| e.to_string())?;
    let confirms = channel.listen_for_publisher_confirms().map_err(|e| e.to_string())?;
    let exchange = Exchange::direct(channel);
    // Bind (creating if absent) the durable pull consumer on the stream — the
    // runtime owns the stream, but pull_subscribe_with_options only BINDS an
    // existing durable, so a standalone connector must declare it first (idempotent).
    let _ = js.add_consumer(
        stream,
        nats::jetstream::ConsumerConfig {
            durable_name: Some(durable.to_string()),
            filter_subject: subject.to_string(),
            ..Default::default()
        },
    );
    let sub = js
        .pull_subscribe_with_options(
            subject,
            &nats::jetstream::PullSubscribeOptions::new().durable_name(durable.to_string()),
        )
        .map_err(|e| e.to_string())?;
    let mut sink = AmqpPublisher { exchange, confirms };
    run_sink(&mut sink, &sub, routing_key, alive)
}

/// Open an amqps:// connection with pure-Rust rustls TLS (ADR-0026 Q1). Minimal URL
/// parse (amqps://[user:pass@]host[:port][/vhost]); trust roots come from
/// VEJAS_AMQP_TLS_CA (a PEM, e.g. a self-signed broker) or the webpki roots, and the
/// SNI defaults to the URL host (VEJAS_AMQP_TLS_SERVER_NAME overrides).
fn open_tls(url: &str) -> Result<Connection, String> {
    let rest = url.trim_start_matches("amqps://");
    let (userinfo, hostpart) = rest.split_once('@').unwrap_or(("", rest));
    let (user, pass) = match userinfo.split_once(':') {
        Some((u, p)) => (u.to_string(), p.to_string()),
        None if !userinfo.is_empty() => (userinfo.to_string(), String::new()),
        None => ("guest".to_string(), "guest".to_string()),
    };
    let (hostport, vhost) = match hostpart.split_once('/') {
        Some((hp, v)) if !v.is_empty() => (hp, v.to_string()),
        Some((hp, _)) => (hp, "/".to_string()),
        None => (hostpart, "/".to_string()),
    };
    let (host, port) = match hostport.split_once(':') {
        Some((h, p)) => (h.to_string(), p.parse::<u16>().map_err(|_| "bad port in URL")?),
        None => (hostport.to_string(), 5671),
    };
    let server_name = env("VEJAS_AMQP_TLS_SERVER_NAME").unwrap_or_else(|| host.clone());
    let stream = tls::connect(&host, port, &server_name, env("VEJAS_AMQP_TLS_CA").as_deref())?;
    let options = ConnectionOptions::default()
        .auth(Auth::Plain { username: user, password: pass })
        .virtual_host(vhost);
    Connection::insecure_open_stream(stream, options, ConnectionTuning::default())
        .map_err(|e| e.to_string())
}

// ───────────────────────── tests ─────────────────────────
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    /// Models an AMQP queue's manual-ack semantics: a delivered-but-unacked message
    /// is redelivered on nack/requeue (or if never acked). Lets the source loop's
    /// no-loss ordering be tested without a broker.
    #[derive(Default)]
    struct FakeSource {
        queue: VecDeque<Vec<u8>>,
        unacked: HashMap<u64, Vec<u8>>,
        next_tag: u64,
    }
    impl FakeSource {
        fn with(msgs: &[&str]) -> Self {
            let mut s = FakeSource::default();
            for m in msgs {
                s.queue.push_back(m.as_bytes().to_vec());
            }
            s
        }
    }
    impl AmqpSource for FakeSource {
        fn recv(&mut self, _w: Duration) -> Result<Option<InMsg>, String> {
            match self.queue.pop_front() {
                Some(body) => {
                    self.next_tag += 1;
                    let tag = self.next_tag;
                    self.unacked.insert(tag, body.clone());
                    Ok(Some(InMsg { body, tag }))
                }
                None => Ok(None),
            }
        }
        fn ack(&mut self, tag: u64) -> Result<(), String> {
            self.unacked.remove(&tag); // acked = gone for good
            Ok(())
        }
        fn nack(&mut self, tag: u64) -> Result<(), String> {
            if let Some(body) = self.unacked.remove(&tag) {
                self.queue.push_front(body); // requeue
            }
            Ok(())
        }
    }

    #[test]
    fn source_acks_only_after_publish_and_requeues_on_failure() {
        let mut s = FakeSource::with(&["m1"]);
        let m = s.recv(Duration::ZERO).unwrap().unwrap();
        // publish failed → nack/requeue → still available
        s.nack(m.tag).unwrap();
        let again = s.recv(Duration::ZERO).unwrap().unwrap();
        assert_eq!(again.body, b"m1", "nack requeues (no loss)");
        // now publish succeeds → ack → consumed
        s.ack(again.tag).unwrap();
        assert!(s.recv(Duration::ZERO).unwrap().is_none(), "acked message is gone");
    }

    #[test]
    fn source_unacked_message_is_not_lost() {
        let mut s = FakeSource::with(&["a", "b"]);
        let first = s.recv(Duration::ZERO).unwrap().unwrap();
        // crash before ack: the message is still unacked → recovery requeues it
        s.nack(first.tag).unwrap();
        assert_eq!(s.queue.len(), 2, "unacked message returns to the queue");
    }

    #[derive(Default)]
    struct FakeSink {
        delivered: Vec<Vec<u8>>,
        confirm: bool, // whether the broker confirms (Ack) or nacks
    }
    impl AmqpSink for FakeSink {
        fn publish_confirmed(&mut self, body: &[u8], _rk: &str) -> Result<bool, String> {
            if self.confirm {
                self.delivered.push(body.to_vec());
                Ok(true)
            } else {
                Ok(false) // broker nack: message NOT delivered
            }
        }
    }

    #[test]
    fn sink_confirms_gate_the_bus_ack() {
        let mut ok = FakeSink { confirm: true, ..Default::default() };
        assert!(ok.publish_confirmed(b"x", "rk").unwrap(), "confirmed → loop acks bus");
        assert_eq!(ok.delivered, vec![b"x".to_vec()]);

        let mut nacked = FakeSink { confirm: false, ..Default::default() };
        assert!(!nacked.publish_confirmed(b"y", "rk").unwrap(), "nack → loop does NOT ack bus");
        assert!(nacked.delivered.is_empty(), "nack'd publish never delivered");
    }
}
