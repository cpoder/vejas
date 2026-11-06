// The bus island (ADR-0035): every NATS byte of the runtime goes through here.
//
// The runtime is synchronous and thread-based; the NATS client is not. So
// `async-nats` lives on one small tokio runtime owned by this module, and the
// rest of the code sees a blocking API and no NATS type. A flow, a connector or
// a supervisor is still a plain OS thread that calls a plain function.
//
// Three rules keep the island honest:
//
//   * Every call that leaves a thread for the island goes through `wait`, which
//     parks that OS thread. `wait` is never called from inside the runtime: the
//     event callback and the echo task below are the only code that runs there,
//     and neither calls back into this API. tokio panics rather than deadlocks
//     if that ever changes (a test pins it).
//   * The connection is shared. `connect` returns the one `Bus` of a URL, whoever
//     asks, so a thousand flows are a thousand threads on one socket instead of
//     two thousand sockets.
//   * What the sync client gave for free is rebuilt here on purpose, because
//     `async-nats` does not give it: see `Batch::confirm`.

use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use async_nats::client::{Request, Statistics};
use async_nats::connection::State;
use async_nats::jetstream::{self, kv, object_store};
use async_nats::{Client, ConnectOptions, Event, ServerAddr, StatusCode, Subject, Subscriber};
use bytes::Bytes;
use futures_util::{FutureExt, StreamExt};
use serde_json::{json, Value};
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tokio::time::timeout;

// ───────────────────────── the runtime ─────────────────────────

/// Worker threads of the island. One connection is one task, and the worker runs
/// nothing that blocks: a second one bought nothing at 1, 10 or 50 flows (ADR-0035).
const WORKERS: usize = 1;

/// How long a publish may wait for room in a connection's queue. Past it the
/// connection is gone for good enough reasons to say so to the caller.
const SEND_WAIT: Duration = Duration::from_secs(5);

/// How long a barrier may wait for its echo (the sync client's flush timeout).
const BARRIER_WAIT: Duration = Duration::from_secs(10);

/// After a failed first connect, later callers get the same answer for this
/// long instead of queueing on the registry lock behind a dead address.
const CONNECT_MEMO: Duration = Duration::from_secs(1);

fn runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(WORKERS)
            .thread_name("vejas-bus")
            .enable_all()
            .build()
            .expect("the bus runtime")
    })
}

/// Park this OS thread on a future of the island. Never call it from a task of
/// the runtime itself.
fn wait<F: Future>(future: F) -> F::Output {
    runtime().block_on(future)
}

/// `wait`, for at most `within`; `None` if the time ran out. The timer has to be
/// built inside the runtime, which is why this is not `wait(timeout(..))`.
fn wait_for<F: Future>(within: Duration, future: F) -> Option<F::Output> {
    wait(async { timeout(within, future).await.ok() })
}

/// `future`, bounded by `SEND_WAIT`, with its error as text.
async fn sent<T, E: std::fmt::Display>(
    future: impl Future<Output = Result<T, E>>,
) -> Result<T, String> {
    match timeout(SEND_WAIT, future).await {
        Ok(result) => result.map_err(|e| e.to_string()),
        Err(_) => Err("timed out: the bus connection is not taking writes".into()),
    }
}

// ───────────────────────── connections ─────────────────────────

/// One NATS connection, and the way to prove the server has read it.
struct Link {
    client: Client,
    stats: Arc<Statistics>,
    /// A subject only this connection listens to, for the barrier.
    echo: Subject,
    next_id: AtomicU64,
    waiting: Arc<Mutex<HashMap<u64, oneshot::Sender<()>>>>,
}

impl Link {
    async fn open(servers: Vec<ServerAddr>) -> Result<Arc<Link>, String> {
        let client = with_url_credentials(ConnectOptions::new(), &servers)
            .name("vejas-runtime")
            .event_callback(|event| async move {
                match event {
                    Event::Connected | Event::Closed => {}
                    Event::Disconnected => eprintln!("[bus] disconnected, reconnecting"),
                    other => eprintln!("[bus] {other}"),
                }
            })
            .connect(servers)
            .await
            .map_err(|e| e.to_string())?;
        let echo: Subject = client.new_inbox().into();
        let mut echoes = client
            .subscribe(echo.clone())
            .await
            .map_err(|e| e.to_string())?;
        let waiting: Arc<Mutex<HashMap<u64, oneshot::Sender<()>>>> = Arc::default();
        let waiters = waiting.clone();
        tokio::spawn(async move {
            while let Some(echoed) = echoes.next().await {
                let id = <[u8; 8]>::try_from(&echoed.payload[..]).map(u64::from_be_bytes);
                if let Ok(id) = id {
                    if let Some(waiter) = waiters.lock().unwrap().remove(&id) {
                        let _ = waiter.send(());
                    }
                }
            }
        });
        Ok(Arc::new(Link {
            stats: client.statistics(),
            client,
            echo,
            next_id: AtomicU64::new(0),
            waiting,
        }))
    }

    fn up(&self) -> bool {
        self.client.connection_state() == State::Connected
    }

    /// Which connection this link is on: it moves on every reconnect.
    fn epoch(&self) -> u64 {
        self.stats.connects.load(Ordering::Relaxed)
    }

    /// Prove the server has read everything sent on this link so far: publish
    /// to ourselves and wait to hear it. A connection's messages are handled in
    /// the order they were sent, so the echo cannot overtake them.
    async fn barrier(&self) -> Result<(), String> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.waiting.lock().unwrap().insert(id, tx);
        let ask = self
            .client
            .publish(self.echo.clone(), Bytes::copy_from_slice(&id.to_be_bytes()));
        let heard = match sent(ask).await {
            Err(error) => Err(error),
            Ok(()) => match timeout(BARRIER_WAIT, rx).await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(_)) => Err("the barrier was dropped".to_string()),
                Err(_) => Err("timed out waiting for the server".to_string()),
            },
        };
        if heard.is_err() {
            self.waiting.lock().unwrap().remove(&id);
        }
        heard
    }

    /// The barrier, on the connection `epoch` names: one that was replaced in
    /// the meantime fails it, whether or not anything was lost with it.
    async fn barrier_since(&self, epoch: u64) -> Result<(), String> {
        self.barrier().await?;
        if self.epoch() != epoch {
            return Err("the connection was replaced during the batch".into());
        }
        Ok(())
    }
}

/// The process's connection to one NATS deployment: a cheap handle, shared by
/// every thread that talks to it.
#[derive(Clone)]
pub struct Bus(Arc<Shared>);

struct Shared {
    /// One connection for everything: publishes, requests, subscriptions, pull
    /// rounds and acks. A connection of their own for the acks bought nothing
    /// here at any flow count (ADR-0035).
    link: Arc<Link>,
    js: jetstream::Context,
}

#[derive(Default)]
struct Registry {
    open: HashMap<String, Bus>,
    failed: HashMap<String, (Instant, String)>,
}

/// The `Bus` of `url`, connecting on first use. `url` is what `NATS_URL` holds:
/// one server or a comma-separated list, with or without a scheme. Once it has
/// connected, a `Bus` reconnects on its own for as long as the process lives,
/// and threads already holding it pick up where they were. While it is down
/// this answers an error: a unit that starts now backs off and comes back,
/// as it did when every start dialled its own connection.
pub fn connect(url: &str) -> Result<Bus, String> {
    static BUSES: OnceLock<Mutex<Registry>> = OnceLock::new();
    let mut registry = BUSES
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(bus) = registry.open.get(url) {
        if !bus.0.link.up() {
            return Err("the connection to NATS is down".into());
        }
        return Ok(bus.clone());
    }
    if let Some((at, error)) = registry.failed.get(url) {
        if at.elapsed() < CONNECT_MEMO {
            return Err(error.clone());
        }
    }
    match open(url) {
        Ok(bus) => {
            registry.open.insert(url.to_string(), bus.clone());
            Ok(bus)
        }
        Err(error) => {
            registry
                .failed
                .insert(url.to_string(), (Instant::now(), error.clone()));
            Err(error)
        }
    }
}

/// `nats://user:pass@host` and `nats://token@host`: `async-nats` parses these and
/// then connects without them, so they are handed over here. One set for the
/// connection: the first server that names any.
fn with_url_credentials(options: ConnectOptions, servers: &[ServerAddr]) -> ConnectOptions {
    let named = servers
        .iter()
        .find_map(|server| Some((server.username()?, server.password())));
    match named {
        Some((user, Some(pass))) => options.user_and_password(user.to_string(), pass.to_string()),
        Some((token, None)) => options.token(token.to_string()),
        None => options,
    }
}

fn servers_of(url: &str) -> Result<Vec<ServerAddr>, String> {
    url.split(',')
        .map(|server| {
            server
                .trim()
                .parse::<ServerAddr>()
                .map_err(|e| e.to_string())
        })
        .collect()
}

fn open(url: &str) -> Result<Bus, String> {
    let servers = servers_of(url)?;
    wait(async move {
        let link = Link::open(servers).await?;
        let js = jetstream::new(link.client.clone());
        Ok(Bus(Arc::new(Shared { link, js })))
    })
}

impl Bus {
    pub fn jetstream(&self) -> JetStream {
        JetStream(self.clone())
    }

    /// Publish one message now, with no promise: for what stands for nothing to
    /// ack. An error means it was never handed to the connection.
    pub fn publish(&self, subject: &str, payload: impl Into<Bytes>) -> Result<(), String> {
        let (client, payload) = (&self.0.link.client, payload.into());
        wait(sent(client.publish(subject.to_string(), payload)))
    }

    /// A batch of publishes that leave together, see [`Batch`].
    pub fn batch(&self) -> Batch {
        Batch {
            bus: self.clone(),
            held: Vec::new(),
        }
    }

    /// Ack every message, in one go. An ack that does not make it only means a
    /// redelivery.
    pub fn ack_all<'a>(
        &self,
        messages: impl IntoIterator<Item = &'a Message>,
    ) -> Result<(), String> {
        let client = &self.0.link.client;
        wait(async {
            for message in messages {
                if let Some(reply) = &message.reply {
                    sent(client.publish(reply.clone(), Bytes::new())).await?;
                }
            }
            Ok(())
        })
    }

    /// A core request: the answer, or an error when nobody answers within `within`.
    pub fn request(
        &self,
        subject: &str,
        payload: impl Into<Bytes>,
        within: Duration,
    ) -> Result<Message, String> {
        let request = Request::new().payload(payload.into()).timeout(Some(within));
        let answer = wait(
            self.0
                .link
                .client
                .send_request(subject.to_string(), request),
        )
        .map_err(|e| e.to_string())?;
        Ok(Message::new(answer, self.0.link.clone()))
    }

    pub fn subscribe(&self, subject: &str) -> Result<Subscription, String> {
        let client = &self.0.link.client;
        let sub = wait(sent(client.subscribe(subject.to_string())))?;
        Ok(Subscription {
            sub,
            link: self.0.link.clone(),
        })
    }

    pub fn queue_subscribe(&self, subject: &str, group: &str) -> Result<Subscription, String> {
        let client = &self.0.link.client;
        let sub = wait(sent(
            client.queue_subscribe(subject.to_string(), group.to_string()),
        ))?;
        Ok(Subscription {
            sub,
            link: self.0.link.clone(),
        })
    }
}

/// The publishes of one batch, held back until [`Batch::confirm`]. A loop that
/// publishes before it acks keeps its emits here, one batch at a time.
///
/// Handing each emit to the connection as it is made costs a cross-thread wake
/// and a write per message, the most expensive thing on the hot path (~33 µs
/// each on the flow hop, measured: 20 000 evt/s fell to 12 500). Held until the
/// end of the batch they cost one of each per batch, and the batch is still
/// whatever has arrived, so a quiet flow's emit leaves as soon as it is made.
pub struct Batch {
    bus: Bus,
    held: Vec<(Subject, Bytes)>,
}

impl Batch {
    /// Hold a publish. An error means this one can never leave (a subject the
    /// protocol cannot carry, a payload over the server's limit), and only this
    /// one: the caller leaves its source message un-acked, and the rest of the
    /// batch goes on.
    pub fn publish(&mut self, subject: &str, payload: impl Into<Bytes>) -> Result<(), String> {
        let payload = payload.into();
        if subject.is_empty() || subject.contains([' ', '\t', '\r', '\n']) {
            return Err(format!("invalid subject {subject:?}"));
        }
        let limit = self.bus.0.link.client.max_payload();
        if payload.len() > limit {
            return Err(format!(
                "{} bytes is over the server's limit of {limit}",
                payload.len()
            ));
        }
        self.held.push((subject.to_string().into(), payload));
        Ok(())
    }

    /// Send what is held and prove it arrived: the publish-before-ack barrier.
    /// Ok means the server has read every publish of the batch, so their source
    /// messages may be acked; a batch with nothing held has nothing to prove.
    /// The batch is empty afterwards, either way. Two things stand in here for
    /// what the sync client's `flush` was.
    ///
    /// `async_nats::Client::flush` only writes the socket; it proves nothing
    /// about the server. The barrier is a round-trip through it instead, see
    /// [`Link::barrier`].
    ///
    /// And on a reconnect `async-nats` drops what it had not yet written: the
    /// old connection's buffer goes with it, in silence, and whatever comes next
    /// leaves on the new one. A round-trip alone would then confirm a batch that
    /// lost its first half. So a connection replaced while the batch was leaving
    /// fails the barrier, and the batch redelivers.
    pub fn confirm(&mut self) -> Result<(), String> {
        if self.held.is_empty() {
            return Ok(());
        }
        let link = &self.bus.0.link;
        if !link.up() {
            self.held.clear();
            return Err("the connection is down".into());
        }
        let epoch = link.epoch();
        let held = &mut self.held;
        wait(async {
            for (subject, payload) in held.drain(..) {
                sent(link.client.publish(subject, payload)).await?;
            }
            link.barrier_since(epoch).await
        })
    }
}

/// A plain subscription: the messages arrive whether or not anyone is waiting,
/// and go back with `respond`.
pub struct Subscription {
    sub: Subscriber,
    link: Arc<Link>,
}

impl Subscription {
    /// The next message, or an error if none arrives within `within`.
    pub fn next_timeout(&mut self, within: Duration) -> Result<Message, String> {
        let next = match self.sub.next().now_or_never() {
            Some(next) => next,
            None => wait_for(within, self.sub.next()).ok_or("timed out")?,
        };
        match next {
            Some(message) => Ok(Message::new(message, self.link.clone())),
            None => Err("subscription closed".into()),
        }
    }
}

/// A message off the bus.
pub struct Message {
    pub subject: String,
    pub data: Bytes,
    reply: Option<Subject>,
    link: Arc<Link>,
}

/// What JetStream says about a delivery, read from its reply subject.
#[derive(Debug, PartialEq, Eq)]
pub struct Delivery {
    /// How many times this message has been delivered, this time included.
    pub delivered: i64,
    pub stream_seq: u64,
}

impl Message {
    fn new(message: async_nats::Message, link: Arc<Link>) -> Message {
        Message {
            subject: message.subject.to_string(),
            data: message.payload,
            reply: message.reply,
            link,
        }
    }

    /// The JetStream delivery behind this message, if it is one.
    pub fn delivery(&self) -> Option<Delivery> {
        parse_ack_subject(self.reply.as_deref()?)
    }

    pub fn ack(&self) -> Result<(), String> {
        self.answer(Bytes::new())
    }

    pub fn nak(&self) -> Result<(), String> {
        self.answer(Bytes::from_static(b"-NAK"))
    }

    /// Answer a request. A message that expects no answer is left alone.
    pub fn respond(&self, payload: impl AsRef<[u8]>) -> Result<(), String> {
        self.answer(Bytes::copy_from_slice(payload.as_ref()))
    }

    fn answer(&self, payload: Bytes) -> Result<(), String> {
        let Some(reply) = &self.reply else {
            return Ok(());
        };
        wait(sent(self.link.client.publish(reply.clone(), payload)))
    }
}

/// `$JS.ACK.<stream>.<consumer>.<delivered>.<stream seq>.<consumer seq>.<ts>.<pending>`,
/// or the same with `<domain>.<account>` in front and a token behind.
fn parse_ack_subject(reply: &str) -> Option<Delivery> {
    let tokens: Vec<&str> = reply.strip_prefix("$JS.ACK.")?.split('.').collect();
    let at = match tokens.len() {
        7 => 2,
        n if n >= 9 => 4,
        _ => return None,
    };
    Some(Delivery {
        delivered: tokens[at].parse().ok()?,
        stream_seq: tokens[at + 1].parse().ok()?,
    })
}

// ───────────────────────── pull rounds ─────────────────────────

/// Messages asked for in one pull request.
const PULL_BATCH: usize = 64;

/// The idle poll window (`expires`). One outstanding pull covers this whole span,
/// so a message arriving any time inside it is delivered at once: there is NO gap
/// between rounds where a message would sit unpolled. It is therefore two things
/// at once: the idle pull cadence (one pull per window when quiet) AND the
/// worst-case stop-response latency (the loop re-checks its `alive` flag each
/// time a pull returns). 500 ms keeps idle chatter to ~2 pulls/s per consumer
/// while leaving stop far inside the transport suite's 3 s bound (T5).
const PULL_EXPIRES_MS: u64 = 500;

/// A continuous long-poll. `no_wait` returns the instant messages are available
/// (a single message is delivered at once, no batch-fill latency, and a high-rate
/// drain still fills 64-batches in one round). When the subject is idle the pull
/// is HELD server-side for `expires` and comes back empty, so a loop paces itself
/// on the server with no client sleep and no coverage gap. Nothing parks in
/// `max_waiting`: every request resolves within `expires`, so a stopped loop is
/// never wedged on one. (An unbounded fetch parks forever on an idle subject: a
/// stopped flow's thread stayed in `recv()`, reported "running", and processed
/// one more message before dying, the zombie-consumer trap caught while
/// rehearsing the demo.)
const PULL_REQUEST: &[u8] = b"{\"batch\":64,\"expires\":500000000,\"no_wait\":true}";

/// Client-side backstop, comfortably past the server's `expires`, so that the
/// server's own status ends a round and not this timeout.
const PULL_WAIT: Duration = Duration::from_millis(PULL_EXPIRES_MS + 250);

/// A pull consumer, bound to one inbox of its own on the shared connection. The
/// messages of every outstanding request land in that inbox, in order, so a
/// caller can keep the next request in flight while it works through a round.
pub struct Puller {
    next: Subject,
    inbox: Subject,
    sub: RefCell<Subscriber>,
    /// Set when a round learned that the consumer is gone: the next round
    /// fails, and the supervisor's attempt starts over from the top.
    fault: RefCell<Option<String>>,
    link: Arc<Link>,
    /// The stream and name of an ephemeral consumer, deleted with this handle.
    owned: Option<(String, String)>,
    js: JetStream,
}

impl Puller {
    /// Send one pull request and return its round. The request leaves now, and
    /// the messages come back one by one as they arrive.
    ///
    /// With the connection down no request is sent: the round times out empty,
    /// and the caller's loop keeps its pace and its stop flag until the
    /// connection is back, when it picks up where it was.
    pub fn round(&self) -> Result<Round<'_>, String> {
        if let Some(fault) = self.fault.borrow_mut().take() {
            return Err(fault);
        }
        if self.link.up() {
            let request = self.link.client.publish_with_reply(
                self.next.clone(),
                self.inbox.clone(),
                Bytes::from_static(PULL_REQUEST),
            );
            wait(sent(request))?;
        }
        Ok(Round {
            puller: self,
            taken: 0,
        })
    }

    fn receive(&self, deadline: Instant) -> Option<async_nats::Message> {
        let mut sub = self.sub.borrow_mut();
        match sub.next().now_or_never() {
            Some(next) => next,
            None => wait_for(deadline - Instant::now(), sub.next()).flatten(),
        }
    }
}

impl Drop for Puller {
    fn drop(&mut self) {
        if let Some((stream, consumer)) = self.owned.take() {
            if self.link.up() {
                let _ = self.js.delete_consumer(&stream, &consumer);
            }
        }
    }
}

/// The messages of one pull request: it ends with the batch, on the server's own
/// status, or at the backstop. With a request in flight behind it, a status may
/// be that request's rather than this one's: both mean "nothing more for now".
pub struct Round<'a> {
    puller: &'a Puller,
    taken: usize,
}

impl Iterator for Round<'_> {
    type Item = Message;

    fn next(&mut self) -> Option<Message> {
        let deadline = Instant::now() + PULL_WAIT;
        while self.taken < PULL_BATCH {
            let message = self.puller.receive(deadline)?;
            match classify(&message) {
                Pulled::Message => {
                    self.taken += 1;
                    return Some(Message::new(message, self.puller.link.clone()));
                }
                Pulled::Nothing => {}
                Pulled::Over => return None,
                Pulled::Gone(why) => {
                    *self.puller.fault.borrow_mut() = Some(why);
                    return None;
                }
            }
        }
        None
    }
}

enum Pulled {
    Message,
    /// 404: a request came back empty; keep waiting for the rest of the round.
    Nothing,
    /// 408, or a 409 that leaves the consumer standing: this round is over.
    Over,
    /// The consumer or JetStream is not there any more.
    Gone(String),
}

fn classify(message: &async_nats::Message) -> Pulled {
    let description = message.description.as_deref().unwrap_or("");
    match message.status {
        None | Some(StatusCode::OK) => Pulled::Message,
        Some(StatusCode::NOT_FOUND) => Pulled::Nothing,
        Some(StatusCode::TIMEOUT) => Pulled::Over,
        Some(StatusCode::REQUEST_TERMINATED) if !description.contains("Deleted") => Pulled::Over,
        Some(status) => Pulled::Gone(format!("consumer gone: {status} {description}")),
    }
}

// ───────────────────────── JetStream ─────────────────────────

/// The persisted side of a `Bus`.
#[derive(Clone)]
pub struct JetStream(Bus);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Discard {
    #[default]
    Old,
    New,
}

/// What a stream is created with. The fields not named here keep the server's
/// defaults, and `add_stream` sends these and only these, so a stream comes out
/// the same as one made by any earlier release of the runtime.
#[derive(Clone, Default)]
pub struct StreamConfig {
    pub name: String,
    pub subjects: Vec<String>,
    pub max_msgs: i64,
    pub max_msgs_per_subject: i64,
    pub max_age: Duration,
    pub discard: Discard,
    pub allow_rollup: bool,
    pub deny_delete: bool,
}

/// A key-value bucket. The stream behind it is the one the sync client made,
/// `allow_direct` off included, so buckets of earlier releases and new ones are
/// the same thing.
#[derive(Clone, Default)]
pub struct KvConfig {
    pub bucket: String,
    pub history: i64,
    pub max_age: Duration,
}

/// A consumer that is not kept: named by the server, deleted with its `Puller`,
/// reaped by the server if the process dies first.
pub struct EphemeralConfig<'a> {
    pub filter: &'a str,
    /// Start at this stream sequence; `None` follows the tail.
    pub start_seq: Option<u64>,
    pub inactive_threshold: Duration,
}

#[derive(Clone, Copy, Debug)]
pub struct StreamState {
    pub first_seq: u64,
    pub last_seq: u64,
    pub messages: u64,
}

pub struct StoredMessage {
    pub sequence: u64,
    pub data: Bytes,
}

impl JetStream {
    fn shared(&self) -> &Shared {
        &(self.0).0
    }

    /// The JetStream API, as the server speaks it: a request and a JSON reply.
    /// Administration goes this way rather than through the client's typed
    /// configs because each of those is free to carry defaults of its own, and
    /// an in-place upgrade must find the same streams and consumers it left.
    fn api(&self, verb: String, body: Bytes) -> Result<Value, String> {
        let client = &self.shared().link.client;
        let reply =
            wait(client.request(format!("$JS.API.{verb}"), body)).map_err(|e| e.to_string())?;
        let answer: Value =
            serde_json::from_slice(&reply.payload).map_err(|e| format!("unreadable reply: {e}"))?;
        match answer.get("error") {
            None => Ok(answer),
            Some(error) => {
                let what = error
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or("error");
                Err(what.to_string())
            }
        }
    }

    /// Publish and wait for the stream to say it stored the message. Ok means
    /// persisted; the stream sequence comes back.
    pub fn publish(&self, subject: &str, payload: impl AsRef<[u8]>) -> Result<u64, String> {
        let bytes = Bytes::copy_from_slice(payload.as_ref());
        wait(async {
            let ack = self.shared().js.publish(subject.to_string(), bytes).await;
            let ack = ack.map_err(|e| e.to_string())?.await;
            ack.map(|ack| ack.sequence).map_err(|e| e.to_string())
        })
    }

    /// Create a stream, or leave it as it is: one that exists is never changed,
    /// and a different config on it is an error the caller may ignore.
    pub fn add_stream(&self, config: &StreamConfig) -> Result<(), String> {
        let discard = match config.discard {
            Discard::Old => "old",
            Discard::New => "new",
        };
        let mut body = json!({
            "name": config.name,
            "subjects": config.subjects,
            "max_msgs": config.max_msgs,
            "max_msgs_per_subject": config.max_msgs_per_subject,
            "max_age": config.max_age.as_nanos() as u64,
            "discard": discard,
            "retention": "limits",
            "storage": "file",
        });
        if config.allow_rollup {
            body["allow_rollup_hdrs"] = json!(true);
        }
        if config.deny_delete {
            body["deny_delete"] = json!(true);
        }
        self.api(
            format!("STREAM.CREATE.{}", config.name),
            body.to_string().into(),
        )
        .map(|_| ())
    }

    #[cfg(test)]
    pub fn delete_stream(&self, stream: &str) -> Result<(), String> {
        self.api(format!("STREAM.DELETE.{stream}"), Bytes::new())
            .map(|_| ())
    }

    pub fn stream_state(&self, stream: &str) -> Result<StreamState, String> {
        let info = self.api(format!("STREAM.INFO.{stream}"), Bytes::new())?;
        let seq = |field: &str| info["state"][field].as_u64().unwrap_or(0);
        Ok(StreamState {
            first_seq: seq("first_seq"),
            last_seq: seq("last_seq"),
            messages: seq("messages"),
        })
    }

    /// The message at a stream sequence.
    pub fn get_message(&self, stream: &str, sequence: u64) -> Result<StoredMessage, String> {
        let js = &self.shared().js;
        let message = wait(async {
            let stream = js
                .get_stream_no_info(stream)
                .await
                .map_err(|e| e.to_string())?;
            stream
                .get_raw_message(sequence)
                .await
                .map_err(|e| e.to_string())
        })?;
        Ok(StoredMessage {
            sequence: message.sequence,
            data: message.payload,
        })
    }

    pub fn delete_message(&self, stream: &str, sequence: u64) -> Result<(), String> {
        let body = json!({"seq": sequence}).to_string();
        self.api(format!("STREAM.MSG.DELETE.{stream}"), body.into())
            .map(|_| ())
    }

    /// Empty a stream; how many messages went comes back.
    pub fn purge_stream(&self, stream: &str) -> Result<u64, String> {
        let reply = self.api(format!("STREAM.PURGE.{stream}"), Bytes::new())?;
        Ok(reply["purged"].as_u64().unwrap_or(0))
    }

    /// Create (or verify) a durable pull consumer that delivers every one of
    /// `filters` in stream order, from the beginning or, with `start_seq`, from
    /// that stream sequence on. Idempotent: the same request on an existing
    /// consumer is accepted, and 2.10 updates the filters of an existing one in
    /// place (its ack floor stays). The reply's filters must be exactly the ones
    /// asked for: a server that ignored `filter_subjects` would deliver the
    /// whole stream.
    pub fn create_pull_consumer(
        &self,
        stream: &str,
        durable: &str,
        filters: &[String],
        ack_wait: Duration,
        start_seq: Option<u64>,
    ) -> Result<(), String> {
        let mut config = json!({
            "durable_name": durable,
            "ack_policy": "explicit",
            "ack_wait": ack_wait.as_nanos() as u64,
            "deliver_policy": "all",
            "replay_policy": "instant",
        });
        // Resuming after a snapshot: start at the sequence after it. The server
        // cannot change an existing consumer's deliver policy, so the caller
        // deletes the durable first.
        if let Some(seq) = start_seq {
            config["deliver_policy"] = Value::from("by_start_sequence");
            config["opt_start_seq"] = Value::from(seq);
        }
        match filters {
            [one] => config["filter_subject"] = Value::String(one.clone()),
            many => config["filter_subjects"] = Value::from(many.to_vec()),
        }
        let body = json!({ "stream_name": stream, "config": config }).to_string();
        let reply = self
            .api(
                format!("CONSUMER.DURABLE.CREATE.{stream}.{durable}"),
                body.into(),
            )
            .map_err(|e| format!("consumer {durable}: {e}"))?;
        let mut kept = filters_of(&reply["config"]);
        let mut asked = filters.to_vec();
        kept.sort();
        asked.sort();
        if kept != asked {
            return Err(format!(
                "consumer {durable}: the server kept the filters {kept:?}, not {asked:?} \
                 (several `.from()` subjects need NATS 2.10 or newer)"
            ));
        }
        Ok(())
    }

    pub fn delete_consumer(&self, stream: &str, consumer: &str) -> Result<(), String> {
        self.api(format!("CONSUMER.DELETE.{stream}.{consumer}"), Bytes::new())
            .map(|_| ())
    }

    /// The stream sequence below which a consumer has acked everything.
    pub fn ack_floor(&self, stream: &str, consumer: &str) -> Result<u64, String> {
        let info = self.api(format!("CONSUMER.INFO.{stream}.{consumer}"), Bytes::new())?;
        Ok(info["ack_floor"]["stream_seq"].as_u64().unwrap_or(0))
    }

    /// Bind to a durable consumer that must exist. With `filter`, it must also
    /// be the one consuming that subject.
    pub fn pull_durable(
        &self,
        stream: &str,
        durable: &str,
        filter: Option<&str>,
    ) -> Result<Puller, String> {
        let info = self
            .api(format!("CONSUMER.INFO.{stream}.{durable}"), Bytes::new())
            .map_err(|e| format!("provided durable consumer doesn't exist: {e}"))?;
        if let Some(filter) = filter {
            let filters = filters_of(&info["config"]);
            if !filters.is_empty() && filters != [filter] {
                return Err("subjects do not match".into());
            }
        }
        self.puller(stream, durable, None)
    }

    /// A read-only cursor over a stream: a consumer of its own, never acked, so
    /// nothing a live consumer does or sees changes.
    pub fn pull_ephemeral(&self, stream: &str, config: &EphemeralConfig) -> Result<Puller, String> {
        let mut consumer = json!({
            "deliver_policy": "new",
            "ack_policy": "explicit",
            "filter_subject": config.filter,
            "replay_policy": "instant",
            "inactive_threshold": config.inactive_threshold.as_nanos() as u64,
        });
        if let Some(seq) = config.start_seq {
            consumer["deliver_policy"] = Value::from("by_start_sequence");
            consumer["opt_start_seq"] = Value::from(seq);
        }
        let body = json!({ "stream_name": stream, "config": consumer }).to_string();
        let info = self.api(format!("CONSUMER.CREATE.{stream}"), body.into())?;
        let name = info["name"]
            .as_str()
            .ok_or("the server named no consumer")?
            .to_string();
        self.puller(stream, &name, Some((stream.to_string(), name.clone())))
    }

    fn puller(
        &self,
        stream: &str,
        consumer: &str,
        owned: Option<(String, String)>,
    ) -> Result<Puller, String> {
        let shared = self.shared();
        let inbox: Subject = shared.link.client.new_inbox().into();
        let sub = wait(sent(shared.link.client.subscribe(inbox.clone())))?;
        Ok(Puller {
            next: format!("$JS.API.CONSUMER.MSG.NEXT.{stream}.{consumer}").into(),
            inbox,
            sub: RefCell::new(sub),
            fault: RefCell::new(None),
            link: shared.link.clone(),
            owned,
            js: self.clone(),
        })
    }

    /// Open a bucket, creating it if it is not there. None if JetStream KV is
    /// unavailable.
    pub fn key_value(&self, config: &KvConfig) -> Option<Kv> {
        let _ = self.add_stream(&StreamConfig {
            name: format!("KV_{}", config.bucket),
            subjects: vec![format!("$KV.{}.>", config.bucket)],
            max_msgs_per_subject: config.history.max(1),
            max_age: config.max_age,
            discard: Discard::New,
            allow_rollup: true,
            deny_delete: true,
            ..Default::default()
        });
        let store = wait(self.shared().js.get_key_value(config.bucket.clone())).ok()?;
        Some(Kv { store })
    }

    /// Open an object bucket, creating it if it is not there. None if JetStream
    /// is unavailable.
    pub fn object_store(&self, bucket: &str) -> Option<Objects> {
        let _ = self.add_stream(&StreamConfig {
            name: format!("OBJ_{bucket}"),
            subjects: vec![format!("$O.{bucket}.C.>"), format!("$O.{bucket}.M.>")],
            discard: Discard::New,
            allow_rollup: true,
            ..Default::default()
        });
        let store = wait(self.shared().js.get_object_store(bucket)).ok()?;
        Some(Objects { store })
    }
}

/// The subject filters of a consumer's config, however it holds them.
fn filters_of(config: &Value) -> Vec<String> {
    match config.get("filter_subjects").and_then(Value::as_array) {
        Some(all) => all
            .iter()
            .filter_map(|x| x.as_str().map(str::to_string))
            .collect(),
        None => config
            .get("filter_subject")
            .and_then(Value::as_str)
            .map(|one| vec![one.to_string()])
            .unwrap_or_default(),
    }
}

// ───────────────────────── key-value ─────────────────────────

/// A JetStream KV bucket. Revisions are stream sequences; `create` and `update`
/// are compare-and-set on them, which is what the singleton lease stands on
/// (ADR-0020).
#[derive(Clone)]
pub struct Kv {
    store: kv::Store,
}

impl Kv {
    pub fn get(&self, key: &str) -> Result<Option<Bytes>, String> {
        wait(self.store.get(key)).map_err(|e| e.to_string())
    }

    pub fn put(&self, key: &str, value: impl AsRef<[u8]>) -> Result<u64, String> {
        let value = Bytes::copy_from_slice(value.as_ref());
        wait(self.store.put(key, value)).map_err(|e| e.to_string())
    }

    /// Atomic create-if-absent: a deleted key counts as absent. The revision
    /// comes back; a key that exists is an error.
    pub fn create(&self, key: &str, value: impl AsRef<[u8]>) -> Result<u64, String> {
        let value = Bytes::copy_from_slice(value.as_ref());
        wait(self.store.create(key, value)).map_err(|e| e.to_string())
    }

    /// Compare-and-set: fails unless `revision` is still the key's latest.
    pub fn update(&self, key: &str, value: impl AsRef<[u8]>, revision: u64) -> Result<u64, String> {
        let value = Bytes::copy_from_slice(value.as_ref());
        wait(self.store.update(key, value, revision)).map_err(|e| e.to_string())
    }

    pub fn delete(&self, key: &str) -> Result<(), String> {
        wait(self.store.delete(key)).map_err(|e| e.to_string())
    }

    /// The keys that hold a value now.
    pub fn keys(&self) -> Result<Vec<String>, String> {
        let collect = async {
            let mut keys = self.store.keys().await.map_err(|e| e.to_string())?;
            let mut all = Vec::new();
            while let Some(key) = keys.next().await {
                all.push(key.map_err(|e| e.to_string())?);
            }
            Ok(all)
        };
        wait_for(BARRIER_WAIT, collect).unwrap_or_else(|| Err("timed out listing keys".into()))
    }

    /// Every change to the bucket, starting with its current keys.
    pub fn watch_all(&self) -> Result<KvWatch, String> {
        let watch = wait(self.store.watch_with_history(">")).map_err(|e| e.to_string())?;
        Ok(KvWatch { watch })
    }
}

pub struct KvWatch {
    watch: kv::Watch,
}

impl KvWatch {
    /// The next key that changed, `None` if nothing did within `within`, an
    /// error when the watch has ended and must be opened again.
    pub fn next_timeout(&mut self, within: Duration) -> Result<Option<String>, String> {
        match wait_for(within, self.watch.next()) {
            None => Ok(None),
            Some(Some(Ok(entry))) => Ok(Some(entry.key)),
            Some(Some(Err(e))) => Err(e.to_string()),
            Some(None) => Err("the watch ended".into()),
        }
    }
}

// ───────────────────────── objects ─────────────────────────

/// A JetStream object bucket.
#[derive(Clone)]
pub struct Objects {
    store: object_store::ObjectStore,
}

impl Objects {
    pub fn put(&self, name: &str, mut bytes: &[u8]) -> Result<(), String> {
        wait(self.store.put(name, &mut bytes))
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    /// The object's bytes, or `None` if there is no such object.
    pub fn get(&self, name: &str) -> Result<Option<Vec<u8>>, String> {
        use object_store::GetErrorKind;
        use tokio::io::AsyncReadExt;
        wait(async {
            let mut object = match self.store.get(name).await {
                Ok(object) => object,
                Err(e) if e.kind() == GetErrorKind::NotFound => return Ok(None),
                Err(e) => return Err(e.to_string()),
            };
            let mut bytes = Vec::new();
            object
                .read_to_end(&mut bytes)
                .await
                .map_err(|e| e.to_string())?;
            Ok(Some(bytes))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_ack_subject_gives_its_delivery() {
        let short = "$JS.ACK.VEJAS.flow_x.3.17.9.1793984542000000000.0";
        assert_eq!(
            parse_ack_subject(short),
            Some(Delivery {
                delivered: 3,
                stream_seq: 17
            })
        );
        let long = "$JS.ACK.dom.ACC.VEJAS.flow_x.2.41.9.1793984542000000000.5.token";
        assert_eq!(
            parse_ack_subject(long),
            Some(Delivery {
                delivered: 2,
                stream_seq: 41
            })
        );
        assert_eq!(parse_ack_subject("_INBOX.abc"), None);
        assert_eq!(parse_ack_subject("$JS.ACK.too.few"), None);
    }

    #[test]
    fn the_pull_request_says_what_the_constants_say() {
        let request: Value = serde_json::from_slice(PULL_REQUEST).unwrap();
        assert_eq!(request["batch"], PULL_BATCH);
        assert_eq!(request["expires"], PULL_EXPIRES_MS * 1_000_000);
        assert_eq!(request["no_wait"], true);
    }

    #[test]
    fn a_url_is_one_server_or_a_list() {
        assert_eq!(servers_of("nats://127.0.0.1:4222").unwrap().len(), 1);
        assert_eq!(servers_of("127.0.0.1:4222").unwrap().len(), 1);
        assert_eq!(servers_of("nats://a:4222, nats://b:4222").unwrap().len(), 2);
        assert!(servers_of("http://a:4222").is_err());
    }

    #[test]
    fn statuses_end_a_round_or_a_consumer() {
        let status = |code: StatusCode, description: &str| async_nats::Message {
            subject: "_INBOX.x".into(),
            reply: None,
            payload: Bytes::new(),
            headers: None,
            status: Some(code),
            description: Some(description.to_string()),
            length: 0,
        };
        let verdict = |m: async_nats::Message| match classify(&m) {
            Pulled::Message => "message",
            Pulled::Nothing => "nothing",
            Pulled::Over => "over",
            Pulled::Gone(_) => "gone",
        };
        assert_eq!(
            verdict(status(StatusCode::NOT_FOUND, "No Messages")),
            "nothing"
        );
        assert_eq!(
            verdict(status(StatusCode::TIMEOUT, "Request Timeout")),
            "over"
        );
        assert_eq!(
            verdict(status(
                StatusCode::REQUEST_TERMINATED,
                "Exceeded MaxWaiting"
            )),
            "over"
        );
        assert_eq!(
            verdict(status(StatusCode::REQUEST_TERMINATED, "Consumer Deleted")),
            "gone"
        );
        assert_eq!(verdict(status(StatusCode::NO_RESPONDERS, "")), "gone");
    }

    /// `wait` inside a task of the island would deadlock a worker; tokio refuses.
    #[test]
    fn blocking_inside_the_runtime_panics_instead_of_deadlocking() {
        let task = runtime().spawn(async { wait(async {}) });
        assert!(wait(task).unwrap_err().is_panic());
    }
}

/// Against a real server: each test starts a `nats-server` of its own, so that
/// it can also kill it. Excluded from CI like every test that needs one. Run:
/// `cargo test bus::live -- --ignored`
#[cfg(test)]
mod live {
    use super::*;
    use std::path::PathBuf;
    use std::process::{Child, Command, Stdio};

    struct Server {
        child: Child,
        port: u16,
        store: PathBuf,
    }

    impl Server {
        fn start() -> Server {
            Server::start_with(&[])
        }

        fn start_with(args: &[&str]) -> Server {
            let port = std::net::TcpListener::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap()
                .port();
            let store =
                std::env::temp_dir().join(format!("vejas-bus-{}-{port}", std::process::id()));
            let child = Server::spawn(port, &store, args);
            Server { child, port, store }
        }

        fn spawn(port: u16, store: &PathBuf, args: &[&str]) -> Child {
            let mut child = Command::new("nats-server")
                .args(["-js", "-a", "127.0.0.1", "-p", &port.to_string()])
                .args(args)
                .arg("-sd")
                .arg(store)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("nats-server on PATH");
            for _ in 0..100 {
                if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
                    return child;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            let _ = child.kill();
            let _ = child.wait();
            panic!("nats-server did not come up");
        }

        fn url(&self) -> String {
            format!("nats://127.0.0.1:{}", self.port)
        }

        fn restart(&mut self) {
            self.child.kill().unwrap();
            self.child.wait().unwrap();
            self.child = Server::spawn(self.port, &self.store, &[]);
        }
    }

    impl Drop for Server {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
            let _ = std::fs::remove_dir_all(&self.store);
        }
    }

    fn until(what: &str, done: impl Fn() -> bool) {
        let start = Instant::now();
        while !done() {
            assert!(
                start.elapsed() < Duration::from_secs(15),
                "never happened: {what}"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    #[ignore]
    fn the_barrier_comes_back_after_everything_published_before_it() {
        let server = Server::start();
        let bus = connect(&server.url()).unwrap();
        let mut sub = bus.subscribe("t.barrier").unwrap();
        let mut batch = bus.batch();
        for i in 0..5000 {
            batch.publish("t.barrier", format!("{i}")).unwrap();
        }
        batch.confirm().unwrap();
        // The echo left the server after these: they are all here, unwaited.
        for i in 0..5000 {
            let message = sub.next_timeout(Duration::ZERO).unwrap();
            assert_eq!(&message.data[..], format!("{i}").as_bytes());
        }
    }

    #[test]
    #[ignore]
    fn a_batch_that_spans_a_reconnect_fails_its_barrier() {
        let mut server = Server::start();
        let bus = connect(&server.url()).unwrap();
        let link = &bus.0.link;
        let epoch = link.epoch();
        bus.publish("t.lost", "published before the crash").unwrap();
        server.restart();
        until("the bus is back", || link.up() && link.epoch() != epoch);
        // The echo comes back on the new connection, and proves nothing about
        // the old one.
        assert!(wait(link.barrier()).is_ok());
        assert!(
            wait(link.barrier_since(epoch)).is_err(),
            "that publish may be gone"
        );
        // The next batch stands on the new connection.
        let mut batch = bus.batch();
        batch.publish("t.lost", "published after").unwrap();
        batch.confirm().unwrap();
    }

    #[test]
    #[ignore]
    fn one_unsendable_emit_fails_alone_and_nothing_held_needs_no_barrier() {
        let server = Server::start();
        let bus = connect(&server.url()).unwrap();
        let mut batch = bus.batch();
        batch.confirm().unwrap();
        assert!(batch.publish("two words", "x").is_err());
        assert!(
            batch.publish("a\r\nPUB b 0", "x").is_err(),
            "no protocol injection"
        );
        assert!(batch.publish("", "x").is_err());
        assert!(batch.publish("t.big", vec![0u8; 2 << 20]).is_err());
        batch.publish("t.fine", "x").unwrap();
        batch.confirm().unwrap();
    }

    #[test]
    #[ignore]
    fn connect_refuses_while_the_connection_is_down_and_recovers() {
        let mut server = Server::start();
        let url = server.url();
        let bus = connect(&url).unwrap();
        server.child.kill().unwrap();
        server.child.wait().unwrap();
        until("the bus to notice", || !bus.0.link.up());
        assert!(connect(&url).is_err());
        let mut batch = bus.batch();
        batch.publish("t.x", "x").unwrap();
        assert!(batch.confirm().is_err());
        server.child = Server::spawn(server.port, &server.store, &[]);
        until("the bus to reconnect", || bus.0.link.up());
        assert!(connect(&url).is_ok());
    }

    #[test]
    #[ignore]
    fn a_durable_survives_a_restart_and_keeps_its_place() {
        let mut server = Server::start();
        let js = connect(&server.url()).unwrap().jetstream();
        js.add_stream(&StreamConfig {
            name: "T".into(),
            subjects: vec!["t.>".into()],
            ..Default::default()
        })
        .unwrap();
        let filter = ["t.in".to_string()];
        js.create_pull_consumer("T", "d", &filter, Duration::ZERO, None)
            .unwrap();
        let puller = js.pull_durable("T", "d", Some("t.in")).unwrap();
        assert!(
            js.pull_durable("T", "d", Some("t.other")).is_err(),
            "bound to its subject"
        );
        js.publish("t.in", "one").unwrap();
        let first: Vec<Message> = puller.round().unwrap().collect();
        assert_eq!(first.len(), 1);
        assert_eq!(
            first[0].delivery().unwrap(),
            Delivery {
                delivered: 1,
                stream_seq: 1
            }
        );
        first[0].ack().unwrap();
        // Let the server take the ack before it goes down.
        wait(connect(&server.url()).unwrap().0.link.barrier()).unwrap();
        server.restart();
        until("the bus is back", || connect(&server.url()).is_ok());
        js.publish("t.in", "two").unwrap();
        until("the next message", || {
            puller
                .round()
                .unwrap()
                .next()
                .is_some_and(|m| &m.data[..] == b"two")
        });
    }

    #[test]
    #[ignore]
    fn a_round_ends_at_the_pull_expiry_and_a_deleted_consumer_fails_the_next() {
        let server = Server::start();
        let js = connect(&server.url()).unwrap().jetstream();
        js.add_stream(&StreamConfig {
            name: "T".into(),
            subjects: vec!["t.>".into()],
            ..Default::default()
        })
        .unwrap();
        let filter = ["t.in".to_string()];
        js.create_pull_consumer("T", "d", &filter, Duration::ZERO, None)
            .unwrap();
        let puller = js.pull_durable("T", "d", None).unwrap();
        let t0 = Instant::now();
        assert_eq!(puller.round().unwrap().count(), 0);
        let idle = t0.elapsed();
        assert!(
            idle >= Duration::from_millis(400) && idle < Duration::from_millis(700),
            "{idle:?}"
        );
        // A deleted consumer is told to the request waiting on it, as a 409.
        let waiting = puller.round().unwrap();
        std::thread::sleep(Duration::from_millis(50));
        js.delete_consumer("T", "d").unwrap();
        assert_eq!(waiting.count(), 0);
        let error = puller.round().err().expect("the consumer is gone");
        assert!(error.contains("consumer gone"), "{error}");
    }

    #[test]
    #[ignore]
    fn a_lease_is_compare_and_set() {
        let server = Server::start();
        let js = connect(&server.url()).unwrap().jetstream();
        let kv = js
            .key_value(&KvConfig {
                bucket: "T".into(),
                history: 1,
                max_age: Duration::from_secs(60),
            })
            .unwrap();
        let rev = kv.create("lease", "a").unwrap();
        assert!(kv.create("lease", "b").is_err(), "exactly one wins");
        let renewed = kv.update("lease", "a", rev).unwrap();
        assert!(
            kv.update("lease", "a", rev).is_err(),
            "a stale revision is fenced"
        );
        assert_eq!(kv.get("lease").unwrap().as_deref(), Some(&b"a"[..]));
        kv.delete("lease").unwrap();
        assert_eq!(kv.get("lease").unwrap(), None);
        assert!(
            kv.update("lease", "a", renewed).is_err(),
            "a released lease is not renewed"
        );
        kv.create("lease", "b").unwrap();
        assert_eq!(kv.keys().unwrap(), vec!["lease".to_string()]);
    }

    #[test]
    #[ignore]
    fn a_watch_opens_with_what_is_there_and_follows() {
        let server = Server::start();
        let js = connect(&server.url()).unwrap().jetstream();
        let kv = js
            .key_value(&KvConfig {
                bucket: "T".into(),
                history: 4,
                ..Default::default()
            })
            .unwrap();
        assert!(kv.keys().unwrap().is_empty());
        kv.put("before", "1").unwrap();
        let mut watch = kv.watch_all().unwrap();
        let wait = Duration::from_secs(2);
        assert_eq!(watch.next_timeout(wait).unwrap().as_deref(), Some("before"));
        kv.put("after", "2").unwrap();
        assert_eq!(watch.next_timeout(wait).unwrap().as_deref(), Some("after"));
        assert_eq!(watch.next_timeout(Duration::from_millis(50)).unwrap(), None);
    }

    #[test]
    #[ignore]
    fn a_watch_goes_on_after_a_restart_or_says_it_ended() {
        let mut server = Server::start();
        let url = server.url();
        let js = connect(&url).unwrap().jetstream();
        let kv = js
            .key_value(&KvConfig {
                bucket: "T".into(),
                history: 4,
                ..Default::default()
            })
            .unwrap();
        let mut watch = kv.watch_all().unwrap();
        server.restart();
        until("the bus is back", || connect(&url).is_ok());
        kv.put("after", "1").unwrap();
        let start = Instant::now();
        loop {
            assert!(
                start.elapsed() < Duration::from_secs(30),
                "the watch went silent"
            );
            match watch.next_timeout(Duration::from_millis(500)) {
                Ok(Some(key)) => {
                    assert_eq!(key, "after");
                    break;
                }
                Ok(None) => continue,
                // `version_watch` opens it again from here, and reads what is there
                Err(_) => watch = kv.watch_all().unwrap(),
            }
        }
    }

    #[test]
    #[ignore]
    fn credentials_in_the_url_are_used() {
        let server = Server::start_with(&["--user", "alice", "--pass", "s3cret"]);
        let at = format!("127.0.0.1:{}", server.port);
        assert!(
            connect(&format!("nats://{at}")).is_err(),
            "the server wants credentials"
        );
        assert!(connect(&format!("nats://alice:wrong@{at}")).is_err());
        connect(&format!("nats://alice:s3cret@{at}")).unwrap();
        let server = Server::start_with(&["--auth", "t0ken"]);
        connect(&format!("nats://t0ken@127.0.0.1:{}", server.port)).unwrap();
    }

    #[test]
    #[ignore]
    fn an_object_comes_back_whole_and_a_missing_one_is_none() {
        let server = Server::start();
        let js = connect(&server.url()).unwrap().jetstream();
        let objects = js.object_store("T").unwrap();
        assert_eq!(objects.get("detect:x").unwrap(), None);
        let big: Vec<u8> = (0..300_000u32).map(|i| i as u8).collect();
        objects.put("detect:x", &big).unwrap();
        assert_eq!(objects.get("detect:x").unwrap().as_deref(), Some(&big[..]));
        objects.put("detect:x", b"smaller").unwrap();
        assert_eq!(
            objects.get("detect:x").unwrap().as_deref(),
            Some(&b"smaller"[..])
        );
    }
}
