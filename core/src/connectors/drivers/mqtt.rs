//! mqtt-in and mqtt-out: a hand-rolled synchronous MQTT 3.1.1 client (ADR-0025).

use std::thread;
use std::time::Duration;

use crate::connectors::hops::{source_publish, trace_fail};
use crate::connectors::sink::run_sink;
use crate::connectors::{Ctx, Driver};
use crate::metrics::Kind;

/// The MQTT client's stream: plain TCP or a rustls TLS stream. One monomorphic
/// type so `Client<MqttTransport>` covers both without a trait object.
enum MqttTransport {
    Plain(std::net::TcpStream),
    Tls(Box<rustls::StreamOwned<rustls::ClientConnection, std::net::TcpStream>>),
}
impl std::io::Read for MqttTransport {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(s) => s.read(buf),
            Self::Tls(s) => s.read(buf),
        }
    }
}
impl std::io::Write for MqttTransport {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(s) => s.write(buf),
            Self::Tls(s) => s.write(buf),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Plain(s) => s.flush(),
            Self::Tls(s) => s.flush(),
        }
    }
}

/// Shared rustls client config (Mozilla roots via webpki-roots), built once.
fn mqtt_tls_config() -> std::sync::Arc<rustls::ClientConfig> {
    static CFG: std::sync::OnceLock<std::sync::Arc<rustls::ClientConfig>> = std::sync::OnceLock::new();
    CFG.get_or_init(|| {
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        std::sync::Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        )
    })
    .clone()
}

/// Connect an MQTT session (plain TCP or TLS) with the given read timeout (drives
/// the keepalive tick / PUBACK wait). The read timeout is set on the underlying
/// TcpStream before the TLS wrap, so an idle keepalive read times out cleanly
/// before any TLS record starts.
fn mqtt_connect(
    broker: &str,
    client_id: &str,
    clean_session: bool,
    user: Option<&str>,
    pass: Option<&str>,
    keepalive: u16,
    read_timeout: Duration,
    tls: bool,
) -> Result<crate::mqtt::Client<MqttTransport>, String> {
    let tcp = std::net::TcpStream::connect(broker).map_err(|e| e.to_string())?;
    tcp.set_read_timeout(Some(read_timeout)).ok();
    tcp.set_nodelay(true).ok();
    let transport = if tls {
        let host = broker.rsplit_once(':').map(|(h, _)| h).unwrap_or(broker);
        let name = rustls::pki_types::ServerName::try_from(host.to_string())
            .map_err(|e| format!("bad TLS host {host:?}: {e}"))?;
        let conn = rustls::ClientConnection::new(mqtt_tls_config(), name)
            .map_err(|e| e.to_string())?;
        MqttTransport::Tls(Box::new(rustls::StreamOwned::new(conn, tcp)))
    } else {
        MqttTransport::Plain(tcp)
    };
    let mut client = crate::mqtt::Client::new(transport, keepalive);
    client.connect(client_id, clean_session, user, pass)?;
    Ok(client)
}

fn mqtt_client_id(ctx: &Ctx) -> String {
    ctx.config
        .str("CLIENT_ID")
        .unwrap_or_else(|| format!("vejas-{}", ctx.name.replace([':', '.'], "-")))
}

pub(super) struct MqttIn;
impl Driver for MqttIn {
    fn kind(&self) -> &'static str {
        // structurally singleton in 3.1.1: shared subscriptions are MQTT5-only, so
        // two subscribers on the same client id can't split a topic (ADR-0025).
        "source:mqtt"
    }
    fn about(&self) -> &'static str {
        "Subscribes to an MQTT topic and publishes each message to SUBJECT (hand-rolled sync client, 3.1.1, QoS 0/1). At-least-once maps onto QoS 1 with no KV: the PUBACK to the broker is sent only AFTER the bus publish is confirmed, so the broker retransmits anything not yet acked — CLEAN_SESSION=false keeps the subscription across reconnects. Singleton (structural in 3.1.1). Config: BROKER (host:port), TOPIC, SUBJECT, QOS (0/1, default 1), CLIENT_ID (default vejas-<name>), USERNAME/PASSWORD (secret()), KEEPALIVE_SECS (default 30), TLS (bool, default false → rustls). QoS2/MQTT5 → mosquitto exec-bridge."
    }
    fn run(&self, ctx: &Ctx) -> Result<(), String> {
        let broker = ctx.config.str("BROKER").ok_or("BROKER required")?;
        let topic = ctx.config.str("TOPIC").ok_or("TOPIC required")?;
        let subject = ctx.subject(&ctx.config.str("SUBJECT").ok_or("SUBJECT required")?);
        let qos = ctx.config.u64_or("QOS", 1).min(1) as u8;
        let keepalive = ctx.config.u64_or("KEEPALIVE_SECS", 30) as u16;
        let tls = ctx.config.value("TLS").and_then(|v| v.as_bool()).unwrap_or(false);
        let client_id = mqtt_client_id(ctx);
        let user = ctx.config.str("USERNAME");
        let pass = ctx.config.str("PASSWORD");
        let js = ctx.jetstream()?;
        while ctx.alive() {
            let attempt = (|| -> Result<(), String> {
                let mut client = mqtt_connect(
                    &broker, &client_id, false, user.as_deref(), pass.as_deref(),
                    keepalive, Duration::from_millis(500), tls,
                )?;
                client.subscribe(&topic, qos)?;
                eprintln!("[{}] mqtt subscribed {topic} @qos{qos} -> {subject}", ctx.name);
                while ctx.alive() {
                    match client.read_packet()? {
                        Some(crate::mqtt::Packet::Publish { payload, qos: pq, pid, .. }) => {
                            // publish-before-ack: bus first (await JetStream pub-ack),
                            // THEN PUBACK — a crash before the bus write re-delivers
                            // from the broker (ADR-0025).
                            source_publish(&ctx.name, &js, &subject, &payload, None, Kind::Producer)
                                .1
                                .map_err(|e| format!("bus publish: {e}"))?;
                            if pq > 0 {
                                client.puback(pid)?;
                            }
                        }
                        Some(_) => {} // PingResp / other
                        None => client.keepalive_tick()?, // idle read → maybe PINGREQ
                    }
                }
                client.disconnect();
                Ok(())
            })();
            match attempt {
                Ok(()) => break,
                Err(e) => {
                    trace_fail(&ctx.name, &subject, e.clone());
                    eprintln!("[{}] mqtt: {e}; reconnect in 2s", ctx.name);
                    let mut slept = 0;
                    while slept < 2000 && ctx.alive() {
                        thread::sleep(Duration::from_millis(200));
                        slept += 200;
                    }
                }
            }
        }
        Ok(())
    }
}

pub(super) struct MqttOut;
impl Driver for MqttOut {
    fn kind(&self) -> &'static str {
        "sink:mqtt"
    }
    fn about(&self) -> &'static str {
        "Consumes SUBJECT and PUBLISHes each message to an MQTT topic (hand-rolled sync client, 3.1.1, QoS 0/1). QoS 1: PUBLISH → await the broker's PUBACK → THEN ack the bus message (side-effect-before-ack); a crash between them redelivers → duplicate PUBLISH (standard at-least-once). Competing-safe (durable pull). Config: BROKER, TOPIC, SUBJECT, QOS (default 1), CLIENT_ID, USERNAME/PASSWORD (secret()), KEEPALIVE_SECS. TLS + QoS2/MQTT5 → mosquitto exec-bridge."
    }
    fn run(&self, ctx: &Ctx) -> Result<(), String> {
        let broker = ctx.config.str("BROKER").ok_or("BROKER required")?;
        let topic = ctx.config.str("TOPIC").ok_or("TOPIC required")?;
        let subject = ctx.subject(&ctx.config.str("SUBJECT").ok_or("SUBJECT required")?);
        let qos = ctx.config.u64_or("QOS", 1).min(1) as u8;
        let keepalive = ctx.config.u64_or("KEEPALIVE_SECS", 30) as u16;
        let tls = ctx.config.value("TLS").and_then(|v| v.as_bool()).unwrap_or(false);
        let client_id = mqtt_client_id(ctx);
        let user = ctx.config.str("USERNAME");
        let pass = ctx.config.str("PASSWORD");
        // one MQTT session, reconnected on error (Mutex<Option<_>> for the Fn handler)
        let slot: std::sync::Mutex<Option<crate::mqtt::Client<MqttTransport>>> =
            std::sync::Mutex::new(None);
        // MQTT 3.1.1 has no headers: the trace stops at the broker.
        run_sink(ctx, &subject, move |data, _trace| {
            let mut guard = slot.lock().unwrap();
            if guard.is_none() {
                *guard = Some(mqtt_connect(
                    &broker, &client_id, true, user.as_deref(), pass.as_deref(),
                    keepalive, Duration::from_secs(5), tls,
                )?);
            }
            let client = guard.as_mut().unwrap();
            let publish = (|| -> Result<(), String> {
                let pid = client.publish(&topic, data, qos)?;
                if let Some(want) = pid {
                    // side-effect-before-ack: wait for the broker's PUBACK
                    loop {
                        match client.read_packet()? {
                            Some(crate::mqtt::Packet::PubAck { pid }) if pid == want => break,
                            Some(_) => {}
                            None => return Err("timed out waiting for PUBACK".into()),
                        }
                    }
                }
                Ok(())
            })();
            if publish.is_err() {
                *guard = None; // drop the broken session so the next call reconnects
            }
            publish.map(|_| None) // Ok → ack the bus message (side-effect done)
        })
    }
}
