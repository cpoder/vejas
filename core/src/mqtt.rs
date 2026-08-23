//! A hand-rolled synchronous MQTT 3.1.1 client, QoS 0/1 (ADR-0025).
//!
//! MQTT 3.1.1 is a small binary protocol, so we own it rather than pull an async
//! stack (rumqttc → tokio) or a C client (paho). Zero dependency, house style,
//! metrics/OTLP-native. The client is generic over a `Read + Write` stream so the
//! same code runs over plain TCP or a rustls TLS stream. Scope is deliberately
//! disciplined: 3.1.1, QoS 0/1 — QoS 2 / MQTT 5 fall to a mosquitto exec-bridge.
//!
//! The end-to-end at-least-once invariant maps onto the QoS 1 handshake with no
//! KV: on the source, PUBACK is sent only AFTER the bus publish is confirmed, so
//! the broker retransmits anything not yet acked. The caller drives that ordering.

use std::io::{self, Read, Write};
use std::time::{Duration, Instant};

// packet types (high nibble of byte 1)
const CONNECT: u8 = 1;
const CONNACK: u8 = 2;
const PUBLISH: u8 = 3;
const PUBACK: u8 = 4;
const SUBSCRIBE: u8 = 8;
const SUBACK: u8 = 9;
const PINGREQ: u8 = 12;
const PINGRESP: u8 = 13;
const DISCONNECT: u8 = 14;

/// A parsed inbound packet (only the ones a QoS 0/1 client must handle). Some
/// fields are carried for completeness/inspection even where a given caller does
/// not read them.
#[derive(Debug)]
#[allow(dead_code)]
pub enum Packet {
    ConnAck { session_present: bool, code: u8 },
    Publish { topic: String, payload: Vec<u8>, qos: u8, pid: u16 },
    PubAck { pid: u16 },
    SubAck { pid: u16, code: u8 },
    PingResp,
    Other(u8),
}

fn put_str(buf: &mut Vec<u8>, s: &str) {
    let b = s.as_bytes();
    buf.extend_from_slice(&(b.len() as u16).to_be_bytes());
    buf.extend_from_slice(b);
}

/// MQTT remaining-length varint (1–4 bytes).
fn put_remaining_len(buf: &mut Vec<u8>, mut n: usize) {
    loop {
        let mut byte = (n % 128) as u8;
        n /= 128;
        if n > 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if n == 0 {
            break;
        }
    }
}

fn frame(packet_type: u8, flags: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len() + 5);
    out.push((packet_type << 4) | (flags & 0x0f));
    put_remaining_len(&mut out, body.len());
    out.extend_from_slice(body);
    out
}

/// A synchronous MQTT client over any `Read + Write` stream (TCP or TLS).
pub struct Client<S: Read + Write> {
    stream: S,
    next_pid: u16,
    keepalive: Duration,
    last_out: Instant,
}

impl<S: Read + Write> Client<S> {
    pub fn new(stream: S, keepalive_secs: u16) -> Self {
        Client {
            stream,
            next_pid: 1,
            keepalive: Duration::from_secs(keepalive_secs.max(1) as u64),
            last_out: Instant::now(),
        }
    }

    fn pid(&mut self) -> u16 {
        let p = self.next_pid;
        self.next_pid = self.next_pid.wrapping_add(1).max(1);
        p
    }

    fn send(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.stream.write_all(bytes)?;
        self.stream.flush()?;
        self.last_out = Instant::now();
        Ok(())
    }

    /// CONNECT + wait for CONNACK. `clean_session=false` keeps the broker's
    /// subscription+inflight across reconnects (ADR-0025 at-least-once).
    pub fn connect(
        &mut self,
        client_id: &str,
        clean_session: bool,
        user: Option<&str>,
        pass: Option<&str>,
    ) -> Result<(), String> {
        let mut body = Vec::new();
        put_str(&mut body, "MQTT");
        body.push(4); // protocol level (3.1.1)
        let mut flags = 0u8;
        if clean_session {
            flags |= 0x02;
        }
        if user.is_some() {
            flags |= 0x80;
        }
        if pass.is_some() {
            flags |= 0x40;
        }
        body.push(flags);
        let ka = self.keepalive.as_secs() as u16;
        body.extend_from_slice(&ka.to_be_bytes());
        put_str(&mut body, client_id);
        if let Some(u) = user {
            put_str(&mut body, u);
        }
        if let Some(p) = pass {
            put_str(&mut body, p);
        }
        self.send(&frame(CONNECT, 0, &body)).map_err(|e| e.to_string())?;
        match self.read_packet_blocking()? {
            Packet::ConnAck { code: 0, .. } => Ok(()),
            Packet::ConnAck { code, .. } => Err(format!("CONNACK refused (code {code})")),
            other => Err(format!("expected CONNACK, got {other:?}")),
        }
    }

    /// SUBSCRIBE to one topic at the given QoS + wait for SUBACK.
    pub fn subscribe(&mut self, topic: &str, qos: u8) -> Result<(), String> {
        let pid = self.pid();
        let mut body = Vec::new();
        body.extend_from_slice(&pid.to_be_bytes());
        put_str(&mut body, topic);
        body.push(qos);
        // SUBSCRIBE requires the reserved flags 0b0010
        self.send(&frame(SUBSCRIBE, 0x02, &body)).map_err(|e| e.to_string())?;
        match self.read_packet_blocking()? {
            Packet::SubAck { code, .. } if code <= 2 => Ok(()),
            Packet::SubAck { code, .. } => Err(format!("SUBACK failure (0x{code:02x})")),
            other => Err(format!("expected SUBACK, got {other:?}")),
        }
    }

    /// PUBLISH a message. QoS 0 returns immediately; QoS 1 returns the packet id
    /// and the caller waits for the matching PUBACK.
    pub fn publish(&mut self, topic: &str, payload: &[u8], qos: u8) -> Result<Option<u16>, String> {
        let pid = if qos > 0 { Some(self.pid()) } else { None };
        let mut body = Vec::new();
        put_str(&mut body, topic);
        if let Some(p) = pid {
            body.extend_from_slice(&p.to_be_bytes());
        }
        body.extend_from_slice(payload);
        let flags = (qos & 0x03) << 1;
        self.send(&frame(PUBLISH, flags, &body)).map_err(|e| e.to_string())?;
        Ok(pid)
    }

    /// Acknowledge an inbound QoS 1 PUBLISH (sent only AFTER its side effect).
    pub fn puback(&mut self, pid: u16) -> Result<(), String> {
        self.send(&frame(PUBACK, 0, &pid.to_be_bytes())).map_err(|e| e.to_string())
    }

    pub fn pingreq(&mut self) -> Result<(), String> {
        self.send(&frame(PINGREQ, 0, &[])).map_err(|e| e.to_string())
    }

    pub fn disconnect(&mut self) {
        let _ = self.send(&frame(DISCONNECT, 0, &[]));
    }

    /// Send a PINGREQ if the keepalive window is running out and nothing else has
    /// gone out — the classic hand-rolled-client trap is an idle line starving the
    /// keepalive until the broker cuts at 1.5× the interval. Call this whenever a
    /// read times out.
    pub fn keepalive_tick(&mut self) -> Result<(), String> {
        if self.last_out.elapsed() >= self.keepalive / 2 {
            self.pingreq()?;
        }
        Ok(())
    }

    /// Read one packet, treating a read timeout (a set_read_timeout on the
    /// underlying TcpStream) as "nothing yet" → Ok(None), so the caller can run
    /// its keepalive tick. A partial-then-timeout mid-packet is a real error.
    pub fn read_packet(&mut self) -> Result<Option<Packet>, String> {
        let mut b1 = [0u8; 1];
        match self.stream.read(&mut b1) {
            Ok(0) => return Err("connection closed by broker".into()),
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut => {
                return Ok(None);
            }
            Err(e) => return Err(e.to_string()),
        }
        let remaining = self.read_remaining_len()?;
        let mut body = vec![0u8; remaining];
        self.read_exact(&mut body)?;
        Ok(Some(parse(b1[0], &body)))
    }

    fn read_packet_blocking(&mut self) -> Result<Packet, String> {
        // used for CONNACK/SUBACK where we always expect a reply promptly
        loop {
            if let Some(p) = self.read_packet()? {
                return Ok(p);
            }
        }
    }

    fn read_remaining_len(&mut self) -> Result<usize, String> {
        let mut mult = 1usize;
        let mut value = 0usize;
        for _ in 0..4 {
            let mut byte = [0u8; 1];
            self.read_exact(&mut byte)?;
            value += (byte[0] & 0x7f) as usize * mult;
            if byte[0] & 0x80 == 0 {
                return Ok(value);
            }
            mult *= 128;
        }
        Err("malformed remaining length".into())
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), String> {
        self.stream.read_exact(buf).map_err(|e| e.to_string())
    }
}

fn parse(b1: u8, body: &[u8]) -> Packet {
    let ptype = b1 >> 4;
    match ptype {
        CONNACK => Packet::ConnAck {
            session_present: body.first().map(|b| b & 1 == 1).unwrap_or(false),
            code: body.get(1).copied().unwrap_or(0xff),
        },
        PUBACK => Packet::PubAck {
            pid: u16::from_be_bytes([body.first().copied().unwrap_or(0), body.get(1).copied().unwrap_or(0)]),
        },
        SUBACK => Packet::SubAck {
            pid: u16::from_be_bytes([body.first().copied().unwrap_or(0), body.get(1).copied().unwrap_or(0)]),
            code: body.get(2).copied().unwrap_or(0x80),
        },
        PINGRESP => Packet::PingResp,
        PUBLISH => {
            let qos = (b1 >> 1) & 0x03;
            // topic
            let tlen = u16::from_be_bytes([body.first().copied().unwrap_or(0), body.get(1).copied().unwrap_or(0)]) as usize;
            let mut i = 2;
            let topic = String::from_utf8_lossy(body.get(i..i + tlen).unwrap_or(&[])).to_string();
            i += tlen;
            let pid = if qos > 0 {
                let p = u16::from_be_bytes([body.get(i).copied().unwrap_or(0), body.get(i + 1).copied().unwrap_or(0)]);
                i += 2;
                p
            } else {
                0
            };
            let payload = body.get(i..).unwrap_or(&[]).to_vec();
            Packet::Publish { topic, payload, qos, pid }
        }
        other => Packet::Other(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    // a fake bidirectional stream: reads from `inbound`, records writes in `out`
    struct Fake {
        inbound: Cursor<Vec<u8>>,
        out: Vec<u8>,
    }
    impl Read for Fake {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.inbound.read(buf)
        }
    }
    impl Write for Fake {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.out.extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn connect_encodes_and_reads_connack() {
        // broker replies CONNACK (type 2), session-present 0, code 0
        let connack = vec![0x20, 0x02, 0x00, 0x00];
        let fake = Fake { inbound: Cursor::new(connack), out: Vec::new() };
        let mut c = Client::new(fake, 30);
        c.connect("vejas-test", false, Some("u"), Some("p")).unwrap();
        // the first written byte is a CONNECT packet (type 1 → 0x10)
        assert_eq!(c.stream.out[0] >> 4, CONNECT);
        // "MQTT" protocol name appears in the variable header
        assert!(c.stream.out.windows(4).any(|w| w == b"MQTT"));
    }

    #[test]
    fn publish_qos1_carries_a_packet_id_and_puback_roundtrips() {
        let fake = Fake { inbound: Cursor::new(vec![]), out: Vec::new() };
        let mut c = Client::new(fake, 30);
        let pid = c.publish("t/1", b"hi", 1).unwrap();
        assert!(pid.is_some());
        // frame is PUBLISH (type 3) with qos bits = 01
        assert_eq!(c.stream.out[0] >> 4, PUBLISH);
        assert_eq!((c.stream.out[0] >> 1) & 0x03, 1);
    }

    #[test]
    fn parses_inbound_publish_qos1() {
        // PUBLISH qos1: byte1 0x32, remaining, topic "a", pid 7, payload "x"
        let mut body = Vec::new();
        put_str(&mut body, "a");
        body.extend_from_slice(&7u16.to_be_bytes());
        body.extend_from_slice(b"x");
        let mut wire = frame(PUBLISH, 0x02, &body);
        wire.extend_from_slice(&[]); // ensure it's a full frame
        let fake = Fake { inbound: Cursor::new(wire), out: Vec::new() };
        let mut c = Client::new(fake, 30);
        match c.read_packet().unwrap().unwrap() {
            Packet::Publish { topic, payload, qos, pid } => {
                assert_eq!(topic, "a");
                assert_eq!(payload, b"x");
                assert_eq!(qos, 1);
                assert_eq!(pid, 7);
            }
            p => panic!("expected Publish, got {p:?}"),
        }
    }
}
