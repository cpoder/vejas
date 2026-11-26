//! Pull rounds: how long a consumer waits and fetches, and subject matching for
//! pull consumers over several subjects.

use std::time::Duration;

use crate::bus;

/// Consumer ack-wait (redelivery latency): a message not acked within this window
/// redelivers (at-least-once, ADR-0002). `VEJAS_ACK_WAIT_SECS` tunes it; unset (or
/// ≤0) keeps `Duration::ZERO`, which JetStream reads as its 30s default — the
/// historical behaviour. Lower it to make transient failures retry sooner (and to
/// let the transport tests exercise redelivery + the poison→DLQ cap at CI speed).
/// A positive value is floored at 1s: JetStream rejects a sub-second ack-wait
/// (the consumer would silently fail to create), so we never pass one through.
pub fn ack_wait() -> Duration {
    std::env::var("VEJAS_ACK_WAIT_SECS")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|&s| s > 0.0)
        .map(|s| Duration::from_secs_f64(s.max(1.0)))
        .unwrap_or_default()
}

/// One whole pull round, collected: for one-off scans (replay hydration, the
/// audit list, the shadow watch). A hot loop iterates `Puller::round` instead,
/// so that it works on messages as they arrive.
pub fn fetch_round(sub: &bus::Puller) -> Result<Vec<bus::Message>, String> {
    Ok(sub.round()?.collect())
}

/// Hydrate up to `n` recent REAL events for `subject` from the persisted stream,
/// for shadow-replay (ADR-0018). Read-only: an **ephemeral** pull consumer (no
/// durable name → a server-assigned name, wholly separate from the flow's own
/// durable) reads history and is never acked — so the live flow's delivery state
/// is untouched and the bus is not consumed. Returns (subject, event) newest
/// last, at most `n`.
///
/// To bound work on a large stream, it starts a window before the stream's last
/// sequence rather than from the beginning: at most `window` messages are
/// scanned, of which the last `n` matching `subject` are kept.
pub fn hydrate_recent(
    js: &bus::JetStream,
    stream: &str,
    subject: &str,
    n: usize,
) -> Result<Vec<(u64, String, serde_json::Value)>, String> {
    let n = n.clamp(1, 5000);
    let last = js.stream_state(stream).map(|state| state.last_seq).unwrap_or(0);
    if last == 0 {
        return Ok(Vec::new()); // empty stream: nothing persisted yet
    }
    // Over-scan so we still find n matching events when other subjects interleave,
    // but keep it bounded (an operator action, not the hot path).
    let window = (n as u64).saturating_mul(8).clamp(256, 200_000);
    let start = last.saturating_sub(window).max(1);
    // A cursor that takes no acks: the consumer is ephemeral and discarded, so
    // nothing redelivers to the flow and nothing is consumed off the stream.
    let cfg = bus::EphemeralConfig {
        filter: subject,
        start_seq: Some(start),
        // auto-reap the ephemeral consumer shortly after we stop pulling
        inactive_threshold: Duration::from_secs(30),
    };
    let sub = js.pull_ephemeral(stream, &cfg)?;
    let mut ring: std::collections::VecDeque<(u64, String, serde_json::Value)> =
        std::collections::VecDeque::with_capacity(n);
    let mut scanned = 0u64;
    loop {
        let batch = fetch_round(&sub)?;
        if batch.is_empty() {
            break; // no_wait + fully-persisted stream: empty means exhausted
        }
        for m in &batch {
            scanned += 1;
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&m.data) {
                // the stream sequence is the intrinsic join key for time-travel /
                // canary diffs (ADR-0021) — no per-flow key to invent.
                let seq = m.delivery().map(|i| i.stream_seq).unwrap_or(0);
                ring.push_back((seq, m.subject.clone(), v));
                while ring.len() > n {
                    ring.pop_front();
                }
            }
        }
        if scanned >= window {
            break; // safety bound hit
        }
    }
    Ok(ring.into_iter().collect())
}

/// NATS subject matching: `*` stands for one token, `>` for the rest (at least one).
pub fn subject_matches(pattern: &str, subject: &str) -> bool {
    let mut p = pattern.split('.');
    let mut s = subject.split('.');
    loop {
        match (p.next(), s.next()) {
            (None, None) => return true,
            (Some(">"), Some(_)) => return true,
            (Some("*"), Some(_)) => {}
            (Some(a), Some(b)) if a == b => {}
            _ => return false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    /// Verifies a confirmed publish keeps the at-least-once contract: messages
    /// actually persist, and a subject with no stream errors (never a false Ok).
    /// Needs a live NATS on 127.0.0.1:4222; excluded from CI. Run:
    /// `cargo test confirmed_publish -- --ignored --nocapture`
    #[test]
    #[ignore]
    fn confirmed_publish_persists_or_errors() {
        let url = std::env::var("NATS_URL").unwrap_or_else(|_| "127.0.0.1:4222".into());
        let js = bus::connect(&url).expect("connect").jetstream();
        let pid = std::process::id();
        let stream = format!("PBFAST_{pid}");
        let subject = format!("pbfast.{pid}.in");
        let _ = js.delete_stream(&stream);
        js.add_stream(&bus::StreamConfig {
            name: stream.clone(),
            subjects: vec![format!("pbfast.{pid}.>")],
            ..Default::default()
        })
        .expect("add_stream");

        const N: usize = 400;
        let t = Instant::now();
        for _ in 0..N {
            js.publish(&subject, b"x").expect("js.publish");
        }
        eprintln!("[pbfast] js.publish={:.0}/s", N as f64 / t.elapsed().as_secs_f64());

        // durability: all N messages actually landed on the stream
        let msgs = js.stream_state(&stream).expect("info").messages;
        assert!(msgs >= N as u64, "every confirmed publish persisted (have {msgs})");

        // no-responders guard: a subject with no stream must ERROR, not false-Ok
        let orphan = format!("orphan.{pid}.no.stream");
        let r = js.publish(&orphan, b"x");
        assert!(r.is_err(), "publish to a stream-less subject must error, got {r:?}");

        let _ = js.delete_stream(&stream);
    }

    /// Latency probe for `fetch_round` — the shared pull loop every sink/flow runs.
    /// Measures (A) idle empty-round wall time and (B) publish→receipt latency when
    /// a message lands mid-idle. Needs a live NATS on 127.0.0.1:4222; excluded from
    /// CI. Run: `cargo test fetch_round_latency_probe -- --ignored --nocapture`.
    #[test]
    #[ignore]
    fn fetch_round_latency_probe() {
        let url = std::env::var("NATS_URL").unwrap_or_else(|_| "127.0.0.1:4222".into());
        let nc = bus::connect(&url).expect("connect nats");
        let js = nc.jetstream();
        let pid = std::process::id();
        let stream = format!("PROBE_{pid}");
        let subject = format!("probe.{pid}.in");
        let _ = js.delete_stream(&stream);
        js.add_stream(&bus::StreamConfig {
            name: stream.clone(),
            subjects: vec![format!("probe.{pid}.>")],
            ..Default::default()
        })
        .expect("add_stream");
        let durable = format!("probe_dur_{pid}");
        let filter = [subject.clone()];
        js.create_pull_consumer(&stream, &durable, &filter, Duration::ZERO, None).expect("consumer");
        let sub = js.pull_durable(&stream, &durable, Some(&subject)).expect("pull_subscribe");

        // (A) idle: how long does an empty round block?
        let mut empties = Vec::new();
        for _ in 0..5 {
            let t = Instant::now();
            let msgs = fetch_round(&sub).expect("fetch");
            assert!(msgs.is_empty(), "probe subject should be idle");
            empties.push(t.elapsed().as_millis());
        }
        eprintln!("[probe] empty-round ms: {empties:?}");

        // (B) publish->receipt: a message lands at a SWEPT offset into an idle
        // stretch (20..~300ms) so samples hit every phase of the poll cycle.
        // Detection latency = receipt - the publish offset.
        let mut lat = Vec::new();
        for i in 0..30u32 {
            let delay_ms = 20 + (i as u64 * 23) % 280; // sweep across the ~252ms cycle
            let (ncp, subj, payload) = (nc.clone(), subject.clone(), format!("m{i}"));
            let t0 = Instant::now();
            let pubber = std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(delay_ms));
                ncp.publish(&subj, payload).expect("publish");
            });
            let detected = loop {
                let msgs = fetch_round(&sub).expect("fetch");
                if !msgs.is_empty() {
                    for m in &msgs {
                        let _ = m.ack();
                    }
                    break t0.elapsed().as_millis() as i64 - delay_ms as i64;
                }
            };
            pubber.join().unwrap();
            lat.push(detected.max(0));
        }
        lat.sort_unstable();
        eprintln!(
            "[probe] publish->receipt detection ms: min={} p50={} p90={} max={} all={:?}",
            lat[0],
            lat[lat.len() / 2],
            lat[(lat.len() * 9) / 10],
            lat[lat.len() - 1],
            lat
        );
        let _ = js.delete_stream(&stream);
    }

    #[test]
    fn literal_and_wildcards() {
        assert!(subject_matches("vx.sysmon.net", "vx.sysmon.net"));
        assert!(!subject_matches("vx.sysmon.net", "vx.sysmon.proc"));
        assert!(subject_matches("vx.sysmon.*", "vx.sysmon.net"));
        assert!(!subject_matches("vx.sysmon.*", "vx.sysmon.net.extra"));
        assert!(subject_matches("vx.>", "vx.sysmon.net"));
        assert!(!subject_matches("vx.>", "vx"));
        assert!(!subject_matches("vx.sysmon", "vx.sysmon.net"));
        assert!(!subject_matches("vx.sysmon.net", "vx.sysmon"));
    }
}
