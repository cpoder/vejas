// Connector SDK (ADR-0007).
//
// A connector is a native Rust **driver** + a declarative **instance manifest**
// (`connectors/<name>.vjs`: `driver "http-in"` plus UPPERCASE literal config).
// Drivers are compiled in; instances are data — hot-addable, editable in the
// panel like any other business surface. Two families:
//
//   Source  — pushes onto the bus. Trigger kinds: webhook, poll, interval,
//             (queue/stream = future drivers). Publishes vx.<...>.
//   Sink    — consumes from the bus (durable pull consumer) and does a side
//             effect. Ack after success, else nak (redelivery = retry).
//
// The subject convention (SUBJECTS.md) remains the whole interface, so an
// external connector in any language is still a first-class citizen over the
// bus. These native drivers are the batteries included.
//
// A driver is written against the connector crate, vejas-connector (ADR-0038):
// its Config, its Ctx, the Driver trait, the bus, and the services every driver
// shares (publishing a source's input as the first hop of its trace, the sink
// loop, offsets, the dead-letter queue). This module holds what the runtime
// adds: the host that records each hop a driver reports, the services that
// only in-process drivers use (the HTTP client, child processes, the thread
// pool), two runtime services that lived here (audit, snapshots), and the
// bundled drivers in connectors/drivers/, one module per protocol.

mod audit;
mod drivers;
mod host;
mod http_client;
mod process;
mod slot;
mod snapshots;

pub use vejas_connector::{Config, Ctx, Driver};

// What the rest of the runtime uses.
pub use audit::{audit_recent, to_audit};
pub use drivers::{catalog, driver_for};
pub use host::host;
pub use http_client::http_request;
pub use process::without_runtime_credentials;
pub use slot::Slot;
pub use snapshots::{open_state_store, snapshot_get, snapshot_put, Snapshot};
pub use vejas_connector::clock::iso8601_utc;
pub use vejas_connector::dlq::{dlq_unit_token, ensure_dlq_stream, to_dlq, DLQ_STREAM};
pub use vejas_connector::pull::{ack_wait, fetch_round, hydrate_recent, subject_matches};
pub use vejas_connector::trace_preview;

/// Plain-words prefix for the errors an admin actually meets.
pub fn humanize(e: &str) -> String {
    if e.contains("HTTP 401") || e.contains("HTTP 403") {
        format!("authentication refused (invalid or expired secret?) — {e}")
    } else if e.contains("not set") || e.contains("not found (set env") || (e.contains("secret") && e.contains("not found")) {
        format!("missing secret — {e}")
    } else if e.starts_with("http:") || e.contains("Connection refused") || e.contains("Could not resolve") || e.contains("timed out") {
        format!("unreachable (network, URL, proxy?) — {e}")
    } else {
        e.to_string()
    }
}
