//! A hop's span (ADR-0034): the type the trace record and the OTLP exporter
//! take, built by whoever ran the hop, the runtime or a standalone connector.

use std::time::{SystemTime, UNIX_EPOCH};

use crate::trace;

pub fn now_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

/// OpenTelemetry span kinds, numbered as OTLP numbers them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Internal = 1,
    Server = 2,
    Client = 3,
    Producer = 4,
    Consumer = 5,
}

impl Kind {
    /// The kind as the trace record writes it.
    pub fn label(self) -> &'static str {
        match self {
            Kind::Internal => "internal",
            Kind::Server => "server",
            Kind::Client => "client",
            Kind::Producer => "producer",
            Kind::Consumer => "consumer",
        }
    }
}

#[derive(Clone)]
pub enum Attr {
    Str(String),
    /// A constant: no allocation on the hot path.
    Static(&'static str),
    /// A value the runtime computed from its own state, never from an event:
    /// a file under the project root, a content hash. Shared, so a hop does
    /// not copy it, and the trace record writes it with no scrub.
    Computed(std::sync::Arc<str>),
    Int(i64),
    Bool(bool),
}

impl From<&str> for Attr {
    fn from(v: &str) -> Attr {
        Attr::Str(v.to_string())
    }
}
impl From<String> for Attr {
    fn from(v: String) -> Attr {
        Attr::Str(v)
    }
}
impl From<i64> for Attr {
    fn from(v: i64) -> Attr {
        Attr::Int(v)
    }
}
impl From<u64> for Attr {
    fn from(v: u64) -> Attr {
        Attr::Int(v.min(i64::MAX as u64) as i64)
    }
}
impl From<bool> for Attr {
    fn from(v: bool) -> Attr {
        Attr::Bool(v)
    }
}

/// One finished span, queued for the background exporter and, for a hop, the
/// runtime's trace record.
#[derive(Clone)]
pub struct Span {
    pub name: String,
    pub kind: Kind,
    pub trace_id: u128,
    pub span_id: u64,
    pub parent: Option<u64>,
    pub sampled: bool,
    pub start_nanos: u128,
    pub end_nanos: u128,
    /// The failure, if the hop failed; a success leaves the status unset, as the
    /// semantic conventions ask of instrumentation.
    pub error: Option<String>,
    pub attrs: Vec<(&'static str, Attr)>,
}

impl Span {
    /// The span of `hop`, named after the unit that ran it, from `start_nanos`
    /// to now.
    pub fn hop(hop: &trace::Hop, kind: Kind, unit: &str, start_nanos: u128) -> Span {
        Span {
            name: unit.to_string(),
            kind,
            trace_id: hop.trace_id,
            span_id: hop.span_id,
            parent: hop.parent,
            sampled: hop.sampled,
            start_nanos,
            end_nanos: now_nanos(),
            error: None,
            attrs: {
                let mut attrs = Vec::with_capacity(10);
                attrs.push(("vejas.unit", Attr::Str(unit.to_string())));
                attrs
            },
        }
    }

    pub fn attr(mut self, key: &'static str, value: impl Into<Attr>) -> Span {
        self.attrs.push((key, value.into()));
        self
    }

    /// The messaging attributes of a hop on the bus: `operation` is `process`
    /// for a consumer, `send` for a producer.
    pub fn messaging(mut self, operation: &'static str, subject: &str) -> Span {
        self.attrs.push(("messaging.system", Attr::Static("nats")));
        self.attrs.push(("messaging.operation.type", Attr::Static(operation)));
        self.attr("messaging.destination.name", subject)
    }

    pub fn failed(mut self, error: impl Into<String>) -> Span {
        self.error = Some(error.into());
        self
    }
}
