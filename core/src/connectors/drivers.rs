//! The bundled drivers, one module per protocol, and the registry that names
//! them.

mod exec;
mod http_in;
mod http_out;
mod http_poll;
mod mqtt;
mod oauth_poll;
mod slack_out;
mod timer;

use crate::connectors::Driver;
use exec::{ExecRpc, ExecSink, ExecSource, ExecStreamSource};
use http_in::HttpIn;
use http_out::HttpOut;
use http_poll::HttpPoll;
use mqtt::{MqttIn, MqttOut};
use oauth_poll::OAuthPoll;
use slack_out::SlackOut;
use timer::Timer;

/// The compiled-in driver catalog. New drivers are added here (and, later,
/// generated onto this trait via `vejas_new_connector`).
pub fn driver_for(name: &str) -> Option<Box<dyn Driver>> {
    match name {
        "http-in" => Some(Box::new(HttpIn)),
        "timer" => Some(Box::new(Timer)),
        "http-poll" => Some(Box::new(HttpPoll)),
        "oauth-poll" => Some(Box::new(OAuthPoll)),
        "slack-out" => Some(Box::new(SlackOut)),
        "http-out" => Some(Box::new(HttpOut)),
        "exec-source" => Some(Box::new(ExecSource)),
        "exec-sink" => Some(Box::new(ExecSink)),
        "exec-stream-source" => Some(Box::new(ExecStreamSource)),
        "exec-rpc" => Some(Box::new(ExecRpc)),
        "mqtt-in" => Some(Box::new(MqttIn)),
        "mqtt-out" => Some(Box::new(MqttOut)),
        _ => None,
    }
}

pub fn catalog() -> Vec<(&'static str, &'static str, &'static str)> {
    [
        "http-in", "timer", "http-poll", "oauth-poll", "slack-out", "http-out",
        "exec-source", "exec-sink", "exec-stream-source", "exec-rpc",
        "mqtt-in", "mqtt-out",
    ]
    .iter()
    .filter_map(|n| driver_for(n).map(|d| (*n, d.kind(), d.about())))
    .collect()
}
