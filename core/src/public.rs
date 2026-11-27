//! The public mode (ADR-0041): a runtime that faces the public, `VEJAS_PUBLIC=1`.
//!
//! It serves the reads the console needs and answers 404 to everything else,
//! whatever the credential: every write, `/mcp`, the `api` flows, the secrets,
//! the previews and shown runs, the samples, the proposals and the metrics.
//! The list is here and nowhere else, so that a route added to the runtime is
//! refused in this mode until it is listed. A proxy in front repeats it: the
//! mode is the barrier that does not depend on the proxy's configuration.

use std::path::{Component, Path};
use std::sync::OnceLock;

/// The reads the console makes, each for one part of it. GET and HEAD only.
pub const READS: &[&str] = &[
    // the panel itself, and the probe a proxy or an orchestrator polls
    "/",
    "/panel",
    "/healthz",
    // the map: units, their state, and how they connect
    "/topology",
    "/graph",
    // the business literals each flow carries, shown, never changed
    "/surface",
    // what went through the bus, as the panel shows it (masked, ADR-0017)
    "/events",
    // the dead letters, without replay or purge
    "/dlq",
    // the trace record: a search, and one trace by its id (below)
    "/traces",
    // a flow's, service's or connector's code and what the page derives from it
    "/file",
    "/shape",
    "/rules",
    // the names of the drivers and the versions a promote evicted
    "/drivers",
    "/evictions",
];

/// The reads served under a prefix: one trace by its id.
pub const READ_PREFIXES: &[&str] = &["/traces/"];

/// The folders whose code a public page may read, and the files there.
const CODE_DIRS: &[&str] = &["flows", "services", "connectors"];

/// What a read may ask for at most.
pub const SEARCH_MAX_CHARS: usize = 200;
pub const SEARCH_LIMIT_MAX: usize = 20;
pub const TRACE_ID_MAX_CHARS: usize = 64;
pub const DEAD_LETTERS_SHOWN: usize = 50;
pub const REQUESTS_IN_FLIGHT_MAX: usize = 32;

/// Is the public mode on? Read once: a mode that changed while the runtime
/// serves would be no barrier.
pub fn on() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| switched_on(std::env::var("VEJAS_PUBLIC").ok().as_deref()))
}

fn switched_on(value: Option<&str>) -> bool {
    match value.map(|v| v.trim().to_ascii_lowercase()) {
        None => false,
        Some(v) => match v.as_str() {
            "" | "0" | "off" | "false" | "no" => false,
            "1" | "on" | "true" | "yes" => true,
            _ => {
                // A value that is neither: the safe reading for a runtime
                // someone meant to put in front of the public.
                eprintln!("[vejas] VEJAS_PUBLIC={v:?} is neither on nor off; on");
                true
            }
        },
    }
}

/// Does the public mode serve this request? `read` is a GET or a HEAD.
pub fn allows(read: bool, path: &str) -> bool {
    read && (READS.contains(&path) || READ_PREFIXES.iter().any(|p| path.len() > p.len() && path.starts_with(p)))
}

/// A file a public page may read: a `.vjs` in the folders of flows, services
/// and connectors, as the panel names it (relative, or absolute under the
/// root). The runtime's own guard (`guard_path`) runs as well.
pub fn file_allowed(root: &Path, rel: &str) -> bool {
    let rel = rel.trim_start_matches("./");
    let rel = Path::new(rel).strip_prefix(root).unwrap_or(Path::new(rel));
    let mut parts = rel.components();
    let in_dir = matches!(parts.next(), Some(Component::Normal(d)) if CODE_DIRS.iter().any(|ok| d == *ok));
    in_dir
        && parts.next().is_some()
        && rel.components().all(|c| matches!(c, Component::Normal(_)))
        && rel.extension().is_some_and(|e| e == "vjs")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_listed_reads_are_served() {
        for path in READS {
            assert!(allows(true, path), "{path}");
            assert!(!allows(false, path), "a write to {path} is refused");
        }
        assert!(allows(true, "/traces/4bf92f3577b34da6a3ce929d0e0e4736"));
        assert!(!allows(true, "/traces/"));
        for path in ["/mcp", "/metrics", "/secrets", "/preview", "/fixture", "/proposals", "/api/x", "/api/openapi.json", "/surface/canary", "/panel/", "/x"] {
            assert!(!allows(true, path), "{path}");
        }
        for path in ["/mcp", "/reload", "/dlq/replay", "/file/set", "/hold/release"] {
            assert!(!allows(false, path), "{path}");
        }
    }

    #[test]
    fn a_public_page_reads_code_and_nothing_else() {
        let root = Path::new("/app");
        for ok in ["flows/a.vjs", "./services/s.vjs", "connectors/c.vjs", "/app/flows/a.vjs", "flows/pkg/a.vjs"] {
            assert!(file_allowed(root, ok), "{ok}");
        }
        for no in ["flows/a.json", "flows/a.fixture.json", "packages/p/flows/a.vjs", "detects/d.vpl", "secrets.json", "flows", "flows/../secrets.vjs", "/etc/x.vjs", "/app/../etc/x.vjs", "services.json"] {
            assert!(!file_allowed(root, no), "{no}");
        }
    }

    #[test]
    fn the_mode_is_on_only_when_asked_and_when_unclear() {
        assert!(!switched_on(None));
        assert!(!switched_on(Some("0")));
        assert!(!switched_on(Some("off")));
        assert!(switched_on(Some("1")));
        assert!(switched_on(Some("TRUE")));
        assert!(switched_on(Some("maybe")));
    }
}
