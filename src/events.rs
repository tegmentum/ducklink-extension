//! Process-wide runtime event log — the sink behind the `ducklink.events`
//! system view.
//!
//! An in-process, bounded audit trail of the host's runtime activity: catalog
//! fetches and fallbacks, cache hits/misses, blob downloads and sha256
//! verification, provider selection, and the load lifecycle (start / ok /
//! error). It is DELIBERATELY decoupled from the `DucklinkRuntime` handle — a
//! plain `static` ring buffer — so ANY code path (catalog resolution deep in
//! `catalog.rs`, the common-tier `ducklink_load` bind, the advanced-tier
//! `LOAD WASM` bridge) can [`emit`] without threading a handle through.
//!
//! Every operation is panic-safe: a poisoned lock or a clock error is swallowed
//! so instrumentation can NEVER break a load. Emitting is a best-effort side
//! effect, not part of the load contract.

use std::collections::VecDeque;
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

/// Maximum number of events retained. When full, the oldest event is evicted as
/// a new one is pushed (a bounded ring buffer), so the log is O(1) memory and a
/// long-lived process can't grow it without bound. Sequence numbers keep
/// climbing across evictions, so an evicted-then-queried gap is visible as a
/// jump in `seq` rather than silently reused.
const MAX_EVENTS: usize = 1024;

/// One recorded runtime event.
#[derive(Debug, Clone)]
pub struct Event {
    /// Monotonic sequence number, assigned in emit order across the process
    /// lifetime (never reused, survives eviction). Drives the view's ordering.
    pub seq: u64,
    /// Wall-clock timestamp as MICROSECONDS since the Unix epoch — DuckDB's
    /// TIMESTAMP physical storage. 0 if the system clock was unreadable.
    pub ts_micros: i64,
    /// The event kind, e.g. `catalog_fetch`, `cache_miss`, `download`,
    /// `verify_ok`, `select_provider`, `load_start`, `load_ok`, `load_error`.
    pub kind: String,
    /// The module/extension name this event concerns, when applicable.
    pub module: Option<String>,
    /// Free-form detail (a URL, a digest, a summary, an error message).
    pub detail: String,
}

/// The bounded ring buffer plus the monotonic sequence counter.
struct EventLog {
    next_seq: u64,
    events: VecDeque<Event>,
}

impl EventLog {
    fn new() -> Self {
        EventLog {
            next_seq: 0,
            events: VecDeque::with_capacity(MAX_EVENTS),
        }
    }
}

/// The single process-wide event sink. Lazily created on first use.
static EVENT_LOG: OnceLock<Mutex<EventLog>> = OnceLock::new();

fn log() -> &'static Mutex<EventLog> {
    EVENT_LOG.get_or_init(|| Mutex::new(EventLog::new()))
}

/// Current wall-clock time as micros since the Unix epoch; 0 if the clock is
/// unreadable (never panics).
fn now_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

/// Record a runtime event. Best-effort and panic-safe: a poisoned lock is
/// recovered (`into_inner`) and a clock error becomes a 0 timestamp, so a
/// failure to log NEVER propagates into the load path. Assigns the next
/// monotonic `seq`, stamps the current time, pushes, and evicts the oldest
/// event if the buffer is at capacity.
pub fn emit(kind: &str, module: Option<&str>, detail: impl Into<String>) {
    let mut guard = log().lock().unwrap_or_else(|e| e.into_inner());
    let seq = guard.next_seq;
    guard.next_seq = guard.next_seq.wrapping_add(1);
    if guard.events.len() >= MAX_EVENTS {
        guard.events.pop_front();
    }
    guard.events.push_back(Event {
        seq,
        ts_micros: now_micros(),
        kind: kind.to_string(),
        module: module.map(|m| m.to_string()),
        detail: detail.into(),
    });
}

/// A snapshot of the current event log, oldest first (ascending `seq`). Cheap
/// clone of the retained events; the lock is held only for the copy.
pub fn snapshot() -> Vec<Event> {
    let guard = log().lock().unwrap_or_else(|e| e.into_inner());
    guard.events.iter().cloned().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // The global sink is process-wide; these tests exercise an isolated
    // `EventLog` directly so they don't race the global one or each other.
    fn push(log: &mut EventLog, kind: &str) {
        let seq = log.next_seq;
        log.next_seq = log.next_seq.wrapping_add(1);
        if log.events.len() >= MAX_EVENTS {
            log.events.pop_front();
        }
        log.events.push_back(Event {
            seq,
            ts_micros: 0,
            kind: kind.to_string(),
            module: None,
            detail: String::new(),
        });
    }

    #[test]
    fn emits_in_monotonic_seq_order() {
        let mut log = EventLog::new();
        for _ in 0..5 {
            push(&mut log, "k");
        }
        let seqs: Vec<u64> = log.events.iter().map(|e| e.seq).collect();
        assert_eq!(seqs, vec![0, 1, 2, 3, 4], "seq must be monotonic in push order");
    }

    #[test]
    fn caps_at_max_and_evicts_oldest() {
        let mut log = EventLog::new();
        for _ in 0..(MAX_EVENTS + 10) {
            push(&mut log, "k");
        }
        assert_eq!(log.events.len(), MAX_EVENTS, "buffer must be bounded at MAX_EVENTS");
        // Oldest 11 (seq 0..=10) evicted; the window is the last MAX_EVENTS seqs.
        assert_eq!(log.events.front().unwrap().seq, 10);
        assert_eq!(log.events.back().unwrap().seq, (MAX_EVENTS + 10 - 1) as u64);
    }

    #[test]
    fn global_emit_and_snapshot_roundtrip() {
        // Smoke-test the real global path is panic-safe and returns rows.
        emit("unit_test", Some("modx"), "detail-x");
        let snap = snapshot();
        let found = snap
            .iter()
            .rev()
            .find(|e| e.kind == "unit_test" && e.module.as_deref() == Some("modx"));
        assert!(found.is_some(), "emitted event should appear in the snapshot");
        assert_eq!(found.unwrap().detail, "detail-x");
    }
}
