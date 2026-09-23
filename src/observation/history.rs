//! 有界事件历史、分页窗口、淘汰和载荷截断。

use super::{Filter, Kind, TraceContext};
use serde::Serialize;
use serde_json::Value;
use std::{collections::VecDeque, time::Duration};
use tokio::time::Instant;

pub(super) const MAX_EVENT_BYTES: usize = 8192;
pub(super) const MAX_EVENTS: usize = 65_536;
pub(super) const MAX_BYTES: usize = 32 * 1024 * 1024;
pub(super) const RETENTION: Duration = Duration::from_secs(900);

#[derive(Debug)]
pub(super) struct Entry {
    pub(super) sequence: u64,
    pub(super) at: Instant,
    pub(super) context: TraceContext,
    pub(super) kind: Kind,
    pub(super) encoded: String,
    pub(super) retained_bytes: usize,
}

#[derive(Debug)]
pub(super) struct History {
    pub(super) events: VecDeque<Entry>,
    pub(super) bytes: usize,
    pub(super) sequence: u64,
    pub(super) evicted: u64,
    pub(super) truncated: u64,
    pub(super) active: std::collections::BTreeMap<String, Value>,
    pub(super) states: std::collections::BTreeMap<&'static str, Value>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct Limits {
    pub(super) count: usize,
    pub(super) bytes: usize,
    pub(super) age: Duration,
}

#[derive(Debug, Serialize)]
pub(crate) struct Window {
    pub(crate) run_id: String,
    pub(crate) oldest: String,
    pub(crate) latest: String,
    pub(crate) retained: usize,
    pub(crate) bytes: usize,
    pub(crate) evicted: u64,
    pub(crate) truncated: u64,
    max_events: usize,
    max_bytes: usize,
    retention_ms: u64,
}

#[derive(Debug, Serialize)]
pub(crate) struct Page {
    pub(crate) events: Vec<Value>,
    pub(crate) next: String,
    pub(crate) window: Window,
    pub(crate) completeness: &'static str,
}

impl History {
    pub(super) fn new() -> Self {
        Self {
            events: VecDeque::new(),
            bytes: 0,
            sequence: 0,
            evicted: 0,
            truncated: 0,
            active: std::collections::BTreeMap::new(),
            states: std::collections::BTreeMap::new(),
        }
    }

    pub(super) fn evict(&mut self, limits: Limits) {
        while self.events.front().is_some_and(|entry| {
            self.events.len() > limits.count
                || self.bytes > limits.bytes
                || entry.at.elapsed() >= limits.age
        }) {
            let entry = self.events.pop_front().expect("已有首项");
            self.bytes -= entry.retained_bytes;
            self.evicted += 1;
        }
    }

    pub(super) fn window(&self, run_id: &str, limits: Limits) -> Window {
        Window {
            run_id: run_id.to_owned(),
            oldest: self
                .events
                .front()
                .map_or(self.sequence + 1, |entry| entry.sequence)
                .to_string(),
            latest: self.sequence.to_string(),
            retained: self.events.len(),
            bytes: self.bytes,
            evicted: self.evicted,
            truncated: self.truncated,
            max_events: limits.count,
            max_bytes: limits.bytes,
            retention_ms: limits.age.as_millis() as u64,
        }
    }
}

pub(super) fn matches(entry: &Entry, filter: &Filter) -> bool {
    entry.context.matches(filter) && filter.kind.is_none_or(|kind| kind == entry.kind)
}

pub(super) fn bound(value: &mut Value, truncated: &mut bool, depth: usize) {
    if depth > 8 {
        *value = Value::Null;
        *truncated = true;
        return;
    }
    match value {
        Value::String(string) if string.len() > 1024 => {
            let mut end = 1024;
            while !string.is_char_boundary(end) {
                end -= 1;
            }
            string.truncate(end);
            *truncated = true;
        }
        Value::Array(array) => {
            if array.len() > 32 {
                array.truncate(32);
                *truncated = true;
            }
            for item in array {
                bound(item, truncated, depth + 1);
            }
        }
        Value::Object(object) => {
            for item in object.values_mut() {
                bound(item, truncated, depth + 1);
            }
        }
        _ => {}
    }
}
