//! 当前进程的只读观测：有界事件历史与独立当前状态，不参与业务决策。
//! 生产者同步追加小事件，不执行 I/O；序号通知可合并，消费者按游标读取历史。

mod context;
mod history;
mod observer;
mod span;

#[cfg(test)]
mod tests;

pub(crate) use context::{Filter, Kind, TraceContext, hex, wall_ms};
pub(crate) use history::{Page, Window};
pub(crate) use observer::Observer;
pub(crate) use span::Span;

#[cfg(test)]
use history::{Limits, MAX_BYTES, MAX_EVENT_BYTES, MAX_EVENTS, RETENTION};
#[cfg(test)]
use serde_json::json;
