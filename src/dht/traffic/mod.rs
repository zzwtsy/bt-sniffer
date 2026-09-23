//! 会话共享 DHT 配额。所有检查在一个短同步临界区完成，不跨 await。
//! governor 负责 GCRA；可探测状态先检查组合配额，再原子提交，受限 IP 不耗尽其他配额。

mod api;
mod limiter;
mod report;
mod state;

#[cfg(test)]
mod tests;

pub(crate) use api::{Class, Config, Decision, Stats, VerificationStats};
pub(crate) use state::{Budget, QueueRecord, VerificationPermit};

#[cfg(test)]
use std::{net::IpAddr, sync::Arc, time::Duration};
