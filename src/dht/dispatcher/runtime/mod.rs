//! Dispatcher 的唯一状态所有者、事件循环、命令分派和出站查询接线。
//!
//! 所有实现仍在同一个 Tokio task 中推进；子模块只按变化原因拆分代码，不复制状态。

mod commands;
mod driver;
mod outbound;
mod state;

pub(super) use driver::current_time;
pub(crate) use state::DhtDispatcher;
pub(super) use state::{PendingDispatch, PendingPurpose};

#[cfg(test)]
mod tests;
