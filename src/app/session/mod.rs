//! 应用运行会话；负责节点装配、任务监督和有序关闭，不绑定公网 socket。
//!
//! `Session` 是任务、节点、Monitor 和 Storage 的唯一关闭入口；各子模块只按生命周期
//! 阶段拆分实现，取消通知、任务结果消费和共同关闭期限保持原有语义。

mod faults;
mod shutdown;
mod startup;
mod state;
mod supervision;

use faults::classify_collector_error;
pub(crate) use faults::{FaultLog, FaultReporter, SessionFault};
use state::{Node, TaskOutput, TaskPhase};
pub(crate) use state::{Session, TaskRole};

#[cfg(test)]
mod tests;
