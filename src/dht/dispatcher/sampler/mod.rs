//! 有界主动采样：结果槽位先预留，节点冷却跨启停保存，所有网络操作仍由 dispatcher 执行。
//!
//! `Sampler` 独占采样状态；协议解析、候选调度、结果处理、持久预约和 dispatcher
//! 接线按变化原因分开，但不会创建额外任务或状态副本。

mod api;
mod driver;
mod durable;
mod outcome;
mod protocol;
mod schedule;
mod state;

pub(crate) use api::{SampleBatch, SamplerConfig, SamplerError, SamplerStatus};
pub(super) use driver::watch_output;
pub(super) use protocol::{SampleResponse, decode_sample};
pub(crate) use state::PauseReason;
pub(super) use state::{Request, RequestKind, Sampler};

#[cfg(test)]
mod tests;
