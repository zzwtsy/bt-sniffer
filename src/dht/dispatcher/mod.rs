//! KRPC 查询分派与 DHT 事件循环。
//!
//! 这个模块把 UDP transport、transaction manager 和 routing table 串在一起。所有
//! 可变状态都由一个 Tokio task 独占，上层通过 [`DhtHandle`] 提交查询，因此不需要
//! 给 routing table 套上 `Arc<Mutex<_>>`。
//!
//! 一条主动查询大致经过以下步骤：
//!
//! 1. 上层通过 [`DhtHandle`] 把命令送进队列；
//! 2. dispatcher 注册 transaction，然后发送 UDP 数据报；
//! 3. 收到响应后，同时核对 transaction ID 和来源地址；
//! 4. 合法响应用于更新 routing table，并通过 oneshot channel 返回给调用者。
//!
//! 内部按职责拆分为：查询接口、事件循环、入站查询处理，以及出站响应的验证、结算
//! 和路由后续动作。程序内其他模块统一从 `dht::dispatcher` 导入所需类型。

mod api;
mod fetch;
mod inspection;
pub(crate) use api::DiscoveredNode;
pub(crate) use fetch::{AnnounceEvent, FetchIngress, GetPeersResponse};
mod maintenance;
mod peer_queries;
mod query;
mod recovery;
pub(crate) use recovery::DispatcherExit;
mod response;
mod runtime;
mod sampler;
mod sampling;
mod traffic;
pub(crate) use sampler::SamplerError;
pub(crate) use sampler::{SampleBatch, SamplerConfig};

pub(crate) use api::{DhtDispatcherConfig, DhtHandle, DispatcherError, QueryError, RemoteNode};
#[cfg(test)]
pub(crate) use api::{DispatcherCreateError, MaintenanceConfig, PingResponse};
pub(crate) use runtime::DhtDispatcher;

// 测试需要构造最小响应；这个名字只在 dispatcher 模块内部可见。
#[cfg(test)]
use query::empty_response;

#[cfg(test)]
mod tests;

pub(crate) use api::RpcProgress;
