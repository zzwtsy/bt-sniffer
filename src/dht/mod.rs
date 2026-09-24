//! BitTorrent DHT 的核心状态与算法。
//!
//! KRPC 模块负责网络消息的形状，这里负责 routing table、节点查找等有状态逻辑。
//!
//! dispatcher 串行驱动路由、transaction 和采样；上层仅持有控制句柄，避免共享可变协议状态。

pub(crate) mod krpc;
pub(crate) mod persistence;
pub(crate) mod udp;
pub(crate) use krpc::NodeId;
pub(crate) mod dispatcher;
pub(crate) mod peer_store;
pub(crate) mod routing;
pub(crate) mod shortlist;
mod token;
pub(crate) mod transaction;

pub(crate) mod traffic;

pub(crate) mod security;
