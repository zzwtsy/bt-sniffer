//! 查询分派器对上层暴露的类型和异步调用入口。
//!
//! 调用者等待 oneshot 回复；查询资源由 dispatcher 持有，取消等待的后果按各接口契约处理。

use serde_bytes::ByteBuf;
use std::error::Error;
use std::fmt;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

use crate::dht::krpc::NodeId;
use crate::dht::peer_store::PeerStoreConfig;
use crate::dht::routing::AddressFamily;
use crate::dht::transaction::TransactionError;
use crate::dht::udp::UdpTransportError;

/// 默认最多缓存的上层命令数量。
///
/// 这是尚未被事件循环处理的“命令”上限，不是等待网络响应的 transaction 上限；
/// 后者由 transaction manager 单独控制。
pub(crate) const DEFAULT_COMMAND_CAPACITY: usize = 64;

/// routing maintenance 的并发、刷新和失败退避参数。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MaintenanceConfig {
    pub(crate) enabled: bool,
    pub(crate) refresh_after: Duration,
    pub(crate) lookup_parallelism: usize,
    pub(crate) shortlist_size: usize,
    pub(crate) max_queries: usize,
    pub(crate) retry_initial: Duration,
    pub(crate) retry_max: Duration,
    pub(crate) inter_lookup_delay: Duration,
    pub(crate) reserved_user_transactions: usize,
}

impl Default for MaintenanceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            refresh_after: crate::dht::routing::DEFAULT_BUCKET_REFRESH_AFTER,
            lookup_parallelism: 3,
            shortlist_size: 8,
            max_queries: 64,
            retry_initial: Duration::from_secs(60),
            retry_max: Duration::from_secs(15 * 60),
            inter_lookup_delay: Duration::from_secs(1),
            reserved_user_transactions: 1,
        }
    }
}

/// 查询目标的地址以及可选的已知 Node ID。
///
/// bootstrap 初期通常只知道地址，此时 `expected_id` 为 `None`；查询 routing table
/// 中的已知节点时应传入 ID，这样可以识别地址冒用，并在超时时更新节点状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct RemoteNode {
    /// 查询实际发送到的 UDP 地址。
    pub(crate) address: SocketAddr,
    /// 已知的远端 Node ID；bootstrap 时还不知道可以填 `None`。
    pub(crate) expected_id: Option<NodeId>,
}

/// 从 `find_node` 响应中发现的一条节点联系方式。
///
/// 这里只表示“远端声称存在这个节点”，不能直接加入 routing table；必须先向它发出
/// 查询并收到匹配的响应。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct DiscoveredNode {
    /// 远端在 compact node record 中给出的 Node ID。
    pub(crate) id: NodeId,
    /// 远端在 compact node record 中给出的 UDP 地址。
    pub(crate) address: SocketAddr,
}

/// 一次成功的 ping 查询结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PingResponse {
    /// 实际响应 ping 的节点 ID。
    pub(crate) responder_id: NodeId,
}

/// 一次成功的 find_node 查询结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FindNodeResponse {
    /// 实际响应 find_node 的节点 ID。
    pub(crate) responder_id: NodeId,
    /// 响应中携带的候选节点；这些节点尚未经过本节点验证。
    pub(crate) nodes: Vec<DiscoveredNode>,
}

/// dispatcher 内部命令队列的配置。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DhtDispatcherConfig {
    /// 等待事件循环处理的上层命令最多可以积压多少条。
    pub(crate) command_capacity: usize,
    /// 自动执行启动自查找和陈旧 bucket 刷新的参数。
    pub(crate) maintenance: MaintenanceConfig,
    /// 合法宣布的保存期限、容量和地址策略。
    pub(crate) peer_store: PeerStoreConfig,
}

impl Default for DhtDispatcherConfig {
    fn default() -> Self {
        Self {
            command_capacity: DEFAULT_COMMAND_CAPACITY,
            maintenance: MaintenanceConfig::default(),
            peer_store: PeerStoreConfig::default(),
        }
    }
}

/// 创建 dispatcher 时发现的配置问题。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DispatcherCreateError {
    /// Tokio 的有界 channel 不允许容量为 0。
    EmptyCommandQueue,
    /// 保存期限为零，或者各层缓存容量互相矛盾。
    InvalidPeerStoreConfig(&'static str),
    /// 启动时系统随机源不可用，不能创建安全的写入令牌。
    TokenEntropy,
    /// routing maintenance 参数之间互相矛盾或包含零值。
    InvalidMaintenanceConfig(&'static str),
    /// UDP socket 和 routing table 分别属于不同的 IP 地址族。
    AddressFamilyMismatch {
        /// UDP socket 实际绑定的地址。
        socket: SocketAddr,
        /// routing table 接受的地址族。
        routing: AddressFamily,
    },
    /// 操作系统没有返回 socket 的本地地址。
    LocalAddress(String),
}

impl fmt::Display for DispatcherCreateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyCommandQueue => write!(formatter, "dispatcher 命令队列容量不能为 0"),
            Self::InvalidPeerStoreConfig(reason) => {
                write!(formatter, "peer store 配置无效：{reason}")
            }
            Self::TokenEntropy => write!(formatter, "无法生成 token 密钥"),
            Self::InvalidMaintenanceConfig(reason) => {
                write!(formatter, "routing maintenance 配置无效：{reason}")
            }
            Self::AddressFamilyMismatch { socket, routing } => write!(
                formatter,
                "UDP socket 地址 {socket} 与 routing table 地址族 {routing:?} 不一致"
            ),
            Self::LocalAddress(error) => write!(formatter, "无法读取 UDP socket 本地地址：{error}"),
        }
    }
}

impl Error for DispatcherCreateError {}

/// 主动查询没有得到可用结果的原因。
#[derive(Debug)]
pub(crate) enum QueryError {
    /// 命令发送前 dispatcher 就已经退出。
    DispatcherClosed,
    /// 待发意图在本地限流队列超时，未发送数据报。
    LocalWait,
    /// 查询已经进入 dispatcher，但在等待期间节点开始关闭。
    ShuttingDown,
    /// 等待响应的 transaction 已达到配置上限。
    AtCapacity { limit: usize },
    /// transaction ID 分配等内部管理操作失败。
    Transaction(TransactionError),
    /// KRPC 编码或 UDP 发送失败。
    Transport(UdpTransportError),
    /// 在 transaction 截止时间前没有收到合法响应。
    Timeout,
    /// 远端返回了一条格式正确的 KRPC Error 消息。
    Remote { code: i64, message: ByteBuf },
    /// 消息能解码，但字段组合不符合当前查询的响应格式。
    InvalidResponse(&'static str),
    /// 响应来源地址正确，但响应中的 Node ID 不是预期节点。
    UnexpectedNodeId { expected: NodeId, actual: NodeId },
}

impl fmt::Display for QueryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LocalWait => write!(formatter, "DHT 本地待发队列超时"),
            Self::DispatcherClosed => write!(formatter, "DHT dispatcher 已经停止"),
            Self::ShuttingDown => write!(formatter, "DHT dispatcher 正在关闭"),
            Self::AtCapacity { limit } => {
                write!(formatter, "等待中的 DHT 查询已达到 {limit} 条上限")
            }
            Self::Transaction(error) => write!(formatter, "transaction 操作失败：{error}"),
            Self::Transport(error) => write!(formatter, "查询发送失败：{error}"),
            Self::Timeout => write!(formatter, "DHT 查询等待响应超时"),
            Self::Remote { code, message } => {
                write!(formatter, "远端返回 KRPC 错误 {code}：")?;
                write_remote_message(formatter, message)
            }
            Self::InvalidResponse(reason) => write!(formatter, "KRPC 响应不合法：{reason}"),
            Self::UnexpectedNodeId { expected, actual } => write!(
                formatter,
                "响应 Node ID 与预期不符：预期 {:02x?}，实际 {:02x?}",
                expected.0, actual.0
            ),
        }
    }
}

/// 远端说明只用于诊断：控制字符转义后最多 256 字符，超长追加省略号。
/// 不切断 UTF-8 或转义序列，不改变 Remote 中保存的原始字节及错误分类。
fn write_remote_message(formatter: &mut fmt::Formatter<'_>, message: &[u8]) -> fmt::Result {
    let mut characters = 0;
    for ch in String::from_utf8_lossy(message).chars() {
        let escaped = if ch.is_control() {
            ch.escape_default().to_string()
        } else {
            ch.to_string()
        };
        let count = escaped.chars().count();
        if characters + count > 256 {
            return formatter.write_str("…");
        }
        formatter.write_str(&escaped)?;
        characters += count;
    }
    Ok(())
}

impl Error for QueryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Transaction(error) => Some(error),
            Self::Transport(error) => Some(error),
            _ => None,
        }
    }
}

/// 实际发送证据由 dispatcher 更新，调用者超时仍能读取。
#[derive(Debug, Default)]
pub(crate) struct RpcProgress {
    pub(crate) sent: std::sync::atomic::AtomicU64,
    pub(crate) limited: std::sync::atomic::AtomicBool,
}

/// 事件循环因底层 socket 故障而停止。
#[derive(Debug)]
pub(crate) enum DispatcherError {
    /// UDP socket 已无法继续接收数据；单个畸形数据报不会产生这个错误。
    Transport(UdpTransportError),
}

impl fmt::Display for DispatcherError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(error) => write!(formatter, "DHT dispatcher 已停止：{error}"),
        }
    }
}

impl Error for DispatcherError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error),
        }
    }
}

/// 可以克隆并交给其他 task 的 DHT 查询入口。
///
/// handle 本身不直接接触 socket 和 routing table。它只把命令交给唯一的 dispatcher
/// task，再等待该命令自己的 oneshot 返回值。
/// RPC future 被丢弃时触发取消通知，实际清理由 dispatcher 后续处理，不是同步回收证明。
#[derive(Debug, Clone)]
pub(crate) struct DhtHandle {
    pub(super) commands: mpsc::Sender<Command>,
}

impl DhtHandle {
    /// 引导流量也必须预留用户名额；检查与注册都在 dispatcher 内完成。
    pub(crate) async fn bootstrap_ping(
        &self,
        remote: RemoteNode,
    ) -> Result<PingResponse, QueryError> {
        let cancel = tokio_util::sync::CancellationToken::new();
        let _guard = cancel.clone().drop_guard();
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command::BootstrapPing {
                remote,
                reply,
                cancel,
            })
            .await
            .map_err(|_| QueryError::DispatcherClosed)?;
        result.await.unwrap_or(Err(QueryError::DispatcherClosed))
    }

    /// 这是本机状态快照，不表示公网已经能主动访问本节点。
    pub(crate) async fn status(&self) -> Result<DhtStatus, QueryError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command::Status { reply })
            .await
            .map_err(|_| QueryError::DispatcherClosed)?;
        result.await.map_err(|_| QueryError::DispatcherClosed)
    }
    /// 向一个节点发送 ping，并等待匹配响应或超时。
    ///
    /// 返回成功只说明目标地址给出了合法响应；是否能够加入 routing table 仍由
    /// dispatcher 根据 Node ID、地址族和 bucket 状态决定。
    #[cfg(test)]
    pub(crate) async fn ping(&self, remote: RemoteNode) -> Result<PingResponse, QueryError> {
        let cancel = tokio_util::sync::CancellationToken::new();
        let _guard = cancel.clone().drop_guard();
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command::Ping {
                remote,
                reply,
                cancel,
            })
            .await
            .map_err(|_| QueryError::DispatcherClosed)?;
        result.await.unwrap_or(Err(QueryError::DispatcherClosed))
    }

    /// 向一个节点发送 find_node，并等待它返回当前地址族的节点列表。
    ///
    /// 返回的 [`DiscoveredNode`] 只是待验证候选，不会在这里自动加入 routing table。
    #[cfg(test)]
    pub(crate) async fn find_node(
        &self,
        remote: RemoteNode,
        target: NodeId,
    ) -> Result<FindNodeResponse, QueryError> {
        let cancel = tokio_util::sync::CancellationToken::new();
        let _guard = cancel.clone().drop_guard();
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command::FindNode {
                remote,
                target,
                cancel,
                reply,
            })
            .await
            .map_err(|_| QueryError::DispatcherClosed)?;
        result.await.unwrap_or(Err(QueryError::DispatcherClosed))
    }

    /// 请求事件循环正常关闭，并等待所有待处理查询收到关闭通知。
    ///
    /// 关闭完成后，其他 handle 再提交查询会得到 [`QueryError::DispatcherClosed`]。
    /// 此确认仅覆盖查询关闭；run_persistent 的存储结算、任务退出和数据库关闭须由会话继续等待。
    pub(crate) async fn shutdown(&self) -> Result<(), QueryError> {
        let (reply, finished) = oneshot::channel();
        self.commands
            .send(Command::Shutdown { reply })
            .await
            .map_err(|_| QueryError::DispatcherClosed)?;
        finished.await.map_err(|_| QueryError::DispatcherClosed)
    }
}

/// 单节点的即时内存快照；数量不是累计值，监听地址与邻居状态也不证明公网入站可达。
#[derive(Debug)]
pub(crate) struct DhtStatus {
    pub(crate) node_id: NodeId,
    pub(crate) family: AddressFamily,
    pub(crate) address: SocketAddr,
    pub(crate) good: usize,
    pub(crate) questionable: usize,
    pub(crate) recovery_queued: usize,
    pub(crate) recovery_active: usize,
    pub(crate) pending: usize,
    pub(crate) sampler: super::sampler::SamplerStatus,
}

#[derive(Debug)]
pub(super) enum Command {
    GetPeers {
        remote: RemoteNode,
        hash: crate::info_hash::InfoHashV1,
        progress: std::sync::Arc<RpcProgress>,
        cancel: tokio_util::sync::CancellationToken,
        reply: oneshot::Sender<Result<super::fetch::GetPeersResponse, QueryError>>,
    },
    FetchSeeds {
        hash: crate::info_hash::InfoHashV1,
        reply: oneshot::Sender<Vec<DiscoveredNode>>,
    },
    FetchIngress {
        ingress: Option<super::fetch::FetchIngress>,
        reply: oneshot::Sender<()>,
    },
    SamplingPause {
        paused: bool,
        reply: oneshot::Sender<()>,
    },
    Status {
        reply: oneshot::Sender<DhtStatus>,
    },
    BootstrapPing {
        cancel: tokio_util::sync::CancellationToken,
        remote: RemoteNode,
        reply: oneshot::Sender<Result<PingResponse, QueryError>>,
    },
    RoutingSnapshot {
        reply: oneshot::Sender<
            Result<Vec<crate::dht::persistence::SavedContact>, crate::storage::StorageError>,
        >,
    },
    StoragePause {
        error: crate::storage::StorageError,
        reply: oneshot::Sender<()>,
    },
    StartSampling {
        config: super::sampler::SamplerConfig,
        reply: oneshot::Sender<
            Result<mpsc::Receiver<super::sampler::SampleBatch>, super::sampler::SamplerError>,
        >,
    },
    StopSampling {
        reply: oneshot::Sender<()>,
    },
    #[cfg(test)]
    SamplingStatus {
        reply: oneshot::Sender<super::sampler::SamplerStatus>,
    },
    /// 上层请求主动探测一个节点是否可达。
    #[cfg(test)]
    Ping {
        cancel: tokio_util::sync::CancellationToken,
        remote: RemoteNode,
        reply: oneshot::Sender<Result<PingResponse, QueryError>>,
    },
    /// 上层请求查找与 `target` 接近的节点。
    #[cfg(test)]
    FindNode {
        cancel: tokio_util::sync::CancellationToken,
        remote: RemoteNode,
        target: NodeId,
        reply: oneshot::Sender<Result<FindNodeResponse, QueryError>>,
    },
    /// 停止接收新消息，并通知仍在等待的调用者。
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}
