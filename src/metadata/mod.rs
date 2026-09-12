//! 获取已知 peer 的 v1 info 字典；不查找 peer，不下载内容，不写入 DHT PeerStore。
//!
//! collector 提供候选 peer；fetcher 顺序尝试地址，所有克隆共享连接名额，只返回经过校验的结果。
mod session;
use crate::{
    krpc::InfoHashV1,
    net::address::AddressPolicy,
    peer_wire::{BLOCK_SIZE, PeerId, WireError},
};
use rand::TryRng;
use std::{collections::HashSet, fmt, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{sync::Semaphore, time::Instant};
use tokio_util::sync::CancellationToken;

/// 单次 fetch 及单个 peer 的资源限制；所有时长均为相对期限。
/// 默认 fetch 总期限为 120 秒，独立于 collector worker 覆盖整轮查找和下载的期限。
#[derive(Debug, Clone)]
pub(crate) struct MetadataConfig {
    /// Fetcher 的所有克隆共用这些名额，不在内部积压任务。
    pub(crate) concurrency: usize,
    /// 顺序尝试不同地址的上限；同一任务不并行连接多个 peer。
    pub(crate) max_peer_attempts: usize,
    /// 包含连接、切换 peer 和传输的总时间。
    pub(crate) task_timeout: Duration,
    /// 单个 peer 即使一直发送无关数据，也不能超过这个期限。
    pub(crate) peer_timeout: Duration,
    /// 从开始 TCP 建连起算，且仍受单 peer 和任务总期限约束。
    pub(crate) connect_timeout: Duration,
    /// TCP 连接成功后开始，标准握手与扩展握手共用此期限。
    pub(crate) handshake_timeout: Duration,
    /// 从每条分片请求发送完成起算，不因其他消息到达而续期。
    pub(crate) piece_timeout: Duration,
    /// 同一 peer 已发送但尚未完成的分片请求数上限。
    pub(crate) request_window: usize,
    /// 在采用远端 metadata_size 分配内存前检查。
    pub(crate) max_metadata_size: usize,
    /// 长度前缀超过此值时，Codec 在读取正文前拒绝该帧。
    pub(crate) max_frame_size: usize,
    /// 扩展握手或 metadata 消息头的字节上限，不含分片原始载荷。
    pub(crate) max_header_size: usize,
    /// Bencode 嵌套层数上限。
    pub(crate) max_depth: usize,
    /// 计入标准握手、长度前缀及所有被忽略的帧。
    pub(crate) max_received_bytes: usize,
    /// 单 peer 长度前缀帧数上限，包含被忽略的帧和 keepalive。
    pub(crate) max_received_frames: usize,
    /// 连接前筛选地址；通过筛选不表示地址可达或 peer 可信。
    pub(crate) address_policy: AddressPolicy,
}
impl Default for MetadataConfig {
    fn default() -> Self {
        Self {
            concurrency: 8,
            max_peer_attempts: 8,
            task_timeout: Duration::from_secs(120),
            peer_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(5),
            handshake_timeout: Duration::from_secs(5),
            piece_timeout: Duration::from_secs(10),
            request_window: 4,
            max_metadata_size: 4 * 1024 * 1024,
            max_frame_size: 64 * 1024,
            max_header_size: 4096,
            max_depth: 64,
            max_received_bytes: 16 * 1024 * 1024,
            max_received_frames: 4096,
            address_policy: AddressPolicy::PublicOnly,
        }
    }
}
impl MetadataConfig {
    fn validate(&self) -> bool {
        self.concurrency > 0
            && self.concurrency <= Semaphore::MAX_PERMITS
            && self.max_peer_attempts > 0
            && self.request_window > 0
            && self.max_metadata_size > 0
            && self.max_metadata_size <= isize::MAX as usize
            && self.max_header_size > 0
            && self
                .max_header_size
                .checked_add(BLOCK_SIZE + 2)
                .is_some_and(|size| size <= self.max_frame_size)
            && self.max_frame_size <= u32::MAX as usize
            && self.max_depth > 0
            && self.max_depth <= 64
            && self.max_received_bytes >= self.max_metadata_size
            && self.max_received_frames > 0
            && [
                self.task_timeout,
                self.peer_timeout,
                self.connect_timeout,
                self.handshake_timeout,
                self.piece_timeout,
            ]
            .iter()
            .all(|d| !d.is_zero() && Instant::now().checked_add(*d).is_some())
    }
}

/// 错误发生阶段；Peer 表示单 peer 总期限，不是额外的协议步骤。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stage {
    Connect,
    Handshake,
    Piece,
    Verify,
    Peer,
}
/// 当前 peer 的失败原因；fetch 可继续尝试下一个地址，不据此判定整个任务失败。
#[derive(Debug)]
pub(crate) enum PeerError {
    Io(std::io::Error),
    Timeout(Stage),
    Protocol(WireError),
    Unsupported,
    Limit(&'static str),
    Rejected(usize),
    Disconnected,
    HashMismatch,
}
impl From<std::io::Error> for PeerError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<WireError> for PeerError {
    fn from(e: WireError) -> Self {
        Self::Protocol(e)
    }
}
impl fmt::Display for PeerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "网络错误：{e}"),
            Self::Timeout(stage) => write!(f, "{stage:?} 阶段超时"),
            Self::Protocol(e) => write!(f, "协议错误：{e}"),
            Self::Unsupported => f.write_str("远端未启用 metadata 扩展"),
            Self::Limit(reason) => write!(f, "超出资源上限：{reason}"),
            Self::Rejected(piece) => write!(f, "远端拒绝分片 {piece}"),
            Self::Disconnected => f.write_str("远端提前断开连接"),
            Self::HashMismatch => f.write_str("原始 metadata 的 SHA-1 不匹配"),
        }
    }
}
impl std::error::Error for PeerError {}
/// 一次已尝试地址的阶段和原因，供全部地址失败时保留诊断信息。
#[derive(Debug)]
pub(crate) struct PeerFailure {
    pub(crate) address: SocketAddr,
    pub(crate) stage: Stage,
    pub(crate) error: PeerError,
}
/// fetch 级结果：本地接纳失败、取消和总超时，与逐 peer 失败列表分开。
#[derive(Debug)]
pub(crate) enum MetadataError {
    InvalidConfig,
    AtCapacity,
    NoUsablePeers,
    Entropy,
    Cancelled,
    TaskTimeout,
    AllPeersFailed(Vec<PeerFailure>),
}
impl fmt::Display for MetadataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig => f.write_str("metadata 配置无效"),
            Self::AtCapacity => f.write_str("metadata 并发已满"),
            Self::NoUsablePeers => f.write_str("没有符合地址策略的 peer"),
            Self::Entropy => f.write_str("无法生成 Peer ID"),
            Self::Cancelled => f.write_str("metadata 获取已取消"),
            Self::TaskTimeout => f.write_str("metadata 任务总期限已到"),
            Self::AllPeersFailed(errors) => write!(f, "全部 {} 个 peer 获取失败", errors.len()),
        }
    }
}
impl std::error::Error for MetadataError {}

/// 字段私有，只有经过原始字节 SHA-1 和字典结构校验才能创建这个结果。
#[derive(Debug)]
pub(crate) struct VerifiedMetadata {
    info_hash: InfoHashV1,
    source: SocketAddr,
    peer_id: PeerId,
    info: Vec<u8>,
}
impl VerifiedMetadata {
    pub(crate) fn info_hash(&self) -> InfoHashV1 {
        self.info_hash
    }
    pub(crate) fn source(&self) -> SocketAddr {
        self.source
    }
    pub(crate) fn peer_id(&self) -> PeerId {
        self.peer_id
    }
    pub(crate) fn info(&self) -> &[u8] {
        &self.info
    }
}
/// 克隆共享配置和并发许可，并沿用同一 TCP Peer ID；不持有后台下载任务。
#[derive(Debug, Clone)]
pub(crate) struct MetadataFetcher {
    config: Arc<MetadataConfig>,
    permits: Arc<Semaphore>,
    peer_id: PeerId,
    metrics: Arc<crate::metrics::Metrics>,
}
impl MetadataFetcher {
    pub(crate) fn with_metrics(mut self, metrics: Arc<crate::metrics::Metrics>) -> Self {
        self.metrics = metrics;
        self
    }
    /// 先校验限制并生成 Peer ID；无效配置或熵源失败时不建立网络连接。
    pub(crate) fn new(config: MetadataConfig) -> Result<Self, MetadataError> {
        Self::with_entropy(config, |bytes| {
            rand::rngs::SysRng
                .try_fill_bytes(bytes)
                .map_err(|_| MetadataError::Entropy)
        })
    }
    fn with_entropy(
        config: MetadataConfig,
        fill: impl FnOnce(&mut [u8; 20]) -> Result<(), MetadataError>,
    ) -> Result<Self, MetadataError> {
        if !config.validate() {
            return Err(MetadataError::InvalidConfig);
        }
        let mut peer_id = [0; 20];
        fill(&mut peer_id)?;
        Ok(Self {
            permits: Arc::new(Semaphore::new(config.concurrency)),
            config: Arc::new(config),
            peer_id: PeerId(peer_id),
            metrics: Arc::default(),
        })
    }
    /// 没有内部任务队列。丢弃这个 future 会一并关闭当前 socket 并释放并发名额。
    /// 按输入顺序过滤、去重并尝试地址；名额已满立即返回 AtCapacity，不排队。
    /// 成功只表示取得已校验的原始 metadata，不表示入库；取消和总超时不返回部分字节。
    pub(crate) async fn fetch(
        &self,
        hash: InfoHashV1,
        peers: &[SocketAddr],
        cancellation: &CancellationToken,
    ) -> Result<VerifiedMetadata, MetadataError> {
        if cancellation.is_cancelled() {
            return Err(MetadataError::Cancelled);
        }
        let _permit = self
            .permits
            .try_acquire()
            .map_err(|_| MetadataError::AtCapacity)?;
        let work = async {
            let mut seen = HashSet::new();
            let mut failures = Vec::new();
            for address in peers.iter().copied() {
                if !self.config.address_policy.accepts(address) || !seen.insert(address) {
                    continue;
                }
                let mut stage = Stage::Connect;
                let mut report = crate::metrics::PhaseReport::new(self.metrics.clone());
                self.metrics.add(crate::metrics::Counter::Connections, 1);
                let result = tokio::time::timeout(
                    self.config.peer_timeout,
                    session::fetch_peer(
                        &self.config,
                        self.peer_id,
                        hash,
                        address,
                        &mut stage,
                        &mut report,
                    ),
                )
                .await;
                let result = match result {
                    Ok(result) => result,
                    Err(_) => {
                        stage = Stage::Peer;
                        Err(PeerError::Timeout(Stage::Peer))
                    }
                };
                report.finish(result.is_ok());
                if result.is_err() {
                    self.metrics.add(crate::metrics::Counter::PeerFailures, 1);
                }
                match result {
                    Ok(value) => return Ok(value),
                    Err(error) => failures.push(PeerFailure {
                        address,
                        stage,
                        error,
                    }),
                }
                if failures.len() >= self.config.max_peer_attempts {
                    break;
                }
            }
            if failures.is_empty() {
                Err(MetadataError::NoUsablePeers)
            } else {
                Err(MetadataError::AllPeersFailed(failures))
            }
        };
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(MetadataError::Cancelled),
            result = tokio::time::timeout(self.config.task_timeout, work) => result.unwrap_or(Err(MetadataError::TaskTimeout)),
        }
    }
}

#[cfg(test)]
mod tests;
