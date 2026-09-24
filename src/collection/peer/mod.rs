//! 获取已知 peer 的 v1 info 字典；不查找 peer，不下载内容，不写入 DHT PeerStore。
//!
//! worker 选择一个候选并持有同 IP 许可；PeerClient 共用 Peer ID 与指标，只返回经过校验的结果。
mod session;
pub(crate) mod wire;
use crate::address::AddressPolicy;
use crate::collection::peer::wire::BLOCK_SIZE;
use crate::collection::peer::wire::PeerId;
use crate::collection::peer::wire::WireError;
use crate::info_hash::SwarmKey;
use rand::TryRng;
use std::{fmt, net::SocketAddr, sync::Arc, time::Duration};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// 单次 fetch 及单个 peer 的资源限制；所有时长均为相对期限。
/// 默认 fetch 总期限为 120 秒，独立于 collector worker 覆盖整轮查找和下载的期限。
#[derive(Debug, Clone)]
pub(crate) struct MetadataConfig {
    /// 单次 fetch 的总时间，包含连接和传输；候选切换由 worker 负责。
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
        self.request_window > 0
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

/// 当前 peer 正在执行的物理阶段；超时范围使用独立 Deadline 表达。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Stage {
    Connect,
    StandardHandshake,
    ExtensionHandshake,
    Transfer,
    Verify,
}
/// 截断当前操作的期限范围；None 用于非超时结果，不是协议阶段。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Deadline {
    None,
    Stage,
    Peer,
    Task,
}
/// Limit 错误的固定资源限制原因，不依赖显示文本分类。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ResourceLimit {
    FrameLength,
    ReceiveOverflow,
    ReceiveBudget,
    MetadataSize,
}
impl fmt::Display for ResourceLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::FrameLength => "peer-wire 帧长度",
            Self::ReceiveOverflow => "接收字节计数溢出",
            Self::ReceiveBudget => "单 peer 累计接收预算",
            Self::MetadataSize => "metadata_size 必须为正且不超过上限",
        })
    }
}
/// 当前 peer 的失败原因；worker 可继续尝试下一个地址，不据此判定整个任务失败。
#[derive(Debug)]
pub(crate) enum PeerError {
    Io(std::io::Error),
    Timeout(Deadline),
    Protocol(WireError),
    Unsupported,
    Limit(ResourceLimit),
    Rejected(usize),
    Disconnected,
    HashMismatch,
    HandshakeHashMismatch,
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
impl From<crate::collection::peer::wire::WireErrorKind> for PeerError {
    fn from(kind: crate::collection::peer::wire::WireErrorKind) -> Self {
        Self::Protocol(kind.into())
    }
}
impl fmt::Display for PeerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "网络错误：{e}"),
            Self::Timeout(deadline) => write!(f, "{deadline:?} 期限超时"),
            Self::Protocol(e) => write!(f, "协议错误：{e}"),
            Self::Unsupported => f.write_str("远端未启用 metadata 扩展"),
            Self::Limit(reason) => write!(f, "超出资源上限：{reason}"),
            Self::Rejected(piece) => write!(f, "远端拒绝分片 {piece}"),
            Self::Disconnected => f.write_str("远端提前断开连接"),
            Self::HandshakeHashMismatch => f.write_str("标准握手 info-hash 不匹配"),
            Self::HashMismatch => f.write_str("原始 metadata 的 SHA-1 不匹配"),
        }
    }
}
impl std::error::Error for PeerError {}
/// 当前地址失败时的实际物理阶段和失败原因；失败切换由 worker 决定。
#[derive(Debug)]
pub(crate) struct PeerFailure {
    pub(crate) address: SocketAddr,
    pub(crate) stage: Stage,
    pub(crate) error: PeerError,
}
/// 仅在构造客户端时发生；已创建客户端的网络调用不会产生这些错误。
#[derive(Debug)]
pub(crate) enum PeerInitError {
    InvalidConfig,
    Entropy,
}
impl fmt::Display for PeerInitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidConfig => "metadata 配置无效",
            Self::Entropy => "无法生成 Peer ID",
        })
    }
}
impl std::error::Error for PeerInitError {}

/// 单次调用的地址筛选、取消、总超时或当前 peer 失败；不包含初始化故障。
#[derive(Debug)]
pub(crate) enum PeerFetchError {
    NoUsablePeers,
    Cancelled,
    TaskTimeout,
    PeerFailed(PeerFailure),
}
impl fmt::Display for PeerFetchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoUsablePeers => f.write_str("没有符合地址策略的 peer"),
            Self::Cancelled => f.write_str("metadata 获取已取消"),
            Self::TaskTimeout => f.write_str("metadata 任务总期限已到"),
            Self::PeerFailed(failure) => write!(f, "peer 获取失败：{}", failure.error),
        }
    }
}
impl std::error::Error for PeerFetchError {}

/// 字段私有，只有经过原始字节身份匹配和字典结构校验才能创建这个结果。
#[derive(Debug)]
pub(crate) struct VerifiedMetadata {
    /// 仅成功来源 peer 的会话标记；不写数据库，不继承此前失败 peer 的状态。
    used_extension_compatibility: bool,
    info_hash: SwarmKey,
    source: SocketAddr,
    peer_id: PeerId,
    info: Vec<u8>,
}
impl VerifiedMetadata {
    /// 固定输入调度测试的已校验样本，沿用生产字典边界和 SHA-1 规则。
    #[cfg(test)]
    pub(crate) fn fixture(info: Vec<u8>) -> Self {
        use sha1::{Digest, Sha1};
        let key = SwarmKey(Sha1::digest(&info).into());
        Self::fixture_key(info, key)
    }
    /// 固定离线夹具显式指定来源查找键，沿用生产摘要匹配与字典边界。
    #[cfg(test)]
    pub(crate) fn fixture_key(info: Vec<u8>, key: SwarmKey) -> Self {
        assert!(super::metainfo::match_identity(&info, key).is_some());
        assert_eq!(
            crate::collection::peer::wire::dictionary_prefix(&info, 64)
                .unwrap()
                .len(),
            info.len()
        );
        Self {
            used_extension_compatibility: false,
            info_hash: key,
            source: "127.0.0.1:6881".parse().unwrap(),
            peer_id: PeerId([7; 20]),
            info,
        }
    }
    pub(crate) fn used_extension_compatibility(&self) -> bool {
        self.used_extension_compatibility
    }
    pub(crate) fn info_hash(&self) -> SwarmKey {
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
/// 单次调用的观察来源与取消覆盖层；不改变连接配置，不保存在客户端克隆中。
#[derive(Clone, Default)]
pub(crate) struct PeerContext {
    pub(crate) observer: crate::observation::Observer,
    pub(crate) source: crate::collection::diagnostics::Source,
    pub(crate) attempt: Option<crate::collection::jobs::AttemptKind>,
    pub(crate) outer_timeout: Arc<std::sync::atomic::AtomicBool>,
}
/// 所有 worker 共享配置、指标和 TCP Peer ID；候选选择与并发归调度器所有。
#[derive(Debug, Clone)]
pub(crate) struct PeerClient {
    pub(crate) observer: crate::observation::Observer,
    config: Arc<MetadataConfig>,
    peer_id: PeerId,
    metrics: Arc<crate::collection::diagnostics::metrics::Metrics>,
}
impl PeerClient {
    pub(crate) fn with_observer(mut self, observer: crate::observation::Observer) -> Self {
        self.observer = observer;
        self
    }

    #[cfg(test)]
    pub(crate) fn test_metrics(&self) -> Arc<crate::collection::diagnostics::metrics::Metrics> {
        self.metrics.clone()
    }
    #[cfg(test)]
    pub(crate) fn with_metrics(
        mut self,
        metrics: Arc<crate::collection::diagnostics::metrics::Metrics>,
    ) -> Self {
        self.metrics = metrics;
        self
    }
    /// 测试可注入局部指标；生产由采集入口一次创建并共享。
    #[cfg(test)]
    pub(crate) fn new(config: MetadataConfig) -> Result<Self, PeerInitError> {
        Self::with_resources(config, Arc::default())
    }
    pub(crate) fn with_resources(
        config: MetadataConfig,
        metrics: Arc<crate::collection::diagnostics::metrics::Metrics>,
    ) -> Result<Self, PeerInitError> {
        Self::build(config, metrics, |bytes| {
            rand::rngs::SysRng
                .try_fill_bytes(bytes)
                .map_err(|_| PeerInitError::Entropy)
        })
    }
    #[cfg(test)]
    fn with_entropy(
        config: MetadataConfig,
        fill: impl FnOnce(&mut [u8; 20]) -> Result<(), PeerInitError>,
    ) -> Result<Self, PeerInitError> {
        Self::build(config, Arc::default(), fill)
    }
    fn build(
        config: MetadataConfig,
        metrics: Arc<crate::collection::diagnostics::metrics::Metrics>,
        fill: impl FnOnce(&mut [u8; 20]) -> Result<(), PeerInitError>,
    ) -> Result<Self, PeerInitError> {
        if !config.validate() {
            return Err(PeerInitError::InvalidConfig);
        }
        let mut peer_id = [0; 20];
        fill(&mut peer_id)?;
        Ok(Self {
            observer: Default::default(),
            config: Arc::new(config),
            peer_id: PeerId(peer_id),
            metrics,
        })
    }
    /// 获取单个已筛选候选；future 丢弃即关闭 socket。120 秒调用期限保留且不续期。
    /// 返回已校验下载或单 peer 失败；成功不表示事务已经提交。
    pub(crate) async fn fetch_one(
        &self,
        hash: SwarmKey,
        address: SocketAddr,
        cancellation: &CancellationToken,
        context: PeerContext,
    ) -> Result<VerifiedMetadata, PeerFetchError> {
        if cancellation.is_cancelled() {
            return Err(PeerFetchError::Cancelled);
        }
        if !self.config.address_policy.accepts(address) {
            return Err(PeerFetchError::NoUsablePeers);
        }
        let task_timeout = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let work = async {
            let mut diagnostic = crate::collection::diagnostics::PeerObservation::new(
                self.metrics.clone(),
                context.source,
                address.is_ipv6(),
                task_timeout.clone(),
                context.outer_timeout.clone(),
            );
            diagnostic.attach_observer(context.observer.clone());
            diagnostic.observe_connect(address, context.attempt);
            self.metrics.add(
                crate::collection::diagnostics::metrics::Counter::PeerAttempts,
                1,
            );
            let result = tokio::time::timeout(
                self.config.peer_timeout,
                session::fetch_peer(&self.config, self.peer_id, hash, address, &mut diagnostic),
            )
            .await;
            let result = match result {
                Ok(result) => result,
                Err(_) => Err(PeerError::Timeout(Deadline::Peer)),
            };
            if let Err(error) = &result {
                diagnostic.failure(error);
            }
            diagnostic.finish(
                result
                    .as_ref()
                    .map_or_else(crate::collection::diagnostics::error_kind, |_| {
                        crate::collection::diagnostics::ResultKind::Success
                    }),
                match &result {
                    Err(PeerError::Timeout(deadline)) => *deadline,
                    _ => Deadline::None,
                },
            );
            if result.is_err() {
                self.metrics.add(
                    crate::collection::diagnostics::metrics::Counter::PeerFailures,
                    1,
                );
            }
            result.map_err(|error| {
                let stage = diagnostic.stage();
                PeerFetchError::PeerFailed(PeerFailure {
                    address,
                    stage,
                    error,
                })
            })
        };
        tokio::pin!(work);
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(PeerFetchError::Cancelled),
            result = tokio::time::timeout(self.config.task_timeout, &mut work) => {
                result.unwrap_or_else(|_| {
                    task_timeout.store(true, std::sync::atomic::Ordering::Relaxed);
                    Err(PeerFetchError::TaskTimeout)
                })
            },
        }
    }
}

#[cfg(test)]
pub(crate) mod tests;

#[cfg(test)]
pub(crate) mod compatibility_tests;

impl Stage {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Connect => "connect",
            Self::StandardHandshake => "standard_handshake",
            Self::ExtensionHandshake => "extension_handshake",
            Self::Transfer => "transfer",
            Self::Verify => "verify",
        }
    }
}
impl PeerError {
    pub(crate) fn label(&self) -> &'static str {
        match self {
            Self::Io(_) => "io",
            Self::Timeout(_) => "timeout",
            Self::Protocol(_) => "protocol",
            Self::Unsupported => "unsupported",
            Self::Limit(_) => "limit",
            Self::Rejected(_) => "rejected",
            Self::Disconnected => "disconnected",
            Self::HashMismatch => "hash_mismatch",
            Self::HandshakeHashMismatch => "handshake_hash_mismatch",
        }
    }
}
