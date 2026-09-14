//! 主动采样的内部配置和结果；这些类型不是对外发布的 library API。
//!
//! 上层通过 DhtHandle 启停采样并消费有界批次；配置中的容量同时约束等待和在途工作。
use super::super::{
    api::{Command, DhtHandle, DiscoveredNode},
    sampler::PauseReason,
};
use crate::dht::krpc::NodeId;
use crate::dht::peer_store::PeerAddressPolicy;
use crate::info_hash::InfoHashV1;
use std::{
    fmt,
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, oneshot};

/// 单节点采样状态机的容量和期限；构造时不分配资源，start_sampling 时统一校验。
#[derive(Debug, Clone, Copy)]
pub(crate) struct SamplerConfig {
    /// 采样和回退查询共用的在途名额。
    pub(crate) parallelism: usize,
    /// 每次发包后重新计时，不积攒突发额度。
    pub(crate) send_spacing: Duration,
    /// 每轮 RPC 上限，包含回退查询。
    pub(crate) max_queries: usize,
    /// 从当前 target 附近选择的路由种子数。
    pub(crate) shortlist_size: usize,
    /// 待验证联系人上限，与 routing table 容量无关。
    pub(crate) candidate_capacity: usize,
    /// 已排队批次和为在途请求预留的槽位共用这个容量。
    pub(crate) output_capacity: usize,
    /// 即使远端 interval=0，也不会比这个间隔更频繁地采样。
    pub(crate) minimum_interval: Duration,
    /// 普通失败从这个时长开始指数退避。
    pub(crate) retry_initial: Duration,
    /// 失败退避上限，不覆盖远端要求的更长冷却。
    pub(crate) retry_max: Duration,
    /// Node ID 和 IP 的冷却表分别使用这个上限。
    pub(crate) cooldown_capacity: usize,
    /// 公网为默认值，私有部署和测试必须显式切换。
    pub(crate) address_policy: PeerAddressPolicy,
}
impl Default for SamplerConfig {
    fn default() -> Self {
        Self {
            parallelism: 3,
            send_spacing: Duration::from_secs(1),
            max_queries: 64,
            shortlist_size: 8,
            candidate_capacity: 1024,
            output_capacity: 64,
            minimum_interval: Duration::from_secs(60),
            retry_initial: Duration::from_secs(60),
            retry_max: Duration::from_secs(900),
            cooldown_capacity: 10000,
            address_policy: PeerAddressPolicy::PublicOnly,
        }
    }
}
impl SamplerConfig {
    pub(super) fn validate(self, max_pending: usize, now: Instant) -> Result<(), SamplerError> {
        if self.parallelism == 0
            || self.parallelism >= max_pending
            || self.send_spacing.is_zero()
            || self.minimum_interval.is_zero()
            || self.retry_initial.is_zero()
            || self.retry_max < self.retry_initial
            || self.shortlist_size == 0
            || self.max_queries < self.shortlist_size
            || self.candidate_capacity < self.shortlist_size
            || self.candidate_capacity < self.parallelism
            || self.output_capacity == 0
            || self.output_capacity > tokio::sync::Semaphore::MAX_PERMITS
            || self.cooldown_capacity == 0
            || [self.send_spacing, self.minimum_interval, self.retry_max]
                .iter()
                .any(|duration| now.checked_add(*duration).is_none())
        {
            return Err(SamplerError::InvalidConfig);
        }
        Ok(())
    }
}

/// 一个合法响应对应一个批次，包括空样本。跨批去重交给消费者，不写入 PeerStore。
#[derive(Debug)]
pub(crate) struct SampleBatch {
    /// 网络响应到达时的 UTC 时间，不用数据库消费者取出批次的时间代替。
    pub(crate) observed_at: std::time::SystemTime,
    pub(crate) responder: DiscoveredNode,
    pub(crate) target: NodeId,
    /// 本次运行的单调接收时刻，用于调度期限；落库观察时间使用 observed_at。
    pub(crate) received_at: Instant,
    pub(crate) interval: Duration,
    /// 远端宣称的已知 hash 总数，不是本批 samples 长度，也不是本地已保存数量。
    pub(crate) num: u64,
    pub(crate) samples: Vec<InfoHashV1>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SamplerError {
    StorageFault,
    AlreadyRunning,
    InvalidConfig,
    DispatcherClosed,
}
impl fmt::Display for SamplerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::StorageFault => "存储故障，需要恢复持久化会话后再采样",
            Self::AlreadyRunning => "主动采样器已经运行",
            Self::InvalidConfig => "采样配置无效或无法保留已有冷却记录",
            Self::DispatcherClosed => "dispatcher 已经关闭",
        })
    }
}
impl std::error::Error for SamplerError {}

/// 采样器状态快照；in_flight/candidates 是当前量，成功/失败/不支持是观察计数。
/// pause 表示当前等待原因，是否真正存储失败要同时检查 storage_error。
#[derive(Debug, Clone, Default)]
pub(crate) struct SamplerStatus {
    /// collector 暂停独立于采样器内部等待原因。
    pub(crate) collector_paused: bool,
    pub(crate) storage_error: Option<crate::storage::StorageError>,
    pub(crate) running: bool,
    pub(crate) in_flight: usize,
    pub(crate) candidates: usize,
    pub(crate) successful: u64,
    pub(crate) failed: u64,
    pub(crate) unsupported: u64,
    pub(crate) pause: PauseReason,
}
impl DhtHandle {
    /// 等待 dispatcher 接纳启动请求并返回有界批次接收端；消费者负责持久化与跨批去重。
    /// 入队后放弃等待不会自动撤回启动命令；丢弃接收端由输出关闭检测触发停止。
    pub(crate) async fn start_sampling(
        &self,
        config: SamplerConfig,
    ) -> Result<mpsc::Receiver<SampleBatch>, SamplerError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command::StartSampling { config, reply })
            .await
            .map_err(|_| SamplerError::DispatcherClosed)?;
        result.await.unwrap_or(Err(SamplerError::DispatcherClosed))
    }
    /// 等待 dispatcher 停止调度；不确认批次已消费、预约已结算或数据库已关闭。
    pub(crate) async fn stop_sampling(&self) -> Result<(), SamplerError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command::StopSampling { reply })
            .await
            .map_err(|_| SamplerError::DispatcherClosed)?;
        result.await.map_err(|_| SamplerError::DispatcherClosed)
    }
    #[cfg(test)]
    pub(crate) async fn sampling_status(&self) -> Result<SamplerStatus, SamplerError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command::SamplingStatus { reply })
            .await
            .map_err(|_| SamplerError::DispatcherClosed)?;
        result.await.map_err(|_| SamplerError::DispatcherClosed)
    }
}
