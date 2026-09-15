//! metadata 任务的类型与入口；领取、状态变更、提示及快照按事务职责组织。
//! Job 的 generation 标识领取版本；只有当前领取可以提交，SQLite 是状态事实来源。
pub(crate) mod admission;
mod claim;
mod hints;
mod policy;
pub(super) use policy::ClaimPolicy;
mod queries;
pub(super) use queries::CollectionStatusSnapshot;
mod transitions;
#[cfg(test)]
use crate::collection::peer::VerifiedMetadata;
use crate::collection::store::CollectionStore;
use crate::info_hash::InfoHashV1;
use crate::storage::StorageError;
#[cfg(test)]
use claim::{CLAIM_SQL, ClaimRow, schedule_sql};
#[cfg(test)]
use hints::install_peer_policy;

#[cfg(test)]
use rusqlite::{Connection, OptionalExtension, params};
use std::{net::SocketAddr, sync::atomic::Ordering};
/// 当前已到期且可领取的任务统计，不包含正在运行或休眠的任务。
#[derive(Debug)]
pub(crate) struct DueStats {
    /// 已到期的 pending 与 retry_wait 任务总数。
    pub(crate) count: i64,
    /// 最早到期任务已等待的毫秒数；没有到期任务时为 0。
    pub(crate) oldest_wait_ms: i64,
    /// 到期任务中，至少有一个未过期且符合地址策略的 peer 提示的任务数。
    pub(crate) fresh: i64,
}

/// 数据库当前状态快照；任务数按状态统计，metadata_bytes 是已存原始字节总量。
#[derive(Debug, Default, serde::Serialize)]
pub(crate) struct Stats {
    pub(crate) pending: i64,
    pub(crate) running: i64,
    pub(crate) retry_wait: i64,
    pub(crate) dormant: i64,
    pub(crate) succeeded: i64,
    pub(crate) metadata_count: i64,
    pub(crate) metadata_bytes: i64,
}

const PEER_HINT_TTL_MS: i64 = 30 * 60 * 1_000;
const DORMANT_REACTIVATION_DELAY_MS: i64 = 24 * 60 * 60 * 1_000;
const LOCAL_RETRY_DELAY_MS: i64 = 60_000;
const RETRY_BASE_DELAY_MS: f64 = 60_000.0;
const MAX_FAILED_ATTEMPTS: u32 = 6;
const MAX_PEER_HINTS: i64 = 8;
const BACKFILL_PAGE_SIZE: i64 = 256;
const PEER_HINT_CLEANUP_BATCH_SIZE: i64 = 1024;

/// 本轮的延期或失败处理方式；数据库保存对应的原因标签和任务状态字符串。
#[derive(Debug, Clone, Copy)]
pub(crate) enum RetryReason {
    /// 取消、无路由等条件只延期，不增加失败次数。
    Deferred,
    /// 记录具体的本地延期原因，同样不增加失败次数。
    Local(LocalReason),
    /// 本轮远端失败，保存 AttemptFailure 的标签并增加一次失败计数。
    Failed(crate::collection::failure::AttemptFailure),
}
impl RetryReason {
    pub(crate) fn failure_category(self) -> Option<&'static str> {
        match self {
            Self::Deferred | Self::Local(_) => None,
            Self::Failed(category) => Some(category.label()),
        }
    }
}

/// 本地条件导致的延期分类，均不消耗远端失败重试次数。
#[derive(Debug, Clone, Copy)]
pub(crate) enum LocalReason {
    /// 资源或配额暂不可用，尚不能据此认定远端失败。
    ResourceWait,
    /// 缺少可用 DHT 路由，等待后续发现。
    NoRoute,
    /// 本轮被取消，领取应交回重试流程。
    Cancelled,
}
impl RetryReason {
    pub(crate) fn category(self) -> Option<&'static str> {
        match self {
            Self::Deferred => Some("local_deferred"),
            Self::Local(LocalReason::ResourceWait) => Some("local_wait"),
            Self::Local(LocalReason::NoRoute) => Some("no_route"),
            Self::Local(LocalReason::Cancelled) => Some("cancelled"),
            Self::Failed(category) => Some(category.label()),
        }
    }
}

/// 重试或完成操作正常返回时，当前领取的更新是否生效。
/// 校验、预算、命令通道或数据库失败通过外层 Result 的 Err 返回，不属于 Stale。
/// Err(Closed) 也可能表示未收到命令结果，不能据此断定已入队事务未提交。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UpdateResult {
    /// 本次更新已提交；重试表示安排延期或休眠，完成表示保存结果并标记成功。
    Applied,
    /// 没有匹配 hash、generation 且仍为 running 的任务；本次未更新任务或 metadata。
    Stale,
}
/// 首次发现窗口；重试和重复观察不会重置首次发现时间。
pub(crate) const RECENT_MS: i64 = 30 * 60 * 1000;
/// 调度版本 2：首次／重复领取预留机会为 3:1，提示不提升重复任务的类别。
pub(crate) const SCHEDULING_POLICY_VERSION: u64 = 2;
/// 四类互斥领取类别；数字同时作为 SQL CASE 的内部返回值。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(i64)]
pub(crate) enum ClaimClass {
    Hint = 0,
    Recent = 1,
    Retry = 2,
    History = 3,
}
impl ClaimClass {
    fn from_sql(value: i64) -> Result<Self, StorageError> {
        match value {
            0 => Ok(Self::Hint),
            1 => Ok(Self::Recent),
            2 => Ok(Self::Retry),
            3 => Ok(Self::History),
            _ => Err(StorageError::Invalid("任务类别无效")),
        }
    }
}
/// 本次领取的不可变分类与时间；供执行和指标使用，不随提示过期重新分类。
pub(crate) struct Claim {
    pub(crate) job: Job,
    pub(crate) due_at: i64,
    pub(crate) first_seen: i64,
}

/// 领取历史分类；再次领取包含恢复、本地延期和休眠再激活，不等于远端重试轮次。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum AttemptKind {
    First,
    Repeat,
}

/// 一次任务领取，以及本轮可优先尝试的 peer 地址。
#[derive(Debug, Clone)]
pub(crate) struct Job {
    /// 领取事务确定的类别；提示变化不改变本次执行及提交的统计归属。
    pub(crate) class: ClaimClass,
    /// 领取事务确认存在合法且未过期的提示；与调度类别独立。
    pub(crate) had_valid_hint: bool,
    pub(crate) hash: InfoHashV1,
    /// 领取版本号；重新领取或恢复后递增，拒绝旧 worker 的迟到结果。
    pub(crate) generation: i64,
    /// 领取事务读取的远端失败次数，取值 0..=6；本地延期和恢复不增加它。
    pub(crate) failed_attempts_before: u32,
    /// 尚未过期的宣布地址；可能为空，需要通过 DHT 查找 peer。
    pub(crate) peers: Vec<SocketAddr>,
}
impl Job {
    /// 领取事务返回的 generation 已加一，值 1 对应此前未领取的 generation=0。
    pub(crate) fn attempt_kind(&self) -> AttemptKind {
        if self.generation == 1 {
            AttemptKind::First
        } else {
            AttemptKind::Repeat
        }
    }
}
impl CollectionStore {
    pub(crate) fn sample_observations(&self) -> u64 {
        self.sample_observations.load(Ordering::Relaxed)
    }
}
fn decode_hash(bytes: Vec<u8>) -> Result<InfoHashV1, StorageError> {
    Ok(InfoHashV1(
        bytes
            .try_into()
            .map_err(|_| StorageError::Invalid("hash 长度无效"))?,
    ))
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod scheduling_tests;

#[cfg(test)]
mod optimization_tests;

#[cfg(test)]
mod query_tests;

#[cfg(test)]
mod first_attempt_comparison;

impl ClaimClass {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Hint => "hint",
            Self::Recent => "recent",
            Self::Retry => "retry",
            Self::History => "history",
        }
    }
}
