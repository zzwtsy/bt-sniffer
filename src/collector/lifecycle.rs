//! worker 的领取所有权与错误来源；异常任务也必须回收领取记录。
//!
//! 关闭时主循环停止领取、回收 worker，再处理仍由领取记录标识的任务，不能只发送取消通知。
use super::worker::Outcome;
use crate::{
    dht::dispatcher::QueryError,
    krpc::InfoHashV1,
    metadata::MetadataError,
    storage::{StorageError, jobs::Job},
};
use std::{collections::HashMap, error::Error, fmt, future::Future};
use tokio::task::{Id, JoinError, JoinSet};

/// 模块故障来源；只有 Storage 可进入存储暂停路径，其他分支由会话判为致命故障。
/// Clock 虽包装 StorageError，也不能因此误归为可恢复写入错误。
#[derive(Debug)]
pub(crate) enum CollectorError {
    Storage(StorageError),
    Clock(StorageError),
    Control {
        operation: &'static str,
        source: QueryError,
    },
    Configuration(MetadataError),
    InspectionTask(JoinError),
    SupervisorClosed,
    /// JoinSet 返回异常时附回原领取标识，便于继续延期或恢复，不能丢失 generation。
    Worker {
        hash: InfoHashV1,
        generation: i64,
        source: JoinError,
    },
}
impl From<StorageError> for CollectorError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}
impl CollectorError {
    pub(super) fn is_storage(&self) -> bool {
        matches!(self, Self::Storage(_))
    }
}
impl fmt::Display for CollectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(error) => write!(f, "采集存储操作失败：{error}"),
            Self::Clock(error) => write!(f, "采集时钟无效：{error}"),
            Self::Control { operation, source } => write!(f, "{operation}失败：{source}"),
            Self::Configuration(error) => write!(f, "metadata 获取器初始化失败：{error}"),
            Self::InspectionTask(error) => write!(f, "状态容量检查任务失败：{error}"),
            Self::SupervisorClosed => f.write_str("会话监督通道意外关闭"),
            Self::Worker {
                hash,
                generation,
                source,
            } => write!(
                f,
                "metadata worker {}，hash={hash:?} generation={generation}：{source}",
                if source.is_cancelled() {
                    "意外取消"
                } else {
                    "panic"
                }
            ),
        }
    }
}
impl Error for CollectorError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Storage(e) | Self::Clock(e) => Some(e),
            Self::Control { source, .. } => Some(source),
            Self::Configuration(e) => Some(e),
            Self::InspectionTask(e) | Self::Worker { source: e, .. } => Some(e),
            Self::SupervisorClosed => None,
        }
    }
}
pub(super) fn already_closed(error: &QueryError) -> bool {
    matches!(
        error,
        QueryError::DispatcherClosed | QueryError::ShuttingDown
    )
}
/// 协调器拥有的 worker 集合；每个 task ID 必须恰好对应一个 Job 领取副本。
/// JoinSet 析构只中止任务；正常收尾须逐个 next 回收，并由 collector 处理数据库结果。
#[derive(Default)]
pub(super) struct Workers {
    tasks: JoinSet<Outcome>,
    claims: HashMap<Id, Job>,
    /// 已回收异常 worker、但尚待退出流程处理的领取记录；不是重新启动的任务队列。
    pub(super) interrupted: Vec<Job>,
}
impl Workers {
    pub(super) fn len(&self) -> usize {
        self.tasks.len()
    }
    pub(super) fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }
    /// 把网络 future 移入 JoinSet 并登记领取副本；返回的 AbortHandle 仅发送中止请求。
    pub(super) fn spawn(
        &mut self,
        job: Job,
        work: impl Future<Output = Outcome> + Send + 'static,
    ) -> tokio::task::AbortHandle {
        let handle = self.tasks.spawn(work);
        self.claims.insert(handle.id(), job);
        handle
    }
    /// 等待并移出一个任务及其 Job；panic/中止同样返回领取记录，空集合才返回 None。
    /// 取消本次等待不消费任务；返回后由调用者保存结果或把 Job 放入 interrupted。
    pub(super) async fn next(&mut self) -> Option<(Job, Result<Outcome, CollectorError>)> {
        let result = self.tasks.join_next_with_id().await?;
        let id = match &result {
            Ok((id, _)) => *id,
            Err(error) => error.id(),
        };
        let job = self.claims.remove(&id).expect("每个 worker 都有领取记录");
        let outcome = match result {
            Ok((_, Outcome::Control(source))) => Err(CollectorError::Control {
                operation: "查找 peer",
                source,
            }),
            Ok((_, outcome)) => Ok(outcome),
            Err(source) => Err(CollectorError::Worker {
                hash: job.hash,
                generation: job.generation,
                source,
            }),
        };
        Some((job, outcome))
    }
}
