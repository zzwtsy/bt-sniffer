//! worker 的领取所有权与错误来源；异常任务也必须回收领取记录。
//!
//! 关闭时主循环停止领取、回收 worker，再处理仍由领取记录标识的任务，不能只发送取消通知。
use super::*;
use crate::dht::dispatcher::QueryError;
use std::{collections::HashMap, error::Error, fmt, future::Future};
use tokio::task::{Id, JoinError, JoinSet};

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
    pub(crate) fn fault(&self) -> SessionFault {
        match self {
            Self::Storage(error) => SessionFault::StorageWrite(error.clone()),
            _ => SessionFault::CollectorFailed {
                detail: self.to_string(),
            },
        }
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
#[derive(Default)]
pub(super) struct Workers {
    tasks: JoinSet<Outcome>,
    claims: HashMap<Id, Job>,
    pub(super) interrupted: Vec<Job>,
}
impl Workers {
    pub(super) fn len(&self) -> usize {
        self.tasks.len()
    }
    pub(super) fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }
    pub(super) fn spawn(
        &mut self,
        job: Job,
        work: impl Future<Output = Outcome> + Send + 'static,
    ) -> tokio::task::AbortHandle {
        let handle = self.tasks.spawn(work);
        self.claims.insert(handle.id(), job);
        handle
    }
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
