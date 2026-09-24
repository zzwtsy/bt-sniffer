//! Session 的唯一状态、受监督任务类型和 Drop 兜底。

use super::{FaultLog, FaultReporter, SessionFault};
use crate::{
    collection::{ingest::SampleIngest, store::CollectionStore},
    dht::{
        dispatcher::{DhtHandle, DispatcherExit},
        persistence::{DhtStore, identity::LocalIdentity},
    },
    storage::{Storage, StorageError},
};
use std::{collections::HashMap, sync::Arc};
use tokio::{
    sync::watch,
    task::{Id, JoinSet},
};
use tokio_util::sync::CancellationToken;

pub(super) struct Node {
    pub(super) identity: LocalIdentity,
    pub(super) handle: DhtHandle,
    pub(super) exit: Option<DispatcherExit>,
    pub(super) collection: Option<SampleIngest>,
    pub(super) collecting: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TaskRole {
    Dispatcher(usize),
    Snapshot(usize),
    SampleCollector(usize),
    FetchCoordinator,
    CatalogMaintenance,
}

impl TaskRole {
    pub(super) fn node(self) -> usize {
        match self {
            Self::Dispatcher(index) | Self::Snapshot(index) | Self::SampleCollector(index) => index,
            Self::FetchCoordinator | Self::CatalogMaintenance => {
                unreachable!("全局采集协调器没有节点索引")
            }
        }
    }
}

impl std::fmt::Display for TaskRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Dispatcher(node) => write!(f, "节点 {node} dispatcher"),
            Self::Snapshot(node) => write!(f, "节点 {node} snapshot"),
            Self::SampleCollector(node) => write!(f, "节点 {node} sample collector"),
            Self::CatalogMaintenance => f.write_str("历史元数据回填"),
            Self::FetchCoordinator => f.write_str("全局 metadata 采集协调器"),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum TaskPhase {
    Running,
    ShuttingDown,
}

pub(super) enum TaskOutput {
    Dispatcher(DispatcherExit),
    Snapshot(Result<(), StorageError>),
    Collector(SampleIngest),
    Fetch(Result<(), Vec<crate::collection::CollectorError>>),
}

/// 会话是运行资源的唯一关闭入口；Drop 只中止任务，完成落盘必须显式 await shutdown。
pub(crate) struct Session {
    pub(super) observer: crate::observation::Observer,
    pub(super) monitor: Option<crate::monitor::Monitor>,
    pub(super) budget: Arc<crate::dht::traffic::Budget>,
    pub(super) storage: Option<Storage>,
    pub(super) collection_store: CollectionStore,
    pub(super) dht_store: DhtStore,
    pub(super) nodes: Vec<Node>,
    pub(super) stop_snapshots: CancellationToken,
    pub(super) stop_fetch: CancellationToken,
    pub(super) tasks: JoinSet<TaskOutput>,
    pub(super) roles: HashMap<Id, TaskRole>,
    pub(super) report: FaultReporter,
    pub(super) errors: watch::Receiver<Option<SessionFault>>,
    pub(super) faults: FaultLog,
    pub(super) shutdown_stage: String,
}

impl Drop for Session {
    fn drop(&mut self) {
        self.stop_snapshots.cancel();
        self.stop_fetch.cancel();
        self.tasks.abort_all();
    }
}

impl Session {
    #[cfg(test)]
    pub(crate) fn test_close_observer(&mut self) -> tokio::sync::oneshot::Receiver<()> {
        self.storage.as_mut().unwrap().take_close_observer()
    }
    #[cfg(test)]
    pub(crate) fn test_budget(&self) -> Arc<crate::dht::traffic::Budget> {
        self.budget.clone()
    }
    pub(crate) fn log_traffic(&self) {
        self.budget.log();
    }
    #[cfg(test)]
    pub(crate) fn test_store(&self) -> CollectionStore {
        self.collection_store.clone()
    }
}
