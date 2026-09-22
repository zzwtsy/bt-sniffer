//! 采集存储入口；配置和计数随采集能力共享，数据库连接仍归 Storage 所有。
use tokio::sync::OwnedSemaphorePermit;

use crate::{
    collection::jobs::admission::{BackfillCounters, RecentAdmission},
    storage::{StorageError, StorageHandle},
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, AtomicUsize},
};
/// 一个采集能力共享一份接纳状态；克隆不创建连接，也不重置计数。
#[derive(Clone)]
pub(crate) struct CollectionStore {
    pub(crate) observer: crate::observation::Observer,
    #[cfg(test)]
    pub(super) test_barrier: Arc<
        Mutex<
            Option<(
                super::test_storage::BlockedOperation,
                super::test_storage::CommandBarrier,
            )>,
        >,
    >,
    pub(super) database: StorageHandle,
    /// 进程内采集接纳上限，0 表示尚未启用；不是数据库中当前任务数量。
    pub(super) fetch_limit: Arc<AtomicUsize>,
    pub(super) recent_admission: Arc<Mutex<RecentAdmission>>,
    pub(super) backfill_counters: Arc<Mutex<BackfillCounters>>,
    /// 已观察采样 hash 数，可能重复；不代表新增任务或成功下载数。
    pub(super) sample_observations: Arc<AtomicU64>,
}
impl CollectionStore {
    pub(crate) fn new(database: StorageHandle) -> Self {
        Self {
            observer: Default::default(),
            #[cfg(test)]
            test_barrier: Arc::default(),
            database,
            fetch_limit: Arc::new(AtomicUsize::new(0)),
            recent_admission: Arc::default(),
            backfill_counters: Arc::default(),
            sample_observations: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl CollectionStore {
    /// 业务 SQL 在唯一数据库线程上执行；等待者取消不撤销已接纳命令。
    pub(crate) async fn call<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut rusqlite::Connection) -> Result<T, StorageError> + Send + 'static,
    ) -> Result<T, StorageError> {
        self.database.call(operation).await
    }
    /// 在复制大载荷前申请预算，许可随后随命令转移给数据库线程。
    pub(super) async fn budget(&self, bytes: usize) -> Result<OwnedSemaphorePermit, StorageError> {
        self.database.budget(bytes).await
    }
    /// 持有载荷许可直至数据库命令执行完毕，返回值是完成确认。
    pub(super) async fn submit<T: Send + 'static>(
        &self,
        permit: OwnedSemaphorePermit,
        operation: impl FnOnce(&mut rusqlite::Connection) -> Result<T, StorageError> + Send + 'static,
    ) -> Result<T, StorageError> {
        self.database.submit(permit, operation).await
    }
}
