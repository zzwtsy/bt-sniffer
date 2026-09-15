//! 临时数据库夹具和命令屏障；阻塞必须有限，并在测试结束前确认原线程关闭。
use super::store::CollectionStore;
use crate::storage::{Storage, StorageConfig, StorageError};
pub(crate) struct TestStorage {
    pub(crate) handle: CollectionStore,
    storage: Storage,
}
impl TestStorage {
    pub(crate) async fn open(config: StorageConfig) -> Result<Self, StorageError> {
        let storage = Storage::open(config).await?;
        let handle = CollectionStore::new(storage.handle.clone());
        Ok(Self { handle, storage })
    }
    pub(crate) async fn shutdown(self) -> Result<(), StorageError> {
        self.storage.shutdown().await
    }
}

/// 只在指定业务命令到达数据库线程时停住，不用计时猜测协调器的位置。
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum BlockedOperation {
    Inspection,
    Completion,
    Status,
}
pub(super) struct CommandBarrier {
    pub(super) entered: tokio::sync::oneshot::Sender<()>,
    pub(super) release: std::sync::mpsc::Receiver<()>,
}
impl CommandBarrier {
    pub(super) fn wait(self) -> Result<(), StorageError> {
        let _ = self.entered.send(());
        self.release
            .recv_timeout(std::time::Duration::from_secs(5))
            .map_err(|_| StorageError::Closed)
    }
}
impl CollectionStore {
    pub(super) fn take_test_barrier(&self, operation: BlockedOperation) -> Option<CommandBarrier> {
        let mut barrier = self.test_barrier.lock().unwrap();
        if barrier
            .as_ref()
            .is_some_and(|(expected, _)| *expected == operation)
        {
            barrier.take().map(|(_, barrier)| barrier)
        } else {
            None
        }
    }
}
