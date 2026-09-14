//! 临时数据库夹具：业务句柄与数据库线程分别持有，关闭仍等待原线程确认。
use super::DhtStore;
use crate::storage::{Storage, StorageConfig, StorageError};
pub(crate) struct TestStorage {
    pub(crate) handle: DhtStore,
    storage: Storage,
}
impl TestStorage {
    pub(crate) async fn open(config: StorageConfig) -> Result<Self, StorageError> {
        let storage = Storage::open(config).await?;
        let handle = DhtStore::new(storage.handle.clone());
        Ok(Self { handle, storage })
    }
    pub(crate) async fn shutdown(self) -> Result<(), StorageError> {
        self.storage.shutdown().await
    }
}
