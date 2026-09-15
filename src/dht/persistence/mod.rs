//! DHT 身份、路由快照和采样冷却；共用应用唯一的 SQLite 执行线程。
mod contacts;
mod cooldown;
pub(crate) mod identity;
use crate::storage::{StorageError, StorageHandle};
pub(crate) use contacts::SavedContact;
pub(crate) use cooldown::{CooldownLease, RestoredCooldown};
#[derive(Debug, Clone)]
pub(crate) struct DhtStore {
    database: StorageHandle,
    pub(crate) observer: crate::observation::Observer,
}
impl DhtStore {
    pub(crate) fn new(database: StorageHandle) -> Self {
        Self {
            database,
            observer: Default::default(),
        }
    }
}

impl DhtStore {
    pub(crate) async fn call<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut rusqlite::Connection) -> Result<T, StorageError> + Send + 'static,
    ) -> Result<T, StorageError> {
        self.database.call(operation).await
    }
    async fn budget(
        &self,
        bytes: usize,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, StorageError> {
        self.database.budget(bytes).await
    }
    async fn submit<T: Send + 'static>(
        &self,
        permit: tokio::sync::OwnedSemaphorePermit,
        operation: impl FnOnce(&mut rusqlite::Connection) -> Result<T, StorageError> + Send + 'static,
    ) -> Result<T, StorageError> {
        self.database.submit(permit, operation).await
    }
}

#[cfg(test)]
pub(crate) mod test_storage;

#[cfg(test)]
mod tests;
