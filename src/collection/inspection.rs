//! Web 只读查询；命令许可归数据库闭包，超时不进入采集写故障路径。
use super::store::CollectionStore;
use rusqlite::Connection;
use serde_json::{Value, json};
use std::time::{Duration, Instant};
use tokio::sync::OwnedSemaphorePermit;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub(crate) enum ReadError {
    Cancelled,
    Unavailable,
    Busy,
}
struct Progress<'a>(&'a Connection);
impl<'a> Progress<'a> {
    fn install(connection: &'a Connection, cancel: CancellationToken) -> rusqlite::Result<Self> {
        let until = Instant::now() + Duration::from_millis(100);
        connection.progress_handler(
            1000,
            Some(move || cancel.is_cancelled() || Instant::now() >= until),
        )?;
        Ok(Self(connection))
    }
}
impl Drop for Progress<'_> {
    fn drop(&mut self) {
        let _ = self.0.progress_handler(0, None::<fn() -> bool>);
    }
}
impl CollectionStore {
    pub(crate) async fn inspect(
        &self,
        permit: OwnedSemaphorePermit,
        cancel: CancellationToken,
    ) -> Result<Value, ReadError> {
        #[cfg(test)]
        let barrier = self.take_test_barrier(super::test_storage::BlockedOperation::Inspection);
        self.call(move |c| {
            #[cfg(test)]
            if let Some(barrier) = barrier {
                barrier.wait()?;
            }
            let _permit = permit;
            if cancel.is_cancelled() {
                return Ok(Err(ReadError::Cancelled));
            }
            let _progress = Progress::install(c, cancel)?;
            Ok(read(c))
        })
        .await
        .map_err(|_| ReadError::Unavailable)?
    }
}
fn read(c: &Connection) -> Result<Value, ReadError> {
    let tx = c.unchecked_transaction().map_err(sql_error)?;
    let mut stmt = tx
        .prepare("SELECT state,count(*) FROM fetch_jobs GROUP BY state")
        .map_err(sql_error)?;
    let rows = stmt
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))
        .map_err(sql_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sql_error)?;
    drop(stmt);
    let mut counts = json!({"pending":0,"running":0,"retry_wait":0,"dormant":0,"succeeded":0});
    for (state, count) in rows {
        counts[state] = json!(count);
    }
    let (count, bytes) = tx
        .query_row(
            "SELECT count(*),coalesce(sum(length(info)),0) FROM metadata",
            [],
            |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
        )
        .map_err(sql_error)?;
    let result = json!({"jobs":counts,"metadata_count":count,"metadata_bytes":bytes});
    tx.commit().map_err(sql_error)?;
    Ok(result)
}
fn sql_error(e: rusqlite::Error) -> ReadError {
    if matches!(e,rusqlite::Error::SqliteFailure(ref e,_) if e.code==rusqlite::ErrorCode::OperationInterrupted)
    {
        ReadError::Cancelled
    } else {
        ReadError::Unavailable
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        collection::test_storage::{BlockedOperation, CommandBarrier},
        storage::{Storage, StorageConfig},
    };
    use std::sync::Arc;
    use tokio::sync::Semaphore;
    #[tokio::test]
    async fn cancelled_reader_holds_permit_until_database_finishes() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::open(StorageConfig::new(dir.path())).await.unwrap();
        let store = CollectionStore::new(storage.handle.clone());
        let (entered, arrival) = tokio::sync::oneshot::channel();
        let (release, receiver) = std::sync::mpsc::channel();
        *store.test_barrier.lock().unwrap() = Some((
            BlockedOperation::Inspection,
            CommandBarrier {
                entered,
                release: receiver,
            },
        ));
        let permits = Arc::new(Semaphore::new(1));
        let permit = permits.clone().acquire_owned().await.unwrap();
        let cancel = CancellationToken::new();
        let read_store = store.clone();
        let read_cancel = cancel.clone();
        let task = tokio::spawn(async move { read_store.inspect(permit, read_cancel).await });
        arrival.await.unwrap();
        cancel.cancel();
        task.abort();
        let _ = task.await;
        assert_eq!(permits.available_permits(), 0);
        release.send(()).unwrap();
        // 同一线程上的屏障确认前一闭包已经返回，不能用 sleep 猜测许可释放。
        store.call(|_| Ok(())).await.unwrap();
        assert_eq!(permits.available_permits(), 1);
        let value = store
            .inspect(
                permits.clone().acquire_owned().await.unwrap(),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(value["metadata_count"], 0);
        storage.shutdown().await.unwrap();
    }
    #[test]
    fn sql_execution_budget_interrupts_expensive_query() {
        let connection = Connection::open_in_memory().unwrap();
        let start = Instant::now();
        {
            let _progress = Progress::install(&connection, CancellationToken::new()).unwrap();
            let result = connection.query_row("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<1000000000) SELECT sum(x) FROM n",[],|row|row.get::<_,i64>(0));
            assert!(
                matches!(result, Err(rusqlite::Error::SqliteFailure(ref error,_)) if error.code==rusqlite::ErrorCode::OperationInterrupted)
            );
        }
        assert!(start.elapsed() >= Duration::from_millis(100));
        assert_eq!(
            connection
                .query_row("SELECT 42", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            42
        );
    }
    #[test]
    fn progress_hook_interrupts_and_is_removed_before_next_query() {
        let c = Connection::open_in_memory().unwrap();
        {
            let cancel = CancellationToken::new();
            cancel.cancel();
            c.progress_handler(1, Some(move || cancel.is_cancelled()))
                .unwrap();
            let _guard = Progress(&c);
            assert!(c.query_row("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<1000000) SELECT sum(x) FROM n",[],|r|r.get::<_,i64>(0)).is_err());
        }
        assert_eq!(
            c.query_row("SELECT 42", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            42
        );
    }
}
