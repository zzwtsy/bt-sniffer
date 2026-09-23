//! Web 只读查询；命令许可归数据库闭包，超时不进入采集写故障路径。
use super::store::CollectionStore;
use rusqlite::Connection;
use serde::Serialize;
use std::time::{Duration, Instant};
use tokio::sync::OwnedSemaphorePermit;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub(crate) enum ReadError {
    Invalid,
    Missing,
    Cancelled,
    Unavailable,
    Busy,
}

/// 首页读取的五种持久任务状态；字段与 fetch_jobs 的 CHECK 约束一致。
#[derive(Debug, Default, PartialEq, Eq, Serialize)]
struct JobCounts {
    pending: i64,
    running: i64,
    retry_wait: i64,
    dormant: i64,
    succeeded: i64,
}
impl JobCounts {
    fn record(&mut self, state: &str, count: i64) -> Result<(), ReadError> {
        match state {
            "pending" => self.pending = count,
            "running" => self.running = count,
            "retry_wait" => self.retry_wait = count,
            "dormant" => self.dormant = count,
            "succeeded" => self.succeeded = count,
            _ => return Err(ReadError::Unavailable),
        }
        Ok(())
    }
}

/// 监控数据库快照；只包含首页需要的计数，不暴露任务或 metadata 正文。
#[derive(Debug, PartialEq, Eq, Serialize)]
pub(crate) struct CollectionInspection {
    jobs: JobCounts,
    metadata_count: i64,
    metadata_bytes: i64,
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
    ) -> Result<CollectionInspection, ReadError> {
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
fn read(c: &Connection) -> Result<CollectionInspection, ReadError> {
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
    let mut jobs = JobCounts::default();
    for (state, count) in rows {
        jobs.record(&state, count)?;
    }
    let (count, bytes) = tx
        .query_row(
            "SELECT count(*),coalesce(sum(length(info)),0) FROM metadata",
            [],
            |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
        )
        .map_err(sql_error)?;
    let result = CollectionInspection {
        jobs,
        metadata_count: count,
        metadata_bytes: bytes,
    };
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
        assert_eq!(value.metadata_count, 0);
        storage.shutdown().await.unwrap();
    }
    #[test]
    fn job_counts_reject_unknown_state() {
        let mut counts = JobCounts::default();
        for state in ["pending", "running", "retry_wait", "dormant", "succeeded"] {
            counts.record(state, 1).unwrap();
        }
        assert_eq!(
            counts,
            JobCounts {
                pending: 1,
                running: 1,
                retry_wait: 1,
                dormant: 1,
                succeeded: 1,
            }
        );
        assert!(matches!(
            counts.record("unknown", 1),
            Err(ReadError::Unavailable)
        ));
    }
    #[test]
    fn read_returns_all_job_states_and_metadata_totals() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE fetch_jobs(state TEXT NOT NULL);
                 CREATE TABLE metadata(info BLOB NOT NULL);
                 INSERT INTO fetch_jobs(state) VALUES
                    ('pending'), ('running'), ('retry_wait'), ('dormant'),
                    ('succeeded'), ('succeeded');
                 INSERT INTO metadata(info) VALUES (X'0102'), (X'030405');",
            )
            .unwrap();
        assert_eq!(
            read(&connection).unwrap(),
            CollectionInspection {
                jobs: JobCounts {
                    pending: 1,
                    running: 1,
                    retry_wait: 1,
                    dormant: 1,
                    succeeded: 2,
                },
                metadata_count: 2,
                metadata_bytes: 5,
            }
        );
    }
    #[test]
    fn read_rejects_unknown_database_state() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE fetch_jobs(state TEXT NOT NULL);
                 CREATE TABLE metadata(info BLOB NOT NULL);
                 INSERT INTO fetch_jobs(state) VALUES ('future');",
            )
            .unwrap();
        assert!(matches!(read(&connection), Err(ReadError::Unavailable)));
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
