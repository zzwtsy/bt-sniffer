//! Web 只读查询；命令许可归数据库闭包，超时不进入采集写故障路径。
use super::store::CollectionStore;
use crate::{
    observation::{hex, parse_hash},
    storage::StorageError,
};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};
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
#[derive(Debug, Clone)]
pub(crate) enum Query {
    Hashes {
        after: Option<String>,
        limit: usize,
    },
    Jobs {
        after: Option<String>,
        limit: usize,
        state: Option<String>,
    },
    Metadata {
        after: Option<String>,
        limit: usize,
    },
    Hash([u8; 20]),
    Stats,
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
        query: Query,
        permit: OwnedSemaphorePermit,
        cancel: CancellationToken,
    ) -> Result<Value, ReadError> {
        let policy = self.inspection_policy;
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
            Ok(read(c, query, policy))
        })
        .await
        .map_err(|_| ReadError::Unavailable)?
    }
}
fn read(
    c: &Connection,
    query: Query,
    policy: crate::address::AddressPolicy,
) -> Result<Value, ReadError> {
    let tx = c.unchecked_transaction().map_err(sql_error)?;
    let result = match query {
        Query::Hash(hash) => {
            let record = tx
                .query_row(
                    "SELECT first_seen,last_seen FROM infohashes WHERE hash=?1",
                    [hash.as_slice()],
                    |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
                )
                .optional()
                .map_err(sql_error)?
                .ok_or(ReadError::Missing)?;
            let job=tx.query_row("SELECT state,attempts,due_at,generation,updated_at,error FROM fetch_jobs WHERE hash=?1",[hash.as_slice()],job_fields).optional().map_err(sql_error)?;
            let metadata=tx.query_row("SELECT length(info),fetched_at FROM metadata WHERE hash=?1",[hash.as_slice()],|r|Ok(json!({"bytes":r.get::<_,i64>(0)?,"fetched_at_ms":r.get::<_,i64>(1)?,"verification":"validated_before_commit","content_rechecked":false}))).optional().map_err(sql_error)?;
            let now = crate::observation::wall_ms() as i64;
            let mut stmt=tx.prepare("SELECT ip,port,observed_at FROM peer_hints WHERE hash=?1 AND observed_at>=?2 ORDER BY observed_at DESC LIMIT 8").map_err(sql_error)?;
            let rows = stmt
                .query_map(params![hash.as_slice(), now - 1_800_000], |r| {
                    Ok((
                        r.get::<_, Vec<u8>>(0)?,
                        r.get::<_, u16>(1)?,
                        r.get::<_, i64>(2)?,
                    ))
                })
                .map_err(sql_error)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(sql_error)?;
            let mut hints = Vec::new();
            for (bytes, port, at) in rows {
                let ip = crate::storage::address::decode_ip(&bytes)
                    .map_err(|_| ReadError::Unavailable)?;
                let address = std::net::SocketAddr::new(ip, port);
                if policy.accepts(address) {
                    hints.push(json!({"peer":address.to_string(),"observed_at_ms":at}));
                }
            }
            json!({"hash":hex(&hash),"first_seen_ms":record.0,"last_seen_ms":record.1,"job":job,"metadata":metadata,"peer_hints":hints,"original_source":null})
        }
        Query::Stats => {
            let mut stmt = tx
                .prepare("SELECT state,count(*) FROM fetch_jobs GROUP BY state")
                .map_err(sql_error)?;
            let rows = stmt
                .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))
                .map_err(sql_error)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(sql_error)?;
            let mut counts =
                json!({"pending":0,"running":0,"retry_wait":0,"dormant":0,"succeeded":0});
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
            json!({"jobs":counts,"metadata_count":count,"metadata_bytes":bytes})
        }
        Query::Hashes { after, limit } => list(
            &tx,
            "SELECT hash,first_seen,last_seen FROM infohashes WHERE hash>?1 ORDER BY hash LIMIT ?2",
            after,
            limit,
            |r| {
                Ok(
                    json!({"hash":hex(&r.get::<_,Vec<u8>>(0)?),"first_seen_ms":r.get::<_,i64>(1)?,"last_seen_ms":r.get::<_,i64>(2)?}),
                )
            },
        )?,
        Query::Metadata { after, limit } => list(
            &tx,
            "SELECT hash,length(info),fetched_at FROM metadata WHERE hash>?1 ORDER BY hash LIMIT ?2",
            after,
            limit,
            |r| {
                Ok(
                    json!({"hash":hex(&r.get::<_,Vec<u8>>(0)?),"bytes":r.get::<_,i64>(1)?,"fetched_at_ms":r.get::<_,i64>(2)?,"verification":"validated_before_commit","content_rechecked":false}),
                )
            },
        )?,
        Query::Jobs {
            after,
            limit,
            state,
        } => {
            if let Some(state) = state {
                if !["pending", "running", "retry_wait", "dormant", "succeeded"]
                    .contains(&state.as_str())
                {
                    return Err(ReadError::Invalid);
                }
                let (at, hash) = match after {
                    None => (-1, Vec::new()),
                    Some(s) => {
                        let (at, hash) = s.split_once(':').ok_or(ReadError::Invalid)?;
                        (
                            at.parse::<i64>().map_err(|_| ReadError::Invalid)?,
                            parse_hash(hash).ok_or(ReadError::Invalid)?.to_vec(),
                        )
                    }
                };
                let mut stmt=tx.prepare("SELECT state,attempts,due_at,generation,updated_at,error,hash FROM fetch_jobs WHERE state=?1 AND (due_at,hash)>(?2,?3) ORDER BY due_at,hash LIMIT ?4").map_err(sql_error)?;
                let mut items = stmt
                    .query_map(params![state, at, hash, (limit + 1) as i64], job_record)
                    .map_err(sql_error)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(sql_error)?;
                let more = items.len() > limit;
                items.truncate(limit);
                let next = if more {
                    items
                        .last()
                        .map(|r| format!("{}:{}", r["due_at_ms"], r["hash"].as_str().unwrap()))
                } else {
                    None
                };
                json!({"items":items,"next":next})
            } else {
                list(
                    &tx,
                    "SELECT state,attempts,due_at,generation,updated_at,error,hash FROM fetch_jobs WHERE hash>?1 ORDER BY hash LIMIT ?2",
                    after,
                    limit,
                    job_record,
                )?
            }
        }
    };
    tx.commit().map_err(sql_error)?;
    Ok(result)
}
fn list(
    c: &Connection,
    sql: &str,
    after: Option<String>,
    limit: usize,
    map: impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<Value>,
) -> Result<Value, ReadError> {
    if !(1..=100).contains(&limit) {
        return Err(ReadError::Invalid);
    }
    let hash = after
        .map(|s| parse_hash(&s).ok_or(ReadError::Invalid).map(|h| h.to_vec()))
        .transpose()?
        .unwrap_or_default();
    let mut stmt = c.prepare(sql).map_err(sql_error)?;
    let mut items = stmt
        .query_map(params![hash, (limit + 1) as i64], map)
        .map_err(sql_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sql_error)?;
    let more = items.len() > limit;
    items.truncate(limit);
    let next = if more {
        items.last().map(|r| r["hash"].clone())
    } else {
        None
    };
    Ok(json!({"items":items,"next":next}))
}
fn job_fields(r: &rusqlite::Row<'_>) -> rusqlite::Result<Value> {
    Ok(
        json!({"state":r.get::<_,String>(0)?,"remote_failures":r.get::<_,i64>(1)?,"due_at_ms":r.get::<_,i64>(2)?,"generation":r.get::<_,i64>(3)?,"updated_at_ms":r.get::<_,i64>(4)?,"error":r.get::<_,Option<String>>(5)?}),
    )
}
fn job_record(r: &rusqlite::Row<'_>) -> rusqlite::Result<Value> {
    let mut value = job_fields(r)?;
    value["hash"] = json!(hex(&r.get::<_, Vec<u8>>(6)?));
    Ok(value)
}
fn sql_error(e: rusqlite::Error) -> ReadError {
    if matches!(e,rusqlite::Error::SqliteFailure(ref e,_) if e.code==rusqlite::ErrorCode::OperationInterrupted)
    {
        ReadError::Cancelled
    } else {
        ReadError::Unavailable
    }
}
impl From<StorageError> for ReadError {
    fn from(_: StorageError) -> Self {
        Self::Unavailable
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
        let task =
            tokio::spawn(
                async move { read_store.inspect(Query::Stats, permit, read_cancel).await },
            );
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
                Query::Stats,
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
