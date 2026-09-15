//! 已接纳任务的只读快照；不参与额外调度，也不扫描未接纳 hash。
use super::admission::{first_attempt_waiting, recent_active};
use super::hints::install_peer_policy;
use super::{DueStats, PEER_HINT_TTL_MS, RECENT_MS, Stats};
use crate::address::AddressPolicy;
use crate::collection::store::CollectionStore;
use crate::storage::StorageError;
use rusqlite::params;
impl Stats {
    pub(crate) fn log(&self, final_snapshot: bool) {
        tracing::info!(
            event = "database_jobs",
            schema_version = 1u64,
            final_snapshot,
            pending = self.pending,
            running = self.running,
            retry_wait = self.retry_wait,
            dormant = self.dormant,
            succeeded = self.succeeded,
            metadata_count = self.metadata_count,
            metadata_bytes = self.metadata_bytes,
            "数据库任务状态"
        );
    }

    /// 接纳额度只计算 pending、running 和 retry_wait，不含 dormant、succeeded。
    pub(crate) fn active(&self) -> i64 {
        self.pending + self.running + self.retry_wait
    }
}
/// 全部已接纳的未首试任务，与 Q 的近期窗口和合法提示筛选无关。
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct FirstAttemptBacklog {
    pub(crate) waiting: i64,
    pub(crate) older_than_30m: i64,
    /// 从首次发现起算的毫秒数；没有任务时为 None，未来观察时间按 0 处理。
    pub(crate) oldest_discovery_age_ms: Option<i64>,
    /// 当前到期时刻之后的最大等待；与最老发现年龄可能来自不同任务。
    pub(crate) oldest_due_wait_ms: Option<i64>,
    /// 原筛选集合中 due_at 严格晚于观察时刻的任务数。
    pub(crate) not_due: i64,
}
impl FirstAttemptBacklog {
    pub(crate) fn log(&self) {
        tracing::info!(
            event = "first_attempt_backlog",
            schema_version = 1u64,
            waiting = self.waiting,
            older_than_30m = self.older_than_30m,
            oldest_discovery_age_ms = self.oldest_discovery_age_ms,
            oldest_due_wait_ms = self.oldest_due_wait_ms,
            not_due = self.not_due,
            "未首试积压；包含有提示、未到期和超过近期窗口的已接纳任务"
        );
    }
}
/// 强制从已有待领任务索引进入，避免大量未接纳观察改变 JOIN 的扫描方向。
pub(super) const FIRST_ATTEMPT_BACKLOG_SQL: &str = "SELECT
        count(*),
        coalesce(sum(i.first_seen < ?1 - ?2), 0),
        CASE WHEN count(*) = 0 THEN NULL ELSE max(0, ?1 - min(i.first_seen)) END,
        CASE WHEN count(*) = 0 THEN NULL ELSE max(0, ?1 - min(j.due_at)) END,
        coalesce(sum(j.due_at > ?1), 0)
    FROM fetch_jobs j INDEXED BY fetch_claim_due
    JOIN infohashes i ON i.hash = j.hash
    WHERE j.state IN ('pending', 'retry_wait')
      AND j.generation = 0";

/// 同一观察时间与 SQLite 读取事务内的诊断数据；不包含进程内计数。
#[derive(Debug)]
pub(crate) struct CollectionStatusSnapshot {
    pub(crate) due: DueStats,
    pub(crate) stats: Stats,
    pub(crate) recent_active: i64,
    pub(crate) first_attempt_waiting: i64,
    pub(crate) backlog: FirstAttemptBacklog,
}

impl CollectionStore {
    /// 一条命令取得完整快照；读取事务失败不返回部分数据，不改变任务状态。
    pub(crate) async fn status_snapshot(
        &self,
        now_ms: i64,
        policy: AddressPolicy,
    ) -> Result<CollectionStatusSnapshot, StorageError> {
        #[cfg(test)]
        let barrier = self.take_test_barrier(super::super::test_storage::BlockedOperation::Status);
        self.call(move |connection| {
            #[cfg(test)]
            if let Some(barrier) = barrier {
                barrier.wait()?;
            }
            install_peer_policy(connection, policy)?;
            let tx = connection.transaction()?;
            let snapshot = CollectionStatusSnapshot {
                due: read_due_stats(&tx, now_ms)?,
                stats: read_fetch_stats(&tx)?,
                recent_active: recent_active(&tx, now_ms)?,
                first_attempt_waiting: first_attempt_waiting(&tx, now_ms)?,
                backlog: read_first_attempt_backlog(&tx, now_ms)?,
            };
            tx.commit()?;
            Ok(snapshot)
        })
        .await
    }

    /// 按 now_ms（UTC 毫秒）统计到期积压；新鲜提示还必须符合本轮地址策略。
    #[cfg(test)]
    pub(crate) async fn due_stats(
        &self,
        now_ms: i64,
        policy: crate::address::AddressPolicy,
    ) -> Result<DueStats, StorageError> {
        self.call(move |connection| {
            install_peer_policy(connection, policy)?;
            read_due_stats(connection, now_ms)
        })
        .await
    }
    pub(crate) async fn active_jobs(&self) -> Result<i64, StorageError> {
        self.call(|connection| {
            Ok(connection.query_row(
                "SELECT count(*)
                 FROM fetch_jobs
                 WHERE state IN ('pending', 'running', 'retry_wait')",
                [],
                |row| row.get(0),
            )?)
        })
        .await
    }
    pub(crate) async fn fetch_stats(&self) -> Result<Stats, StorageError> {
        self.call(move |connection| read_fetch_stats(connection))
            .await
    }
    /// 每分钟诊断快照；不参与接纳判断，查询失败仍按现有存储错误路径传播。
    #[cfg(test)]
    pub(crate) async fn first_attempt_backlog(
        &self,
        now_ms: i64,
    ) -> Result<FirstAttemptBacklog, StorageError> {
        self.call(move |connection| read_first_attempt_backlog(connection, now_ms))
            .await
    }
    pub(crate) async fn first_attempt_waiting(
        &self,
        now: i64,
        policy: AddressPolicy,
    ) -> Result<i64, StorageError> {
        self.call(move |c| {
            install_peer_policy(c, policy)?;
            first_attempt_waiting(c, now)
        })
        .await
    }
    #[cfg(test)]
    pub(crate) async fn recent_active_jobs(&self, now: i64) -> Result<i64, StorageError> {
        self.call(move |c| recent_active(c, now)).await
    }
}

fn read_due_stats(
    connection: &rusqlite::Connection,
    now_ms: i64,
) -> Result<DueStats, StorageError> {
    Ok(connection.query_row(
        "SELECT
                     count(*),
                     coalesce(max(?1 - due_at), 0),
                     coalesce(sum(EXISTS (
                         SELECT 1
                         FROM peer_hints h
                         WHERE h.hash = j.hash
                           AND h.observed_at >= ?1 - ?2
                           AND legal_peer(h.ip, h.port)
                     )), 0)
                 FROM fetch_jobs j
                 WHERE state IN ('pending', 'retry_wait')
                   AND due_at <= ?1",
        params![now_ms, PEER_HINT_TTL_MS],
        |row| {
            Ok(DueStats {
                count: row.get(0)?,
                oldest_wait_ms: row.get(1)?,
                fresh: row.get(2)?,
            })
        },
    )?)
}

fn read_fetch_stats(connection: &rusqlite::Connection) -> Result<Stats, StorageError> {
    let mut stats = Stats::default();
    let mut statement = connection.prepare(
        "SELECT state, count(*)
                 FROM fetch_jobs
                 GROUP BY state",
    )?;
    for row in statement.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    })? {
        let (state, count) = row?;
        match state.as_str() {
            "pending" => stats.pending = count,
            "running" => stats.running = count,
            "retry_wait" => stats.retry_wait = count,
            "dormant" => stats.dormant = count,
            "succeeded" => stats.succeeded = count,
            _ => return Err(StorageError::Invalid("未知任务状态")),
        }
    }
    (stats.metadata_count, stats.metadata_bytes) = connection.query_row(
        "SELECT count(*), coalesce(sum(length(info)), 0)
                 FROM metadata",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    Ok(stats)
}

fn read_first_attempt_backlog(
    connection: &rusqlite::Connection,
    now_ms: i64,
) -> Result<FirstAttemptBacklog, StorageError> {
    Ok(connection.query_row(
        FIRST_ATTEMPT_BACKLOG_SQL,
        params![now_ms, RECENT_MS],
        |row| {
            Ok(FirstAttemptBacklog {
                waiting: row.get(0)?,
                older_than_30m: row.get(1)?,
                oldest_discovery_age_ms: row.get(2)?,
                oldest_due_wait_ms: row.get(3)?,
                not_due: row.get(4)?,
            })
        },
    )?)
}
