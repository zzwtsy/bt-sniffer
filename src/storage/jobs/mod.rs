//! 管理 metadata 下载任务的创建、领取、重试和完成。
//!
//! SQLite 保存任务状态，collector 负责网络下载。领取时递增 generation，
//! 只有当前领取可以提交结果。保存 metadata 和标记成功必须在同一事务内完成。
//! 所有数据库操作通过 StorageHandle 交给专用线程；本模块不执行网络 I/O。
//!
//! collector 领取后持有 generation；保存 metadata 与完成任务共用事务，旧领取的迟到结果不会写入。
use super::{StorageError, StorageHandle, records::*};
use crate::{krpc::InfoHashV1, metadata::VerifiedMetadata};
use rusqlite::{Connection, OptionalExtension, params};
use std::{net::SocketAddr, sync::atomic::Ordering};

const PEER_HINT_TTL_MS: i64 = 30 * 60 * 1_000;
const DORMANT_REACTIVATION_DELAY_MS: i64 = 24 * 60 * 60 * 1_000;
const LOCAL_RETRY_DELAY_MS: i64 = 60_000;
const RETRY_BASE_DELAY_MS: f64 = 60_000.0;
const MAX_FAILED_ATTEMPTS: u32 = 6;
const MAX_PEER_HINTS: i64 = 8;
const BACKFILL_PAGE_SIZE: i64 = 256;
const PEER_HINT_CLEANUP_BATCH_SIZE: i64 = 1024;

/// 本轮如何重试；数据库仍保存原有的错误类别和状态字符串。
#[derive(Debug, Clone, Copy)]
pub(crate) enum RetryReason {
    /// 取消、无路由等条件只延期，不增加失败次数。
    Deferred,
    Local(LocalReason),
    /// 本轮失败，按原有类别记录并增加一次失败计数。
    Failed(&'static str),
}
impl RetryReason {
    pub(crate) fn failure_category(self) -> Option<&'static str> {
        match self {
            Self::Deferred | Self::Local(_) => None,
            Self::Failed(category) => Some(category),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum LocalReason {
    ResourceWait,
    NoRoute,
    Cancelled,
}
impl RetryReason {
    fn category(self) -> Option<&'static str> {
        match self {
            Self::Deferred => Some("local_deferred"),
            Self::Local(LocalReason::ResourceWait) => Some("local_wait"),
            Self::Local(LocalReason::NoRoute) => Some("no_route"),
            Self::Local(LocalReason::Cancelled) => Some("cancelled"),
            Self::Failed(category) => Some(category),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UpdateResult {
    Applied,
    Stale,
}
#[derive(Debug)]
pub(crate) struct DueStats {
    pub(crate) count: i64,
    pub(crate) oldest_wait_ms: i64,
    pub(crate) fresh: i64,
}

/// 一次任务领取，以及本轮可优先尝试的 peer 地址。
#[derive(Debug, Clone)]
pub(crate) struct Job {
    pub(crate) hash: InfoHashV1,
    /// 领取版本号；重新领取或恢复后递增，拒绝旧 worker 的迟到结果。
    pub(crate) generation: i64,
    /// 尚未过期的宣布地址；可能为空，需要通过 DHT 查找 peer。
    pub(crate) peers: Vec<SocketAddr>,
}
#[derive(Debug, Default)]
#[cfg_attr(test, derive(serde::Serialize))]
pub(crate) struct Stats {
    pub(crate) pending: i64,
    pub(crate) running: i64,
    pub(crate) retry_wait: i64,
    pub(crate) dormant: i64,
    pub(crate) succeeded: i64,
    pub(crate) metadata_count: i64,
    pub(crate) metadata_bytes: i64,
}
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

    pub(crate) fn active(&self) -> i64 {
        self.pending + self.running + self.retry_wait
    }
}
pub(super) fn available(connection: &Connection, limit: usize) -> Result<usize, StorageError> {
    if limit == 0 {
        return Ok(0);
    }
    let count: i64 = connection.query_row(
        "SELECT count(*)
         FROM fetch_jobs
         WHERE state IN ('pending', 'running', 'retry_wait')",
        [],
        |row| row.get(0),
    )?;
    Ok(limit.saturating_sub(count as usize))
}
pub(super) fn enqueue(
    connection: &Connection,
    hash: InfoHashV1,
    now_ms: i64,
    available_slots: &mut usize,
) -> Result<(), StorageError> {
    if *available_slots == 0 {
        return Ok(());
    }
    let inserted = connection.execute(
        "INSERT INTO fetch_jobs(hash, state, due_at, updated_at)
         SELECT ?1, 'pending', ?2, ?2
         WHERE NOT EXISTS(SELECT 1
             FROM metadata
             WHERE hash = ?1)
         ON CONFLICT (hash) DO NOTHING",
        params![hash.0.as_slice(), now_ms],
    )?;
    let revived = if inserted == 0 {
        connection.execute(
            "UPDATE fetch_jobs
             SET state = 'pending', attempts = 0, due_at = ?2, updated_at = ?2, error = NULL
             WHERE hash = ?1 AND state = 'dormant' AND updated_at <= ?2 - ?3 AND NOT EXISTS(SELECT 1
                 FROM metadata
                 WHERE hash = ?1)",
            params![hash.0.as_slice(), now_ms, DORMANT_REACTIVATION_DELAY_MS],
        )?
    } else {
        0
    };
    *available_slots = available_slots.saturating_sub(inserted + revived);
    Ok(())
}

impl StorageHandle {
    pub(crate) fn sample_observations(&self) -> u64 {
        self.sample_observations.load(Ordering::Relaxed)
    }
    pub(crate) fn enable_fetch(&self, limit: usize) {
        self.fetch_limit.store(limit, Ordering::Relaxed);
    }

    /// 启动时恢复未完成任务并使旧领取失效；now_ms 是恢复时的 UTC 毫秒。
    pub(crate) async fn recover_jobs(&self, now_ms: i64) -> Result<(), StorageError> {
        self.call(move |connection| {
            let tx = connection.transaction()?;
            tx.execute(
                "UPDATE fetch_jobs
                 SET state = 'pending', due_at = ?1, updated_at = ?1, generation = generation + 1
                 WHERE state = 'running'",
                [now_ms],
            )?;
            tx.execute(
                "UPDATE fetch_jobs
                 SET state = 'succeeded', error = NULL
                 WHERE EXISTS(SELECT 1
                     FROM metadata
                     WHERE metadata.hash = fetch_jobs.hash)",
                [],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    /// 保存宣布地址；observed_at_ms 是事件观察时的 UTC 毫秒。
    /// 返回 false 表示满载且该 hash 没有已接纳的活跃任务。
    pub(crate) async fn discover_peer(
        &self,
        hash: InfoHashV1,
        peer: SocketAddr,
        observed_at_ms: i64,
    ) -> Result<bool, StorageError> {
        if peer.port() == 0 || observed_at_ms < 0 {
            return Err(StorageError::Invalid("peer 发现参数无效"));
        }
        let limit = self.fetch_limit.load(Ordering::Relaxed);
        self.call(move |connection| {
            let tx = connection.transaction()?;
            let mut available_slots = available(&tx, limit)?;
            // 满载时不扩大数据库；已接纳任务仍可更新短期地址。
            let active: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1
                 FROM fetch_jobs
                 WHERE hash = ?1 AND state IN ('pending', 'running', 'retry_wait'))",
                [hash.0.as_slice()],
                |row| row.get(0),
            )?;
            if available_slots == 0 && !active {
                return Ok(false);
            }
            upsert_hash(&tx, hash, observed_at_ms)?;
            enqueue(&tx, hash, observed_at_ms, &mut available_slots)?;
            tx.execute(
                "INSERT INTO peer_hints VALUES(?1, ?2, ?3, ?4)
                 ON CONFLICT (hash, ip, port) DO UPDATE
                 SET observed_at = max(observed_at, excluded.observed_at)",
                params![
                    hash.0.as_slice(),
                    ip_bytes(peer.ip()),
                    peer.port(),
                    observed_at_ms
                ],
            )?;
            tx.execute(
                "DELETE FROM peer_hints
                 WHERE hash = ?1 AND (ip, port) NOT IN (
                     SELECT ip, port
                     FROM peer_hints
                     WHERE hash = ?1
                     ORDER BY observed_at DESC, ip, port
                     LIMIT ?2)",
                params![hash.0.as_slice(), MAX_PEER_HINTS],
            )?;
            tx.commit()?;
            Ok(true)
        })
        .await
    }

    #[cfg(test)]
    pub(crate) async fn backfill_jobs(&self, now_ms: i64) -> Result<(), StorageError> {
        self.backfill_page(now_ms, None).await.map(|_| ())
    }
    /// 从 cursor 之后分批补建任务，now_ms 是本轮 UTC 毫秒。
    /// 按 hash 游标分页，避免每秒从头扫描大量已完成记录。
    /// 返回最后扫描的 hash；扫描结束返回 None，满载时保留原游标。
    pub(crate) async fn backfill_page(
        &self,
        now_ms: i64,
        cursor: Option<InfoHashV1>,
    ) -> Result<Option<InfoHashV1>, StorageError> {
        let limit = self.fetch_limit.load(Ordering::Relaxed);
        self.call(move |connection| {
            let tx = connection.transaction()?;
            let mut available_slots = available(&tx, limit)?;
            let mut next = cursor;
            if available_slots > 0 {
                // 先收集一页并释放 statement，再在同一事务中写入。
                let page = {
                    let mut statement = tx.prepare(
                        "SELECT h.hash, NOT EXISTS(SELECT 1
                             FROM fetch_jobs j
                             WHERE j.hash = h.hash) AND NOT EXISTS(SELECT 1
                             FROM metadata m
                             WHERE m.hash = h.hash)
                         FROM infohashes h
                         WHERE h.hash > ?1
                         ORDER BY h.hash
                         LIMIT ?2",
                    )?;
                    let after = cursor.map(|h| h.0.to_vec()).unwrap_or_default();
                    statement
                        .query_map(params![after, BACKFILL_PAGE_SIZE], |row| {
                            Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, bool>(1)?))
                        })?
                        .collect::<Result<Vec<_>, _>>()?
                };
                next = None;
                for (bytes, eligible) in page {
                    if available_slots == 0 {
                        break;
                    }
                    let hash = decode_hash(bytes)?;
                    if eligible {
                        enqueue(&tx, hash, now_ms, &mut available_slots)?;
                    }
                    next = Some(hash);
                }
            }
            tx.execute(
                "DELETE FROM peer_hints
                 WHERE (hash, ip, port) IN (
                     SELECT hash, ip, port
                     FROM peer_hints
                     WHERE observed_at < ?1 - ?2
                     ORDER BY observed_at
                     LIMIT ?3)",
                params![now_ms, PEER_HINT_TTL_MS, PEER_HINT_CLEANUP_BATCH_SIZE],
            )?;
            tx.commit()?;
            Ok(next)
        })
        .await
    }

    /// 领取一条已到期任务；now_ms 是本轮 UTC 毫秒。
    /// Ok(None) 表示当前没有可领取任务；Err 表示存储失败。
    #[cfg(test)]
    pub(crate) async fn claim_job(&self, now_ms: i64) -> Result<Option<Job>, StorageError> {
        self.claim_preferred(
            now_ms,
            None,
            crate::net::address::AddressPolicy::LocalUnicast,
        )
        .await
        .map(|claim| claim.map(|(job, _, _)| job))
    }
    /// 两类分别按到期时间和 hash 查找，首选类为空时借用另一类。
    pub(crate) async fn claim_preferred(
        &self,
        now_ms: i64,
        fresh: Option<bool>,
        policy: crate::net::address::AddressPolicy,
    ) -> Result<Option<(Job, bool, i64)>, StorageError> {
        self.call(move |connection| {
            let tx = connection.transaction()?;
            install_peer_policy(&tx, policy)?;
            let select = |preference: Option<bool>| -> Result<Option<ClaimRow>, StorageError> {
                Ok(tx
                    .query_row(
                        CLAIM_SQL,
                        params![now_ms, PEER_HINT_TTL_MS, preference],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                    )
                    .optional()?)
            };
            let candidate = match select(fresh)? {
                Some(candidate) => Some(candidate),
                None if fresh.is_some() => select(fresh.map(|value| !value))?,
                None => None,
            };
            let Some((hash_bytes, generation, due_at, fresh)) = candidate else {
                return Ok(None);
            };
            let hash = decode_hash(hash_bytes)?;
            tx.execute(
                "UPDATE fetch_jobs
                 SET state = 'running', generation = generation + 1, updated_at = ?2
                 WHERE hash = ?1",
                params![hash.0.as_slice(), now_ms],
            )?;
            let peers = {
                let mut statement = tx.prepare(
                    "SELECT ip, port
                     FROM peer_hints
                     WHERE hash = ?1 AND observed_at >= ?2 - ?3
                     ORDER BY observed_at DESC, ip, port
                     LIMIT ?4",
                )?;
                let rows = statement.query_map(
                    params![hash.0.as_slice(), now_ms, PEER_HINT_TTL_MS, MAX_PEER_HINTS],
                    |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, u16>(1)?)),
                )?;
                let mut peers = Vec::new();
                for row in rows {
                    let (ip, port) = row?;
                    let peer = SocketAddr::new(decode_ip(&ip)?, port);
                    if policy.accepts(peer) {
                        peers.push(peer);
                    }
                }
                peers
            };
            tx.commit()?;
            Ok(Some((
                Job {
                    hash,
                    generation: generation + 1,
                    peers,
                },
                fresh,
                due_at,
            )))
        })
        .await
    }

    /// 安排当前领取的下一轮尝试，now_ms 是 UTC 毫秒。
    /// Deferred 只延期；Failed 增加失败次数，达到上限后休眠。
    /// 领取已失效时忽略结果，返回 Ok(())。
    pub(crate) async fn retry_job(
        &self,
        job: Job,
        now_ms: i64,
        reason: RetryReason,
    ) -> Result<UpdateResult, StorageError> {
        let error = reason.failure_category();
        let jitter = 0.8 + rand::random::<f64>() * 0.4;
        self.call(move |connection| {
            let tx = connection.transaction()?;
            let attempts: Option<u32> = tx
                .query_row(
                    "SELECT attempts
                     FROM fetch_jobs
                     WHERE hash = ?1 AND generation = ?2 AND state = 'running'",
                    params![job.hash.0.as_slice(), job.generation],
                    |row| row.get(0),
                )
                .optional()?;
            let applied = attempts.is_some();
            if let Some(attempts) = attempts {
                let attempts = attempts + u32::from(error.is_some());
                let state = if attempts >= MAX_FAILED_ATTEMPTS {
                    "dormant"
                } else {
                    "retry_wait"
                };
                let delay_ms = if error.is_none() {
                    LOCAL_RETRY_DELAY_MS
                } else {
                    (RETRY_BASE_DELAY_MS * f64::from(1u32 << attempts.saturating_sub(1)) * jitter)
                        as i64
                };
                tx.execute(
                    "UPDATE fetch_jobs
                     SET state = ?3, attempts = ?4, due_at = ?5, updated_at = ?6, error = ?7
                     WHERE hash = ?1 AND generation = ?2",
                    params![
                        job.hash.0.as_slice(),
                        job.generation,
                        state,
                        attempts,
                        now_ms.saturating_add(delay_ms),
                        now_ms,
                        reason.category(),
                    ],
                )?;
            }
            tx.commit()?;
            Ok(if applied {
                UpdateResult::Applied
            } else {
                UpdateResult::Stale
            })
        })
        .await
    }

    /// 保存当前领取的 metadata，并将任务标记为成功；now_ms 是 UTC 毫秒。
    ///
    /// hash 不匹配返回 Conflict；领取失效时忽略结果并返回 Ok(())，不写入数据。
    /// 保存 metadata、更新任务状态和清除地址提示共用事务，失败时一起回滚。
    /// 调用者取消等待，不会撤销已经入队的数据库操作。
    pub(crate) async fn complete_job(
        &self,
        job: Job,
        metadata: VerifiedMetadata,
        now_ms: i64,
    ) -> Result<UpdateResult, StorageError> {
        if metadata.info_hash() != job.hash {
            return Err(StorageError::Conflict);
        }
        let permit = self.budget(metadata.info().len()).await?;
        self.submit(permit, move |connection| {
            let tx = connection.transaction()?;
            let current: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1
                 FROM fetch_jobs
                 WHERE hash = ?1 AND generation = ?2 AND state = 'running')",
                params![job.hash.0.as_slice(), job.generation],
                |row| row.get(0),
            )?;
            if !current {
                return Ok(UpdateResult::Stale);
            }
            check_metadata_size(&tx, job.hash)?;
            let existing_info: Option<Vec<u8>> = tx
                .query_row(
                    "SELECT info
                     FROM metadata
                     WHERE hash = ?1",
                    [job.hash.0.as_slice()],
                    |row| row.get(0),
                )
                .optional()?;
            if existing_info
                .as_ref()
                .is_some_and(|bytes| bytes.as_slice() != metadata.info())
            {
                return Err(StorageError::Conflict);
            }
            tx.execute(
                "INSERT INTO metadata VALUES(?1, ?2, ?3)
                 ON CONFLICT(hash) DO NOTHING",
                params![job.hash.0.as_slice(), metadata.info(), now_ms],
            )?;
            tx.execute(
                "UPDATE fetch_jobs
                 SET state = 'succeeded', updated_at = ?2, error = NULL
                 WHERE hash = ?1",
                params![job.hash.0.as_slice(), now_ms],
            )?;
            tx.execute(
                "DELETE FROM peer_hints
                 WHERE hash = ?1",
                [job.hash.0.as_slice()],
            )?;
            tx.commit()?;
            tracing::debug!(
                hash = ?metadata.info_hash(), source = %metadata.source(),
                peer_id = ?metadata.peer_id(), bytes = metadata.info().len(),
                "原始 metadata 已提交"
            );
            Ok(UpdateResult::Applied)
        })
        .await
    }

    pub(crate) async fn due_stats(
        &self,
        now_ms: i64,
        policy: crate::net::address::AddressPolicy,
    ) -> Result<DueStats, StorageError> {
        self.call(move |connection| {
            install_peer_policy(connection, policy)?;
            Ok(connection.query_row("SELECT count(*), coalesce(max(?1-due_at),0), coalesce(sum(EXISTS(SELECT 1 FROM peer_hints h WHERE h.hash=j.hash AND h.observed_at>=?1-?2 AND legal_peer(h.ip,h.port))),0) FROM fetch_jobs j WHERE state IN ('pending','retry_wait') AND due_at<=?1", params![now_ms, PEER_HINT_TTL_MS], |row| Ok(DueStats { count:row.get(0)?, oldest_wait_ms:row.get(1)?, fresh:row.get(2)? }))?)
        }).await
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
        self.call(|connection| {
            let mut stats = Stats::default();
            let mut statement = connection.prepare(
                "SELECT state, count(*)
                 FROM fetch_jobs GROUP BY state",
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
        })
        .await
    }
}
type ClaimRow = (Vec<u8>, i64, i64, bool);
const CLAIM_SQL: &str = "SELECT j.hash, j.generation, j.due_at,
    EXISTS(SELECT 1 FROM peer_hints h WHERE h.hash=j.hash AND h.observed_at>=?1-?2 AND legal_peer(h.ip,h.port)) AS fresh
    FROM fetch_jobs j INDEXED BY fetch_claim_due
    WHERE j.state IN ('pending','retry_wait') AND j.due_at<=?1 AND (?3 IS NULL OR fresh=?3)
    ORDER BY j.due_at,j.hash LIMIT 1";
fn install_peer_policy(
    connection: &Connection,
    policy: crate::net::address::AddressPolicy,
) -> Result<(), StorageError> {
    connection.create_scalar_function(
        "legal_peer",
        2,
        rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC
            | rusqlite::functions::FunctionFlags::SQLITE_UTF8,
        move |context| {
            let bytes: Vec<u8> = context.get(0)?;
            let port: u16 = context.get(1)?;
            Ok(decode_ip(&bytes).is_ok_and(|ip| policy.accepts(SocketAddr::new(ip, port))))
        },
    )?;
    Ok(())
}

fn decode_hash(bytes: Vec<u8>) -> Result<InfoHashV1, StorageError> {
    Ok(InfoHashV1(
        bytes
            .try_into()
            .map_err(|_| StorageError::Invalid("hash 长度无效"))?,
    ))
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod scheduling_tests;
