//! 近期接纳额度在数据库事务内检查；只限制任务创建，所有采样 hash 仍保留。
use super::hints::install_peer_policy;
use super::{
    BACKFILL_PAGE_SIZE, DORMANT_REACTIVATION_DELAY_MS, PEER_HINT_TTL_MS, RECENT_MS, decode_hash,
};
use crate::address::AddressPolicy;
use crate::collection::store::CollectionStore;
use crate::info_hash::InfoHashV1;
use crate::storage::StorageError;
use rusqlite::{Connection, params};
use std::sync::atomic::Ordering;

pub(crate) const POLICY_VERSION: u64 = 2;

/// 下界已合并时间窗口与游标，SQLite 可直接从复合索引定位，不重扫窗口前缀。
pub(super) const RECENT_PAGE_SQL: &str = "SELECT first_seen, hash
    FROM infohashes
    WHERE (first_seen, hash) > (?2, ?3)
      AND first_seen <= ?1
    ORDER BY first_seen, hash
    LIMIT 128";

/// 进程内配置，不写入 schema；由 collector 在启动生产者前设置。
#[derive(Debug, Clone, Copy)]
pub(in crate::collection) struct RecentAdmission {
    pub(in crate::collection) limit: usize,
    pub(in crate::collection) policy: AddressPolicy,
}
impl Default for RecentAdmission {
    fn default() -> Self {
        Self {
            limit: 0,
            policy: AddressPolicy::PublicOnly,
        }
    }
}
/// 延后计数按事件定义；前三项统计补建调用，最后一项统计可重复的 hash 观察。
#[derive(Debug, Clone, Copy)]
pub(in crate::collection) enum Deferral {
    TotalCapacity,
    FirstAttemptBuffer,
    HistoryReserve,
    SampleWaitingBackfill,
}
impl Deferral {
    fn label(self) -> &'static str {
        match self {
            Self::TotalCapacity => "total_capacity",
            Self::FirstAttemptBuffer => "first_attempt_buffer",
            Self::HistoryReserve => "history_reserve",
            Self::SampleWaitingBackfill => "sample_waiting_backfill",
        }
    }
    fn unit(self) -> &'static str {
        match self {
            Self::SampleWaitingBackfill => "hash_observation",
            _ => "backfill_call",
        }
    }
}
/// 延后原因各自计数；前三项单位为补建调用，最后一项为可重复的 hash 观察。
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct DeferralCounts {
    pub(super) total_capacity: u64,
    pub(super) first_attempt_buffer: u64,
    pub(super) history_reserve: u64,
    pub(super) sample_waiting_backfill: u64,
}

/// 单个统计范围的扫描、创建与延后观察；不表示全库未接纳 hash 的精确数量。
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct BackfillSnapshot {
    pub(super) recent_scanned: u64,
    pub(super) recent_inserted: u64,
    pub(super) history_scanned: u64,
    pub(super) history_inserted: u64,
    pub(super) deferrals: DeferralCounts,
}

/// 累计值与自上次日志以来的区间值；仅在数据库事务提交后更新。
#[derive(Debug, Default)]
pub(in crate::collection) struct BackfillCounters {
    pub(super) total: BackfillSnapshot,
    interval: BackfillSnapshot,
}
impl BackfillCounters {
    /// 只在对应事务提交后累计，回滚不增加观察数。
    pub(in crate::collection) fn defer(&mut self, reason: Deferral, count: u64) {
        for snapshot in [&mut self.total, &mut self.interval] {
            let counts = &mut snapshot.deferrals;
            let target = match reason {
                Deferral::TotalCapacity => &mut counts.total_capacity,
                Deferral::FirstAttemptBuffer => &mut counts.first_attempt_buffer,
                Deferral::HistoryReserve => &mut counts.history_reserve,
                Deferral::SampleWaitingBackfill => &mut counts.sample_waiting_backfill,
            };
            *target += count;
        }
    }
    pub(super) fn add_recent(&mut self, scanned: u64, inserted: u64) {
        for snapshot in [&mut self.total, &mut self.interval] {
            snapshot.recent_scanned += scanned;
            snapshot.recent_inserted += inserted;
        }
    }
    pub(super) fn add_history(&mut self, scanned: u64, inserted: u64) {
        for snapshot in [&mut self.total, &mut self.interval] {
            snapshot.history_scanned += scanned;
            snapshot.history_inserted += inserted;
        }
    }
}
/// Q 包含未到期的首试，提示有效性与领取共用 legal_peer；不计运行和已领取过的重试。
pub(super) const FIRST_ATTEMPT_SQL: &str = "SELECT count(*)
    FROM infohashes i
    JOIN fetch_jobs j ON j.hash = i.hash
    WHERE i.first_seen BETWEEN ?1 - ?2 AND ?1
      AND j.generation = 0
      AND j.state IN ('pending', 'retry_wait')
      AND NOT EXISTS (
          SELECT 1
          FROM peer_hints h
          WHERE h.hash = j.hash
            AND h.observed_at >= ?1 - ?3
            AND legal_peer(h.ip, h.port)
      )";
pub(super) fn first_attempt_waiting(
    connection: &Connection,
    now: i64,
) -> Result<i64, StorageError> {
    Ok(connection.query_row(
        FIRST_ATTEMPT_SQL,
        params![now, RECENT_MS, PEER_HINT_TTL_MS],
        |r| r.get(0),
    )?)
}
/// 一个批次持有的剩余额度；历史补建不能消耗近期预留空间。
pub(in crate::collection) struct Admission {
    total: usize,
    recent: usize,
    history: usize,
    enabled: bool,
}
pub(super) fn recent_active(connection: &Connection, now: i64) -> Result<i64, StorageError> {
    Ok(connection.query_row(
        "SELECT count(*)
         FROM infohashes i
         JOIN fetch_jobs j ON j.hash = i.hash
         WHERE i.first_seen BETWEEN ?1 - ?2 AND ?1
           AND j.state IN ('pending', 'running', 'retry_wait')",
        params![now, RECENT_MS],
        |r| r.get(0),
    )?)
}
impl Admission {
    pub(super) fn history_blocked(&self) -> Option<Deferral> {
        if self.total == 0 {
            Some(Deferral::TotalCapacity)
        } else if self.enabled && self.history == 0 {
            Some(Deferral::HistoryReserve)
        } else {
            None
        }
    }
    pub(super) fn has_history_room(&self) -> bool {
        self.total > 0 && (!self.enabled || self.history > 0)
    }
    pub(in crate::collection) fn load(
        connection: &Connection,
        now: i64,
        limit: usize,
        recent_config: RecentAdmission,
    ) -> Result<Self, StorageError> {
        let recent_limit = recent_config.limit;
        install_peer_policy(connection, recent_config.policy)?;
        let total = available(connection, limit)?;
        let reserve = recent_limit.min(limit.saturating_sub(1));
        Ok(Self {
            total,
            recent: if recent_limit == 0 {
                total
            } else {
                recent_limit.saturating_sub(first_attempt_waiting(connection, now)? as usize)
            },
            history: total.saturating_sub(reserve),
            enabled: recent_limit != 0,
        })
    }
    /// 重复观察只允许按原冷却复活已有 dormant，不能让新批次越过补建游标。
    /// 返回 true 表示该次观察仍无任务且无 metadata，计入等待补建观察次数。
    pub(in crate::collection) fn observe(
        &mut self,
        connection: &Connection,
        hash: InfoHashV1,
        now: i64,
    ) -> Result<bool, StorageError> {
        if !self.enabled {
            self.enqueue(connection, hash, now)?;
            return Ok(false);
        }
        let dormant: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM fetch_jobs WHERE hash=?1 AND state='dormant')",
            [hash.0.as_slice()],
            |r| r.get(0),
        )?;
        if dormant {
            let before = self.total;
            enqueue(connection, hash, now, &mut self.total)?;
            self.history = self.history.saturating_sub(before - self.total);
        }
        Ok(connection.query_row(
            "SELECT NOT EXISTS(SELECT 1 FROM fetch_jobs WHERE hash=?1)
                AND NOT EXISTS(SELECT 1 FROM metadata WHERE hash=?1)",
            [hash.0.as_slice()],
            |r| r.get(0),
        )?)
    }
    pub(in crate::collection) fn enqueue(
        &mut self,
        connection: &Connection,
        hash: InfoHashV1,
        now: i64,
    ) -> Result<usize, StorageError> {
        let first_seen: i64 = connection.query_row(
            "SELECT first_seen FROM infohashes WHERE hash=?1",
            [hash.0.as_slice()],
            |r| r.get(0),
        )?;
        let recent = (now.saturating_sub(RECENT_MS)..=now).contains(&first_seen);
        let mut room = self.total;
        if self.enabled {
            room = room.min(if recent { self.recent } else { self.history });
        }
        let before = room;
        enqueue(connection, hash, now, &mut room)?;
        let used = before - room;
        self.total = self.total.saturating_sub(used);
        self.history = self.history.saturating_sub(used);
        if recent {
            self.recent = self.recent.saturating_sub(used);
        }
        Ok(used)
    }
}
impl CollectionStore {
    /// 取走区间计数；仅在每分钟及关闭尾段调用，不增加 SQLite 扫描。
    pub(crate) fn log_backfill(&self, final_snapshot: bool) {
        // 锁内只复制累计值、取走区间值，日志格式化不占用数据库线程需要的统计锁。
        let (total, interval) = {
            let mut counters = self.backfill_counters.lock().expect("补建统计锁");
            (counters.total, std::mem::take(&mut counters.interval))
        };
        for (scope, counts) in [("total", total), ("interval", interval)] {
            for (reason, count) in [
                (Deferral::TotalCapacity, counts.deferrals.total_capacity),
                (
                    Deferral::FirstAttemptBuffer,
                    counts.deferrals.first_attempt_buffer,
                ),
                (Deferral::HistoryReserve, counts.deferrals.history_reserve),
                (
                    Deferral::SampleWaitingBackfill,
                    counts.deferrals.sample_waiting_backfill,
                ),
            ] {
                tracing::info!(
                    event = "admission_deferral",
                    schema_version = 1u64,
                    scope,
                    final_snapshot,
                    reason = reason.label(),
                    count,
                    unit = reason.unit(),
                    "接纳延后观察，可重复；不代表去重 hash 数"
                );
            }
            tracing::info!(
                event = "admission_backfill",
                schema_version = 1u64,
                scope,
                final_snapshot,
                recent_scanned = counts.recent_scanned,
                recent_inserted = counts.recent_inserted,
                history_scanned = counts.history_scanned,
                history_inserted = counts.history_inserted,
                "已提交补建事务观察；扫描可重复，未创建不等于额度拒绝"
            );
        }
    }

    /// 在采样生产者启动前配置；0 表示不设首试缓冲，仅按总容量接纳。
    pub(crate) fn enable_recent_admission(&self, limit: usize, policy: AddressPolicy) {
        *self.recent_admission.lock().expect("接纳配置锁") = RecentAdmission { limit, policy };
    }
    pub(in crate::collection) fn recent_admission(&self) -> RecentAdmission {
        *self.recent_admission.lock().expect("接纳配置锁")
    }
    /// 近期补建按 first_seen/hash 游标检查最多 128 行；下一页不反复扫描已完成前缀。
    pub(crate) async fn backfill_recent_page(
        &self,
        now: i64,
        cursor: Option<(i64, InfoHashV1)>,
    ) -> Result<Option<(i64, InfoHashV1)>, StorageError> {
        let limit = self.fetch_limit.load(Ordering::Relaxed);
        let recent_config = self.recent_admission();
        let counters = self.backfill_counters.clone();
        self.call(move |connection| {
            let tx = connection.transaction()?;
            let (mut scanned, mut inserted) = (0, 0);
            let mut admission = Admission::load(&tx, now, limit, recent_config)?;
            if admission.total == 0 || admission.recent == 0 {
                tx.commit()?;
                counters.lock().expect("补建统计锁").defer(
                    if admission.total == 0 {
                        Deferral::TotalCapacity
                    } else {
                        Deferral::FirstAttemptBuffer
                    },
                    1,
                );
                return Ok(cursor);
            }
            let window_start = now.saturating_sub(RECENT_MS);
            let (at, hash) = cursor
                .filter(|(at, _)| *at >= window_start)
                .map(|(at, h)| (at, h.0.to_vec()))
                // 空 BLOB 小于任何合法 hash，保留窗口下界上尚未扫描的所有记录。
                .unwrap_or((window_start, Vec::new()));
            let page = {
                let mut statement = tx.prepare(RECENT_PAGE_SQL)?;
                statement
                    .query_map(params![now, at, hash], |r| {
                        Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?))
                    })?
                    .collect::<Result<Vec<_>, _>>()?
            };
            let mut next = None;
            for (at, bytes) in page {
                scanned += 1;
                let hash = decode_hash(bytes)?;
                // 补建不复活 dormant；重新观察后的复活仍由原 enqueue 规则控制。
                let exists: bool = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM fetch_jobs WHERE hash=?1)",
                    [hash.0.as_slice()],
                    |r| r.get(0),
                )?;
                if !exists {
                    inserted += admission.enqueue(&tx, hash, now)? as u64;
                }
                next = Some((at, hash));
                if admission.total == 0 || admission.recent == 0 {
                    break;
                }
            }
            tx.commit()?;
            counters
                .lock()
                .expect("补建统计锁")
                .add_recent(scanned, inserted);
            Ok(next)
        })
        .await
    }
}

/// 返回活跃任务剩余名额；limit 为 0 表示不接纳，超额时饱和为 0。
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
/// 在调用者的事务内尝试创建或重新激活任务，并扣减本批剩余名额。
/// 无名额、已有 metadata 或现有任务无需激活时也可返回 Ok，不代表新建了任务。
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
        "INSERT INTO fetch_jobs (
             hash,
             state,
             due_at,
             updated_at
         )
         SELECT ?1, 'pending', ?2, ?2
         WHERE NOT EXISTS (
             SELECT 1
             FROM metadata
             WHERE hash = ?1
         )
         ON CONFLICT (hash) DO NOTHING",
        params![hash.0.as_slice(), now_ms],
    )?;
    let revived = if inserted == 0 {
        connection.execute(
            "UPDATE fetch_jobs
             SET state = 'pending',
                 attempts = 0,
                 due_at = ?2,
                 updated_at = ?2,
                 error = NULL
             WHERE hash = ?1
               AND state = 'dormant'
               AND updated_at <= ?2 - ?3
               AND NOT EXISTS (
                   SELECT 1
                   FROM metadata
                   WHERE hash = ?1
               )",
            params![
                hash.0.as_slice(),             // ?1：任务 hash
                now_ms,                        // ?2：本轮 UTC 毫秒
                DORMANT_REACTIVATION_DELAY_MS, // ?3：重新激活前的最短休眠间隔
            ],
        )?
    } else {
        0
    };
    *available_slots = available_slots.saturating_sub(inserted + revived);
    Ok(())
}

impl CollectionStore {
    /// 设置后续写入 hash 时的任务接纳上限；不会在此回填或取消已有任务。
    pub(crate) fn enable_fetch(&self, limit: usize) {
        self.fetch_limit.store(limit, Ordering::Relaxed);
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
        let recent_config = self.recent_admission();
        let recent_limit = recent_config.limit;
        let counters = self.backfill_counters.clone();
        self.call(move |connection| {
            let (mut scanned, mut inserted) = (0, 0);
            let tx = connection.transaction()?;
            let mut admission = Admission::load(&tx, now_ms, limit, recent_config)?;
            let mut next = cursor;
            let blocked = admission.history_blocked();
            if admission.has_history_room() {
                // 先收集一页并释放 statement，再在同一事务中写入。
                let page = {
                    let mut statement = tx.prepare(
                        "SELECT
                             h.hash,
                             h.first_seen,
                             NOT EXISTS (
                                 SELECT 1
                                 FROM fetch_jobs j
                                 WHERE j.hash = h.hash
                             ) AND NOT EXISTS (
                                 SELECT 1
                                 FROM metadata m
                                 WHERE m.hash = h.hash
                             )
                         FROM infohashes h
                         WHERE h.hash > ?1
                         ORDER BY h.hash
                         LIMIT ?2",
                    )?;
                    let after = cursor.map(|h| h.0.to_vec()).unwrap_or_default();
                    statement
                        .query_map(
                            params![
                                after,
                                if recent_limit == 0 {
                                    BACKFILL_PAGE_SIZE
                                } else {
                                    BACKFILL_PAGE_SIZE / 2
                                }
                            ],
                            |row| {
                                Ok((
                                    row.get::<_, Vec<u8>>(0)?,
                                    row.get::<_, i64>(1)?,
                                    row.get::<_, bool>(2)?,
                                ))
                            },
                        )?
                        .collect::<Result<Vec<_>, _>>()?
                };
                next = None;
                for (bytes, first_seen, eligible) in page {
                    if !admission.has_history_room() {
                        break;
                    }
                    scanned += 1;
                    let hash = decode_hash(bytes)?;
                    if eligible
                        && (recent_limit == 0
                            || !(now_ms.saturating_sub(RECENT_MS)..=now_ms).contains(&first_seen))
                    {
                        inserted += admission.enqueue(&tx, hash, now_ms)? as u64;
                    }
                    next = Some(hash);
                }
            }
            super::hints::cleanup_expired(&tx, now_ms)?;
            tx.commit()?;
            counters
                .lock()
                .expect("补建统计锁")
                .add_history(scanned, inserted);
            if let Some(reason) = blocked {
                counters.lock().expect("补建统计锁").defer(reason, 1);
            }
            Ok(next)
        })
        .await
    }
}
