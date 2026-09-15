//! 领取候选与提示在同一事务中读取；只有成功领取才由调度器推进轮转。
use super::hints::install_peer_policy;
use super::{Claim, ClaimClass, Job, MAX_PEER_HINTS, PEER_HINT_TTL_MS, RECENT_MS, decode_hash};
use crate::collection::store::CollectionStore;
use crate::storage::{StorageError, address::decode_ip};
use rusqlite::{OptionalExtension, params};
use std::net::SocketAddr;
impl CollectionStore {
    /// 领取一条已到期任务；now_ms 是本轮 UTC 毫秒。
    /// Ok(None) 表示当前没有可领取任务；Err 表示存储失败。
    #[cfg(test)]
    pub(crate) async fn claim_job(&self, now_ms: i64) -> Result<Option<Job>, StorageError> {
        self.claim_class(now_ms, None, crate::address::AddressPolicy::LocalUnicast)
            .await
            .map(|claim| claim.map(|claim| claim.job))
    }
    /// 按调用方提供的完整类别顺序原子领取，首个命中即停止查询。
    /// now_ms 为 UTC 毫秒；空队列返回 None，只有提交后才返回领取事实。
    pub(in crate::collection) async fn claim_by_order(
        &self,
        now_ms: i64,
        order: [ClaimClass; 4],
        policy: crate::address::AddressPolicy,
    ) -> Result<Option<Claim>, StorageError> {
        self.claim_order(now_ms, Some(order), policy).await
    }

    /// 测试兼容入口：None 保留全局 due_at/hash 顺序，不采用类别轮转。
    #[cfg(test)]
    pub(in crate::collection) async fn claim_class(
        &self,
        now_ms: i64,
        preference: Option<ClaimClass>,
        policy: crate::address::AddressPolicy,
    ) -> Result<Option<Claim>, StorageError> {
        self.claim_order(now_ms, preference.map(super::policy::order_for), policy)
            .await
    }

    /// 两种选择方式共用同一事务，分类、版本与提示均来自事务事实。
    async fn claim_order(
        &self,
        now_ms: i64,
        order: Option<[ClaimClass; 4]>,
        policy: crate::address::AddressPolicy,
    ) -> Result<Option<Claim>, StorageError> {
        let observer = self.observer.clone();
        self.call(move |connection| {
            let tx = connection.transaction()?;
            install_peer_policy(&tx, policy)?;
            let select =
                |preference: Option<ClaimClass>| -> Result<Option<ScheduledRow>, StorageError> {
                    Ok(tx
                        .query_row(
                            &schedule_sql(preference),
                            params![
                                now_ms,           // ?1：本轮 UTC 毫秒
                                PEER_HINT_TTL_MS, // ?2：地址提示有效期
                                RECENT_MS,
                            ],
                            |row| {
                                Ok((
                                    row.get(0)?, // hash 原始字节
                                    row.get(1)?, // 当前 generation，尚未递增
                                    row.get(2)?, // due_at
                                    row.get(3)?, // class
                                    row.get(4)?, // first_seen
                                    row.get(5)?, // 领取前的远端失败次数
                                ))
                            },
                        )
                        .optional()?)
                };
            let candidate = if let Some(order) = order {
                let mut candidate = None;
                for class in order {
                    candidate = select(Some(class))?;
                    if candidate.is_some() {
                        break;
                    }
                }
                candidate
            } else {
                select(None)?
            };
            let Some((hash_bytes, generation, due_at, class, first_seen, failed_attempts_before)) =
                candidate
            else {
                return Ok(None);
            };
            let class = ClaimClass::from_sql(class)?;
            let hash = decode_hash(hash_bytes)?;
            // 查询候选、递增版本和读取提示共用事务；提交后才把新版本交给 collector。
            tx.execute(
                "UPDATE fetch_jobs
                 SET state = 'running',
                     generation = generation + 1,
                     updated_at = ?2
                 WHERE hash = ?1",
                params![hash.0.as_slice(), now_ms],
            )?;
            let peers = {
                let mut statement = tx.prepare(
                    "SELECT ip, port
                     FROM peer_hints
                     WHERE hash = ?1
                       AND observed_at >= ?2 - ?3
                     ORDER BY observed_at DESC, ip, port
                     LIMIT ?4",
                )?;
                let rows = statement.query_map(
                    params![
                        hash.0.as_slice(), // ?1：本次领取的 hash
                        now_ms,            // ?2：本轮 UTC 毫秒
                        PEER_HINT_TTL_MS,  // ?3：地址提示有效期
                        MAX_PEER_HINTS,    // ?4：最多读取的地址数
                    ],
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
            observer.for_job(&hash.0,generation+1).emit(crate::observation::Kind::Job,"claim","applied",||serde_json::json!({"class":class.label(),"attempt_kind":if generation==0{"first"}else{"repeat"},"failed_attempts_before":failed_attempts_before,"had_valid_hint":!peers.is_empty(),"due_at_ms":due_at,"first_seen_ms":first_seen}));
            Ok(Some(Claim {
                job: Job {
                    hash,
                    generation: generation + 1,
                    failed_attempts_before,
                    class,
                    had_valid_hint: !peers.is_empty(),
                    peers,
                },
                due_at,
                first_seen,
            }))
        })
        .await
    }
}
/// 领取查询的列序：hash 原始字节、递增前 generation、到期 UTC 毫秒、是否有新鲜合法提示。
#[cfg(test)]
pub(super) type ClaimRow = (Vec<u8>, i64, i64, bool);
#[cfg(test)]
pub(super) const CLAIM_SQL: &str = "SELECT
        j.hash,
        j.generation,
        j.due_at,
        EXISTS (
            SELECT 1
            FROM peer_hints h
            WHERE h.hash = j.hash
              AND h.observed_at >= ?1 - ?2
              AND legal_peer(h.ip, h.port)
        ) AS fresh
    FROM fetch_jobs j INDEXED BY fetch_claim_due
    WHERE j.state IN ('pending', 'retry_wait')
      AND j.due_at <= ?1
      AND (?3 IS NULL OR fresh = ?3)
    ORDER BY j.due_at, j.hash
    LIMIT 1";
/// 领取行包含冻结分类与失败次数；列序与下方候选 SQL 一致。
type ScheduledRow = (Vec<u8>, i64, i64, i64, i64, u32);
/// 按到期索引检查已接纳任务，通过 hash 关联首见时间；不构建全库近期 hash 集合。
/// SQL 片段只来自本地枚举，不接受外部字符串。所有分支保持 due_at/hash 顺序。
pub(super) fn schedule_sql(preference: Option<ClaimClass>) -> String {
    let hints = "SELECT h.hash FROM peer_hints h
        WHERE h.observed_at >= ?1 - ?2 AND legal_peer(h.ip, h.port)";
    let predicate = match preference {
        Some(ClaimClass::Hint) => format!("j.generation = 0 AND j.hash IN ({hints})"),
        Some(ClaimClass::Recent) => {
            format!(
                "j.generation = 0 AND i.first_seen BETWEEN ?1 - ?3 AND ?1
                AND j.hash NOT IN ({hints})"
            )
        }
        Some(ClaimClass::Retry) => "j.generation > 0".to_owned(),
        Some(ClaimClass::History) => {
            format!(
                "j.generation = 0 AND i.first_seen NOT BETWEEN ?1 - ?3 AND ?1
                AND j.hash NOT IN ({hints})"
            )
        }
        None => "1".to_owned(),
    };
    let index = if preference == Some(ClaimClass::Retry) {
        "fetch_retry_due"
    } else {
        "fetch_claim_due"
    };
    format!(
        "SELECT j.hash, j.generation, j.due_at,
        CASE WHEN j.generation > 0 THEN 2
             WHEN j.hash IN ({hints}) THEN 0
             WHEN i.first_seen BETWEEN ?1 - ?3 AND ?1 THEN 1 ELSE 3 END,
        i.first_seen,
        j.attempts
        FROM fetch_jobs j INDEXED BY {index}
        JOIN infohashes i ON i.hash = j.hash
        WHERE j.state IN ('pending', 'retry_wait') AND j.due_at <= ?1
          AND ({predicate})
        ORDER BY j.due_at, j.hash LIMIT 1"
    )
}
