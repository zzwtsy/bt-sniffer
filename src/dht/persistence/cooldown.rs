//! 发包前在磁盘占住 Node ID 和 IP；崩溃后宁可多等，也不立即重采样。
//!
//! sampler 先预约再发包，响应或取消后结算；Node ID 和 IP 两条记录必须在同一事务中更新。
use super::DhtStore;
use crate::dht::krpc::NodeId;
use crate::dht::persistence::identity::LocalIdentity;
use crate::storage::{
    StorageError,
    address::{decode_ip, ip_bytes},
};
use rand::TryRng;
use rusqlite::{OptionalExtension, params};
use std::net::IpAddr;

/// 预约凭据绑定身份、Node ID 和 IP；私有 token 阻止旧回调覆盖新预约。
#[derive(Debug, Clone)]
pub(crate) struct CooldownLease {
    pub(crate) duration_ms: i64,
    pub(crate) identity: i64,
    pub(crate) id: NodeId,
    pub(crate) ip: IpAddr,
    token: [u8; 16],
}
/// 从磁盘恢复的是剩余毫秒数；sampler 将它映射到本次进程的单调时钟。
#[derive(Debug, Clone)]
pub(crate) enum RestoredCooldown {
    Id {
        node_id: NodeId,
        remaining_ms: i64,
        failures: u32,
    },
    Ip {
        ip: IpAddr,
        remaining_ms: i64,
    },
}
/// 先读完再更新同一张表，避免修改正在迭代的 SQLite 结果集。
struct CooldownRecoveryRow {
    kind: i64,
    key: Vec<u8>,
    remaining_ms: i64,
    until_ms: i64,
    failures: u32,
}

impl DhtStore {
    /// 明确未发包时只撤销对应租约，旧请求不能删除后来建立的预约。
    pub(crate) async fn abandon_sampling(&self, lease: CooldownLease) -> Result<(), StorageError> {
        self.call(move |connection| {
            let tx=connection.transaction()?;
            tx.execute("DELETE FROM sampling_cooldowns WHERE identity=?1 AND lease=?2 AND pending=1 AND ((kind=0 AND key=?3) OR (kind=1 AND key=?4))", params![lease.identity,lease.token.as_slice(),lease.id.0.as_slice(),ip_bytes(lease.ip)])?;
            tx.commit()?;
            Ok(())
        }).await
    }
    /// 发包前原子预约两个键；now 为 UTC 毫秒，capacity 分别限制 ID 和 IP 两类记录。
    /// 返回租约表示事务已提交；冷却冲突或容量不足会回滚整个预约。
    pub(crate) async fn reserve_sampling(
        &self,
        identity: LocalIdentity,
        id: NodeId,
        ip: IpAddr,
        now: i64,
        duration_ms: i64,
        capacity: usize,
    ) -> Result<CooldownLease, StorageError> {
        if now < 0
            || duration_ms < 21_600_000
            || now.checked_add(duration_ms).is_none()
            || capacity == 0
            || capacity > 10_000
        {
            return Err(StorageError::Invalid("冷却预约参数无效"));
        }
        self.call(move |connection| {
            let tx = connection.transaction()?;
            tx.execute(
                "DELETE FROM sampling_cooldowns
                 WHERE identity=?1
                     AND pending=0
                     AND until_at<=?2",
                params![identity.key, now],
            )?;
            let ip_key = ip_bytes(ip);
            for (kind, key) in [(0, id.0.as_slice()), (1, ip_key.as_slice())] {
                let exists: Option<i64> = tx
                    .query_row(
                        "SELECT 1
                         FROM sampling_cooldowns
                         WHERE identity=?1
                             AND kind=?2
                             AND key=?3",
                        params![identity.key, kind, key],
                        |r| r.get(0),
                    )
                    .optional()?;
                if exists.is_some() {
                    return Err(StorageError::Cooldown);
                }
                let count: i64 = tx.query_row(
                    "SELECT count(*)
                     FROM sampling_cooldowns
                     WHERE identity=?1
                         AND kind=?2",
                    params![identity.key, kind],
                    |r| r.get(0),
                )?;
                if count as usize >= capacity {
                    return Err(StorageError::Capacity);
                }
            }
            let mut token = [0; 16];
            rand::rngs::SysRng
                .try_fill_bytes(&mut token)
                .map_err(|_| StorageError::Entropy)?;
            for (kind, key) in [(0, id.0.as_slice()), (1, ip_key.as_slice())] {
                tx.execute(
                    "INSERT INTO sampling_cooldowns
                     VALUES(?1, ?2, ?3, ?4, 1, ?5, ?6, 0)",
                    params![
                        identity.key,
                        kind,
                        key,
                        token.as_slice(),
                        now + duration_ms,
                        duration_ms
                    ],
                )?;
            }
            tx.commit()?;
            Ok(CooldownLease {
                duration_ms,
                identity: identity.key,
                id,
                ip,
                token,
            })
        })
        .await
    }
    /// 按租约结算冷却，now 为 UTC 毫秒；过期 token 不更新记录，也不作为数据库错误。
    pub(crate) async fn settle_sampling(
        &self,
        lease: CooldownLease,
        now: i64,
        duration_ms: i64,
        failures: u32,
    ) -> Result<(), StorageError> {
        if now < 0 || duration_ms <= 0 || now.checked_add(duration_ms).is_none() {
            return Err(StorageError::Invalid("冷却结算参数无效"));
        }
        self.call(move |connection| {
            let tx = connection.transaction()?;
            // token 是比较并交换条件：旧回调绝不能缩短后来重新取得的预约。
            for (kind, key) in [(0, lease.id.0.to_vec()), (1, ip_bytes(lease.ip))] {
                tx.execute(
                    "UPDATE sampling_cooldowns
                     SET pending=0, until_at=?1, duration_ms=?2, failures=?3
                     WHERE identity=?4
                         AND kind=?5
                         AND key=?6
                         AND lease=?7
                         AND pending=1",
                    params![
                        now + duration_ms,
                        duration_ms,
                        failures,
                        lease.identity,
                        kind,
                        key,
                        lease.token.as_slice()
                    ],
                )?;
            }
            tx.commit()?;
            Ok(())
        })
        .await
    }
    /// 只在创建持久化 dispatcher 时调用一次，不在每次 start_sampling 时重新延长。
    pub(crate) async fn restore_cooldowns(
        &self,
        identity: LocalIdentity,
        now: i64,
    ) -> Result<Vec<RestoredCooldown>, StorageError> {
        if now < 0 {
            return Err(StorageError::Invalid("恢复时间无效"));
        }
        self.call(move |connection| {
            let tx = connection.transaction()?;
            let mut rows_out = Vec::new();
            {
                let mut stmt = tx.prepare(
                    "SELECT kind, key, pending, until_at, duration_ms, failures
                     FROM sampling_cooldowns
                     WHERE identity=?1
                     LIMIT 20001",
                )?;
                let mut rows = stmt.query([identity.key])?;
                while let Some(row) = rows.next()? {
                    let kind: i64 = row.get(0)?;
                    let key: Vec<u8> = row.get(1)?;
                    let pending: bool = row.get(2)?;
                    let until: i64 = row.get(3)?;
                    let duration: i64 = row.get(4)?;
                    let failures: u32 = row.get(5)?;
                    if duration <= 0 || until < 0 {
                        return Err(StorageError::Invalid("数据库冷却时间无效"));
                    }
                    // 未结算预约可能已经发过包，恢复时保守等待；已结算记录只恢复剩余期限。
                    let remaining = if pending {
                        duration.max(21_600_000)
                    } else {
                        until.saturating_sub(now).clamp(0, duration)
                    };
                    let new_until = now
                        .checked_add(remaining)
                        .ok_or(StorageError::Invalid("恢复冷却溢出"))?;
                    rows_out.push(CooldownRecoveryRow {
                        kind,
                        key,
                        remaining_ms: remaining,
                        until_ms: new_until,
                        failures,
                    });
                }
            }
            // SELECT 多取一行作为越界哨兵，不能把截断结果误认为完整恢复。
            if rows_out.len() > 20_000 {
                return Err(StorageError::Capacity);
            }
            let mut restored = Vec::new();
            for CooldownRecoveryRow {
                kind,
                key,
                remaining_ms: remaining,
                until_ms: until,
                failures,
            } in rows_out
            {
                if remaining == 0 {
                    tx.execute(
                        "DELETE FROM sampling_cooldowns
                         WHERE identity=?1
                             AND kind=?2
                             AND key=?3",
                        params![identity.key, kind, key],
                    )?;
                    continue;
                }
                let value = match kind {
                    0 => RestoredCooldown::Id {
                        node_id: NodeId(
                            key.clone()
                                .try_into()
                                .map_err(|_| StorageError::Invalid("冷却节点 ID 长度无效"))?,
                        ),
                        remaining_ms: remaining,
                        failures,
                    },
                    1 => RestoredCooldown::Ip {
                        ip: decode_ip(&key)?,
                        remaining_ms: remaining,
                    },
                    _ => return Err(StorageError::Invalid("冷却键类型无效")),
                };
                tx.execute(
                    "UPDATE sampling_cooldowns
                     SET pending=0, until_at=?1
                     WHERE identity=?2
                         AND kind=?3
                         AND key=?4",
                    params![until, identity.key, kind, key],
                )?;
                restored.push(value);
            }
            tx.commit()?;
            Ok(restored)
        })
        .await
    }
}
