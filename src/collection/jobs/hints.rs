//! 提示保存、合法性检查与有界清理；清理由补建事务在原位置调用。
use super::admission::{available, enqueue};
use super::{MAX_PEER_HINTS, PEER_HINT_CLEANUP_BATCH_SIZE, PEER_HINT_TTL_MS};
use crate::collection::{records::upsert_hash, store::CollectionStore};
use crate::{
    info_hash::InfoHashV1,
    storage::{
        StorageError,
        address::{decode_ip, ip_bytes},
    },
};
use rusqlite::{Connection, params};
use std::{net::SocketAddr, sync::atomic::Ordering};
impl CollectionStore {
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
        let observer = self.observer.for_hash(&hash.0);
        let mut observation = observer.span(crate::observation::Kind::Admission, "announce_save");
        let limit = self.fetch_limit.load(Ordering::Relaxed);
        self.call(move |connection| {
            observation.executing();
            let tx = connection.transaction()?;
            let mut available_slots = available(&tx, limit)?;
            // 满载时不扩大数据库；已接纳任务仍可更新短期地址。
            let active: bool = tx.query_row(
                "SELECT EXISTS (
                     SELECT 1
                     FROM fetch_jobs
                     WHERE hash = ?1
                       AND state IN ('pending', 'running', 'retry_wait')
                 )",
                [hash.0.as_slice()],
                |row| row.get(0),
            )?;
            if available_slots == 0 && !active {
                observation.finish("capacity");
                return Ok(false);
            }
            upsert_hash(&tx, hash, observed_at_ms)?;
            enqueue(&tx, hash, observed_at_ms, &mut available_slots)?;
            tx.execute(
                "INSERT INTO peer_hints
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT (hash, ip, port) DO UPDATE
                 SET observed_at = max(observed_at, excluded.observed_at)",
                params![
                    hash.0.as_slice(),
                    ip_bytes(peer.ip()),
                    peer.port(),
                    observed_at_ms,
                ],
            )?;
            tx.execute(
                "DELETE FROM peer_hints
                 WHERE hash = ?1
                   AND (ip, port) NOT IN (
                       SELECT ip, port
                       FROM peer_hints
                       WHERE hash = ?1
                       ORDER BY observed_at DESC, ip, port
                       LIMIT ?2
                   )",
                params![hash.0.as_slice(), MAX_PEER_HINTS],
            )?;
            tx.commit()?;
            observer.emit(
                crate::observation::Kind::Discovery,
                "peer_hint",
                "applied",
                || serde_json::json!({"peer":peer.to_string(),"observed_at_ms":observed_at_ms}),
            );
            observation.finish("applied");
            Ok(true)
        })
        .await
    }
}
pub(super) fn install_peer_policy(
    connection: &Connection,
    policy: crate::address::AddressPolicy,
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

/// 必须在原补建事务内调用；每批最多清理固定数量的过期提示。
pub(super) fn cleanup_expired(connection: &Connection, now_ms: i64) -> Result<(), StorageError> {
    connection.execute(
        "DELETE FROM peer_hints
                 WHERE (hash, ip, port) IN (
                     SELECT hash, ip, port
                     FROM peer_hints
                     WHERE observed_at < ?1 - ?2
                     ORDER BY observed_at
                     LIMIT ?3
                 )",
        params![
            now_ms,                       // ?1：本轮 UTC 毫秒
            PEER_HINT_TTL_MS,             // ?2：地址提示有效期
            PEER_HINT_CLEANUP_BATCH_SIZE, // ?3：单批最多清理的记录数
        ],
    )?;
    Ok(())
}
