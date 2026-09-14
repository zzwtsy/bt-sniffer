//! 发现记录、原始 metadata 及采集事务辅助函数。
use std::sync::atomic::Ordering;

use super::store::CollectionStore;
#[cfg(test)]
use crate::collection::peer::VerifiedMetadata;
use crate::info_hash::InfoHashV1;
use crate::storage::StorageError;
use rusqlite::{OptionalExtension, params};
#[cfg(test)]
use sha1::{Digest, Sha1};
impl CollectionStore {
    /// 测试夹具使用同一逻辑时间观察和接纳。
    #[cfg(test)]
    pub(crate) async fn save_hashes(
        &self,
        hashes: &[InfoHashV1],
        observed_at: i64,
    ) -> Result<(), StorageError> {
        self.save_hashes_at(hashes, observed_at, observed_at).await
    }
    /// 完整保存观察；freshness 新任务统一补建，处理时刻只用于已有休眠任务的再激活。
    /// 两个时间均为 UTC 毫秒；非 freshness 模式使用 observed_at 作为接纳时刻。
    pub(crate) async fn save_hashes_at(
        &self,
        hashes: &[InfoHashV1],
        observed_at: i64,
        admission_at: i64,
    ) -> Result<(), StorageError> {
        if hashes.len() > 1024 {
            return Err(StorageError::Capacity);
        }
        if observed_at < 0 || admission_at < 0 {
            return Err(StorageError::Invalid("观察时间无效"));
        }
        let budget = self.budget(hashes.len() * 20).await?;
        let count = hashes.len() as u64;
        let hashes = hashes.to_vec();
        let limit = self.fetch_limit.load(Ordering::Relaxed);
        let recent_config = self.recent_admission();
        let admission_at = if recent_config.limit == 0 {
            observed_at
        } else {
            admission_at
        };
        let counters = self.backfill_counters.clone();
        let result = self
            .submit(budget, move |connection| {
                let mut deferred = 0;
                let tx = connection.transaction()?;
                let mut admission = super::jobs::admission::Admission::load(
                    &tx,
                    admission_at,
                    limit,
                    recent_config,
                )?;
                for hash in hashes {
                    upsert_hash(&tx, hash, observed_at)?;
                    deferred += u64::from(admission.observe(&tx, hash, admission_at)?);
                }
                tx.commit()?;
                counters.lock().expect("补建统计锁").defer(
                    super::jobs::admission::Deferral::SampleWaitingBackfill,
                    deferred,
                );
                Ok(())
            })
            .await;
        if result.is_ok() {
            self.sample_observations.fetch_add(count, Ordering::Relaxed);
        }
        result
    }
    /// 只接收协议层已经校验的结果，不接受未经验证的任意字节。
    #[cfg(test)]
    pub(crate) async fn save_metadata(
        &self,
        metadata: &VerifiedMetadata,
        fetched_at: i64,
    ) -> Result<(), StorageError> {
        if metadata.info().is_empty() || metadata.info().len() > 4 * 1024 * 1024 || fetched_at < 0 {
            return Err(StorageError::Invalid("metadata 大小或时间无效"));
        }
        let budget = self.budget(metadata.info().len()).await?;
        let bytes = metadata.info().to_vec();
        let hash = metadata.info_hash();
        self.submit(budget, move |connection| {
            let tx = connection.transaction()?;
            check_metadata_size(&tx, hash)?;
            let old: Option<Vec<u8>> = tx
                .query_row(
                    "SELECT info
                     FROM metadata
                     WHERE hash=?1",
                    [hash.0.as_slice()],
                    |r| r.get(0),
                )
                .optional()?;
            if old.as_ref().is_some_and(|old| old != &bytes) {
                return Err(StorageError::Conflict);
            }
            upsert_hash(&tx, hash, fetched_at)?;
            tx.execute(
                "INSERT INTO metadata
                 VALUES(?1, ?2, ?3)
                 ON CONFLICT(hash) DO NOTHING",
                params![hash.0.as_slice(), bytes, fetched_at],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
    }
    #[cfg(test)]
    /// 缺失返回 None；存在时重新校验大小、hash 和完整字典，损坏返回错误。
    pub(crate) async fn metadata(&self, hash: InfoHashV1) -> Result<Option<Vec<u8>>, StorageError> {
        // 读取同样预留最大载荷，防止并发读取绕过字节上限。
        let budget = self.budget(4 * 1024 * 1024).await?;
        self.submit(budget, move |connection| {
            check_metadata_size(connection, hash)?;
            let bytes: Option<Vec<u8>> = connection
                .query_row(
                    "SELECT info
                     FROM metadata
                     WHERE hash=?1",
                    [hash.0.as_slice()],
                    |r| r.get(0),
                )
                .optional()?;
            if let Some(bytes) = &bytes {
                if bytes.is_empty()
                    || bytes.len() > 4 * 1024 * 1024
                    || Sha1::digest(bytes).as_slice() != hash.0
                {
                    return Err(StorageError::Invalid("metadata 校验失败"));
                }
                let raw = crate::collection::peer::wire::dictionary_prefix(bytes, 64)
                    .map_err(|_| StorageError::Invalid("metadata 字典不合法"))?;
                if raw.len() != bytes.len() {
                    return Err(StorageError::Invalid("metadata 有尾随字节"));
                }
            }
            Ok(bytes)
        })
        .await
    }
}

/// 先查长度再加载正文，避免损坏的大记录绕过读取预算。
pub(super) fn check_metadata_size(
    connection: &rusqlite::Connection,
    hash: InfoHashV1,
) -> Result<(), StorageError> {
    let size: Option<i64> = connection
        .query_row(
            "SELECT length(info)
             FROM metadata
             WHERE hash=?1",
            [hash.0.as_slice()],
            |row| row.get(0),
        )
        .optional()?;
    if size.is_some_and(|size| !(1..=4_194_304).contains(&size)) {
        return Err(StorageError::Invalid("磁盘 metadata 大小无效"));
    }
    Ok(())
}
/// 合并 UTC 毫秒观察时间，保留最早 first_seen 与最晚 last_seen；事务由调用者管理。
pub(super) fn upsert_hash(
    connection: &rusqlite::Connection,
    hash: InfoHashV1,
    at: i64,
) -> Result<(), StorageError> {
    connection.execute(
        "INSERT INTO infohashes
         VALUES(?1, ?2, ?2)
         ON CONFLICT(hash)
         DO UPDATE
         SET first_seen=min(first_seen, excluded.first_seen), last_seen=max(last_seen, excluded.last_seen)", params![hash.0.as_slice(), at],
    )?;
    Ok(())
}
