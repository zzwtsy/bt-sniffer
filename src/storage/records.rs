//! 保存和加载路由快照、hash 与原始 metadata；调用方提供数据，SQLite 操作均在 Storage 专用线程执行。
//! 快照替换和批量写入保持事务边界；读回磁盘记录时仍校验结构，不能假定磁盘内容可信。
use super::{StorageError, StorageHandle};
use crate::{
    identity::LocalIdentity,
    krpc::{InfoHashV1, NodeId},
    metadata::VerifiedMetadata,
};
use rusqlite::{OptionalExtension, params};
use sha1::{Digest, Sha1};
use std::net::{IpAddr, SocketAddr};

/// 过去验证过的联系人快照；恢复后仍需网络验证，不能当作本轮已响应节点。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SavedContact {
    pub(crate) id: NodeId,
    pub(crate) address: SocketAddr,
    /// 最近验证响应的 UTC 毫秒时间，不是当前进程的 Instant。
    pub(crate) responded_at: i64,
}
/// 保存 4 或 16 字节 IP 地址，不包含端口；端口独立存储。
pub(super) fn ip_bytes(ip: IpAddr) -> Vec<u8> {
    match ip {
        IpAddr::V4(ip) => ip.octets().to_vec(),
        IpAddr::V6(ip) => ip.octets().to_vec(),
    }
}
/// 只检查地址编码长度；地址族、端口和使用策略由调用层继续校验。
pub(super) fn decode_ip(bytes: &[u8]) -> Result<IpAddr, StorageError> {
    match bytes.len() {
        4 => Ok(IpAddr::from(<[u8; 4]>::try_from(bytes).unwrap())),
        16 => Ok(IpAddr::from(<[u8; 16]>::try_from(bytes).unwrap())),
        _ => Err(StorageError::Invalid("IP 长度无效")),
    }
}
impl StorageHandle {
    /// 在一个事务内替换该身份的快照；任一联系人非法时原快照仍保留。
    pub(crate) async fn save_contacts(
        &self,
        identity: LocalIdentity,
        contacts: &[SavedContact],
    ) -> Result<(), StorageError> {
        if contacts.len() > 2048 {
            return Err(StorageError::Capacity);
        }
        let budget = self.budget(contacts.len() * 64).await?;
        let contacts = contacts.to_vec();
        self.submit(budget, move |connection| {
            let tx = connection.transaction()?;
            tx.execute(
                "DELETE FROM routing_contacts
                 WHERE identity=?1",
                [identity.key],
            )?;
            for c in contacts {
                if !identity.family.accepts(c.address)
                    || c.address.port() == 0
                    || c.id == identity.node_id
                    || c.responded_at < 0
                {
                    return Err(StorageError::Invalid("路由快照联系人无效"));
                }
                tx.execute(
                    "INSERT INTO routing_contacts
                     VALUES(?1, ?2, ?3, ?4, ?5)",
                    params![
                        identity.key,
                        c.id.0.as_slice(),
                        ip_bytes(c.address.ip()),
                        c.address.port(),
                        c.responded_at
                    ],
                )?;
            }
            tx.commit()?;
            Ok(())
        })
        .await
    }
    /// 读取有界快照并重新检查地址和 ID；这些记录仍需由 DHT 恢复流程联网验证。
    pub(crate) async fn load_contacts(
        &self,
        identity: LocalIdentity,
    ) -> Result<Vec<SavedContact>, StorageError> {
        self.call(move |connection| {
            let mut statement = connection.prepare(
                "SELECT node_id, ip, port, responded_at
                 FROM routing_contacts
                 WHERE identity=?1
                 ORDER BY responded_at DESC, node_id
                 LIMIT 2048",
            )?;
            let mut rows = statement.query([identity.key])?;
            let mut contacts = Vec::new();
            while let Some(row) = rows.next()? {
                let id: Vec<u8> = row.get(0)?;
                let ip: Vec<u8> = row.get(1)?;
                let c = SavedContact {
                    id: NodeId(
                        id.try_into()
                            .map_err(|_| StorageError::Invalid("节点 ID 长度无效"))?,
                    ),
                    address: SocketAddr::new(decode_ip(&ip)?, row.get(2)?),
                    responded_at: row.get(3)?,
                };
                if !identity.family.accepts(c.address)
                    || c.address.port() == 0
                    || c.id == identity.node_id
                    || c.responded_at < 0
                {
                    return Err(StorageError::Invalid("数据库中的路由联系人无效"));
                }
                contacts.push(c);
            }
            Ok(contacts)
        })
        .await
    }
    /// observed_at 为 UTC 毫秒；UPSERT 保持记录幂等，但成功批次仍计入观察量。
    pub(crate) async fn save_hashes(
        &self,
        hashes: &[InfoHashV1],
        observed_at: i64,
    ) -> Result<(), StorageError> {
        if hashes.len() > 1024 {
            return Err(StorageError::Capacity);
        }
        if observed_at < 0 {
            return Err(StorageError::Invalid("观察时间无效"));
        }
        let budget = self.budget(hashes.len() * 20).await?;
        let count = hashes.len() as u64;
        let hashes = hashes.to_vec();
        let limit = self.fetch_limit.load(std::sync::atomic::Ordering::Relaxed);
        let result = self
            .submit(budget, move |connection| {
                let tx = connection.transaction()?;
                let mut room = super::jobs::available(&tx, limit)?;
                for hash in hashes {
                    upsert_hash(&tx, hash, observed_at)?;
                    super::jobs::enqueue(&tx, hash, observed_at, &mut room)?;
                }
                tx.commit()?;
                Ok(())
            })
            .await;
        if result.is_ok() {
            self.sample_observations
                .fetch_add(count, std::sync::atomic::Ordering::Relaxed);
        }
        result
    }
    /// 只接收协议层已经校验的结果，不接受未经验证的任意字节。
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
    #[cfg_attr(
        not(test),
        allow(dead_code, reason = "保留元数据读取能力，尚无自动下载消费者")
    )]
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
                let raw = crate::peer_wire::dictionary_prefix(bytes, 64)
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
