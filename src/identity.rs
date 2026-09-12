//! DHT 身份是稳定的路由位置，不是密码，也不与 TCP Peer ID 共用。
//!
//! persistence 为每个实例和地址族加载身份；数据库线程原子地完成首次创建。
use crate::{
    dht::routing::AddressFamily,
    krpc::NodeId,
    storage::{StorageError, StorageHandle},
};
use rand::TryRng;
use rusqlite::{OptionalExtension, params};

#[derive(Debug, Clone, Copy)]
pub(crate) struct LocalIdentity {
    /// 数据库行号，只用于外键关联，不会发送到网络。
    pub(crate) key: i64,
    /// KRPC 报文使用的 20 字节节点身份。
    pub(crate) node_id: NodeId,
    pub(crate) family: AddressFamily,
}
/// 不存在才创建；已存在但损坏时返回错误，绝不静默换一个 Node ID。
pub(crate) async fn load_or_create(
    store: &StorageHandle,
    instance: &str,
    family: AddressFamily,
    now_ms: i64,
) -> Result<LocalIdentity, StorageError> {
    load_with_entropy(store, instance, family, now_ms, |bytes| {
        rand::rngs::SysRng
            .try_fill_bytes(bytes)
            .map_err(|_| StorageError::Entropy)
    })
    .await
}
async fn load_with_entropy(
    store: &StorageHandle,
    instance: &str,
    family: AddressFamily,
    now_ms: i64,
    entropy: fn(&mut [u8; 20]) -> Result<(), StorageError>,
) -> Result<LocalIdentity, StorageError> {
    if instance.is_empty() || instance.len() > 128 || now_ms < 0 {
        return Err(StorageError::Invalid("身份参数无效"));
    }
    let instance = instance.to_owned();
    // 查询和首次创建共用一个事务；损坏记录不能被新的随机身份掩盖。
    store
        .call(move |connection| {
            let family_number = match family {
                AddressFamily::Ipv4 => 4,
                AddressFamily::Ipv6 => 6,
            };
            let tx = connection.transaction()?;
            let old: Option<(i64, Vec<u8>, String)> = tx
                .query_row(
                    "SELECT identity, node_id, method
                     FROM node_identities
                     WHERE instance=?1
                         AND family=?2",
                    params![instance, family_number],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()?;
            let (key, node_id) = if let Some((key, bytes, method)) = old {
                if method != "random-v1" {
                    return Err(StorageError::Invalid("未知身份生成方式"));
                }
                (
                    key,
                    NodeId(
                        bytes
                            .try_into()
                            .map_err(|_| StorageError::Invalid("身份 ID 长度错误"))?,
                    ),
                )
            } else {
                let mut bytes = [0; 20];
                entropy(&mut bytes)?;
                tx.execute(
                    "INSERT INTO node_identities(instance, family, node_id, created_at, method)
                     VALUES(?1, ?2, ?3, ?4, 'random-v1')",
                    params![instance, family_number, bytes.as_slice(), now_ms],
                )?;
                (tx.last_insert_rowid(), NodeId(bytes))
            };
            tx.commit()?;
            Ok(LocalIdentity {
                key,
                node_id,
                family,
            })
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    // 没有随机数就不能创建身份，但加载已经提交的身份不需要再次取得随机数。
    #[tokio::test]
    async fn entropy_failure_does_not_create_or_replace_identity() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::storage::Storage::open(crate::storage::StorageConfig::new(dir.path()))
            .await
            .unwrap();
        assert!(matches!(
            load_with_entropy(&store.handle, "node", AddressFamily::Ipv4, 1, |_| Err(
                StorageError::Entropy
            ))
            .await,
            Err(StorageError::Entropy)
        ));
        let first = load_with_entropy(&store.handle, "node", AddressFamily::Ipv4, 1, |b| {
            *b = [4; 20];
            Ok(())
        })
        .await
        .unwrap();
        let second = load_with_entropy(&store.handle, "node", AddressFamily::Ipv4, 1, |_| {
            Err(StorageError::Entropy)
        })
        .await
        .unwrap();
        assert_eq!(first.node_id, second.node_id);
        store.shutdown().await.unwrap();
    }
}
