//! DHT 身份是稳定的路由位置，不是密码，也不与 TCP Peer ID 共用。
//!
//! app::session 为每个实例和地址族加载身份；数据库线程原子地完成首次创建。
use crate::dht::krpc::NodeId;
use crate::dht::persistence::DhtStore;
use crate::dht::routing::AddressFamily;
use crate::storage::StorageError;
use rand::TryRng;
use rusqlite::{OptionalExtension, params};

/// 数据库中一个 (instance, family) 对应的稳定身份；复制此值不会创建新身份。
#[derive(Debug, Clone, Copy)]
pub(crate) struct LocalIdentity {
    /// 数据库行号，只用于外键关联，不会发送到网络。
    pub(crate) key: i64,
    /// KRPC 报文使用的 20 字节节点身份。
    pub(crate) node_id: NodeId,
    /// 身份所属地址族；同一实例的 IPv4 和 IPv6 使用不同记录。
    pub(crate) family: AddressFamily,
}
/// 不存在才创建；已存在但损坏时返回错误，绝不静默换一个 Node ID。
/// now_ms 为非负 UTC 毫秒，仅首次创建记录时使用；已有身份不再请求随机熵。
pub(crate) async fn load_or_create(
    store: &DhtStore,
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
    store: &DhtStore,
    instance: &str,
    family: AddressFamily,
    now_ms: i64,
    entropy: fn(&mut [u8; 20]) -> Result<(), StorageError>,
) -> Result<LocalIdentity, StorageError> {
    if instance.is_empty() || instance.len() > 128 || now_ms < 0 {
        return Err(StorageError::Invalid("身份参数无效"));
    }
    // 数据库命令可能比当前等待存活更久，持有字符串以满足跨线程的 Send + 'static。
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
                if method != "random-v1" && method != "bep42" {
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

/// 缓存只用于恢复身份，地址是否仍有效由本次运行的观察或显式配置决定。
pub(crate) async fn external_ip(
    store: &DhtStore,
    identity: LocalIdentity,
) -> Result<Option<std::net::IpAddr>, StorageError> {
    store
        .call(move |c| {
            let bytes: Option<Vec<u8>> = c.query_row(
                "SELECT external_ip FROM node_identities WHERE identity=?1",
                [identity.key],
                |r| r.get(0),
            )?;
            bytes
                .map(|b| match b.len() {
                    4 => Ok(std::net::IpAddr::V4(std::net::Ipv4Addr::from(
                        <[u8; 4]>::try_from(b).expect("已校验 IPv4 长度"),
                    ))),
                    16 => Ok(std::net::IpAddr::V6(std::net::Ipv6Addr::from(
                        <[u8; 16]>::try_from(b).expect("已校验 IPv6 长度"),
                    ))),
                    _ => Err(StorageError::Invalid("持久化外部地址长度错误")),
                })
                .transpose()
        })
        .await
}
/// 先持久化再更换内存身份；条件更新防止旧所有者覆盖新身份。
pub(crate) async fn bind(
    store: &DhtStore,
    identity: LocalIdentity,
    ip: std::net::IpAddr,
) -> Result<LocalIdentity, StorageError> {
    if !identity.family.accepts(std::net::SocketAddr::new(ip, 1))
        || !crate::dht::security::public(ip)
    {
        return Err(StorageError::Invalid("BEP42 外部地址无效"));
    }
    let mut random = [0; 20];
    rand::rngs::SysRng
        .try_fill_bytes(&mut random)
        .map_err(|_| StorageError::Entropy)?;
    let node_id = crate::dht::security::generate(ip, random);
    let bytes = match ip {
        std::net::IpAddr::V4(ip) => ip.octets().to_vec(),
        std::net::IpAddr::V6(ip) => ip.octets().to_vec(),
    };
    let changed_at = crate::clock::unix_millis(std::time::SystemTime::now())?;
    store.call(move |c| {
        if c.execute("UPDATE node_identities SET node_id=?2,method='bep42',external_ip=?3,external_changed_at=?5 WHERE identity=?1 AND node_id=?4",params![identity.key,node_id.0.as_slice(),bytes,identity.node_id.0.as_slice(),changed_at])? != 1 { return Err(StorageError::Conflict); }
        Ok(LocalIdentity {node_id,..identity})
    }).await
}

/// 重启恢复切换冷却；墙钟回退时保守等待完整冷却，不依赖进程内 Instant。
pub(crate) async fn cooldown_remaining(
    store: &DhtStore,
    identity: LocalIdentity,
) -> Result<std::time::Duration, StorageError> {
    let now = crate::clock::unix_millis(std::time::SystemTime::now())?;
    store
        .call(move |c| {
            let changed: Option<i64> = c.query_row(
                "SELECT external_changed_at FROM node_identities WHERE identity=?1",
                [identity.key],
                |r| r.get(0),
            )?;
            let elapsed = changed.map_or(1_800_000, |at| now.saturating_sub(at).max(0));
            Ok(std::time::Duration::from_millis(
                1_800_000u64.saturating_sub(elapsed as u64),
            ))
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
        let store = crate::dht::persistence::test_storage::TestStorage::open(
            crate::storage::StorageConfig::new(dir.path()),
        )
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
