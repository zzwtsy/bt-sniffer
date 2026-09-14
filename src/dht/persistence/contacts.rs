//! DHT 联系人快照的原子替换与有界读取。
use super::{DhtStore, identity::LocalIdentity};
use crate::dht::krpc::NodeId;
use crate::storage::{
    StorageError,
    address::{decode_ip, ip_bytes},
};
use rusqlite::params;
use std::net::SocketAddr;
/// 过去验证过的联系人快照；恢复后仍需网络验证，不能当作本轮已响应节点。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SavedContact {
    pub(crate) id: NodeId,
    pub(crate) address: SocketAddr,
    /// 最近验证响应的 UTC 毫秒时间，不是当前进程的 Instant。
    pub(crate) responded_at: i64,
}
impl DhtStore {
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
}
