//! 磁盘联系人只是恢复候选；收到本次运行中的合法响应后才进入路由表。
//!
//! app::session 提供磁盘快照；恢复联系人仍须重新验证，退出快照和存储错误分别交还会话。
use super::{
    api::{Command, QueryError, RemoteNode},
    runtime::{DhtDispatcher, PendingPurpose},
};
use crate::address::AddressPolicy;
use crate::dht::krpc::NodeId;
use crate::dht::krpc::QueryMethod;
use crate::dht::persistence::DhtStore;
use crate::dht::persistence::RestoredCooldown;
use crate::dht::persistence::SavedContact;
use crate::dht::persistence::identity::LocalIdentity;
use crate::storage::StorageError;
use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, Instant},
};
use tokio::sync::oneshot;

/// 节点退出的三个独立结果；网络失败也不能丢弃快照和存储结果。
pub(crate) struct DispatcherExit {
    /// UDP 事件循环的退出结果。
    pub(crate) network_result: Result<(), super::DispatcherError>,
    /// 最后的内存联系人快照，由会话继续保存到数据库。
    pub(crate) routing_snapshot: Result<Vec<SavedContact>, StorageError>,
    /// 采样器排空待保存结果的执行结果。
    pub(crate) storage_flush_result: Result<(), StorageError>,
}

/// 磁盘候选恢复进度；active 含已提交验证意图但尚未结束的项，不保证均已发包。
#[derive(Debug, Default)]
pub(super) struct Recovery {
    queued: VecDeque<SavedContact>,
    active: HashMap<NodeId, SavedContact>,
    next: Option<Instant>,
}
impl Recovery {
    pub(super) fn counts(&self) -> (usize, usize) {
        (self.queued.len(), self.active.len())
    }
    pub(super) fn deadline(&self, capacity: bool) -> Option<Instant> {
        if capacity && self.active.len() < 3 && !self.queued.is_empty() {
            self.next
        } else {
            None
        }
    }
    pub(super) fn finished(&mut self, id: NodeId) {
        self.active.remove(&id);
    }
}
impl DhtDispatcher {
    pub(crate) fn report_storage_errors_to(
        &mut self,
        report: Box<dyn Fn(StorageError) + Send + Sync>,
    ) {
        self.sampler.report_errors_to(report);
    }
    /// 校验身份并挂接冷却存储，将过滤后的磁盘联系人放入恢复队列，不直接加入路由表。
    pub(crate) fn attach_storage(
        &mut self,
        storage: DhtStore,
        identity: LocalIdentity,
        contacts: Vec<SavedContact>,
        cooldowns: Vec<RestoredCooldown>,
        policy: AddressPolicy,
    ) -> Result<(), StorageError> {
        if identity.node_id != self.routing.local_id()
            || identity.family != self.routing.address_family()
            || contacts.len() > 2048
        {
            return Err(StorageError::Invalid("持久化身份与 dispatcher 不匹配"));
        }
        self.identity_store = Some((storage.clone(), identity));
        self.sampler.attach_storage(storage, identity, cooldowns)?;
        let mut seen = std::collections::HashSet::new();
        self.recovery.queued = contacts
            .into_iter()
            .filter(|c| {
                c.id != identity.node_id
                    && identity.family.accepts(c.address)
                    && policy.accepts(c.address)
                    && seen.insert(c.id)
            })
            .collect();
        self.recovery.next = Some(super::runtime::current_time());
        Ok(())
    }
    pub(super) fn recovery_capacity(&self) -> bool {
        self.occupied()
            < self
                .transactions
                .max_pending()
                .saturating_sub(self.maintenance.config.reserved_user_transactions.max(1))
    }
    /// 在恢复容量和发送节奏允许时推进一个验证；仍受整体待发/transaction 预算约束。
    pub(super) async fn advance_recovery(&mut self, now: Instant) {
        if self
            .recovery
            .deadline(self.recovery_capacity())
            .is_none_or(|at| at > now)
        {
            return;
        }
        let Some(c) = self.recovery.queued.pop_front() else {
            return;
        };
        self.recovery.active.insert(c.id, c.clone());
        self.recovery.next = Some(now + Duration::from_secs(1));
        self.start_query(
            RemoteNode {
                address: c.address,
                expected_id: Some(c.id),
            },
            QueryMethod::Ping,
            None,
            PendingPurpose::Recovery { id: c.id },
            now,
        )
        .await;
    }
    /// 合并未验证完的磁盘候选与当前路由快照，已重新验证的地址优先；这里不写数据库。
    pub(super) fn saved_contacts(&self) -> Result<Vec<SavedContact>, StorageError> {
        let mut contacts: HashMap<_, _> = self
            .recovery
            .queued
            .iter()
            .chain(self.recovery.active.values())
            .map(|c| (c.id, c.clone()))
            .collect();
        // 已经重新验证的地址优先于旧快照地址。
        let now = super::runtime::current_time();
        for c in self.routing.snapshot(now, self.clock.wall_at(now)?)? {
            contacts.insert(c.id, c);
        }
        let mut contacts: Vec<_> = contacts.into_values().collect();
        contacts.sort_by(|a, b| {
            b.responded_at
                .cmp(&a.responded_at)
                .then(a.id.0.cmp(&b.id.0))
        });
        contacts.truncate(2048);
        Ok(contacts)
    }
    /// 网络循环结束后等待采样预约/结算，再把网络、快照、存储三个独立结果交给会话。
    /// 网络错误不会阻止尝试生成快照；调用方仍须检查各结果并保存快照、关闭数据库。
    pub(crate) async fn run_persistent(mut self) -> DispatcherExit {
        let network_result = self.run_loop().await;
        let storage_flush_result = self.sampler.flush_storage().await;
        DispatcherExit {
            network_result,
            routing_snapshot: self.saved_contacts(),
            storage_flush_result,
        }
    }
}
impl super::DhtHandle {
    pub(crate) async fn routing_snapshot(&self) -> Result<Vec<SavedContact>, StorageError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command::RoutingSnapshot { reply })
            .await
            .map_err(|_| StorageError::Closed)?;
        result.await.map_err(|_| StorageError::Closed)?
    }
    pub(crate) async fn pause_for_storage(&self, error: StorageError) -> Result<(), QueryError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command::StoragePause { error, reply })
            .await
            .map_err(|_| QueryError::DispatcherClosed)?;
        result.await.map_err(|_| QueryError::DispatcherClosed)
    }
}

impl DhtDispatcher {
    /// 只有匹配并校验完的 response/error 可提交地址观察。
    pub(in crate::dht::dispatcher) fn observe_external(
        &mut self,
        source: std::net::SocketAddr,
        bytes: Option<&[u8]>,
        now: Instant,
    ) {
        if let Some(ip) = bytes.and_then(crate::dht::security::observed)
            && self
                .routing
                .address_family()
                .accepts(std::net::SocketAddr::new(ip, 1))
        {
            self.pending_external_ip = self
                .security
                .observe(source.ip(), ip, now)
                .or(self.pending_external_ip);
        }
    }
    /// 原 socket/handle 保留；取消旧 transaction，保存身份后重新验证联系人。
    pub(in crate::dht::dispatcher) async fn rotate_identity(&mut self, now: Instant) {
        let Some(ip) = self.pending_external_ip.take() else {
            return;
        };
        let Some((store, identity)) = self.identity_store.clone() else {
            return;
        };
        let contacts = match self.saved_contacts() {
            Ok(c) => c,
            Err(e) => {
                self.sampler.mark_storage_fault(e);
                return;
            }
        };
        self.identity_paused = true;
        self.cancel_for_identity();
        if let Err(error) = self.sampler.prepare_identity_change(now).await {
            self.sampler.mark_storage_fault(error);
            return;
        }
        match crate::dht::persistence::identity::bind(&store, identity, ip).await {
            Ok(identity) => {
                self.identity_store = Some((store, identity));
                self.sampler.identity_changed(identity);
                self.routing =
                    crate::dht::routing::RoutingTable::new(identity.node_id, identity.family, now);
                self.recovery = Recovery {
                    queued: contacts.into(),
                    active: HashMap::new(),
                    next: Some(now),
                };
                self.maintenance =
                    super::maintenance::MaintenanceState::new(self.maintenance.config, now);
                self.security.committed(ip, now);
                self.identity_paused = false;
                if self.observer.enabled() {
                    self.observer.context.node_id =
                        Some(crate::observation::hex(&identity.node_id.0));
                }
                self.observer.emit(crate::observation::Kind::Lifecycle,"identity_changed","applied",||serde_json::json!({"external_ip":ip.to_string(),"node_id":crate::observation::hex(&identity.node_id.0)}));
            }
            Err(error) => self.sampler.mark_storage_fault(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dht::{
        routing::{AddressFamily, RoutingTable},
        transaction::TransactionManager,
        udp::UdpTransport,
    };
    #[tokio::test]
    async fn identity_rotation_cancels_old_queries_and_persists_restart_cooldown() {
        let dir = tempfile::tempdir().unwrap();
        let storage = crate::dht::persistence::test_storage::TestStorage::open(
            crate::storage::StorageConfig::new(dir.path()),
        )
        .await
        .unwrap();
        let store = &storage.handle;
        let identity = crate::dht::persistence::identity::load_or_create(
            store,
            "rotate",
            AddressFamily::Ipv4,
            1,
        )
        .await
        .unwrap();
        let transport = UdpTransport::bind("127.0.0.1:0", Default::default())
            .await
            .unwrap();
        let address = transport.local_addr().unwrap();
        let remote = UdpTransport::bind("127.0.0.1:0", Default::default())
            .await
            .unwrap();
        let now = Instant::now();
        let (mut dispatcher, _handle) = DhtDispatcher::new(
            transport,
            RoutingTable::new(identity.node_id, identity.family, now),
            TransactionManager::new(Duration::from_secs(5), 8),
        )
        .unwrap();
        dispatcher
            .attach_storage(
                store.clone(),
                identity,
                vec![],
                vec![],
                AddressPolicy::LocalUnicast,
            )
            .unwrap();
        let (reply, result) = oneshot::channel();
        dispatcher
            .start_query(
                RemoteNode {
                    address: remote.local_addr().unwrap(),
                    expected_id: None,
                },
                QueryMethod::Ping,
                None,
                PendingPurpose::UserPing {
                    reply,
                    cancel: Default::default(),
                },
                now,
            )
            .await;
        let old = remote.recv().await.unwrap();
        let ip = "8.8.8.8".parse().unwrap();
        dispatcher.pending_external_ip = Some(ip);
        dispatcher.rotate_identity(now).await;
        assert!(matches!(
            result.await.unwrap(),
            Err(QueryError::IdentityChanged)
        ));
        assert_eq!(dispatcher.transport.local_addr().unwrap(), address);
        assert!(dispatcher.pending.is_empty());
        assert_eq!(dispatcher.transactions.len(), 0);
        assert!(crate::dht::security::valid(
            dispatcher.routing.local_id(),
            ip
        ));
        assert_ne!(dispatcher.routing.local_id(), identity.node_id);
        assert!(
            dispatcher
                .transactions
                .complete(&old.message.t, remote.local_addr().unwrap(), now)
                .is_err()
        );
        let restored =
            crate::dht::persistence::identity::load_or_create(store, "rotate", identity.family, 2)
                .await
                .unwrap();
        assert_eq!(restored.node_id, dispatcher.routing.local_id());
        assert!(
            crate::dht::persistence::identity::cooldown_remaining(store, restored)
                .await
                .unwrap()
                > Duration::from_secs(1700)
        );
        store.call(|c| {c.execute_batch("CREATE TRIGGER fail_identity BEFORE UPDATE ON node_identities BEGIN SELECT RAISE(ABORT,'test'); END;")?;Ok(())}).await.unwrap();
        dispatcher.pending_external_ip = Some("9.9.9.9".parse().unwrap());
        dispatcher
            .rotate_identity(now + Duration::from_secs(1801))
            .await;
        assert!(dispatcher.identity_paused);
        assert_eq!(dispatcher.routing.local_id(), restored.node_id);
        dispatcher.cancel_for_identity();
        assert!(dispatcher.sampler.flush_storage().await.is_err());
        storage.shutdown().await.unwrap();
    }
}
