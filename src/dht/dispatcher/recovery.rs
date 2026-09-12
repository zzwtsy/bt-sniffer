//! 磁盘联系人只是恢复候选；收到本次运行中的合法响应后才进入路由表。
//!
//! persistence 提供磁盘快照；恢复联系人仍须重新验证，退出快照和存储错误分别交还会话。
use super::{
    api::{Command, QueryError, RemoteNode},
    runtime::{DhtDispatcher, PendingPurpose},
};
use crate::{
    identity::LocalIdentity,
    krpc::{NodeId, QueryMethod},
    net::address::AddressPolicy,
    storage::{RestoredCooldown, SavedContact, StorageError, StorageHandle},
};
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
        report: tokio::sync::watch::Sender<Option<crate::persistence::SessionFault>>,
    ) {
        self.sampler.report_errors_to(report);
    }
    pub(crate) fn attach_storage(
        &mut self,
        storage: StorageHandle,
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
    /// 即使 socket 出错，协调器仍然可以得到最后一份内存快照。
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
