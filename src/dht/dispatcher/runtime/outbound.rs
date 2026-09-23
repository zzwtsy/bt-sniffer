//! 查询转换、transaction 登记、UDP 发送及发送前失败和关闭清理。

use super::super::api::{QueryError, RemoteNode};
use super::{DhtDispatcher, PendingDispatch, PendingPurpose, current_time};
use crate::dht::{
    krpc::{NodeId, QueryMethod},
    transaction::TransactionError,
    udp::UdpTransportError,
};
use std::time::Instant;

impl DhtDispatcher {
    pub(in crate::dht::dispatcher) async fn start_query(
        &mut self,
        remote: RemoteNode,
        method: QueryMethod,
        target: Option<NodeId>,
        purpose: PendingPurpose,
        now: Instant,
    ) {
        let query = match (method, target) {
            (QueryMethod::Ping, None) => super::super::fetch::Outbound::Ping,
            (QueryMethod::FindNode, Some(target)) => {
                super::super::fetch::Outbound::FindNode(target)
            }
            (QueryMethod::SampleInfohashes, Some(target)) => {
                super::super::fetch::Outbound::Sample(target)
            }
            _ => {
                self.finish_start_error(
                    purpose,
                    QueryError::InvalidResponse("出站查询参数不匹配"),
                    now,
                );
                return;
            }
        };
        self.start_rpc(remote, query, purpose, now).await;
    }

    pub(in crate::dht::dispatcher) async fn send_rpc(
        &mut self,
        remote: RemoteNode,
        query: super::super::fetch::Outbound,
        purpose: PendingPurpose,
        mut observation: crate::observation::Span,
        now: Instant,
    ) {
        let (method, _, _) = query.fields();
        let transaction_id = match self
            .transactions
            .register(remote.address, method.clone(), now)
        {
            Ok(id) => id,
            Err(error) => {
                observation.finish("registration_failed");
                self.finish_start_error(purpose, transaction_query_error(error), now);
                return;
            }
        };
        let message = query.message(self.routing.local_id(), transaction_id.to_byte_buf());
        let class = purpose.class();
        self.pending.insert(
            transaction_id,
            PendingDispatch {
                remote,
                purpose,
                observation,
            },
        );
        if let Err(error) = self.transport.try_send_to(remote.address, &message) {
            self.transactions.cancel(transaction_id);
            if let Some(mut pending) = self.pending.remove(&transaction_id) {
                pending.observation.finish("send_failed");
                self.finish_start_error(pending.purpose, QueryError::Transport(error), now);
            }
        } else {
            if let Some(pending) = self.pending.get(&transaction_id) {
                pending.observation.observer.emit(
                    crate::observation::Kind::Rpc,
                    "send",
                    "sent",
                    || serde_json::json!({"peer":remote.address.to_string()}),
                );
            }
            self.budget.sent(
                class,
                bendy::serde::to_bytes(&message).expect("已编码消息").len(),
            );
            if let Some(PendingDispatch {
                purpose: PendingPurpose::Fetch { progress, .. },
                ..
            }) = self.pending.get(&transaction_id)
            {
                progress
                    .sent
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }

    pub(in crate::dht::dispatcher) fn finish_start_error(
        &mut self,
        purpose: PendingPurpose,
        error: QueryError,
        now: Instant,
    ) {
        match purpose {
            PendingPurpose::Fetch { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            PendingPurpose::Recovery { id } => self.recovery.finished(id),
            PendingPurpose::Sampling(request) => {
                if matches!(&error,QueryError::Transport(UdpTransportError::Io(io)) if io.kind()==std::io::ErrorKind::WriteZero)
                {
                    self.sampler.uncertain_send(request, now);
                } else {
                    self.sampler.abandon_unsent(request, now);
                }
            }
            PendingPurpose::UserPing { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            #[cfg(test)]
            PendingPurpose::UserFindNode { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            PendingPurpose::Verification { key, .. } => {
                self.verifications.remove(&key);
            }
            PendingPurpose::BucketProbe { incumbent, .. } => {
                self.bucket_probes
                    .remove(&(incumbent.id, incumbent.address));
            }
            PendingPurpose::MaintenanceLookup { node_id } => {
                if let Some(lookup) = self.maintenance.lookup.as_mut() {
                    lookup.defer(node_id);
                }
            }
        }
    }

    pub(super) fn close_pending(&mut self) {
        self.stop_sampler(current_time());
        while let Some(queued) = self.queued.pop_front() {
            self.finish_start_error(queued.purpose, QueryError::ShuttingDown, current_time());
        }
        let pending = std::mem::take(&mut self.pending);
        for (id, pending) in pending {
            self.budget.inflight_cancelled(pending.purpose.class());
            self.transactions.cancel(id);
            match pending.purpose {
                PendingPurpose::Fetch { reply, .. } => {
                    let _ = reply.send(Err(QueryError::ShuttingDown));
                }
                PendingPurpose::UserPing { reply, .. } => {
                    let _ = reply.send(Err(QueryError::ShuttingDown));
                }
                #[cfg(test)]
                PendingPurpose::UserFindNode { reply, .. } => {
                    let _ = reply.send(Err(QueryError::ShuttingDown));
                }
                PendingPurpose::Verification { .. }
                | PendingPurpose::Recovery { .. }
                | PendingPurpose::Sampling(_)
                | PendingPurpose::BucketProbe { .. }
                | PendingPurpose::MaintenanceLookup { .. } => {}
            }
        }
        self.verifications.clear();
        self.bucket_probes.clear();
        self.maintenance.clear();
    }
}

fn transaction_query_error(error: TransactionError) -> QueryError {
    match error {
        TransactionError::AtCapacity { limit } => QueryError::AtCapacity { limit },
        error => QueryError::Transaction(error),
    }
}
