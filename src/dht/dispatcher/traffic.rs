//! 有界待发意图由 dispatcher 事件循环驱动；排队时不注册 transaction。
use super::{
    api::{QueryError, RemoteNode},
    fetch::Outbound,
    runtime::{DhtDispatcher, PendingPurpose},
};
use crate::dht::krpc::KrpcMessage;
use crate::dht::krpc::MessageType;
use crate::dht::krpc::NodeId;
use crate::dht::krpc::QueryArgs;
use crate::dht::traffic::Class;
use std::time::{Duration, Instant};

/// 尚未登记 transaction 的待发意图；持有业务上下文、统计票据及本地排队期限。
#[derive(Debug)]
pub(super) struct Queued {
    pub(super) remote: RemoteNode,
    pub(super) query: Outbound,
    pub(super) purpose: PendingPurpose,
    deadline: Instant,
    pub(super) record: crate::dht::traffic::QueueRecord,
}
impl PendingPurpose {
    pub(super) fn class(&self) -> Class {
        match self {
            Self::Fetch { .. } => Class::Collector,
            Self::Sampling(_) => Class::Sampling,
            Self::Verification { .. } => Class::Verification,
            _ => Class::Control,
        }
    }
    pub(super) fn cancelled(&self) -> bool {
        match self {
            Self::Fetch { cancel, reply, .. } => cancel.is_cancelled() || reply.is_closed(),
            Self::UserPing { reply, cancel } => reply.is_closed() || cancel.is_cancelled(),
            #[cfg(test)]
            Self::UserFindNode { reply, cancel } => reply.is_closed() || cancel.is_cancelled(),
            _ => false,
        }
    }
}
impl Outbound {
    pub(super) fn message(self, id: NodeId, t: serde_bytes::ByteBuf) -> KrpcMessage {
        let (method, target, info_hash) = self.fields();
        KrpcMessage {
            t,
            y: MessageType::Query,
            q: Some(method),
            a: Some(QueryArgs {
                id,
                target,
                info_hash,
                port: None,
                token: None,
                implied_port: None,
                want: vec![],
            }),
            r: None,
            e: None,
            ro: None,
        }
    }
}
impl DhtDispatcher {
    /// 待发加已登记请求共用容量；仅查看 transactions 会漏掉尚未发出的占用。
    pub(super) fn occupied(&self) -> usize {
        self.transactions.len() + self.queued.len()
    }
    /// 检查共享容量与用户名额后入队，再尝试推进发送；返回不代表已经发出或收到响应。
    pub(super) async fn start_rpc(
        &mut self,
        remote: RemoteNode,
        query: Outbound,
        mut purpose: PendingPurpose,
        now: Instant,
    ) {
        let reserve = if match purpose {
            PendingPurpose::UserPing { .. } => true,
            #[cfg(test)]
            PendingPurpose::UserFindNode { .. } => true,
            _ => false,
        } {
            0
        } else {
            self.maintenance.config.reserved_user_transactions.max(1)
        };
        let limit = self.transactions.max_pending().saturating_sub(reserve);
        if self.occupied() >= limit {
            self.finish_start_error(purpose, QueryError::AtCapacity { limit }, now);
            return;
        }
        if let PendingPurpose::Verification { permit, .. } = &mut purpose {
            *permit = self.budget.admit_verification(remote.address.ip());
            if permit.is_none() {
                self.finish_start_error(purpose, QueryError::LocalWait, now);
                return;
            }
        }
        let record = self.budget.queue_record(purpose.class());
        self.queued.push_back(Queued {
            record,
            remote,
            query,
            purpose,
            deadline: now + Duration::from_secs(5),
        });
        self.advance_outbound(now).await;
    }
    /// 扫描取消、超时和配额；受限目的地址不阻止后续可发送意图前进。
    /// 获得额度后出队并交给 send_rpc 登记/发送，下一唤醒点由等待期限和排队期限共同决定。
    pub(super) async fn advance_outbound(&mut self, now: Instant) {
        self.queue_deadline = None;
        let mut index = 0;
        while index < self.queued.len() {
            let queued = &self.queued[index];
            if queued.purpose.cancelled() {
                self.queued.remove(index);
                continue;
            }
            if now >= queued.deadline {
                let mut queued = self.queued.remove(index).unwrap();
                queued.record.finish(2);
                self.finish_start_error(queued.purpose, QueryError::LocalWait, now);
                continue;
            }
            let encoded = bendy::serde::to_bytes(
                &queued
                    .query
                    .message(self.routing.local_id(), vec![0; 4].into()),
            );
            let bytes = match encoded {
                Ok(bytes) => bytes.len(),
                Err(error) => {
                    let mut queued = self.queued.remove(index).unwrap();
                    queued.record.finish(3);
                    self.finish_start_error(
                        queued.purpose,
                        QueryError::Transport(crate::dht::udp::UdpTransportError::Encode(error)),
                        now,
                    );
                    continue;
                }
            };
            let decision = self.budget.query_observed(
                queued.purpose.class(),
                queued.remote.address.ip(),
                bytes,
            );
            let wait = decision.wait;
            self.queued[index].record.blocked(decision.reasons);
            let queued = &self.queued[index];
            if wait.is_zero() {
                let mut queued = self.queued.remove(index).unwrap();
                queued.record.finish(0);
                if let PendingPurpose::Verification { permit, .. } = &mut queued.purpose {
                    drop(permit.take());
                }
                drop(queued.record);
                self.send_rpc(queued.remote, queued.query, queued.purpose, now)
                    .await;
            } else {
                if let PendingPurpose::Fetch { progress, .. } = &queued.purpose {
                    progress
                        .limited
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                }
                let next = (now + wait).min(queued.deadline);
                self.queue_deadline = Some(self.queue_deadline.map_or(next, |old| old.min(next)));
                index += 1;
            }
        }
    }
    /// 只撤销尚在待发队列中的采样，因此可走明确未发送的租约回退路径。
    pub(super) fn discard_queued_sampling(&mut self, now: Instant) {
        let mut index = 0;
        while index < self.queued.len() {
            if matches!(self.queued[index].purpose, PendingPurpose::Sampling(_)) {
                if let PendingPurpose::Sampling(request) =
                    self.queued.remove(index).unwrap().purpose
                {
                    self.sampler.abandon_unsent(request, now);
                }
            } else {
                index += 1;
            }
        }
    }
}
