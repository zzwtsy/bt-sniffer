//! dispatcher 事件循环与采样状态机的接线；网络操作仍由原 dispatcher task 执行。

use super::{
    super::{
        api::RemoteNode,
        runtime::{DhtDispatcher, PendingPurpose},
    },
    state::{OutputWatch, RequestKind},
};
use crate::dht::krpc::QueryMethod;
use std::{future::pending, time::Instant};
use tokio::sync::mpsc;

/// 如果队列满就等待一个槽位；否则只监听消费者离开，避免可写队列导致空轮询。
pub(in crate::dht::dispatcher) async fn watch_output(
    watch: OutputWatch,
) -> Result<mpsc::OwnedPermit<super::api::SampleBatch>, ()> {
    match watch {
        OutputWatch::Inactive => pending().await,
        OutputWatch::Capacity(sender) => sender.reserve_owned().await.map_err(|_| ()),
        OutputWatch::Closed(sender) => {
            sender.closed().await;
            Err(())
        }
    }
}

impl DhtDispatcher {
    pub(in crate::dht::dispatcher) async fn advance_sampler(&mut self, now: Instant) {
        if self.sampling_paused {
            return;
        }
        if self
            .sampler
            .session
            .as_ref()
            .is_some_and(|session| session.output.is_closed())
        {
            self.stop_sampler(now);
            return;
        }
        let reserve = self.maintenance.config.reserved_user_transactions.max(1);
        let capacity = self
            .transactions
            .max_pending()
            .saturating_sub(reserve)
            .saturating_sub(self.occupied());
        let ready = self.sampler.take_reserved(capacity, now);
        if ready.is_none()
            && self
                .sampler
                .durable
                .as_ref()
                .is_some_and(|durable| durable.ready.is_some())
        {
            return;
        }
        let selected = ready.or_else(|| self.sampler.next(&self.routing, capacity, now));
        if let Some(mut request) = selected {
            if !request.observer.enabled() {
                request.observer = self.observer.child(crate::observation::Kind::Sampling);
                request.observer.emit(crate::observation::Kind::Sampling,"candidate","selected",||serde_json::json!({"node_id":crate::observation::hex(&request.node.id.0),"peer":request.node.address.to_string(),"target":crate::observation::hex(&request.target.0)}));
            }
            let Some(request) = self.sampler.reserve_request(request, now) else {
                return;
            };
            self.start_query(
                RemoteNode {
                    address: request.node.address,
                    expected_id: Some(request.node.id),
                },
                if request.kind == RequestKind::FindNodeFallback {
                    QueryMethod::FindNode
                } else {
                    QueryMethod::SampleInfohashes
                },
                Some(request.target),
                PendingPurpose::Sampling(request),
                now,
            )
            .await;
        }
    }

    pub(in crate::dht::dispatcher) fn stop_sampler(&mut self, now: Instant) {
        self.discard_queued_sampling(now);
        self.sampler.stop(now);
        let ids: Vec<_> = self
            .pending
            .iter()
            .filter_map(|(id, pending)| {
                matches!(pending.purpose, PendingPurpose::Sampling(_)).then_some(*id)
            })
            .collect();
        for id in ids {
            self.transactions.cancel(id);
            if let Some(pending) = self.pending.remove(&id)
                && let PendingPurpose::Sampling(request) = pending.purpose
            {
                self.sampler.cancel_request(&request, now);
            }
        }
    }
}
