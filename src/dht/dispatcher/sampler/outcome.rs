//! 已验证响应和失败的状态转换；在途释放、冷却更新、租约结算与批次交付保持原顺序。

use super::super::api::{DiscoveredNode, QueryError};
use super::{
    protocol::SampleResponse,
    state::{Request, RequestKind, Sampler, UNSUPPORTED_FOR},
};
use crate::dht::routing::RoutingTable;
use std::time::Instant;

impl Sampler {
    pub(super) fn finished(&mut self, request: &Request) {
        if let Some(session) = &mut self.session {
            session.in_flight.remove(&request.node.id);
            self.status.in_flight = session.in_flight.len();
        }
    }

    pub(in crate::dht::dispatcher) fn success(
        &mut self,
        mut request: Request,
        response: SampleResponse,
        routing: &RoutingTable,
        now: Instant,
    ) {
        self.finished(&request);
        let Some(session) = &self.session else {
            return;
        };
        let observed_at = match self
            .durable
            .as_ref()
            .map(|durable| durable.clock.wall_at(now))
            .transpose()
        {
            Ok(value) => value.unwrap_or_else(std::time::SystemTime::now),
            Err(error) => {
                self.mark_storage_fault(error.into());
                return;
            }
        };
        let until = now + response.interval.max(session.config.minimum_interval);
        self.cooldown(request.node, until, 0);
        self.settle_request(&request, now);
        self.add_nodes(response.nodes, routing, now);
        self.status.successful += 1;
        request.observer.emit(crate::observation::Kind::Sampling,"response","validated",||serde_json::json!({"count":response.samples.len(),"num":response.num,"interval_secs":response.interval.as_secs()}));
        if let Some(permit) = request.permit.take() {
            permit.send(super::api::SampleBatch {
                observer: request.observer.clone(),
                observed_at,
                responder: request.node,
                target: request.target,
                received_at: now,
                interval: response.interval,
                num: response.num,
                samples: response.samples,
            });
        }
    }

    pub(in crate::dht::dispatcher) fn nodes(
        &mut self,
        request: Request,
        nodes: Vec<DiscoveredNode>,
        routing: &RoutingTable,
        now: Instant,
    ) {
        request.observer.emit(
            crate::observation::Kind::Sampling,
            "response",
            if request.kind == RequestKind::Sample {
                "unsupported"
            } else {
                "fallback_nodes"
            },
            || serde_json::json!({"nodes":nodes.len()}),
        );
        self.finished(&request);
        if request.kind == RequestKind::Sample {
            self.status.unsupported += 1;
            self.cooldown(request.node, now + UNSUPPORTED_FOR, 0);
        }
        self.settle_request(&request, now);
        self.add_nodes(nodes, routing, now);
    }

    pub(in crate::dht::dispatcher) fn failure(
        &mut self,
        request: Request,
        error: &QueryError,
        now: Instant,
    ) {
        request.observer.emit(
            crate::observation::Kind::Sampling,
            "response",
            error.label(),
            || serde_json::json!({"fallback":matches!(error,QueryError::Remote{code:204,..})}),
        );
        self.finished(&request);
        let Some(session) = self.session.as_mut() else {
            return;
        };
        if request.kind == RequestKind::Sample
            && matches!(error, QueryError::Remote { code: 204, .. })
        {
            if let Some(candidate) = session.candidates.get_mut(&request.node.id) {
                candidate.kind = RequestKind::FindNodeFallback;
            }
            self.status.unsupported += 1;
            self.cooldown(request.node, now + UNSUPPORTED_FOR, 0);
        } else {
            let failures = self
                .ids
                .get(&request.node.id)
                .map_or(1, |value| value.failures.saturating_add(1));
            let delay = session
                .config
                .retry_initial
                .saturating_mul(2_u32.saturating_pow(failures.saturating_sub(1)))
                .min(session.config.retry_max);
            self.status.failed += 1;
            self.cooldown(request.node, now + delay, failures);
        }
        self.settle_request(&request, now);
    }
}
