//! 已发出查询的成功、失败和超时结算。

use super::super::{
    api::{PingResponse, QueryError, RemoteNode},
    runtime::{DhtDispatcher, PendingDispatch, PendingPurpose},
};
use super::validation::ValidatedResponse;
use crate::dht::{
    krpc::{NodeId, QueryMethod},
    routing::NodeStatus,
};
use std::time::Instant;

impl DhtDispatcher {
    pub(super) async fn finish_success(
        &mut self,
        mut pending: PendingDispatch,
        responder_id: NodeId,
        decoded: ValidatedResponse,
        now: Instant,
    ) {
        pending.observation.finish("validated");
        match pending.purpose {
            PendingPurpose::Fetch { reply, .. } => {
                if let ValidatedResponse::Peers(response) = decoded {
                    let _ = reply.send(Ok(response));
                }
            }
            PendingPurpose::Recovery { id } => self.recovery.finished(id),
            PendingPurpose::Sampling(request) => match decoded {
                ValidatedResponse::Samples(response) => {
                    self.sampler.success(request, response, &self.routing, now)
                }
                ValidatedResponse::Nodes(response) => {
                    self.sampler
                        .nodes(request, response.nodes, &self.routing, now)
                }
                ValidatedResponse::Basic | ValidatedResponse::Peers(_) => {
                    unreachable!("采样响应必须先校验方法字段")
                }
            },
            PendingPurpose::UserPing { reply, .. } => {
                let _ = reply.send(Ok(PingResponse { responder_id }));
            }
            #[cfg(test)]
            PendingPurpose::UserFindNode { reply, .. } => {
                let _ = reply.send(Ok(decoded.nodes()));
            }
            PendingPurpose::Verification { key, .. } => {
                self.budget.verification_result(true);
                self.verifications.remove(&key);
            }
            PendingPurpose::BucketProbe {
                incumbent,
                mut remaining,
                candidate,
                ..
            } => {
                self.bucket_probes
                    .remove(&(incumbent.id, incumbent.address));
                if remaining.is_empty() {
                    let outcome = self.routing.reconsider_candidate(candidate, now);
                    self.handle_insert_outcome(outcome, now).await;
                } else {
                    let next = remaining.remove(0);
                    self.start_bucket_probe(next, remaining, candidate, 1, now)
                        .await;
                }
            }
            PendingPurpose::MaintenanceLookup { node_id } => {
                let response = decoded.nodes();
                let local_id = self.routing.local_id();
                let discovered = response
                    .nodes
                    .into_iter()
                    .filter_map(|node| {
                        self.routing
                            .contact(node.id)
                            .map(|contact| (contact, true))
                            .or_else(|| {
                                self.automatic_policy.accepts(node.address).then(|| {
                                    (
                                        crate::dht::routing::NodeContact::for_lookup(
                                            node.id,
                                            node.address,
                                            now,
                                        ),
                                        false,
                                    )
                                })
                            })
                    })
                    .collect();
                if let Some(lookup) = self.maintenance.lookup.as_mut() {
                    lookup.complete_success(node_id, discovered, local_id);
                }
            }
        }
    }

    pub(super) async fn finish_network_failure(
        &mut self,
        mut pending: PendingDispatch,
        error: QueryError,
        now: Instant,
    ) {
        let remote_replied_with_error = matches!(&error, QueryError::Remote { .. });
        pending.observation.observer.emit(
            crate::observation::Kind::Rpc,
            "response",
            error.label(),
            || serde_json::json!({"detail":error.to_string()}),
        );
        pending.observation.finish(error.label());
        match pending.purpose {
            PendingPurpose::Fetch { reply, .. } => {
                if !remote_replied_with_error {
                    self.record_expected_failure(pending.remote, now);
                }
                let _ = reply.send(Err(error));
            }
            PendingPurpose::Recovery { id } => self.recovery.finished(id),
            PendingPurpose::Sampling(request) => {
                if !remote_replied_with_error {
                    self.record_expected_failure(pending.remote, now);
                }
                self.sampler.failure(request, &error, now);
            }
            PendingPurpose::UserPing { reply, .. } => {
                if !remote_replied_with_error {
                    self.record_expected_failure(pending.remote, now);
                }
                let _ = reply.send(Err(error));
            }
            #[cfg(test)]
            PendingPurpose::UserFindNode { reply, .. } => {
                if !remote_replied_with_error {
                    self.record_expected_failure(pending.remote, now);
                }
                let _ = reply.send(Err(error));
            }
            PendingPurpose::Verification { key, .. } => {
                self.budget.verification_result(false);
                self.verifications.remove(&key);
            }
            PendingPurpose::BucketProbe {
                incumbent,
                remaining,
                candidate,
                attempt,
            } => {
                let key = (incumbent.id, incumbent.address);
                let status = self.routing.record_failure(incumbent.id, now);
                if status == Some(NodeStatus::Bad) {
                    self.bucket_probes.remove(&key);
                    let outcome = self.routing.reconsider_candidate(candidate, now);
                    self.handle_insert_outcome(outcome, now).await;
                } else if attempt < 2 {
                    self.start_query(
                        RemoteNode {
                            address: incumbent.address,
                            expected_id: Some(incumbent.id),
                        },
                        QueryMethod::Ping,
                        None,
                        PendingPurpose::BucketProbe {
                            incumbent,
                            remaining,
                            candidate,
                            attempt: attempt + 1,
                        },
                        now,
                    )
                    .await;
                } else {
                    self.bucket_probes.remove(&key);
                }
            }
            PendingPurpose::MaintenanceLookup { node_id } => {
                if !remote_replied_with_error {
                    self.record_expected_failure(pending.remote, now);
                }
                if let Some(lookup) = self.maintenance.lookup.as_mut() {
                    lookup.complete_failure(node_id);
                }
            }
        }
    }

    pub(in crate::dht::dispatcher) async fn expire_transactions(&mut self, now: Instant) {
        for transaction in self.transactions.expire(now) {
            if let Some(pending) = self.pending.remove(&transaction.id) {
                self.finish_network_failure(pending, QueryError::Timeout, now)
                    .await;
            }
        }
    }
}
