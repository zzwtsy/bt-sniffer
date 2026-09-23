//! 上层控制命令到 dispatcher 动作的转换和输入校验。

use super::super::api::{Command, QueryError};
use super::{DhtDispatcher, PendingPurpose};
use crate::dht::krpc::{NodeId, QueryMethod};
use std::time::Instant;

impl DhtDispatcher {
    pub(super) async fn handle_command(&mut self, command: Command, now: Instant) {
        match command {
            Command::GetPeers {
                observer,
                remote,
                hash,
                progress,
                cancel,
                reply,
            } => {
                if cancel.is_cancelled() || reply.is_closed() {
                    observer.emit(
                        crate::observation::Kind::Rpc,
                        "command",
                        "reclaimed",
                        || serde_json::json!({}),
                    );
                    return;
                }
                if !self.automatic_policy.accepts(remote.address)
                    || !self.routing.address_family().accepts(remote.address)
                {
                    observer.emit(
                        crate::observation::Kind::Rpc,
                        "command",
                        "invalid_address",
                        || serde_json::json!({}),
                    );
                    let _ = reply.send(Err(QueryError::InvalidResponse("get_peers 目标地址无效")));
                } else if self.occupied() >= self.transactions.max_pending().saturating_sub(1) {
                    observer.emit(crate::observation::Kind::Rpc, "command", "capacity", || {
                        serde_json::json!({})
                    });
                    progress
                        .limited
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                    let _ = reply.send(Err(QueryError::AtCapacity {
                        limit: self.transactions.max_pending().saturating_sub(1),
                    }));
                } else {
                    self.start_rpc(
                        remote,
                        super::super::fetch::Outbound::GetPeers(hash),
                        PendingPurpose::Fetch {
                            observer,
                            cancel,
                            reply,
                            progress,
                        },
                        now,
                    )
                    .await;
                }
            }
            Command::FetchSeeds { hash, reply } => {
                let nodes = self
                    .routing
                    .closest_usable(NodeId(hash.0), 256, now)
                    .into_iter()
                    .filter(|node| self.automatic_policy.accepts(node.address))
                    .map(|node| super::super::api::DiscoveredNode {
                        id: node.id,
                        address: node.address,
                    })
                    .collect();
                let _ = reply.send(nodes);
            }
            Command::FetchIngress { ingress, reply } => {
                self.fetch_ingress = ingress;
                let _ = reply.send(());
            }
            Command::SamplingPause { paused, reply } => {
                self.sampling_paused = paused;
                if paused {
                    self.discard_queued_sampling(now);
                }
                let _ = reply.send(());
            }
            Command::RoutingSnapshot { reply } => {
                let _ = reply.send(self.saved_contacts());
            }
            Command::StoragePause { error, reply } => {
                self.stop_sampler(now);
                self.sampler.mark_storage_fault(error);
                let _ = reply.send(());
            }
            Command::StartSampling { config, reply } => {
                self.sampler.observer = self.observer.clone();
                let reserve = self.maintenance.config.reserved_user_transactions.max(1);
                let available = self.transactions.max_pending().saturating_sub(reserve);
                let result = self.sampler.start(config, available.saturating_add(1), now);
                let _ = reply.send(result);
            }
            Command::StopSampling { reply } => {
                self.stop_sampler(now);
                let _ = reply.send(());
            }
            #[cfg(test)]
            Command::SamplingStatus { reply } => {
                let _ = reply.send(super::super::sampler::SamplerStatus {
                    collector_paused: self.sampling_paused,
                    ..self.sampler.status()
                });
            }
            #[cfg(test)]
            Command::Ping {
                remote,
                reply,
                cancel,
            } => {
                self.start_query(
                    remote,
                    QueryMethod::Ping,
                    None,
                    PendingPurpose::UserPing { reply, cancel },
                    now,
                )
                .await;
            }
            Command::BootstrapPing {
                remote,
                reply,
                cancel,
            } => {
                if !self.recovery_capacity() {
                    let _ = reply.send(Err(QueryError::AtCapacity {
                        limit: self.transactions.max_pending().saturating_sub(1),
                    }));
                } else {
                    self.start_query(
                        remote,
                        QueryMethod::Ping,
                        None,
                        PendingPurpose::UserPing { reply, cancel },
                        now,
                    )
                    .await;
                }
            }
            Command::Inspect { routing, reply } => {
                let _ = reply.send(self.inspection(routing));
            }
            Command::Status { reply } => {
                let nodes = self
                    .routing
                    .closest_usable(self.routing.local_id(), usize::MAX, now);
                let good = nodes
                    .iter()
                    .filter(|node| node.status(now) == crate::dht::routing::NodeStatus::Good)
                    .count();
                let (recovery_queued, recovery_active) = self.recovery.counts();
                let _ = reply.send(super::super::api::DhtStatus {
                    node_id: self.routing.local_id(),
                    family: self.routing.address_family(),
                    address: self.transport.local_addr().expect("已绑定的 UDP socket"),
                    good,
                    questionable: nodes.len() - good,
                    recovery_queued,
                    recovery_active,
                    pending: self.occupied(),
                    sampler: super::super::sampler::SamplerStatus {
                        collector_paused: self.sampling_paused,
                        ..self.sampler.status()
                    },
                });
            }
            #[cfg(test)]
            Command::FindNode {
                cancel,
                remote,
                target,
                reply,
            } => {
                self.start_query(
                    remote,
                    QueryMethod::FindNode,
                    Some(target),
                    PendingPurpose::UserFindNode { reply, cancel },
                    now,
                )
                .await;
            }
            Command::Shutdown { .. } => unreachable!("shutdown 已在事件循环中处理"),
        }
    }
}
