//! 内存候选、冷却期限和发送节奏；这里只选择请求，不执行网络或数据库 I/O。

use super::{
    super::api::DiscoveredNode,
    state::{
        Candidate, CandidateSelection, Cooldown, PauseReason, Request, RequestKind, Sampler,
        SamplingSession,
    },
};
use crate::dht::{
    krpc::NodeId,
    routing::{NodeStatus, RoutingTable, xor_distance},
};
use std::{collections::HashMap, net::IpAddr, time::Instant};

impl SamplingSession {
    /// 按距离选择一个可发送候选，同时报告无候选时的下一次冷却期限。
    /// 此处只读取状态；真正占用在途名额由 register_request 完成。
    fn select_candidate(
        &self,
        ids: &HashMap<NodeId, Cooldown>,
        ips: &HashMap<IpAddr, Instant>,
        now: Instant,
    ) -> CandidateSelection {
        let mut eligible = Vec::new();
        let mut earliest = None;
        let mut tracking_full = false;
        for candidate in self.candidates.values() {
            if (candidate.visited && candidate.kind == RequestKind::Sample)
                || self.in_flight.contains_key(&candidate.node.id)
                || self
                    .in_flight
                    .values()
                    .any(|ip| *ip == candidate.node.address.ip())
            {
                continue;
            }
            let id_until = ids
                .get(&candidate.node.id)
                .map(|cooldown| cooldown.until)
                .unwrap_or(now);
            let ip_until = ips
                .get(&candidate.node.address.ip())
                .copied()
                .unwrap_or(now);
            // 回退不是再次采样，允许在 204 后取一次联系人，但仍受全局发送间隔约束。
            let due = if candidate.kind == RequestKind::FindNodeFallback {
                now
            } else {
                id_until.max(ip_until)
            };
            if due > now {
                earliest = Some(earliest.map_or(due, |at: Instant| at.min(due)));
                continue;
            }
            if (!ids.contains_key(&candidate.node.id) && ids.len() >= self.config.cooldown_capacity)
                || (!ips.contains_key(&candidate.node.address.ip())
                    && ips.len() >= self.config.cooldown_capacity)
            {
                tracking_full = true;
                continue;
            }
            eligible.push(*candidate);
        }
        eligible.sort_by_key(|candidate| xor_distance(&candidate.node.id.0, &self.target.0));
        eligible.truncate(self.config.shortlist_size);
        CandidateSelection {
            candidate: eligible.first().copied(),
            earliest,
            tracking_full,
        }
    }

    /// 换一个查询目标并清除本轮进度；Node ID/IP 冷却保存在 Sampler 中，不随轮次清除。
    fn reset_round(&mut self, now: Instant) {
        self.observer.emit(crate::observation::Kind::Sampling,"round","finished",||serde_json::json!({"target":crate::observation::hex(&self.target.0),"queries":self.queries}));
        self.observer = self.observer.child(crate::observation::Kind::Lifecycle);
        self.target = NodeId(rand::random());
        self.observer.emit(
            crate::observation::Kind::Sampling,
            "round",
            "started",
            || serde_json::json!({"target":crate::observation::hex(&self.target.0)}),
        );
        self.queries = 0;
        for candidate in self.candidates.values_mut() {
            candidate.visited = false;
            candidate.kind = RequestKind::Sample;
        }
        self.next_send = now + self.config.send_spacing;
    }
}

impl Sampler {
    pub(super) fn add_nodes(
        &mut self,
        nodes: Vec<DiscoveredNode>,
        routing: &RoutingTable,
        now: Instant,
    ) {
        let Some(session) = &mut self.session else {
            return;
        };
        for node in nodes {
            if node.id == routing.local_id() {
                continue;
            }
            let known = routing.contact(node.id);
            if known.is_some_and(|contact| contact.status(now) == NodeStatus::Bad) {
                continue;
            }
            let verified = known.is_some();
            let node = known
                .map(|contact| DiscoveredNode {
                    id: contact.id,
                    address: contact.address,
                })
                .unwrap_or(node);
            if !routing.address_family().accepts(node.address)
                || !session.config.address_policy.accepts(node.address)
            {
                continue;
            }
            session
                .candidates
                .entry(node.id)
                .and_modify(|old| {
                    if verified && !session.in_flight.contains_key(&node.id) {
                        old.node = node;
                    }
                })
                .or_insert(Candidate {
                    node,
                    visited: false,
                    kind: RequestKind::Sample,
                });
        }
        // 一次响应最多一个数据报，临时新增量有界；绝不淘汰在途节点。
        while session.candidates.len() > session.config.candidate_capacity {
            let worst = session
                .candidates
                .values()
                .filter(|candidate| !session.in_flight.contains_key(&candidate.node.id))
                .max_by_key(|candidate| xor_distance(&candidate.node.id.0, &session.target.0))
                .map(|candidate| candidate.node.id);
            if let Some(id) = worst {
                session.candidates.remove(&id);
            } else {
                break;
            }
        }
    }

    /// 每次最多启动一条 RPC，下一条至少间隔 send_spacing，不积攒突发额度。
    pub(super) fn next(
        &mut self,
        routing: &RoutingTable,
        capacity: usize,
        now: Instant,
    ) -> Option<Request> {
        self.deadline = None;
        if self.status.storage_error.is_some() {
            self.status.pause = PauseReason::Storage;
            return None;
        }
        if self.durable.as_ref().is_some_and(|durable| {
            durable.reserving.is_some() || !durable.settling.is_empty() || durable.ready.is_some()
        }) {
            self.status.pause = PauseReason::Storage;
            return None;
        }
        let session = self.session.as_ref()?;
        let seeds = routing
            .closest_usable(session.target, session.config.shortlist_size, now)
            .into_iter()
            .map(|node| DiscoveredNode {
                id: node.id,
                address: node.address,
            })
            .collect();
        self.add_nodes(seeds, routing, now);
        let session = self.session.as_mut()?;
        session.candidates.retain(|id, _| {
            session.in_flight.contains_key(id)
                || routing
                    .contact(*id)
                    .is_none_or(|node| node.status(now) != NodeStatus::Bad)
        });
        self.status.in_flight = session.in_flight.len();
        self.status.candidates = session.candidates.len();
        if session.candidates.is_empty() {
            self.status.pause = PauseReason::NoSeeds;
            return None;
        }
        if capacity == 0 || session.in_flight.len() >= session.config.parallelism {
            self.status.pause = PauseReason::Transactions;
            return None;
        }
        if session.output.is_closed() {
            return None;
        }
        if session.ready_permit.is_none() {
            match session.output.clone().try_reserve_owned() {
                Ok(permit) => session.ready_permit = Some(permit),
                Err(_) => {
                    self.status.pause = PauseReason::Output;
                    return None;
                }
            }
        }
        if now < session.next_send {
            self.status.pause = PauseReason::Cooldown;
            self.deadline = Some(session.next_send);
            return None;
        }
        if session.queries >= session.config.max_queries
            || session
                .candidates
                .values()
                .all(|candidate| candidate.visited && candidate.kind == RequestKind::Sample)
        {
            if !session.in_flight.is_empty() {
                self.status.pause = PauseReason::Transactions;
                return None;
            }
            session.reset_round(now);
            self.deadline = Some(session.next_send);
            self.status.pause = PauseReason::Cooldown;
            return None;
        }
        // 只在需要腾出记录槽位时清理到期项，平时保留失败次数用于指数退避。
        if self.ids.len() >= session.config.cooldown_capacity {
            self.ids
                .retain(|id, value| value.until > now || session.in_flight.contains_key(id));
        }
        if self.ips.len() >= session.config.cooldown_capacity {
            self.ips.retain(|ip, until| {
                *until > now || session.in_flight.values().any(|busy| busy == ip)
            });
        }
        let CandidateSelection {
            candidate,
            mut earliest,
            tracking_full,
        } = session.select_candidate(&self.ids, &self.ips, now);
        let Some(candidate) = candidate else {
            // 一轮有部分节点完成、其余都在冷却时，不必等最远期限才换 target。
            // 清掉 visited 后不会重复走这条分支，下一轮直接等待真实的冷却期限。
            if session.in_flight.is_empty()
                && session
                    .candidates
                    .values()
                    .any(|candidate| candidate.visited)
            {
                session.reset_round(now);
                self.deadline = Some(session.next_send);
                self.status.pause = PauseReason::Cooldown;
                return None;
            }
            self.status.pause = if tracking_full {
                PauseReason::TrackingCapacity
            } else {
                PauseReason::Cooldown
            };
            if tracking_full {
                earliest = earliest
                    .into_iter()
                    .chain(
                        self.ids
                            .values()
                            .map(|value| value.until)
                            .chain(self.ips.values().copied())
                            .filter(|at| *at > now),
                    )
                    .min();
            }
            self.deadline = earliest;
            return None;
        };
        Some(self.register_request(candidate, now))
    }

    /// 选中请求后同时登记候选、在途 IP 和发送间隔，并把输出许可移入请求。
    fn register_request(&mut self, candidate: Candidate, now: Instant) -> Request {
        let session = self.session.as_mut().expect("候选只能来自正在运行的会话");
        let tracked = session
            .candidates
            .get_mut(&candidate.node.id)
            .expect("候选来自当前会话，登记前没有删除");
        tracked.visited = true;
        tracked.kind = RequestKind::Sample;
        session
            .in_flight
            .insert(candidate.node.id, candidate.node.address.ip());
        session.queries += 1;
        session.next_send = now + session.config.send_spacing;
        self.ids.entry(candidate.node.id).or_insert(Cooldown {
            until: now,
            failures: 0,
        });
        self.ips.entry(candidate.node.address.ip()).or_insert(now);
        self.deadline = Some(session.next_send);
        self.status.pause = PauseReason::Ready;
        self.status.in_flight = session.in_flight.len();
        let observer = session.observer.child(crate::observation::Kind::Sampling);
        observer.emit(crate::observation::Kind::Sampling,"candidate","selected",||serde_json::json!({"node_id":crate::observation::hex(&candidate.node.id.0),"peer":candidate.node.address.to_string(),"target":crate::observation::hex(&session.target.0),"query_index":session.queries}));
        Request {
            observer,
            generation: self.generation,
            lease: None,
            node: candidate.node,
            target: session.target,
            kind: candidate.kind,
            permit: session.ready_permit.take(),
        }
    }
}
