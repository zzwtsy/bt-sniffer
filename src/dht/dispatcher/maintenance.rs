//! routing maintenance 的迭代查找状态机和自动地址安全策略。
//!
//! 由 dispatcher 在每轮事件后推进；候选联系人先查询验证，再进入路由表。

use crate::dht::shortlist::CandidateState;
use crate::dht::shortlist::closest_valid;
use std::collections::{HashMap, HashSet};
#[cfg(test)]
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use super::api::{MaintenanceConfig, RemoteNode};
use super::runtime::{DhtDispatcher, PendingPurpose};
use crate::dht::krpc::NodeId;
use crate::dht::krpc::QueryMethod;
use crate::dht::routing::NodeContact;
use crate::dht::routing::RoutingTable;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LookupKind {
    Startup,
    Refresh,
}

#[derive(Debug, Clone, Copy)]
struct LookupCandidate {
    node: NodeContact,
    state: CandidateState,
    verified: bool,
}

/// 一次迭代 find_node 的全部短期状态。
#[derive(Debug)]
pub(super) struct IterativeLookup {
    pub(super) kind: LookupKind,
    pub(super) target: NodeId,
    candidates: HashMap<NodeId, LookupCandidate>,
    attempted_addresses: HashSet<std::net::SocketAddr>,
    in_flight: usize,
    queries_started: usize,
    successful_responses: usize,
}

impl IterativeLookup {
    fn new(kind: LookupKind, target: NodeId, seeds: Vec<NodeContact>) -> Self {
        let candidates = seeds
            .into_iter()
            .map(|node| {
                (
                    node.id,
                    LookupCandidate {
                        node,
                        state: CandidateState::Unqueried,
                        verified: true,
                    },
                )
            })
            .collect();
        Self {
            kind,
            target,
            candidates,
            attempted_addresses: HashSet::new(),
            in_flight: 0,
            queries_started: 0,
            successful_responses: 0,
        }
    }

    fn take_next(&mut self, config: MaintenanceConfig, capacity: usize) -> Vec<NodeContact> {
        let available_parallelism = config.lookup_parallelism.saturating_sub(self.in_flight);
        let available_queries = config.max_queries.saturating_sub(self.queries_started);
        let count = capacity.min(available_parallelism).min(available_queries);
        if count == 0 {
            return Vec::new();
        }

        let ids = closest_valid(
            self.candidates.iter().map(|(id, c)| (*id, c.state)),
            self.target,
            config.shortlist_size,
        );
        let ids: Vec<_> = ids
            .into_iter()
            .filter(|id| self.candidates[id].state == CandidateState::Unqueried)
            .take(count)
            .collect();

        ids.into_iter()
            .filter_map(|id| {
                let candidate = self.candidates.get_mut(&id)?;
                if !self.attempted_addresses.insert(candidate.node.address) {
                    candidate.state = CandidateState::Failed;
                    return None;
                }
                candidate.state = CandidateState::InFlight;
                self.in_flight += 1;
                self.queries_started += 1;
                Some(candidate.node)
            })
            .collect()
    }

    pub(super) fn complete_success(
        &mut self,
        id: NodeId,
        discovered: Vec<(NodeContact, bool)>,
        local_id: NodeId,
    ) {
        self.finish_candidate(id, true);
        for (node, verified) in discovered {
            if node.id == local_id {
                continue;
            }
            self.candidates
                .entry(node.id)
                .and_modify(|existing| {
                    if verified && !existing.verified && existing.state == CandidateState::Unqueried
                    {
                        existing.node = node;
                        existing.verified = true;
                    }
                })
                .or_insert(LookupCandidate {
                    node,
                    state: CandidateState::Unqueried,
                    verified,
                });
        }
        // 已尝试项保留用于去重，其余只保存最近候选。
        if self.candidates.len() > 256 {
            let mut nodes: Vec<_> = self
                .candidates
                .iter()
                .map(|(id, c)| (*id, c.state))
                .collect();
            nodes.sort_by_key(|(id, state)| {
                (
                    *state == CandidateState::Unqueried,
                    crate::dht::routing::xor_distance(&id.0, &self.target.0),
                )
            });
            let keep: HashSet<_> = nodes.into_iter().take(256).map(|(id, _)| id).collect();
            self.candidates.retain(|id, _| keep.contains(id));
        }
    }

    /// 本地未发送：撤销本次尝试，保留候选资格。
    pub(super) fn defer(&mut self, id: NodeId) {
        if let Some(candidate) = self.candidates.get_mut(&id)
            && candidate.state == CandidateState::InFlight
        {
            candidate.state = CandidateState::Unqueried;
            self.in_flight -= 1;
            self.queries_started -= 1;
            self.attempted_addresses.remove(&candidate.node.address);
        }
    }

    pub(super) fn complete_failure(&mut self, id: NodeId) {
        self.finish_candidate(id, false);
    }

    fn finish_candidate(&mut self, id: NodeId, success: bool) {
        if let Some(candidate) = self.candidates.get_mut(&id)
            && candidate.state == CandidateState::InFlight
        {
            candidate.state = if success {
                CandidateState::Succeeded
            } else {
                CandidateState::Failed
            };
            self.in_flight = self.in_flight.saturating_sub(1);
            if success {
                self.successful_responses += 1;
            }
        }
    }

    fn is_complete(&self, config: MaintenanceConfig) -> bool {
        if self.in_flight != 0 {
            return false;
        }
        if self.queries_started >= config.max_queries {
            return true;
        }
        closest_valid(
            self.candidates.iter().map(|(id, c)| (*id, c.state)),
            self.target,
            config.shortlist_size,
        )
        .into_iter()
        .all(|id| self.candidates[&id].state == CandidateState::Succeeded)
    }

    fn succeeded(&self) -> bool {
        self.successful_responses > 0
    }
}

impl DhtDispatcher {
    /// 推进一次维护状态机：开始到期查找、补足并发槽位或完成本轮查找。
    pub(super) async fn advance_maintenance(&mut self, now: Instant) {
        self.maintenance.begin_if_due(&self.routing, now);
        self.maintenance.finish_if_complete(&mut self.routing, now);
        let occupied = self.occupied();
        let Some(lookup) = self.maintenance.lookup.as_mut() else {
            return;
        };

        // 自动维护永远不会占用为用户请求预留的 transaction 名额。
        let maintenance_limit = self
            .transactions
            .max_pending()
            .saturating_sub(self.maintenance.config.reserved_user_transactions.max(1));
        let capacity = maintenance_limit.saturating_sub(occupied);
        let target = lookup.target;
        let nodes = lookup.take_next(self.maintenance.config, capacity);
        for node in nodes {
            if !self.automatic_policy.accepts(node.address) {
                self.maintenance
                    .lookup
                    .as_mut()
                    .expect("维护查询在候选处理期间保持活动")
                    .complete_failure(node.id);
                continue;
            }
            self.start_query(
                RemoteNode {
                    address: node.address,
                    expected_id: Some(node.id),
                },
                QueryMethod::FindNode,
                Some(target),
                PendingPurpose::MaintenanceLookup { node_id: node.id },
                now,
            )
            .await;
        }
        self.maintenance.finish_if_complete(&mut self.routing, now);
    }
}

/// dispatcher 内部的维护调度状态。它不持有 routing table，避免产生两份事实来源。
#[derive(Debug)]
pub(super) struct MaintenanceState {
    pub(super) config: MaintenanceConfig,
    pub(super) lookup: Option<IterativeLookup>,
    startup_complete: bool,
    not_before: Instant,
    retry_delay: Duration,
}

impl MaintenanceState {
    pub(super) fn new(config: MaintenanceConfig, now: Instant) -> Self {
        Self {
            config,
            lookup: None,
            startup_complete: false,
            not_before: now,
            retry_delay: config.retry_initial,
        }
    }

    pub(super) fn deadline(&self, routing: &RoutingTable) -> Option<Instant> {
        if !self.config.enabled || self.lookup.is_some() || routing.is_empty() {
            return None;
        }
        if !self.startup_complete {
            return Some(self.not_before);
        }
        routing
            .next_refresh_deadline(self.config.refresh_after)
            .map(|deadline| deadline.max(self.not_before))
    }

    pub(super) fn begin_if_due(&mut self, routing: &RoutingTable, now: Instant) {
        if self.lookup.is_some() || self.deadline(routing).is_none_or(|deadline| now < deadline) {
            return;
        }
        let (kind, target) = if !self.startup_complete {
            (LookupKind::Startup, routing.local_id())
        } else {
            let Some(target) =
                routing.stale_refresh_target(now, self.config.refresh_after, rand::random())
            else {
                return;
            };
            (LookupKind::Refresh, target)
        };
        let seeds = routing.closest_usable(target, 256, now);
        if seeds.is_empty() {
            self.schedule_retry(now);
            return;
        }
        self.lookup = Some(IterativeLookup::new(kind, target, seeds));
    }

    pub(super) fn finish_if_complete(&mut self, routing: &mut RoutingTable, now: Instant) {
        let Some(lookup) = self.lookup.as_ref() else {
            return;
        };
        if !lookup.is_complete(self.config) {
            return;
        }
        let lookup = self.lookup.take().expect("上方已经确认维护查找存在");
        if lookup.succeeded() {
            if lookup.kind == LookupKind::Startup {
                self.startup_complete = true;
            } else {
                routing.mark_refreshed(lookup.target, now);
            }
            self.retry_delay = self.config.retry_initial;
            self.not_before = now + self.config.inter_lookup_delay;
        } else {
            self.schedule_retry(now);
        }
    }

    fn schedule_retry(&mut self, now: Instant) {
        self.not_before = now + self.retry_delay;
        self.retry_delay = self
            .retry_delay
            .saturating_mul(2)
            .min(self.config.retry_max);
    }

    pub(super) fn clear(&mut self) {
        self.lookup = None;
    }
}

/// 公网返回的地址不能驱动本节点访问内网、回环或特殊用途网段。
#[cfg(test)]
fn is_public_automatic_address(address: SocketAddr) -> bool {
    crate::address::AddressPolicy::PublicOnly.accepts(address)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn contact(id: u8, address: &str) -> NodeContact {
        NodeContact::for_lookup(NodeId([id; 20]), address.parse().unwrap(), Instant::now())
    }

    /// 自动地址只允许公网单播和非零端口，防止被远端诱导访问本机或内网。
    #[test]
    fn automatic_addresses_must_be_public_unicast() {
        assert!(is_public_automatic_address("8.8.8.8:6881".parse().unwrap()));
        assert!(!is_public_automatic_address(
            "127.0.0.1:6881".parse().unwrap()
        ));
        assert!(!is_public_automatic_address(
            "10.0.0.1:6881".parse().unwrap()
        ));
        assert!(!is_public_automatic_address(
            "198.51.100.1:6881".parse().unwrap()
        ));
        assert!(!is_public_automatic_address("8.8.8.8:0".parse().unwrap()));
        assert!(is_public_automatic_address(
            "[2606:4700:4700::1111]:6881".parse().unwrap()
        ));
        assert!(!is_public_automatic_address("[::1]:6881".parse().unwrap()));
        assert!(!is_public_automatic_address(
            "[2001:db8::1]:6881".parse().unwrap()
        ));
    }

    /// 每轮最多取 alpha 个最近节点；相同 Node ID 不会被重复查询。
    #[test]
    fn lookup_respects_parallelism_and_deduplicates_candidates() {
        let config = MaintenanceConfig::default();
        let mut lookup = IterativeLookup::new(
            LookupKind::Startup,
            NodeId([0; 20]),
            (1..=8)
                .map(|id| contact(id, &format!("8.8.8.{id}:6881")))
                .collect(),
        );
        let first = lookup.take_next(config, 10);
        assert_eq!(first.len(), 3);

        let duplicate = contact(4, "1.1.1.1:6881");
        lookup.complete_success(
            first[0].id,
            vec![(duplicate, false), (duplicate, false)],
            NodeId([0; 20]),
        );
        lookup.complete_failure(first[1].id);
        lookup.complete_failure(first[2].id);
        assert_eq!(lookup.candidates.len(), 8);
        assert_eq!(lookup.take_next(config, 10).len(), 3);
    }

    /// 达到查询硬上限后，即使还有更近的候选也必须停止，避免无限扩散流量。
    #[test]
    fn lookup_stops_at_query_limit() {
        let config = MaintenanceConfig {
            lookup_parallelism: 1,
            shortlist_size: 1,
            max_queries: 2,
            ..MaintenanceConfig::default()
        };
        let mut lookup = IterativeLookup::new(
            LookupKind::Refresh,
            NodeId([0; 20]),
            vec![contact(9, "8.8.8.8:6881")],
        );
        let first = lookup.take_next(config, 1).remove(0);
        lookup.complete_success(
            first.id,
            vec![(contact(8, "1.1.1.1:6881"), false)],
            NodeId([0; 20]),
        );
        let second = lookup.take_next(config, 1).remove(0);
        lookup.complete_success(
            second.id,
            vec![(contact(7, "9.9.9.9:6881"), false)],
            NodeId([0; 20]),
        );
        assert!(lookup.is_complete(config));
        assert!(lookup.take_next(config, 1).is_empty());
    }

    /// 七个失败近邻不能挡住第九个备用节点；重复返回失败 ID 不重试。
    #[test]
    fn seven_failed_neighbors_promote_reserve_for_both_families() {
        for v6 in [false, true] {
            let config = MaintenanceConfig::default();
            let nodes: Vec<_> = (1..=9)
                .map(|id| {
                    contact(
                        id,
                        &if v6 {
                            format!("[2606:4700::{id}]:6881")
                        } else {
                            format!("8.8.8.{id}:6881")
                        },
                    )
                })
                .collect();
            let mut lookup =
                IterativeLookup::new(LookupKind::Startup, NodeId([0; 20]), nodes.clone());
            while !lookup.is_complete(config) {
                let next = lookup.take_next(config, 3);
                assert!(!next.is_empty());
                for node in next {
                    if node.id.0[0] <= 7 {
                        lookup.complete_failure(node.id);
                    } else {
                        lookup.complete_success(
                            node.id,
                            nodes.iter().copied().map(|n| (n, false)).collect(),
                            NodeId([0; 20]),
                        );
                    }
                }
            }
            assert_eq!(lookup.queries_started, 9);
            assert_eq!(lookup.successful_responses, 2);
        }
    }

    /// routing table 已验证的地址应覆盖同一 Node ID 的未验证声明。
    #[test]
    fn verified_address_replaces_unqueried_claim() {
        let id = NodeId([5; 20]);
        let mut lookup = IterativeLookup::new(
            LookupKind::Startup,
            NodeId([0; 20]),
            vec![contact(9, "8.8.8.8:6881")],
        );
        let seed = lookup.take_next(MaintenanceConfig::default(), 1).remove(0);
        lookup.complete_success(
            seed.id,
            vec![(contact(5, "1.1.1.1:6881"), false)],
            NodeId([0; 20]),
        );
        lookup.complete_success(
            seed.id,
            vec![(contact(5, "9.9.9.9:6881"), true)],
            NodeId([0; 20]),
        );
        assert_eq!(
            lookup.candidates[&id].node.address,
            "9.9.9.9:6881".parse().unwrap()
        );
    }

    /// find_node 返回值只进入 shortlist；没有亲自收到它的响应前不能进入 routing table。
    #[test]
    fn discovered_candidate_is_not_inserted_before_its_response() {
        let now = Instant::now();
        let local_id = NodeId([0; 20]);
        let seed = contact(9, "8.8.8.8:6881");
        let discovered = contact(5, "1.1.1.1:6881");
        let mut routing =
            RoutingTable::new(local_id, crate::dht::routing::AddressFamily::Ipv4, now);
        routing.observe_response(seed.id, seed.address, now);
        let mut lookup = IterativeLookup::new(LookupKind::Startup, local_id, vec![seed]);
        let queried = lookup.take_next(MaintenanceConfig::default(), 1).remove(0);
        lookup.complete_success(queried.id, vec![(discovered, false)], local_id);

        assert!(lookup.candidates.contains_key(&discovered.id));
        assert!(routing.contact(discovered.id).is_none());
    }

    /// 连续失败应按 1、2、4 倍延长重试时间，并受最大退避时间限制。
    #[test]
    fn failed_lookup_uses_bounded_exponential_backoff() {
        let now = Instant::now();
        let mut routing = RoutingTable::new(
            NodeId([0; 20]),
            crate::dht::routing::AddressFamily::Ipv4,
            now,
        );
        routing.observe_response(NodeId([9; 20]), "8.8.8.8:6881".parse().unwrap(), now);
        let config = MaintenanceConfig {
            retry_initial: Duration::from_secs(10),
            retry_max: Duration::from_secs(20),
            ..MaintenanceConfig::default()
        };
        let mut maintenance = MaintenanceState::new(config, now);

        for expected_delay in [10, 20, 20] {
            maintenance.begin_if_due(&routing, now);
            let lookup = maintenance.lookup.as_mut().unwrap();
            let node = lookup.take_next(config, 1).remove(0);
            lookup.complete_failure(node.id);
            maintenance.finish_if_complete(&mut routing, now);
            assert_eq!(
                maintenance.deadline(&routing),
                Some(now + Duration::from_secs(expected_delay))
            );
            maintenance.not_before = now;
        }
    }
}
