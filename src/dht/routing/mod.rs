//! DHT routing table。
//!
//! routing table 按 BEP 5 从一个覆盖完整 Node ID 空间的 bucket 开始。只有包含本地
//! Node ID 的满 bucket 才会继续二分，因此远距离节点不会无上限地占用内存。
//! 本模块只保存状态并给出决策，真正的 ping 和 find_node 由 dispatcher 负责。
//!
//! response 处理器验证并更新联系人；maintenance 依据本表的距离、状态和期限安排探测与刷新。

use crate::dht::krpc::NodeId;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

/// BEP 5 规定每个 bucket 最多保存 8 个节点。
pub(super) const BUCKET_SIZE: usize = 8;
/// 节点在最近 15 分钟内有有效活动时被视为 good。
pub(super) const GOOD_FOR: Duration = Duration::from_secs(15 * 60);
/// BEP 5 要求 bucket 连续 15 分钟没有变化时执行刷新查找。
pub(super) const DEFAULT_BUCKET_REFRESH_AFTER: Duration = Duration::from_secs(15 * 60);
/// 连续两次查询失败后，节点才会变成 bad。
const MAX_CONSECUTIVE_FAILURES: u8 = 2;

/// 当前 routing table 对应的网络地址族。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AddressFamily {
    Ipv4,
    Ipv6,
}

impl AddressFamily {
    pub(crate) fn accepts(self, address: SocketAddr) -> bool {
        matches!(
            (self, address),
            (Self::Ipv4, SocketAddr::V4(_)) | (Self::Ipv6, SocketAddr::V6(_))
        )
    }
}

/// routing table 中节点当前的可信状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NodeStatus {
    Good,
    Questionable,
    Bad,
}

/// routing table 中保存的一条、已经响应过本节点的联系方式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct NodeContact {
    pub(super) id: NodeId,
    pub(super) address: SocketAddr,
    last_response: Instant,
    last_query: Option<Instant>,
    consecutive_failures: u8,
}

impl NodeContact {
    fn verified(id: NodeId, address: SocketAddr, now: Instant) -> Self {
        Self {
            id,
            address,
            last_response: now,
            last_query: None,
            consecutive_failures: 0,
        }
    }

    /// 构造只存在于迭代查找候选集合中的联系人；它不会因此进入 routing table。
    pub(super) fn for_lookup(id: NodeId, address: SocketAddr, now: Instant) -> Self {
        Self::verified(id, address, now)
    }

    pub(super) fn status(&self, now: Instant) -> NodeStatus {
        if self.consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
            return NodeStatus::Bad;
        }
        let responded_recently = now.saturating_duration_since(self.last_response) <= GOOD_FOR;
        let queried_recently = self
            .last_query
            .is_some_and(|seen| now.saturating_duration_since(seen) <= GOOD_FOR);
        if responded_recently || queried_recently {
            NodeStatus::Good
        } else {
            NodeStatus::Questionable
        }
    }

    fn last_activity(&self) -> Instant {
        self.last_query
            .map_or(self.last_response, |query| self.last_response.max(query))
    }

    #[cfg(test)]
    fn consecutive_failures(&self) -> u8 {
        self.consecutive_failures
    }
}

/// 一个动态 bucket 的稳定标识：前 `prefix_len` 位必须与 `prefix` 相同。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct BucketPrefix {
    prefix: NodeId,
    prefix_len: u8,
}

impl BucketPrefix {
    fn root() -> Self {
        Self {
            prefix: NodeId([0; 20]),
            prefix_len: 0,
        }
    }

    fn contains(self, id: NodeId) -> bool {
        (0..usize::from(self.prefix_len))
            .all(|bit| bit_value(self.prefix, bit) == bit_value(id, bit))
    }

    fn split(self) -> (Self, Self) {
        debug_assert!(self.prefix_len < 160);
        let mut right = self.prefix;
        set_bit(&mut right, usize::from(self.prefix_len), true);
        (
            Self {
                prefix: self.prefix,
                prefix_len: self.prefix_len + 1,
            },
            Self {
                prefix: right,
                prefix_len: self.prefix_len + 1,
            },
        )
    }

    fn random_target(self, random: [u8; 20]) -> NodeId {
        let mut target = NodeId(random);
        for bit in 0..usize::from(self.prefix_len) {
            set_bit(&mut target, bit, bit_value(self.prefix, bit));
        }
        target
    }
}

#[derive(Debug)]
struct Bucket {
    range: BucketPrefix,
    nodes: Vec<NodeContact>,
    last_changed: Instant,
    last_refresh: Option<Instant>,
}

impl Bucket {
    fn new(range: BucketPrefix, now: Instant) -> Self {
        Self {
            range,
            nodes: Vec::with_capacity(BUCKET_SIZE),
            last_changed: now,
            last_refresh: None,
        }
    }

    fn maintenance_base(&self) -> Instant {
        self.last_refresh
            .map_or(self.last_changed, |refresh| self.last_changed.max(refresh))
    }
}

/// 收到一个经过验证的响应后，routing table 作出的插入决策。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum InsertOutcome {
    Inserted,
    Updated,
    ReplacedBad {
        removed: NodeContact,
    },
    /// bucket 中还有需要检查的旧节点。dispatcher 应按顺序逐个探测。
    ProbeRequired {
        incumbents: Vec<NodeContact>,
        candidate: NodeContact,
    },
    RejectedFull,
    IgnoredSelf,
    IgnoredWrongAddressFamily,
}

/// 收到远端查询时作出的决策。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum QueryObservation {
    Updated,
    VerificationRequired { id: NodeId, address: SocketAddr },
    IgnoredReadOnly,
    IgnoredSelf,
    IgnoredWrongAddressFamily,
}

/// 一个本地 DHT 节点的动态 routing table。
#[derive(Debug)]
pub(crate) struct RoutingTable {
    local_id: NodeId,
    address_family: AddressFamily,
    buckets: Vec<Bucket>,
}

impl RoutingTable {
    /// 导出联系人时换算历史 UTC 时间，不把旧节点伪装成刚刚响应。
    pub(super) fn snapshot(
        &self,
        now: Instant,
        wall: std::time::SystemTime,
    ) -> Result<Vec<crate::dht::persistence::SavedContact>, crate::storage::StorageError> {
        self.buckets
            .iter()
            .flat_map(|b| &b.nodes)
            .filter(|n| n.status(now) != NodeStatus::Bad)
            .map(|n| {
                let age = now
                    .checked_duration_since(n.last_response)
                    .ok_or(crate::storage::StorageError::Invalid("路由响应时间在未来"))?;
                let at = wall
                    .checked_sub(age)
                    .ok_or(crate::storage::StorageError::Invalid("路由时间换算溢出"))?;
                Ok(crate::dht::persistence::SavedContact {
                    id: n.id,
                    address: n.address,
                    responded_at: crate::clock::unix_millis(at)?,
                })
            })
            .collect()
    }
    pub(crate) fn new(local_id: NodeId, address_family: AddressFamily, now: Instant) -> Self {
        Self {
            local_id,
            address_family,
            buckets: vec![Bucket::new(BucketPrefix::root(), now)],
        }
    }

    pub(super) fn local_id(&self) -> NodeId {
        self.local_id
    }
    pub(super) fn address_family(&self) -> AddressFamily {
        self.address_family
    }
    pub(super) fn is_empty(&self) -> bool {
        self.buckets.iter().all(|bucket| bucket.nodes.is_empty())
    }

    /// 已入表节点的验证地址比其他节点新声明的地址更可信。
    pub(super) fn contact(&self, id: NodeId) -> Option<NodeContact> {
        let index = self.bucket_index(id);
        self.buckets[index]
            .nodes
            .iter()
            .find(|contact| contact.id == id)
            .copied()
    }
    #[cfg(test)]
    fn len(&self) -> usize {
        self.buckets.iter().map(|bucket| bucket.nodes.len()).sum()
    }
    #[cfg(test)]
    fn bucket_count(&self) -> usize {
        self.buckets.len()
    }
    #[cfg(test)]
    fn bucket_len(&self, index: usize) -> Option<usize> {
        self.buckets.get(index).map(|bucket| bucket.nodes.len())
    }

    pub(super) fn observe_response(
        &mut self,
        id: NodeId,
        address: SocketAddr,
        now: Instant,
    ) -> InsertOutcome {
        if id == self.local_id {
            return InsertOutcome::IgnoredSelf;
        }
        if !self.address_family.accepts(address) {
            return InsertOutcome::IgnoredWrongAddressFamily;
        }
        let index = self.bucket_index(id);
        if let Some(contact) = self.buckets[index]
            .nodes
            .iter_mut()
            .find(|contact| contact.id == id)
        {
            contact.address = address;
            contact.last_response = now;
            contact.consecutive_failures = 0;
            self.buckets[index].last_changed = now;
            return InsertOutcome::Updated;
        }
        self.insert_verified(NodeContact::verified(id, address, now), now)
    }

    /// 探测完旧节点后重新尝试插入先前已经验证的候选者。
    pub(super) fn reconsider_candidate(
        &mut self,
        candidate: NodeContact,
        now: Instant,
    ) -> InsertOutcome {
        self.insert_verified(candidate, now)
    }

    fn insert_verified(&mut self, candidate: NodeContact, now: Instant) -> InsertOutcome {
        loop {
            let index = self.bucket_index(candidate.id);
            let bucket = &mut self.buckets[index];
            if bucket.nodes.len() < BUCKET_SIZE {
                bucket.nodes.push(candidate);
                bucket.last_changed = now;
                return InsertOutcome::Inserted;
            }
            if let Some(position) = bucket
                .nodes
                .iter()
                .position(|contact| contact.status(now) == NodeStatus::Bad)
            {
                let removed = std::mem::replace(&mut bucket.nodes[position], candidate);
                bucket.last_changed = now;
                return InsertOutcome::ReplacedBad { removed };
            }
            let mut incumbents: Vec<_> = bucket
                .nodes
                .iter()
                .filter(|contact| contact.status(now) == NodeStatus::Questionable)
                .cloned()
                .collect();
            incumbents.sort_by_key(NodeContact::last_activity);
            if !incumbents.is_empty() {
                return InsertOutcome::ProbeRequired {
                    incumbents,
                    candidate,
                };
            }
            if bucket.range.contains(self.local_id) && bucket.range.prefix_len < 160 {
                self.split_bucket(index, now);
                continue;
            }
            return InsertOutcome::RejectedFull;
        }
    }

    fn split_bucket(&mut self, index: usize, now: Instant) {
        let old = self.buckets.remove(index);
        let (left_range, right_range) = old.range.split();
        let mut left = Bucket::new(left_range, now);
        let mut right = Bucket::new(right_range, now);
        for contact in old.nodes {
            if left_range.contains(contact.id) {
                left.nodes.push(contact);
            } else {
                right.nodes.push(contact);
            }
        }
        self.buckets.insert(index, right);
        self.buckets.insert(index, left);
    }

    pub(super) fn observe_query(
        &mut self,
        id: NodeId,
        address: SocketAddr,
        read_only: bool,
        now: Instant,
    ) -> QueryObservation {
        if read_only {
            return QueryObservation::IgnoredReadOnly;
        }
        if id == self.local_id {
            return QueryObservation::IgnoredSelf;
        }
        if !self.address_family.accepts(address) {
            return QueryObservation::IgnoredWrongAddressFamily;
        }
        let index = self.bucket_index(id);
        if let Some(contact) = self.buckets[index]
            .nodes
            .iter_mut()
            .find(|contact| contact.id == id && contact.address == address)
        {
            contact.last_query = Some(now);
            return QueryObservation::Updated;
        }
        QueryObservation::VerificationRequired { id, address }
    }

    pub(super) fn record_failure(&mut self, id: NodeId, now: Instant) -> Option<NodeStatus> {
        let index = self.bucket_index(id);
        let contact = self.buckets[index]
            .nodes
            .iter_mut()
            .find(|contact| contact.id == id)?;
        contact.consecutive_failures = contact.consecutive_failures.saturating_add(1);
        Some(contact.status(now))
    }

    pub(super) fn closest_good(
        &self,
        target: &[u8; 20],
        limit: usize,
        now: Instant,
    ) -> Vec<NodeContact> {
        self.closest_matching(target, limit, |contact| {
            contact.status(now) == NodeStatus::Good
        })
    }

    /// 维护查找可以再次联系 questionable 节点，但绝不会使用 bad 节点。
    pub(super) fn closest_usable(
        &self,
        target: NodeId,
        limit: usize,
        now: Instant,
    ) -> Vec<NodeContact> {
        let mut contacts = self.closest_matching(&target.0, usize::MAX, |contact| {
            contact.status(now) != NodeStatus::Bad
        });
        contacts.sort_by_key(|contact| {
            (
                contact.status(now) != NodeStatus::Good,
                xor_distance(&contact.id.0, &target.0),
            )
        });
        contacts.truncate(limit);
        contacts
    }

    fn closest_matching(
        &self,
        target: &[u8; 20],
        limit: usize,
        include: impl Fn(&NodeContact) -> bool,
    ) -> Vec<NodeContact> {
        let mut contacts: Vec<_> = self
            .buckets
            .iter()
            .flat_map(|bucket| &bucket.nodes)
            .filter(|contact| include(contact))
            .cloned()
            .collect();
        contacts.sort_by_key(|contact| xor_distance(&contact.id.0, target));
        contacts.truncate(limit);
        contacts
    }

    pub(super) fn next_refresh_deadline(&self, refresh_after: Duration) -> Option<Instant> {
        if self.is_empty() {
            return None;
        }
        self.buckets
            .iter()
            .map(|bucket| bucket.maintenance_base() + refresh_after)
            .min()
    }

    /// 为最早到期的 bucket 生成一个位于其范围内的随机查询目标。
    pub(super) fn stale_refresh_target(
        &self,
        now: Instant,
        refresh_after: Duration,
        random: [u8; 20],
    ) -> Option<NodeId> {
        if self.is_empty() {
            return None;
        }
        self.buckets
            .iter()
            .filter(|bucket| now >= bucket.maintenance_base() + refresh_after)
            .min_by_key(|bucket| bucket.maintenance_base())
            .map(|bucket| bucket.range.random_target(random))
    }

    /// 查找完成时按目标重新定位 bucket，避免查找期间发生二分导致标记丢失。
    pub(super) fn mark_refreshed(&mut self, target: NodeId, now: Instant) {
        let index = self.bucket_index(target);
        self.buckets[index].last_refresh = Some(now);
    }

    fn bucket_index(&self, id: NodeId) -> usize {
        self.buckets
            .iter()
            .position(|bucket| bucket.range.contains(id))
            .expect("动态 bucket 必须完整覆盖 160 位 Node ID 空间")
    }
}

pub(crate) fn xor_distance(left: &[u8; 20], right: &[u8; 20]) -> [u8; 20] {
    std::array::from_fn(|index| left[index] ^ right[index])
}

fn bit_value(id: NodeId, bit: usize) -> bool {
    let byte = bit / 8;
    let mask = 1 << (7 - bit % 8);
    id.0[byte] & mask != 0
}

fn set_bit(id: &mut NodeId, bit: usize, value: bool) {
    let byte = bit / 8;
    let mask = 1 << (7 - bit % 8);
    if value {
        id.0[byte] |= mask;
    } else {
        id.0[byte] &= !mask;
    }
}

#[cfg(test)]
mod tests;

impl RoutingTable {
    /// 当前路由桶和联系人；不把恢复候选或未经验证的响应节点描述为已可达。
    pub(crate) fn inspection(&self, now: Instant) -> serde_json::Value {
        let mut buckets = Vec::new();
        let mut contacts = Vec::new();
        for bucket in &self.buckets {
            let id = format!(
                "{}/{}",
                crate::observation::hex(&bucket.range.prefix.0),
                bucket.range.prefix_len
            );
            buckets.push(
                serde_json::json!({"id":id,"count":bucket.nodes.len(),"capacity":BUCKET_SIZE}),
            );
            for node in &bucket.nodes {
                contacts.push(serde_json::json!({
                    "bucket": id,
                    "node_id": crate::observation::hex(&node.id.0),
                    "address": node.address.to_string(),
                    "status": match node.status(now) {
                        NodeStatus::Good => "good",
                        NodeStatus::Questionable => "questionable",
                        NodeStatus::Bad => "bad",
                    },
                    "last_response_age_ms": now
                        .saturating_duration_since(node.last_response)
                        .as_millis() as u64,
                }));
            }
        }
        serde_json::json!({"observed_at_ms":crate::observation::wall_ms(),"buckets":buckets,"contacts":contacts})
    }
}
