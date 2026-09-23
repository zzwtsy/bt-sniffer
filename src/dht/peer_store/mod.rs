//! 有界 peer 地址缓存。只有合法 announce 能写入，查询不会延长保存时间。
//!
//! dispatcher 写入合法宣布并按期清理；get_peers 读取这些地址，metadata 下载不会反向写入。

use super::routing::AddressFamily;
use crate::info_hash::InfoHashV1;
use rand::{Rng, seq::IteratorRandom};
use std::{
    collections::{BTreeSet, HashMap},
    net::SocketAddr,
    time::{Duration, Instant},
};

pub(crate) use crate::address::AddressPolicy as PeerAddressPolicy;

/// 默认值是本程序的资源策略，不是 BEP 规定的固定数值。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PeerStoreConfig {
    /// 从最后一次合法宣布开始计时；读取不会延长这个期限。
    pub(crate) ttl: Duration,
    /// 最多同时保存多少个不同 info-hash。
    pub(crate) max_hashes: usize,
    /// 所有 info-hash 加起来最多保存多少条 peer 地址。
    pub(crate) max_peers: usize,
    /// 单个热门种子也不能独占整个缓存。
    pub(crate) max_peers_per_hash: usize,
    /// 单次随机抽样上限；最终响应可能因 UDP 字节预算再次缩短。
    pub(crate) response_peers: usize,
    /// 私有部署必须显式开启本地单播，公网默认不接收 loopback 宣布。
    pub(crate) address_policy: PeerAddressPolicy,
}
impl Default for PeerStoreConfig {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(1800),
            max_hashes: 10_000,
            max_peers: 50_000,
            max_peers_per_hash: 100,
            response_peers: 32,
            address_policy: PeerAddressPolicy::PublicOnly,
        }
    }
}

#[derive(Debug)]
pub(super) struct PeerStore {
    config: PeerStoreConfig,
    family: AddressFamily,
    peers: HashMap<InfoHashV1, HashMap<SocketAddr, Instant>>,
    /// 每个 peer 恰有一个索引项，续期会删除旧项。
    expiry: BTreeSet<(Instant, [u8; 20], SocketAddr)>,
    /// 每个 hash 恰有一个索引项，以组内最新宣布的过期时刻排序。
    hashes: BTreeSet<(Instant, [u8; 20])>,
}

impl PeerStore {
    /// 每个 hash 只出现一次；最新的 peer 也已到期时，整个 hash 就不再有效。
    fn active_infohashes(&self, now: Instant) -> impl Iterator<Item = InfoHashV1> + '_ {
        self.hashes
            .iter()
            .filter(move |(deadline, _)| *deadline > now)
            .map(|(_, hash)| InfoHashV1(*hash))
    }

    pub(super) fn active_infohash_count(&self, now: Instant) -> usize {
        self.active_infohashes(now).count()
    }

    /// 不依赖后台清理是否及时执行，也不因读取而延长 peer 的寿命。
    pub(super) fn contains_active_infohash(&self, hash: InfoHashV1, now: Instant) -> bool {
        self.peers
            .get(&hash)
            .is_some_and(|group| group.values().any(|deadline| *deadline > now))
    }

    /// 按 hash 等概率抽样，热门种子不会因为 peer 多就占据更多样本名额。
    pub(super) fn sample_infohashes(
        &self,
        limit: usize,
        now: Instant,
        rng: &mut impl Rng,
    ) -> Vec<InfoHashV1> {
        self.active_infohashes(now)
            .sample(rng, limit.min(self.config.max_hashes))
    }

    /// 配置在构造后保持不变，避免 TTL 改动破坏过期索引与宣布顺序的对应关系。
    pub(super) fn response_limit(&self) -> usize {
        self.config.response_peers
    }

    pub(super) fn new(
        config: PeerStoreConfig,
        family: AddressFamily,
        now: Instant,
    ) -> Result<Self, &'static str> {
        if config.ttl.is_zero()
            || now.checked_add(config.ttl).is_none()
            || config.max_hashes == 0
            || config.max_peers_per_hash == 0
            || config.response_peers == 0
            || config.max_hashes > config.max_peers
            || config.max_peers_per_hash > config.max_peers
            || config.response_peers > config.max_peers_per_hash
        {
            return Err("TTL、容量必须为正，hash/单组容量不能超过总容量，响应数不能超过单组容量");
        }
        Ok(Self {
            config,
            family,
            peers: HashMap::new(),
            expiry: BTreeSet::new(),
            hashes: BTreeSet::new(),
        })
    }

    pub(super) fn accepts(&self, address: SocketAddr) -> bool {
        self.family.accepts(address) && self.config.address_policy.accepts(address)
    }

    fn remove(&mut self, hash: InfoHashV1, address: SocketAddr) {
        let Some(group) = self.peers.get_mut(&hash) else {
            return;
        };
        self.hashes
            .remove(&(*group.values().max().expect("组非空"), hash.0));
        if let Some(expiry) = group.remove(&address) {
            self.expiry.remove(&(expiry, hash.0, address));
        }
        if let Some(latest) = group.values().max() {
            self.hashes.insert((*latest, hash.0));
        } else {
            self.peers.remove(&hash);
        }
    }

    /// 保存的是“收到合法宣布”，并不保证这个 peer 的下载端口真的可达。
    pub(super) fn announce(
        &mut self,
        hash: InfoHashV1,
        address: SocketAddr,
        now: Instant,
    ) -> Result<(), &'static str> {
        if !self.accepts(address) {
            return Err("peer 地址不符合当前地址策略");
        }
        let deadline = now.checked_add(self.config.ttl).ok_or("peer 期限溢出")?;
        // 优先回收过期项；单次工作量有界，其余由事件循环继续清理。
        self.expire(now, 256);
        if self
            .peers
            .get(&hash)
            .is_some_and(|group| group.contains_key(&address))
        {
            self.remove(hash, address);
        }
        if !self.peers.contains_key(&hash) && self.peers.len() >= self.config.max_hashes {
            let (_, oldest) = *self.hashes.first().expect("hash 索引非空");
            let old_hash = InfoHashV1(oldest);
            let addresses: Vec<_> = self.peers[&old_hash].keys().copied().collect();
            for address in addresses {
                self.remove(old_hash, address);
            }
        }
        if let Some(group) = self.peers.get(&hash)
            && group.len() >= self.config.max_peers_per_hash
        {
            let oldest = *group
                .iter()
                .min_by_key(|(addr, time)| (**time, **addr))
                .expect("组容量为正且该 peer 组已满")
                .0;
            self.remove(hash, oldest);
        }
        if self.expiry.len() >= self.config.max_peers {
            let (_, oldest_hash, oldest_address) = *self
                .expiry
                .first()
                .expect("全局 peer 容量为正且过期索引非空");
            self.remove(InfoHashV1(oldest_hash), oldest_address);
        }
        let group = self.peers.entry(hash).or_default();
        if let Some(latest) = group.values().max() {
            self.hashes.remove(&(*latest, hash.0));
        }
        group.insert(address, deadline);
        self.hashes.insert((
            *group.values().max().expect("刚插入的 peer 组至少包含一项"),
            hash.0,
        ));
        self.expiry.insert((deadline, hash.0, address));
        Ok(())
    }

    /// 查询只读有效记录；后台还没来得及删除的到期项也不会泄漏到响应里。
    pub(super) fn sample(
        &self,
        hash: InfoHashV1,
        limit: usize,
        now: Instant,
        rng: &mut impl Rng,
    ) -> Vec<SocketAddr> {
        self.peers
            .get(&hash)
            .into_iter()
            .flat_map(|group| group.iter())
            .filter(|(_, deadline)| **deadline > now)
            .map(|(address, _)| *address)
            .sample(rng, limit.min(self.config.response_peers))
    }

    /// 空缓存没有下一次期限，事件循环无需为它周期唤醒。
    pub(super) fn next_deadline(&self) -> Option<Instant> {
        self.expiry.first().map(|entry| entry.0)
    }

    /// 分批释放内存，给网络收发、用户命令和其他定时任务留下处理机会。
    pub(super) fn expire(&mut self, now: Instant, budget: usize) {
        for _ in 0..budget {
            let Some(&(deadline, hash, address)) = self.expiry.first() else {
                break;
            };
            if deadline > now {
                break;
            }
            self.remove(InfoHashV1(hash), address);
        }
    }
}

#[cfg(test)]
mod tests;
