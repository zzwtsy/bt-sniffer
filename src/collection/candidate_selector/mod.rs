//! 单轮 metadata 采集的同步候选顺序与已选集合。
//! worker 负责地址校验、硬上限与网络执行；这里不缓存 DHT 结果或判断查找结束。
use super::diagnostics::Source;
use std::{
    collections::{HashSet, VecDeque},
    net::SocketAddr,
};

/// 尚待 worker 校验和登记的候选，来源随本次选取保留。
pub(super) struct PeerCandidate {
    pub(super) address: SocketAddr,
    pub(super) source: Source,
}

/// 每次 run_job 独有；去重键是完整地址，同 IP 不同端口仍可分别选择。
pub(super) struct CandidateSelector {
    first_hints: VecDeque<SocketAddr>,
    remaining_hints: VecDeque<SocketAddr>,
    selected: HashSet<SocketAddr>,
}

impl CandidateSelector {
    /// 接收 worker 已过滤的提示，保序；前两个条目即使重复也占优先槽。
    pub(super) fn new(eligible_hints: impl IntoIterator<Item = SocketAddr>) -> Self {
        let mut remaining_hints: VecDeque<_> = eligible_hints.into_iter().collect();
        let first_hints = remaining_hints
            .drain(..remaining_hints.len().min(2))
            .collect();
        Self {
            first_hints,
            remaining_hints,
            selected: HashSet::new(),
        }
    }

    /// 优先提示 → 即时 DHT → 剩余提示；本方法不登记候选。
    /// poll_dht 必须非阻塞，仅在优先提示耗尽时调用至多一次；None 不表示查找结束。
    pub(super) fn next_ready(
        &mut self,
        poll_dht: impl FnOnce() -> Option<SocketAddr>,
    ) -> Option<PeerCandidate> {
        self.first_hints
            .pop_front()
            .map(|address| PeerCandidate {
                address,
                source: Source::Announce,
            })
            .or_else(|| {
                poll_dht().map(|address| PeerCandidate {
                    address,
                    source: Source::Dht,
                })
            })
            .or_else(|| {
                self.remaining_hints
                    .pop_front()
                    .map(|address| PeerCandidate {
                        address,
                        source: Source::Announce,
                    })
            })
    }

    /// 仅在 worker 通过地址策略和地址族校验后调用；首次登记为 true，重复为 false。
    pub(super) fn select_once(&mut self, address: SocketAddr) -> bool {
        self.selected.insert(address)
    }

    /// 已选的不同合法地址数，登记先于 TCP 许可等待，不代表已建连或下载成功。
    pub(super) fn selected_count(&self) -> usize {
        self.selected.len()
    }
}

#[cfg(test)]
mod tests;
