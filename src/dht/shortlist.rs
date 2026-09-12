//! 两种迭代查找共用的有效近邻规则；失败项不占 shortlist 名额。
use crate::{dht::routing::xor_distance, krpc::NodeId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CandidateState {
    Unqueried,
    InFlight,
    Succeeded,
    Failed,
}

pub(crate) fn closest_valid(
    candidates: impl Iterator<Item = (NodeId, CandidateState)>,
    target: NodeId,
    limit: usize,
) -> Vec<NodeId> {
    let mut nodes: Vec<_> = candidates
        .filter(|(_, state)| *state != CandidateState::Failed)
        .collect();
    nodes.sort_by_key(|(id, _)| xor_distance(&id.0, &target.0));
    nodes.into_iter().take(limit).map(|(id, _)| id).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn failed_neighbors_do_not_hide_live_reserve() {
        let candidates: Vec<_> = (1..=9)
            .map(|n| {
                (
                    NodeId([n; 20]),
                    if n <= 7 {
                        CandidateState::Failed
                    } else {
                        CandidateState::Unqueried
                    },
                )
            })
            .collect();
        assert_eq!(
            closest_valid(candidates.into_iter(), NodeId([0; 20]), 8),
            vec![NodeId([8; 20]), NodeId([9; 20])]
        );
    }
}
