//! BEP 51 响应字段校验；普通节点响应仍交给 dispatcher 的联系人处理路径。

use super::super::api::{DiscoveredNode, QueryError};
use crate::{dht::krpc::ResponseArgs, info_hash::SwarmKey};
use std::{collections::HashSet, time::Duration};

#[derive(Debug)]
pub(in crate::dht::dispatcher) struct SampleResponse {
    pub(super) nodes: Vec<DiscoveredNode>,
    pub(super) interval: Duration,
    pub(super) num: u64,
    pub(super) samples: Vec<SwarmKey>,
}

/// 缺少 samples 的普通节点响应不是采样成功，但可以复用其联系人。
pub(in crate::dht::dispatcher) fn decode_sample(
    response: &ResponseArgs,
    nodes: Vec<DiscoveredNode>,
    encoded_len: usize,
) -> Result<Option<SampleResponse>, QueryError> {
    if encoded_len > 1024 {
        return Err(QueryError::InvalidResponse("BEP 51 响应超过 1024 字节"));
    }
    let Some(samples) = &response.samples else {
        return Ok(None);
    };
    let interval = response
        .interval
        .filter(|n| *n <= 21600)
        .ok_or(QueryError::InvalidResponse("采样响应缺少合法 interval"))?;
    let num = response
        .num
        .ok_or(QueryError::InvalidResponse("采样响应缺少 num"))?;
    let mut seen = HashSet::new();
    let samples: Vec<_> = samples
        .0
        .iter()
        .copied()
        .filter(|hash| seen.insert(*hash))
        .collect();
    if num < samples.len() as u64 {
        return Err(QueryError::InvalidResponse("num 小于不同样本数量"));
    }
    Ok(Some(SampleResponse {
        nodes,
        interval: Duration::from_secs(interval.into()),
        num,
        samples,
    }))
}
