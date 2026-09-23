//! 方法级响应字段校验；只有通过这里的结果才允许写入路由。

use super::super::{
    api::{DiscoveredNode, FindNodeResponse, QueryError},
    runtime::DhtDispatcher,
};
use crate::dht::{krpc::ResponseArgs, routing::AddressFamily};
use std::net::SocketAddr;

pub(super) enum ValidatedResponse {
    Basic,
    Peers(super::super::fetch::GetPeersResponse),
    Nodes(FindNodeResponse),
    Samples(super::super::sampler::SampleResponse),
}

impl ValidatedResponse {
    pub(super) fn nodes(self) -> FindNodeResponse {
        match self {
            Self::Nodes(nodes) => nodes,
            _ => unreachable!("节点响应已完成方法校验"),
        }
    }
}

impl DhtDispatcher {
    pub(super) fn decode_find_node_response(
        &self,
        response: &ResponseArgs,
    ) -> Result<FindNodeResponse, QueryError> {
        let responder_id = response.id;
        let nodes = match self.routing.address_family() {
            AddressFamily::Ipv4 => response
                .nodes
                .as_ref()
                .ok_or(QueryError::InvalidResponse(
                    "IPv4 find_node 响应缺少 nodes 字段",
                ))?
                .0
                .iter()
                .map(|node| DiscoveredNode {
                    id: node.id,
                    address: SocketAddr::V4(node.address),
                })
                .collect(),
            AddressFamily::Ipv6 => response
                .nodes6
                .as_ref()
                .ok_or(QueryError::InvalidResponse(
                    "IPv6 find_node 响应缺少 nodes6 字段",
                ))?
                .0
                .iter()
                .map(|node| DiscoveredNode {
                    id: node.id,
                    address: SocketAddr::V6(node.address),
                })
                .collect(),
        };
        Ok(FindNodeResponse {
            responder_id,
            nodes,
        })
    }
}
