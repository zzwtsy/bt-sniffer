//! 出站查询的 Response/Error 匹配、结果交付和路由探测后续动作。
//!
//! 先匹配 transaction 的来源和预期身份，再交付结果；无关响应不能消耗其他查询的名额。

use std::net::SocketAddr;
use std::time::Instant;

use super::api::{DiscoveredNode, FindNodeResponse, PingResponse, QueryError, RemoteNode};
use super::runtime::{DhtDispatcher, PendingDispatch, PendingPurpose};
use crate::dht::krpc::NodeId;
use crate::dht::krpc::QueryMethod;
use crate::dht::krpc::ResponseArgs;
use crate::dht::routing::AddressFamily;
use crate::dht::routing::InsertOutcome;
use crate::dht::routing::NodeStatus;
use crate::dht::transaction::TransactionError;
use crate::dht::udp::ReceivedMessage;

/// 方法级校验完成后才产生的结果，避免为每个新方法增加一个可选参数。
enum ValidatedResponse {
    Basic,
    Peers(super::fetch::GetPeersResponse),
    Nodes(FindNodeResponse),
    Samples(super::sampler::SampleResponse),
}

impl ValidatedResponse {
    fn nodes(self) -> FindNodeResponse {
        match self {
            Self::Nodes(nodes) => nodes,
            _ => unreachable!("节点响应已完成方法校验"),
        }
    }
}

impl DhtDispatcher {
    fn observe_rejected_response(&self, error: &TransactionError, source: std::net::SocketAddr) {
        let observer = match error {
            TransactionError::SourceMismatch { id, .. } => self
                .pending
                .get(id)
                .map(|p| &p.observation.observer)
                .unwrap_or(&self.observer),
            _ => &self.observer,
        };
        observer.emit(
            crate::observation::Kind::Rpc,
            "response_match",
            match error {
                TransactionError::SourceMismatch { .. } => "source_mismatch",
                TransactionError::InvalidTransactionIdLength { .. } => "invalid_transaction_length",
                TransactionError::UnknownTransaction(_) => "unknown_transaction",
                _ => "rejected",
            },
            || serde_json::json!({"source":source.to_string()}),
        );
    }

    /// 匹配并处理一条成功响应消息。
    ///
    /// 校验顺序很重要：先匹配 transaction 与来源地址，再检查消息字段和 Node ID，
    /// 最后才允许更新 routing table。任一步失败都不能把响应者当作可信节点。
    pub(super) async fn handle_response(&mut self, received: ReceivedMessage, now: Instant) {
        let transaction_id = received.message.t.clone();
        let completed = match self
            .transactions
            .complete(&transaction_id, received.source, now)
        {
            Ok(completed) => completed,
            Err(TransactionError::Expired(transaction)) => {
                if let Some(pending) = self.pending.remove(&transaction.id) {
                    self.finish_network_failure(pending, QueryError::Timeout, now)
                        .await;
                }
                return;
            }
            // 来源不匹配、未知或错误长度的响应不会消费合法 transaction。
            Err(error) => {
                self.observe_rejected_response(&error, received.source);
                return;
            }
        };
        let Some(pending) = self.pending.remove(&completed.transaction.id) else {
            // 正常情况下两张 pending 表始终同步；防御性处理避免异常状态导致 panic。
            return;
        };

        // Response 只能携带 r，不能同时伪装成 Query 或 Error。
        if received.message.q.is_some()
            || received.message.a.is_some()
            || received.message.e.is_some()
            || received.message.ro.is_some()
        {
            self.finish_network_failure(
                pending,
                QueryError::InvalidResponse("响应消息包含互相冲突的字段"),
                now,
            )
            .await;
            return;
        }
        let Some(response) = received.message.r else {
            self.finish_network_failure(
                pending,
                QueryError::InvalidResponse("响应缺少 r 字段"),
                now,
            )
            .await;
            return;
        };
        if let Some(expected) = pending.remote.expected_id
            && response.id != expected
        {
            // 地址正确但身份不符同样不能入表，且这条 transaction 已经结束。
            let actual = response.id;
            self.finish_network_failure(
                pending,
                QueryError::UnexpectedNodeId { expected, actual },
                now,
            )
            .await;
            return;
        }

        // find_node 除了通用响应字段，还必须带回当前地址族的 compact nodes。先完成
        // 方法级校验，避免把格式错误的响应当成已验证节点写入 routing table。
        let decoded = if matches!(&pending.purpose, PendingPurpose::Fetch { .. }) {
            match self.decode_peers(&response) {
                Ok(response) => ValidatedResponse::Peers(response),
                Err(error) => {
                    self.finish_network_failure(pending, error, now).await;
                    return;
                }
            }
        } else if match &pending.purpose {
            PendingPurpose::MaintenanceLookup { .. } => true,
            #[cfg(test)]
            PendingPurpose::UserFindNode { .. } => true,
            _ => false,
        } {
            match self.decode_find_node_response(&response) {
                Ok(response) => ValidatedResponse::Nodes(response),
                Err(error) => {
                    self.finish_network_failure(pending, error, now).await;
                    return;
                }
            }
        } else if let PendingPurpose::Sampling(request) = &pending.purpose {
            let decoded = self.decode_find_node_response(&response).and_then(|nodes| {
                if request.kind == super::sampler::RequestKind::FindNodeFallback {
                    return Ok(ValidatedResponse::Nodes(nodes));
                }
                super::sampler::decode_sample(&response, nodes.nodes.clone(), received.encoded_len)
                    .map(|sample| match sample {
                        Some(sample) => ValidatedResponse::Samples(sample),
                        None => ValidatedResponse::Nodes(nodes),
                    })
            });
            match decoded {
                Ok(result) => result,
                Err(error) => {
                    self.finish_network_failure(pending, error, now).await;
                    return;
                }
            }
        } else {
            ValidatedResponse::Basic
        };

        self.budget.validated(received.source.ip());
        // 到这里才算得到一条经过 transaction、地址、结构和身份共同验证的响应。
        self.observer.emit(crate::observation::Kind::Routing,"contact","validated",||serde_json::json!({"node_id":crate::observation::hex(&response.id.0),"address":received.source.to_string()}));
        let outcome = self
            .routing
            .observe_response(response.id, received.source, now);
        self.finish_success(pending, response.id, decoded, now)
            .await;
        self.handle_insert_outcome(outcome, now).await;
    }

    /// 根据查询用途交付成功结果，并清除对应的内部去重状态。
    async fn finish_success(
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
                let response = decoded.nodes();
                let _ = reply.send(Ok(response));
            }
            PendingPurpose::Verification { key, .. } => {
                self.budget.verification_result(true);
                // observe_response 已在调用本函数前完成，移除标记后允许未来重新验证。
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
                    // 所有 questionable 节点都已恢复为 good；现在再按分裂规则处理候选。
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

    /// 把当前地址族的 compact nodes 转换成统一的节点列表。
    ///
    /// IPv4 实例要求 `nodes`，IPv6 实例要求 `nodes6`。缺少对应字段说明远端没有按
    /// 本次 find_node 的响应格式返回数据。
    fn decode_find_node_response(
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

    /// 匹配远端返回的 KRPC Error 消息。
    ///
    /// 格式正确的 Error 会结束 transaction 并原样交给查询调用者；它证明远端确实作出
    /// 了响应，所以普通用户查询不会因此给该节点增加一次“未响应”失败记录。
    pub(super) async fn handle_error(&mut self, received: ReceivedMessage, now: Instant) {
        let completed = match self
            .transactions
            .complete(&received.message.t, received.source, now)
        {
            Ok(completed) => completed,
            Err(TransactionError::Expired(transaction)) => {
                if let Some(pending) = self.pending.remove(&transaction.id) {
                    self.finish_network_failure(pending, QueryError::Timeout, now)
                        .await;
                }
                return;
            }
            Err(error) => {
                self.observe_rejected_response(&error, received.source);
                return;
            }
        };
        let Some(pending) = self.pending.remove(&completed.transaction.id) else {
            return;
        };

        if received.message.q.is_some()
            || received.message.a.is_some()
            || received.message.r.is_some()
            || received.message.ro.is_some()
        {
            self.finish_network_failure(
                pending,
                QueryError::InvalidResponse("错误消息包含互相冲突的字段"),
                now,
            )
            .await;
            return;
        }
        let error = match received.message.e {
            Some((code, message)) => QueryError::Remote { code, message },
            None => QueryError::InvalidResponse("错误消息缺少 e 字段"),
        };
        self.finish_network_failure(pending, error, now).await;
    }

    /// 处理已经发出查询之后发生的超时、非法响应或身份不匹配。
    ///
    /// 用户查询直接收到错误；陌生节点验证只清理状态；bucket probe 则按照 BEP 5
    /// 记录失败并最多再试一次。
    async fn finish_network_failure(
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
                // 只有查询真正发出后发生的网络失败才会走到这里，因此可以记在旧节点上。
                let status = self.routing.record_failure(incumbent.id, now);
                if status == Some(NodeStatus::Bad) {
                    // 连续失败达到阈值后，旧节点可以被已验证候选安全替换。
                    self.bucket_probes.remove(&key);
                    let outcome = self.routing.reconsider_candidate(candidate, now);
                    self.handle_insert_outcome(outcome, now).await;
                } else if attempt < 2 {
                    // 第一次失败不立即删除旧节点，按 BEP 5 再 ping 一次。
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
                    // 节点可能已经被其他状态变化移除；无法继续探测时只清理标记。
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

    /// 在已知 Node ID 时，把查询失败写回 routing table。
    ///
    /// bootstrap 地址没有已知 ID，因此不能猜测应该更新哪一条联系人记录。
    fn record_expected_failure(&mut self, remote: RemoteNode, now: Instant) {
        if let Some(id) = remote.expected_id {
            self.routing.record_failure(id, now);
        }
    }

    /// 执行 routing table 插入决定中需要网络交互的部分。
    ///
    /// 大多数结果已经由 routing table 自己处理；只有 `ProbeRequired` 需要 dispatcher
    /// 暂存候选节点，并向 bucket 中的旧节点发送 ping。
    async fn handle_insert_outcome(&mut self, outcome: InsertOutcome, now: Instant) {
        self.observer.emit(crate::observation::Kind::Routing, "insert", match &outcome {
            InsertOutcome::Inserted => "inserted",
            InsertOutcome::Updated => "updated",
            InsertOutcome::ReplacedBad { .. } => "replaced_bad",
            InsertOutcome::ProbeRequired { .. } => "probe_required",
            InsertOutcome::RejectedFull => "rejected_full",
            InsertOutcome::IgnoredSelf => "ignored_self",
            InsertOutcome::IgnoredWrongAddressFamily => "ignored_address_family",
        }, || match &outcome {
            InsertOutcome::ReplacedBad { removed } => serde_json::json!({"removed_id":crate::observation::hex(&removed.id.0),"removed_address":removed.address.to_string()}),
            _ => serde_json::json!({}),
        });
        if let InsertOutcome::ProbeRequired {
            mut incumbents,
            candidate,
        } = outcome
        {
            let incumbent = incumbents.remove(0);
            self.start_bucket_probe(incumbent, incumbents, candidate, 1, now)
                .await;
        }
    }

    /// 启动一轮旧节点探测，并用集合阻止同一节点被同时重复 ping。
    async fn start_bucket_probe(
        &mut self,
        incumbent: crate::dht::routing::NodeContact,
        remaining: Vec<crate::dht::routing::NodeContact>,
        candidate: crate::dht::routing::NodeContact,
        attempt: u8,
        now: Instant,
    ) {
        let key = (incumbent.id, incumbent.address);
        if !self.bucket_probes.insert(key) {
            return;
        }
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
                attempt,
            },
            now,
        )
        .await;
    }

    /// 取出所有到期 transaction，并按各自用途完成失败处理。
    pub(super) async fn expire_transactions(&mut self, now: Instant) {
        for transaction in self.transactions.expire(now) {
            if let Some(pending) = self.pending.remove(&transaction.id) {
                self.finish_network_failure(pending, QueryError::Timeout, now)
                    .await;
            }
        }
    }
}
