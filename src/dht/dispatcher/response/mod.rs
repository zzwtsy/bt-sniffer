//! 出站 Response/Error 的 transaction 匹配、方法校验和结果结算。
//!
//! 先匹配 transaction 的来源和预期身份，再交付结果；无关响应不能消耗其他查询名额。

mod completion;
mod routing;
mod validation;

use self::validation::ValidatedResponse;
use super::{
    api::QueryError,
    runtime::{DhtDispatcher, PendingPurpose},
};
use crate::dht::{transaction::TransactionError, udp::ReceivedMessage};
use std::{net::SocketAddr, time::Instant};

impl DhtDispatcher {
    fn observe_rejected_response(&self, error: &TransactionError, source: SocketAddr) {
        let observer = match error {
            TransactionError::SourceMismatch { id, .. } => self
                .pending
                .get(id)
                .map(|pending| &pending.observation.observer)
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

    pub(in crate::dht::dispatcher) async fn handle_response(
        &mut self,
        received: ReceivedMessage,
        now: Instant,
    ) {
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
            self.finish_network_failure(
                pending,
                QueryError::UnexpectedNodeId {
                    expected,
                    actual: response.id,
                },
                now,
            )
            .await;
            return;
        }
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
        self.observe_external(
            received.source,
            received.message.ip.as_ref().map(|b| b.as_slice()),
            now,
        );
        self.budget.validated(received.source.ip());
        self.observer.emit(crate::observation::Kind::Routing,"contact","validated",||serde_json::json!({"node_id":crate::observation::hex(&response.id.0),"address":received.source.to_string()}));
        let outcome = self
            .routing
            .observe_response(response.id, received.source, now);
        self.finish_success(pending, response.id, decoded, now)
            .await;
        self.handle_insert_outcome(outcome, now).await;
    }

    pub(in crate::dht::dispatcher) async fn handle_error(
        &mut self,
        received: ReceivedMessage,
        now: Instant,
    ) {
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
        if received.message.e.is_some() {
            self.observe_external(
                received.source,
                received.message.ip.as_ref().map(|b| b.as_slice()),
                now,
            );
        }
        let error = match received.message.e {
            Some((code, message)) => QueryError::Remote { code, message },
            None => QueryError::InvalidResponse("错误消息缺少 e 字段"),
        };
        self.finish_network_failure(pending, error, now).await;
    }
}

#[cfg(test)]
mod security_tests;
