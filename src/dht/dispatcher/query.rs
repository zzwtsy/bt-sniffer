//! 入站 KRPC Query 的校验、响应和发送者验证。
//!
//! 入站查询可触发回复和联系人验证；收到陌生节点的请求不足以将其视为已验证邻居。

use serde_bytes::ByteBuf;
use std::net::SocketAddr;
use std::time::Instant;

use super::api::RemoteNode;
use super::runtime::{DhtDispatcher, PendingPurpose};
use crate::dht::routing::{AddressFamily, BUCKET_SIZE, QueryObservation};
use crate::krpc::{
    CompactNodeV4, CompactNodeV6, CompactNodesV4, CompactNodesV6, KrpcErrorCode, KrpcMessage,
    MessageType, NodeId, QueryMethod, ResponseArgs,
};
use crate::net::udp::ReceivedMessage;

impl DhtDispatcher {
    /// 校验并处理远端发来的查询。
    ///
    /// 这里只能处理已经被 transport 成功解码的消息。完全无法解码、因而无法可靠取出
    /// transaction ID 的数据报会在事件循环中直接丢弃。
    pub(super) async fn handle_query(&mut self, received: ReceivedMessage, now: Instant) {
        let source = received.source;
        let message = received.message;
        let transaction_id = message.t.clone();

        // Query 不应混入响应或错误字段；BEP 43 的 ro 目前也只接受规定值 1。
        if message.r.is_some() || message.e.is_some() || !matches!(message.ro, None | Some(1)) {
            self.send_error(
                source,
                transaction_id,
                KrpcErrorCode::Protocol,
                "查询消息字段不合法",
            )
            .await;
            return;
        }

        let (Some(method), Some(arguments)) = (message.q, message.a) else {
            self.send_error(
                source,
                transaction_id,
                KrpcErrorCode::Protocol,
                "查询缺少 q 或 a 字段",
            )
            .await;
            return;
        };
        let read_only = message.ro == Some(1);

        match method {
            QueryMethod::Ping => {
                // 宽松 wire struct 可以表达所有方法的参数，因此需要在分派处排除
                // 明显属于其他方法的字段。
                if arguments.target.is_some()
                    || arguments.info_hash.is_some()
                    || arguments.port.is_some()
                    || arguments.token.is_some()
                    || arguments.implied_port.is_some()
                {
                    self.send_error(
                        source,
                        transaction_id,
                        KrpcErrorCode::Protocol,
                        "ping 包含不属于该方法的参数",
                    )
                    .await;
                    return;
                }

                if self.send_basic_response(source, transaction_id).await {
                    self.observe_query(arguments.id, source, read_only, now)
                        .await;
                }
            }
            QueryMethod::FindNode => {
                let Some(target) = arguments.target else {
                    self.send_error(
                        source,
                        transaction_id,
                        KrpcErrorCode::Protocol,
                        "find_node 缺少 target 参数",
                    )
                    .await;
                    return;
                };
                if arguments.info_hash.is_some()
                    || arguments.port.is_some()
                    || arguments.token.is_some()
                    || arguments.implied_port.is_some()
                {
                    self.send_error(
                        source,
                        transaction_id,
                        KrpcErrorCode::Protocol,
                        "find_node 包含不属于该方法的参数",
                    )
                    .await;
                    return;
                }

                let response = self.node_response(&target.0, &arguments.want, now);

                if self.send_response(source, transaction_id, response).await {
                    self.observe_query(arguments.id, source, read_only, now)
                        .await;
                }
            }
            QueryMethod::GetPeers => {
                if self
                    .handle_get_peers(source, transaction_id, &arguments, now)
                    .await
                {
                    self.observe_query(arguments.id, source, read_only, now)
                        .await;
                }
            }
            QueryMethod::AnnouncePeer => {
                if self
                    .handle_announce_peer(source, transaction_id, &arguments, now)
                    .await
                {
                    self.observe_query(arguments.id, source, read_only, now)
                        .await;
                }
            }
            QueryMethod::SampleInfohashes => {
                if self
                    .handle_sample_infohashes(source, transaction_id, &arguments, now)
                    .await
                {
                    self.observe_query(arguments.id, source, read_only, now)
                        .await;
                }
            }
            QueryMethod::Unknown(_) => {
                self.send_error(
                    source,
                    transaction_id,
                    KrpcErrorCode::MethodUnknown,
                    "当前节点不支持该查询方法",
                )
                .await;
            }
        }
    }

    /// 在成功服务一条查询后，把发送者的活动交给 routing table 判断。
    ///
    /// 陌生节点主动联系我们并不能证明它可以接收 UDP，所以 routing table 不会直接
    /// 插入它，而是要求 dispatcher 发出一条反向 ping。
    async fn observe_query(
        &mut self,
        id: NodeId,
        address: SocketAddr,
        read_only: bool,
        now: Instant,
    ) {
        if let QueryObservation::VerificationRequired { id, address } =
            self.routing.observe_query(id, address, read_only, now)
        {
            self.schedule_verification(id, address, now).await;
        }
    }

    /// 为陌生查询发送者安排一次去重后的反向 ping。
    async fn schedule_verification(&mut self, id: NodeId, address: SocketAddr, now: Instant) {
        let key = (id, address);
        if !self.verifications.insert(key) {
            self.budget.verification_duplicate();
            // 同一验证尚未结束，不因对方重复查询而不断发送新 ping。
            return;
        }
        self.start_query(
            RemoteNode {
                address,
                expected_id: Some(id),
            },
            QueryMethod::Ping,
            None,
            PendingPurpose::Verification { key, permit: None },
            now,
        )
        .await;
    }

    /// ping 和 announce_peer 的成功响应都只需包含本地 Node ID。
    pub(super) async fn send_basic_response(
        &mut self,
        destination: SocketAddr,
        t: ByteBuf,
    ) -> bool {
        self.send_response(destination, t, empty_response(self.routing.local_id()))
            .await
    }

    /// 组装并发送 KRPC Response；返回值表示完整数据报是否成功交给 socket。
    pub(super) async fn send_response(
        &mut self,
        destination: SocketAddr,
        t: ByteBuf,
        response: ResponseArgs,
    ) -> bool {
        let mut message = KrpcMessage {
            t,
            y: MessageType::Response,
            q: None,
            a: None,
            r: Some(response),
            e: None,
            ro: None,
        };
        if !fit_response(&mut message, self.transport.max_message_size().min(1024)) {
            return false;
        }
        self.send_reply(destination, &message)
    }

    /// 回显 transaction ID，并发送一条标准 KRPC Error。
    ///
    /// 错误响应发送失败时没有 transaction 可以继续等待，因此这里选择结束当前处理，
    /// 不让单个远端地址的发送错误关闭整个节点。
    pub(super) async fn send_error(
        &mut self,
        destination: SocketAddr,
        t: ByteBuf,
        code: KrpcErrorCode,
        explanation: &'static str,
    ) {
        let message = KrpcMessage {
            t,
            y: MessageType::Error,
            q: None,
            a: None,
            r: None,
            e: Some((
                code.as_i64(),
                ByteBuf::from(explanation.as_bytes().to_vec()),
            )),
            ro: None,
        };
        if bendy::serde::to_bytes(&message)
            .is_ok_and(|bytes| bytes.len() <= self.transport.max_message_size().min(1024))
        {
            self.send_reply(destination, &message);
        }
    }

    fn send_reply(&self, destination: SocketAddr, message: &KrpcMessage) -> bool {
        let Ok(bytes) = bendy::serde::to_bytes(message) else {
            return false;
        };
        if !self.budget.reply(bytes.len()) {
            return false;
        }
        match self.transport.try_send_to(destination, message) {
            Ok(sent) => {
                self.budget.reply_sent(sent);
                true
            }
            Err(_) => {
                self.budget.dropped();
                false
            }
        }
    }

    /// 各种查找共用同一 want 规则；另一地址族没有本地数据时返回空字段。
    pub(super) fn node_response(
        &self,
        target: &[u8; 20],
        want: &[String],
        now: Instant,
    ) -> ResponseArgs {
        let mut v4 = want.iter().any(|s| s == "n4");
        let mut v6 = want.iter().any(|s| s == "n6");
        if !v4 && !v6 {
            v4 = self.routing.address_family() == AddressFamily::Ipv4;
            v6 = !v4;
        }
        let contacts = self.routing.closest_good(target, BUCKET_SIZE, now);
        let mut response = empty_response(self.routing.local_id());
        if v4 {
            response.nodes = Some(CompactNodesV4(
                contacts
                    .iter()
                    .filter_map(|contact| match contact.address {
                        SocketAddr::V4(address) => Some(CompactNodeV4 {
                            id: contact.id,
                            address,
                        }),
                        _ => None,
                    })
                    .collect(),
            ));
        }
        if v6 {
            response.nodes6 = Some(CompactNodesV6(
                contacts
                    .iter()
                    .filter_map(|contact| match contact.address {
                        SocketAddr::V6(address) => Some(CompactNodeV6 {
                            id: contact.id,
                            address,
                        }),
                        _ => None,
                    })
                    .collect(),
            ));
        }
        response
    }
}

/// 每次删除完整记录后重新编码，实际字节数包含长 transaction ID 的全部开销。
/// 必需字段都不能放下时返回 false，调用方静默丢弃而不截断 token 或 transaction ID。
fn fit_response(message: &mut KrpcMessage, limit: usize) -> bool {
    loop {
        match bendy::serde::to_bytes(message) {
            Ok(bytes) if bytes.len() <= limit => return true,
            Err(_) => return false,
            _ => {}
        }
        let Some(response) = message.r.as_mut() else {
            return false;
        };
        if let Some(values) = response.values.as_mut() {
            values.pop();
            if values.is_empty() {
                response.values = None;
            }
            continue;
        }
        // BEP 51 即使没有样本，也必须保留空字节串作为支持该扩展的标志。
        if response
            .samples
            .as_mut()
            .is_some_and(|samples| samples.0.pop().is_some())
        {
            continue;
        }
        // 节点按距离排列，删除末尾即可优先保留最近的节点。
        if response
            .nodes
            .as_mut()
            .is_some_and(|nodes| nodes.0.pop().is_some())
        {
            continue;
        }
        if response
            .nodes6
            .as_mut()
            .is_some_and(|nodes| nodes.0.pop().is_some())
        {
            continue;
        }
        return false;
    }
}

/// 创建只填入必需 Node ID、其余扩展字段为空的响应字典。
pub(super) fn empty_response(id: NodeId) -> ResponseArgs {
    ResponseArgs {
        id,
        token: None,
        nodes: None,
        nodes6: None,
        values: None,
        samples: None,
        interval: None,
        num: None,
    }
}
