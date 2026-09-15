//! get_peers 提供地址和写令牌，announce_peer 校验成功后才保存宣布。
//!
//! 处理入站 get_peers 和 announce_peer；只有通过 token 与地址校验的宣布才能写入 peer store。

use super::runtime::DhtDispatcher;
use crate::dht::krpc::CompactPeerAddress;
use crate::dht::krpc::KrpcErrorCode;
use crate::dht::krpc::QueryArgs;
use serde_bytes::ByteBuf;
use std::{net::SocketAddr, time::Instant};

impl DhtDispatcher {
    pub(super) async fn handle_get_peers(
        &mut self,
        source: SocketAddr,
        t: ByteBuf,
        args: &QueryArgs,
        now: Instant,
    ) -> bool {
        let observer = args.info_hash.map_or_else(
            || self.observer.clone(),
            |hash| self.observer.for_hash(&hash.0),
        );
        let mut observation = observer.span(crate::observation::Kind::Rpc, "inbound_get_peers");
        observation.observer.emit(
            crate::observation::Kind::Rpc,
            "inbound_get_peers",
            "received",
            || serde_json::json!({"source":source.to_string()}),
        );
        let Some(hash) = args.info_hash else {
            observation.finish("missing_hash");
            self.send_error(
                source,
                t,
                KrpcErrorCode::Protocol,
                "get_peers 缺少 info_hash",
            )
            .await;
            return false;
        };
        if args.target.is_some()
            || args.port.is_some()
            || args.token.is_some()
            || args.implied_port.is_some()
        {
            observation.finish("invalid_arguments");
            self.send_error(
                source,
                t,
                KrpcErrorCode::Protocol,
                "get_peers 包含其他方法的参数",
            )
            .await;
            return false;
        }
        let token = match self.tokens.issue(source.ip(), now) {
            Ok(token) => token,
            Err(_) => {
                observation.finish("token_unavailable");
                self.send_error(source, t, KrpcErrorCode::Server, "暂时无法生成写令牌")
                    .await;
                return false;
            }
        };
        let mut response = self.node_response(&hash.0, &args.want, now);
        response.token = Some(token);
        // RNG 只在同步抽样期间使用，不跨越 await。
        let peers = self
            .peers
            .sample(hash, self.peers.response_limit(), now, &mut rand::rng());
        if !peers.is_empty() {
            response.values = Some(
                peers
                    .into_iter()
                    .map(|address| match address {
                        SocketAddr::V4(address) => CompactPeerAddress::V4(address),
                        SocketAddr::V6(address) => CompactPeerAddress::V6(address),
                    })
                    .collect(),
            );
        }
        observation.observer.emit(
            crate::observation::Kind::Rpc,
            "get_peers_reply",
            "prepared",
            || serde_json::json!({"peer_count":response.values.as_ref().map_or(0,Vec::len)}),
        );
        let sent = self.send_response(source, t, response).await;
        observation.finish(if sent { "reply_sent" } else { "reply_unsent" });
        sent
    }

    pub(super) async fn handle_announce_peer(
        &mut self,
        source: SocketAddr,
        t: ByteBuf,
        args: &QueryArgs,
        now: Instant,
    ) -> bool {
        let observer = args.info_hash.map_or_else(
            || self.observer.clone(),
            |hash| self.observer.for_hash(&hash.0),
        );
        let mut observation = observer.span(crate::observation::Kind::Discovery, "announce");
        observation.observer.emit(
            crate::observation::Kind::Discovery,
            "announce",
            "received",
            || serde_json::json!({"source":source.to_string()}),
        );
        let (Some(hash), Some(token)) = (args.info_hash, args.token.as_ref()) else {
            observation.finish("missing_fields");
            self.send_error(
                source,
                t,
                KrpcErrorCode::Protocol,
                "announce_peer 缺少 info_hash 或 token",
            )
            .await;
            return false;
        };
        if args.target.is_some()
            || !args.want.is_empty()
            || !matches!(args.implied_port, None | Some(0 | 1))
        {
            observation.finish("invalid_arguments");
            self.send_error(
                source,
                t,
                KrpcErrorCode::Protocol,
                "announce_peer 参数不合法",
            )
            .await;
            return false;
        }
        observation.observer = observation.observer.for_hash(&hash.0);
        let port = if args.implied_port == Some(1) {
            Some(source.port())
        } else {
            args.port
        };
        let Some(port) = port.filter(|port| *port != 0) else {
            observation.finish("invalid_port");
            self.send_error(
                source,
                t,
                KrpcErrorCode::Protocol,
                "announce_peer 缺少有效端口",
            )
            .await;
            return false;
        };
        let address = SocketAddr::new(source.ip(), port);
        if !self.peers.accepts(address) {
            observation.finish("invalid_address");
            self.send_error(
                source,
                t,
                KrpcErrorCode::Protocol,
                "peer 地址不符合当前地址策略",
            )
            .await;
            return false;
        }
        match self.tokens.validate(source.ip(), token, now) {
            Ok(true) => {}
            Ok(false) => {
                observation.finish("invalid_token");
                self.send_error(source, t, KrpcErrorCode::Protocol, "写令牌无效或已过期")
                    .await;
                return false;
            }
            Err(_) => {
                observation.finish("token_unavailable");
                self.send_error(source, t, KrpcErrorCode::Server, "暂时无法校验写令牌")
                    .await;
                return false;
            }
        }
        // 写入在发送确认之前完成：UDP 确认丢失不应撤销合法宣布。
        if self.peers.announce(hash, address, now).is_err() {
            observation.finish("peer_store_failed");
            self.send_error(source, t, KrpcErrorCode::Server, "暂时无法保存 peer")
                .await;
            return false;
        }
        if let Some(ingress) = &self.fetch_ingress {
            ingress.announce(super::fetch::AnnounceEvent {
                observer: observation.observer.clone(),
                hash,
                peer: address,
                observed_at: self
                    .clock
                    .wall_at(now)
                    .unwrap_or_else(|_| std::time::SystemTime::now()),
            });
        }
        observation.finish("validated");
        self.send_basic_response(source, t).await
    }
}

#[cfg(test)]
mod tests;
