//! get_peers 提供地址和写令牌，announce_peer 校验成功后才保存宣布。
//!
//! 处理入站 get_peers 和 announce_peer；只有通过 token 与地址校验的宣布才能写入 peer store。

use super::runtime::DhtDispatcher;
use crate::krpc::{CompactPeerAddress, KrpcErrorCode, QueryArgs};
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
        let Some(hash) = args.info_hash else {
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
        self.send_response(source, t, response).await
    }

    pub(super) async fn handle_announce_peer(
        &mut self,
        source: SocketAddr,
        t: ByteBuf,
        args: &QueryArgs,
        now: Instant,
    ) -> bool {
        let (Some(hash), Some(token)) = (args.info_hash, args.token.as_ref()) else {
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
            self.send_error(
                source,
                t,
                KrpcErrorCode::Protocol,
                "announce_peer 参数不合法",
            )
            .await;
            return false;
        }
        let port = if args.implied_port == Some(1) {
            Some(source.port())
        } else {
            args.port
        };
        let Some(port) = port.filter(|port| *port != 0) else {
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
                self.send_error(source, t, KrpcErrorCode::Protocol, "写令牌无效或已过期")
                    .await;
                return false;
            }
            Err(_) => {
                self.send_error(source, t, KrpcErrorCode::Server, "暂时无法校验写令牌")
                    .await;
                return false;
            }
        }
        // 写入在发送确认之前完成：UDP 确认丢失不应撤销合法宣布。
        if self.peers.announce(hash, address, now).is_err() {
            self.send_error(source, t, KrpcErrorCode::Server, "暂时无法保存 peer")
                .await;
            return false;
        }
        if let Some(ingress) = &self.fetch_ingress {
            ingress.announce(super::fetch::AnnounceEvent {
                hash,
                peer: address,
                observed_at: self
                    .clock
                    .wall_at(now)
                    .unwrap_or_else(|_| std::time::SystemTime::now()),
            });
        }
        self.send_basic_response(source, t).await
    }
}

#[cfg(test)]
mod tests;
