//! 不发送公网请求：直接向登记事务注入报文，验证地址观察必须经过响应边界。
use super::*;
use crate::dht::dispatcher::{RemoteNode, runtime::PendingDispatch};
use crate::dht::{
    krpc::{KrpcMessage, MessageType, NodeId, QueryMethod},
    routing::{AddressFamily, RoutingTable},
    transaction::TransactionManager,
    udp::UdpTransport,
};
use serde_bytes::ByteBuf;
use std::time::Duration;

#[tokio::test]
async fn error_observations_reject_forged_expired_and_replayed_packets() {
    let now = Instant::now();
    let transport = UdpTransport::bind("127.0.0.1:0", Default::default())
        .await
        .unwrap();
    let (mut dispatcher, _handle) = DhtDispatcher::new(
        transport,
        RoutingTable::new(NodeId([1; 20]), AddressFamily::Ipv4, now),
        TransactionManager::new(Duration::from_secs(5), 8),
    )
    .unwrap();
    let ip = "8.8.4.4:6881".parse().unwrap();
    for (index, source) in ["1.1.1.1:6881", "2.2.2.2:6881", "3.3.3.3:6881"]
        .into_iter()
        .enumerate()
    {
        let source = source.parse().unwrap();
        let id = dispatcher
            .transactions
            .register(source, QueryMethod::Ping, now)
            .unwrap();
        let (reply, _result) = tokio::sync::oneshot::channel();
        dispatcher.pending.insert(
            id,
            PendingDispatch {
                observation: dispatcher
                    .observer
                    .span(crate::observation::Kind::Rpc, "test"),
                remote: RemoteNode {
                    address: source,
                    expected_id: None,
                },
                purpose: PendingPurpose::UserPing {
                    reply,
                    cancel: Default::default(),
                },
            },
        );
        let message = || KrpcMessage {
            t: id.to_byte_buf(),
            y: MessageType::Error,
            q: None,
            a: None,
            r: None,
            e: Some((202, ByteBuf::from(b"busy".to_vec()))),
            ro: None,
            ip: Some(crate::dht::security::compact(ip)),
        };
        let received = |source, message| ReceivedMessage {
            source,
            message,
            encoded_len: 100,
        };
        dispatcher
            .handle_error(received("9.9.9.9:6881".parse().unwrap(), message()), now)
            .await;
        assert!(dispatcher.pending_external_ip.is_none());
        dispatcher
            .handle_error(received(source, message()), now)
            .await;
        if index < 2 {
            assert!(dispatcher.pending_external_ip.is_none());
        }
        dispatcher
            .handle_error(received(source, message()), now)
            .await;
    }
    assert_eq!(dispatcher.pending_external_ip, Some(ip.ip()));
    dispatcher.pending_external_ip = None;
    let source = "4.4.4.4:6881".parse().unwrap();
    let id = dispatcher
        .transactions
        .register(source, QueryMethod::Ping, now)
        .unwrap();
    let (reply, _result) = tokio::sync::oneshot::channel();
    dispatcher.pending.insert(
        id,
        PendingDispatch {
            observation: dispatcher
                .observer
                .span(crate::observation::Kind::Rpc, "expired"),
            remote: RemoteNode {
                address: source,
                expected_id: None,
            },
            purpose: PendingPurpose::UserPing {
                reply,
                cancel: Default::default(),
            },
        },
    );
    let message = KrpcMessage {
        t: id.to_byte_buf(),
        y: MessageType::Error,
        q: None,
        a: None,
        r: None,
        e: Some((202, ByteBuf::from(b"busy".to_vec()))),
        ro: None,
        ip: Some(crate::dht::security::compact(ip)),
    };
    dispatcher
        .handle_error(
            ReceivedMessage {
                source,
                message,
                encoded_len: 100,
            },
            now + Duration::from_secs(6),
        )
        .await;
    assert!(dispatcher.pending_external_ip.is_none());
}
