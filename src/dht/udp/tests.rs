//! 只在 loopback 收发固定报文，验证大小、解码及错误来源，不依赖公网连通性。
use super::*;
use crate::dht::krpc::KrpcMessage;
use crate::dht::krpc::MessageType;
use crate::dht::krpc::NodeId;
use crate::dht::krpc::QueryArgs;
use crate::dht::krpc::QueryMethod;
use serde_bytes::ByteBuf;

/// 构造一条测试用的 ping 查询。
fn ping(transaction_id: &[u8]) -> KrpcMessage {
    KrpcMessage {
        t: ByteBuf::from(transaction_id.to_vec()),
        y: MessageType::Query,
        q: Some(QueryMethod::Ping),
        a: Some(QueryArgs {
            id: NodeId([7; 20]),
            target: None,
            info_hash: None,
            port: None,
            token: None,
            implied_port: None,
            want: Vec::new(),
        }),
        r: None,
        e: None,
        ro: None,
    }
}

/// 两个本地 socket 应能通过 transport 完整收发一条 KRPC 消息。
#[tokio::test]
async fn message_round_trips_over_udp() {
    let sender = UdpTransport::bind("127.0.0.1:0", UdpTransportConfig::default())
        .await
        .expect("发送端应该能够绑定");
    let receiver = UdpTransport::bind("127.0.0.1:0", UdpTransportConfig::default())
        .await
        .expect("接收端应该能够绑定");

    let sent = sender
        .send_to(receiver.local_addr().unwrap(), &ping(b"test"))
        .await
        .expect("ping 应该能够发送");
    let received = receiver.recv().await.expect("ping 应该能够接收");

    assert_eq!(received.encoded_len, sent);
    assert_eq!(received.source, sender.local_addr().unwrap());
    assert_eq!(received.message.t, b"test".as_slice());
    assert_eq!(received.message.q, Some(QueryMethod::Ping));
}

/// 超过配置上限的出站消息应在发送前被拒绝。
#[tokio::test]
async fn oversized_outgoing_message_is_rejected() {
    let transport = UdpTransport::bind(
        "127.0.0.1:0",
        UdpTransportConfig {
            max_message_size: 64,
        },
    )
    .await
    .expect("transport 应该能够绑定");
    let mut message = ping(b"test");
    message.q = Some(QueryMethod::Unknown("x".repeat(128)));

    assert!(matches!(
        transport
            .send_to(transport.local_addr().unwrap(), &message)
            .await,
        Err(UdpTransportError::MessageTooLarge { source: None, .. })
    ));
}

/// 不合法的 Bencode 数据报应返回带来源地址的解码错误。
#[tokio::test]
async fn malformed_datagram_reports_its_source() {
    let transport = UdpTransport::bind("127.0.0.1:0", UdpTransportConfig::default())
        .await
        .expect("transport 应该能够绑定");
    let raw_sender = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("原始发送端应该能够绑定");
    raw_sender
        .send_to(b"not-bencode", transport.local_addr().unwrap())
        .await
        .expect("测试数据应该能够发送");

    match transport.recv().await {
        Err(UdpTransportError::Decode { source, .. }) => {
            assert_eq!(source, raw_sender.local_addr().unwrap());
        }
        other => panic!("期望解码错误，实际得到 {other:?}"),
    }
}

/// 数据报超过接收上限时必须被丢弃，不能尝试解析被截断的内容。
#[tokio::test]
async fn oversized_incoming_datagram_is_rejected() {
    let transport = UdpTransport::bind(
        "127.0.0.1:0",
        UdpTransportConfig {
            max_message_size: 64,
        },
    )
    .await
    .expect("transport 应该能够绑定");
    let raw_sender = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("原始发送端应该能够绑定");
    raw_sender
        .send_to(&[b'x'; 65], transport.local_addr().unwrap())
        .await
        .expect("测试数据应该能够发送");

    assert!(matches!(
        transport.recv().await,
        Err(UdpTransportError::MessageTooLarge {
            source: Some(_),
            size: 65,
            limit: 64
        })
    ));
}

/// 一个合法消息后面追加额外数据时，不能把前半段误当成完整 KRPC 消息。
#[tokio::test]
async fn trailing_bytes_are_rejected() {
    let transport = UdpTransport::bind("127.0.0.1:0", UdpTransportConfig::default())
        .await
        .expect("transport 应该能够绑定");
    let raw_sender = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("原始发送端应该能够绑定");
    let mut encoded = bendy::serde::to_bytes(&ping(b"test")).unwrap();
    encoded.extend_from_slice(b"junk");
    raw_sender
        .send_to(&encoded, transport.local_addr().unwrap())
        .await
        .expect("测试数据应该能够发送");

    assert!(matches!(
        transport.recv().await,
        Err(UdpTransportError::Decode { .. })
    ));
}

/// 大小上限为零或超过 UDP payload 极限时，应在创建 transport 时拒绝配置。
#[tokio::test]
async fn invalid_message_size_limit_is_rejected() {
    for max_message_size in [0, MAX_UDP_PAYLOAD_SIZE + 1] {
        let socket = UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("测试 socket 应该能够绑定");
        assert!(matches!(
            UdpTransport::from_socket(socket, UdpTransportConfig { max_message_size }),
            Err(UdpTransportError::InvalidMaxMessageSize { actual })
                if actual == max_message_size
        ));
    }
}

fn decode_bytes(bytes: &[u8]) -> Result<ReceivedMessage, UdpTransportError> {
    Datagram {
        source: "[::1]:6881".parse().unwrap(),
        bytes: bytes.to_vec(),
        limit: DEFAULT_MAX_MESSAGE_SIZE,
    }
    .decode()
}

/// 服务器抓包的 libtorrent ping 和响应，保留实际键顺序及二进制字段。
#[test]
fn captured_libtorrent_unsorted_packets_decode() {
    let ip = [
        0x24, 0x0d, 0xc0, 0, 0xf0, 0, 0x95, 0, 0x36, 0x34, 0x6f, 0x4c, 0xa2, 0x98, 0, 0, 0x1a, 0xe1,
    ];
    let id = [
        0x13, 0xc3, 0x6e, 0x69, 0x73, 0x51, 0xff, 0x4a, 0xec, 0x29, 0xcd, 0xba, 0xab, 0xf2, 0xfb,
        0xe3, 0x46, 0x7c, 0xc2, 0x67,
    ];
    let response = [
        b"d2:ip18:".as_slice(),
        &ip,
        b"1:rd2:id20:",
        &id,
        b"e1:t4:\x00\x00\x00\x0c1:y1:r1:v4:LT\x01\x02e",
    ]
    .concat();
    let query = [
        b"d2:ip18:".as_slice(),
        &ip,
        b"1:ad2:id20:",
        &id,
        b"e1:t4:\xa9\x14g01:q4:ping1:v4:LT\x01\x021:y1:qe",
    ]
    .concat();
    assert_eq!(response.len(), 83);
    assert_eq!(query.len(), 92);
    assert!(bendy::serde::from_bytes::<KrpcMessage>(&response).is_err());
    let received = decode_bytes(&response).unwrap();
    assert_eq!(received.encoded_len, 83);
    assert_eq!(
        received.source,
        "[::1]:6881".parse::<std::net::SocketAddr>().unwrap()
    );
    assert_eq!(received.message.y, MessageType::Response);
    assert_eq!(received.message.t, [0, 0, 0, 12]);
    assert_eq!(received.message.r.unwrap().id, NodeId(id));
    let received = decode_bytes(&query).unwrap();
    assert_eq!(received.encoded_len, 92);
    assert_eq!(received.message.y, MessageType::Query);
    assert_eq!(received.message.q, Some(QueryMethod::Ping));
    assert_eq!(received.message.a.unwrap().id, NodeId(id));
    assert_eq!(received.message.t, [0xa9, 0x14, 0x67, 0x30]);
}

#[test]
fn nested_unknown_extensions_are_checked_and_binary_values_preserved() {
    let bytes = b"d1:y1:q1:t8:d1:be\x00\xffe1:q4:ping1:ad2:id20:abcdefghijklmnopqrste1:xld1:zi1e1:ali2e3:fooeeee";
    let result = decode_bytes(bytes).unwrap();
    assert_eq!(result.message.t, b"d1:be\x00\xffe".as_slice());
    assert_eq!(
        result.message.a.unwrap().id,
        NodeId(*b"abcdefghijklmnopqrst")
    );
}

#[test]
fn duplicate_keys_are_rejected_even_in_unknown_extensions() {
    for extension in [
        b"1:y1:q".as_slice(),
        b"1:xd1:ai1e1:ai2ee",
        b"1:xld1:bi1e1:ai2e1:bi3eee",
        b"1:xd0:i1e0:i2ee",
    ] {
        let bytes = [
            b"d1:y1:q1:t2:aa1:q4:ping1:ad2:id20:abcdefghijklmnopqrste".as_slice(),
            extension,
            b"e",
        ]
        .concat();
        assert!(
            matches!(decode_bytes(&bytes), Err(UdpTransportError::Decode { .. })),
            "{extension:?}"
        );
    }
    let nested_known =
        b"d1:y1:q1:t2:aa1:q4:ping1:ad2:id20:abcdefghijklmnopqrst2:id20:abcdefghijklmnopqrstee";
    assert!(decode_bytes(nested_known).is_err());
}

#[test]
fn ordering_compatibility_does_not_repair_malformed_atoms_or_containers() {
    let prefix = b"d1:y1:q1:t2:aa1:q4:ping1:ad2:id20:abcdefghijklmnopqrste1:x";
    for value in [
        b"i-0e".as_slice(),
        b"i01e",
        b"i+1e",
        b"ie",
        b"i1",
        b"01:x",
        b"-1:x",
        b"999999999999999999999999999999:x",
        b"9:x",
        b"d",
        b"l",
        b"di1ei2ee",
        b"d1:ae",
    ] {
        assert!(
            decode_bytes(&[prefix.as_slice(), value, b"e"].concat()).is_err(),
            "{value:?}"
        );
    }
    let valid = [prefix.as_slice(), b"i1ee"].concat();
    assert!(decode_bytes(&valid).is_ok());
    for suffix in [b"e".as_slice(), b"junk", b"de", b"\0"] {
        assert!(decode_bytes(&[valid.as_slice(), suffix].concat()).is_err());
    }
    for end in 0..valid.len() {
        assert!(decode_bytes(&valid[..end]).is_err(), "truncated at {end}");
    }
}

#[test]
fn unknown_extension_depth_is_bounded_for_sorted_and_unsorted_messages() {
    for prefix in [
        b"d1:ad2:id20:abcdefghijklmnopqrste1:q4:ping1:t2:aa1:y1:q1:z".as_slice(),
        b"d1:y1:q1:t2:aa1:q4:ping1:ad2:id20:abcdefghijklmnopqrste1:z",
    ] {
        for (lists, accepted) in [
            (ordering::MAX_DEPTH - 1, true),
            (ordering::MAX_DEPTH, false),
        ] {
            let bytes = [prefix, &vec![b'l'; lists], b"0:", &vec![b'e'; lists + 1]].concat();
            assert_eq!(decode_bytes(&bytes).is_ok(), accepted);
        }
    }
}

#[test]
fn oversize_unsorted_packet_is_rejected_before_normalization() {
    let mut bytes = b"d1:y1:q1:t2:aa1:q4:ping1:ad2:id20:abcdefghijklmnopqrste1:x".to_vec();
    bytes.extend_from_slice(&vec![b'l'; DEFAULT_MAX_MESSAGE_SIZE]);
    assert!(matches!(
        decode_bytes(&bytes),
        Err(UdpTransportError::MessageTooLarge { .. })
    ));
}
