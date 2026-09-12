//! KRPC 消息级测试。
//!
//! 这些测试直接比较协议规定的 Bencode 字节，避免“编码和解码同时写错，但往返测试
//! 仍然通过”的情况。compact 二进制格式的细节测试位于 `compact.rs`。

use super::*;
use bendy::serde::{from_bytes, to_bytes};
use serde_bytes::ByteBuf;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};

const QUERYING_NODE_ID: [u8; 20] = *b"abcdefghij0123456789";
const RESPONDING_NODE_ID: [u8; 20] = *b"mnopqrstuvwxyz123456";

/// 生成只包含必填 `id` 的查询参数，方便每个测试专注于自己关心的字段。
fn query_args() -> QueryArgs {
    QueryArgs {
        id: NodeId(QUERYING_NODE_ID),
        target: None,
        info_hash: None,
        port: None,
        token: None,
        implied_port: None,
        want: Vec::new(),
    }
}

/// 生成一条查询消息。
fn query_message(method: QueryMethod, arguments: QueryArgs) -> KrpcMessage {
    KrpcMessage {
        t: ByteBuf::from(b"aa".to_vec()),
        y: MessageType::Query,
        q: Some(method),
        a: Some(arguments),
        r: None,
        e: None,
        ro: None,
    }
}

/// 生成只包含必填 `id` 的响应参数。
fn response_args() -> ResponseArgs {
    ResponseArgs {
        id: NodeId(RESPONDING_NODE_ID),
        token: None,
        nodes: None,
        nodes6: None,
        values: None,
        samples: None,
        interval: None,
        num: None,
    }
}

/// 生成一条成功响应消息。
fn response_message(arguments: ResponseArgs) -> KrpcMessage {
    KrpcMessage {
        t: ByteBuf::from(b"aa".to_vec()),
        y: MessageType::Response,
        q: None,
        a: None,
        r: Some(arguments),
        e: None,
        ro: None,
    }
}

/// 构造一个合法的 Bencode 字节串，用于测试固定长度字段。
fn bencode_byte_string(payload: &[u8]) -> Vec<u8> {
    let mut encoded = format!("{}:", payload.len()).into_bytes();
    encoded.extend_from_slice(payload);
    encoded
}

/// BEP 5 给出的 ping 查询格式：字段名、字符串长度和字典层级都必须完全一致。
#[test]
fn ping_query_matches_bep5_wire_format() {
    let message = query_message(QueryMethod::Ping, query_args());
    let expected = b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe";

    let encoded = to_bytes(&message).expect("ping 查询应该能够编码");
    assert_eq!(encoded, expected);

    let decoded: KrpcMessage = from_bytes(expected).expect("BEP 5 ping 查询应该能够解码");
    assert_eq!(decoded.t, b"aa".as_slice());
    assert_eq!(decoded.y, MessageType::Query);
    assert_eq!(decoded.q, Some(QueryMethod::Ping));
    assert_eq!(
        decoded.a.expect("查询必须包含参数").id,
        NodeId(QUERYING_NODE_ID)
    );
    assert!(decoded.r.is_none());
    assert!(decoded.e.is_none());
}

/// find_node 必须把目标放在 `target` 字段中，并按 20 字节值编码。
#[test]
fn find_node_query_encodes_target() {
    let mut arguments = query_args();
    arguments.target = Some(NodeId(RESPONDING_NODE_ID));
    let message = query_message(QueryMethod::FindNode, arguments);
    let expected = b"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t2:aa1:y1:qe";

    assert_eq!(
        to_bytes(&message).expect("find_node 应该能够编码"),
        expected
    );

    let decoded: KrpcMessage = from_bytes(expected).expect("find_node 应该能够解码");
    assert_eq!(
        decoded.a.expect("查询必须包含参数").target,
        Some(NodeId(RESPONDING_NODE_ID))
    );
}

/// get_peers 使用 `info_hash`，不能误编码成 Node ID 或普通整数列表。
#[test]
fn get_peers_query_encodes_info_hash() {
    let mut arguments = query_args();
    arguments.info_hash = Some(InfoHashV1(RESPONDING_NODE_ID));
    let message = query_message(QueryMethod::GetPeers, arguments);
    let expected = b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz123456e1:q9:get_peers1:t2:aa1:y1:qe";

    assert_eq!(
        to_bytes(&message).expect("get_peers 应该能够编码"),
        expected
    );

    let decoded: KrpcMessage = from_bytes(expected).expect("get_peers 应该能够解码");
    assert_eq!(
        decoded.a.expect("查询必须包含参数").info_hash,
        Some(InfoHashV1(RESPONDING_NODE_ID))
    );
}

/// announce_peer 的整数、令牌和可选字段必须直接出现在参数字典中，
/// 不能因为 Rust 使用 `Option` 而多出一层 Bencode 列表。
#[test]
fn announce_peer_query_encodes_all_arguments_transparently() {
    let mut arguments = query_args();
    arguments.info_hash = Some(InfoHashV1(RESPONDING_NODE_ID));
    arguments.port = Some(6881);
    arguments.token = Some(Token(ByteBuf::from(b"XX".to_vec())));
    arguments.implied_port = Some(1);
    let message = query_message(QueryMethod::AnnouncePeer, arguments);
    let expected = b"d1:ad2:id20:abcdefghij012345678912:implied_porti1e9:info_hash20:mnopqrstuvwxyz1234564:porti6881e5:token2:XXe1:q13:announce_peer1:t2:aa1:y1:qe";

    assert_eq!(
        to_bytes(&message).expect("announce_peer 应该能够编码"),
        expected
    );

    let decoded: KrpcMessage = from_bytes(expected).expect("announce_peer 应该能够解码");
    let arguments = decoded.a.expect("查询必须包含参数");
    assert_eq!(arguments.port, Some(6881));
    assert_eq!(arguments.implied_port, Some(1));
    assert_eq!(arguments.token, Some(Token(ByteBuf::from(b"XX".to_vec()))));
}

/// BEP 51 的 sample_infohashes 查询应保留 `want` 地址族列表。
#[test]
fn sample_infohashes_query_encodes_target_and_want() {
    let mut arguments = query_args();
    arguments.target = Some(NodeId(RESPONDING_NODE_ID));
    arguments.want = vec!["n4".to_owned(), "n6".to_owned()];
    let message = query_message(QueryMethod::SampleInfohashes, arguments);
    let expected = b"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz1234564:wantl2:n42:n6ee1:q17:sample_infohashes1:t2:aa1:y1:qe";

    assert_eq!(
        to_bytes(&message).expect("sample_infohashes 应该能够编码"),
        expected
    );

    let decoded: KrpcMessage = from_bytes(expected).expect("sample_infohashes 应该能够解码");
    assert_eq!(decoded.a.expect("查询必须包含参数").want, ["n4", "n6"]);
}

/// 未认识的第三方查询方法不能导致整条消息解析失败，也不能丢失原始方法名。
#[test]
fn unknown_query_method_is_preserved() {
    let encoded = b"d1:ad2:id20:abcdefghij0123456789e1:q8:x_lookup1:t2:aa1:y1:qe";

    let decoded: KrpcMessage = from_bytes(encoded).expect("未知查询方法也应该能够解码");
    assert_eq!(decoded.q, Some(QueryMethod::Unknown("x_lookup".to_owned())));
    assert_eq!(
        to_bytes(&decoded).expect("未知查询方法应该能够重新编码"),
        encoded
    );
}

/// BEP 5 的成功响应只包含事务 ID、消息类型和响应参数。
#[test]
fn ping_response_matches_bep5_wire_format() {
    let message = response_message(response_args());
    let expected = b"d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re";

    assert_eq!(to_bytes(&message).expect("ping 响应应该能够编码"), expected);

    let decoded: KrpcMessage = from_bytes(expected).expect("BEP 5 ping 响应应该能够解码");
    assert_eq!(decoded.y, MessageType::Response);
    assert_eq!(
        decoded.r.expect("响应消息必须包含 r").id,
        NodeId(RESPONDING_NODE_ID)
    );
    assert!(decoded.q.is_none());
    assert!(decoded.a.is_none());
    assert!(decoded.e.is_none());
}

/// find_node 响应中的节点集合必须作为一个连续字节串出现，不能变成 Bencode 列表。
#[test]
fn find_node_response_embeds_compact_nodes_without_extra_wrapper() {
    let node = CompactNodeV4 {
        id: NodeId(QUERYING_NODE_ID),
        address: SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 1), 6881),
    };
    let mut arguments = response_args();
    arguments.nodes = Some(CompactNodesV4(vec![node.clone()]));
    let message = response_message(arguments);

    let mut expected = b"d1:rd2:id20:".to_vec();
    expected.extend_from_slice(&RESPONDING_NODE_ID);
    expected.extend_from_slice(b"5:nodes26:");
    expected.extend_from_slice(&QUERYING_NODE_ID);
    expected.extend_from_slice(&[192, 0, 2, 1]);
    expected.extend_from_slice(&6881_u16.to_be_bytes());
    expected.extend_from_slice(b"e1:t2:aa1:y1:re");

    let encoded = to_bytes(&message).expect("find_node 响应应该能够编码");
    assert_eq!(encoded, expected);

    let decoded: KrpcMessage = from_bytes(&encoded).expect("find_node 响应应该能够解码");
    assert_eq!(
        decoded.r.expect("响应必须包含 r").nodes,
        Some(CompactNodesV4(vec![node]))
    );
}

/// IPv6 节点集合应放在 `nodes6` 字段中，每条记录固定为 38 字节。
#[test]
fn find_node_response_encodes_ipv6_nodes_in_nodes6_field() {
    let node = CompactNodeV6 {
        id: NodeId(QUERYING_NODE_ID),
        address: SocketAddrV6::new(Ipv6Addr::LOCALHOST, 6881, 0, 0),
    };
    let mut arguments = response_args();
    arguments.nodes6 = Some(CompactNodesV6(vec![node.clone()]));
    let message = response_message(arguments);

    let mut expected = b"d1:rd2:id20:".to_vec();
    expected.extend_from_slice(&RESPONDING_NODE_ID);
    expected.extend_from_slice(b"6:nodes638:");
    expected.extend_from_slice(&QUERYING_NODE_ID);
    expected.extend_from_slice(&Ipv6Addr::LOCALHOST.octets());
    expected.extend_from_slice(&6881_u16.to_be_bytes());
    expected.extend_from_slice(b"e1:t2:aa1:y1:re");

    let encoded = to_bytes(&message).expect("IPv6 find_node 响应应该能够编码");
    assert_eq!(encoded, expected);

    let decoded: KrpcMessage = from_bytes(&encoded).expect("IPv6 find_node 响应应该能够解码");
    assert_eq!(
        decoded.r.expect("响应必须包含 r").nodes6,
        Some(CompactNodesV6(vec![node]))
    );
}

/// get_peers 响应中的令牌应是普通字节串，peer 地址应是字节串列表。
#[test]
fn get_peers_response_encodes_token_and_peer_values() {
    let peer = CompactPeerAddress::V4(SocketAddrV4::new(Ipv4Addr::new(203, 0, 113, 5), 51413));
    let mut arguments = response_args();
    arguments.token = Some(Token(ByteBuf::from(b"XX".to_vec())));
    arguments.values = Some(vec![peer.clone()]);
    let message = response_message(arguments);

    let mut expected = b"d1:rd2:id20:".to_vec();
    expected.extend_from_slice(&RESPONDING_NODE_ID);
    expected.extend_from_slice(b"5:token2:XX6:valuesl6:");
    expected.extend_from_slice(&[203, 0, 113, 5]);
    expected.extend_from_slice(&51413_u16.to_be_bytes());
    expected.extend_from_slice(b"ee1:t2:aa1:y1:re");

    let encoded = to_bytes(&message).expect("get_peers 响应应该能够编码");
    assert_eq!(encoded, expected);

    let decoded: KrpcMessage = from_bytes(&encoded).expect("get_peers 响应应该能够解码");
    let response = decoded.r.expect("响应必须包含 r");
    assert_eq!(response.token, Some(Token(ByteBuf::from(b"XX".to_vec()))));
    assert_eq!(response.values, Some(vec![peer]));
}

/// BEP 51 响应应直接携带刷新间隔、远端样本总数和连续的 info-hash 字节串。
#[test]
fn sample_infohashes_response_encodes_all_bep51_fields() {
    let samples = InfoHashSamples(vec![InfoHashV1([1; 20]), InfoHashV1([2; 20])]);
    let mut arguments = response_args();
    arguments.samples = Some(samples.clone());
    arguments.interval = Some(300);
    arguments.num = Some(2);
    let message = response_message(arguments);

    let mut expected = b"d1:rd2:id20:".to_vec();
    expected.extend_from_slice(&RESPONDING_NODE_ID);
    expected.extend_from_slice(b"8:intervali300e3:numi2e7:samples40:");
    expected.extend_from_slice(&[1; 20]);
    expected.extend_from_slice(&[2; 20]);
    expected.extend_from_slice(b"e1:t2:aa1:y1:re");

    let encoded = to_bytes(&message).expect("sample_infohashes 响应应该能够编码");
    assert_eq!(encoded, expected);

    let decoded: KrpcMessage = from_bytes(&encoded).expect("sample_infohashes 响应应该能够解码");
    let response = decoded.r.expect("响应必须包含 r");
    assert_eq!(response.samples, Some(samples));
    assert_eq!(response.interval, Some(300));
    assert_eq!(response.num, Some(2));
}

/// 错误响应的 `e` 必须是 `[错误码, 错误说明]` 列表。
#[test]
fn error_response_encodes_code_and_message_as_a_list() {
    let message = KrpcMessage {
        t: ByteBuf::from(b"aa".to_vec()),
        y: MessageType::Error,
        q: None,
        a: None,
        r: None,
        e: Some((201, ByteBuf::from(b"A Generic Error Occurred".to_vec()))),
        ro: None,
    };
    let expected = b"d1:eli201e24:A Generic Error Occurrede1:t2:aa1:y1:ee";

    assert_eq!(to_bytes(&message).expect("错误响应应该能够编码"), expected);

    let decoded: KrpcMessage = from_bytes(expected).expect("错误响应应该能够解码");
    assert_eq!(decoded.y, MessageType::Error);
    assert_eq!(
        decoded.e,
        Some((201, ByteBuf::from(b"A Generic Error Occurred".to_vec())))
    );
}

/// 值为 `None` 的响应字段应从字典中消失，字段缺失时应重新得到 `None`。
#[test]
fn response_omits_absent_optional_fields() {
    let response = response_args();
    let encoded = to_bytes(&response).expect("响应应该能够编码");

    let mut expected = b"d2:id20:".to_vec();
    expected.extend_from_slice(&RESPONDING_NODE_ID);
    expected.push(b'e');
    assert_eq!(encoded, expected);

    let decoded: ResponseArgs = from_bytes(&encoded).expect("响应应该能够解码");
    assert_eq!(decoded.id, response.id);
    assert!(decoded.token.is_none());
    assert!(decoded.nodes.is_none());
    assert!(decoded.nodes6.is_none());
    assert!(decoded.values.is_none());
    assert!(decoded.samples.is_none());
    assert!(decoded.interval.is_none());
    assert!(decoded.num.is_none());
}

/// Node ID 和 v1 info-hash 都必须恰好为 20 字节。
#[test]
fn twenty_byte_identifiers_reject_wrong_lengths() {
    assert!(from_bytes::<NodeId>(&bencode_byte_string(&[0; 19])).is_err());
    assert!(from_bytes::<NodeId>(&bencode_byte_string(&[0; 21])).is_err());
    assert!(from_bytes::<InfoHashV1>(&bencode_byte_string(&[0; 19])).is_err());
    assert!(from_bytes::<InfoHashV1>(&bencode_byte_string(&[0; 21])).is_err());

    assert_eq!(
        from_bytes::<NodeId>(&bencode_byte_string(&[7; 20])).expect("20 字节 Node ID 应该合法"),
        NodeId([7; 20])
    );
    assert_eq!(
        from_bytes::<InfoHashV1>(&bencode_byte_string(&[8; 20]))
            .expect("20 字节 info-hash 应该合法"),
        InfoHashV1([8; 20])
    );
}

/// `y` 只有 q、r、e 三个合法值，其他值不能伪装成正常 KRPC 消息。
#[test]
fn unknown_message_type_is_rejected() {
    let encoded = b"d1:t2:aa1:y1:xe";
    assert!(from_bytes::<KrpcMessage>(encoded).is_err());
}

/// `t` 和 `y` 是所有 KRPC 消息都必须具有的字段。
#[test]
fn missing_required_message_fields_are_rejected() {
    assert!(from_bytes::<KrpcMessage>(b"d1:y1:qe").is_err());
    assert!(from_bytes::<KrpcMessage>(b"d1:t2:aae").is_err());
}

/// 查询参数和响应参数中的 `id` 同样是必填字段，空字典不能被当作合法参数。
#[test]
fn missing_node_id_in_arguments_is_rejected() {
    let query_with_empty_arguments = b"d1:ade1:q4:ping1:t2:aa1:y1:qe";
    let response_with_empty_arguments = b"d1:rde1:t2:aa1:y1:re";

    assert!(from_bytes::<KrpcMessage>(query_with_empty_arguments).is_err());
    assert!(from_bytes::<KrpcMessage>(response_with_empty_arguments).is_err());
}

/// 错误字段必须正好包含错误码和错误说明，缺少其中一项时应拒绝消息。
#[test]
fn incomplete_error_list_is_rejected() {
    let encoded = b"d1:eli201ee1:t2:aa1:y1:ee";
    assert!(from_bytes::<KrpcMessage>(encoded).is_err());
}

/// 事务 ID 是原始字节串，不要求是 UTF-8 文本。
#[test]
fn transaction_id_preserves_arbitrary_bytes() {
    let mut message = query_message(QueryMethod::Ping, query_args());
    message.t = ByteBuf::from(vec![0x00, 0xff]);

    let encoded = to_bytes(&message).expect("二进制事务 ID 应该能够编码");
    let decoded: KrpcMessage = from_bytes(&encoded).expect("二进制事务 ID 应该能够解码");
    assert_eq!(decoded.t, [0x00, 0xff]);
}

/// BEP 43 的 `ro=1` 必须保留下来，routing table 才能忽略只读节点。
#[test]
fn read_only_flag_is_preserved() {
    let encoded = b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping2:roi1e1:t2:aa1:y1:qe";

    let decoded: KrpcMessage = from_bytes(encoded).expect("只读查询应该能够解码");
    assert_eq!(decoded.ro, Some(1));
    assert_eq!(
        to_bytes(&decoded).expect("只读查询应该能够重新编码"),
        encoded
    );
}

/// 公网协议可能增加新字段；不认识的字段应被忽略，而不是破坏整条消息。
#[test]
fn unknown_extension_fields_are_ignored() {
    let encoded = b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:v4:test1:y1:qe";

    let decoded: KrpcMessage = from_bytes(encoded).expect("扩展字段不应导致解析失败");
    assert_eq!(decoded.q, Some(QueryMethod::Ping));
    assert_eq!(
        decoded.a.expect("查询必须包含参数").id,
        NodeId(QUERYING_NODE_ID)
    );
}
