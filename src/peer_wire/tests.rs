//! wire 层用固定字节验证协议，不依赖网络。
use super::*;

/// 扩展能力只占 reserved[5] 的 0x10，Peer ID 与 DHT Node ID 不混用。
#[test]
fn standard_handshake_layout_and_validation() {
    let hash = InfoHashV1([3; 20]);
    let id = PeerId([4; 20]);
    let mut bytes = handshake(hash, id);
    assert_eq!(&bytes[20..28], &[0, 0, 0, 0, 0, 16, 0, 0]);
    assert_eq!(
        parse_handshake(&bytes, hash).unwrap(),
        HandshakeInfo {
            peer_id: id,
            supports_extensions: true
        }
    );
    bytes[25] = 0;
    assert_eq!(
        parse_handshake(&bytes, hash).unwrap(),
        HandshakeInfo {
            peer_id: id,
            supports_extensions: false
        }
    );
    bytes[28] ^= 1;
    assert!(parse_handshake(&bytes, hash).is_err());
    bytes[28] ^= 1;
    bytes[1] = b'x';
    assert!(parse_handshake(&bytes, hash).is_err());
}

/// 可选字段省略不表示禁用；ID=0 则是明确禁用。未知字段与扩展名可以忽略。
#[test]
fn extension_handshake_is_an_additive_update() {
    assert_eq!(parse_extension(b"de", 4096, 64).unwrap().metadata_id, None);
    let update =
        parse_extension(b"d1:md11:ut_metadatai7ee13:metadata_sizei42ee", 4096, 64).unwrap();
    assert_eq!(update.metadata_id, Some(7));
    assert_eq!(update.metadata_size, Some(42));
    assert_eq!(
        parse_extension(b"d1:md11:ut_metadatai0eee", 4096, 64)
            .unwrap()
            .metadata_id,
        Some(0)
    );
    assert!(parse_extension(b"d1:md6:futured1:xi1eeee", 4096, 64).is_ok());
    for bytes in [
        &b"d1:md11:ut_metadatai256eee"[..],
        b"d13:metadata_sizei-1ee",
        b"degarbage",
        b"li1ee",
    ] {
        assert!(parse_extension(bytes, 4096, 64).is_err());
    }
    assert!(parse_extension(b"de", 1, 64).is_err());
}

/// 二进制正文不是字典的一部分，其中的 e、零字节以及看似 Bencode 的内容都要原样保留。
#[test]
fn metadata_header_boundary_does_not_consume_binary_data() {
    let mut bytes = b"d8:msg_typei1e5:piecei0e10:total_sizei9ee".to_vec();
    bytes.extend_from_slice(b"e\0d1:ai1e");
    match parse_metadata(&bytes, 4096, 64).unwrap() {
        MetadataMessage::Data {
            piece,
            total_size,
            data,
        } => {
            assert_eq!(piece, 0);
            assert_eq!(total_size, 9);
            assert_eq!(data, b"e\0d1:ai1e");
        }
        other => panic!("错误消息类型：{other:?}"),
    }
    assert!(matches!(
        parse_metadata(b"d8:msg_typei2e5:piecei3ee", 4096, 64).unwrap(),
        MetadataMessage::Reject { piece: 3 }
    ));
    // 未知类型无需理解其未来扩展字段，不能因为缺少 piece 就拒绝。
    assert!(matches!(
        parse_metadata(b"d8:msg_typei99eeopaque", 4096, 64).unwrap(),
        MetadataMessage::Unknown
    ));
}

/// 负数、缺字段、额外正文和过深的字典都必须在 wire 层拒绝。
#[test]
fn malformed_metadata_headers_are_rejected() {
    for bytes in [
        &b"d8:msg_typei1e5:piecei0ee"[..],
        b"d8:msg_typei0e5:piecei-1ee",
        b"d8:msg_typei2e5:piecei0eex",
        b"d5:piecei0ee",
    ] {
        assert!(parse_metadata(bytes, 4096, 64).is_err());
    }
    assert!(dictionary_prefix(b"d1:ad1:bd1:ci1eeee", 2).is_err());
    assert!(parse_metadata(b"d8:msg_typei0e5:piecei0ee", 8, 64).is_err());
}
