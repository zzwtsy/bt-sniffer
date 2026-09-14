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

/// 错误维度来自失败位置；未知嵌套字段仍按原协议契约忽略。
#[test]
fn typed_extension_and_metadata_failures() {
    for (bytes, reason) in [
        (&b"degarbage"[..], WireErrorKind::ExtensionTrailing),
        (b"li1ee", WireErrorKind::DictionaryRoot),
        (b"", WireErrorKind::MissingDictionary),
        (b"x", WireErrorKind::MalformedBencode),
        (b"d1:mi1ee", WireErrorKind::ExtensionMapType),
        (b"d1:md11:ut_metadata1:xee", WireErrorKind::IntegerType),
        (
            b"d1:md11:ut_metadatai256eee",
            WireErrorKind::ExtensionIdRange,
        ),
        (
            b"d1:md11:ut_metadatai-1eee",
            WireErrorKind::ExtensionIdRange,
        ),
        (b"d13:metadata_sizei-1ee", WireErrorKind::MetadataSizeRange),
        (
            b"d13:metadata_sizei999999999999999999999999ee",
            WireErrorKind::IntegerOverflow,
        ),
    ] {
        assert_eq!(parse_extension(bytes, 4096, 64).unwrap_err().kind, reason);
    }
    for (bytes, reason) in [
        (
            &b"d8:msg_typei1e5:piecei0ee"[..],
            WireErrorKind::MissingTotalSize,
        ),
        (b"d8:msg_typei0e5:piecei-1ee", WireErrorKind::PieceRange),
        (
            b"d8:msg_typei1e5:piecei0e10:total_sizei-1ee",
            WireErrorKind::TotalSizeRange,
        ),
        (b"d8:msg_typei0ee", WireErrorKind::MissingPiece),
        (b"d5:piecei0ee", WireErrorKind::MissingMessageType),
        (
            b"d8:msg_typei2e5:piecei0eex",
            WireErrorKind::UnexpectedPayload,
        ),
    ] {
        assert_eq!(parse_metadata(bytes, 4096, 64).unwrap_err().kind, reason);
    }
    assert_eq!(
        parse_extension(b"de", 1, 64).unwrap_err().kind,
        WireErrorKind::ExtensionTooLarge
    );
    assert!(parse_extension(b"d1:md6:futured1:xli1eeee1:xd1:ai2eee", 4096, 64).is_ok());
}

/// Bendy 保持严格字典规则；说明仅供人读，乱序与重复键均报告 UnsortedKeys。
#[test]
fn bencode_details_preserve_strict_rejection() {
    for (bytes, depth, kind, detail_part) in [
        (
            &b"d1:bi1e1:ai2ee"[..],
            64,
            WireErrorKind::InvalidDictionary,
            "Keys were not sorted",
        ),
        (
            b"d1:ai1e1:ai2ee",
            64,
            WireErrorKind::InvalidDictionary,
            "Keys were not sorted",
        ),
        (
            b"d1:ai1e",
            64,
            WireErrorKind::InvalidDictionary,
            "Reached EOF",
        ),
        (b"d1:ai03ee", 64, WireErrorKind::InvalidDictionary, ""),
        (
            b"d1:ad1:bd1:ci1eeee",
            2,
            WireErrorKind::InvalidDictionary,
            "Maximum nesting depth exceeded",
        ),
        (b"x", 64, WireErrorKind::MalformedBencode, ""),
    ] {
        let error = parse_extension_strict(bytes, 4096, depth).unwrap_err();
        assert_eq!(error.kind, kind);
        let detail = error.detail.unwrap();
        assert!(!detail.text.is_empty());
        assert!(detail.text.contains(detail_part), "{detail:?}");
        assert!(!detail.truncated);
    }
    let error = parse_extension_strict(b"d1:md11:ut_metadatai256eee", 4096, 64).unwrap_err();
    assert_eq!(error.kind, WireErrorKind::ExtensionIdRange);
    assert!(error.detail.is_none());
    let valid =
        parse_extension_strict(b"d1:md11:ut_metadatai7ee13:metadata_sizei42ee", 4096, 64).unwrap();
    assert_eq!(
        (valid.metadata_id, valid.metadata_size),
        (Some(7), Some(42))
    );
}

/// 独立检查只记录已见键序证据；此测试针对兼容之前的严格入口。
#[test]
fn rejected_extensions_report_order_duplicate_and_partial_evidence() {
    use super::inspection::{InspectionStatus, inspect};
    for (bytes, unsorted, duplicate, status) in [
        (
            &b"d1:bi1e1:ai2ee"[..],
            true,
            false,
            InspectionStatus::Complete,
        ),
        (b"d1:ai1e1:ai2ee", false, true, InspectionStatus::Complete),
        (
            b"d1:ai1e1:bi2e1:ai3ee",
            true,
            true,
            InspectionStatus::Complete,
        ),
        (
            b"d1:ad1:bi1e1:ai2eee",
            true,
            false,
            InspectionStatus::Complete,
        ),
        (b"d1:bi1e1:ai2e", true, false, InspectionStatus::Malformed),
        (b"d1:ai03ee", false, false, InspectionStatus::Malformed),
    ] {
        let error = parse_extension_strict(bytes, 4096, 64).unwrap_err();
        assert_eq!(error.kind, WireErrorKind::InvalidDictionary);
        let check = error.inspection.unwrap();
        assert_eq!(
            (
                check.unsorted_keys,
                check.duplicate_keys,
                check.inspection_status
            ),
            (unsorted, duplicate, status)
        );
    }
    // 不跨字典判重，也不解释字节串内部看起来像字典的内容。
    for bytes in [&b"d1:ad1:xi1ee1:bd1:xi2eee"[..], b"d1:a14:d1:bi1e1:ai2eee"] {
        assert!(parse_extension_strict(bytes, 4096, 64).is_ok());
        let check = inspect(bytes, 4096, 64);
        assert!(!check.unsorted_keys && !check.duplicate_keys);
        assert_eq!(check.inspection_status, InspectionStatus::Complete);
    }
    assert_eq!(
        inspect(b"d1:ad1:bd1:ci1eeee", 4096, 2).inspection_status,
        InspectionStatus::DepthLimit
    );
    // 上限内检查，超过独立预算只给出 SizeLimit，不延续部分检查。
    let mut boundary = b"d1:bi1e1:a4080:".to_vec();
    boundary.extend(vec![b'x'; 4080]);
    boundary.push(b'e');
    assert_eq!(boundary.len(), 4096);
    assert_eq!(
        parse_extension_strict(&boundary, 5000, 64)
            .unwrap_err()
            .inspection
            .unwrap()
            .inspection_status,
        InspectionStatus::Complete
    );
    boundary.push(b'e');
    assert_eq!(
        parse_extension_strict(&boundary, 5000, 64)
            .unwrap_err()
            .inspection
            .unwrap()
            .inspection_status,
        InspectionStatus::SizeLimit
    );
    assert!(
        parse_extension_strict(&boundary, 4096, 64)
            .unwrap_err()
            .inspection
            .is_none()
    );
    assert!(
        parse_metadata(b"d1:bi1e1:ai2ee", 4096, 64)
            .unwrap_err()
            .inspection
            .is_none()
    );
}

/// 兼容只改变完整字典的条目顺序；每个成功结果都再次经过原严格字段校验。
#[test]
fn extension_compatibility_checks_every_container_and_preserves_atoms() {
    let expected =
        parse_extension(b"d1:md11:ut_metadatai7ee13:metadata_sizei42ee", 4096, 64).unwrap();
    for input in [
        &b"d13:metadata_sizei42e1:md11:ut_metadatai7eee"[..],
        b"d1:md11:ut_metadatai7e1:ai1ee13:metadata_sizei42ee",
        b"d1:md11:ut_metadatai7ee13:metadata_sizei42e1:zld1:bi1e1:ai2eeee",
    ] {
        let mut actual = parse_extension(input, 4096, 64).unwrap();
        assert_eq!(actual.mode, ExtensionMode::UnsortedCompatible);
        actual.mode = ExtensionMode::Strict;
        assert_eq!(actual, expected);
    }
    for input in [
        &b"d1:ai1e1:ai2ee"[..],
        b"d1:ai1e1:bi2e1:ai3ee",
        b"d1:zld1:ai1e1:ai2eeee",
        b"d1:bi1e1:ai2e",
        b"d1:bi1e1:ai03ee",
        b"d1:bi1e1:a03:abce",
        b"d1:bi1e1:ai-0ee",
        b"d1:bi1ei1ei2ee",
        b"d1:bi1e1:ai2eejunk",
        b"d13:metadata_sizei42e1:mi1ee",
        b"d13:metadata_sizei42e1:md11:ut_metadatai256eee",
        b"d13:metadata_sizei-1e1:md11:ut_metadatai7eee",
    ] {
        let original = parse_extension_strict(input, 4096, 64).unwrap_err();
        assert_eq!(parse_extension(input, 4096, 64).unwrap_err(), original);
    }
    // 相同键可位于不同字典；不解析字节串正文中的伪字典。
    for input in [
        &b"d1:bd1:xi2ee1:ad1:xi1eee"[..],
        b"d1:bi1e1:a14:d1:bi1e1:ai2eee",
    ] {
        assert_eq!(
            parse_extension(input, 4096, 64).unwrap().mode,
            ExtensionMode::UnsortedCompatible
        );
    }
    assert!(parse_metadata(b"d5:piecei0e8:msg_typei0ee", 4096, 64).is_err());
    assert!(dictionary_prefix(b"d1:bi1e1:ai2ee", 64).is_err());
}

#[test]
fn extension_compatibility_size_and_depth_budgets_do_not_tighten_strict_path() {
    let mut input = b"d1:bi1e1:a4080:".to_vec();
    input.extend(vec![b'x'; 4080]);
    input.push(b'e');
    assert_eq!(input.len(), 4096);
    assert_eq!(
        parse_extension(&input, 4096, 64).unwrap().mode,
        ExtensionMode::UnsortedCompatible
    );
    assert_eq!(
        parse_extension(&input, 4095, 64).unwrap_err().kind,
        WireErrorKind::ExtensionTooLarge
    );
    let mut larger = b"d1:bi1e1:a4081:".to_vec();
    larger.extend(vec![b'x'; 4081]);
    larger.push(b'e');
    assert!(parse_extension(&larger, 5000, 64).is_err());
    let mut sorted = b"d1:a4081:".to_vec();
    sorted.extend(vec![b'x'; 4081]);
    sorted.extend_from_slice(b"1:bi1ee");
    assert_eq!(
        parse_extension(&sorted, 5000, 64).unwrap().mode,
        ExtensionMode::Strict
    );
    for limit in [1, 2, 64] {
        let mut nested = b"d1:bi1e1:a".to_vec();
        nested.extend(vec![b'l'; limit - 1]);
        nested.extend_from_slice(b"i1e");
        nested.extend(vec![b'e'; limit]);
        assert!(parse_extension(&nested, 4096, limit).is_ok());
        assert!(parse_extension(&nested, 4096, limit - 1).is_err());
    }
}
