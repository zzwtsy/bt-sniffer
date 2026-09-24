//! 只处理 BEP 3/10/9 在线上的字节形状，不连接网络、不保存下载状态。
//!
//! collection::peer::session 负责协商状态和超时；本层只借用输入字节解析字段，不接纳未经校验的下载结果。
use crate::info_hash::SwarmKey;
use bendy::decoding::{Decoder, Object};
use bytes::Bytes;
use serde::Serialize;
mod error;
mod extension_ordering;
mod inspection;
pub(crate) use error::{WireError, WireErrorKind};

/// 版本 2 仅兼容扩展握手乱序；metadata 消息头及原始 info 继续严格校验。
pub(crate) const EXTENSION_HANDSHAKE_POLICY_VERSION: u64 = 2;

/// BEP 9 的标准分片字节数，末片可以更短。
pub(crate) const BLOCK_SIZE: usize = 16384;
/// 本端向对端声明的接收 ID；发送请求必须使用对端声明的 ID。
pub(crate) const LOCAL_METADATA_ID: u8 = 1;

/// TCP peer-wire 的 20 字节身份，与 DHT Node ID 独立。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PeerId(pub(crate) [u8; 20]);

/// 标准握手不属于长度前缀帧，必须先单独收发这 68 字节。
pub(crate) fn handshake(hash: SwarmKey, peer_id: PeerId) -> [u8; 68] {
    let mut bytes = [0; 68];
    bytes[0] = 19;
    bytes[1..20].copy_from_slice(b"BitTorrent protocol");
    bytes[25] = 0x10;
    bytes[28..48].copy_from_slice(&hash.0);
    bytes[48..].copy_from_slice(&peer_id.0);
    bytes
}
/// 已校验的标准握手字段；扩展能力只表示允许继续 BEP 10 协商。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HandshakeInfo {
    pub(crate) peer_id: PeerId,
    pub(crate) supports_extensions: bool,
}

/// 校验协议名及目标 info-hash，不进行 TCP 读写，也不改变会话状态。
pub(crate) fn parse_handshake(
    bytes: &[u8; 68],
    hash: SwarmKey,
) -> Result<HandshakeInfo, WireError> {
    if bytes[0] != 19 || &bytes[1..20] != b"BitTorrent protocol" {
        return Err(WireErrorKind::ProtocolName.into());
    }
    if bytes[28..48] != hash.0 {
        return Err(WireErrorKind::HandshakeHash.into());
    }
    Ok(HandshakeInfo {
        peer_id: PeerId(bytes[48..].try_into().expect("Peer ID 固定 20 字节")),
        supports_extensions: bytes[25] & 0x10 != 0,
    })
}

/// 让 Bendy 找到完整字典边界；分片中的任意 'e' 字节都不会被误判成结束符。
pub(crate) fn dictionary_prefix(bytes: &[u8], depth: usize) -> Result<&[u8], WireError> {
    let mut decoder = Decoder::new(bytes).with_max_depth(depth);
    decoder
        .next_object()
        .map_err(|error| WireError::bendy(WireErrorKind::MalformedBencode, &error))?
        .ok_or(WireErrorKind::MissingDictionary)?
        .try_into_dictionary()
        .map_err(|error| WireError::bendy(WireErrorKind::DictionaryRoot, &error))?
        .into_raw()
        .map_err(|error| WireError::bendy(WireErrorKind::InvalidDictionary, &error))
}
fn integer(object: Object<'_, '_>) -> Result<i64, WireError> {
    object
        .try_into_integer()
        .map_err(|error| WireError::bendy(WireErrorKind::IntegerType, &error))?
        .parse()
        .map_err(|_| WireErrorKind::IntegerOverflow.into())
}

/// 一次扩展握手的增量字段，不能用缺省值覆盖已协商状态。
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct ExtensionUpdate {
    /// 只表示本帧解析方式，不表示 metadata 下载成功。
    pub(crate) mode: ExtensionMode,
    /// None 表示未更新；Some(0) 明确禁用，非零值用于向对端发送。
    pub(crate) metadata_id: Option<u8>,
    /// 本次声明的原始 info 字典字节数；大小上限和后续一致性由会话检查。
    pub(crate) metadata_size: Option<usize>,
}
/// 严格解析优先；仅字典键乱序兼容路径接纳的帧标记为兼容。
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExtensionMode {
    #[default]
    Strict,
    UnsortedCompatible,
}
/// 所有握手字段都可省略；返回增量更新，由会话决定何时已有足够信息发请求。
/// header_limit 限制整个握手的字节数，depth 限制 Bencode 嵌套层数；拒绝尾随字节。
pub(crate) fn parse_extension(
    bytes: &[u8],
    header_limit: usize,
    depth: usize,
) -> Result<ExtensionUpdate, WireError> {
    let error = match parse_extension_strict(bytes, header_limit, depth) {
        Ok(update) => return Ok(update),
        Err(error) if error.kind == WireErrorKind::InvalidDictionary => error,
        Err(error) => return Err(error),
    };
    // 独立完整校验，不把 inspection 的诊断结论当成接纳依据。
    if let Ok(normalized) = extension_ordering::normalize(bytes, header_limit, depth)
        && let Ok(mut update) = parse_extension_strict(&normalized, header_limit, depth)
    {
        update.mode = ExtensionMode::UnsortedCompatible;
        return Ok(update);
    }
    Err(error)
}

/// 原严格路径同时用于首次解析与规范化后的完整字段校验。
fn parse_extension_strict(
    bytes: &[u8],
    header_limit: usize,
    depth: usize,
) -> Result<ExtensionUpdate, WireError> {
    if bytes.len() > header_limit {
        return Err(WireErrorKind::ExtensionTooLarge.into());
    }
    let raw = dictionary_prefix(bytes, depth).map_err(|mut error| {
        if error.kind == WireErrorKind::InvalidDictionary {
            error.inspection = Some(inspection::inspect(bytes, header_limit, depth));
        }
        error
    })?;
    if raw.len() != bytes.len() {
        return Err(WireErrorKind::ExtensionTrailing.into());
    }
    let mut decoder = Decoder::new(raw).with_max_depth(depth);
    let mut dict = decoder
        .next_object()
        .map_err(|error| WireError::bendy(WireErrorKind::ExtensionMalformed, &error))?
        .ok_or(WireErrorKind::MissingDictionary)?
        .try_into_dictionary()
        .map_err(|error| WireError::bendy(WireErrorKind::DictionaryRoot, &error))?;
    let mut update = ExtensionUpdate::default();
    while let Some((key, value)) = dict
        .next_pair()
        .map_err(|error| WireError::bendy(WireErrorKind::ExtensionMalformed, &error))?
    {
        match key {
            b"m" => {
                let mut extensions = value
                    .try_into_dictionary()
                    .map_err(|error| WireError::bendy(WireErrorKind::ExtensionMapType, &error))?;
                while let Some((name, value)) = extensions.next_pair().map_err(|error| {
                    WireError::bendy(WireErrorKind::ExtensionMapMalformed, &error)
                })? {
                    if name == b"ut_metadata" {
                        update.metadata_id = Some(
                            u8::try_from(integer(value)?)
                                .map_err(|_| WireErrorKind::ExtensionIdRange)?,
                        );
                    }
                }
            }
            b"metadata_size" => {
                update.metadata_size = Some(
                    usize::try_from(integer(value)?)
                        .map_err(|_| WireErrorKind::MetadataSizeRange)?,
                );
            }
            _ => {} // 不把不认识的扩展字段当成错误。
        }
    }
    Ok(update)
}

/// 解析后的消息头与借用载荷；Data 中的切片不能比输入帧活得更久。
#[derive(Debug)]
pub(crate) enum MetadataMessage<'a> {
    Request {
        piece: usize,
    },
    Data {
        piece: usize,
        total_size: usize,
        data: &'a [u8],
    },
    Reject {
        piece: usize,
    },
    Unknown,
}
/// header_limit 只限制 Bencode 头部字节数，头部之后保留为原始载荷。
/// 未知消息类型返回 Unknown；分片大小、请求状态及重复内容由会话校验。
pub(crate) fn parse_metadata(
    bytes: &[u8],
    header_limit: usize,
    depth: usize,
) -> Result<MetadataMessage<'_>, WireError> {
    // 只允许头部在 header_limit 内；后面的 16 KiB 数据不属于 Bencode。
    let raw = dictionary_prefix(&bytes[..bytes.len().min(header_limit)], depth)?;
    let payload = &bytes[raw.len()..];
    let mut decoder = Decoder::new(raw).with_max_depth(depth);
    let mut dict = decoder
        .next_object()
        .map_err(|error| WireError::bendy(WireErrorKind::MetadataHeaderMalformed, &error))?
        .ok_or(WireErrorKind::MissingDictionary)?
        .try_into_dictionary()
        .map_err(|error| WireError::bendy(WireErrorKind::MetadataHeaderMalformed, &error))?;
    let (mut kind, mut piece, mut size) = (None, None, None);
    while let Some((key, value)) = dict
        .next_pair()
        .map_err(|error| WireError::bendy(WireErrorKind::MetadataHeaderMalformed, &error))?
    {
        match key {
            b"msg_type" => {
                let n = integer(value)?;
                if !(0..=2).contains(&n) {
                    return Ok(MetadataMessage::Unknown);
                }
                kind = Some(n);
            }
            b"piece" => {
                piece =
                    Some(usize::try_from(integer(value)?).map_err(|_| WireErrorKind::PieceRange)?)
            }
            b"total_size" => {
                size = Some(
                    usize::try_from(integer(value)?).map_err(|_| WireErrorKind::TotalSizeRange)?,
                )
            }
            _ => {}
        }
    }
    let piece = piece.ok_or(WireErrorKind::MissingPiece)?;
    match kind.ok_or(WireErrorKind::MissingMessageType)? {
        0 if payload.is_empty() => Ok(MetadataMessage::Request { piece }),
        2 if payload.is_empty() => Ok(MetadataMessage::Reject { piece }),
        1 => Ok(MetadataMessage::Data {
            piece,
            total_size: size.ok_or(WireErrorKind::MissingTotalSize)?,
            data: payload,
        }),
        _ => Err(WireErrorKind::UnexpectedPayload.into()),
    }
}

/// Codec 负责写四字节长度；这里的返回值只包含消息 ID、扩展 ID 和消息体。
pub(crate) fn extended(id: u8, payload: &[u8]) -> Bytes {
    let mut bytes = Vec::with_capacity(2 + payload.len());
    bytes.extend_from_slice(&[20, id]);
    bytes.extend_from_slice(payload);
    bytes.into()
}
/// 使用保留的扩展 ID 0 发送握手，声明本端接收 ut_metadata 的 ID 为 1。
pub(crate) fn extension_handshake() -> Bytes {
    extended(0, b"d1:md11:ut_metadatai1eee")
}
/// id 使用对端声明值，piece 从 0 起；调用者负责选择 kind（0 请求、2 拒绝）。
pub(crate) fn metadata_control(id: u8, kind: u8, piece: usize) -> Bytes {
    #[derive(Serialize)]
    struct Header {
        msg_type: u8,
        piece: usize,
    }
    extended(
        id,
        &bendy::serde::to_bytes(&Header {
            msg_type: kind,
            piece,
        })
        .expect("固定整数头可编码"),
    )
}

#[cfg(test)]
mod tests;
