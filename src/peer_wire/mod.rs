//! 只处理 BEP 3/10/9 在线上的字节形状，不连接网络、不保存下载状态。
//!
//! metadata::session 负责协商状态和超时；本层只借用输入字节解析字段，不接纳未经校验的下载结果。
use crate::krpc::InfoHashV1;
use bendy::decoding::{Decoder, Object};
use bytes::Bytes;
use serde::Serialize;
use std::fmt;

/// BEP 9 的标准分片字节数，末片可以更短。
pub(crate) const BLOCK_SIZE: usize = 16384;
/// 本端向对端声明的接收 ID；发送请求必须使用对端声明的 ID。
pub(crate) const LOCAL_METADATA_ID: u8 = 1;

/// TCP peer-wire 的 20 字节身份，与 DHT Node ID 独立。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PeerId(pub(crate) [u8; 20]);

/// 字节结构或字段约束错误；网络失败和会话资源限制由上层分类。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WireError(pub(crate) &'static str);
impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for WireError {}

/// 标准握手不属于长度前缀帧，必须先单独收发这 68 字节。
pub(crate) fn handshake(hash: InfoHashV1, peer_id: PeerId) -> [u8; 68] {
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
    hash: InfoHashV1,
) -> Result<HandshakeInfo, WireError> {
    if bytes[0] != 19 || &bytes[1..20] != b"BitTorrent protocol" {
        return Err(WireError("peer-wire 协议名不匹配"));
    }
    if bytes[28..48] != hash.0 {
        return Err(WireError("标准握手 info-hash 不匹配"));
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
        .map_err(|_| WireError("Bencode 字典损坏"))?
        .ok_or(WireError("缺少 Bencode 字典"))?
        .try_into_dictionary()
        .map_err(|_| WireError("Bencode 根必须为字典"))?
        .into_raw()
        .map_err(|_| WireError("Bencode 字典结构不合法"))
}
fn integer(object: Object<'_, '_>) -> Result<i64, WireError> {
    object
        .try_into_integer()
        .map_err(|_| WireError("字段必须是整数"))?
        .parse()
        .map_err(|_| WireError("整数溢出"))
}

/// 一次扩展握手的增量字段，不能用缺省值覆盖已协商状态。
#[derive(Debug, Default)]
pub(crate) struct ExtensionUpdate {
    /// None 表示未更新；Some(0) 明确禁用，非零值用于向对端发送。
    pub(crate) metadata_id: Option<u8>,
    /// 本次声明的原始 info 字典字节数；大小上限和后续一致性由会话检查。
    pub(crate) metadata_size: Option<usize>,
}
/// 所有握手字段都可省略；返回增量更新，由会话决定何时已有足够信息发请求。
/// header_limit 限制整个握手的字节数，depth 限制 Bencode 嵌套层数；拒绝尾随字节。
pub(crate) fn parse_extension(
    bytes: &[u8],
    header_limit: usize,
    depth: usize,
) -> Result<ExtensionUpdate, WireError> {
    if bytes.len() > header_limit {
        return Err(WireError("扩展握手过大"));
    }
    let raw = dictionary_prefix(bytes, depth)?;
    if raw.len() != bytes.len() {
        return Err(WireError("扩展握手存在尾随字节"));
    }
    let mut decoder = Decoder::new(raw).with_max_depth(depth);
    let mut dict = decoder
        .next_object()
        .unwrap()
        .unwrap()
        .try_into_dictionary()
        .unwrap();
    let mut update = ExtensionUpdate::default();
    while let Some((key, value)) = dict.next_pair().map_err(|_| WireError("扩展握手损坏"))? {
        match key {
            b"m" => {
                let mut extensions = value
                    .try_into_dictionary()
                    .map_err(|_| WireError("m 必须是字典"))?;
                while let Some((name, value)) = extensions
                    .next_pair()
                    .map_err(|_| WireError("m 字典损坏"))?
                {
                    if name == b"ut_metadata" {
                        update.metadata_id = Some(
                            u8::try_from(integer(value)?)
                                .map_err(|_| WireError("扩展 ID 必须在 0..255"))?,
                        );
                    }
                }
            }
            b"metadata_size" => {
                update.metadata_size = Some(
                    usize::try_from(integer(value)?)
                        .map_err(|_| WireError("metadata_size 不能为负"))?,
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
        .unwrap()
        .unwrap()
        .try_into_dictionary()
        .unwrap();
    let (mut kind, mut piece, mut size) = (None, None, None);
    while let Some((key, value)) = dict
        .next_pair()
        .map_err(|_| WireError("metadata 消息头损坏"))?
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
                piece = Some(
                    usize::try_from(integer(value)?).map_err(|_| WireError("piece 不能为负"))?,
                )
            }
            b"total_size" => {
                size = Some(
                    usize::try_from(integer(value)?)
                        .map_err(|_| WireError("total_size 不能为负"))?,
                )
            }
            _ => {}
        }
    }
    let piece = piece.ok_or(WireError("缺少 piece"))?;
    match kind.ok_or(WireError("缺少 msg_type"))? {
        0 if payload.is_empty() => Ok(MetadataMessage::Request { piece }),
        2 if payload.is_empty() => Ok(MetadataMessage::Reject { piece }),
        1 => Ok(MetadataMessage::Data {
            piece,
            total_size: size.ok_or(WireError("data 缺少 total_size"))?,
            data: payload,
        }),
        _ => Err(WireError("request/reject 不应带二进制数据")),
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
