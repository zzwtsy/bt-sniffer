//! v1 info 字典的原始字节 SHA-1；DHT 与采集共用，不属于 KRPC 消息层。
use serde::{Deserialize, Serialize};
/// 固定 20 字节，以 Bencode 字节串编码；与 DHT NodeId 不可互换。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct InfoHashV1(#[serde(with = "serde_bytes")] pub(crate) [u8; 20]);
