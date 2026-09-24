//! DHT 查找键与完整 torrent 身份；Node ID 使用独立类型。
use serde::{Deserialize, Serialize};
/// 线上 20 字节查找键，尚不能判断 SHA-1 或截断 SHA-256。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct SwarmKey(#[serde(with = "serde_bytes")] pub(crate) [u8; 20]);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct InfoHashV1(pub(crate) [u8; 20]);
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct InfoHashV2(pub(crate) [u8; 32]);

/// 完整身份不从长度为 20 的 DHT 结果直接推断。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum TorrentIdentity {
    V1(InfoHashV1),
    V2(InfoHashV2),
}
impl TorrentIdentity {
    pub(crate) fn bytes(&self) -> &[u8] {
        match self {
            Self::V1(hash) => &hash.0,
            Self::V2(hash) => &hash.0,
        }
    }
    pub(crate) fn kind(self) -> &'static str {
        match self {
            Self::V1(_) => "v1",
            Self::V2(_) => "v2",
        }
    }
    pub(crate) fn swarm_key(self) -> SwarmKey {
        SwarmKey(self.bytes()[..20].try_into().expect("完整摘要至少 20 字节"))
    }
}
#[cfg(test)]
impl From<SwarmKey> for TorrentIdentity {
    // 仅用于明确按历史 v1 完整身份查询的调用者，DHT 不使用此转换。
    fn from(key: SwarmKey) -> Self {
        Self::V1(InfoHashV1(key.0))
    }
}
