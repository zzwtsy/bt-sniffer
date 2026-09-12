//! KRPC（Kademlia RPC）消息的数据结构。
//!
//! BitTorrent DHT 节点通过 UDP 交换 Bencode 编码的 KRPC 消息。本模块只描述
//! 消息在网络上的形状，实际的收发、路由和业务处理由其他模块负责。
//!
//! 描述报文的可选字段，不承担业务授权；缺失字段是否非法由具体查询处理器决定。

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_bytes::ByteBuf;

use super::compact::{CompactNodesV4, CompactNodesV6, CompactPeerAddress, InfoHashSamples};

/// 帮助 Serde 正确处理“字段不存在”的可选值。
///
/// Bencode 没有通用的 `null` 值，所以 `None` 不应该被编码成一个具体值，而应该
/// 让整个字段从字典中消失。反序列化时，字段存在就包装成 `Some`；字段不存在则由
/// 字段上的 `default` 产生 `None`。
mod transparent_option {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub(super) fn serialize<S, T>(value: &Option<T>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: Serialize,
    {
        match value {
            // 字段有值时，直接序列化内部的 T，不额外编码 Option 这一层。
            Some(value) => value.serialize(serializer),
            // 正常情况下，None 会被 skip_serializing_if 提前跳过。
            None => Err(serde::ser::Error::custom(
                "None must be omitted with skip_serializing_if",
            )),
        }
    }

    pub(super) fn deserialize<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
    where
        D: Deserializer<'de>,
        T: Deserialize<'de>,
    {
        // 这个函数只会在字段存在时调用，因此读到的值一定是 Some。
        T::deserialize(deserializer).map(Some)
    }
}

/// DHT 节点的唯一标识，固定为 20 字节。
///
/// `serde_bytes` 确保它被编码为一个 Bencode 字节串，而不是 20 个整数组成的列表。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct NodeId(#[serde(with = "serde_bytes")] pub(crate) [u8; 20]);

/// BitTorrent v1 种子的 info-hash，固定为 20 字节。
///
/// 它和 [`NodeId`] 长度相同，但含义不同；使用独立类型可以避免把二者误传。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct InfoHashV1(#[serde(with = "serde_bytes")] pub(crate) [u8; 20]);

/// KRPC 消息的种类，对应消息字典中的 `y` 字段。
///
/// Rust 代码使用有意义的变体名，Serde 则负责把它们转换成协议规定的单字母值。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum MessageType {
    /// 查询消息，在线上写作 `y = "q"`。
    #[serde(rename = "q")]
    Query,

    /// 成功响应，在线上写作 `y = "r"`。
    #[serde(rename = "r")]
    Response,

    /// 错误响应，在线上写作 `y = "e"`。
    #[serde(rename = "e")]
    Error,
}

/// KRPC 标准错误码。
///
/// 错误消息在线上仍然使用整数；这个 enum 只是避免业务代码到处直接书写 203、204
/// 之类难以理解的数字。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i64)]
pub(crate) enum KrpcErrorCode {
    /// 无法归入更具体类别的通用错误。
    #[allow(dead_code, reason = "保留标准错误码，本节点目前不产生通用错误")]
    Generic = 201,
    /// 节点内部发生错误，暂时无法完成请求。
    Server = 202,
    /// 消息形状或查询参数不符合协议。
    Protocol = 203,
    /// 查询方法不存在，或者当前节点尚未支持。
    MethodUnknown = 204,
}

impl KrpcErrorCode {
    /// 返回写入 KRPC 错误列表的整数值。
    pub(crate) const fn as_i64(self) -> i64 {
        self as i64
    }
}

/// KRPC 查询方法，对应查询消息中的 `q` 字段。
///
/// 常见方法使用明确的 enum 变体，便于后续用 `match` 分派处理逻辑。公网节点也可能
/// 发送私有扩展，因此 [`QueryMethod::Unknown`] 会保留陌生方法的原始名称。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum QueryMethod {
    /// 检查另一个节点是否在线。
    Ping,
    /// 查找 Node ID 最接近目标 ID 的节点。
    FindNode,
    /// 查找正在参与某个种子的 peer，或者取得更接近目标的 DHT 节点。
    GetPeers,
    /// 告诉 DHT 节点：自己正在某个端口参与指定种子。
    AnnouncePeer,
    /// BEP 51：从远端节点获取一批活跃的 info-hash 样本。
    SampleInfohashes,

    /// 尚未支持的标准方法或第三方扩展方法，并保留收到的原始名称。
    Unknown(String),
}

impl QueryMethod {
    /// 返回 KRPC 在线上传输的方法名。
    ///
    /// 未知方法会原样返回，因此消息解码后仍可再次编码而不丢失信息。
    pub(crate) fn as_str(&self) -> &str {
        match self {
            Self::Ping => "ping",
            Self::FindNode => "find_node",
            Self::GetPeers => "get_peers",
            Self::AnnouncePeer => "announce_peer",
            Self::SampleInfohashes => "sample_infohashes",
            Self::Unknown(value) => value,
        }
    }
}

impl From<String> for QueryMethod {
    fn from(value: String) -> Self {
        // 已知名称转成便于匹配的 enum；其他名称完整保存在 Unknown 中。
        match value.as_str() {
            "ping" => Self::Ping,
            "find_node" => Self::FindNode,
            "get_peers" => Self::GetPeers,
            "announce_peer" => Self::AnnouncePeer,
            "sample_infohashes" => Self::SampleInfohashes,
            _ => Self::Unknown(value),
        }
    }
}

impl Serialize for QueryMethod {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // KRPC 要求 q 是普通字符串，不能使用 Serde 默认的 enum 表示法。
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for QueryMethod {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // 先读取线上字符串，再判断它是不是当前认识的查询方法。
        let value = String::deserialize(deserializer)?;
        Ok(Self::from(value))
    }
}

/// 一条完整的 KRPC 消息。
///
/// KRPC 共有查询、响应和错误三种消息。`y` 决定消息类型，其余可选字段只在对应
/// 类型中出现。例如查询通常包含 `q` 和 `a`，响应包含 `r`，错误包含 `e`。
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct KrpcMessage {
    /// 事务 ID。请求方生成，响应方原样返回，用来匹配请求和响应。
    ///
    /// 长度由通信双方约定，通常是一个很短的字节串，因此不能用 [`NodeId`]。
    pub(crate) t: ByteBuf,

    /// 消息类型：`"q"` 表示查询，`"r"` 表示响应，`"e"` 表示错误。
    pub(crate) y: MessageType,

    /// 查询方法名，例如 `ping`、`find_node`、`get_peers` 或 `announce_peer`。
    #[serde(
        default,
        with = "transparent_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) q: Option<QueryMethod>,

    /// 查询参数，对应 KRPC 字典中的 `a`（arguments）。
    #[serde(
        default,
        with = "transparent_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) a: Option<QueryArgs>,

    /// 响应内容，对应 KRPC 字典中的 `r`（response）。
    #[serde(
        default,
        with = "transparent_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) r: Option<ResponseArgs>,

    /// 错误信息：Bencode 列表 `[错误码, 错误说明]`。
    #[serde(
        default,
        with = "transparent_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) e: Option<(i64, ByteBuf)>,

    /// BEP 43：值为 `1` 表示发送方是只读节点，不能被加入 routing table。
    #[serde(
        default,
        with = "transparent_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) ro: Option<u8>,
}

/// KRPC 查询参数。
///
/// 不同查询方法使用的字段不同，只有 `id` 是所有查询都必须携带的。其余字段没有
/// 用到时会保持为 `None`，序列化时也不会出现在 Bencode 字典中。
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct QueryArgs {
    /// 发起查询的节点 ID。
    pub(crate) id: NodeId,

    /// 要查找的目标 ID，仅供 `find_node` 和 `sample_infohashes` 使用。
    #[serde(
        default,
        with = "transparent_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) target: Option<NodeId>,

    /// 要查询或宣布的种子，仅供 `get_peers` 和 `announce_peer` 使用。
    #[serde(
        default,
        with = "transparent_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) info_hash: Option<InfoHashV1>,

    /// peer 接受 BitTorrent 连接的端口（TCP 或 uTP），仅供 `announce_peer` 使用。
    #[serde(
        default,
        with = "transparent_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) port: Option<u16>,

    /// 接收方之前发放的令牌，用来证明这次 `announce_peer` 来自同一 IP。
    #[serde(
        default,
        with = "transparent_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) token: Option<Token>,

    /// 非零时，接收方应使用 UDP 数据包的来源端口，而不是 `port` 字段。
    #[serde(
        default,
        with = "transparent_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) implied_port: Option<u8>,

    /// 希望返回的节点地址类型；`"n4"` 表示 IPv4，`"n6"` 表示 IPv6。
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) want: Vec<String>,
}

/// `get_peers` 与 `announce_peer` 使用的防伪令牌。
///
/// wire 层不解释令牌结构。远端令牌由远端决定；本节点发放和校验的令牌由
/// DHT 的 TokenManager 管理，不在序列化层限制所有节点的令牌长度。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct Token(pub(crate) ByteBuf);

/// KRPC 成功响应中的内容。
///
/// 和 [`QueryArgs`] 一样，具体出现哪些字段取决于所响应的查询方法。
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ResponseArgs {
    /// 返回响应的节点 ID。
    pub(crate) id: NodeId,

    /// `get_peers` 返回的令牌；之后调用 `announce_peer` 时需要带回。
    #[serde(
        default,
        with = "transparent_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) token: Option<Token>,

    /// 连续拼接的 IPv4 compact node records，每条记录占 26 字节。
    ///
    /// 每条记录由 20 字节 Node ID、4 字节 IPv4 地址和 2 字节端口组成。
    #[serde(
        default,
        with = "transparent_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) nodes: Option<CompactNodesV4>,

    /// 连续拼接的 IPv6 compact node records，每条记录占 38 字节。
    ///
    /// 每条记录由 20 字节 Node ID、16 字节 IPv6 地址和 2 字节端口组成。
    #[serde(
        default,
        with = "transparent_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) nodes6: Option<CompactNodesV6>,

    /// 找到的 peer 地址列表，每个元素都是一个 compact peer address。
    ///
    /// IPv4 地址占 6 字节，IPv6 地址占 18 字节；末尾 2 字节都是端口。
    #[serde(
        default,
        with = "transparent_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) values: Option<Vec<CompactPeerAddress>>,

    /// BEP 51：连续拼接的 info-hash 样本，每个样本占 20 字节。
    #[serde(
        default,
        with = "transparent_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) samples: Option<InfoHashSamples>,

    /// BEP 51：建议等待多久再向该节点请求新样本，单位为秒。
    #[serde(
        default,
        with = "transparent_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) interval: Option<u32>,

    /// BEP 51：远端节点保存的 info-hash 总数，不是本次返回的样本数。
    #[serde(
        default,
        with = "transparent_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) num: Option<u64>,
}
