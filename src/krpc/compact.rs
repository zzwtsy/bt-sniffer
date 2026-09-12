//! BitTorrent DHT 的 compact 二进制格式。
//!
//! KRPC 为了减少 UDP 报文大小，会把节点、peer 地址和 info-hash 样本压缩成连续
//! 字节串。本模块负责在这些字节串和便于 Rust 代码使用的强类型之间转换。
//!
//! 紧凑格式只是地址或 hash 的字节表示；解码成功不代表联系人可信或允许联网。

use super::message::{InfoHashV1, NodeId};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_bytes::ByteBuf;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};

/// 一条已经拆解好的 IPv4 compact node record。
///
/// 在线上它占 26 字节：前 20 字节是节点 ID，接着是 4 字节 IP 和 2 字节端口。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompactNodeV4 {
    /// 这个联系人的 DHT 节点 ID。
    pub(crate) id: NodeId,
    /// 这个联系人的 IPv4 地址和 UDP 端口。
    pub(crate) address: SocketAddrV4,
}

/// 一条已经拆解好的 IPv6 compact node record。
///
/// 在线上它占 38 字节：前 20 字节是节点 ID，接着是 16 字节 IP 和 2 字节端口。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompactNodeV6 {
    /// 这个联系人的 DHT 节点 ID。
    pub(crate) id: NodeId,
    /// 这个联系人的 IPv6 地址和 UDP 端口。
    pub(crate) address: SocketAddrV6,
}

/// 一组 IPv4 节点联系人。
///
/// Rust 中使用 `Vec` 方便逐条访问；在线上不是 Bencode 列表，而是把所有 26 字节
/// 记录首尾相接，编码成一个 Bencode 字节串。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompactNodesV4(pub(crate) Vec<CompactNodeV4>);

/// 一组 IPv6 节点联系人。
///
/// Rust 中使用 `Vec`，在线上则把所有 38 字节记录拼成一个 Bencode 字节串。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompactNodesV6(pub(crate) Vec<CompactNodeV6>);

/// `get_peers` 响应中的一个 peer 地址。
///
/// `values` 字段本身是 Bencode 列表，列表中的每一项才是这里表示的 compact
/// address：IPv4 占 6 字节，IPv6 占 18 字节。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CompactPeerAddress {
    /// 4 字节 IPv4 地址加 2 字节端口。
    V4(SocketAddrV4),
    /// 16 字节 IPv6 地址加 2 字节端口。
    V6(SocketAddrV6),
}

/// BEP 51 返回的一组 info-hash 样本。
///
/// Rust 中将每个样本表示为独立的 [`InfoHashV1`]；在线上则把所有 20 字节样本
/// 连续拼接，编码成一个 Bencode 字节串。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InfoHashSamples(pub(crate) Vec<InfoHashV1>);

impl Serialize for CompactNodesV4 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // 每条记录恰好 26 字节，提前分配完整空间可以避免 Vec 反复扩容。
        let mut bytes = Vec::with_capacity(self.0.len() * 26);

        for node in &self.0 {
            // 写入顺序由协议固定：Node ID -> IPv4 -> 大端序端口。
            bytes.extend_from_slice(&node.id.0);
            bytes.extend_from_slice(&node.address.ip().octets());
            bytes.extend_from_slice(&node.address.port().to_be_bytes());
        }

        // 整组记录必须是一个 Bencode 字节串，不能序列化成 Vec 对应的列表。
        serializer.serialize_bytes(&bytes)
    }
}

impl<'de> Deserialize<'de> for CompactNodesV4 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        const RECORD_SIZE: usize = 26;

        // 先让 Serde 从 Bencode 中取出完整字节串。
        let bytes = ByteBuf::deserialize(deserializer)?;
        if bytes.len() % RECORD_SIZE != 0 {
            return Err(serde::de::Error::custom(format_args!(
                "IPv4 compact node 数据长度必须是 {RECORD_SIZE} 的倍数，实际为 {}",
                bytes.len()
            )));
        }

        // 长度检查通过后，每 26 字节切出一条完整记录。
        let (records, remainder) = bytes.as_chunks::<RECORD_SIZE>();
        debug_assert!(remainder.is_empty(), "长度已在上方检查");
        let nodes = records
            .iter()
            .map(|record| {
                // 前 20 字节是 Node ID。
                let mut id = [0_u8; 20];
                id.copy_from_slice(&record[..20]);

                // 随后的 4 字节是 IPv4，最后 2 字节是网络大端序端口。
                let ip = Ipv4Addr::new(record[20], record[21], record[22], record[23]);
                let port = u16::from_be_bytes([record[24], record[25]]);

                CompactNodeV4 {
                    id: NodeId(id),
                    address: SocketAddrV4::new(ip, port),
                }
            })
            .collect();

        Ok(Self(nodes))
    }
}

impl Serialize for CompactNodesV6 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // 每条 IPv6 记录恰好 38 字节。
        let mut bytes = Vec::with_capacity(self.0.len() * 38);

        for node in &self.0 {
            // Compact 格式没有地方保存 flowinfo 和 scope ID，编码前先防止信息丢失。
            ensure_plain_ipv6_address::<S>(&node.address)?;
            // 写入顺序由协议固定：Node ID -> IPv6 -> 大端序端口。
            bytes.extend_from_slice(&node.id.0);
            bytes.extend_from_slice(&node.address.ip().octets());
            bytes.extend_from_slice(&node.address.port().to_be_bytes());
        }

        // 和 IPv4 一样，整组记录在线上是一个字节串。
        serializer.serialize_bytes(&bytes)
    }
}

impl<'de> Deserialize<'de> for CompactNodesV6 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        const RECORD_SIZE: usize = 38;

        // 先读取完整字节串，再检查能否整齐切成 38 字节记录。
        let bytes = ByteBuf::deserialize(deserializer)?;
        if bytes.len() % RECORD_SIZE != 0 {
            return Err(serde::de::Error::custom(format_args!(
                "IPv6 compact node 数据长度必须是 {RECORD_SIZE} 的倍数，实际为 {}",
                bytes.len()
            )));
        }

        let (records, remainder) = bytes.as_chunks::<RECORD_SIZE>();
        debug_assert!(remainder.is_empty(), "长度已在上方检查");
        let nodes = records
            .iter()
            .map(|record| {
                // 前 20 字节是 Node ID。
                let mut id = [0_u8; 20];
                id.copy_from_slice(&record[..20]);

                // 接着复制 16 字节 IPv6，最后读取大端序端口。
                let mut ip = [0_u8; 16];
                ip.copy_from_slice(&record[20..36]);
                let port = u16::from_be_bytes([record[36], record[37]]);

                CompactNodeV6 {
                    id: NodeId(id),
                    // Compact 格式不含 flowinfo 和 scope ID，因此二者初始化为 0。
                    address: SocketAddrV6::new(Ipv6Addr::from(ip), port, 0, 0),
                }
            })
            .collect();

        Ok(Self(nodes))
    }
}

impl Serialize for CompactPeerAddress {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::V4(address) => {
                // IPv4 compact address = 4 字节 IP + 2 字节大端序端口。
                let mut bytes = Vec::with_capacity(6);
                bytes.extend_from_slice(&address.ip().octets());
                bytes.extend_from_slice(&address.port().to_be_bytes());
                serializer.serialize_bytes(&bytes)
            }
            Self::V6(address) => {
                ensure_plain_ipv6_address::<S>(address)?;
                // IPv6 compact address = 16 字节 IP + 2 字节大端序端口。
                let mut bytes = Vec::with_capacity(18);
                bytes.extend_from_slice(&address.ip().octets());
                bytes.extend_from_slice(&address.port().to_be_bytes());
                serializer.serialize_bytes(&bytes)
            }
        }
    }
}

impl<'de> Deserialize<'de> for CompactPeerAddress {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes = ByteBuf::deserialize(deserializer)?;

        // 两种地址的长度不同，因此无需额外的类型标记即可判断 IPv4 或 IPv6。
        match bytes.len() {
            6 => {
                let ip = Ipv4Addr::new(bytes[0], bytes[1], bytes[2], bytes[3]);
                let port = u16::from_be_bytes([bytes[4], bytes[5]]);
                Ok(Self::V4(SocketAddrV4::new(ip, port)))
            }
            18 => {
                let mut ip = [0_u8; 16];
                ip.copy_from_slice(&bytes[..16]);
                let port = u16::from_be_bytes([bytes[16], bytes[17]]);
                Ok(Self::V6(SocketAddrV6::new(Ipv6Addr::from(ip), port, 0, 0)))
            }
            size => Err(serde::de::Error::custom(format_args!(
                "compact peer address 必须是 6 或 18 字节，实际为 {size}"
            ))),
        }
    }
}

impl Serialize for InfoHashSamples {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // 线上没有列表边界，只是依次拼接每个 20 字节 info-hash。
        let mut bytes = Vec::with_capacity(self.0.len() * 20);
        for info_hash in &self.0 {
            bytes.extend_from_slice(&info_hash.0);
        }
        serializer.serialize_bytes(&bytes)
    }
}

impl<'de> Deserialize<'de> for InfoHashSamples {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        const INFO_HASH_SIZE: usize = 20;

        let bytes = ByteBuf::deserialize(deserializer)?;
        if bytes.len() % INFO_HASH_SIZE != 0 {
            return Err(serde::de::Error::custom(format_args!(
                "info-hash 样本数据长度必须是 {INFO_HASH_SIZE} 的倍数，实际为 {}",
                bytes.len()
            )));
        }

        // 长度合法后，每 20 字节恢复成一个具有明确语义的 InfoHashV1。
        let (records, remainder) = bytes.as_chunks::<INFO_HASH_SIZE>();
        debug_assert!(remainder.is_empty(), "长度已在上方检查");
        let samples = records
            .iter()
            .map(|sample| {
                let mut info_hash = [0_u8; INFO_HASH_SIZE];
                info_hash.copy_from_slice(sample);
                InfoHashV1(info_hash)
            })
            .collect();

        Ok(Self(samples))
    }
}

/// 确认 IPv6 地址可以无损写入 compact 格式。
///
/// [`SocketAddrV6`] 还可以携带 `flowinfo` 和 `scope_id`，但 BitTorrent compact
/// address 只给 IP 和端口留了空间。如果这两个值不为零，直接编码会静默丢失信息，
/// 所以这里选择明确报错。
fn ensure_plain_ipv6_address<S>(address: &SocketAddrV6) -> Result<(), S::Error>
where
    S: Serializer,
{
    if address.flowinfo() != 0 || address.scope_id() != 0 {
        return Err(serde::ser::Error::custom(
            "compact IPv6 address 不能包含 flowinfo 或 scope ID",
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bendy::serde::{from_bytes, to_bytes};

    /// 验证一条 IPv4 节点记录会被编码成一个 26 字节的 Bencode 字节串，
    /// 并且解码后仍然得到原来的节点 ID、IP 和端口。
    #[test]
    fn compact_nodes_v4_round_trip_as_one_byte_string() {
        // 准备一个容易辨认的节点：Node ID 全是 7，地址是 192.0.2.1:6881。
        let nodes = CompactNodesV4(vec![CompactNodeV4 {
            id: NodeId([7; 20]),
            address: SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 1), 6881),
        }]);

        // 手动拼出协议规定的结果，用它检查序列化的字节顺序是否正确。
        let encoded = to_bytes(&nodes).expect("IPv4 compact node 应该能够编码");
        let mut expected = b"26:".to_vec();
        expected.extend_from_slice(&[7; 20]);
        expected.extend_from_slice(&[192, 0, 2, 1]);
        expected.extend_from_slice(&6881_u16.to_be_bytes());
        assert_eq!(encoded, expected);

        // 再把字节串解码回来，确认整个过程不会改变数据。
        let decoded: CompactNodesV4 = from_bytes(&encoded).expect("IPv4 compact node 应该能够解码");
        assert_eq!(decoded, nodes);
    }

    /// 验证 IPv6 节点记录在线上是一个 38 字节的 Bencode 字节串，
    /// 并且可以完整地编码后再解码。
    #[test]
    fn compact_nodes_v6_round_trip_as_one_byte_string() {
        let nodes = CompactNodesV6(vec![CompactNodeV6 {
            id: NodeId([9; 20]),
            address: SocketAddrV6::new(Ipv6Addr::LOCALHOST, 6881, 0, 0),
        }]);

        let encoded = to_bytes(&nodes).expect("IPv6 compact node 应该能够编码");
        // `38:` 是 Bencode 字节串的长度前缀，说明后面正好有 38 字节。
        assert!(encoded.starts_with(b"38:"));

        let decoded: CompactNodesV6 = from_bytes(&encoded).expect("IPv6 compact node 应该能够解码");
        assert_eq!(decoded, nodes);
    }

    /// 验证 peer 地址集合会编码成 Bencode 列表，而且列表中的 IPv4、IPv6 地址
    /// 都能正确恢复。
    #[test]
    fn compact_peer_addresses_round_trip_as_a_list_of_byte_strings() {
        // 同时放入 IPv4 和 IPv6，确保两个 enum 分支都经过测试。
        let addresses = vec![
            CompactPeerAddress::V4(SocketAddrV4::new(Ipv4Addr::new(198, 51, 100, 2), 80)),
            CompactPeerAddress::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 443, 0, 0)),
        ];

        let encoded = to_bytes(&addresses).expect("compact peer addresses 应该能够编码");
        // Bencode 列表以 `l` 开头、以 `e` 结尾，第一项是 6 字节 IPv4 地址。
        assert!(encoded.starts_with(b"l6:"));
        assert!(encoded.ends_with(b"e"));

        let decoded: Vec<CompactPeerAddress> =
            from_bytes(&encoded).expect("compact peer addresses 应该能够解码");
        assert_eq!(decoded, addresses);
    }

    /// 验证多个 info-hash 会直接拼成一个字节串，而不是编码成 Bencode 列表。
    #[test]
    fn info_hash_samples_round_trip_as_one_byte_string() {
        // 两个样本各占 20 字节，因此线上字节串的总长度应为 40。
        let samples = InfoHashSamples(vec![InfoHashV1([1; 20]), InfoHashV1([2; 20])]);

        let encoded = to_bytes(&samples).expect("info-hash 样本应该能够编码");
        // `40:` 表示这是一个包含 40 字节内容的 Bencode 字节串。
        assert!(encoded.starts_with(b"40:"));

        let decoded: InfoHashSamples = from_bytes(&encoded).expect("info-hash 样本应该能够解码");
        assert_eq!(decoded, samples);
    }

    /// 验证长度不符合协议的数据会返回错误，而不是被截断或错误解析。
    #[test]
    fn malformed_compact_lengths_are_rejected() {
        // `1:x` 是一个只有 1 字节内容的 Bencode 字节串，不符合下面任何一种格式。
        assert!(from_bytes::<CompactNodesV4>(b"1:x").is_err());
        assert!(from_bytes::<CompactNodesV6>(b"1:x").is_err());
        assert!(from_bytes::<CompactPeerAddress>(b"1:x").is_err());
        assert!(from_bytes::<InfoHashSamples>(b"1:x").is_err());
    }

    /// 验证 compact IPv6 无法表示的附加信息不会被悄悄丢弃。
    #[test]
    fn ipv6_metadata_that_cannot_be_encoded_is_rejected() {
        // flowinfo=1、scope_id=2，但 compact 格式只能保存 IP 和端口。
        let address = CompactPeerAddress::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 6881, 1, 2));

        assert!(to_bytes(&address).is_err());
    }
}
