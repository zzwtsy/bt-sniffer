//! 网络传输层。
//!
//! 这一层只负责收发网络数据，不处理 routing table、节点查找等 DHT 业务规则。
//!
//! 上层通过 udp 收发 KRPC；address 为自动查询和 TCP 下载提供统一的目的地址过滤。

pub(crate) mod address;
pub(crate) mod udp;
