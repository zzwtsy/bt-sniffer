//! 网络目标地址策略；DHT peer 缓存和 metadata TCP 获取使用相同规则。
//!
//! 地址通过策略过滤只表示允许访问，身份与协议内容仍须由 DHT 和 metadata 分别验证。
use std::net::{IpAddr, SocketAddr};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AddressPolicy {
    /// 默认公网服务策略：不接受内网、文档或特殊用途地址。
    PublicOnly,
    /// 显式用于私有部署和 loopback 测试，仍然排除非单播地址。
    LocalUnicast,
}

impl AddressPolicy {
    pub(crate) fn accepts(self, address: SocketAddr) -> bool {
        if address.port() == 0 {
            return false;
        }
        match address.ip() {
            IpAddr::V4(ip) => {
                let [a, b, c, _] = ip.octets();
                if a == 0 || a >= 224 || ip.is_broadcast() {
                    return false;
                }
                if self == Self::LocalUnicast {
                    return true;
                }
                !(ip.is_private()
                    || ip.is_loopback()
                    || ip.is_link_local()
                    || ip.is_documentation()
                    || (a == 100 && (64..=127).contains(&b))
                    || (a == 198 && (18..=19).contains(&b))
                    || (a == 192 && b == 0 && c == 0)
                    || (a == 192 && b == 88 && c == 99))
            }
            IpAddr::V6(ip) => {
                if ip.is_unspecified() || ip.is_multicast() || ip.to_ipv4_mapped().is_some() {
                    return false;
                }
                if self == Self::LocalUnicast {
                    return true;
                }
                let s = ip.segments();
                (s[0] & 0xe000) == 0x2000
                    && !(s[0] == 0x2001 && (s[1] < 0x200 || s[1] == 0xdb8))
                    && s[0] != 0x2002
                    && (s[0] & 0xfff0) != 0x3ff0
            }
        }
    }
}
