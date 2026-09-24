//! BEP 42：Node ID 的 IP 约束与有界外部地址共识，不拥有 socket 或后台任务。
use crate::{address::AddressPolicy, dht::NodeId};
use serde_bytes::ByteBuf;
use std::{
    net::{IpAddr, SocketAddr},
    time::{Duration, Instant},
};

fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0x82f63b78u32 & 0u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}
fn prefix(ip: IpAddr, r: u8) -> u32 {
    match ip {
        IpAddr::V4(ip) => {
            crc32c(&((u32::from(ip) & 0x030f3fff) | (u32::from(r & 7) << 29)).to_be_bytes())
        }
        IpAddr::V6(ip) => {
            let high = u64::from_be_bytes(ip.octets()[..8].try_into().expect("IPv6 高 64 位"));
            crc32c(&((high & 0x0103070f1f3f7fff) | (u64::from(r & 7) << 61)).to_be_bytes())
        }
    }
}
pub(crate) fn generate(ip: IpAddr, mut random: [u8; 20]) -> NodeId {
    let crc = prefix(ip, random[19]).to_be_bytes();
    random[0] = crc[0];
    random[1] = crc[1];
    random[2] = (crc[2] & 0xf8) | (random[2] & 7);
    NodeId(random)
}
pub(crate) fn valid(id: NodeId, ip: IpAddr) -> bool {
    let crc = prefix(ip, id.0[19]).to_be_bytes();
    id.0[0] == crc[0] && id.0[1] == crc[1] && id.0[2] & 0xf8 == crc[2] & 0xf8
}
pub(crate) fn public(ip: IpAddr) -> bool {
    AddressPolicy::PublicOnly.accepts(SocketAddr::new(ip, 1))
}
pub(crate) fn class(id: NodeId, ip: IpAddr) -> &'static str {
    let exempt = match ip {
        IpAddr::V4(ip) => ip.is_private() || ip.is_loopback() || ip.is_link_local(),
        IpAddr::V6(ip) => ip.is_loopback() || ip.is_unique_local() || ip.is_unicast_link_local(),
    };
    if exempt {
        "local_exempt"
    } else if valid(id, ip) {
        "compliant"
    } else {
        "compatible"
    }
}
pub(crate) fn trusted(id: NodeId, ip: IpAddr) -> bool {
    class(id, ip) != "compatible"
}
pub(crate) fn compact(address: SocketAddr) -> ByteBuf {
    let mut bytes = match address.ip() {
        IpAddr::V4(ip) => ip.octets().to_vec(),
        IpAddr::V6(ip) => ip.octets().to_vec(),
    };
    bytes.extend(address.port().to_be_bytes());
    ByteBuf::from(bytes)
}
pub(crate) fn observed(bytes: &[u8]) -> Option<IpAddr> {
    if !matches!(bytes.len(), 6 | 18) || bytes[bytes.len() - 2..] == [0, 0] {
        return None;
    }
    match bytes.len() {
        6 => Some(IpAddr::V4(std::net::Ipv4Addr::from(
            <[u8; 4]>::try_from(&bytes[..4]).ok()?,
        ))),
        18 => Some(IpAddr::V6(std::net::Ipv6Addr::from(
            <[u8; 16]>::try_from(&bytes[..16]).ok()?,
        ))),
        _ => None,
    }
}
#[derive(Debug)]
struct Vote {
    prefix: Vec<u8>,
    ip: IpAddr,
    at: Instant,
}
#[derive(Debug, Default)]
pub(crate) struct AddressConsensus {
    votes: Vec<Vote>,
    pub(crate) external: Option<IpAddr>,
    pub(crate) confirmed: bool,
    fixed: bool,
    switch_after: Option<Instant>,
}
impl AddressConsensus {
    pub(crate) fn new(fixed: Option<IpAddr>, cached: Option<IpAddr>) -> Self {
        Self {
            external: fixed.or(cached),
            confirmed: fixed.is_some(),
            fixed: fixed.is_some(),
            ..Default::default()
        }
    }
    /// 每来源前缀一票；更新不会扩张容量，同一地址族才可形成共识。
    pub(crate) fn observe(&mut self, source: IpAddr, ip: IpAddr, now: Instant) -> Option<IpAddr> {
        if self.fixed || source.is_ipv4() != ip.is_ipv4() || !public(source) || !public(ip) {
            return None;
        }
        self.votes
            .retain(|v| now.saturating_duration_since(v.at) < Duration::from_secs(600));
        let prefix = match source {
            IpAddr::V4(ip) => ip.octets()[..3].to_vec(),
            IpAddr::V6(ip) => ip.octets()[..6].to_vec(),
        };
        if let Some(v) = self.votes.iter_mut().find(|v| v.prefix == prefix) {
            v.ip = ip;
            v.at = now;
        } else if self.votes.len() < 64 {
            self.votes.push(Vote {
                prefix,
                ip,
                at: now,
            });
        }
        let count = self.votes.iter().filter(|v| v.ip == ip).count();
        if count < 3 || count * 3 < self.votes.len() * 2 {
            return None;
        }
        if self.external == Some(ip) {
            self.confirmed = true;
            return None;
        }
        if self.switch_after.is_some_and(|at| now < at) {
            return None;
        }
        Some(ip)
    }
    pub(crate) fn restore_cooldown(&mut self, remaining: Duration, now: Instant) {
        self.switch_after = now.checked_add(remaining);
    }
    pub(crate) fn committed(&mut self, ip: IpAddr, now: Instant) {
        self.external = Some(ip);
        self.confirmed = true;
        self.switch_after = Some(now + Duration::from_secs(1800));
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ipv6_prefix_votes_expire_and_fixed_configuration_wins() {
        let now = Instant::now();
        let ip = "2001:4860:4860::8888".parse().unwrap();
        let other = "2606:4700:4700::1111".parse().unwrap();
        let mut state = AddressConsensus::default();
        for source in ["2001:4860:1::1", "2001:4860:1:2::1", "2001:4860:2::1"] {
            assert!(state.observe(source.parse().unwrap(), ip, now).is_none());
        }
        assert_eq!(state.votes.len(), 2);
        assert_eq!(
            state.observe("2001:4860:3::1".parse().unwrap(), ip, now),
            Some(ip)
        );
        assert!(
            state
                .observe("2001:4860:4::1".parse().unwrap(), other, now)
                .is_none()
        );
        let later = now + Duration::from_secs(601);
        assert!(
            state
                .observe("2001:4860:5::1".parse().unwrap(), ip, later)
                .is_none()
        );
        assert_eq!(state.votes.len(), 1);
        let mut fixed = AddressConsensus::new(Some(ip), Some(other));
        assert!(fixed.confirmed);
        assert!(
            fixed
                .observe("2001:4860:1::1".parse().unwrap(), other, now)
                .is_none()
        );
        assert_eq!(fixed.external, Some(ip));
        assert!(observed(&[8, 8, 8, 8, 0, 0]).is_none());
    }
    #[test]
    fn official_ipv4_vectors_and_crc32c() {
        assert_eq!(crc32c(b"123456789"), 0xe3069283);
        for (ip, r, prefix) in [
            ("124.31.75.21", 1, [0x5f, 0xbf, 0xbf]),
            ("21.75.31.124", 86, [0x5a, 0x3c, 0xe9]),
            ("65.23.51.170", 22, [0xa5, 0xd4, 0x32]),
            ("84.124.73.14", 65, [0x1b, 0x03, 0x21]),
            ("43.213.53.83", 90, [0xe5, 0x6f, 0x6c]),
        ] {
            let ip = ip.parse().unwrap();
            let mut random = [7; 20];
            random[19] = r;
            let id = generate(ip, random);
            assert_eq!(&id.0[..2], &prefix[..2]);
            assert_eq!(id.0[2] & 0xf8, prefix[2] & 0xf8);
            assert!(valid(id, ip));
        }
    }
    #[test]
    fn ipv6_binding_and_local_exemption() {
        let ip = "2001:4860:4860::8888".parse().unwrap();
        let id = generate(ip, [9; 20]);
        assert!(valid(id, ip));
        let mut bad = id;
        bad.0[0] ^= 1;
        assert!(!valid(bad, ip));
        assert!(trusted(bad, "::1".parse().unwrap()));
    }
    #[test]
    fn consensus_requires_diverse_votes_and_cooldown() {
        let now = Instant::now();
        let ip = "8.8.8.8".parse().unwrap();
        let mut state = AddressConsensus::default();
        for source in ["1.1.1.1", "1.1.1.2", "2.2.2.2"] {
            assert!(state.observe(source.parse().unwrap(), ip, now).is_none());
        }
        assert_eq!(state.observe("3.3.3.3".parse().unwrap(), ip, now), Some(ip));
        state.committed(ip, now);
        let next = "9.9.9.9".parse().unwrap();
        for source in ["1.1.1.1", "2.2.2.2", "3.3.3.3"] {
            assert!(state.observe(source.parse().unwrap(), next, now).is_none());
        }
        let later = now + Duration::from_secs(1801);
        for source in ["1.1.1.1", "2.2.2.2"] {
            assert!(
                state
                    .observe(source.parse().unwrap(), next, later)
                    .is_none()
            );
        }
        assert_eq!(
            state.observe("3.3.3.3".parse().unwrap(), next, later),
            Some(next)
        );
    }
}
