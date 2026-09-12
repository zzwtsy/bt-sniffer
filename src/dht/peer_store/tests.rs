//! 验证缓存的期限、容量与索引，不用网络就能重现大量重复宣布。

use super::*;
use rand::{SeedableRng, rngs::StdRng};

/// 采样按种子而不是 peer 计数；部分 peer 过期时，种子仍然有效。
#[test]
fn infohash_sampling_counts_live_keys_and_does_not_renew() {
    let now = Instant::now();
    let mut store = PeerStore::new(config(), AddressFamily::Ipv4, now).unwrap();
    store.announce(hash(1), addr(1), now).unwrap();
    store
        .announce(hash(1), addr(2), now + Duration::from_secs(1))
        .unwrap();
    store.announce(hash(2), addr(1), now).unwrap();
    assert_eq!(store.active_infohash_count(now), 2);
    let samples = store.sample_infohashes(100, now, &mut StdRng::seed_from_u64(1));
    assert_eq!(samples.len(), 2);
    assert!(samples.contains(&hash(1)) && samples.contains(&hash(2)));
    let deadline = store.next_deadline();
    let later = now + config().ttl;
    assert_eq!(store.active_infohash_count(later), 1);
    assert!(store.contains_active_infohash(hash(1), later));
    assert!(!store.contains_active_infohash(hash(2), later));
    assert_eq!(
        store.sample_infohashes(100, later, &mut StdRng::seed_from_u64(2)),
        vec![hash(1)]
    );
    assert_eq!(
        store.next_deadline(),
        deadline,
        "采样不能续期或偷偷执行清理"
    );
    assert_eq!(
        store.active_infohash_count(later + Duration::from_secs(1)),
        0
    );
    assert_indexes(&store);
}

/// 各个 hash 都有机会被选中；容量淘汰后，旧 hash 不得继续出现在样本里。
#[test]
fn infohash_sampling_is_unique_bounded_and_tracks_evictions() {
    let now = Instant::now();
    let mut store = PeerStore::new(config(), AddressFamily::Ipv4, now).unwrap();
    for n in 1..=3 {
        store
            .announce(hash(n), addr(1), now + Duration::from_secs(n.into()))
            .unwrap();
    }
    let mut seen = std::collections::HashSet::new();
    for seed in 0..100 {
        let samples = store.sample_infohashes(
            2,
            now + Duration::from_secs(3),
            &mut StdRng::seed_from_u64(seed),
        );
        assert_eq!(samples.len(), 2);
        assert_ne!(samples[0], samples[1]);
        seen.extend(samples);
    }
    assert_eq!(seen.len(), 3);
    store
        .announce(hash(4), addr(1), now + Duration::from_secs(4))
        .unwrap();
    assert!(!store.contains_active_infohash(hash(1), now + Duration::from_secs(4)));
    assert!(
        !store
            .sample_infohashes(
                100,
                now + Duration::from_secs(4),
                &mut StdRng::seed_from_u64(7)
            )
            .contains(&hash(1))
    );
    assert!(
        store
            .sample_infohashes(0, now, &mut StdRng::seed_from_u64(7))
            .is_empty()
    );
    assert_indexes(&store);
}

fn config() -> PeerStoreConfig {
    PeerStoreConfig {
        ttl: Duration::from_secs(10),
        max_hashes: 3,
        max_peers: 6,
        max_peers_per_hash: 3,
        response_peers: 2,
        address_policy: PeerAddressPolicy::LocalUnicast,
    }
}
fn addr(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}
fn hash(n: u8) -> InfoHashV1 {
    InfoHashV1([n; 20])
}
fn sample(store: &PeerStore, hash: InfoHashV1, now: Instant) -> Vec<SocketAddr> {
    store.sample(hash, 100, now, &mut StdRng::seed_from_u64(7))
}

/// 主数据和两个索引必须一一对应，反复续期也不能留下旧项。
fn assert_indexes(store: &PeerStore) {
    let expiry: BTreeSet<_> = store
        .peers
        .iter()
        .flat_map(|(hash, group)| {
            group
                .iter()
                .map(|(address, deadline)| (*deadline, hash.0, *address))
        })
        .collect();
    let hashes: BTreeSet<_> = store
        .peers
        .iter()
        .map(|(hash, group)| (*group.values().max().unwrap(), hash.0))
        .collect();
    assert_eq!(store.expiry, expiry);
    assert_eq!(store.hashes, hashes);
    assert!(store.peers.len() <= store.config.max_hashes);
    assert!(store.expiry.len() <= store.config.max_peers);
    assert!(
        store
            .peers
            .values()
            .all(|g| !g.is_empty() && g.len() <= store.config.max_peers_per_hash)
    );
}

/// 同一地址重复宣布只续期；读取再多次也不会推迟过期时刻。
#[test]
fn renewal_is_bounded_and_reads_do_not_renew() {
    let start = Instant::now();
    let mut store = PeerStore::new(config(), AddressFamily::Ipv4, start).unwrap();
    for i in 0..1000 {
        store
            .announce(hash(1), addr(1), start + Duration::from_millis(i))
            .unwrap();
        assert_indexes(&store);
        assert_eq!(store.expiry.len(), 1);
    }
    let deadline = start + Duration::from_millis(999) + config().ttl;
    assert_eq!(store.next_deadline(), Some(deadline));
    assert_eq!(
        sample(&store, hash(1), deadline - Duration::from_nanos(1)),
        vec![addr(1)]
    );
    assert!(sample(&store, hash(1), deadline).is_empty());
    assert_eq!(store.next_deadline(), Some(deadline));
    store.expire(deadline, 256);
    assert_eq!(store.next_deadline(), None);
    assert!(store.peers.is_empty());
    assert_indexes(&store);
}

/// hash 和端口都是 peer 身份的一部分，采样不能把它们混在一起。
#[test]
fn hashes_and_ports_are_isolated_and_sampling_has_no_duplicates() {
    let now = Instant::now();
    let mut store = PeerStore::new(config(), AddressFamily::Ipv4, now).unwrap();
    for port in 1..=3 {
        store.announce(hash(1), addr(port), now).unwrap();
    }
    store.announce(hash(2), addr(1), now).unwrap();
    let peers = sample(&store, hash(1), now);
    assert_eq!(peers.len(), 2);
    assert_ne!(peers[0], peers[1]);
    assert!(peers.iter().all(|peer| (1..=3).contains(&peer.port())));
    assert_eq!(sample(&store, hash(2), now), vec![addr(1)]);
    assert!(sample(&store, hash(3), now).is_empty());
    let mut seen = BTreeSet::new();
    for seed in 0..40 {
        seen.extend(store.sample(hash(1), 1, now, &mut StdRng::seed_from_u64(seed)));
    }
    assert_eq!(seen.len(), 3);
    assert_indexes(&store);
}

/// 单个 hash 满时移除最久未宣布的 peer，不能误伤其他 hash。
#[test]
fn per_hash_limit_evicts_oldest_peer() {
    let start = Instant::now();
    let mut store = PeerStore::new(config(), AddressFamily::Ipv4, start).unwrap();
    for port in 1..=4 {
        store
            .announce(
                hash(1),
                addr(port),
                start + Duration::from_secs(port.into()),
            )
            .unwrap();
    }
    assert!(!store.peers[&hash(1)].contains_key(&addr(1)));
    assert!(store.peers[&hash(1)].contains_key(&addr(4)));
    assert_indexes(&store);
}

/// 全局满时移除最旧 peer，达到 hash 上限时按“该 hash 最新宣布”选择淘汰组。
#[test]
fn global_and_hash_limits_use_announcement_recency() {
    let start = Instant::now();
    let mut store = PeerStore::new(config(), AddressFamily::Ipv4, start).unwrap();
    for n in 1..=3 {
        for port in 1..=2 {
            store
                .announce(hash(n), addr(port), start + Duration::from_secs(n.into()))
                .unwrap();
        }
    }
    store
        .announce(hash(3), addr(3), start + Duration::from_secs(4))
        .unwrap();
    assert_eq!(store.peers[&hash(1)].len(), 1);
    assert_indexes(&store);
    store
        .announce(hash(1), addr(9), start + Duration::from_secs(5))
        .unwrap();
    store
        .announce(hash(4), addr(1), start + Duration::from_secs(6))
        .unwrap();
    assert!(!store.peers.contains_key(&hash(2)));
    assert!(store.peers.contains_key(&hash(1)));
    assert_indexes(&store);
}

/// 到期条目优先释放；分批清理必须遵守预算，最后自动删除空 hash。
#[test]
fn expiration_is_bounded_and_precedes_capacity_eviction() {
    let start = Instant::now();
    let mut store = PeerStore::new(config(), AddressFamily::Ipv4, start).unwrap();
    for port in 1..=3 {
        store.announce(hash(1), addr(port), start).unwrap();
    }
    store
        .announce(hash(2), addr(1), start + Duration::from_secs(2))
        .unwrap();
    let deadline = start + config().ttl;
    store.expire(deadline, 0);
    assert_eq!(store.expiry.len(), 4);
    store.expire(deadline, 2);
    assert_eq!(store.expiry.len(), 2);
    assert_eq!(store.next_deadline(), Some(deadline));
    store.announce(hash(3), addr(1), deadline).unwrap();
    assert!(!store.peers.contains_key(&hash(1)));
    assert!(store.peers.contains_key(&hash(2)));
    assert_indexes(&store);
}

/// 非法配置在开始服务前报告，避免容量为零导致运行中索引为空。
#[test]
fn invalid_configurations_are_rejected() {
    let now = Instant::now();
    let base = config();
    for invalid in [
        PeerStoreConfig {
            ttl: Duration::ZERO,
            ..base
        },
        PeerStoreConfig {
            ttl: Duration::MAX,
            ..base
        },
        PeerStoreConfig {
            max_hashes: 0,
            ..base
        },
        PeerStoreConfig {
            max_peers: 0,
            ..base
        },
        PeerStoreConfig {
            max_peers_per_hash: 0,
            ..base
        },
        PeerStoreConfig {
            max_hashes: 7,
            ..base
        },
        PeerStoreConfig {
            max_peers_per_hash: 7,
            ..base
        },
        PeerStoreConfig {
            response_peers: 4,
            ..base
        },
        PeerStoreConfig {
            response_peers: 0,
            ..base
        },
    ] {
        assert!(PeerStore::new(invalid, AddressFamily::Ipv4, now).is_err());
    }
}

/// 默认策略仅保存公网地址；私有部署可以显式允许 loopback，但两种策略都拒绝非单播。
#[test]
fn address_policy_and_family_are_enforced() {
    let public = PeerAddressPolicy::PublicOnly;
    for ip in ["8.8.8.8:1", "[2606:4700:4700::1111]:1"] {
        assert!(public.accepts(ip.parse().unwrap()));
    }
    for ip in [
        "0.0.0.0:1",
        "127.0.0.1:1",
        "10.1.2.3:1",
        "172.16.1.1:1",
        "192.168.1.1:1",
        "100.64.1.1:1",
        "169.254.1.1:1",
        "192.0.2.1:1",
        "198.18.0.1:1",
        "198.51.100.1:1",
        "203.0.113.1:1",
        "224.0.0.1:1",
        "240.0.0.1:1",
        "255.255.255.255:1",
        "8.8.8.8:0",
        "[::]:1",
        "[::1]:1",
        "[fc00::1]:1",
        "[fe80::1]:1",
        "[ff02::1]:1",
        "[2001:db8::1]:1",
        "[2001:2::1]:1",
        "[3fff::1]:1",
        "[::ffff:8.8.8.8]:1",
    ] {
        assert!(!public.accepts(ip.parse().unwrap()), "{ip}");
    }
    let now = Instant::now();
    let mut store = PeerStore::new(config(), AddressFamily::Ipv4, now).unwrap();
    assert!(store.announce(hash(1), addr(1), now).is_ok());
    for ip in ["0.0.0.0:1", "224.0.0.1:1", "255.255.255.255:1", "[::1]:1"] {
        assert!(store.announce(hash(1), ip.parse().unwrap(), now).is_err());
    }
    assert_indexes(&store);
    let mut v6 = PeerStore::new(config(), AddressFamily::Ipv6, now).unwrap();
    assert!(
        v6.announce(hash(1), "[::1]:1".parse().unwrap(), now)
            .is_ok()
    );
    assert!(v6.announce(hash(1), addr(1), now).is_err());
}
