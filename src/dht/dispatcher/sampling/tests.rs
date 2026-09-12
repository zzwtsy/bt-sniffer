//! 用可控 RNG 和暂停的时钟检查缓存，不把“随机结果必须改变”当作正确性条件。
use super::*;
use crate::dht::peer_store::{PeerAddressPolicy, PeerStoreConfig};
use crate::dht::routing::AddressFamily;
use rand::{SeedableRng, rngs::StdRng};

fn store(ttl: Duration, now: Instant) -> PeerStore {
    PeerStore::new(
        PeerStoreConfig {
            ttl,
            address_policy: PeerAddressPolicy::LocalUnicast,
            ..Default::default()
        },
        AddressFamily::Ipv4,
        now,
    )
    .unwrap()
}
fn announce(store: &mut PeerStore, n: u8, now: Instant) {
    store
        .announce(InfoHashV1([n; 20]), "127.0.0.1:6881".parse().unwrap(), now)
        .unwrap();
}
fn now() -> Instant {
    tokio::time::Instant::now().into_std()
}

/// 重复访问不推迟刷新；空快照也等到整整 300 秒后才收录新宣布。
#[tokio::test(start_paused = true)]
async fn empty_cache_and_refresh_boundary_follow_paused_time() {
    let mut peers = store(Duration::from_secs(1800), now());
    let mut cache = SampleCache::default();
    let mut rng = StdRng::seed_from_u64(1);
    assert!(cache.sample(&peers, now(), &mut rng).0.is_empty());
    let generated_at = cache.generated_at;
    announce(&mut peers, 1, now());
    tokio::time::advance(SAMPLE_INTERVAL - Duration::from_nanos(1)).await;
    assert!(cache.sample(&peers, now(), &mut rng).0.is_empty());
    assert_eq!(cache.generated_at, generated_at);
    assert_eq!(peers.active_infohash_count(now()), 1);
    tokio::time::advance(Duration::from_nanos(1)).await;
    assert_eq!(
        cache.sample(&peers, now(), &mut rng).0,
        vec![InfoHashV1([1; 20])]
    );
}

/// 缓存过期项只能删除，不能用新 hash 填空；裁剪返回副本也不能改变缓存。
#[test]
fn cached_samples_are_pruned_without_refill_or_reordering() {
    let start = Instant::now();
    let mut peers = store(Duration::from_secs(10), start);
    let mut cache = SampleCache::default();
    let mut rng = StdRng::seed_from_u64(1);
    announce(&mut peers, 1, start);
    announce(&mut peers, 2, start + Duration::from_secs(1));
    let mut first = cache.sample(&peers, start + Duration::from_secs(1), &mut rng);
    let snapshot = first.clone();
    first.0.clear();
    assert_eq!(
        cache.sample(&peers, start + Duration::from_secs(2), &mut rng),
        snapshot
    );
    announce(&mut peers, 3, start + Duration::from_secs(9));
    assert_eq!(
        cache
            .sample(&peers, start + Duration::from_secs(10), &mut rng)
            .0,
        vec![InfoHashV1([2; 20])]
    );
    assert!(
        cache
            .sample(&peers, start + Duration::from_secs(11), &mut rng)
            .0
            .is_empty()
    );
    announce(&mut peers, 1, start + Duration::from_secs(12));
    assert!(
        cache
            .sample(&peers, start + Duration::from_secs(12), &mut rng)
            .0
            .is_empty(),
        "已经剔除的 hash 也不能在同轮重新加入"
    );
}

/// 32 个名额与缓存中的 hash 数量无关，不会因为存了大量种子而让快照无限增长。
#[test]
fn sample_cache_has_a_hard_size_limit() {
    let start = Instant::now();
    let mut peers = store(Duration::from_secs(1800), start);
    for n in 0..100 {
        announce(&mut peers, n, start);
    }
    let mut cache = SampleCache::default();
    let mut rng = StdRng::seed_from_u64(10);
    let samples = cache.sample(&peers, start, &mut rng);
    assert_eq!(samples.0.len(), MAX_SAMPLES);
    assert_eq!(
        samples
            .0
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len(),
        MAX_SAMPLES
    );
    assert_eq!(
        cache.sample(&peers, start + Duration::from_secs(299), &mut rng),
        samples
    );
}

/// 容量淘汰与 TTL 过期一样会使快照失效，新 hash 不应提前补进同一轮。
#[test]
fn evicted_hash_is_removed_from_cached_samples() {
    let start = Instant::now();
    let mut peers = PeerStore::new(
        PeerStoreConfig {
            max_hashes: 1,
            address_policy: PeerAddressPolicy::LocalUnicast,
            ..Default::default()
        },
        AddressFamily::Ipv4,
        start,
    )
    .unwrap();
    let mut cache = SampleCache::default();
    let mut rng = StdRng::seed_from_u64(1);
    announce(&mut peers, 1, start);
    assert_eq!(
        cache.sample(&peers, start, &mut rng).0,
        vec![InfoHashV1([1; 20])]
    );
    announce(&mut peers, 2, start + Duration::from_secs(1));
    assert!(
        cache
            .sample(&peers, start + Duration::from_secs(1), &mut rng)
            .0
            .is_empty()
    );
    assert_eq!(
        peers.active_infohash_count(start + Duration::from_secs(1)),
        1
    );
    assert_eq!(
        cache.sample(&peers, start + SAMPLE_INTERVAL, &mut rng).0,
        vec![InfoHashV1([2; 20])]
    );
}
