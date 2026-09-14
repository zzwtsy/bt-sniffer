//! 功能存储测试使用临时数据库；事务与恢复观察沿用真实生产入口。
use super::{DhtStore, RestoredCooldown, SavedContact, identity::load_or_create};
use crate::dht::{NodeId, routing::AddressFamily};
use crate::storage::{Storage, StorageConfig, StorageError};
// 旧预约的迟到结算不能覆盖新预约；到期边界允许复用，未到期时不能驱逐。
#[tokio::test]
async fn cooldown_capacity_expiry_and_stale_completion() {
    let dir = tempfile::tempdir().unwrap();
    let store = Storage::open(StorageConfig::new(dir.path())).await.unwrap();
    let id = load_or_create(
        &DhtStore::new(store.handle.clone()),
        "test",
        AddressFamily::Ipv4,
        0,
    )
    .await
    .unwrap();
    let ip = "8.8.8.8".parse().unwrap();
    let old = DhtStore::new(store.handle.clone())
        .reserve_sampling(id, NodeId([1; 20]), ip, 100, 21_600_000, 1)
        .await
        .unwrap();
    DhtStore::new(store.handle.clone())
        .settle_sampling(old.clone(), 100, 1000, 2)
        .await
        .unwrap();
    assert!(matches!(
        DhtStore::new(store.handle.clone())
            .reserve_sampling(
                id,
                NodeId([2; 20]),
                "9.9.9.9".parse().unwrap(),
                101,
                21_600_000,
                1
            )
            .await,
        Err(StorageError::Capacity)
    ));
    let next = DhtStore::new(store.handle.clone())
        .reserve_sampling(id, NodeId([1; 20]), ip, 1100, 21_600_000, 1)
        .await
        .unwrap();
    DhtStore::new(store.handle.clone())
        .settle_sampling(old, 1100, 1, 0)
        .await
        .unwrap();
    assert!(matches!(
        DhtStore::new(store.handle.clone())
            .reserve_sampling(id, NodeId([1; 20]), ip, 1102, 21_600_000, 1)
            .await,
        Err(StorageError::Cooldown)
    ));
    DhtStore::new(store.handle.clone())
        .settle_sampling(next, 1200, 1000, 3)
        .await
        .unwrap();
    let restored = DhtStore::new(store.handle.clone())
        .restore_cooldowns(id, 1000)
        .await
        .unwrap();
    assert!(restored.iter().any(|value| matches!(
        value,
        RestoredCooldown::Id {
            remaining_ms: 1000,
            failures: 3,
            ..
        }
    )));
    assert!(
        DhtStore::new(store.handle.clone())
            .restore_cooldowns(id, 5000)
            .await
            .unwrap()
            .is_empty()
    );
    store.shutdown().await.unwrap();
}

// 重启加载同一身份；目录锁防止两个进程同时冒用同一个本地节点。
#[tokio::test]
async fn identity_is_stable_and_directory_is_locked() {
    let dir = tempfile::tempdir().unwrap();
    let config = StorageConfig::new(dir.path());
    let store = Storage::open(config.clone()).await.unwrap();
    let first = load_or_create(
        &DhtStore::new(store.handle.clone()),
        "test",
        AddressFamily::Ipv4,
        100,
    )
    .await
    .unwrap();
    assert!(matches!(
        Storage::open(config.clone()).await,
        Err(StorageError::Locked)
    ));
    let v6 = load_or_create(
        &DhtStore::new(store.handle.clone()),
        "test",
        AddressFamily::Ipv6,
        100,
    )
    .await
    .unwrap();
    assert_ne!(first.node_id, v6.node_id);
    store.shutdown().await.unwrap();
    let store = Storage::open(config).await.unwrap();
    let again = load_or_create(
        &DhtStore::new(store.handle.clone()),
        "test",
        AddressFamily::Ipv4,
        200,
    )
    .await
    .unwrap();
    assert_eq!(first.node_id, again.node_id);
    store.shutdown().await.unwrap();
}

// 快照中有一条坏记录时，整个替换回滚，上一份有效快照仍在。
#[tokio::test]
async fn snapshot_replacement_is_atomic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Storage::open(StorageConfig::new(dir.path())).await.unwrap();
    let identity = load_or_create(
        &DhtStore::new(store.handle.clone()),
        "test",
        AddressFamily::Ipv4,
        100,
    )
    .await
    .unwrap();
    let contact = SavedContact {
        id: NodeId([1; 20]),
        address: "127.0.0.1:1234".parse().unwrap(),
        responded_at: 100,
    };
    DhtStore::new(store.handle.clone())
        .save_contacts(identity, std::slice::from_ref(&contact))
        .await
        .unwrap();
    let mut bad = contact.clone();
    bad.address.set_port(0);
    assert!(
        DhtStore::new(store.handle.clone())
            .save_contacts(identity, &[bad])
            .await
            .is_err()
    );
    assert_eq!(
        DhtStore::new(store.handle.clone())
            .load_contacts(identity)
            .await
            .unwrap(),
        [contact]
    );
    store.shutdown().await.unwrap();
}

// 发出请求后未结算便重启，冷却重新保守等待六小时；端口不参与冷却键。
#[tokio::test]
async fn unfinished_reservation_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let config = StorageConfig::new(dir.path());
    let store = Storage::open(config.clone()).await.unwrap();
    let identity = load_or_create(
        &DhtStore::new(store.handle.clone()),
        "test",
        AddressFamily::Ipv4,
        100,
    )
    .await
    .unwrap();
    DhtStore::new(store.handle.clone())
        .reserve_sampling(
            identity,
            NodeId([1; 20]),
            "8.8.8.8".parse().unwrap(),
            100,
            21_600_000,
            10,
        )
        .await
        .unwrap();
    assert!(matches!(
        DhtStore::new(store.handle.clone())
            .reserve_sampling(
                identity,
                NodeId([2; 20]),
                "8.8.8.8".parse().unwrap(),
                101,
                21_600_000,
                10
            )
            .await,
        Err(StorageError::Cooldown)
    ));
    store.shutdown().await.unwrap();
    let store = Storage::open(config).await.unwrap();
    let restored = DhtStore::new(store.handle.clone())
        .restore_cooldowns(identity, 50_000_000)
        .await
        .unwrap();
    assert_eq!(restored.len(), 2);
    assert!(matches!(
        restored[0],
        RestoredCooldown::Id {
            remaining_ms: 21_600_000,
            ..
        } | RestoredCooldown::Ip {
            remaining_ms: 21_600_000,
            ..
        }
    ));
    store.shutdown().await.unwrap();
}

// 已确认未发包时撤销租约，旧撤销不得删除新预约。
#[tokio::test]
async fn unsent_sampling_lease_revocation_is_generation_safe() {
    let dir = tempfile::tempdir().unwrap();
    let store = Storage::open(StorageConfig::new(dir.path())).await.unwrap();
    let id = load_or_create(
        &DhtStore::new(store.handle.clone()),
        "unsent",
        AddressFamily::Ipv4,
        100,
    )
    .await
    .unwrap();
    let ip = "8.8.8.8".parse().unwrap();
    let old = DhtStore::new(store.handle.clone())
        .reserve_sampling(id, NodeId([1; 20]), ip, 100, 21_600_000, 2)
        .await
        .unwrap();
    DhtStore::new(store.handle.clone())
        .abandon_sampling(old.clone())
        .await
        .unwrap();
    let current = DhtStore::new(store.handle.clone())
        .reserve_sampling(id, NodeId([1; 20]), ip, 101, 21_600_000, 2)
        .await
        .unwrap();
    DhtStore::new(store.handle.clone())
        .abandon_sampling(old)
        .await
        .unwrap();
    assert!(
        DhtStore::new(store.handle.clone())
            .reserve_sampling(id, NodeId([1; 20]), ip, 102, 21_600_000, 2)
            .await
            .is_err()
    );
    DhtStore::new(store.handle.clone())
        .abandon_sampling(current)
        .await
        .unwrap();
    assert!(
        DhtStore::new(store.handle.clone())
            .reserve_sampling(id, NodeId([1; 20]), ip, 103, 21_600_000, 2)
            .await
            .is_ok()
    );
    store.shutdown().await.unwrap();
}
