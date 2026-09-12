//! 用显式时间和固定地址检查登记、响应匹配、容量及取消，不发送网络请求。
use super::*;
use std::net::{Ipv4Addr, SocketAddrV4};

fn address(last_octet: u8) -> SocketAddr {
    SocketAddr::V4(SocketAddrV4::new(
        Ipv4Addr::new(192, 0, 2, last_octet),
        6881,
    ))
}

/// 每次注册应生成不同 ID，并保存查询方法和目标地址。
#[test]
fn register_creates_unique_pending_transactions() {
    let now = Instant::now();
    let mut manager = TransactionManager::with_initial_counter(Duration::from_secs(3), 8, 10);

    let first = manager
        .register(address(1), QueryMethod::Ping, now)
        .expect("第一条请求应该能够注册");
    let second = manager
        .register(address(2), QueryMethod::FindNode, now)
        .expect("第二条请求应该能够注册");

    assert_ne!(first, second);
    assert_eq!(first.as_bytes(), &10_u32.to_be_bytes());
    assert_eq!(manager.len(), 2);
    assert_eq!(manager.next_deadline(), Some(now + Duration::from_secs(3)));
}

/// 只有 transaction ID 和来源地址都匹配时，响应才能完成请求。
#[test]
fn response_must_match_id_and_source_address() {
    let now = Instant::now();
    let mut manager = TransactionManager::new(Duration::from_secs(3), 8);
    let id = manager
        .register(address(1), QueryMethod::Ping, now)
        .expect("请求应该能够注册");

    assert!(matches!(
        manager.complete(id.as_bytes(), address(2), now),
        Err(TransactionError::SourceMismatch { .. })
    ));
    assert_eq!(manager.len(), 1, "错误来源不能消费合法 transaction");

    let completed = manager
        .complete(id.as_bytes(), address(1), now + Duration::from_secs(1))
        .expect("正确来源应该完成请求");
    assert_eq!(completed.transaction.method, QueryMethod::Ping);
    assert!(manager.is_empty());
}

/// 到达 deadline 的请求应被视为超时并从 manager 中删除。
#[test]
fn expired_transactions_are_removed() {
    let now = Instant::now();
    let mut manager = TransactionManager::new(Duration::from_secs(3), 8);
    manager
        .register(address(1), QueryMethod::Ping, now)
        .expect("请求应该能够注册");

    assert!(manager.expire(now + Duration::from_secs(2)).is_empty());
    let expired = manager.expire(now + Duration::from_secs(3));
    assert_eq!(expired.len(), 1);
    assert!(manager.is_empty());
}

/// 达到并发上限后应拒绝新请求，取消旧请求后才能继续注册。
#[test]
fn pending_limit_applies_backpressure() {
    let now = Instant::now();
    let mut manager = TransactionManager::new(Duration::from_secs(3), 1);
    let id = manager
        .register(address(1), QueryMethod::Ping, now)
        .expect("第一条请求应该能够注册");

    assert_eq!(
        manager.register(address(2), QueryMethod::Ping, now),
        Err(TransactionError::AtCapacity { limit: 1 })
    );
    assert!(manager.cancel(id).is_some());
    assert!(manager.register(address(2), QueryMethod::Ping, now).is_ok());
}

/// 长度不正确和未知的 transaction ID 都不能匹配等待中的请求。
#[test]
fn invalid_and_unknown_ids_are_rejected() {
    let now = Instant::now();
    let mut manager = TransactionManager::new(Duration::from_secs(3), 8);

    assert_eq!(
        manager.complete(b"x", address(1), now),
        Err(TransactionError::InvalidTransactionIdLength { actual: 1 })
    );
    assert!(matches!(
        manager.complete(&99_u32.to_be_bytes(), address(1), now),
        Err(TransactionError::UnknownTransaction(_))
    ));
}

/// 同 IPv6 地址从另一端口回包，也不能完成发往 6881 的 transaction。
#[test]
fn changed_ipv6_response_port_preserves_pending_transaction() {
    let now = Instant::now();
    let expected: SocketAddr = "[2001:41d0:203:4cca:5::]:6881".parse().unwrap();
    let actual: SocketAddr = "[2001:41d0:203:4cca:5::]:39499".parse().unwrap();
    let mut manager = TransactionManager::new(Duration::from_secs(3), 8);
    let id = manager.register(expected, QueryMethod::Ping, now).unwrap();
    assert!(matches!(
        manager.complete(id.as_bytes(), actual, now),
        Err(TransactionError::SourceMismatch { .. })
    ));
    assert_eq!(manager.len(), 1);
    manager.complete(id.as_bytes(), expected, now).unwrap();
    assert!(manager.is_empty());
}
