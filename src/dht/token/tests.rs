//! 验证 IP 绑定、轮换边界及随机源故障，不依赖真实时间流逝。

use super::*;

fn first_key() -> Result<Secret, TokenError> {
    Ok([0x11; 32])
}
fn second_key() -> Result<Secret, TokenError> {
    Ok([0x22; 32])
}
fn third_key() -> Result<Secret, TokenError> {
    Ok([0x33; 32])
}
fn unavailable() -> Result<Secret, TokenError> {
    Err(TokenError)
}

/// 用独立 Python 标准库 HMAC 计算的固定向量，避免生成和校验一起写错。
#[test]
fn fixed_vector_and_ip_binding() {
    let now = Instant::now();
    let mut manager = TokenManager::with_source(now, first_key).unwrap();
    let ip = "203.0.113.9".parse().unwrap();
    let token = manager.issue(ip, now).unwrap();
    assert_eq!(token.0.len(), 32);
    let hex: String = token.0.iter().map(|byte| format!("{byte:02x}")).collect();
    assert_eq!(
        hex,
        "0eb91ae5b86faa0792241dac381be4150e062577a0540356d28e8d042ba373d6"
    );
    assert!(manager.validate(ip, &token, now).unwrap());
    assert!(
        !manager
            .validate("203.0.113.10".parse().unwrap(), &token, now)
            .unwrap()
    );
    assert!(
        !manager
            .validate("::ffff:203.0.113.9".parse().unwrap(), &token, now)
            .unwrap()
    );
    let v6 = "2001:db8::9".parse().unwrap();
    let token6 = manager.issue(v6, now).unwrap();
    assert!(manager.validate(v6, &token6, now).unwrap());
    assert!(!manager.validate(ip, &token6, now).unwrap());
}

/// 修改一位、截短、加长和空令牌都不能通过认证。
#[test]
fn malformed_tokens_are_rejected() {
    let now = Instant::now();
    let mut manager = TokenManager::with_source(now, first_key).unwrap();
    let ip = "1.1.1.1".parse().unwrap();
    let token = manager.issue(ip, now).unwrap();
    let mut changed = token.0.to_vec();
    changed[7] ^= 1;
    for bytes in [changed, Vec::new(), token.0[..31].to_vec(), vec![0; 33]] {
        assert!(!manager.validate(ip, &Token(bytes.into()), now).unwrap());
    }
}

/// 第一次轮换保留旧密钥，第二次轮换使最初的 token 失效。
#[test]
fn rotation_boundaries_and_late_request_do_not_extend_lifetime() {
    let start = Instant::now();
    let ip = "1.1.1.1".parse().unwrap();
    let mut manager = TokenManager::with_source(start, first_key).unwrap();
    let token = manager
        .issue(ip, start + ROTATION - Duration::from_nanos(1))
        .unwrap();
    manager.key_source = second_key;
    assert!(
        manager
            .validate(ip, &token, start + ROTATION + Duration::from_secs(17))
            .unwrap()
    );
    assert_eq!(manager.epoch, start + ROTATION);
    assert!(
        manager
            .validate(ip, &token, start + ROTATION * 2 - Duration::from_nanos(1))
            .unwrap()
    );
    manager.key_source = third_key;
    assert!(!manager.validate(ip, &token, start + ROTATION * 2).unwrap());
}

/// 长时间没有流量后不能把数小时前的密钥当成“上一轮”密钥。
#[test]
fn long_idle_period_discards_both_old_keys() {
    let start = Instant::now();
    let ip = "1.1.1.1".parse().unwrap();
    let mut manager = TokenManager::with_source(start, first_key).unwrap();
    let token = manager.issue(ip, start).unwrap();
    manager.key_source = second_key;
    assert!(!manager.validate(ip, &token, start + ROTATION * 20).unwrap());
    assert!(manager.previous.is_none());
}

/// 随机源故障必须返回错误，不能继续签发或认可过期密钥；恢复后正常轮换。
#[test]
fn entropy_failure_is_returned_and_recovery_is_possible() {
    let start = Instant::now();
    assert!(TokenManager::with_source(start, unavailable).is_err());
    let ip = "1.1.1.1".parse().unwrap();
    let mut manager = TokenManager::with_source(start, first_key).unwrap();
    let token = manager.issue(ip, start).unwrap();
    manager.key_source = unavailable;
    assert!(manager.issue(ip, start + ROTATION).is_err());
    assert!(manager.validate(ip, &token, start + ROTATION).is_err());
    assert_eq!(manager.epoch, start);
    manager.key_source = second_key;
    assert!(!manager.validate(ip, &token, start + ROTATION * 2).unwrap());
}

/// Debug 可能被 dispatcher 的日志间接调用，不能出现密钥或随机源细节。
#[test]
fn debug_hides_secret_material() {
    let manager = TokenManager::with_source(Instant::now(), first_key).unwrap();
    assert_eq!(format!("{manager:?}"), "TokenManager { .. }");
}

/// 使用与 dispatcher 一样的 Tokio 时钟转换，暂停时间也能准确触发按需轮换。
#[tokio::test(start_paused = true)]
async fn paused_clock_rotates_only_when_token_is_used() {
    let start = tokio::time::Instant::now().into_std();
    let mut manager = TokenManager::with_source(start, first_key).unwrap();
    let ip = "127.0.0.1".parse().unwrap();
    let token = manager.issue(ip, start).unwrap();
    tokio::time::advance(ROTATION).await;
    assert_eq!(manager.epoch, start, "没有请求时不需要独立定时器轮换密钥");
    manager.key_source = second_key;
    assert!(
        manager
            .validate(ip, &token, tokio::time::Instant::now().into_std())
            .unwrap()
    );
    tokio::time::advance(ROTATION).await;
    manager.key_source = third_key;
    assert!(
        !manager
            .validate(ip, &token, tokio::time::Instant::now().into_std())
            .unwrap()
    );
}
