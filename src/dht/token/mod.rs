//! 写入令牌只证明请求来自曾收到令牌的 IP，不证明 peer 端口可达。
//!
//! announce_peer 校验的是此前向同一 IP 发放的 token；token 不证明远端确实持有文件内容。

use crate::krpc::Token;
use hmac::{Hmac, KeyInit, Mac};
use rand::TryRng;
use sha2::Sha256;
use std::{
    fmt,
    net::IpAddr,
    time::{Duration, Instant},
};

// 每五分钟换一把密钥，只接受当前和上一轮。令牌实际可用约 5～10 分钟，
// 不是从每枚令牌发放时刻起精确计时十分钟。
const ROTATION: Duration = Duration::from_secs(300);
type Secret = [u8; 32];
type KeySource = fn() -> Result<Secret, TokenError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct TokenError;

impl fmt::Display for TokenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("无法从操作系统生成 token 密钥")
    }
}
impl std::error::Error for TokenError {}

pub(super) struct TokenManager {
    current: Secret,
    previous: Option<Secret>,
    epoch: Instant,
    key_source: KeySource,
}

// dispatcher 的 Debug 也会打印其成员，因此这里必须隐藏全部密钥。
impl fmt::Debug for TokenManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenManager").finish_non_exhaustive()
    }
}

fn os_key() -> Result<Secret, TokenError> {
    let mut key = [0; 32];
    rand::rngs::SysRng
        .try_fill_bytes(&mut key)
        .map_err(|_| TokenError)?;
    Ok(key)
}

impl TokenManager {
    /// 仅供故障测试：下次轮换模拟操作系统随机源不可用，不改变已有密钥。
    #[cfg(test)]
    pub(super) fn fail_next_rotations(&mut self) {
        self.key_source = || Err(TokenError);
    }

    pub(super) fn new(now: Instant) -> Result<Self, TokenError> {
        Self::with_source(now, os_key)
    }

    fn with_source(now: Instant, key_source: KeySource) -> Result<Self, TokenError> {
        Ok(Self {
            current: key_source()?,
            previous: None,
            epoch: now,
            key_source,
        })
    }

    fn rotate(&mut self, now: Instant) -> Result<(), TokenError> {
        let elapsed = now.saturating_duration_since(self.epoch);
        if elapsed < ROTATION {
            return Ok(());
        }
        // 先取得新密钥，失败时保留原状态，但本次请求必须返回错误。
        let next = (self.key_source)()?;
        self.previous = (elapsed < ROTATION * 2).then_some(self.current);
        self.current = next;
        // 对齐原周期，不能因为请求晚到而延长旧 token 的寿命。
        self.epoch = now - Duration::from_nanos((elapsed.as_nanos() % ROTATION.as_nanos()) as u64);
        Ok(())
    }

    fn mac(key: &Secret, ip: IpAddr) -> Hmac<Sha256> {
        let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC 接受 32 字节密钥");
        mac.update(b"bt-sniffer/announce-token/v1");
        match ip {
            IpAddr::V4(ip) => {
                mac.update(&[4]);
                mac.update(&ip.octets());
            }
            IpAddr::V6(ip) => {
                mac.update(&[6]);
                mac.update(&ip.octets());
            }
        }
        mac
    }

    /// 仅绑定来源 IP：同 IP 更换 UDP 端口或宣布另一个 hash 时可以复用。
    pub(super) fn issue(&mut self, ip: IpAddr, now: Instant) -> Result<Token, TokenError> {
        self.rotate(now)?;
        Ok(Token(
            Self::mac(&self.current, ip)
                .finalize()
                .into_bytes()
                .to_vec()
                .into(),
        ))
    }

    /// 使用 HMAC 库的恒定时间比较，不自己逐字节比较密钥衍生的 MAC。
    pub(super) fn validate(
        &mut self,
        ip: IpAddr,
        token: &Token,
        now: Instant,
    ) -> Result<bool, TokenError> {
        self.rotate(now)?;
        let current = Self::mac(&self.current, ip).verify_slice(&token.0).is_ok();
        let previous = self
            .previous
            .as_ref()
            .is_some_and(|key| Self::mac(key, ip).verify_slice(&token.0).is_ok());
        Ok(current | previous)
    }
}

#[cfg(test)]
mod tests;
