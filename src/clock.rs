//! 将进程内单调时间转换为持久化 UTC 期限，不持有数据库资源。
use std::time::Duration;
/// 时钟计算失败；调用边界决定故障归属，不依赖持久化实现。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClockError {
    BeforeEpoch,
    MillisOverflow,
    MonotonicBackwards,
    WallOverflow,
}
impl ClockError {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::BeforeEpoch => "时间早于 Unix epoch",
            Self::MillisOverflow => "时间溢出",
            Self::MonotonicBackwards => "单调时间回退",
            Self::WallOverflow => "UTC 时间溢出",
        }
    }
}
impl std::fmt::Display for ClockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}
impl std::error::Error for ClockError {}

/// UTC 毫秒只用于磁盘；进程内的 deadline 仍用 Instant。
pub(crate) fn unix_millis(time: std::time::SystemTime) -> Result<i64, ClockError> {
    let millis = time
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| ClockError::BeforeEpoch)?
        .as_millis();
    i64::try_from(millis).map_err(|_| ClockError::MillisOverflow)
}

/// 一次运行内以单调时钟推进 UTC，避免系统校时改变已经安排好的协议期限。
#[derive(Debug, Clone)]
pub(crate) struct Clock {
    monotonic: std::time::Instant,
    wall: std::time::SystemTime,
}
impl Default for Clock {
    fn default() -> Self {
        Self::new(
            tokio::time::Instant::now().into_std(),
            std::time::SystemTime::now(),
        )
    }
}
impl Clock {
    pub(crate) fn new(monotonic: std::time::Instant, wall: std::time::SystemTime) -> Self {
        Self { monotonic, wall }
    }
    pub(crate) fn wall_at(
        &self,
        now: std::time::Instant,
    ) -> Result<std::time::SystemTime, ClockError> {
        let elapsed = now
            .checked_duration_since(self.monotonic)
            .ok_or(ClockError::MonotonicBackwards)?;
        self.wall
            .checked_add(elapsed)
            .ok_or(ClockError::WallOverflow)
    }
    pub(crate) fn millis_at(&self, now: std::time::Instant) -> Result<i64, ClockError> {
        // 冷却时间向上取整，不能因毫秒精度损失而比远端允许的时间早发包。
        let wall = self
            .wall_at(now)?
            .checked_add(Duration::from_nanos(999_999))
            .ok_or(ClockError::WallOverflow)?;
        unix_millis(wall)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // UTC 锚点跟随单调时间，暂停时间测试无需真的等待六小时。
    #[tokio::test(start_paused = true)]
    async fn clock_is_injectable_and_rejects_invalid_time() {
        let now = tokio::time::Instant::now().into_std();
        let clock = Clock::new(now, std::time::UNIX_EPOCH + Duration::from_secs(100));
        tokio::time::advance(Duration::from_secs(21600)).await;
        assert_eq!(
            clock
                .millis_at(tokio::time::Instant::now().into_std())
                .unwrap(),
            21_700_000
        );
        assert!(clock.millis_at(now - Duration::from_secs(1)).is_err());
        assert!(unix_millis(std::time::UNIX_EPOCH - Duration::from_secs(1)).is_err());
    }
}
