//! Tokio 时钟适配与可分离探测、提交的 GCRA 限流器。

use governor::{
    Quota, RateLimiter,
    clock::Clock,
    nanos::Nanos,
    state::{NotKeyed, StateStore},
};
use std::{
    num::NonZeroU32,
    sync::{Arc, Mutex},
    time::Duration,
};

/// governor 使用从 Tokio 单调时钟锚点起的时长，暂停时间测试也能驱动配额。
#[derive(Debug, Clone)]
struct TokioClock(tokio::time::Instant);

impl Clock for TokioClock {
    type Instant = Duration;

    fn now(&self) -> Duration {
        self.0.elapsed()
    }
}

/// 元组为 GCRA 的已提交时间状态与提交开关；探测时计算结果但不写回时间。
/// 外层 Budget 锁覆盖探测和提交全过程，防止中间被其他请求消耗配额。
#[derive(Debug, Clone, Default)]
struct ProbeState(Arc<Mutex<(Option<Nanos>, bool)>>);

impl StateStore for ProbeState {
    type Key = NotKeyed;

    fn measure_and_replace<T, F, E>(&self, _: &NotKeyed, operation: F) -> Result<T, E>
    where
        F: Fn(Option<Nanos>) -> Result<(T, Nanos), E>,
    {
        let mut state = self.0.lock().expect("配额状态锁");
        let (value, next) = operation(state.0)?;
        if state.1 {
            state.0 = Some(next);
        }
        Ok(value)
    }
}

#[derive(Debug)]
pub(super) struct Limiter {
    limiter: RateLimiter<
        NotKeyed,
        ProbeState,
        TokioClock,
        governor::middleware::NoOpMiddleware<Duration>,
    >,
    state: ProbeState,
}

impl Limiter {
    pub(super) fn new(rate: u32) -> Self {
        let state = ProbeState::default();
        // 向上取整每单位间隔，避免非整除速率因纳秒舍入超过约定上界。
        let quota =
            Quota::with_period(Duration::from_nanos(1_000_000_000u64.div_ceil(rate as u64)))
                .unwrap()
                .allow_burst(NonZeroU32::new(rate).unwrap());
        Self {
            limiter: RateLimiter::new(
                quota,
                state.clone(),
                TokioClock(tokio::time::Instant::now()),
            ),
            state,
        }
    }

    /// 返回需要等待的时长；commit=false 只探测，true 才提交成功的扣减。
    /// 单次请求超过突发容量时返回 5 秒等待提示，不表示等待后该请求必定可发送。
    pub(super) fn check(&self, count: u32, commit: bool) -> Duration {
        self.state.0.lock().expect("配额状态锁").1 = commit;
        match self.limiter.check_n(NonZeroU32::new(count.max(1)).unwrap()) {
            Ok(Ok(())) => Duration::ZERO,
            Ok(Err(until)) => until.wait_time_from(self.limiter.clock().now()),
            Err(_) => Duration::from_secs(5),
        }
    }
}
