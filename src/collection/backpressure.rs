//! 只控制主动采样，不限制已接纳任务处理或宣布入口；硬容量和存储暂停单独保留。

// 两种模式仍保留容量和存储暂停。
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum Mode {
    // 在硬容量之外，用近期等待首试数量抑制主动采样积压。
    Freshness,
    // 只按活跃任务容量迟滞暂停，不判断到期队列的新鲜程度。
    Capacity,
}

/// 协调器独占的主动采样背压状态；三个暂停原因做 OR，不限制已接纳任务的处理。
#[derive(Debug)]
pub(crate) struct Backpressure {
    mode: Mode,
    max_active: usize,
    recent_limit: usize,
    pub(crate) capacity: bool,
    pub(crate) backlog: bool,
    storage: bool,
    /// 最近一次持续满足低水位条件的起点，UTC 毫秒；条件被打断后清空。
    recovery_since: Option<i64>,
    /// 上次累计暂停耗时的观察点，和 update 的 now 使用同一时钟与毫秒单位。
    last_at: Option<i64>,
    pub(crate) paused_ms: u64,
    pub(crate) resumes: u64,
}
impl Backpressure {
    pub(crate) fn new(mode: Mode, max_active: usize, concurrency: usize) -> Self {
        Self {
            mode,
            max_active,
            recent_limit: max_active.min(concurrency.saturating_mul(4)),
            capacity: false,
            backlog: false,
            storage: false,
            recovery_since: None,
            last_at: None,
            paused_ms: 0,
            resumes: 0,
        }
    }
    pub(crate) fn paused(&self) -> bool {
        self.capacity || self.backlog || self.storage
    }
    fn account(&mut self, now: i64) {
        if let Some(last) = self.last_at
            && self.paused()
        {
            self.paused_ms = self
                .paused_ms
                .saturating_add(now.saturating_sub(last).max(0) as u64);
        }
        self.last_at = Some(now);
    }
    /// now 为协调器时钟 UTC 毫秒；recent 是 Q，含未到期首试，不含 running、有提示和 generation>0 的重试。
    /// recent=None 表示未刷新，不表示近期队列为空。
    /// 返回总体暂停状态是否变化，单个原因变化但仍暂停时返回 false。
    /// 调用者每秒检查容量、每五秒提供 recent；存储暂停期间保持上次容量判断。
    pub(crate) fn update(
        &mut self,
        now: i64,
        active: i64,
        recent: Option<i64>,
        storage: bool,
    ) -> bool {
        self.account(now);
        let old = self.paused();
        self.storage = storage;
        if !storage {
            // 达到上限即暂停，降至 80% 以下才恢复；小容量仍至少保留一个阈值单位。
            self.capacity = if self.capacity {
                active >= (self.max_active as i64 * 8 / 10).max(1)
            } else {
                active >= self.max_active as i64
            };
        }
        if self.mode == Mode::Freshness
            && let Some(recent) = recent
        {
            // 高水位为四倍 worker 数（不超过容量）；低水位为四分之一，持续 30 秒恢复。
            let high = self.recent_limit as i64;
            let low = (self.recent_limit / 4) as i64;
            if !self.backlog {
                self.backlog = recent >= high;
            } else if recent <= low {
                let since = *self.recovery_since.get_or_insert(now);
                if now.saturating_sub(since) >= 30_000 {
                    self.backlog = false;
                    self.recovery_since = None;
                }
            } else {
                self.recovery_since = None;
            }
        }
        if old && !self.paused() {
            self.resumes += 1;
        }
        old != self.paused()
    }
    /// 先累计到 now 的暂停时长再输出；此操作会推进 last_at，不是只读查看。
    pub(crate) fn log(&mut self, now: i64) {
        self.account(now);
        tracing::info!(event = "sampling_backpressure",
            schema_version = 1u64,
            mode = ?self.mode,
            capacity = self.capacity,
            backlog = self.backlog,
            storage = self.storage,
            paused_ms = self.paused_ms,
            resumes = self.resumes,
            "主动采样背压"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn recent_watermarks_and_storage_capacity_are_independent() {
        let mut b = Backpressure::new(Mode::Freshness, 10000, 4);
        assert!(!b.update(0, 9000, Some(0), false)); // 历史积压不阻止发现
        assert!(b.update(5000, 9016, Some(16), false));
        assert!(!b.update(10000, 9004, Some(4), false));
        assert!(!b.update(35000, 9005, Some(5), false)); // 打断恢复观察
        b.update(40000, 9004, Some(4), false);
        assert!(!b.update(65000, 9004, Some(4), false));
        assert!(b.update(70000, 9004, Some(4), false));
        assert_eq!(b.resumes, 1);
        assert!(b.update(75000, 10000, Some(0), false));
        assert!(!b.update(80000, 8000, Some(0), false));
        assert!(b.update(85000, 7999, Some(0), false));
        b.update(90000, 0, Some(0), true);
        assert!(b.paused());
    }
    #[test]
    fn small_capacity_and_capacity_mode() {
        for capacity in 1..=4 {
            let mut b = Backpressure::new(Mode::Freshness, capacity, 4);
            assert!(b.update(0, capacity as i64, Some(capacity as i64), false));
            b.update(5000, 0, Some(0), false);
            assert!(b.update(35000, 0, Some(0), false));
        }
        let mut b = Backpressure::new(Mode::Capacity, 10000, 4);
        assert!(!b.update(0, 9999, Some(9999), false));
    }
}

#[cfg(test)]
mod comparison {
    use super::*;
    use std::collections::VecDeque;
    /// 输入与服务成本固定，只比较主动发现接纳策略；不是网络吞吐测量。
    #[test]
    #[ignore = "独立固定输入 Release 背压模拟"]
    fn sampling_backpressure_release_comparison() {
        let mut reports = Vec::new();
        for mode in [Mode::Capacity, Mode::Freshness] {
            let mut policy = Backpressure::new(mode, 10000, 4);
            let mut queue = VecDeque::<i64>::new();
            let mut workers = [None; 4];
            let mut admitted = 0u64;
            let mut completed = 0u64;
            let mut peak = 0usize;
            let mut waits = Vec::new();
            for second in 0..=1800i64 {
                let now = second * 1000;
                for worker in &mut workers {
                    if worker.is_some_and(|until| until <= now) {
                        *worker = None;
                        completed += 1;
                    }
                }
                let active = queue.len() + workers.iter().filter(|w| w.is_some()).count();
                policy.update(
                    now,
                    active as i64,
                    (second % 5 == 0).then_some(active as i64),
                    false,
                );
                if second < 1800 && !policy.paused() {
                    for _ in 0..4 {
                        if queue.len() + workers.iter().filter(|w| w.is_some()).count() < 10000 {
                            queue.push_back(now);
                            admitted += 1;
                        }
                    }
                }
                for worker in &mut workers {
                    if worker.is_none()
                        && let Some(at) = queue.pop_front()
                    {
                        waits.push(now - at);
                        *worker = Some(now + 30000);
                    }
                }
                peak = peak.max(queue.len() + workers.iter().filter(|w| w.is_some()).count());
            }
            waits.sort_unstable();
            reports.push(serde_json::json!({
                "mode": format!("{mode:?}"),
                "offered": 7200,
                "admitted": admitted,
                "completed": completed,
                "peak_active": peak,
                "waiting": queue.len(),
                "claim_wait_p95_ms": waits[(waits.len() * 95).div_ceil(100) - 1],
                "paused_ms": policy.paused_ms,
                "resumes": policy.resumes,
            }));
        }
        assert_eq!(reports[0]["completed"], reports[1]["completed"]);
        assert!(
            reports[1]["admitted"].as_u64().unwrap() < reports[0]["admitted"].as_u64().unwrap()
        );
        assert!(reports[1]["peak_active"].as_u64().unwrap() <= 1020);
        println!(
            "BACKPRESSURE_REPORT={}",
            serde_json::json!({
                "kind": "simulation",
                "duration_ms": 1800000,
                "workers": 4,
                "service_ms": 30000,
                "offered_per_second": 4,
                "results": reports,
            })
        );
    }
}
