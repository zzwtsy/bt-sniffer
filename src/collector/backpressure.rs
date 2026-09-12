//! 只控制主动采样，不限制已接纳任务处理或宣布入口；硬容量和存储暂停单独保留。
use crate::storage::jobs::DueStats;

// 两种模式仍保留容量和存储暂停。
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum Mode {
    // 在硬容量之外，用到期任务数量和等待时间抑制主动采样积压。
    Freshness,
    // 只按活跃任务容量迟滞暂停，不判断到期队列的新鲜程度。
    Capacity,
}

/// 协调器独占的主动采样背压状态；三个暂停原因做 OR，不限制已接纳任务的处理。
#[derive(Debug)]
pub(super) struct Backpressure {
    mode: Mode,
    max_active: usize,
    pub(super) capacity: bool,
    pub(super) backlog: bool,
    storage: bool,
    /// 最近一次持续满足低水位条件的起点，UTC 毫秒；条件被打断后清空。
    recovery_since: Option<i64>,
    /// 上次累计暂停耗时的观察点，和 update 的 now 使用同一时钟与毫秒单位。
    last_at: Option<i64>,
    pub(super) paused_ms: u64,
    pub(super) resumes: u64,
}
impl Backpressure {
    pub(super) fn new(mode: Mode, max_active: usize) -> Self {
        Self {
            mode,
            max_active,
            capacity: false,
            backlog: false,
            storage: false,
            recovery_since: None,
            last_at: None,
            paused_ms: 0,
            resumes: 0,
        }
    }
    pub(super) fn paused(&self) -> bool {
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
    /// now 为协调器时钟的 UTC 毫秒；active 为活跃任务数，due 是本次可用的到期快照。
    /// due=None 表示未刷新快照，不表示队列为空，不重置已有积压与恢复起点。
    /// 返回总体暂停状态是否变化，单个原因变化但仍暂停时返回 false。
    /// 调用者每秒检查容量、每五秒提供 due；存储暂停期间保持上次容量判断。
    pub(super) fn update(
        &mut self,
        now: i64,
        active: i64,
        due: Option<&DueStats>,
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
            && let Some(due) = due
        {
            // 数量达到容量的 10% 或最老等待达到 5 分钟时暂停；恢复要求
            // 数量不超过 2%、最老等待不超过 1 分钟，并在快照观察中持续至少 30 秒。
            let high = self.max_active.div_ceil(10).max(1) as i64;
            let low = (self.max_active / 50) as i64;
            if !self.backlog {
                self.backlog = due.count >= high || due.oldest_wait_ms >= 300_000;
            } else if due.count <= low && due.oldest_wait_ms <= 60_000 {
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
    pub(super) fn log(&mut self, now: i64) {
        self.account(now);
        tracing::info!(event="sampling_backpressure",schema_version=1u64,mode=?self.mode, capacity=self.capacity, backlog=self.backlog, storage=self.storage,
            paused_ms=self.paused_ms,resumes=self.resumes,"主动采样背压");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn due(count: i64, oldest_wait_ms: i64) -> DueStats {
        DueStats {
            count,
            oldest_wait_ms,
            fresh: 0,
        }
    }
    #[test]
    fn thresholds_debounce_and_pause_reasons_are_independent() {
        let mut b = Backpressure::new(Mode::Freshness, 10000);
        assert!(!b.update(0, 999, Some(&due(999, 299999)), false));
        assert!(b.update(5000, 1000, Some(&due(1000, 0)), false));
        assert!(!b.update(10000, 200, Some(&due(200, 60000)), false));
        assert!(!b.update(35000, 201, Some(&due(201, 0)), false)); // 中断低水位连续时间
        assert!(!b.update(40000, 0, Some(&due(0, 0)), false));
        assert!(!b.update(65000, 0, Some(&due(0, 0)), false));
        assert!(b.update(70000, 0, Some(&due(0, 0)), false));
        assert_eq!(b.paused_ms, 65000);
        assert_eq!(b.resumes, 1);
        b.update(75000, 10000, Some(&due(0, 300000)), false);
        b.update(80000, 10000, Some(&due(0, 0)), false);
        b.update(110000, 10000, Some(&due(0, 0)), false);
        assert!(!b.backlog && b.capacity && b.paused());
        b.update(115000, 0, None, true);
        assert!(b.paused());
    }
    #[test]
    fn small_capacities_and_legacy_mode() {
        let mut b = Backpressure::new(Mode::Freshness, 1);
        b.update(0, 0, Some(&due(1, 0)), false);
        assert!(b.backlog);
        b.update(5000, 0, Some(&due(0, 0)), false);
        b.update(35000, 0, Some(&due(0, 0)), false);
        assert!(!b.paused());
        let mut b = Backpressure::new(Mode::Capacity, 10000);
        b.update(0, 9999, Some(&due(9999, 86400000)), false);
        assert!(!b.paused());
        b.update(1, 10000, None, false);
        assert!(b.paused());
        b.update(2, 8000, None, false);
        assert!(b.paused());
        b.update(3, 7999, None, false);
        assert!(!b.paused());
    }
}

#[cfg(test)]
mod sqlite_tests {
    use super::*;
    use crate::{
        krpc::InfoHashV1,
        net::address::AddressPolicy,
        storage::{Storage, StorageConfig, jobs::RetryReason},
    };
    #[tokio::test]
    async fn sqlite_due_jobs_drive_backpressure_without_blocking_claims_or_announces() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::open(StorageConfig::new(dir.path())).await.unwrap();
        let store = &storage.handle;
        store.enable_fetch(100);
        let hashes: Vec<_> = (1..=10).map(|n| InfoHashV1([n; 20])).collect();
        store.save_hashes(&hashes, 100).await.unwrap();
        let mut b = Backpressure::new(Mode::Freshness, 100);
        let due = store
            .due_stats(200, AddressPolicy::LocalUnicast)
            .await
            .unwrap();
        assert!(b.update(200, 10, Some(&due), false));
        for _ in 0..8 {
            let job = store.claim_job(200).await.unwrap().unwrap();
            store
                .retry_job(job, 200, RetryReason::Deferred)
                .await
                .unwrap();
        }
        let due = store
            .due_stats(1000, AddressPolicy::LocalUnicast)
            .await
            .unwrap();
        assert_eq!(due.count, 2); // 八个 retry_wait 尚未到期
        b.update(1000, 10, Some(&due), false);
        // 暂停采样不阻止 hint 刷新或历史任务领取。
        assert!(
            store
                .discover_peer(hashes[9], "127.0.0.1:6881".parse().unwrap(), 1000)
                .await
                .unwrap()
        );
        for now in (6000..=31000).step_by(5000) {
            let due = store
                .due_stats(now, AddressPolicy::LocalUnicast)
                .await
                .unwrap();
            b.update(now, 10, Some(&due), false);
        }
        assert!(!b.paused());
        let job = store
            .claim_preferred(31000, Some(true), AddressPolicy::LocalUnicast)
            .await
            .unwrap()
            .unwrap()
            .0;
        assert_eq!(job.hash, hashes[9]);
        store
            .retry_job(job, 31000, RetryReason::Deferred)
            .await
            .unwrap();
        storage.shutdown().await.unwrap();
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
            let mut policy = Backpressure::new(mode, 10000);
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
                let due = DueStats {
                    count: queue.len() as i64,
                    oldest_wait_ms: queue.front().map_or(0, |at| now - *at),
                    fresh: 0,
                };
                policy.update(now, active as i64, (second % 5 == 0).then_some(&due), false);
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
            reports.push(serde_json::json!({"mode":format!("{mode:?}"),"offered":7200,"admitted":admitted,"completed":completed,"peak_active":peak,"waiting":queue.len(),"claim_wait_p95_ms":waits[(waits.len()*95).div_ceil(100)-1],"paused_ms":policy.paused_ms,"resumes":policy.resumes}));
        }
        assert_eq!(reports[0]["completed"], reports[1]["completed"]);
        assert!(
            reports[1]["admitted"].as_u64().unwrap() < reports[0]["admitted"].as_u64().unwrap()
        );
        assert!(reports[1]["peak_active"].as_u64().unwrap() <= 1020);
        println!(
            "BACKPRESSURE_REPORT={}",
            serde_json::json!({"kind":"simulation","duration_ms":1800000,"workers":4,"service_ms":30000,"offered_per_second":4,"results":reports})
        );
    }
}
