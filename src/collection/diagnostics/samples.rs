//! 所有 worker 共用的 Bendy 说明采样预算；固定窗口限量，不依赖定时后台任务。
use super::{Diagnostics, PeerKey};
use crate::collection::peer::wire::WireError;
use std::time::Duration;
use tokio::time::Instant;

#[derive(Debug, Default)]
pub(super) struct SampleWindow {
    start: Option<Instant>,
    emitted: u8,
}
impl SampleWindow {
    fn admit(&mut self, now: Instant) -> bool {
        if self
            .start
            .is_none_or(|start| now.duration_since(start) >= Duration::from_secs(60))
        {
            self.start = Some(now);
            self.emitted = 0;
        }
        if self.emitted == 8 {
            return false;
        }
        self.emitted += 1;
        true
    }
}
#[derive(Debug, Default, Clone)]
pub(super) struct SampleCounts {
    pub(super) emitted: u64,
    pub(super) suppressed: u64,
}
impl Diagnostics {
    pub(super) fn sample_bencode(&self, key: PeerKey, error: &WireError) {
        let Some(detail) = &error.detail else {
            return;
        };
        let admitted = self
            .sample_window
            .lock()
            .expect("错误样本预算锁")
            .admit(Instant::now());
        {
            let mut pair = self.snapshots.lock().expect("诊断聚合锁");
            let (total, interval) = &mut *pair;
            for snapshot in [total, interval] {
                if admitted {
                    snapshot.samples.emitted += 1;
                } else {
                    snapshot.samples.suppressed += 1;
                }
            }
        }
        if !admitted {
            return;
        }
        // 额度申请和聚合更新后释放全部锁；仅获准样本执行日志格式化。
        tracing::info!(event = "bencode_error_sample",
            schema_version = 2u64,
            stage = ?key.stage,
            source = ?key.source,
            family = if key.ipv6 { "ipv6" } else { "ipv4" },
            reason = ?error.kind,
            detail = %detail.text,
            truncated = detail.truncated,
            unsorted_keys = error.inspection.map(|check| check.unsorted_keys),
            duplicate_keys = error.inspection.map(|check| check.duplicate_keys),
            inspection_status = error.inspection.map(|check| format!("{:?}", check.inspection_status)),
            "Bendy 底层说明样本；不代表错误频数，不用于程序分类"
        );
    }
}
