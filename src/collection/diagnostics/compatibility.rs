//! 扩展握手兼容的帧、会话与实际提交收益；固定计数不保留远端身份。
use super::{Diagnostics, Snapshot};

#[derive(Debug, Clone, Default)]
#[cfg_attr(test, derive(serde::Serialize))]
pub(super) struct CompatibilityCounts {
    pub(super) attempted_frames: u64,
    pub(super) accepted_frames: u64,
    pub(super) rejected_frames: u64,
    pub(super) sessions: u64,
    pub(super) downloaded: u64,
    pub(super) committed: u64,
}
impl Diagnostics {
    /// 正常提交与退出保存仅在 Applied 后调用；不把下载成功当作已落盘。
    pub(crate) fn compatibility_committed(&self) {
        let mut pair = self.snapshots.lock().expect("诊断聚合锁");
        pair.0.compatibility.committed += 1;
        pair.1.compatibility.committed += 1;
    }
}
impl Snapshot {
    pub(super) fn log_compatibility(&self, scope: &str, final_snapshot: bool) {
        let counts = &self.compatibility;
        tracing::info!(
            event = "extension_compatibility_summary",
            schema_version = 1u64,
            scope,
            final_snapshot,
            attempted_frames = counts.attempted_frames,
            accepted_frames = counts.accepted_frames,
            rejected_frames = counts.rejected_frames,
            sessions = counts.sessions,
            downloaded = counts.downloaded,
            committed = counts.committed,
            "扩展握手乱序兼容；帧接纳、会话、下载与 Applied 提交分别统计"
        );
    }
}
