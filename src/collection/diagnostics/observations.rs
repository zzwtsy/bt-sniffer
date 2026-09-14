//! 任务与单 peer 的观察生命周期；正常结束、取消和丢弃只提交一次计时。
//! 兼容会话标记跟随当前 peer，完整握手独立计时，不从细分阶段分位数推导。
use super::{
    AttemptContext, AttemptResult, Deadline, FailureReason, PeerKey, ResultKind, Source, Stage,
    TaskTiming, connections, failure_reason,
};
use crate::collection::peer::wire::{ExtensionMode, ExtensionUpdate, WireError, WireErrorKind};
use std::sync::Arc;
/// worker 拥有执行计时器；Downloaded 只表示网络结果，提交成功由 Applied 路径另行统计。
pub(crate) struct TaskReport {
    metrics: Arc<crate::collection::diagnostics::metrics::Metrics>,
    attempt: AttemptContext,
    attempt_result: AttemptResult,
    start: tokio::time::Instant,
}
impl TaskReport {
    pub(crate) fn new(
        metrics: Arc<crate::collection::diagnostics::metrics::Metrics>,
        attempt: AttemptContext,
    ) -> Self {
        Self {
            metrics,
            attempt,
            attempt_result: AttemptResult::Cancelled,
            start: tokio::time::Instant::now(),
        }
    }
    pub(crate) fn finish(&mut self, attempt_result: AttemptResult) {
        self.attempt_result = attempt_result;
    }
}
impl Drop for TaskReport {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();
        self.metrics.diagnostics.attempt(
            self.attempt,
            TaskTiming::Execution,
            self.attempt_result,
            elapsed,
        );
    }
}
/// guard 默认记录取消；阶段成功转换和最终失败均只提交一次。
pub(crate) struct PeerObservation {
    /// 此状态只属于一条 peer 会话；切换地址时随 report 重新初始化。
    used_extension_compatibility: bool,
    compatibility_downloaded: bool,
    connection: Option<connections::Observation>,
    connect_failure: Option<FailureReason>,
    metrics: Arc<crate::collection::diagnostics::metrics::Metrics>,
    key: PeerKey,
    start: tokio::time::Instant,
    /// 仅在标准握手开始时设置；跨扩展更新保留，到握手结束后取走。
    handshake_start: Option<tokio::time::Instant>,
    finished: bool,
    failure_recorded: bool,
    /// fetch 总期限的真实触发标记；在丢弃内部 future 前设置。
    task_timeout: Arc<std::sync::atomic::AtomicBool>,
    /// collector 整轮期限的真实触发标记；普通取消不设置。
    outer_timeout: Arc<std::sync::atomic::AtomicBool>,
}
impl PeerObservation {
    pub(crate) fn new(
        metrics: Arc<crate::collection::diagnostics::metrics::Metrics>,
        source: Source,
        ipv6: bool,
        task_timeout: Arc<std::sync::atomic::AtomicBool>,
        outer_timeout: Arc<std::sync::atomic::AtomicBool>,
    ) -> Self {
        Self {
            metrics,
            used_extension_compatibility: false,
            compatibility_downloaded: false,
            connection: None,
            connect_failure: None,
            key: PeerKey {
                stage: Stage::Connect,
                source,
                ipv6,
                result: ResultKind::Cancelled,
                deadline: Deadline::None,
            },
            start: tokio::time::Instant::now(),
            handshake_start: None,
            finished: false,
            failure_recorded: false,
            task_timeout,
            outer_timeout,
        }
    }
    /// 在进入真实 TCP 连接前冻结历史；只用于观测，不能用于选择候选。
    pub(crate) fn observe_connect(
        &mut self,
        address: std::net::SocketAddr,
        attempt: Option<crate::collection::jobs::AttemptKind>,
    ) {
        self.connection = Some(self.metrics.diagnostics.observe_connect(address, attempt));
    }
    /// 在最终失败处附加一次细分；finish 和 Drop 负责原阶段耗时统计。
    pub(crate) fn failure(&mut self, error: &crate::collection::peer::PeerError) {
        if !self.finished && !self.failure_recorded {
            self.metrics
                .diagnostics
                .failure(self.key, failure_reason(error));
            self.failure_recorded = true;
            if self.key.stage == Stage::Connect {
                self.connect_failure = Some(failure_reason(error));
            }
            if let crate::collection::peer::PeerError::Protocol(error) = error {
                self.metrics.diagnostics.sample_bencode(self.key, error);
            }
        }
    }
    pub(crate) fn stage(&self) -> Stage {
        self.key.stage
    }
    pub(crate) fn advance(&mut self, stage: Stage) {
        if self.key.stage == stage {
            return;
        }
        self.finish_detail(ResultKind::Success, Deadline::None);
        if !matches!(stage, Stage::StandardHandshake | Stage::ExtensionHandshake) {
            self.finish_handshake(ResultKind::Success, Deadline::None);
        }
        self.key.stage = stage;
        self.key.result = ResultKind::Cancelled;
        self.key.deadline = Deadline::None;
        self.start = tokio::time::Instant::now();
        if stage == Stage::StandardHandshake {
            self.handshake_start = Some(self.start);
        }
        self.finished = false;
    }
    /// 正常结束与 Drop 共用；Task 截断记超时，普通取消不产生远端失败历史。
    pub(crate) fn finish(&mut self, result: ResultKind, deadline: Deadline) {
        if self.finished {
            return;
        }
        self.finish_handshake(result, deadline);
        self.finish_detail(result, deadline);
    }
    /// 整体握手只在结束时入桶，跨分钟也不切割为多个耗时样本。
    fn finish_handshake(&mut self, result: ResultKind, deadline: Deadline) {
        let Some(start) = self.handshake_start.take() else {
            return;
        };
        let duration = start.elapsed();
        let key = (self.key.source, self.key.ipv6, result, deadline);
        let mut pair = self
            .metrics
            .diagnostics
            .snapshots
            .lock()
            .expect("诊断聚合锁");
        let (total, interval) = &mut *pair;
        for snapshot in [total, interval] {
            snapshot
                .handshakes
                .entry(key)
                .or_insert_with(|| super::Distribution::new(crate::histogram::Buckets::Network))
                .record(duration);
        }
    }
    fn finish_detail(&mut self, result: ResultKind, deadline: Deadline) {
        if self.finished {
            return;
        }
        if result == ResultKind::Timeout && !self.failure_recorded {
            self.metrics
                .diagnostics
                .failure(self.key, FailureReason::Timeout);
            self.failure_recorded = true;
        }
        self.key.result = result;
        self.key.deadline = deadline;
        let elapsed = self.start.elapsed();
        if self.key.stage == Stage::Connect
            && let Some(observation) = self.connection.take()
        {
            self.metrics.diagnostics.finish_connect(
                observation,
                self.key,
                self.connect_failure,
                elapsed,
            );
        }
        self.metrics.diagnostics.record(self.key, elapsed);
        self.finished = true;
    }
}
impl Drop for PeerObservation {
    fn drop(&mut self) {
        if !self.finished {
            if self.task_timeout.load(std::sync::atomic::Ordering::Relaxed)
                || self
                    .outer_timeout
                    .load(std::sync::atomic::Ordering::Relaxed)
            {
                self.finish(ResultKind::Timeout, Deadline::Task);
            } else {
                self.finish(ResultKind::Cancelled, Deadline::None);
            }
        }
    }
}

impl PeerObservation {
    /// 在解析返回后记录一次；帧接纳不表示会话字段一致性检查或下载已完成。
    pub(crate) fn extension_frame(&mut self, parsed: &Result<ExtensionUpdate, WireError>) {
        let accepted = match parsed {
            Ok(update) if update.mode == ExtensionMode::UnsortedCompatible => true,
            Err(error) if error.kind == WireErrorKind::InvalidDictionary => false,
            _ => return,
        };
        let first = accepted && !self.used_extension_compatibility;
        self.used_extension_compatibility |= accepted;
        let mut pair = self
            .metrics
            .diagnostics
            .snapshots
            .lock()
            .expect("诊断聚合锁");
        let (total, interval) = &mut *pair;
        for snapshot in [total, interval] {
            let counts = &mut snapshot.compatibility;
            counts.attempted_frames += 1;
            counts.accepted_frames += u64::from(accepted);
            counts.rejected_frames += u64::from(!accepted);
            counts.sessions += u64::from(first);
        }
    }
    /// 仅完整 metadata 校验成功后调用；返回成功来源会话的标记供 Applied 提交使用。
    pub(crate) fn extension_downloaded(&mut self) -> bool {
        if self.used_extension_compatibility && !self.compatibility_downloaded {
            self.compatibility_downloaded = true;
            let mut pair = self
                .metrics
                .diagnostics
                .snapshots
                .lock()
                .expect("诊断聚合锁");
            pair.0.compatibility.downloaded += 1;
            pair.1.compatibility.downloaded += 1;
        }
        self.used_extension_compatibility
    }
}
