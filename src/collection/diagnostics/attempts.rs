//! 首次／再次领取的成本与提交聚合；按领取类别与提示生成实用摘要，不保存 hash 或领取编号。
use super::{Diagnostics, Distribution, Snapshot, TaskTiming};
use crate::collection::failure::AttemptFailure;
use crate::collection::jobs::AttemptKind;
use crate::collection::jobs::ClaimClass;
use crate::collection::jobs::Job;
use std::{collections::BTreeMap, time::Duration};

/// 领取时冻结的诊断维度；远端失败次数来自同一领取事务，恢复不会伪造一次远端失败。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct AttemptContext {
    pub(crate) kind: AttemptKind,
    pub(crate) class: ClaimClass,
    pub(crate) failed_attempts_before: u32,
    pub(crate) had_valid_hint: bool,
}
impl AttemptContext {
    pub(crate) fn from_job(job: &Job) -> Self {
        Self {
            kind: job.attempt_kind(),
            class: job.class,
            had_valid_hint: job.had_valid_hint,
            failed_attempts_before: job.failed_attempts_before,
        }
    }
}

/// 领取历史视图只区分领取历史与失败次数，省略提示维度不等于提示为 false。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct AttemptHistory {
    pub(super) kind: AttemptKind,
    pub(super) failed_attempts_before: u32,
}
impl AttemptContext {
    pub(super) fn history(self) -> AttemptHistory {
        AttemptHistory {
            kind: self.kind,
            failed_attempts_before: self.failed_attempts_before,
        }
    }
}

/// 只描述整轮执行结果；远端失败原因来自最后一个失败，不代表本轮全部 peer。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum AttemptResult {
    Observed,
    Downloaded,
    RemoteFailure(AttemptFailure),
    LocalDeferral,
    ControlFailure,
    Cancelled,
}
impl AttemptResult {
    fn label(self) -> &'static str {
        match self {
            Self::Observed => "Observed",
            Self::Downloaded => "Downloaded",
            Self::RemoteFailure(_) => "RemoteFailure",
            Self::LocalDeferral => "LocalDeferral",
            Self::ControlFailure => "ControlFailure",
            Self::Cancelled => "Cancelled",
        }
    }
    fn reason(self) -> Option<&'static str> {
        match self {
            Self::RemoteFailure(reason) => Some(reason.label()),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct AttemptViews {
    pub(super) hints: BTreeMap<(AttemptKind, bool), HintSummary>,
    pub(super) timings: BTreeMap<(AttemptHistory, TaskTiming, AttemptResult), Distribution>,
    pub(super) committed: BTreeMap<AttemptHistory, u64>,
    pub(super) class_timings:
        BTreeMap<(AttemptHistory, ClaimClass, TaskTiming, AttemptResult), Distribution>,
    pub(super) class_committed: BTreeMap<(AttemptHistory, ClaimClass), u64>,
}
/// 提示摘要按提示存在性汇总，不拆分领取历史或类别视图的键。
#[derive(Debug, Clone, Default)]
pub(super) struct HintSummary {
    pub(super) claims: u64,
    pub(super) executions: u64,
    pub(super) execution_sum_ms: u64,
    pub(super) downloaded: u64,
    pub(super) committed: u64,
}
/// 唯一持久聚合：领取完整维度只保存一份耗时与独立 Applied 计数。
#[derive(Debug, Clone, Default)]
pub(super) struct AttemptStats {
    pub(super) timings: BTreeMap<(AttemptContext, TaskTiming, AttemptResult), Distribution>,
    pub(super) committed: BTreeMap<AttemptContext, u64>,
}
impl AttemptStats {
    /// 先合并固定桶，再计算分位数；不能平均或相加各子组的分位数。
    pub(super) fn project(&self) -> AttemptViews {
        let mut views = AttemptViews::default();
        for (&(context, timing, result), distribution) in &self.timings {
            let hint = views
                .hints
                .entry((context.kind, context.had_valid_hint))
                .or_default();
            if timing == TaskTiming::DueWait {
                hint.claims += distribution.count;
            }
            if timing == TaskTiming::Execution {
                hint.executions += distribution.count;
                hint.execution_sum_ms = hint.execution_sum_ms.saturating_add(distribution.sum_ms);
                if result == AttemptResult::Downloaded {
                    hint.downloaded += distribution.count;
                }
            }
            let class = context.class;
            let context = context.history();
            views
                .timings
                .entry((context, timing, result))
                .or_default()
                .merge(distribution);
            views
                .class_timings
                .entry((context, class, timing, result))
                .or_default()
                .merge(distribution);
        }
        for (&context, &count) in &self.committed {
            views
                .hints
                .entry((context.kind, context.had_valid_hint))
                .or_default()
                .committed += count;
            let class = context.class;
            let context = context.history();
            *views.committed.entry(context).or_default() += count;
            *views.class_committed.entry((context, class)).or_default() += count;
        }
        views
    }
}
impl Diagnostics {
    pub(crate) fn attempt(
        &self,
        context: AttemptContext,
        timing: TaskTiming,
        result: AttemptResult,
        duration: Duration,
    ) {
        let mut pair = self.snapshots.lock().expect("诊断聚合锁");
        let (total, interval) = &mut *pair;
        for snapshot in [total, interval] {
            snapshot
                .attempts
                .timings
                .entry((context, timing, result))
                .or_default()
                .record(duration);
        }
    }
    /// 事务返回 Applied 后调用；执行成功不能替代实际提交确认。
    pub(crate) fn committed(&self, context: AttemptContext) {
        let mut pair = self.snapshots.lock().expect("诊断聚合锁");
        let (total, interval) = &mut *pair;
        for snapshot in [total, interval] {
            *snapshot.attempts.committed.entry(context).or_default() += 1;
        }
    }
}
impl Snapshot {
    pub(super) fn log_attempts(&self, scope: &str, final_snapshot: bool) {
        let views = self.attempts.project();
        for ((kind, had_valid_hint), summary) in &views.hints {
            tracing::info!(event = "attempt_hint_summary",
                schema_version = 1u64,
                scope,
                final_snapshot,
                attempt_kind = ?kind,
                had_valid_hint,
                claims = summary.claims,
                executions = summary.executions,
                execution_sum_ms = summary.execution_sum_ms,
                downloaded = summary.downloaded,
                committed = summary.committed,
                "领取时提示存在性；与调度类别独立"
            );
        }
        for ((context, timing, result), distribution) in &views.timings {
            tracing::info!(event = "attempt_diagnostic",
                schema_version = 1u64,
                scope,
                final_snapshot,
                attempt_kind = ?context.kind,
                failed_attempts_before = context.failed_attempts_before,
                ?timing,
                result = result.label(),
                reason = result.reason(),
                count = distribution.count,
                sum_ms = distribution.sum_ms,
                p50_upper_bound_ms = distribution.quantile(50),
                p95_upper_bound_ms = distribution.quantile(95),
                p99_upper_bound_ms = distribution.quantile(99),
                p50_exceeds_ms = distribution.exceeds(50),
                p95_exceeds_ms = distribution.exceeds(95),
                p99_exceeds_ms = distribution.exceeds(99),
                overflow = distribution.overflow(),
                "领取历史分类耗时；执行耗时包含网络等待"
            );
        }
        for (context, committed) in &views.committed {
            tracing::info!(event = "attempt_commits",
                schema_version = 1u64,
                scope,
                final_snapshot,
                attempt_kind = ?context.kind,
                failed_attempts_before = context.failed_attempts_before,
                committed,
                "事务 Applied 后的分类提交数"
            );
        }
        for ((context, class, timing, result), distribution) in &views.class_timings {
            tracing::info!(event = "attempt_class_diagnostic",
                schema_version = 1u64,
                scope,
                final_snapshot,
                attempt_kind = ?context.kind,
                claim_class = ?class,
                failed_attempts_before = context.failed_attempts_before,
                ?timing,
                result = result.label(),
                reason = result.reason(),
                count = distribution.count,
                sum_ms = distribution.sum_ms,
                p50_upper_bound_ms = distribution.quantile(50),
                p95_upper_bound_ms = distribution.quantile(95),
                p99_upper_bound_ms = distribution.quantile(99),
                p50_exceeds_ms = distribution.exceeds(50),
                p95_exceeds_ms = distribution.exceeds(95),
                p99_exceeds_ms = distribution.exceeds(99),
                overflow = distribution.overflow(),
                "领取历史分类耗时；执行耗时包含网络等待"
            );
        }
        for ((context, class), committed) in &views.class_committed {
            tracing::info!(event = "attempt_class_commits",
                schema_version = 1u64,
                scope,
                final_snapshot,
                attempt_kind = ?context.kind,
                claim_class = ?class,
                failed_attempts_before = context.failed_attempts_before,
                committed,
                "事务 Applied 后的分类提交数"
            );
        }
        for kind in [AttemptKind::First, AttemptKind::Repeat] {
            let (mut claims, mut executions, mut downloaded, mut execution_sum_ms) =
                (0u64, 0u64, 0u64, 0u64);
            for ((context, timing, result), distribution) in &views.timings {
                if context.kind != kind {
                    continue;
                }
                if *timing == TaskTiming::DueWait {
                    claims += distribution.count;
                }
                if *timing == TaskTiming::Execution {
                    executions += distribution.count;
                    execution_sum_ms = execution_sum_ms.saturating_add(distribution.sum_ms);
                    if *result == AttemptResult::Downloaded {
                        downloaded += distribution.count;
                    }
                }
            }
            let committed: u64 = views
                .committed
                .iter()
                .filter(|(context, _)| context.kind == kind)
                .map(|(_, count)| count)
                .sum();
            let execution_ms_per_commit = execution_sum_ms.checked_div(committed);
            tracing::info!(event = "attempt_summary",
                schema_version = 1u64,
                scope,
                final_snapshot,
                attempt_kind = ?kind,
                claims,
                executions,
                downloaded,
                committed,
                execution_sum_ms,
                execution_ms_per_commit,
                "领取与结束可能跨区间；每份提交成本使用全部已结束执行耗时"
            );
        }
    }
}
