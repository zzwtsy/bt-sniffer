//! 两端日志投递丢弃的观察值；由应用摘要驱动，不负责 writer 生命周期。
use super::QUEUE_CAPACITY;
use tracing_appender::non_blocking::ErrorCounter;

/// main 持有诊断句柄并借给应用；上次观察值只由这一条主流程更新。
pub(crate) struct QueueDiagnostics {
    pub(super) run_id: String,
    counters: [ErrorCounter; 2],
    previous: [usize; 2],
    filter: String,
}
impl QueueDiagnostics {
    pub(super) fn new(file: ErrorCounter, stderr: ErrorCounter, filter: String) -> Self {
        Self {
            run_id: "test".into(),
            counters: [file, stderr],
            previous: [0; 2],
            filter,
        }
    }
    /// 日志初始化确定的运行标识，供只读观测复用。
    pub(crate) fn run_id(&self) -> &str {
        &self.run_id
    }
    /// 启动实际使用的指令，不重新读取环境变量。
    pub(crate) fn filter(&self) -> &str {
        &self.filter
    }

    /// 快照先于两条事件获取；最终报告不涵盖它自身和之后刷新阶段的丢弃。
    pub(crate) fn report(&mut self, final_snapshot: bool) {
        let totals = self.counters.each_ref().map(ErrorCounter::dropped_lines);
        for (index, sink) in ["file", "stderr"].into_iter().enumerate() {
            let dropped_total = totals[index];
            let dropped_since_last_report = dropped_total.saturating_sub(self.previous[index]);
            self.previous[index] = dropped_total;
            if dropped_since_last_report > 0 {
                tracing::warn!(
                    event = "logging_queue",
                    schema_version = 1u64,
                    sink,
                    queue_capacity = QUEUE_CAPACITY,
                    dropped_total,
                    dropped_since_last_report,
                    final_snapshot,
                    "日志投递发生丢弃"
                );
            } else {
                tracing::info!(
                    event = "logging_queue",
                    schema_version = 1u64,
                    sink,
                    queue_capacity = QUEUE_CAPACITY,
                    dropped_total,
                    dropped_since_last_report,
                    final_snapshot,
                    "日志投递队列观察值"
                );
            }
        }
    }
}

/// 业务测试不安装全局 subscriber；独立计数只作为无日志 I/O 的应用输入。
#[cfg(test)]
pub(crate) fn test_diagnostics() -> QueueDiagnostics {
    let (writer, guard) = tracing_appender::non_blocking(std::io::sink());
    let diagnostics = QueueDiagnostics::new(
        writer.error_counter(),
        writer.error_counter(),
        super::DEFAULT_FILTER.into(),
    );
    drop(guard);
    diagnostics
}
