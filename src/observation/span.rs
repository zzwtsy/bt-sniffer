//! 阶段开始、完成、取消请求和 Drop 结算。

use super::{Kind, Observer};
use serde_json::json;
use tokio::time::Instant;

/// 阶段 guard 在普通丢弃时记录取消；事务闭包可将它一起移入，提交后才 finish。
#[derive(Debug)]
pub(crate) struct Span {
    pub(crate) observer: Observer,
    kind: Kind,
    step: &'static str,
    start: Instant,
    finished: bool,
    dropped_result: &'static str,
}

impl Span {
    pub(super) fn new(observer: Observer, kind: Kind, step: &'static str) -> Self {
        observer.emit(kind, step, "started", || json!({}));
        observer.active(step);
        Self {
            observer,
            kind,
            step,
            start: Instant::now(),
            finished: false,
            dropped_result: "cancelled",
        }
    }

    /// 调用者取消仅发出请求，资源退出由持有者的事件另行确认。
    pub(crate) fn cancellation_request_on_drop(&mut self) {
        self.dropped_result = "cancel_requested";
    }

    pub(crate) fn executing(&mut self) {
        self.dropped_result = "failed";
    }

    pub(crate) fn finish(&mut self, result: &'static str) {
        if self.finished {
            return;
        }
        self.observer.emit(
            self.kind,
            self.step,
            result,
            || json!({"elapsed_ms":self.start.elapsed().as_millis() as u64}),
        );
        self.observer.finish_active();
        self.finished = true;
    }
}

impl Drop for Span {
    fn drop(&mut self) {
        self.finish(self.dropped_result);
    }
}
