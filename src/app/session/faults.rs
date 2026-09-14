//! 会话故障分类、粘性发布与共享诊断记录；仅同步内存操作，不负责任务关闭。
use super::TaskRole;
use crate::storage::StorageError;
use tokio::sync::watch;
/// 诊断由会话持有；取消正在收尾的任务不会丢失它已经报告的错误。
#[derive(Clone, Default)]
pub(crate) struct FaultLog(std::sync::Arc<std::sync::Mutex<Vec<String>>>);
impl FaultLog {
    pub(crate) fn push(&self, error: String) {
        self.0.lock().expect("会话错误记录锁").push(error);
    }
    pub(super) fn take(&self) -> Vec<String> {
        std::mem::take(&mut *self.0.lock().expect("会话错误记录锁"))
    }
}
/// 可恢复的写入故障只暂停采样；关键任务退出则结束整个会话。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SessionFault {
    StorageWrite(StorageError),
    DatabaseExited,
    CollectorFailed {
        detail: String,
    },
    TaskExited {
        role: TaskRole,
        detail: String,
    },
    TaskFailed {
        cancelled: bool,
        role: TaskRole,
        detail: String,
    },
}
impl SessionFault {
    /// 稳定故障类别；不通过 Display 文案推断恢复策略。
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::StorageWrite(_) => "storage_write",
            Self::DatabaseExited => "database_exited",
            Self::CollectorFailed { .. } => "collector_failed",
            Self::TaskExited { .. } => "task_exited",
            Self::TaskFailed { .. } => "task_failed",
        }
    }
    pub(crate) fn fatal(&self) -> bool {
        !matches!(self, Self::StorageWrite(_))
    }
}
impl std::fmt::Display for SessionFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StorageWrite(error) => write!(f, "存储操作失败，暂停采样：{error}"),
            Self::DatabaseExited => f.write_str("数据库线程意外退出"),
            Self::TaskExited { role, detail } => {
                write!(f, "{role} 意外退出：{detail}")
            }
            Self::TaskFailed {
                role,
                cancelled,
                detail,
            } => {
                write!(
                    f,
                    "{role} {}：{detail}",
                    if *cancelled { "意外取消" } else { "panic" }
                )
            }
            Self::CollectorFailed { detail } => write!(f, "采集协调器失败：{detail}"),
        }
    }
}
/// 会话拥有故障详情；独立的单位通知使 collector 暂停采集。
/// 两种 watch 都合并未消费的变化，不是错误历史；已追加诊断由 FaultLog 保存。
#[derive(Clone)]
pub(crate) struct FaultReporter {
    current: watch::Sender<Option<SessionFault>>,
    pause: watch::Sender<()>,
}
impl FaultReporter {
    pub(crate) fn new() -> (Self, watch::Receiver<Option<SessionFault>>) {
        let (current, errors) = watch::channel(None);
        let (pause, _) = watch::channel(());
        (Self { current, pause }, errors)
    }

    /// 更新故障详情后同步通知暂停；重复故障及致命状态之后的告警不通知。
    pub(super) fn publish(&self, fault: SessionFault) {
        let changed = self.current.send_if_modified(|state| {
            if state.as_ref() == Some(&fault) || state.as_ref().is_some_and(SessionFault::fatal) {
                return false;
            }
            *state = Some(fault);
            true
        });
        if changed {
            // () 的值永远相等，必须使用仍会通知同值更新的操作。
            self.pause.send_replace(());
        }
    }

    /// 交给 collector 在构造末尾订阅；不能提前创建 receiver 而补收启动期间的旧通知。
    pub(crate) fn pause_notifications(&self) -> watch::Sender<()> {
        self.pause.clone()
    }

    /// 回调只做同步内存记录：先保留原始诊断，再发布应用故障，禁止在这里等待 I/O。
    pub(crate) fn collector_callback(
        &self,
        faults: FaultLog,
    ) -> Box<dyn Fn(&crate::collection::CollectorError) + Send + Sync> {
        let report = self.clone();
        Box::new(move |error| {
            faults.push(error.to_string());
            report.publish(classify_collector_error(error));
        })
    }

    /// 发布 DHT 存储故障；不向 FaultLog 逐条追加诊断，重复通知由 publish 合并。
    pub(super) fn storage_callback(&self) -> Box<dyn Fn(StorageError) + Send + Sync> {
        let report = self.clone();
        Box::new(move |error| report.publish(SessionFault::StorageWrite(error)))
    }
}

/// 应用层决定模块错误对应的会话故障；时钟、控制、配置与 worker 错误仍属于致命故障。
pub(super) fn classify_collector_error(error: &crate::collection::CollectorError) -> SessionFault {
    match error {
        crate::collection::CollectorError::Storage(error) => {
            SessionFault::StorageWrite(error.clone())
        }
        _ => SessionFault::CollectorFailed {
            detail: error.to_string(),
        },
    }
}
