//! 任务结果消费、运行期故障监督和存储故障停产。

use super::{Session, SessionFault, TaskOutput, TaskPhase, classify_collector_error};
use tokio::task::Id;

impl Session {
    pub(crate) async fn next_fault(&mut self) -> SessionFault {
        loop {
            if self.errors.has_changed().unwrap_or(false) {
                let fault = self.errors.borrow().clone();
                if let Some(fault) = fault {
                    if let SessionFault::StorageWrite(error) = &fault {
                        futures_util::future::join_all(
                            self.nodes
                                .iter()
                                .map(|node| node.handle.pause_for_storage(error.clone())),
                        )
                        .await;
                    }
                    self.errors.borrow_and_update();
                    return fault;
                }
            }
            tokio::select! {
                _ = async { match &mut self.monitor { Some(monitor) => monitor.changed().await, None => std::future::pending::<()>().await } } => {},
                _ = self.storage.as_ref().unwrap().handle.closed() => { self.report.publish(SessionFault::DatabaseExited); }
                result = self.tasks.join_next_with_id(), if !self.tasks.is_empty() => {
                    self.accept_task(result.expect("仍有受监督任务"), TaskPhase::Running);
                }
                _ = async { let mut receiver = self.errors.clone(); let _ = receiver.changed().await; } => {}
            }
        }
    }

    pub(super) fn accept_task(
        &mut self,
        result: Result<(Id, TaskOutput), tokio::task::JoinError>,
        phase: TaskPhase,
    ) {
        let closing = phase == TaskPhase::ShuttingDown;
        let id = match &result {
            Ok((id, _)) => *id,
            Err(error) => error.id(),
        };
        let role = self.roles.remove(&id).expect("每个任务都有角色");
        let fault = match result {
            Ok((_, TaskOutput::Fetch(result))) => match result {
                Err(errors) => {
                    for error in errors {
                        self.report.publish(classify_collector_error(&error));
                    }
                    None
                }
                Ok(()) if !closing => Some(SessionFault::TaskExited {
                    role,
                    detail: "采集协调器提前结束".into(),
                }),
                Ok(()) => None,
            },
            Ok((_, TaskOutput::Dispatcher(exit))) => {
                let detail = match &exit.network_result {
                    Ok(()) => "正常返回但没有收到关闭请求".into(),
                    Err(error) => error.to_string(),
                };
                self.nodes[role.node()].exit = Some(exit);
                (!closing).then_some(SessionFault::TaskExited { role, detail })
            }
            Ok((_, TaskOutput::Snapshot(result))) => match result {
                Err(error) => Some(SessionFault::StorageWrite(error)),
                Ok(()) if !closing => Some(SessionFault::TaskExited {
                    role,
                    detail: "快照任务提前结束".into(),
                }),
                Ok(()) => None,
            },
            Ok((_, TaskOutput::Collector(state))) => {
                let fault = state
                    .error()
                    .cloned()
                    .map(SessionFault::StorageWrite)
                    .or_else(|| {
                        (!closing
                            && !matches!(
                                self.errors.borrow().as_ref(),
                                Some(SessionFault::StorageWrite(_))
                            ))
                        .then_some(SessionFault::TaskExited {
                            role,
                            detail: "采集通道提前关闭".into(),
                        })
                    });
                self.nodes[role.node()].collection = Some(state);
                fault
            }
            Err(error) => Some(SessionFault::TaskFailed {
                cancelled: error.is_cancelled(),
                role,
                detail: error.to_string(),
            }),
        };
        if let Some(fault) = fault {
            self.faults.push(fault.to_string());
            self.report.publish(fault);
        }
    }
}
