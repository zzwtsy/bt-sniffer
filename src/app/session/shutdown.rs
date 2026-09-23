//! 一个共同期限内的分阶段会话收尾。

use super::{Session, TaskPhase, TaskRole};
use std::time::Duration;

impl Session {
    pub(crate) async fn shutdown(mut self) -> Result<(), Vec<String>> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        if let Some(monitor) = &self.monitor {
            monitor.begin_shutdown();
        }
        self.observer.emit(
            crate::observation::Kind::Lifecycle,
            "shutdown",
            "started",
            || serde_json::json!({}),
        );
        let result = match tokio::time::timeout_at(deadline, self.shutdown_inner()).await {
            Ok(result) => result,
            Err(_) => {
                let mut errors = self.faults.take();
                if let Some(fault) = self.errors.borrow().as_ref() {
                    errors.push(fault.to_string());
                }
                errors.push(format!(
                    "持久化关闭超过 30 秒（阶段：{}）；未确认的结果可能尚未落盘",
                    self.shutdown_stage
                ));
                Err(errors)
            }
        };
        self.observer.emit(
            crate::observation::Kind::Lifecycle,
            "shutdown",
            if result.is_ok() {
                "completed"
            } else {
                "failed"
            },
            || serde_json::json!({}),
        );
        if let Some(monitor) = &mut self.monitor {
            monitor.finish(deadline).await;
        }
        result
    }

    fn enter_shutdown_stage(&mut self, stage: impl Into<String>) {
        self.shutdown_stage = stage.into();
        self.observer.emit(
            crate::observation::Kind::Lifecycle,
            "shutdown_stage",
            "started",
            || serde_json::json!({"stage":self.shutdown_stage}),
        );
    }

    async fn shutdown_inner(&mut self) -> Result<(), Vec<String>> {
        self.enter_shutdown_stage("回收采集协调器");
        self.stop_snapshots.cancel();
        let store = self.collection_store.clone();
        self.stop_fetch.cancel();
        while self
            .roles
            .values()
            .any(|role| *role == TaskRole::FetchCoordinator)
        {
            if let Some(result) = self.tasks.join_next_with_id().await {
                self.accept_task(result, TaskPhase::ShuttingDown);
            }
        }
        self.enter_shutdown_stage("停止节点采样");
        let faults = &self.faults;
        futures_util::future::join_all(self.nodes.iter().enumerate().map(
            |(index, node)| async move {
                if let Err(error) = node.handle.stop_sampling().await {
                    if matches!(
                        error,
                        crate::dht::dispatcher::SamplerError::DispatcherClosed
                    ) {
                        return;
                    }
                    faults.push(format!("节点 {index} 停止采样失败：{error}"));
                }
            },
        ))
        .await;
        self.enter_shutdown_stage("关闭节点");
        let faults = &self.faults;
        futures_util::future::join_all(self.nodes.iter().enumerate().map(
            |(index, node)| async move {
                if let Err(error) = node.handle.shutdown().await {
                    use crate::dht::dispatcher::QueryError;
                    if matches!(
                        error,
                        QueryError::DispatcherClosed | QueryError::ShuttingDown
                    ) {
                        return;
                    }
                    faults.push(format!("节点 {index} 关闭失败：{error}"));
                }
            },
        ))
        .await;
        self.enter_shutdown_stage("回收节点任务");
        while let Some(result) = self.tasks.join_next_with_id().await {
            self.accept_task(result, TaskPhase::ShuttingDown);
        }
        for index in 0..self.nodes.len() {
            self.enter_shutdown_stage(format!("保存节点 {index} 状态"));
            let node = &mut self.nodes[index];
            if let Some(exit) = node.exit.take() {
                if let Err(error) = exit.network_result {
                    self.faults.push(error.to_string());
                }
                if let Err(error) = exit.storage_flush_result {
                    self.faults.push(error.to_string());
                }
                match exit.routing_snapshot {
                    Ok(contacts) => {
                        if let Err(error) =
                            self.dht_store.save_contacts(node.identity, &contacts).await
                        {
                            self.faults.push(error.to_string());
                        }
                    }
                    Err(error) => self.faults.push(error.to_string()),
                }
            }
            if let Some(mut state) = node.collection.take()
                && let Err(error) = state.run(&store).await
            {
                self.faults.push(error.to_string());
            }
        }
        if let Some(error) = self.errors.borrow().as_ref() {
            self.faults.push(error.to_string());
        }
        self.enter_shutdown_stage("关闭数据库（读取最终统计）");
        match store.fetch_stats().await {
            Ok(stats) => stats.log(true),
            Err(error) => self.faults.push(error.to_string()),
        }
        self.budget.log();
        self.enter_shutdown_stage("关闭数据库");
        if let Some(storage) = self.storage.take()
            && let Err(error) = storage.shutdown().await
        {
            self.faults.push(error.to_string());
        }
        let errors = self.faults.take();
        tracing::info!(
            target: "bt_sniffer::app::session",
            event = "session_shutdown",
            schema_version = 1u64,
            success = errors.is_empty(),
            error_count = errors.len(),
            "会话关闭完成"
        );
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}
