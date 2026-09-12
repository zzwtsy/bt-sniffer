//! 应用生命周期：组装内部模块，监督任务，并在退出前排空持久化队列。
//!
//! run 绑定 socket、创建会话并启动所选能力；收到信号或致命故障后进入收尾。
//! 应用先停止自己持有的引导任务，再由 Session 关闭采集、节点和存储。
mod bootstrap;
pub(crate) mod session;
mod sockets;
#[cfg(test)]
mod tests;

use crate::{
    app::session::Session,
    config::Cli,
    dht::{
        dispatcher::{DhtDispatcherConfig, DhtHandle, SamplerConfig},
        transaction::TransactionManager,
    },
    net::udp::UdpTransport,
    storage::StorageConfig,
};
use std::{future::Future, time::Duration};
use tokio::task::JoinSet;

/// shutdown 由调用者注入，协议测试不需要向测试进程发送真实信号。
pub(crate) async fn run(
    config: Cli,
    shutdown: impl Future<Output = ()>,
) -> Result<(), Vec<String>> {
    let budget = std::sync::Arc::new(
        crate::dht::traffic::Budget::new(config.traffic()).map_err(|e| vec![e.to_string()])?,
    );
    run_with_budget(config, shutdown, budget).await
}
async fn run_with_budget(
    config: Cli,
    shutdown: impl Future<Output = ()>,
    budget: std::sync::Arc<crate::dht::traffic::Budget>,
) -> Result<(), Vec<String>> {
    tokio::pin!(shutdown);
    config
        .traffic()
        .validate()
        .map_err(|e| vec![e.to_string()])?;
    let sockets = sockets::bind(&config).map_err(|error| vec![error])?;
    let directory = config.directory().map_err(|error| vec![error])?;
    let mut session = tokio::select! {
        biased;
        _ = &mut shutdown => return Ok(()),
        result = Session::open_with_traffic(StorageConfig::new(directory), config.traffic()) => result.map_err(|e| vec![e.to_string()])?,
    };
    session.budget = budget;
    let mut handles = Vec::new();
    let startup = tokio::select! {
        biased;
        _ = &mut shutdown => None,
        result = start_nodes(&mut session, &config, sockets, &mut handles) => Some(result),
    };
    let mut errors = Vec::new();
    let mut bootstrap_tasks = JoinSet::new();
    match startup {
        Some(Ok(())) => {
            let seeds = config.seeds();
            if !seeds.is_empty() {
                for handle in &handles {
                    bootstrap_tasks.spawn(bootstrap::run(
                        handle.clone(),
                        seeds.clone(),
                        config.policy(),
                    ));
                }
            }
            let mut summary = tokio::time::interval(Duration::from_secs(60));
            summary.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    biased;
                    _ = &mut shutdown => break,
                    fault = session.next_fault() => {
                        tracing::error!(event="session_fault",schema_version=1u64,error=%fault,"会话故障");
                        if fault.fatal() {
                            errors.push(fault.to_string());
                            break;
                        }
                    }
                    result = bootstrap_tasks.join_next(), if !bootstrap_tasks.is_empty() => {
                        errors.push(format!("引导任务意外结束：{result:?}"));
                        break;
                    }
                    _ = summary.tick() => {
                        session.budget.log();
                        // 日志读取也受退出信号控制，不让慢命令阻止关闭。
                        tokio::select! {
                            biased;
                            _ = &mut shutdown => break,
                            _ = log_status(&handles) => {}
                        }
                    }
                }
            }
        }
        Some(Err(error)) => errors.push(error),
        None => {}
    }
    tracing::info!("停止引导和采样，开始保存最终状态");
    bootstrap_tasks.abort_all();
    while bootstrap_tasks.join_next().await.is_some() {}
    // session 内部共用 30 秒预算；外层不再叠加一轮 30 秒。
    if let Err(cleanup) = session.shutdown().await {
        errors.extend(cleanup);
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

/// 每个成功创建的节点立即交给 session；后续节点失败时，已启动节点仍可统一关闭。
async fn start_nodes(
    session: &mut Session,
    config: &Cli,
    sockets: Vec<UdpTransport>,
    handles: &mut Vec<DhtHandle>,
) -> Result<(), String> {
    for socket in sockets {
        let mut dispatcher = DhtDispatcherConfig::default();
        dispatcher.peer_store.address_policy = config.policy();
        let handle = session
            .add_node(
                &config.instance,
                socket,
                TransactionManager::new(Duration::from_secs(5), 64),
                dispatcher,
                config.policy(),
            )
            .await
            .map_err(|e| e.to_string())?;
        handles.push(handle);
    }
    if config.fetch {
        session
            .start_fetch(crate::collector::Config {
                concurrency: usize::from(config.fetch_concurrency),
                max_active: config.fetch_max_active_jobs as usize,
                sample_backpressure: if config.sample {
                    config.sample_backpressure
                } else {
                    crate::collector::SampleBackpressure::Capacity
                },
                state_max_bytes: config.state_max_bytes,
                directory: config.directory()?,
                policy: config.policy(),
            })
            .await
            .map_err(|e| e.to_string())?;
    }
    if config.sample {
        for index in 0..handles.len() {
            session
                .start_sampling(
                    index,
                    SamplerConfig {
                        address_policy: config.policy(),
                        ..Default::default()
                    },
                )
                .await
                .map_err(|e| e.to_string())?;
        }
    }
    Ok(())
}

async fn log_status(handles: &[DhtHandle]) {
    for handle in handles {
        if let Ok(status) = handle.status().await {
            tracing::info!(address = %status.address, family = ?status.family, node_id = ?status.node_id,
                good = status.good, questionable = status.questionable, pending = status.pending,
                recovery_queued = status.recovery_queued, recovery_active = status.recovery_active,
                sampling = status.sampler.running, samples_ok = status.sampler.successful,
                "节点正在监听；邻居响应不代表公网入站可达");
        }
    }
}

/// 注册信号时的错误在创建数据库之前返回；重复信号不会跳过落盘。
#[cfg(unix)]
pub(crate) fn shutdown_signal() -> std::io::Result<impl Future<Output = ()>> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    Ok(async move {
        tokio::select! {
            _ = interrupt.recv() => {},
            _ = terminate.recv() => {},
        }
    })
}
#[cfg(windows)]
pub(crate) fn shutdown_signal() -> std::io::Result<impl Future<Output = ()>> {
    let mut signal = tokio::signal::windows::ctrl_c()?;
    Ok(async move {
        signal.recv().await;
    })
}
