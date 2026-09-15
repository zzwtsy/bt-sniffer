//! 应用生命周期：组装内部模块，监督任务，并在退出前排空持久化队列。
//!
//! run 绑定 socket、创建会话并启动所选能力；收到信号或致命故障后进入收尾。
//! 应用先停止自己持有的引导任务，再由 Session 关闭采集、节点和存储。
mod bootstrap;
mod collection_config;
use collection_config::CollectionSettings;
pub(crate) mod config;
pub(crate) mod logging;
pub(crate) mod session;
mod sockets;
#[cfg(test)]
mod tests;

use crate::app::config::Cli;
use crate::app::session::Session;
use crate::dht::dispatcher::DhtDispatcherConfig;
use crate::dht::dispatcher::DhtHandle;
use crate::dht::dispatcher::SamplerConfig;
use crate::dht::transaction::TransactionManager;
use crate::dht::udp::UdpTransport;
use crate::storage::StorageConfig;
use std::{future::Future, time::Duration};
use tokio::task::JoinSet;

/// shutdown 由调用者注入，协议测试不需要向测试进程发送真实信号。
pub(crate) async fn run(
    config: Cli,
    shutdown: impl Future<Output = ()>,
    log_diagnostics: &mut logging::QueueDiagnostics,
) -> Result<(), Vec<String>> {
    let settings = CollectionSettings::new(&config);
    let backpressure = settings.backpressure;
    tracing::info!(event = "application_start",
        schema_version = 3u64,
        log_contract_version = 3u64,
        log_filter = log_diagnostics.filter(),
        version = env!("CARGO_PKG_VERSION"),
        admission_policy_version = crate::collection::jobs::admission::POLICY_VERSION,
        scheduling_policy_version = crate::collection::jobs::SCHEDULING_POLICY_VERSION,
        extension_handshake_policy_version = crate::collection::peer::wire::EXTENSION_HANDSHAKE_POLICY_VERSION,
        backpressure_basis = if backpressure == crate::collection::SampleBackpressure::Freshness {
            "first_attempt_waiting"
        } else {
            "capacity"
        },
        sample = config.sample,
        fetch = config.fetch,
        concurrency = config.fetch_concurrency,
        max_active = config.fetch_max_active_jobs,
        max_peer_attempts = crate::collection::MAX_PEER_ATTEMPTS,
        fetch_timeout_ms = settings.metadata.task_timeout.as_millis() as u64,
        peer_timeout_ms = settings.metadata.peer_timeout.as_millis() as u64,
        connect_timeout_ms = settings.metadata.connect_timeout.as_millis() as u64,
        handshake_timeout_ms = settings.metadata.handshake_timeout.as_millis() as u64,
        piece_timeout_ms = settings.metadata.piece_timeout.as_millis() as u64,
        request_window = settings.metadata.request_window,
        max_metadata_size_bytes = settings.metadata.max_metadata_size,
        max_frame_size_bytes = settings.metadata.max_frame_size,
        max_header_size_bytes = settings.metadata.max_header_size,
        max_received_bytes = settings.metadata.max_received_bytes,
        max_depth = settings.metadata.max_depth,
        max_received_frames = settings.metadata.max_received_frames,
        address_policy = ?settings.metadata.address_policy,
        ?backpressure,
        dht_query_rate = config.dht_query_rate,
        dht_inbound_rate = config.dht_inbound_rate,
        dht_upload_bytes_per_sec = config.dht_upload_bytes_per_sec,
        state_max_bytes = config.state_max_bytes,
        "程序有效运行配置"
    );
    let budget = std::sync::Arc::new(
        crate::dht::traffic::Budget::new(config.traffic()).map_err(|e| vec![e.to_string()])?,
    );
    run_prepared(config, shutdown, budget, settings, log_diagnostics).await
}
async fn run_prepared(
    config: Cli,
    shutdown: impl Future<Output = ()>,
    budget: std::sync::Arc<crate::dht::traffic::Budget>,
    settings: CollectionSettings,
    log_diagnostics: &mut logging::QueueDiagnostics,
) -> Result<(), Vec<String>> {
    tokio::pin!(shutdown);
    config
        .traffic()
        .validate()
        .map_err(|e| vec![e.to_string()])?;
    let observer = if config.monitor_listen.is_some() {
        crate::observation::Observer::new(log_diagnostics.run_id().into())
    } else {
        Default::default()
    };
    observer.state("config",||serde_json::json!({"sample":config.sample,"fetch":config.fetch,"fetch_concurrency":config.fetch_concurrency,"fetch_max_active_jobs":config.fetch_max_active_jobs,"dht_query_rate":config.dht_query_rate,"dht_inbound_rate":config.dht_inbound_rate,"dht_upload_bytes_per_sec":config.dht_upload_bytes_per_sec,"state_max_bytes":config.state_max_bytes,"request_window":settings.metadata.request_window,"max_metadata_bytes":settings.metadata.max_metadata_size,"task_timeout_ms":settings.metadata.task_timeout.as_millis() as u64,"peer_timeout_ms":settings.metadata.peer_timeout.as_millis() as u64,"connect_timeout_ms":settings.metadata.connect_timeout.as_millis() as u64,"handshake_timeout_ms":settings.metadata.handshake_timeout.as_millis() as u64,"piece_timeout_ms":settings.metadata.piece_timeout.as_millis() as u64,"read_only":true,"history_persistent":false}));
    let monitor_listener = match config.monitor_listen {
        Some(address) => Some(
            crate::monitor::bind(address)
                .await
                .map_err(|e| vec![e.to_string()])?,
        ),
        None => None,
    };
    if let Some(listener) = &monitor_listener {
        observer.state("monitor",||serde_json::json!({"phase":"starting","listen":listener.local_addr().ok().map(|a|a.to_string())}));
        tracing::info!(address=%listener.local_addr().map_err(|e|vec![e.to_string()])?,"只读监控已绑定");
    }
    let sockets = sockets::bind(&config).map_err(|error| vec![error])?;
    let directory = config.directory().map_err(|error| vec![error])?;
    let mut session = tokio::select! {
        biased;
        _ = &mut shutdown => return Ok(()),
        result = Session::open_observed(StorageConfig::new(directory), budget,observer.clone()) => result.map_err(|e| vec![e.to_string()])?,
    };
    let mut handles = Vec::new();
    let startup = tokio::select! {
        biased;
        _ = &mut shutdown => None,
        result = start_nodes(&mut session, &config, &settings, sockets, &mut handles) => Some(result),
    };
    let mut errors = Vec::new();
    let mut bootstrap_tasks = JoinSet::new();
    match startup {
        Some(Ok(())) => {
            if let Some(listener) = monitor_listener {
                session.start_monitor(listener);
            }
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
                        tracing::error!(
                            event = "session_fault",
                            schema_version = 2u64,
                            phase = "running",
                            kind = fault.kind(),
                            fatal = fault.fatal(),
                            action = if fault.fatal() { "shutdown" } else { "pause_collection" },
                            error = %fault,
                            "会话故障"
                        );
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
                        log_diagnostics.report(false);
                        session.log_traffic();
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
    tracing::info!(
        event = "application_shutdown_started",
        schema_version = 1u64,
        phase = "shutdown",
        "停止引导和采样，开始保存最终状态"
    );
    bootstrap_tasks.abort_all();
    while bootstrap_tasks.join_next().await.is_some() {}
    // session 内部共用 30 秒预算；外层直接等待其收尾结果。
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
    settings: &CollectionSettings,
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
            .start_fetch(crate::collection::Config {
                metadata: settings.metadata.clone(),
                concurrency: usize::from(config.fetch_concurrency),
                max_active: config.fetch_max_active_jobs as usize,
                sample_backpressure: settings.backpressure,
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
            tracing::info!(event = "sampler_diagnostic",
                schema_version = 1u64,
                family = ?status.family,
                running = status.sampler.running,
                collector_paused = status.sampler.collector_paused,
                candidates = status.sampler.candidates,
                in_flight = status.sampler.in_flight,
                successful = status.sampler.successful,
                failed = status.sampler.failed,
                unsupported = status.sampler.unsupported,
                pause = ?status.sampler.pause,
                storage_error = status.sampler.storage_error.is_some(),
                "主动采样状态快照；结果数为累计值"
            );
            tracing::info!(
                event = "node_status",
                schema_version = 1u64,
                phase = "running",
                address = %status.address,
                family = ?status.family,
                node_id = ?status.node_id,
                good = status.good,
                questionable = status.questionable,
                pending = status.pending,
                recovery_queued = status.recovery_queued,
                recovery_active = status.recovery_active,
                sampling = status.sampler.running,
                samples_ok = status.sampler.successful,
                "节点正在监听；邻居响应不代表公网入站可达"
            );
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

/// 测试注入同一个双栈 Budget，不创建后再覆盖资源。
#[cfg(test)]
async fn run_with_budget(
    config: Cli,
    shutdown: impl Future<Output = ()>,
    budget: std::sync::Arc<crate::dht::traffic::Budget>,
) -> Result<(), Vec<String>> {
    let settings = CollectionSettings::new(&config);
    run_prepared(
        config,
        shutdown,
        budget,
        settings,
        &mut logging::test_diagnostics(),
    )
    .await
}
