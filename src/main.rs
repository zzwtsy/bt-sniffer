//! `bt-sniffer` 可执行程序入口。
//!
//! 协议、网络和 DHT 代码都只供当前可执行程序内部使用，不对其他 crate 提供接口。
//! main 解析参数并创建 runtime，app 组装会话，app::session 监督任务及关闭。
//! 下载流程从 collector 跟进，任务状态和提交规则从 storage::jobs 跟进。
//!
//! 参数解析和信号注册完成后才进入 app；runtime 负责驱动异步任务，SQLite 另有专用线程。
mod app;
mod collector;
mod config;
mod dht;
mod identity;
mod krpc;
mod logging;
mod metadata;
mod metrics;
mod net;
mod peer_wire;
mod storage;

/// 程序入口只负责启动应用，具体协议和 DHT 逻辑由内部模块提供。
fn main() -> std::process::ExitCode {
    use clap::Parser;
    // help/version 在这里结束，不创建 runtime、目录或网络连接。
    let config = match config::Cli::try_parse() {
        Ok(config) => config,
        Err(error) => {
            let code = error.exit_code();
            let _ = error.print();
            return std::process::ExitCode::from(code as u8);
        }
    };
    let _logging = match logging::init() {
        Ok(logging) => logging,
        Err(error) => {
            eprintln!("日志初始化失败：{error}");
            return std::process::ExitCode::FAILURE;
        }
    };
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            tracing::error!(event="runtime_start_failed",schema_version=1u64,error=%error,"无法创建运行时");
            return std::process::ExitCode::FAILURE;
        }
    };
    let result = runtime.block_on(async {
        let shutdown =
            app::shutdown_signal().map_err(|e| vec![format!("无法注册退出信号：{e}")])?;
        app::run(config, shutdown).await
    });
    // 系统 DNS 可能在线程池中阻塞；这里只限制等待时间，并不声称取消了底层 DNS。
    runtime.shutdown_timeout(std::time::Duration::from_secs(1));
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(errors) => {
            for error in errors {
                tracing::error!(event="application_failed",schema_version=1u64,error=%error,"程序退出失败");
            }
            std::process::ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod acceptance;
