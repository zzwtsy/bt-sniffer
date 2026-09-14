//! 日志入口：stderr 文本与 UTC 日轮转 JSONL，共用严格 RUST_LOG 过滤。
//! 两端各有有界后台 writer；main 持有 guard 至业务、runtime 和最终诊断结束。
mod diagnostics;
#[cfg(test)]
mod event_tests;
mod format;
#[cfg(test)]
mod output_tests;
#[cfg(test)]
mod writer_tests;
pub(crate) use diagnostics::QueueDiagnostics;
#[cfg(test)]
pub(crate) use diagnostics::test_diagnostics;
use format::RunFormat;
use std::io::IsTerminal;
use tracing_appender::{
    non_blocking::{NonBlockingBuilder, WorkerGuard},
    rolling::{RollingFileAppender, Rotation},
};
use tracing_subscriber::{EnvFilter, prelude::*};

pub(super) const QUEUE_CAPACITY: usize = 4096;
pub(super) const DEFAULT_FILTER: &str = "warn,bt_sniffer=info";

/// guard 的有限退出等待不代表持久落盘；诊断句柄不拥有后台 writer。
pub(crate) struct Logging {
    _file_guard: WorkerGuard,
    _stderr_guard: WorkerGuard,
}

/// 过滤先于文件资源初始化；非空 RUST_LOG 完整替换默认指令。
fn read_filter() -> Result<(EnvFilter, String), String> {
    let directive = match std::env::var("RUST_LOG") {
        Ok(value) if !value.is_empty() => value,
        Ok(_) | Err(std::env::VarError::NotPresent) => DEFAULT_FILTER.into(),
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err("RUST_LOG 必须是 Unicode 字符串".into());
        }
    };
    let filter = EnvFilter::builder()
        .parse(&directive)
        .map_err(|error| format!("RUST_LOG 过滤指令无效：{error}"))?;
    Ok((filter, directive))
}

/// 在 runtime 前初始化；目录属于工作目录，不随状态目录变化，不支持多进程共写。
pub(crate) fn init() -> Result<(Logging, QueueDiagnostics), String> {
    let (filter, directive) = read_filter()?;
    let run_id = format!("{:032x}", rand::random::<u128>());
    let directory = std::env::current_dir()
        .map_err(|error| format!("无法确定日志工作目录：{error}"))?
        .join("logs");
    std::fs::create_dir_all(&directory)
        .map_err(|error| format!("无法创建日志目录 {}：{error}", directory.display()))?;
    let file = RollingFileAppender::builder()
        .rotation(Rotation::DAILY)
        .filename_prefix("bt-sniffer")
        .filename_suffix("jsonl")
        .max_log_files(7)
        .build(&directory)
        .map_err(|error| format!("无法创建日志目录或文件 {}：{error}", directory.display()))?;
    let (file_writer, file_guard) = NonBlockingBuilder::default()
        .buffered_lines_limit(QUEUE_CAPACITY)
        .lossy(true)
        .finish(file);
    let (stderr_writer, stderr_guard) = NonBlockingBuilder::default()
        .buffered_lines_limit(QUEUE_CAPACITY)
        .lossy(true)
        .finish(std::io::stderr());
    let diagnostics = QueueDiagnostics::new(
        file_writer.error_counter(),
        stderr_writer.error_counter(),
        directive,
    );
    tracing_subscriber::registry()
        .with(filter)
        .with(
            tracing_subscriber::fmt::layer()
                .json()
                .event_format(RunFormat::Json { id: run_id.clone() })
                .with_ansi(false)
                .with_writer(file_writer),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .event_format(RunFormat::Text { id: run_id })
                .with_ansi(std::io::stderr().is_terminal())
                .with_writer(stderr_writer),
        )
        .try_init()
        .map_err(|error| format!("无法注册日志输出：{error}"))?;
    Ok((
        Logging {
            _file_guard: file_guard,
            _stderr_guard: stderr_guard,
        },
        diagnostics,
    ))
}
