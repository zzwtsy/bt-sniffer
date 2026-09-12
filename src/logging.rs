//! 固定的 stderr 与按 UTC 自然日轮转的文本文件日志，入口为 init。
//!
//! 两个输出端各有独立的有界后台 writer，慢 I/O 不阻塞网络事件循环。
//! main 持有两个 guard，直到业务、runtime 收尾和最终错误日志完成。
use std::io::IsTerminal;
use tracing_appender::{
    non_blocking::{NonBlockingBuilder, WorkerGuard},
    rolling::{RollingFileAppender, Rotation},
};
use tracing_subscriber::{filter::Targets, prelude::*};

/// 固定 target 过滤；不读取环境变量，也不提供运行时调级。
fn filter() -> Targets {
    Targets::new()
        .with_default(tracing::Level::WARN)
        .with_target("bt_sniffer", tracing::Level::INFO)
}

/// 两个 guard 分别负责输出端的有限退出等待；返回不代表持久落盘。
pub(crate) struct Logging {
    _file_guard: WorkerGuard,
    _stderr_guard: WorkerGuard,
}

/// 在创建 runtime 前初始化；目录、文件或 subscriber 注册失败时返回带上下文的错误。
/// logs 属于当前工作目录，不随状态目录变化；不支持多进程共写。
pub(crate) fn init() -> Result<Logging, String> {
    let directory = std::env::current_dir()
        .map_err(|error| format!("无法确定日志工作目录：{error}"))?
        .join("logs");
    // appender 会先扫描旧文件再打开新文件；先建目录，避免首次启动误报扫描失败。
    std::fs::create_dir_all(&directory)
        .map_err(|error| format!("无法创建日志目录 {}：{error}", directory.display()))?;
    let file = RollingFileAppender::builder()
        .rotation(Rotation::DAILY)
        .filename_prefix("bt-sniffer")
        .filename_suffix("log")
        .max_log_files(7)
        .build(&directory)
        .map_err(|error| format!("无法创建日志目录或文件 {}：{error}", directory.display()))?;

    // lossy 模式满载丢弃投递，不能等待慢文件或慢终端；条数不是字节上限。
    let (file_writer, file_guard) = NonBlockingBuilder::default()
        .buffered_lines_limit(4096)
        .lossy(true)
        .finish(file);
    let (stderr_writer, stderr_guard) = NonBlockingBuilder::default()
        .buffered_lines_limit(4096)
        .lossy(true)
        .finish(std::io::stderr());

    // 显式设置 ANSI，覆盖依赖创建 fmt 层时内部读取的 NO_COLOR 默认值。
    tracing_subscriber::registry()
        .with(filter())
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(file_writer),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(std::io::stderr().is_terminal())
                .with_writer(stderr_writer),
        )
        .try_init()
        .map_err(|error| format!("无法注册日志输出：{error}"))?;

    Ok(Logging {
        _file_guard: file_guard,
        _stderr_guard: stderr_guard,
    })
}

#[cfg(test)]
mod output_tests {
    use std::{
        ffi::OsStr,
        path::Path,
        process::{Command, Output},
    };

    /// 全局 subscriber 和工作目录只在隔离子进程中改变，避免污染并行测试。
    #[test]
    fn probe() {
        if std::env::var_os("BT_SNIFFER_LOGGING_TEST_CHILD").is_none() {
            return;
        }
        let _logging = super::init().unwrap();
        tracing::info!(
            event = "application_info",
            schema_version = 1u64,
            count = 7u64,
            success = true
        );
        tracing::warn!(event = "application_warn", schema_version = 1u64);
        tracing::error!(event = "application_error", schema_version = 1u64);
        tracing::warn!(target:"foreign_dependency", event="dependency_warn", schema_version=1u64);
        tracing::error!(target:"foreign_dependency", event="dependency_error", schema_version=1u64);
        tracing::debug!("hidden_application_debug");
        tracing::info!(target:"foreign_dependency", "hidden_dependency_info");
    }

    fn run_probe(directory: &Path, environment: &OsStr) -> Output {
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "logging::output_tests::probe", "--nocapture"])
            .current_dir(directory)
            .env("BT_SNIFFER_LOGGING_TEST_CHILD", "1")
            .env("RUST_LOG", environment)
            .env("NO_COLOR", environment)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        output
    }

    fn log_files(directory: &Path) -> Vec<std::path::PathBuf> {
        std::fs::read_dir(directory.join("logs"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                let name = path.file_name().unwrap().to_str().unwrap();
                name.starts_with("bt-sniffer.") && name.ends_with(".log")
            })
            .collect()
    }

    #[test]
    fn actual_layers_keep_text_fields_and_ignore_environment() {
        let mut values = vec![
            std::ffi::OsString::from(""),
            "off".into(),
            "bt_sniffer=invalid".into(),
        ];
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStringExt;
            values.push(std::ffi::OsString::from_vec(vec![255]));
        }
        for value in values {
            let directory = tempfile::tempdir().unwrap();
            std::fs::write(directory.path().join(".env"), "RUST_LOG=off\nNO_COLOR=1\n").unwrap();
            let output = run_probe(directory.path(), &value);
            let files = log_files(directory.path());
            assert_eq!(files.len(), 1);
            let file = std::fs::read_to_string(&files[0]).unwrap();
            let stderr = String::from_utf8(output.stderr).unwrap();
            for text in [&file, &stderr] {
                assert!(!text.contains('\u{1b}'));
                assert!(!text.contains("hidden_"));
                assert!(!text.contains("logging_queue"));
                assert!(!text.contains("Error reading the log directory"));
                for event in [
                    "application_info",
                    "application_warn",
                    "application_error",
                    "dependency_warn",
                    "dependency_error",
                ] {
                    assert!(text.contains(event), "{text}");
                }
                assert!(text.contains("schema_version=1"));
                assert!(text.contains("count=7"));
                assert!(text.contains("success=true"));
                assert!(text.contains("bt_sniffer::logging"));
                assert!(text.contains("foreign_dependency"));
            }
            // 文本默认 UTC 时间与文件的 UTC 日期一致；不把此检查当跨午夜轮转测试。
            assert_eq!(
                files[0].file_name().unwrap().to_str().unwrap(),
                format!("bt-sniffer.{}.log", &file[..10])
            );
            assert!(
                file.lines()
                    .next()
                    .unwrap()
                    .split_whitespace()
                    .next()
                    .unwrap()
                    .ends_with('Z')
            );
            run_probe(directory.path(), &value);
            let appended = std::fs::read_to_string(&files[0]).unwrap();
            assert!(appended.starts_with(&file));
            assert_eq!(appended.matches("application_info").count(), 2);
        }
    }

    #[test]
    fn startup_limits_matching_files_and_preserves_unrelated_files() {
        let directory = tempfile::tempdir().unwrap();
        let logs = directory.path().join("logs");
        std::fs::create_dir(&logs).unwrap();
        for day in 1..=9 {
            std::fs::write(logs.join(format!("bt-sniffer.2000-01-{day:02}.log")), "old").unwrap();
        }
        for name in ["other.2000-01-01.log", "bt-sniffer.2000-01-01.txt"] {
            std::fs::write(logs.join(name), "unrelated").unwrap();
        }
        run_probe(directory.path(), OsStr::new("off"));
        assert_eq!(log_files(directory.path()).len(), 7);
        for name in ["other.2000-01-01.log", "bt-sniffer.2000-01-01.txt"] {
            assert_eq!(
                std::fs::read_to_string(logs.join(name)).unwrap(),
                "unrelated"
            );
        }
        run_probe(directory.path(), OsStr::new("off"));
        assert!(log_files(directory.path()).len() <= 7);
    }
}

#[cfg(test)]
mod writer_tests {
    use std::{
        io::{self, Write},
        sync::{Arc, Condvar, Mutex, mpsc},
        time::Duration,
    };
    struct Gate {
        released: Arc<(Mutex<bool>, Condvar)>,
        entered: mpsc::Sender<()>,
        bytes: Arc<Mutex<Vec<u8>>>,
    }
    impl Write for Gate {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let _ = self.entered.send(());
            let (lock, wake) = &*self.released;
            let _ready = wake
                .wait_while(lock.lock().unwrap(), |ready| !*ready)
                .unwrap();
            self.bytes.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    #[test]
    fn each_output_progresses_while_the_other_is_blocked() {
        for blocked_file in [true, false] {
            let mut writers = Vec::new();
            let mut guards = Vec::new();
            let mut observations = Vec::new();
            for is_file in [true, false] {
                let released = Arc::new((Mutex::new(is_file != blocked_file), Condvar::new()));
                let bytes = Arc::new(Mutex::new(Vec::new()));
                let (entered, receiver) = mpsc::channel();
                let (writer, guard) = tracing_appender::non_blocking::NonBlockingBuilder::default()
                    .buffered_lines_limit(4096)
                    .lossy(true)
                    .finish(Gate {
                        released: released.clone(),
                        entered,
                        bytes: bytes.clone(),
                    });
                writers.push(writer);
                guards.push(guard);
                observations.push((released, receiver, bytes));
            }
            let (done, completed) = mpsc::channel();
            let producer = std::thread::spawn(move || {
                for writer in &mut writers {
                    writer.write_all(b"final event\n").unwrap();
                }
                done.send(()).unwrap();
            });
            let produced = completed.recv_timeout(Duration::from_secs(2));
            let file_entered = observations[0].1.recv_timeout(Duration::from_secs(2));
            let stderr_entered = observations[1].1.recv_timeout(Duration::from_secs(2));
            let healthy_index = usize::from(blocked_file);
            // 健康端 guard 交接完成后才核对输出，不通过固定睡眠推测已写出。
            drop(guards.remove(healthy_index));
            let healthy = observations[healthy_index].2.lock().unwrap().clone();
            for (released, _, _) in &observations {
                *released.0.lock().unwrap() = true;
                released.1.notify_all();
            }
            producer.join().unwrap();
            drop(guards);
            assert!(produced.is_ok());
            assert!(file_entered.is_ok() && stderr_entered.is_ok());
            assert_eq!(healthy, b"final event\n");
            for (_, _, bytes) in observations {
                assert_eq!(*bytes.lock().unwrap(), b"final event\n");
            }
        }
    }

    #[test]
    fn slow_writer_comparison_and_guard_drain() {
        for (nonblocking, healthy) in [(false, false), (true, false), (false, true), (true, true)] {
            let gate = Arc::new((Mutex::new(healthy), Condvar::new()));
            let bytes = Arc::new(Mutex::new(Vec::new()));
            let (entered, rx) = mpsc::channel();
            let sink = Gate {
                released: gate.clone(),
                entered,
                bytes: bytes.clone(),
            };
            let (mut writer, guard, counter): (Box<dyn Write + Send>, _, _) = if nonblocking {
                let (writer, guard) = tracing_appender::non_blocking::NonBlockingBuilder::default()
                    .buffered_lines_limit(if healthy { 4096 } else { 2 })
                    .lossy(true)
                    .finish(sink);
                let counter = writer.error_counter();
                (Box::new(writer), Some(guard), Some(counter))
            } else {
                (Box::new(sink), None, None)
            };
            let (done, result) = mpsc::channel();
            let producer = std::thread::spawn(move || {
                for _ in 0..128 {
                    writer.write_all(b"fixed input\n").unwrap();
                }
                done.send(()).unwrap();
            });
            rx.recv_timeout(Duration::from_secs(2)).unwrap();
            let before_release = result.recv_timeout(Duration::from_millis(50)).is_ok();
            *gate.0.lock().unwrap() = true;
            gate.1.notify_all();
            producer.join().unwrap();
            drop(guard);
            assert_eq!(before_release, nonblocking || healthy);
            let dropped = counter.map_or(0, |c| c.dropped_lines());
            assert_eq!(bytes.lock().unwrap().len() / 12 + dropped, 128);
            println!(
                "WRITER_REPORT={}",
                serde_json::json!({"nonblocking":nonblocking,"input":128,"queue_capacity":if healthy {4096} else {2},"healthy":healthy,"producer_completed_while_sink_blocked":before_release,"dropped":dropped,"written":bytes.lock().unwrap().len()/12})
            );
        }
    }
}

#[cfg(test)]
mod event_tests {
    use super::*;
    use std::{
        io::{self, Write},
        sync::{Arc, Mutex},
        time::Duration,
    };
    #[derive(Clone)]
    struct Buffer(Arc<Mutex<Vec<u8>>>);
    impl Write for Buffer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    #[test]
    fn json_events_preserve_types_filters_overflow_and_interval_reset() {
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let sink = Buffer(bytes.clone());
        let subscriber = tracing_subscriber::registry().with(filter()).with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_ansi(false)
                .with_writer(move || sink.clone()),
        );
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(target:"foreign_dependency", "must not appear");
            tracing::debug!("must not appear");
            let metrics = Arc::new(crate::metrics::Metrics::default());
            metrics.add(crate::metrics::Counter::Connections, 1);
            metrics.observe(
                crate::metrics::Timing::ClaimWait,
                Duration::from_secs(90000),
            );
            metrics.log();
            metrics.log();
            crate::dht::traffic::Budget::default().log();
            crate::storage::jobs::Stats::default().log(true);
        });
        let text = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
        assert!(!text.contains('\u{1b}'));
        assert!(!text.contains("must not appear"));
        let events: Vec<serde_json::Value> = text
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert!(events.iter().all(|e| e["fields"]["schema_version"] == 1));
        let fields: Vec<_> = events.iter().map(|e| &e["fields"]).collect();
        let counts: Vec<_> = fields
            .iter()
            .filter(|f| {
                f["event"] == "collector_counter"
                    && f["counter"] == "Connections"
                    && f["scope"] == "interval"
            })
            .map(|f| f["value"].as_u64().unwrap())
            .collect();
        assert_eq!(counts, vec![1, 0]);
        let overflow = fields
            .iter()
            .find(|f| f["timing"] == "ClaimWait" && f["count"] == 1)
            .unwrap();
        assert_eq!(overflow["p95_exceeds_ms"], 86400000u64);
        assert!(overflow["p95_upper_bound_ms"].is_null());
        let empty = fields
            .iter()
            .find(|f| f["event"] == "duration_histogram" && f["count"] == 0)
            .unwrap();
        assert!(empty["p95_upper_bound_ms"].is_null());
        assert!(empty["p95_exceeds_ms"].is_null());
        assert!(fields.iter().any(|f| f["class"] == "verification"
            && f["event"] == "dht_class"
            && f["packets"].is_u64()));
        assert!(
            fields
                .iter()
                .any(|f| f["final_snapshot"] == true && f["metadata_bytes"].is_i64())
        );
    }
}
