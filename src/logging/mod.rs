//! 日志配置先于运行资源校验；writer 生命周期由 main 持有。
use std::{ffi::OsString, io::IsTerminal};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub(crate) enum Format {
    Text,
    Json,
}
fn filter(value: Option<OsString>) -> Result<EnvFilter, String> {
    let value = match value {
        None => "warn,bt_sniffer=info".to_owned(),
        Some(value) => value
            .into_string()
            .map_err(|_| "RUST_LOG 不是 Unicode".to_owned())?,
    };
    EnvFilter::try_new(if value.is_empty() { "off" } else { &value }).map_err(|e| e.to_string())
}
fn color(terminal: bool, no_color: Option<OsString>) -> bool {
    terminal && no_color.is_none_or(|v| v.is_empty())
}
static DROPS: std::sync::OnceLock<tracing_appender::non_blocking::ErrorCounter> =
    std::sync::OnceLock::new();
static LAST: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
pub(crate) fn report() {
    if let Some(counter) = DROPS.get() {
        let total = counter.dropped_lines();
        let previous = LAST.swap(total, std::sync::atomic::Ordering::Relaxed);
        tracing::info!(
            event = "logging_queue",
            schema_version = 1u64,
            dropped_total = total as u64,
            dropped_interval = total.saturating_sub(previous) as u64,
            "日志队列投递失败统计"
        );
    }
}
pub(crate) struct Logging {
    _guard: tracing_appender::non_blocking::WorkerGuard,
}
impl Drop for Logging {
    fn drop(&mut self) {
        report();
    }
}
pub(crate) fn init(format: Format) -> Result<Logging, String> {
    let filter = filter(std::env::var_os("RUST_LOG"))?;
    let (writer, guard) = tracing_appender::non_blocking::NonBlockingBuilder::default()
        .buffered_lines_limit(4096)
        .lossy(true)
        .finish(std::io::stderr());
    let counter = writer.error_counter();
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(writer);
    match format {
        Format::Text => builder
            .with_ansi(color(
                std::io::stderr().is_terminal(),
                std::env::var_os("NO_COLOR"),
            ))
            .try_init(),
        Format::Json => builder.json().with_ansi(false).try_init(),
    }
    .map_err(|e| e.to_string())?;
    let _ = DROPS.set(counter);
    Ok(Logging { _guard: guard })
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn configuration_is_strict_and_color_requires_terminal() {
        assert_eq!(filter(None).unwrap().to_string(), "bt_sniffer=info,warn");
        assert_eq!(filter(Some("".into())).unwrap().to_string(), "off");
        assert!(filter(Some("bt_sniffer=invalid".into())).is_err());
        assert!(!color(false, None));
        assert!(color(true, None));
        assert!(!color(true, Some("1".into())));
        assert!(color(true, Some("".into())));
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStringExt;
            assert!(filter(Some(OsString::from_vec(vec![255]))).is_err());
        }
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
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_ansi(false)
            .with_env_filter(filter(None).unwrap())
            .with_writer(move || sink.clone())
            .finish();
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
