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

/// 远端错误通过实际双端 formatter 后仍为单行，保留中文和稳定错误类别。
#[test]
fn remote_query_errors_are_bounded_and_safe_on_both_outputs() {
    use crate::dht::dispatcher::QueryError;

    let text_bytes = Arc::new(Mutex::new(Vec::new()));
    let json_bytes = Arc::new(Mutex::new(Vec::new()));
    let text_sink = Buffer(text_bytes.clone());
    let json_sink = Buffer(json_bytes.clone());
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new("debug"))
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .event_format(RunFormat::Text {
                    id: "remote-test".into(),
                })
                .with_writer(move || text_sink.clone()),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_ansi(false)
                .event_format(RunFormat::Json {
                    id: "remote-test".into(),
                })
                .with_writer(move || json_sink.clone()),
        );
    let cases = [
        (
            "中文\n\r\t\0\u{1b}[2J\u{85}".as_bytes().to_vec(),
            r"中文\n\r\t\u{0}\u{1b}[2J\u{85}".to_string(),
        ),
        (vec![b'a', 255, b'b'], "a�b".to_string()),
        ("中".repeat(256).into_bytes(), "中".repeat(256)),
        (
            "中".repeat(257).into_bytes(),
            format!("{}…", "中".repeat(256)),
        ),
        (
            format!("{}\n", "a".repeat(255)).into_bytes(),
            format!("{}…", "a".repeat(255)),
        ),
    ];
    tracing::subscriber::with_default(subscriber, || {
        for (message, expected) in &cases {
            let error = QueryError::Remote {
                code: 201,
                message: message.clone().into(),
            };
            assert_eq!(
                error.to_string(),
                format!("远端返回 KRPC 错误 201：{expected}")
            );
            tracing::debug!(
                event = "bootstrap_query_failed",
                schema_version = 1u64,
                phase = "query",
                %error
            );
            // 只改变说明的显示方式，协议数据仍可供调用者检查。
            assert!(
                matches!(error, QueryError::Remote { code: 201, message: original } if original.as_ref() == message)
            );
        }
    });
    let text = String::from_utf8(text_bytes.lock().unwrap().clone()).unwrap();
    assert_eq!(text.lines().count(), cases.len());
    assert!(!text.chars().any(|ch| ch.is_control() && ch != '\n'));
    let json = String::from_utf8(json_bytes.lock().unwrap().clone()).unwrap();
    let events: Vec<serde_json::Value> = json
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(events.len(), cases.len());
    for ((event, line), (_, expected)) in events.iter().zip(text.lines()).zip(&cases) {
        assert_eq!(event["run_id"], "remote-test");
        assert_eq!(
            event["fields"]["error"],
            format!("远端返回 KRPC 错误 201：{expected}")
        );
        assert!(line.contains(expected));
        assert!(line.ends_with("run_id=remote-test"));
    }
}

#[test]
fn json_events_preserve_types_filters_overflow_and_interval_reset() {
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let sink = Buffer(bytes.clone());
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(DEFAULT_FILTER))
        .with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_ansi(false)
                .with_writer(move || sink.clone()),
        );
    tracing::subscriber::with_default(subscriber, || {
        tracing::info!(target:"foreign_dependency", "must not appear");
        tracing::debug!("must not appear");
        let metrics = Arc::new(crate::collection::diagnostics::metrics::Metrics::default());
        metrics.add(
            crate::collection::diagnostics::metrics::Counter::PeerAttempts,
            1,
        );
        metrics.observe(
            crate::collection::diagnostics::metrics::Timing::TcpWait,
            Duration::from_secs(90000),
        );
        metrics.log();
        metrics.log();
        crate::dht::traffic::Budget::default().log();
        crate::collection::jobs::Stats::default().log(true);
    });
    let text = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
    assert!(!text.contains('\u{1b}'));
    assert!(!text.contains("must not appear"));
    let events: Vec<serde_json::Value> = text
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert!(events.iter().all(|e| e["fields"]["schema_version"]
        == if e["fields"]["event"] == "collector_counter" {
            2
        } else {
            1
        }));
    let fields: Vec<_> = events.iter().map(|e| &e["fields"]).collect();
    let counts: Vec<_> = fields
        .iter()
        .filter(|f| {
            f["event"] == "collector_counter"
                && f["counter"] == "PeerAttempts"
                && f["scope"] == "interval"
        })
        .map(|f| f["value"].as_u64().unwrap())
        .collect();
    assert_eq!(counts, vec![1, 0]);
    let overflow = fields
        .iter()
        .find(|f| f["timing"] == "TcpWait" && f["count"] == 1)
        .unwrap();
    assert_eq!(overflow["p95_exceeds_ms"], 180000u64);
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
/// DHT 与采集各自输出日志，但相同桶样本必须保持字段、缺失值和区间语义一致。
#[tokio::test(start_paused = true)]
async fn dht_and_collection_histogram_contracts_match() {
    use tracing::instrument::WithSubscriber;
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let sink = Buffer(bytes.clone());
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_ansi(false)
        .with_writer(move || sink.clone())
        .finish();
    async {
        let metrics = Arc::new(crate::collection::diagnostics::metrics::Metrics::default());
        let budget = Arc::new(crate::dht::traffic::Budget::default());
        for millis in [1, 10_000, 181_000] {
            let queued = budget.queue_record(crate::dht::traffic::Class::Collector);
            tokio::time::advance(Duration::from_millis(millis)).await;
            drop(queued);
            metrics.observe(
                crate::collection::diagnostics::metrics::Timing::TcpWait,
                Duration::from_millis(millis),
            );
        }
        for _ in 0..2 {
            metrics.log();
            budget.log();
        }
    }
    .with_subscriber(subscriber)
    .await;
    let text = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
    let events: Vec<serde_json::Value> = text
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let select = |timing: &str| -> Vec<serde_json::Value> {
        events
            .iter()
            .filter(|event| {
                event["fields"]["event"] == "duration_histogram"
                    && event["fields"]["timing"] == timing
                    && event["fields"]["class"] == "collector"
            })
            .map(|event| {
                assert_eq!(
                    event["target"],
                    if timing == "dht_queue" {
                        "bt_sniffer::dht::traffic"
                    } else {
                        "bt_sniffer::collection::diagnostics::metrics"
                    }
                );
                let mut fields = event["fields"].clone();
                fields.as_object_mut().unwrap().remove("timing");
                fields
            })
            .collect()
    };
    let collector = select("TcpWait");
    assert_eq!(collector, select("dht_queue"));
    assert_eq!(collector.len(), 4);
    assert_eq!(collector[0]["count"], 3);
    assert_eq!(collector[0]["overflow"], 1);
    assert_eq!(collector[3]["count"], 0);
}

/// JSON 包装不丢失标准 span 字段；两个交错执行的 future 不共享业务上下文。
#[tokio::test]
async fn json_run_format_preserves_async_span_context() {
    use tracing::{Instrument, instrument::WithSubscriber};
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let sink = Buffer(bytes.clone());
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new("info"))
        .with(
            tracing_subscriber::fmt::layer()
                .json()
                .event_format(super::RunFormat::Json {
                    id: "test-run".into(),
                })
                .with_writer(move || sink.clone()),
        );
    async {
        let mut tasks = tokio::task::JoinSet::new();
        for attempt_id in [1u64, 2] {
            let span = tracing::info_span!("attempt", attempt_id, result = tracing::field::Empty);
            tasks.spawn(
                async move {
                    tokio::task::yield_now().await;
                    tracing::Span::current().record("result", "observed");
                    let child = tracing::debug_span!("disabled_child");
                    async move {
                        tracing::info!(event = "attempt_observed", attempt_id);
                    }
                    .instrument(child.or_current())
                    .await;
                }
                .instrument(span)
                .with_current_subscriber(),
            );
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    }
    .with_subscriber(subscriber)
    .await;
    let text = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
    let events: Vec<serde_json::Value> = text
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(events.len(), 2);
    for event in events {
        assert_eq!(event["run_id"], "test-run");
        assert_eq!(event["span"]["name"], "attempt");
        assert_eq!(event["span"]["attempt_id"], event["fields"]["attempt_id"]);
        assert_eq!(event["span"]["result"], "observed");
        assert_eq!(event["spans"].as_array().unwrap().len(), 1);
    }
}
