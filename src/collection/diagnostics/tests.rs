//! 固定诊断聚合与真实 peer 生命周期回归。

use super::*;
use crate::collection::jobs::ClaimClass;
#[test]
fn failure_details_are_typed_once_and_intervals_reset() {
    use crate::collection::peer::PeerError;
    for (kind, reason) in [
        (
            std::io::ErrorKind::ConnectionRefused,
            FailureReason::ConnectionRefused,
        ),
        (
            std::io::ErrorKind::NetworkUnreachable,
            FailureReason::NetworkUnreachable,
        ),
        (
            std::io::ErrorKind::HostUnreachable,
            FailureReason::HostUnreachable,
        ),
        (
            std::io::ErrorKind::PermissionDenied,
            FailureReason::PermissionDenied,
        ),
        (std::io::ErrorKind::Other, FailureReason::OtherIo),
    ] {
        assert_eq!(failure_reason(&PeerError::Io(kind.into())), reason);
    }
    let metrics = Arc::new(crate::collection::diagnostics::metrics::Metrics::default());
    let mut report = PeerObservation::new(
        metrics.clone(),
        Source::Announce,
        false,
        Arc::default(),
        Arc::default(),
    );
    report.advance(Stage::ExtensionHandshake);
    let error =
        PeerError::Protocol(crate::collection::peer::wire::WireErrorKind::ExtensionIdRange.into());
    report.failure(&error);
    report.failure(&error);
    report.finish(error_kind(&error), Deadline::None);
    drop(report);
    let pair = metrics.diagnostics.snapshots.lock().unwrap();
    assert_eq!(pair.0.failures.values().sum::<u64>(), 1);
    assert_eq!((pair.0.samples.emitted, pair.0.samples.suppressed), (0, 0));
    assert_eq!(pair.1.failures.values().sum::<u64>(), 1);
    drop(pair);
    metrics.diagnostics.log();
    let pair = metrics.diagnostics.snapshots.lock().unwrap();
    assert!(pair.1.failures.is_empty());
    assert_eq!(pair.0.failures.values().sum::<u64>(), 1);
}
/// 完整 TCP 路径把具体失败透传到扩展阶段，成功的标准握手不会重复计失败。
#[tokio::test]
async fn extension_failure_detail_survives_fetch() {
    use crate::collection::peer::MetadataConfig;
    use crate::collection::peer::PeerClient;
    use crate::collection::peer::wire as peer_wire;
    use crate::info_hash::SwarmKey;
    use futures_util::{SinkExt, StreamExt};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    use tracing::instrument::WithSubscriber;
    for payload in [
        &b"d1:mi1ee"[..],
        b"degarbage",
        b"d1:bi1e1:ai2e",
        b"d1:ai1e1:ai2ee",
        b"d1:md11:ut_metadatai256eee",
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut hello = [0; 68];
            socket.read_exact(&mut hello).await.unwrap();
            socket
                .write_all(&peer_wire::handshake(
                    SwarmKey([1; 20]),
                    peer_wire::PeerId([7; 20]),
                ))
                .await
                .unwrap();
            let mut frames = tokio_util::codec::LengthDelimitedCodec::builder().new_framed(socket);
            frames.next().await.unwrap().unwrap();
            frames.send(peer_wire::extended(0, payload)).await.unwrap();
        });
        let captured = tempfile::NamedTempFile::new().unwrap();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_ansi(false)
            .with_writer(captured.reopen().unwrap())
            .finish();
        let metrics = Arc::new(crate::collection::diagnostics::metrics::Metrics::default());
        let fetcher = PeerClient::new(MetadataConfig {
            address_policy: crate::address::AddressPolicy::LocalUnicast,
            ..Default::default()
        })
        .unwrap()
        .with_metrics(metrics.clone());
        assert!(
            fetcher
                .fetch_one(
                    SwarmKey([1; 20]),
                    address,
                    &tokio_util::sync::CancellationToken::new(),
                    crate::collection::peer::PeerContext::default()
                )
                .with_subscriber(subscriber)
                .await
                .is_err()
        );
        server.await.unwrap();
        let expected = peer_wire::parse_extension(payload, 4096, 64).unwrap_err();
        let logs = std::fs::read_to_string(captured.path()).unwrap();
        let samples: Vec<serde_json::Value> = logs
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .filter(|event| event["fields"]["event"] == "bencode_error_sample")
            .collect();
        assert_eq!(samples.len(), usize::from(expected.detail.is_some()));
        if let Some(detail) = &expected.detail {
            let fields = &samples[0]["fields"];
            assert_eq!(fields["stage"], "ExtensionHandshake");
            assert_eq!(fields["source"], "Dht");
            assert_eq!(fields["family"], "ipv4");
            assert_eq!(fields["reason"], format!("{:?}", expected.kind));
            assert_eq!(fields["detail"], detail.text);
            assert_eq!(fields["truncated"], detail.truncated);
            if let Some(check) = expected.inspection {
                assert_eq!(fields["unsorted_keys"], check.unsorted_keys);
                assert_eq!(fields["duplicate_keys"], check.duplicate_keys);
                assert_eq!(
                    fields["inspection_status"],
                    format!("{:?}", check.inspection_status)
                );
            } else {
                assert!(fields["inspection_status"].is_null());
            }
            assert_eq!(fields["schema_version"], 2);
        }
        let pair = metrics.diagnostics.snapshots.lock().unwrap();
        assert_eq!(pair.0.failures.len(), 1);
        assert_eq!(pair.0.samples.emitted, u64::from(expected.detail.is_some()));
        assert_eq!(pair.0.samples.suppressed, 0);
        assert_eq!(
            pair.0.failures.get(&(
                Stage::ExtensionHandshake,
                Source::Dht,
                false,
                FailureReason::Protocol(expected.kind)
            )),
            Some(&1)
        );
    }
}
/// 标准握手消耗 3 秒后，扩展协商只能使用原 5 秒期限的剩余时间。
#[tokio::test]
async fn extension_negotiation_does_not_restart_handshake_deadline() {
    use crate::collection::peer::PeerClient;
    use crate::collection::peer::wire as peer_wire;
    use crate::collection::peer::wire::PeerId;
    use crate::info_hash::SwarmKey;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let stop = tokio_util::sync::CancellationToken::new();
    let server_stop = stop.clone();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut hello = [0; 68];
        socket.read_exact(&mut hello).await.unwrap();
        tokio::time::sleep(Duration::from_secs(3)).await;
        socket
            .write_all(&peer_wire::handshake(SwarmKey([1; 20]), PeerId([2; 20])))
            .await
            .unwrap();
        // 3 秒和 4 秒各发一次兼容增量更新，仍必须在起始 5 秒处超时。
        let frame = peer_wire::extended(0, b"d1:zi1e1:md11:ut_metadatai7eee");
        for update in 0..2 {
            if update > 0 {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            socket
                .write_all(&(frame.len() as u32).to_be_bytes())
                .await
                .unwrap();
            socket.write_all(&frame).await.unwrap();
        }
        server_stop.cancelled().await;
    });
    let metrics = Arc::new(crate::collection::diagnostics::metrics::Metrics::default());
    let fetcher = PeerClient::new(crate::collection::peer::MetadataConfig {
        address_policy: crate::address::AddressPolicy::LocalUnicast,
        ..Default::default()
    })
    .unwrap()
    .with_metrics(metrics.clone());
    let started = tokio::time::Instant::now();
    assert!(
        fetcher
            .fetch_one(
                SwarmKey([1; 20]),
                address,
                &stop,
                crate::collection::peer::PeerContext::default()
            )
            .await
            .is_err()
    );
    let elapsed = started.elapsed();
    stop.cancel();
    server.await.unwrap();
    assert!(
        elapsed >= Duration::from_secs(5) && elapsed < Duration::from_secs(6),
        "{elapsed:?}"
    );
    let report = metrics.diagnostics.report();
    assert_eq!(report["extension_compatibility"]["accepted_frames"], 2);
    assert_eq!(report["extension_compatibility"]["sessions"], 1);
    assert_eq!(report["extension_compatibility"]["downloaded"], 0);
    let failures: Vec<_> = report["peers"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|p| p["result"] != "Success")
        .collect();
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0]["stage"], "ExtensionHandshake");
    assert_eq!(failures[0]["result"], "Timeout");
    assert_eq!(failures[0]["deadline"], "Stage");
}
#[tokio::test(start_paused = true)]
async fn phases_finish_once_and_outer_deadline_is_distinct_from_cancellation() {
    let metrics = Arc::new(crate::collection::diagnostics::metrics::Metrics::default());
    let timeout = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut report = PeerObservation::new(
        metrics.clone(),
        Source::Announce,
        false,
        timeout.clone(),
        Arc::default(),
    );
    report.advance(Stage::StandardHandshake);
    report.advance(Stage::StandardHandshake);
    tokio::time::advance(Duration::from_secs(5)).await;
    timeout.store(true, std::sync::atomic::Ordering::Relaxed);
    drop(report);
    drop(PeerObservation::new(
        metrics.clone(),
        Source::Dht,
        true,
        Arc::default(),
        Arc::default(),
    ));
    let pair = metrics.diagnostics.snapshots.lock().unwrap();
    assert_eq!(pair.0.peers.values().map(|d| d.count).sum::<u64>(), 3);
    assert!(
        pair.0
            .peers
            .keys()
            .any(|k| k.stage == Stage::StandardHandshake
                && k.result == ResultKind::Timeout
                && k.deadline == Deadline::Task)
    );
    assert!(
        pair.0
            .peers
            .keys()
            .any(|k| k.ipv6 && k.result == ResultKind::Cancelled)
    );
    drop(pair);
    metrics.diagnostics.log();
    let pair = metrics.diagnostics.snapshots.lock().unwrap();
    assert!(pair.1.peers.is_empty());
    assert_eq!(pair.0.peers.len(), 3);
}
#[test]
fn error_categories_and_long_wait_buckets_do_not_parse_messages() {
    use crate::collection::peer::PeerError;
    assert_eq!(
        error_kind(&PeerError::Io(std::io::ErrorKind::UnexpectedEof.into())),
        ResultKind::Eof
    );
    assert_eq!(
        error_kind(&PeerError::Io(std::io::ErrorKind::ConnectionReset.into())),
        ResultKind::Reset
    );
    assert_eq!(
        error_kind(&PeerError::HashMismatch),
        ResultKind::HashMismatch
    );
    let mut d = Distribution::default();
    d.record(Duration::from_secs(10 * 3600));
    assert_eq!(d.quantile(95), Some(12 * 3600 * 1000));
    d.record(Duration::from_secs(16 * 3600));
    assert_eq!(d.quantile(95), Some(18 * 3600 * 1000));
}

#[tokio::test]
async fn real_handshakes_report_standard_extension_eof_and_hash_mismatch() {
    use crate::collection::peer::MetadataConfig;
    use crate::collection::peer::PeerClient;
    use crate::collection::peer::wire as peer_wire;
    use crate::collection::peer::wire::PeerId;
    use crate::info_hash::SwarmKey;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    for case in 0..10 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let stop = tokio_util::sync::CancellationToken::new();
        let server_stop = stop.clone();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut hello = [0; 68];
            socket.read_exact(&mut hello).await.unwrap();
            if case == 0 {
                return;
            }
            if case == 9 {
                socket2::SockRef::from(&socket)
                    .set_linger(Some(Duration::ZERO))
                    .unwrap();
                return;
            }
            if case != 1 {
                let mut response = peer_wire::handshake(
                    SwarmKey(if case == 4 { [9; 20] } else { [1; 20] }),
                    PeerId([2; 20]),
                );
                if case == 8 {
                    response[0] = 18;
                }
                if case == 3 {
                    response[25] = 0;
                }
                // 碎片到达仍属同一次标准握手，不增加阶段完成次数。
                socket.write_all(&response[..34]).await.unwrap();
                tokio::task::yield_now().await;
                socket.write_all(&response[34..]).await.unwrap();
            }
            server_stop.cancelled().await;
        });
        let metrics = Arc::new(crate::collection::diagnostics::metrics::Metrics::default());
        let fetcher = PeerClient::new(MetadataConfig {
            handshake_timeout: Duration::from_millis(100),
            task_timeout: if case == 5 {
                Duration::from_millis(30)
            } else {
                Duration::from_secs(1)
            },
            peer_timeout: if case == 6 {
                Duration::from_millis(30)
            } else {
                Duration::from_secs(1)
            },
            address_policy: crate::address::AddressPolicy::LocalUnicast,
            ..Default::default()
        })
        .unwrap()
        .with_metrics(metrics.clone());
        let cancellation = tokio_util::sync::CancellationToken::new();
        let child = cancellation.clone();
        let cancel_task = tokio::spawn(async move {
            if case == 7 {
                tokio::time::sleep(Duration::from_millis(30)).await;
                child.cancel();
            }
        });
        assert!(
            fetcher
                .fetch_one(
                    SwarmKey([1; 20]),
                    address,
                    &cancellation,
                    crate::collection::peer::PeerContext {
                        observer: Default::default(),
                        source: Source::Announce,
                        ..Default::default()
                    }
                )
                .await
                .is_err()
        );
        cancel_task.await.unwrap();
        stop.cancel();
        server.await.unwrap();
        let pair = metrics.diagnostics.snapshots.lock().unwrap();
        let failures: Vec<_> = pair
            .0
            .peers
            .iter()
            .filter(|(k, _)| k.result != ResultKind::Success)
            .collect();
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].1.count, 1);
        let key = failures[0].0;
        let (stage, result) = match case {
            0 => (Stage::StandardHandshake, ResultKind::Eof),
            1 => (Stage::StandardHandshake, ResultKind::Timeout),
            2 => (Stage::ExtensionHandshake, ResultKind::Timeout),
            3 => (Stage::ExtensionHandshake, ResultKind::Unsupported),
            4 => (Stage::StandardHandshake, ResultKind::HashMismatch),
            5 | 6 => (Stage::ExtensionHandshake, ResultKind::Timeout),
            7 => (Stage::ExtensionHandshake, ResultKind::Cancelled),
            8 => (Stage::StandardHandshake, ResultKind::Protocol),
            _ => (Stage::StandardHandshake, ResultKind::Reset),
        };
        assert_eq!((key.stage, key.result), (stage, result));
        let handshake = pair
            .0
            .handshakes
            .get(&(key.source, key.ipv6, key.result, key.deadline))
            .unwrap();
        assert_eq!(handshake.count, 1);
        assert!(handshake.sum_ms >= failures[0].1.sum_ms);
        assert_eq!(key.source, Source::Announce);
        if case == 5 {
            assert_eq!(key.deadline, Deadline::Task);
        }
        if case == 6 {
            assert_eq!(key.deadline, Deadline::Peer);
        }
        if case == 7 {
            assert_eq!(key.deadline, Deadline::None);
        }
    }
}

/// 每个 Diagnostics 独立限额；不同阶段／来源／地址族共享窗口，输出区间不重置预算。
#[tokio::test(start_paused = true)]
async fn bencode_budget_is_shared_bounded_and_resets_after_sixty_seconds() {
    let metrics = Arc::new(crate::collection::diagnostics::metrics::Metrics::default());
    let error = crate::collection::peer::PeerError::Protocol(
        crate::collection::peer::wire::parse_extension(b"d1:bi1e1:ai2e", 4096, 64).unwrap_err(),
    );
    let emit = |n| {
        let mut report = PeerObservation::new(
            metrics.clone(),
            if n % 2 == 0 {
                Source::Dht
            } else {
                Source::Announce
            },
            n % 2 == 0,
            Arc::default(),
            Arc::default(),
        );
        report.advance(if n % 2 == 0 {
            Stage::ExtensionHandshake
        } else {
            Stage::Transfer
        });
        report.failure(&error);
        report.failure(&error);
    };
    for n in 0..9 {
        emit(n);
    }
    {
        let pair = metrics.diagnostics.snapshots.lock().unwrap();
        assert_eq!((pair.0.samples.emitted, pair.0.samples.suppressed), (8, 1));
        assert_eq!(pair.0.failures.values().sum::<u64>(), 9);
    }
    metrics.diagnostics.log();
    tokio::time::advance(Duration::from_secs(59)).await;
    emit(9);
    {
        let pair = metrics.diagnostics.snapshots.lock().unwrap();
        assert_eq!((pair.1.samples.emitted, pair.1.samples.suppressed), (0, 1));
    }
    tokio::time::advance(Duration::from_secs(1)).await;
    emit(10);
    {
        let pair = metrics.diagnostics.snapshots.lock().unwrap();
        assert_eq!((pair.0.samples.emitted, pair.0.samples.suppressed), (9, 2));
        assert_eq!((pair.1.samples.emitted, pair.1.samples.suppressed), (1, 1));
    }
    metrics.diagnostics.log_snapshot(true);
    let pair = metrics.diagnostics.snapshots.lock().unwrap();
    assert_eq!((pair.1.samples.emitted, pair.1.samples.suppressed), (0, 0));
    assert_eq!(pair.0.failures.values().sum::<u64>(), 11);
}

/// guard 仅在析构时计时，完成和取消都恰好一次；耗时与事务提交彼此独立。
#[tokio::test(start_paused = true)]
async fn attempt_costs_end_once_and_are_separate_from_commits() {
    use crate::collection::jobs::AttemptKind;
    let metrics = Arc::new(crate::collection::diagnostics::metrics::Metrics::default());
    for kind in [AttemptKind::First, AttemptKind::Repeat] {
        let context = AttemptContext {
            class: crate::collection::jobs::ClaimClass::Hint,
            had_valid_hint: false,
            kind,
            failed_attempts_before: 0,
        };
        for result in [
            AttemptResult::Downloaded,
            AttemptResult::LocalDeferral,
            AttemptResult::RemoteFailure(AttemptFailure::Protocol),
            AttemptResult::ControlFailure,
            AttemptResult::Cancelled,
        ] {
            metrics.diagnostics.attempt(
                context,
                TaskTiming::DueWait,
                AttemptResult::Observed,
                Duration::ZERO,
            );
            let mut report = TaskReport::new(metrics.clone(), context);
            tokio::time::advance(Duration::from_secs(2)).await;
            if result != AttemptResult::Cancelled {
                report.finish(result);
            }
            drop(report);
        }
        let context_for_abort = context;
        let report = TaskReport::new(metrics.clone(), context_for_abort);
        let task = tokio::spawn(async move {
            let _report = report;
            std::future::pending::<()>().await;
        });
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let pair = metrics.diagnostics.snapshots.lock().unwrap();
        assert!(pair.0.attempts.project().committed.is_empty());
        let timings = &pair.0.attempts.project().timings;
        assert_eq!(
            timings[&(
                context.history(),
                TaskTiming::Execution,
                AttemptResult::Cancelled
            )]
                .count,
            2
        );
        assert_eq!(
            timings[&(
                context.history(),
                TaskTiming::Execution,
                AttemptResult::Downloaded
            )]
                .sum_ms,
            2000
        );
    }
    let context = AttemptContext {
        class: crate::collection::jobs::ClaimClass::Hint,
        had_valid_hint: false,
        kind: AttemptKind::First,
        failed_attempts_before: 0,
    };
    metrics.diagnostics.committed(context);
    metrics.diagnostics.log_snapshot(true);
    let pair = metrics.diagnostics.snapshots.lock().unwrap();
    assert_eq!(pair.0.attempts.project().committed[&context.history()], 1);
    assert!(pair.1.attempts.project().timings.is_empty());
    assert!(pair.1.attempts.project().committed.is_empty());
    for label in [
        "no_peers",
        "peer_io",
        "peer_timeout",
        "protocol",
        "hash_mismatch",
        "unsupported",
        "receive_limit",
        "rejected",
        "metadata_unavailable",
        "task_timeout",
    ] {
        assert_ne!(
            AttemptFailure::from_label(label),
            AttemptFailure::Other,
            "{label}"
        );
    }
    assert_eq!(
        AttemptFailure::from_label("an unknown message"),
        AttemptFailure::Other
    );
}

/// 日志契约以实际 subscriber 序列化验证：零提交成本缺失，关闭输出累计及尾段。
#[test]
fn diagnostic_events_keep_schema_and_missing_cost_contract() {
    use crate::collection::jobs::AttemptKind;
    let captured = tempfile::NamedTempFile::new().unwrap();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_ansi(false)
        .with_writer(captured.reopen().unwrap())
        .finish();
    let diagnostics = Diagnostics::default();
    tracing::subscriber::with_default(subscriber, || {
        let context = AttemptContext {
            class: crate::collection::jobs::ClaimClass::Hint,
            had_valid_hint: false,
            kind: AttemptKind::First,
            failed_attempts_before: 0,
        };
        diagnostics.attempt(
            context,
            TaskTiming::DueWait,
            AttemptResult::Observed,
            Duration::from_secs(3),
        );
        diagnostics.attempt(
            context,
            TaskTiming::Execution,
            AttemptResult::Downloaded,
            Duration::from_secs(6),
        );
        diagnostics.committed(context);
        diagnostics.log_snapshot(true);
    });
    let logs = std::fs::read_to_string(captured.path()).unwrap();
    let events: Vec<serde_json::Value> = logs
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap()["fields"].clone())
        .collect();
    assert!(
        events
            .iter()
            .all(|event| event["schema_version"] == 1 && event["final_snapshot"] == true)
    );
    for scope in ["total", "interval"] {
        let first = events
            .iter()
            .find(|event| {
                event["event"] == "attempt_summary"
                    && event["scope"] == scope
                    && event["attempt_kind"] == "First"
            })
            .unwrap();
        assert_eq!(first["claims"], 1);
        assert_eq!(first["downloaded"], 1);
        assert_eq!(first["committed"], 1);
        assert_eq!(first["execution_sum_ms"], 6000);
        assert_eq!(first["execution_ms_per_commit"], 6000);
        for event_name in ["attempt_class_diagnostic", "attempt_class_commits"] {
            let joint = events
                .iter()
                .find(|event| event["event"] == event_name && event["scope"] == scope)
                .unwrap();
            assert_eq!(joint["attempt_kind"], "First");
            assert_eq!(joint["claim_class"], "Hint");
        }
        let history = events
            .iter()
            .find(|event| event["event"] == "connect_history_summary" && event["scope"] == scope)
            .unwrap();
        assert_eq!(history["entries"], 0);
        assert_eq!(history["expired"], 0);
        assert_eq!(history["capacity_dropped"], 0);
        let repeat = events
            .iter()
            .find(|event| {
                event["event"] == "attempt_summary"
                    && event["scope"] == scope
                    && event["attempt_kind"] == "Repeat"
            })
            .unwrap();
        assert_eq!(repeat["committed"], 0);
        assert!(repeat["execution_ms_per_commit"].is_null());
        assert!(
            events
                .iter()
                .any(|event| event["event"] == "bencode_sample_summary"
                    && event["scope"] == scope
                    && event["emitted"] == 0
                    && event["suppressed"] == 0)
        );
    }
}

/// 联合维度相加必须回到领取历史汇总，不能把原事件同一组合拆成重复行。
#[tokio::test(start_paused = true)]
async fn class_breakdown_rolls_up_and_preserves_frozen_class() {
    use crate::collection::jobs::AttemptKind;
    let metrics = Arc::new(crate::collection::diagnostics::metrics::Metrics::default());
    for kind in [AttemptKind::First, AttemptKind::Repeat] {
        let context = AttemptContext {
            class: crate::collection::jobs::ClaimClass::Hint,
            had_valid_hint: false,
            kind,
            failed_attempts_before: 1,
        };
        for class in [
            ClaimClass::Hint,
            ClaimClass::Recent,
            ClaimClass::Retry,
            ClaimClass::History,
        ] {
            let context = AttemptContext { class, ..context };
            metrics.diagnostics.attempt(
                context,
                TaskTiming::DueWait,
                AttemptResult::Observed,
                Duration::from_secs(3),
            );
            let mut report = TaskReport::new(metrics.clone(), context);
            tokio::time::advance(Duration::from_secs(1)).await;
            report.finish(AttemptResult::Downloaded);
            drop(report);
            metrics.diagnostics.committed(context);
        }
    }
    {
        let pair = metrics.diagnostics.snapshots.lock().unwrap();
        for snapshot in [&pair.0, &pair.1] {
            for ((context, timing, result), total) in &snapshot.attempts.project().timings {
                let mut count = 0;
                let mut sum_ms = 0;
                for ((found, _, found_timing, found_result), distribution) in
                    &snapshot.attempts.project().class_timings
                {
                    if (found, found_timing, found_result) == (context, timing, result) {
                        count += distribution.count;
                        sum_ms += distribution.sum_ms;
                    }
                }
                assert_eq!((count, sum_ms), (total.count, total.sum_ms));
            }
            for (context, total) in &snapshot.attempts.project().committed {
                assert_eq!(
                    snapshot
                        .attempts
                        .project()
                        .class_committed
                        .iter()
                        .filter(|((found, _), _)| found == context)
                        .map(|(_, count)| count)
                        .sum::<u64>(),
                    *total
                );
            }
        }
    }
    metrics.diagnostics.log_snapshot(true);
    let pair = metrics.diagnostics.snapshots.lock().unwrap();
    assert!(pair.1.attempts.project().class_timings.is_empty());
    assert!(pair.1.attempts.project().class_committed.is_empty());
    assert_eq!(pair.0.attempts.project().class_committed.len(), 8);
}

/// 提示维度求和回到领取历史聚合；领取历史事件不能因新上下文出现两个相同日志键。
#[test]
fn hint_summaries_reconcile_without_splitting_old_keys() {
    use crate::collection::jobs::AttemptKind;
    let diagnostics = Diagnostics::default();
    for hint in [false, true] {
        let context = AttemptContext {
            class: crate::collection::jobs::ClaimClass::Retry,
            kind: AttemptKind::Repeat,
            failed_attempts_before: 1,
            had_valid_hint: hint,
        };
        diagnostics.attempt(
            context,
            TaskTiming::DueWait,
            AttemptResult::Observed,
            Duration::from_secs(1),
        );
        diagnostics.attempt(
            context,
            TaskTiming::Execution,
            AttemptResult::Downloaded,
            Duration::from_secs(2),
        );
        diagnostics.committed(context);
    }
    {
        let pair = diagnostics.snapshots.lock().unwrap();
        assert_eq!(pair.0.attempts.project().timings.len(), 2);
        assert_eq!(pair.0.attempts.project().class_timings.len(), 2);
        assert_eq!(pair.0.attempts.project().committed.len(), 1);
        assert_eq!(pair.0.attempts.project().class_committed.len(), 1);
        assert_eq!(pair.0.attempts.project().committed.values().sum::<u64>(), 2);
        for counts in pair.0.attempts.project().hints.values() {
            assert_eq!(
                (
                    counts.claims,
                    counts.executions,
                    counts.downloaded,
                    counts.committed
                ),
                (1, 1, 1, 1)
            );
            assert_eq!(counts.execution_sum_ms, 2000);
        }
    }
    diagnostics.log_snapshot(true);
    let pair = diagnostics.snapshots.lock().unwrap();
    assert!(pair.1.attempts.project().hints.is_empty());
    assert_eq!(pair.0.attempts.project().hints.len(), 2);
}

/// 帧、会话、下载与提交采用不同时间点；取消未下载的会话不产生下载或提交。
#[test]
fn compatibility_events_reconcile_intervals_and_keep_payloads_private() {
    use crate::collection::peer::wire::parse_extension;
    let captured = tempfile::NamedTempFile::new().unwrap();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_ansi(false)
        .with_writer(captured.reopen().unwrap())
        .finish();
    let metrics = Arc::new(crate::collection::diagnostics::metrics::Metrics::default());
    tracing::subscriber::with_default(subscriber, || {
        let accepted = parse_extension(b"d1:zi1e1:ai2ee", 4096, 64);
        let rejected = parse_extension(b"d1:zi1e1:zi2ee", 4096, 64);
        let mut peer = PeerObservation::new(
            metrics.clone(),
            Source::Dht,
            false,
            Arc::default(),
            Arc::default(),
        );
        peer.extension_frame(&accepted);
        metrics.diagnostics.log();
        peer.extension_frame(&accepted);
        peer.extension_frame(&rejected);
        assert!(peer.extension_downloaded());
        assert!(peer.extension_downloaded());
        metrics.diagnostics.compatibility_committed();
        drop(peer);
        let mut cancelled = PeerObservation::new(
            metrics.clone(),
            Source::Dht,
            false,
            Arc::default(),
            Arc::default(),
        );
        cancelled.extension_frame(&accepted);
        drop(cancelled);
        metrics.diagnostics.log_snapshot(true);
        metrics.diagnostics.log_snapshot(true);
    });
    let logs = std::fs::read_to_string(captured.path()).unwrap();
    let events: Vec<serde_json::Value> = logs
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap()["fields"].clone())
        .filter(|fields| fields["event"] == "extension_compatibility_summary")
        .collect();
    assert_eq!(events.len(), 6);
    for field in [
        "attempted_frames",
        "accepted_frames",
        "rejected_frames",
        "sessions",
        "downloaded",
        "committed",
    ] {
        let total = events.iter().rev().find(|e| e["scope"] == "total").unwrap()[field]
            .as_u64()
            .unwrap();
        let sum: u64 = events
            .iter()
            .filter(|e| e["scope"] == "interval")
            .map(|e| e[field].as_u64().unwrap())
            .sum();
        assert_eq!(sum, total);
    }
    let final_total = &events[4];
    assert_eq!(final_total["attempted_frames"], 4);
    assert_eq!(final_total["accepted_frames"], 3);
    assert_eq!(final_total["rejected_frames"], 1);
    assert_eq!(final_total["sessions"], 2);
    assert_eq!(final_total["downloaded"], 1);
    assert_eq!(final_total["committed"], 1);
    for event in &events {
        assert_eq!(event["schema_version"], 1);
        for forbidden in ["hash", "address", "key", "payload", "metadata", "detail"] {
            assert!(event.get(forbidden).is_none());
        }
    }
    assert!(!logs.contains("bencode_error_sample"));
}

/// 两段握手各自诊断并记录完整 5 秒；Task 截断统一记录 Timeout/Task。
#[tokio::test(start_paused = true)]
async fn unified_observer_records_full_handshake_and_task_timeout() {
    let metrics = Arc::new(metrics::Metrics::default());
    let outer = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut peer = PeerObservation::new(
        metrics.clone(),
        Source::Dht,
        false,
        Arc::default(),
        outer.clone(),
    );
    peer.advance(Stage::StandardHandshake);
    tokio::time::advance(Duration::from_secs(1)).await;
    peer.advance(Stage::ExtensionHandshake);
    tokio::time::advance(Duration::from_secs(4)).await;
    peer.advance(Stage::Transfer);
    outer.store(true, std::sync::atomic::Ordering::Relaxed);
    drop(peer);
    let pair = metrics.diagnostics.snapshots.lock().unwrap();
    let handshake = pair.0.handshakes.values().next().unwrap();
    assert_eq!(handshake.count, 1);
    assert_eq!(handshake.sum_ms, 5000);
    assert_eq!(handshake.quantile(95), Some(5000));
    assert!(
        pair.0
            .peers
            .iter()
            .any(|(key, value)| key.stage == Stage::ExtensionHandshake && value.sum_ms == 4000)
    );
    assert!(pair.0.peers.keys().any(|key| key.stage == Stage::Transfer
        && key.result == ResultKind::Timeout
        && key.deadline == Deadline::Task));
}

/// 保存两条完整维度，输出同键只生成一条；先合桶再取分位，提交仍独立计数。
#[test]
fn complete_dimensions_project_without_duplicate_rows_or_quantile_arithmetic() {
    use crate::collection::jobs::AttemptKind;
    let diagnostics = Diagnostics::default();
    for (hint, millis) in [(false, 1), (true, 10_000)] {
        let context = AttemptContext {
            class: crate::collection::jobs::ClaimClass::Hint,
            kind: AttemptKind::First,
            failed_attempts_before: 0,
            had_valid_hint: hint,
        };
        diagnostics.attempt(
            context,
            TaskTiming::Execution,
            AttemptResult::Downloaded,
            Duration::from_millis(millis),
        );
        if hint {
            diagnostics.committed(context);
        }
    }
    let pair = diagnostics.snapshots.lock().unwrap();
    assert_eq!(pair.0.attempts.timings.len(), 2);
    let views = pair.0.attempts.project();
    assert_eq!(views.timings.len(), 1);
    assert_eq!(views.class_timings.len(), 1);
    let distribution = views.timings.values().next().unwrap();
    assert_eq!((distribution.count, distribution.sum_ms), (2, 10_001));
    assert_eq!(distribution.quantile(50), Some(1));
    assert_eq!(distribution.quantile(95), Some(10_000));
    assert_eq!(views.class_timings.values().next().unwrap().sum_ms, 10_001);
    assert_eq!(views.committed.values().sum::<u64>(), 1);
    assert_eq!(views.hints[&(AttemptKind::First, false)].committed, 0);
}

/// 完整握手跨区间在结束时记一次；失败、外层超时和取消保持独立结果。
#[tokio::test(start_paused = true)]
async fn handshake_totals_intervals_failures_and_overflow_are_explicit() {
    use tracing::instrument::WithSubscriber;
    let logs = tempfile::NamedTempFile::new().unwrap();
    let writer = logs.reopen().unwrap();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_ansi(false)
        .with_writer(move || writer.try_clone().unwrap())
        .finish();
    async {
        let metrics = Arc::new(metrics::Metrics::default());
        // Connect 取消不构造不存在的握手样本。
        drop(PeerObservation::new(
            metrics.clone(),
            Source::Dht,
            false,
            Arc::default(),
            Arc::default(),
        ));
        assert!(
            metrics
                .diagnostics
                .snapshots
                .lock()
                .unwrap()
                .0
                .handshakes
                .is_empty()
        );
        for (index, result, deadline) in [
            (0, ResultKind::Success, Deadline::None),
            (1, ResultKind::Protocol, Deadline::None),
            (2, ResultKind::Timeout, Deadline::Stage),
            (3, ResultKind::Timeout, Deadline::Peer),
            (4, ResultKind::Timeout, Deadline::Task),
            (5, ResultKind::Cancelled, Deadline::None),
        ] {
            let outer = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let mut peer = PeerObservation::new(
                metrics.clone(),
                Source::Announce,
                true,
                Arc::default(),
                outer.clone(),
            );
            peer.advance(Stage::StandardHandshake);
            tokio::time::advance(Duration::from_secs(1)).await;
            peer.advance(Stage::ExtensionHandshake);
            metrics.log();
            tokio::time::advance(Duration::from_secs(if index == 0 { 180 } else { 4 })).await;
            // 相同阶段的增量更新不重置整体握手起点。
            peer.advance(Stage::ExtensionHandshake);
            match index {
                0 => peer.advance(Stage::Transfer),
                4 => outer.store(true, std::sync::atomic::Ordering::Relaxed),
                5 => {}
                _ => {
                    peer.finish(result, deadline);
                    peer.finish(result, deadline);
                }
            }
            drop(peer);
            let pair = metrics.diagnostics.snapshots.lock().unwrap();
            let key = (Source::Announce, true, result, deadline);
            let total = &pair.0.handshakes[&key];
            let interval = &pair.1.handshakes[&key];
            assert_eq!((total.count, interval.count), (1, 1));
            assert_eq!(total.sum_ms, if index == 0 { 181_000 } else { 5_000 });
        }
        metrics.log_final();
        assert!(
            metrics
                .diagnostics
                .snapshots
                .lock()
                .unwrap()
                .1
                .handshakes
                .is_empty()
        );
        metrics.log_final();
    }
    .with_subscriber(subscriber)
    .await;
    let text = std::fs::read_to_string(logs.path()).unwrap();
    let events: Vec<serde_json::Value> = text
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap()["fields"].clone())
        .collect();
    assert!(
        !events
            .iter()
            .any(|f| f["event"] == "peer_phase" || f["event"] == "task_diagnostic")
    );
    let handshakes: Vec<_> = events
        .iter()
        .filter(|f| f["event"] == "peer_handshake_diagnostic")
        .collect();
    assert_eq!(
        handshakes
            .iter()
            .filter(|f| f["scope"] == "interval")
            .map(|f| f["count"].as_u64().unwrap())
            .sum::<u64>(),
        6
    );
    let success = handshakes
        .iter()
        .find(|f| f["result"] == "Success")
        .unwrap();
    assert_eq!(success["overflow"], 1);
    assert_eq!(success["p95_exceeds_ms"], 180_000);
    assert!(success.get("p95_upper_bound_ms").is_none());
    for event in &handshakes {
        assert_eq!(event["schema_version"], 1);
        assert!(event["final_snapshot"].is_boolean());
        for key in ["hash", "address", "metadata", "payload"] {
            assert!(event.get(key).is_none());
        }
    }
    for event in &events {
        if matches!(
            event["event"].as_str(),
            Some("peer_diagnostic" | "peer_failure_detail" | "collector_counter")
        ) {
            assert_eq!(event["schema_version"], 2);
        }
    }
}

/// 未 poll 的 future 不开始计时；跨区间中止与 panic 只在析构时归入一次执行。
#[tokio::test(start_paused = true)]
async fn execution_starts_on_poll_and_abort_or_panic_finishes_once() {
    let metrics = Arc::new(metrics::Metrics::default());
    let context = AttemptContext {
        kind: crate::collection::jobs::AttemptKind::First,
        class: ClaimClass::Recent,
        failed_attempts_before: 0,
        had_valid_hint: false,
    };
    let unpolled = async {
        let _report = TaskReport::new(metrics.clone(), context);
        std::future::pending::<()>().await;
    };
    tokio::time::advance(Duration::from_secs(10)).await;
    drop(unpolled);
    assert!(
        metrics
            .diagnostics
            .snapshots
            .lock()
            .unwrap()
            .0
            .attempts
            .timings
            .is_empty()
    );
    let (started, ready) = tokio::sync::oneshot::channel();
    let worker_metrics = metrics.clone();
    let worker = tokio::spawn(async move {
        let _report = TaskReport::new(worker_metrics, context);
        started.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    ready.await.unwrap();
    tokio::time::advance(Duration::from_secs(2)).await;
    metrics.log();
    tokio::time::advance(Duration::from_secs(3)).await;
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
    let worker_metrics = metrics.clone();
    let panicked = tokio::spawn(async move {
        let _report = TaskReport::new(worker_metrics, context);
        panic!("测试 worker 异常释放");
    });
    assert!(panicked.await.unwrap_err().is_panic());
    let pair = metrics.diagnostics.snapshots.lock().unwrap();
    let key = (context, TaskTiming::Execution, AttemptResult::Cancelled);
    for snapshot in [&pair.0, &pair.1] {
        let execution = &snapshot.attempts.timings[&key];
        assert_eq!((execution.count, execution.sum_ms), (2, 5000));
        assert!(snapshot.attempts.committed.is_empty());
    }
}
