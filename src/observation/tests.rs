//! 有限历史与取消的离线契约。
#![cfg(test)]
use super::*;
#[tokio::test(start_paused = true)]
async fn retention_counts_bytes_and_time_are_independent() {
    for limits in [
        Limits {
            count: 2,
            bytes: MAX_BYTES,
            age: RETENTION,
        },
        Limits {
            count: MAX_EVENTS,
            bytes: 900,
            age: RETENTION,
        },
    ] {
        let observer = Observer::with_limits("test".into(), limits);
        for _ in 0..10 {
            observer.emit(Kind::Job, "claim", "applied", || json!({}));
        }
        let w = observer.window();
        assert!(w.evicted > 0);
        assert!(w.retained <= limits.count);
        assert!(w.bytes <= limits.bytes);
        tokio::time::advance(RETENTION).await;
        assert_eq!(observer.window().retained, 0);
    }
}
#[tokio::test]
async fn disabled_lazy_data_and_dropped_span() {
    Observer::default().emit(Kind::Job, "claim", "applied", || panic!("关闭时不构造数据"));
    let observer = Observer::new("test".into());
    {
        let _span = observer.for_job(&[1; 20], 2).span(Kind::Peer, "connect");
    }
    let page = observer.page(0, 100, &Filter::default());
    assert_eq!(page.events[1]["result"], "cancelled");
    assert_eq!(page.events[1]["context"]["generation"], 2);
    assert_eq!(observer.states()["active"].as_array().unwrap().len(), 0);
}
#[tokio::test]
async fn truncation_and_filtered_cursor() {
    let observer = Observer::new("test".into());
    observer.emit(
        Kind::Rpc,
        "response",
        "error",
        || json!({"detail":"汉".repeat(10000)}),
    );
    let page = observer.page(
        0,
        50,
        &Filter {
            hash: Some("absent".into()),
            ..Default::default()
        },
    );
    assert!(page.events.is_empty());
    assert_eq!(page.next, "1");
    assert_eq!(page.window.truncated, 1);
    let page = observer.page(0, 50, &Filter::default());
    assert!(page.events[0].to_string().len() <= MAX_EVENT_BYTES);
}

#[tokio::test]
async fn peer_stages_share_attempt_and_eviction_preserves_current_state() {
    let observer = Observer::with_limits(
        "test".into(),
        Limits {
            count: 2,
            bytes: MAX_BYTES,
            age: RETENTION,
        },
    );
    let attempt = observer.for_job(&[1; 20], 3).child(Kind::Peer);
    let connect = attempt.span(Kind::Peer, "connect");
    let transfer = attempt.span(Kind::Peer, "transfer");
    assert_eq!(
        connect.observer.context.peer_attempt_id,
        transfer.observer.context.peer_attempt_id
    );
    assert_ne!(
        connect.observer.context.span_id,
        transfer.observer.context.span_id
    );
    for _ in 0..10 {
        observer.emit(Kind::Rpc, "query", "sent", || json!({}));
    }
    assert_eq!(observer.states()["active"].as_array().unwrap().len(), 2);
    drop(connect);
    drop(transfer);
    assert!(observer.states()["active"].as_array().unwrap().is_empty());
    let next = observer.child(Kind::Peer);
    assert_ne!(
        attempt.context.peer_attempt_id,
        next.context.peer_attempt_id
    );
}
