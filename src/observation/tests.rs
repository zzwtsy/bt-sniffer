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
        assert_eq!(
            observer.attempts(&hex(&[1; 20]), 0, 50)["completeness"],
            "unavailable"
        );
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

#[tokio::test]
async fn attempt_pagination_orders_late_generations_without_losing_them() {
    let observer = Observer::new("test".into());
    for generation in [3, 4, 2] {
        observer
            .for_job(&[1; 20], generation)
            .emit(Kind::Job, "claim", "applied", || json!({}));
    }
    let first = observer.attempts(&hex(&[1; 20]), 0, 1);
    assert_eq!(first["items"][0]["generation"], 2);
    assert_eq!(first["next"], "2");
    let second = observer.attempts(&hex(&[1; 20]), 2, 1);
    assert_eq!(second["items"][0]["generation"], 3);
    assert_eq!(second["next"], "3");
}

use std::collections::{HashMap, HashSet};
impl Observer {
    fn replay_discoveries(&self, after: u64, limit: usize) -> Value {
        let hub = self.hub.as_ref().expect("已启用监控");
        let mut h = hub.history.lock().expect("观测锁");
        h.evict(hub.limits);
        let mut seen = HashSet::new();
        let mut selected = HashMap::new();
        let mut items: Vec<Value> = Vec::new();
        let mut next = None;
        for entry in &h.events {
            let Some(id) = entry
                .context
                .batch_id
                .as_deref()
                .or(entry.context.observation_id.as_deref())
            else {
                continue;
            };
            if seen.insert(id) && entry.sequence > after && items.len() < limit + 1 {
                selected.insert(id, items.len());
                items.push(json!({"id":id,"source":if entry.context.batch_id.is_some(){"sampling"}else{"announce"},"first_sequence":entry.sequence.to_string(),"first_retained_at_ms":serde_json::from_str::<Value>(&entry.encoded).unwrap()["at_ms"],"last_sequence":entry.sequence.to_string(),"completeness":if h.evicted>0{"partial"}else{"complete"}}));
            }
            if let Some(&index) = selected.get(id) {
                let event: Value = serde_json::from_str(&entry.encoded).unwrap();
                items[index]["last_sequence"] = json!(entry.sequence.to_string());
                items[index]["last_step"] = event["step"].clone();
                items[index]["last_result"] = event["result"].clone();
            }
        }
        if items.len() > limit {
            items.truncate(limit);
            next = items.last().map(|item| item["first_sequence"].clone());
        }
        json!({"items":items,"next":next,"window":h.window(hub)})
    }
}

/// 重放仅用于测试，独立验证索引在交错追加和淘汰后仍保持接口语义。
fn assert_discovery_index(observer: &Observer) {
    for after in [0, 1, 2, 4, 8, 20, u64::MAX] {
        for limit in [1, 2, 100] {
            assert_eq!(
                observer.discoveries(after, limit),
                observer.replay_discoveries(after, limit)
            );
        }
    }
    let h = observer.hub.as_ref().unwrap().history.lock().unwrap();
    assert_eq!(h.discovery_order.len(), h.discoveries.len());
    let indexed: usize = h.discoveries.values().map(BTreeMap::len).sum();
    assert_eq!(
        indexed,
        h.events
            .iter()
            .filter(|e| e.context.batch_id.is_some() || e.context.observation_id.is_some())
            .count()
    );
    assert_eq!(
        h.bytes,
        h.events.iter().map(|e| e.retained_bytes).sum::<usize>()
            + h.discoveries.keys().map(|id| id.len()).sum::<usize>()
    );
}

#[tokio::test(start_paused = true)]
async fn discovery_index_matches_replay_through_eviction_and_reuse() {
    for (count, bytes) in [(MAX_EVENTS, MAX_BYTES), (3, MAX_BYTES), (MAX_EVENTS, 1500)] {
        let observer = Observer::with_limits(
            "test".into(),
            Limits {
                count,
                bytes,
                age: RETENTION,
            },
        );
        assert_discovery_index(&observer);
        let mut batch = observer.clone();
        batch.context.batch_id = Some("batch".into());
        batch.context.observation_id = Some("alias".into());
        let mut announce = observer.clone();
        announce.context.observation_id = Some("announce".into());
        for i in 0..12 {
            let source = match i % 3 {
                0 => &batch,
                1 => &announce,
                _ => &observer,
            };
            source.emit(
                Kind::Discovery,
                if i < 3 { "observed" } else { "saved" },
                if i % 2 == 0 { "new" } else { "duplicate" },
                || json!({}),
            );
            assert_discovery_index(&observer);
            tokio::time::advance(Duration::from_millis(1)).await;
        }
        assert!(observer.has_discovery("alias"));
        assert!(observer.has_discovery("batch"));
        assert!(
            !observer.discoveries(0, 100)["items"]
                .as_array()
                .unwrap()
                .iter()
                .any(|v| v["id"] == "alias")
        );
        tokio::time::advance(RETENTION).await;
        assert_discovery_index(&observer);
        assert_eq!(observer.window().bytes, 0);
        assert!(!observer.has_discovery("alias"));
        batch.emit(Kind::Discovery, "reused", "new", || json!({}));
        assert_discovery_index(&observer);
        let page = observer.discoveries(0, 100);
        assert_eq!(page["items"][0]["first_sequence"], "13");
        assert_eq!(page["items"][0]["last_step"], "reused");
        assert_eq!(page["items"][0]["completeness"], "partial");
    }
}

#[tokio::test]
async fn discovery_id_allocation_participates_in_byte_budget() {
    let observer = Observer::new("test".into());
    let mut batch = observer.clone();
    batch.context.batch_id = Some("batch".into());
    batch.emit(Kind::Discovery, "saved", "new", || json!({}));
    let mut h = observer.hub.as_ref().unwrap().history.lock().unwrap();
    let event_bytes = h.events[0].retained_bytes;
    assert_eq!(h.bytes, event_bytes + 5);
    h.evict(Limits {
        count: MAX_EVENTS,
        bytes: event_bytes,
        age: RETENTION,
    });
    assert!(h.events.is_empty());
    assert!(h.discoveries.is_empty());
    assert!(h.discovery_order.is_empty());
    assert_eq!(h.bytes, 0);
}

#[tokio::test(start_paused = true)]
async fn discovery_time_eviction_moves_page_key_and_first_source() {
    let observer = Observer::new("test".into());
    let mut announce = observer.clone();
    announce.context.observation_id = Some("shared".into());
    announce.emit(Kind::Discovery, "observed", "new", || json!({}));
    tokio::time::advance(Duration::from_secs(1)).await;
    let other = observer.child(Kind::Sampling);
    other.emit(Kind::Discovery, "observed", "new", || json!({}));
    let mut batch = observer.clone();
    batch.context.batch_id = Some("shared".into());
    batch.emit(Kind::Discovery, "saved", "duplicate", || json!({}));
    assert_eq!(observer.discoveries(0, 1)["items"][0]["source"], "announce");
    tokio::time::advance(RETENTION - Duration::from_secs(1)).await;
    assert_discovery_index(&observer);
    let first = observer.discoveries(0, 1);
    assert_eq!(first["next"], "2");
    assert_eq!(
        first["items"][0]["id"],
        other.context.batch_id.as_deref().unwrap()
    );
    let second = observer.discoveries(2, 1);
    assert_eq!(second["items"][0]["id"], "shared");
    assert_eq!(second["items"][0]["source"], "sampling");
    assert_eq!(second["items"][0]["first_sequence"], "3");
    assert_eq!(second["items"][0]["last_result"], "duplicate");
    assert_eq!(second["next"], Value::Null);
}
