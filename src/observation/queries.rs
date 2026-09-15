//! 在保留窗口内汇总发现与领取；不把淘汰后的事件当作从未发生。
use super::*;
use std::ops::Bound::{Excluded, Unbounded};

/// 只保存接口所需的标量，不引用事件正文或关联上下文。
#[derive(Debug, Clone, Copy)]
pub(super) struct DiscoveryFact {
    at_ms: u64,
    source: &'static str,
    step: &'static str,
    result: &'static str,
}

impl History {
    pub(super) fn index_discovery(
        &mut self,
        context: &Context,
        sequence: u64,
        at_ms: u64,
        step: &'static str,
        result: &'static str,
    ) {
        let Some(id) = context
            .batch_id
            .as_deref()
            .or(context.observation_id.as_deref())
        else {
            return;
        };
        if !self.discoveries.contains_key(id) {
            // 两个索引共享这一份 ID 分配；固定树节点数量受保留事件数限制。
            let id: Arc<str> = Arc::from(id);
            self.bytes += id.len();
            self.discovery_order.insert(sequence, id.clone());
            self.discoveries.insert(id, BTreeMap::new());
        }
        self.discoveries.get_mut(id).expect("已有发现对象").insert(
            sequence,
            DiscoveryFact {
                at_ms,
                source: if context.batch_id.is_some() {
                    "sampling"
                } else {
                    "announce"
                },
                step,
                result,
            },
        );
    }

    pub(super) fn remove_discovery(&mut self, entry: &Entry) {
        let Some(id) = entry
            .context
            .batch_id
            .as_deref()
            .or(entry.context.observation_id.as_deref())
        else {
            return;
        };
        let records = self.discoveries.get_mut(id).expect("事件已有发现索引");
        records.remove(&entry.sequence);
        // 全局按序淘汰，所以删除的必然也是这个对象的最早保留记录。
        let owned_id = self
            .discovery_order
            .remove(&entry.sequence)
            .expect("最早记录已有分页键");
        if let Some((&first, _)) = records.first_key_value() {
            self.discovery_order.insert(first, owned_id);
        } else {
            self.discoveries.remove(id);
            self.bytes -= owned_id.len();
        }
    }
}

impl Observer {
    pub(crate) fn has_discovery(&self, id: &str) -> bool {
        let hub = self.hub.as_ref().expect("已启用监控");
        let mut history = hub.history.lock().expect("观测锁");
        history.evict(hub.limits);
        history.events.iter().any(|entry| {
            entry.context.observation_id.as_deref() == Some(id)
                || entry.context.batch_id.as_deref() == Some(id)
        })
    }

    /// 游标为对象最早保留事件序号；锁内只复制一页摘要，JSON 在锁外构造。
    pub(crate) fn discoveries(&self, after: u64, limit: usize) -> Value {
        let hub = self.hub.as_ref().expect("已启用监控");
        let (mut summaries, window) = {
            let mut h = hub.history.lock().expect("观测锁");
            h.evict(hub.limits);
            let summaries: Vec<_> = h
                .discovery_order
                .range((Excluded(after), Unbounded))
                .take(limit + 1)
                .map(|(&first, id)| {
                    let records = &h.discoveries[id];
                    let (&last, last_fact) = records.last_key_value().expect("发现记录非空");
                    (id.to_string(), first, records[&first], last, *last_fact)
                })
                .collect();
            (summaries, h.window(hub))
        };
        let more = summaries.len() > limit;
        summaries.truncate(limit);
        let next = if more {
            summaries.last().map(|s| s.1.to_string())
        } else {
            None
        };
        let completeness = if window.evicted > 0 {
            "partial"
        } else {
            "complete"
        };
        let items: Vec<_> = summaries.into_iter().map(|(id, first, first_fact, last, last_fact)| {
            json!({"id":id,"source":first_fact.source,"first_sequence":first.to_string(),
                "first_retained_at_ms":first_fact.at_ms,"last_sequence":last.to_string(),
                "last_step":last_fact.step,"last_result":last_fact.result,"completeness":completeness})
        }).collect();
        json!({"items":items,"next":next,"window":window})
    }
    /// 按 generation 汇总本次运行仍保留的领取；peer 列表最多为本轮实际尝试的 8 个。
    pub(crate) fn attempts(&self, hash: &str, after: u64, limit: usize) -> Value {
        let hub = self.hub.as_ref().expect("已启用监控");
        let mut h = hub.history.lock().expect("观测锁");
        h.evict(hub.limits);
        let mut groups: BTreeMap<i64, Value> = BTreeMap::new();
        for entry in &h.events {
            if entry.context.hash.as_deref() != Some(hash) {
                continue;
            }
            let Some(generation) = entry
                .context
                .generation
                .filter(|g| *g >= 0 && *g as u64 > after)
            else {
                continue;
            };
            if !groups.contains_key(&generation) && groups.len() > limit {
                // 旧领取的迟到结果可能晚于新领取出现，分页始终选择最小 generation。
                if groups
                    .last_key_value()
                    .is_some_and(|(last, _)| generation >= *last)
                {
                    continue;
                }
                groups.pop_last();
            }
            let group=groups.entry(generation).or_insert_with(||json!({"generation":generation,"first_sequence":entry.sequence.to_string(),"peers":{},"completeness":if h.evicted>0{"partial"}else{"complete"}}));
            let event: Value = serde_json::from_str(&entry.encoded).unwrap();
            group["last_sequence"] = json!(entry.sequence.to_string());
            if entry.kind == Kind::Job && event["step"] == "claim" {
                group["claim"] = event["data"].clone();
            }
            if [Kind::Job, Kind::Commit, Kind::Retry].contains(&entry.kind) {
                group["last_step"] = event["step"].clone();
                group["last_result"] = event["result"].clone();
            }
            if let Some(id) = &entry.context.peer_attempt_id
                && (group["peers"].as_object().unwrap().contains_key(id)
                    || group["peers"].as_object().unwrap().len() < 8)
            {
                if group["peers"].get(id).is_none() {
                    group["peers"][id] = json!({"peer_attempt_id":id});
                }
                let peer = &mut group["peers"][id];
                peer["last_step"] = event["step"].clone();
                peer["last_result"] = event["result"].clone();
                if event["step"] == "candidate" {
                    peer["peer"] = event["data"]["peer"].clone();
                    peer["source"] = event["data"]["source"].clone();
                }
            }
        }
        let more = groups.len() > limit;
        let mut items: Vec<_> = groups.into_values().take(limit).collect();
        for item in &mut items {
            let peers = item["peers"]
                .as_object()
                .unwrap()
                .values()
                .cloned()
                .collect::<Vec<_>>();
            item["peers"] = json!(peers);
        }
        let next = if more {
            items.last().map(|v| v["generation"].to_string())
        } else {
            None
        };
        json!({"hash":hash,"items":items,"next":next,"window":h.window(hub),"completeness":if items.is_empty(){"unavailable"}else if h.evicted>0{"partial"}else{"complete"}})
    }
}
