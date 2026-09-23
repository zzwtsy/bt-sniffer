//! 查询结果对路由失败、插入决策和 bucket probe 的后续处理。

use super::super::{
    api::RemoteNode,
    runtime::{DhtDispatcher, PendingPurpose},
};
use crate::dht::{
    krpc::QueryMethod,
    routing::{InsertOutcome, NodeContact},
};
use std::time::Instant;

impl DhtDispatcher {
    pub(super) fn record_expected_failure(&mut self, remote: RemoteNode, now: Instant) {
        if let Some(id) = remote.expected_id {
            self.routing.record_failure(id, now);
        }
    }

    pub(super) async fn handle_insert_outcome(&mut self, outcome: InsertOutcome, now: Instant) {
        self.observer.emit(crate::observation::Kind::Routing, "insert", match &outcome {
            InsertOutcome::Inserted => "inserted",
            InsertOutcome::Updated => "updated",
            InsertOutcome::ReplacedBad { .. } => "replaced_bad",
            InsertOutcome::ProbeRequired { .. } => "probe_required",
            InsertOutcome::RejectedFull => "rejected_full",
            InsertOutcome::IgnoredSelf => "ignored_self",
            InsertOutcome::IgnoredWrongAddressFamily => "ignored_address_family",
        }, || match &outcome {
            InsertOutcome::ReplacedBad { removed } => serde_json::json!({"removed_id":crate::observation::hex(&removed.id.0),"removed_address":removed.address.to_string()}),
            _ => serde_json::json!({}),
        });
        if let InsertOutcome::ProbeRequired {
            mut incumbents,
            candidate,
        } = outcome
        {
            let incumbent = incumbents.remove(0);
            self.start_bucket_probe(incumbent, incumbents, candidate, 1, now)
                .await;
        }
    }

    pub(super) async fn start_bucket_probe(
        &mut self,
        incumbent: NodeContact,
        remaining: Vec<NodeContact>,
        candidate: NodeContact,
        attempt: u8,
        now: Instant,
    ) {
        let key = (incumbent.id, incumbent.address);
        if !self.bucket_probes.insert(key) {
            return;
        }
        self.start_query(
            RemoteNode {
                address: incumbent.address,
                expected_id: Some(incumbent.id),
            },
            QueryMethod::Ping,
            None,
            PendingPurpose::BucketProbe {
                incumbent,
                remaining,
                candidate,
                attempt,
            },
            now,
        )
        .await;
    }
}
