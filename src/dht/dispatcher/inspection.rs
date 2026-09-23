//! dispatcher 自身提供快照，HTTP 不直接读取协议状态。
use super::{
    api::{Command, DhtHandle, QueryError},
    runtime::DhtDispatcher,
};
use serde_json::{Value, json};
impl DhtHandle {
    pub(crate) async fn inspect(&self, routing: bool) -> Result<Value, QueryError> {
        let (reply, result) = tokio::sync::oneshot::channel();
        self.commands
            .send(Command::Inspect { routing, reply })
            .await
            .map_err(|_| QueryError::DispatcherClosed)?;
        result.await.map_err(|_| QueryError::DispatcherClosed)
    }
}
impl DhtDispatcher {
    pub(super) fn inspection(&self, routing: bool) -> Value {
        let sampler = self.sampler.status();
        let mut value = json!({
            "available": true,
            "family": match self.routing.address_family() {
                crate::dht::routing::AddressFamily::Ipv4 => "ipv4",
                crate::dht::routing::AddressFamily::Ipv6 => "ipv6",
            },
            "node_id": crate::observation::hex(&self.routing.local_id().0),
            "address": self.transport.local_addr().ok().map(|a| a.to_string()),
            "pending": self.pending.len(),
            "queued": self.queued.len(),
            "sampling": {
                "pause": sampler.pause,
                "running": sampler.running,
                "collector_paused": self.sampling_paused,
                "candidates": sampler.candidates,
                "in_flight": sampler.in_flight,
                "successful": sampler.successful,
                "failed": sampler.failed,
                "unsupported": sampler.unsupported,
                "storage_error": sampler.storage_error.is_some(),
            },
        });
        value["traffic"] = serde_json::to_value(self.budget.snapshot()).expect("流量快照");
        value["sampling"]["detail"] = self.sampler.inspection();
        value["rpc"] = json!({
            "queued": self.queued.iter().map(|queued| json!({
                "context": queued.observation.observer.context,
                "peer": queued.remote.address.to_string(),
            })).collect::<Vec<_>>(),
            "pending": self.pending.values().map(|pending| json!({
                "context": pending.observation.observer.context,
                "peer": pending.remote.address.to_string(),
            })).collect::<Vec<_>>(),
        });
        if routing {
            value["routing"] = self.routing.inspection(std::time::Instant::now());
        }
        value
    }
}
