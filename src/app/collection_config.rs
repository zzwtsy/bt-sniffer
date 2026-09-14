//! 应用一次确定有效采集配置；启动日志和执行入口共用这些值。
use super::config::Cli;
use crate::collection::{SampleBackpressure, peer::MetadataConfig};

pub(super) struct CollectionSettings {
    pub(super) metadata: MetadataConfig,
    pub(super) backpressure: SampleBackpressure,
}
impl CollectionSettings {
    pub(super) fn new(cli: &Cli) -> Self {
        Self {
            metadata: MetadataConfig {
                address_policy: cli.policy(),
                ..Default::default()
            },
            backpressure: if cli.sample && cli.fetch {
                cli.sample_backpressure
            } else {
                SampleBackpressure::Capacity
            },
        }
    }
}
