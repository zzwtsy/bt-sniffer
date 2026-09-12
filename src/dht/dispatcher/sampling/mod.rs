//! BEP 51：公开本地有效 hash 的随机样本，不采集查询记录，也不主动遍历网络。
//!
//! 此处服务远端的采样请求；本机主动发起采样的状态由相邻 sampler 模块持有。

use super::runtime::DhtDispatcher;
use crate::dht::peer_store::PeerStore;
use crate::krpc::{InfoHashSamples, InfoHashV1, KrpcErrorCode, QueryArgs};
use rand::{Rng, seq::SliceRandom};
use serde_bytes::ByteBuf;
use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

// 这是本程序的资源策略；BEP 51 允许 interval 为 0～21600 秒。
const SAMPLE_INTERVAL: Duration = Duration::from_secs(300);
const MAX_SAMPLES: usize = 32;

#[derive(Debug, Default)]
pub(super) struct SampleCache {
    generated_at: Option<Instant>,
    hashes: Vec<InfoHashV1>,
}

impl SampleCache {
    /// 同一轮不补抽新 hash；过期或被淘汰的 hash 只能移除，不能继续公开。
    fn sample(&mut self, peers: &PeerStore, now: Instant, rng: &mut impl Rng) -> InfoHashSamples {
        if self
            .generated_at
            .is_none_or(|at| now.saturating_duration_since(at) >= SAMPLE_INTERVAL)
        {
            self.hashes = peers.sample_infohashes(MAX_SAMPLES, now, rng);
            // 固定随机顺序，使预算裁剪取前缀时也不偏向某个 hash 或宣布时间。
            self.hashes.shuffle(rng);
            self.generated_at = Some(now);
        } else {
            self.hashes
                .retain(|hash| peers.contains_active_infohash(*hash, now));
        }
        // 发送层可以裁剪副本，但不能因某次长 transaction ID 缩短整个缓存。
        InfoHashSamples(self.hashes.clone())
    }
}

impl DhtDispatcher {
    pub(super) async fn handle_sample_infohashes(
        &mut self,
        source: SocketAddr,
        t: ByteBuf,
        args: &QueryArgs,
        now: Instant,
    ) -> bool {
        let Some(target) = args.target else {
            self.send_error(
                source,
                t,
                KrpcErrorCode::Protocol,
                "sample_infohashes 缺少 target",
            )
            .await;
            return false;
        };
        if args.info_hash.is_some()
            || args.port.is_some()
            || args.token.is_some()
            || args.implied_port.is_some()
        {
            self.send_error(
                source,
                t,
                KrpcErrorCode::Protocol,
                "sample_infohashes 包含其他方法的参数",
            )
            .await;
            return false;
        }
        let mut response = self.node_response(&target.0, &args.want, now);
        response.samples = Some(self.samples.sample(&self.peers, now, &mut rand::rng()));
        response.interval = Some(SAMPLE_INTERVAL.as_secs() as u32);
        // num 是当前有效 hash 数，不是快照生成时的数量，更不是 peer 地址总数。
        response.num = Some(self.peers.active_infohash_count(now) as u64);
        self.send_response(source, t, response).await
    }
}

#[cfg(test)]
mod tests;
