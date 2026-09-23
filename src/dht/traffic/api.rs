//! Traffic 配置、类别、决策与稳定统计结构。

use std::time::Duration;

/// 会话级 DHT 配额输入；双栈及各主动来源共享，校验后用于构造限流器。
#[derive(Debug, Clone, Copy)]
pub(crate) struct Config {
    /// 主动查询总配额，单位包/秒，按 Class 分配；允许一秒额度突发。
    pub(crate) queries: u32,
    /// 普通入站数据报的包/秒上限，响应预留另计。
    pub(crate) inbound: u32,
    /// UDP payload 字节/秒；1/8 用于主动查询，剩余用于回复。
    pub(crate) upload: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            queries: 20,
            inbound: 200,
            upload: 262144,
        }
    }
}

impl Config {
    pub(crate) fn validate(self) -> Result<Self, &'static str> {
        if self.queries < 10 || self.inbound == 0 || self.upload < 32768 {
            return Err("DHT 主动速率至少 10/s，入站至少 1/s，发送至少 32768 B/s");
        }
        Ok(self)
    }

    pub(super) fn classes(self) -> [u32; 4] {
        let queries = self.queries;
        let fetch = queries / 2;
        let sample = queries / 10;
        [fetch, queries - fetch - 2 * sample, sample, sample]
    }
}

/// 数组顺序固定为采集、控制、采样、反向验证，不能只调整枚举而不核对统计数组。
#[derive(Debug, Clone, Copy)]
#[repr(usize)]
pub(crate) enum Class {
    Collector,
    Control,
    Sampling,
    Verification,
}

/// 一次组合探测的结果；允许发送时配额已扣减，实际 socket 发送仍由 dispatcher 执行。
pub(crate) struct Decision {
    pub(crate) wait: Duration,
    /// 位 0..=3 分别表示类别、目的 IP、发送字节、IP 表容量受限，可同时出现。
    pub(crate) reasons: u8,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub(crate) struct Stats {
    pub(crate) validated_v4: u64,
    pub(crate) validated_v6: u64,
    /// 按 Class 排列的实际成功发送包数；不是配额扣减或出队次数。
    pub(crate) packets: [u64; 4],
    pub(crate) bytes: [u64; 4],
    pub(crate) inbound_packets: u64,
    pub(crate) inbound_bytes: u64,
    pub(crate) reply_packets: u64,
    pub(crate) reply_bytes: u64,
    pub(crate) limited_drops: u64,
    pub(crate) queue_timeouts: u64,
    /// 累计接纳的待发意图数；当前占用另由 State.queued_current 保存。
    pub(crate) queued: [u64; 4],
    /// 每类按出队发送、取消、超时、本地拒绝排列。
    pub(crate) queue_finished: [[u64; 4]; 4],
    pub(crate) queue_wait: [crate::histogram::Histogram; 4],
    /// 每请求每原因至多一次：类别、IP、字节、IP 表。
    pub(crate) blocked: [[u64; 4]; 4],
    pub(crate) inflight_cancelled: [u64; 4],
    pub(crate) verification: VerificationStats,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub(crate) struct VerificationStats {
    pub(crate) admitted: u64,
    pub(crate) duplicate: u64,
    pub(crate) cooldown: u64,
    pub(crate) capacity: u64,
    pub(crate) ip_table: u64,
    pub(crate) succeeded: u64,
    pub(crate) failed: u64,
}
