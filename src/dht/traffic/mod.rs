//! 会话共享 DHT 配额。所有检查在一个短同步临界区完成，不跨 await。
//! governor 负责 GCRA；可探测状态先检查组合配额，再原子提交，受限 IP 不耗尽其他配额。
use governor::{
    Quota, RateLimiter,
    clock::Clock,
    nanos::Nanos,
    state::{NotKeyed, StateStore},
};
use std::{
    collections::HashMap,
    net::IpAddr,
    num::NonZeroU32,
    sync::{Arc, Mutex},
    time::Duration,
};

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
    fn classes(self) -> [u32; 4] {
        let q = self.queries;
        let fetch = q / 2;
        let sample = q / 10;
        [fetch, q - fetch - 2 * sample, sample, sample]
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
/// governor 使用从 Tokio 单调时钟锚点起的时长，暂停时间测试也能驱动配额。
#[derive(Debug, Clone)]
struct TokioClock(tokio::time::Instant);
impl Clock for TokioClock {
    type Instant = Duration;
    fn now(&self) -> Duration {
        self.0.elapsed()
    }
}
/// 元组为 GCRA 的已提交时间状态与提交开关；探测时计算结果但不写回时间。
/// 外层 Budget 锁覆盖探测和提交全过程，防止中间被其他请求消耗配额。
#[derive(Debug, Clone, Default)]
struct ProbeState(Arc<Mutex<(Option<Nanos>, bool)>>);
impl StateStore for ProbeState {
    type Key = NotKeyed;
    fn measure_and_replace<T, F, E>(&self, _: &NotKeyed, f: F) -> Result<T, E>
    where
        F: Fn(Option<Nanos>) -> Result<(T, Nanos), E>,
    {
        let mut state = self.0.lock().expect("配额状态锁");
        let (value, next) = f(state.0)?;
        if state.1 {
            state.0 = Some(next);
        }
        Ok(value)
    }
}
#[derive(Debug)]
struct Limiter {
    limiter: RateLimiter<
        NotKeyed,
        ProbeState,
        TokioClock,
        governor::middleware::NoOpMiddleware<Duration>,
    >,
    state: ProbeState,
}
impl Limiter {
    fn new(rate: u32) -> Self {
        let state = ProbeState::default();
        // 向上取整每单位间隔，避免非整除速率因纳秒舍入超过约定上界。
        let quota =
            Quota::with_period(Duration::from_nanos(1_000_000_000u64.div_ceil(rate as u64)))
                .unwrap()
                .allow_burst(NonZeroU32::new(rate).unwrap());
        Self {
            limiter: RateLimiter::new(
                quota,
                state.clone(),
                TokioClock(tokio::time::Instant::now()),
            ),
            state,
        }
    }
    /// 返回需要等待的时长；commit=false 只探测，true 才提交成功的扣减。
    /// 单次请求超过突发容量时返回 5 秒等待提示，不表示等待后该请求必定可发送。
    fn check(&self, n: u32, commit: bool) -> Duration {
        self.state.0.lock().expect("配额状态锁").1 = commit;
        match self.limiter.check_n(NonZeroU32::new(n.max(1)).unwrap()) {
            Ok(Ok(())) => Duration::ZERO,
            Ok(Err(until)) => until.wait_time_from(self.limiter.clock().now()),
            Err(_) => Duration::from_secs(5),
        }
    }
}
/// 一次组合探测的结果；允许发送时配额已扣减，实际 socket 发送仍由 dispatcher 执行。
pub(crate) struct Decision {
    pub(crate) wait: Duration,
    /// 位 0..=3 分别表示类别、目的 IP、发送字节、IP 表容量受限，可同时出现。
    pub(crate) reasons: u8,
}
/// 待发意图拥有统计票据，所有 remove/retain/关闭路径都会归还当前数量。
#[derive(Debug)]
pub(crate) struct QueueRecord {
    budget: Arc<Budget>,
    class: Class,
    start: tokio::time::Instant,
    /// 0 出队发送、1 取消、2 排队超时、3 本地拒绝；默认取消，Drop 时结算。
    outcome: usize,
    /// 本请求已记录的等待原因，避免循环探测重复增加统计。
    seen: u8,
}
impl QueueRecord {
    /// 只设置最终原因；outcome 必须为 0..=3，当前占用和耗时到 Drop 才结算。
    pub(crate) fn finish(&mut self, outcome: usize) {
        self.outcome = outcome;
    }
    /// 每请求每原因最多记录一次；位定义与 Decision.reasons 一致。
    pub(crate) fn blocked(&mut self, reasons: u8) {
        let fresh = reasons & !self.seen;
        self.seen |= reasons;
        self.budget.update(|s| {
            for i in 0..4 {
                if fresh & (1 << i) != 0 {
                    s.blocked[self.class as usize][i] += 1;
                }
            }
        });
    }
}
impl Drop for QueueRecord {
    fn drop(&mut self) {
        let mut state = self.budget.0.lock().expect("流量锁");
        state.queued_current[self.class as usize] -= 1;
        let elapsed = self.start.elapsed();
        state.update(|s| {
            s.queue_finished[self.class as usize][self.outcome] += 1;
            s.queue_wait[self.class as usize].record(elapsed, false);
            if self.outcome == 2 {
                s.queue_timeouts += 1;
            }
        });
    }
}
#[derive(Debug)]
struct IpState {
    inbound: Limiter,
    queries: Limiter,
    touched: tokio::time::Instant,
    verification_until: Option<tokio::time::Instant>,
}
#[derive(Debug, Clone, Default)]
#[cfg_attr(test, derive(serde::Serialize))]
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
    pub(crate) queue_wait: [crate::metrics::Histogram; 4],
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
/// 只占用共享待发名额；移交发送前归还，不跟随在途 transaction。
#[derive(Debug)]
pub(crate) struct VerificationPermit(Arc<Budget>);
impl Drop for VerificationPermit {
    fn drop(&mut self) {
        self.0.0.lock().expect("流量锁").verification_queued -= 1;
    }
}
#[derive(Debug)]
struct State {
    queries: [Limiter; 4],
    inbound: Limiter,
    responses: Limiter,
    query_bytes: Limiter,
    reply_bytes: Limiter,
    ips: HashMap<IpAddr, IpState>,
    cleanup: tokio::time::Instant,
    total: Stats,
    interval: Stats,
    queued_current: [u64; 4],
    verification_queued: u64,
    verification_limit: u64,
}
/// 会话共享的配额和统计所有者；一个同步锁保证组合配额检查与扣减不可交错。
#[derive(Debug)]
pub(crate) struct Budget(Mutex<State>);
impl Default for Budget {
    fn default() -> Self {
        Self::new(Config::default()).expect("默认配额有效")
    }
}
impl Budget {
    pub(crate) fn new(config: Config) -> Result<Self, &'static str> {
        let config = config.validate()?;
        Ok(Self(Mutex::new(State {
            queries: config.classes().map(Limiter::new),
            inbound: Limiter::new(config.inbound),
            responses: Limiter::new(128),
            query_bytes: Limiter::new(config.upload / 8),
            reply_bytes: Limiter::new(config.upload - config.upload / 8),
            ips: HashMap::new(),
            cleanup: tokio::time::Instant::now(),
            total: Stats::default(),
            interval: Stats::default(),
            queued_current: [0; 4],
            verification_queued: 0,
            verification_limit: u64::from(config.classes()[Class::Verification as usize]) * 5,
        })))
    }
    /// 返回下次值得尝试的间隔，成功时一次性扣除类别、IP 与发送字节。
    #[cfg(test)]
    pub(crate) fn query(&self, class: Class, ip: IpAddr, bytes: usize) -> Duration {
        self.query_observed(class, ip, bytes).wait
    }
    /// 同一临界区内先探测类别、IP、字节三项，仅全部可用时统一扣减。
    /// wait=0 表示已获发送额度；IP 表满或任一项受限都不扣减其余配额。
    pub(crate) fn query_observed(&self, class: Class, ip: IpAddr, bytes: usize) -> Decision {
        let mut s = self.0.lock().expect("流量锁");
        if !s.track(ip) {
            return Decision {
                wait: Duration::from_secs(1),
                reasons: 8,
            };
        }
        let limits = [
            (&s.queries[class as usize], 1),
            (&s.ips[&ip].queries, 1),
            (&s.query_bytes, bytes as u32),
        ];
        let waits = limits.map(|(l, n)| l.check(n, false));
        let wait = *waits.iter().max().unwrap();
        let reasons =
            waits.iter().enumerate().fold(
                0,
                |mask, (i, w)| if w.is_zero() { mask } else { mask | (1 << i) },
            );
        if wait.is_zero() {
            for (l, n) in limits {
                l.check(n, true);
            }
        }
        Decision { wait, reasons }
    }
    pub(crate) fn verification_duplicate(&self) {
        self.update(|s| s.verification.duplicate += 1);
    }
    pub(crate) fn verification_result(&self, success: bool) {
        self.update(|s| {
            if success {
                s.verification.succeeded += 1
            } else {
                s.verification.failed += 1
            }
        });
    }
    /// 检查 IP 表、60 秒冷却和共享待发名额；成功即开始冷却并返回待发许可。
    /// 拒绝不会延长已有冷却；许可释放只归还名额，不撤销已开始的冷却。
    pub(crate) fn admit_verification(self: &Arc<Self>, ip: IpAddr) -> Option<VerificationPermit> {
        let mut s = self.0.lock().expect("流量锁");
        if !s.track(ip) {
            s.update(|s| s.verification.ip_table += 1);
            return None;
        }
        let now = tokio::time::Instant::now();
        if s.ips[&ip]
            .verification_until
            .is_some_and(|until| until > now)
        {
            s.update(|s| s.verification.cooldown += 1);
            return None;
        }
        if s.verification_queued >= s.verification_limit {
            s.update(|s| s.verification.capacity += 1);
            return None;
        }
        s.ips.get_mut(&ip).unwrap().verification_until = Some(now + Duration::from_secs(60));
        s.verification_queued += 1;
        s.update(|s| s.verification.admitted += 1);
        Some(VerificationPermit(self.clone()))
    }
    pub(crate) fn validated(&self, ip: IpAddr) {
        self.update(|s| {
            if ip.is_ipv4() {
                s.validated_v4 += 1;
            } else {
                s.validated_v6 += 1;
            }
        });
    }
    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> Stats {
        self.0.lock().expect("流量锁").total.clone()
    }
    pub(crate) fn sent(&self, class: Class, bytes: usize) {
        self.update(|s| {
            s.packets[class as usize] += 1;
            s.bytes[class as usize] += bytes as u64;
        });
    }
    /// 解码前接纳数据报；pending_source 只选择响应预留，不能替代后续身份与来源校验。
    pub(crate) fn inbound(&self, ip: IpAddr, bytes: usize, pending_source: bool) -> bool {
        let mut s = self.0.lock().expect("流量锁");
        let allowed = if pending_source {
            s.responses.check(1, true).is_zero()
        } else {
            s.ordinary(ip)
        };
        s.update(|stats| {
            stats.inbound_packets += 1;
            stats.inbound_bytes += bytes as u64;
            if !allowed {
                stats.limited_drops += 1;
            }
        });
        allowed
    }
    pub(crate) fn disguised_query(&self, ip: IpAddr) -> bool {
        let mut s = self.0.lock().expect("流量锁");
        let allowed = s.ordinary(ip);
        if !allowed {
            s.update(|stats| stats.limited_drops += 1);
        }
        allowed
    }
    pub(crate) fn reply(&self, bytes: usize) -> bool {
        let mut s = self.0.lock().expect("流量锁");
        let allowed = s.reply_bytes.check(bytes as u32, true).is_zero();
        if !allowed {
            s.update(|stats| stats.limited_drops += 1);
        }
        allowed
    }
    pub(crate) fn reply_sent(&self, bytes: usize) {
        self.update(|s| {
            s.reply_packets += 1;
            s.reply_bytes += bytes as u64;
        });
    }
    pub(crate) fn dropped(&self) {
        self.update(|s| s.limited_drops += 1);
    }
    pub(crate) fn inflight_cancelled(&self, class: Class) {
        self.update(|s| s.inflight_cancelled[class as usize] += 1);
    }
    pub(crate) fn queue_record(self: &Arc<Self>, class: Class) -> QueueRecord {
        let mut state = self.0.lock().expect("流量锁");
        state.queued_current[class as usize] += 1;
        state.update(|s| s.queued[class as usize] += 1);
        QueueRecord {
            budget: self.clone(),
            class,
            start: tokio::time::Instant::now(),
            outcome: 1,
            seen: 0,
        }
    }
    fn update(&self, f: impl Fn(&mut Stats)) {
        self.0.lock().expect("流量锁").update(f);
    }
    /// 清理 IP 表并取走区间统计；当前实现持 Budget 锁输出，队列丢弃也不会还原区间。
    pub(crate) fn log(&self) {
        let mut s = self.0.lock().expect("流量锁");
        s.clean();
        let interval = std::mem::take(&mut s.interval);
        tracing::info!(
            event = "dht_occupancy",
            schema_version = 1u64,
            tracked_ips = s.ips.len(),
            verification_queued = s.verification_queued,
            "DHT 当前占用"
        );
        for (scope, stats) in [("total", &s.total), ("interval", &interval)] {
            tracing::info!(
                event = "dht_traffic",
                schema_version = 1u64,
                scope,
                inbound_packets = stats.inbound_packets,
                inbound_bytes = stats.inbound_bytes,
                reply_packets = stats.reply_packets,
                reply_bytes = stats.reply_bytes,
                limited_drops = stats.limited_drops,
                queue_timeouts = stats.queue_timeouts,
                validated_v4 = stats.validated_v4,
                validated_v6 = stats.validated_v6,
                "DHT 流量"
            );
            let v = &stats.verification;
            tracing::info!(
                event = "verification_admission",
                schema_version = 1u64,
                scope,
                admitted = v.admitted,
                duplicate = v.duplicate,
                cooldown = v.cooldown,
                capacity = v.capacity,
                ip_table = v.ip_table,
                succeeded = v.succeeded,
                failed = v.failed,
                "反向验证接纳"
            );
            for (i, class) in ["collector", "control", "sampling", "verification"]
                .iter()
                .enumerate()
            {
                tracing::info!(
                    event = "dht_class",
                    schema_version = 1u64,
                    scope,
                    class,
                    packets = stats.packets[i],
                    bytes = stats.bytes[i],
                    queued = stats.queued[i],
                    dequeued_to_send = stats.queue_finished[i][0],
                    queued_cancelled = stats.queue_finished[i][1],
                    queue_timeouts = stats.queue_finished[i][2],
                    local_rejected = stats.queue_finished[i][3],
                    inflight_cancelled = stats.inflight_cancelled[i],
                    "DHT 类别计数"
                );
                for (reason, count) in [
                    "class_quota",
                    "destination_ip_quota",
                    "upload_bytes",
                    "ip_table_capacity",
                ]
                .iter()
                .zip(stats.blocked[i])
                {
                    tracing::info!(
                        event = "dht_wait_reason",
                        schema_version = 1u64,
                        scope,
                        class,
                        reason,
                        count,
                        "DHT 待发限制"
                    );
                }
                stats.queue_wait[i].log(scope, "dht_queue", class, false);
            }
        }
        for (class, current) in ["collector", "control", "sampling", "verification"]
            .iter()
            .zip(s.queued_current)
        {
            tracing::info!(
                event = "dht_queue_occupancy",
                schema_version = 1u64,
                class,
                current,
                "DHT 待发占用"
            );
        }
    }
}
impl State {
    fn update(&mut self, f: impl Fn(&mut Stats)) {
        f(&mut self.total);
        f(&mut self.interval);
    }
    fn clean(&mut self) {
        let now = tokio::time::Instant::now();
        if now >= self.cleanup {
            self.ips.retain(|_, v| {
                now.duration_since(v.touched) < Duration::from_secs(60)
                    || v.verification_until.is_some_and(|until| until > now)
            });
            self.cleanup = now + Duration::from_secs(1);
        }
    }
    fn track(&mut self, ip: IpAddr) -> bool {
        self.clean();
        if self.ips.len() >= 10000 && !self.ips.contains_key(&ip) {
            return false;
        }
        let now = tokio::time::Instant::now();
        self.ips
            .entry(ip)
            .or_insert_with(|| IpState {
                inbound: Limiter::new(5),
                queries: Limiter::new(2),
                touched: now,
                verification_until: None,
            })
            .touched = now;
        true
    }
    fn ordinary(&mut self, ip: IpAddr) -> bool {
        if !self.track(ip) {
            return false;
        }
        let limits = [&self.inbound, &self.ips[&ip].inbound];
        if limits.iter().any(|l| !l.check(1, false).is_zero()) {
            return false;
        }
        for limiter in limits {
            limiter.check(1, true);
        }
        true
    }
}
#[cfg(test)]
mod tests;
