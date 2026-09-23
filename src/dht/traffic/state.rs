//! Budget、IP 状态、组合配额、验证许可和排队票据。

use super::{Class, Config, Decision, Stats, limiter::Limiter};
use std::{
    collections::HashMap,
    net::IpAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

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
        self.budget.update(|stats| {
            for index in 0..4 {
                if fresh & (1 << index) != 0 {
                    stats.blocked[self.class as usize][index] += 1;
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
        state.update(|stats| {
            stats.queue_finished[self.class as usize][self.outcome] += 1;
            stats.queue_wait[self.class as usize].record(elapsed);
            if self.outcome == 2 {
                stats.queue_timeouts += 1;
            }
        });
    }
}

#[derive(Debug)]
pub(super) struct IpState {
    inbound: Limiter,
    queries: Limiter,
    touched: tokio::time::Instant,
    verification_until: Option<tokio::time::Instant>,
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
pub(super) struct State {
    queries: [Limiter; 4],
    inbound: Limiter,
    responses: Limiter,
    query_bytes: Limiter,
    reply_bytes: Limiter,
    pub(super) ips: HashMap<IpAddr, IpState>,
    cleanup: tokio::time::Instant,
    pub(super) total: Stats,
    pub(super) interval: Stats,
    pub(super) queued_current: [u64; 4],
    pub(super) verification_queued: u64,
    verification_limit: u64,
}

/// 会话共享的配额和统计所有者；一个同步锁保证组合配额检查与扣减不可交错。
#[derive(Debug)]
pub(crate) struct Budget(pub(super) Mutex<State>);

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
        let mut state = self.0.lock().expect("流量锁");
        if !state.track(ip) {
            return Decision {
                wait: Duration::from_secs(1),
                reasons: 8,
            };
        }
        let limits = [
            (&state.queries[class as usize], 1),
            (&state.ips[&ip].queries, 1),
            (&state.query_bytes, bytes as u32),
        ];
        let waits = limits.map(|(limiter, count)| limiter.check(count, false));
        let wait = waits
            .iter()
            .copied()
            .fold(Duration::ZERO, |longest, wait| longest.max(wait));
        let reasons = waits.iter().enumerate().fold(0, |mask, (index, wait)| {
            if wait.is_zero() {
                mask
            } else {
                mask | (1 << index)
            }
        });
        if wait.is_zero() {
            for (limiter, count) in limits {
                limiter.check(count, true);
            }
        }
        Decision { wait, reasons }
    }

    pub(crate) fn verification_duplicate(&self) {
        self.update(|stats| stats.verification.duplicate += 1);
    }

    pub(crate) fn verification_result(&self, success: bool) {
        self.update(|stats| {
            if success {
                stats.verification.succeeded += 1
            } else {
                stats.verification.failed += 1
            }
        });
    }

    /// 检查 IP 表、60 秒冷却和共享待发名额；成功即开始冷却并返回待发许可。
    /// 拒绝不会延长已有冷却；许可释放只归还名额，不撤销已开始的冷却。
    pub(crate) fn admit_verification(self: &Arc<Self>, ip: IpAddr) -> Option<VerificationPermit> {
        let mut state = self.0.lock().expect("流量锁");
        if !state.track(ip) {
            state.update(|stats| stats.verification.ip_table += 1);
            return None;
        }
        let now = tokio::time::Instant::now();
        if state.ips[&ip]
            .verification_until
            .is_some_and(|until| until > now)
        {
            state.update(|stats| stats.verification.cooldown += 1);
            return None;
        }
        if state.verification_queued >= state.verification_limit {
            state.update(|stats| stats.verification.capacity += 1);
            return None;
        }
        state
            .ips
            .get_mut(&ip)
            .expect("track 成功后 IP 项仍受 state 锁保护")
            .verification_until = Some(now + Duration::from_secs(60));
        state.verification_queued += 1;
        state.update(|stats| stats.verification.admitted += 1);
        Some(VerificationPermit(self.clone()))
    }

    pub(crate) fn validated(&self, ip: IpAddr) {
        self.update(|stats| {
            if ip.is_ipv4() {
                stats.validated_v4 += 1;
            } else {
                stats.validated_v6 += 1;
            }
        });
    }

    pub(crate) fn snapshot(&self) -> Stats {
        self.0.lock().expect("流量锁").total.clone()
    }

    pub(crate) fn sent(&self, class: Class, bytes: usize) {
        self.update(|stats| {
            stats.packets[class as usize] += 1;
            stats.bytes[class as usize] += bytes as u64;
        });
    }

    /// 解码前接纳数据报；pending_source 只选择响应预留，不能替代后续身份与来源校验。
    pub(crate) fn inbound(&self, ip: IpAddr, bytes: usize, pending_source: bool) -> bool {
        let mut state = self.0.lock().expect("流量锁");
        let allowed = if pending_source {
            state.responses.check(1, true).is_zero()
        } else {
            state.ordinary(ip)
        };
        state.update(|stats| {
            stats.inbound_packets += 1;
            stats.inbound_bytes += bytes as u64;
            if !allowed {
                stats.limited_drops += 1;
            }
        });
        allowed
    }

    pub(crate) fn disguised_query(&self, ip: IpAddr) -> bool {
        let mut state = self.0.lock().expect("流量锁");
        let allowed = state.ordinary(ip);
        if !allowed {
            state.update(|stats| stats.limited_drops += 1);
        }
        allowed
    }

    pub(crate) fn reply(&self, bytes: usize) -> bool {
        let mut state = self.0.lock().expect("流量锁");
        let allowed = state.reply_bytes.check(bytes as u32, true).is_zero();
        if !allowed {
            state.update(|stats| stats.limited_drops += 1);
        }
        allowed
    }

    pub(crate) fn reply_sent(&self, bytes: usize) {
        self.update(|stats| {
            stats.reply_packets += 1;
            stats.reply_bytes += bytes as u64;
        });
    }

    pub(crate) fn dropped(&self) {
        self.update(|stats| stats.limited_drops += 1);
    }

    pub(crate) fn inflight_cancelled(&self, class: Class) {
        self.update(|stats| stats.inflight_cancelled[class as usize] += 1);
    }

    pub(crate) fn queue_record(self: &Arc<Self>, class: Class) -> QueueRecord {
        let mut state = self.0.lock().expect("流量锁");
        state.queued_current[class as usize] += 1;
        state.update(|stats| stats.queued[class as usize] += 1);
        QueueRecord {
            budget: self.clone(),
            class,
            start: tokio::time::Instant::now(),
            outcome: 1,
            seen: 0,
        }
    }

    pub(super) fn update(&self, operation: impl Fn(&mut Stats)) {
        self.0.lock().expect("流量锁").update(operation);
    }
}

impl State {
    pub(super) fn update(&mut self, operation: impl Fn(&mut Stats)) {
        operation(&mut self.total);
        operation(&mut self.interval);
    }

    pub(super) fn clean(&mut self) {
        let now = tokio::time::Instant::now();
        if now >= self.cleanup {
            self.ips.retain(|_, state| {
                now.duration_since(state.touched) < Duration::from_secs(60)
                    || state.verification_until.is_some_and(|until| until > now)
            });
            self.cleanup = now + Duration::from_secs(1);
        }
    }

    pub(super) fn track(&mut self, ip: IpAddr) -> bool {
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
        if limits
            .iter()
            .any(|limiter| !limiter.check(1, false).is_zero())
        {
            return false;
        }
        for limiter in limits {
            limiter.check(1, true);
        }
        true
    }
}
