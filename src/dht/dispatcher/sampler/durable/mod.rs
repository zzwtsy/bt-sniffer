//! 数据库确认由 select 驱动，绝不在 UDP 收发分支中等待磁盘。
//!
//! Sampler 长期持有预约和结算 future；事件循环取消一次等待不会撤销已接纳的数据库命令。
use super::state::{Cooldown, PauseReason, Request, RequestKind, Sampler, UNSUPPORTED_FOR};
use crate::clock::Clock;
use crate::dht::persistence::CooldownLease;
use crate::dht::persistence::DhtStore;
use crate::dht::persistence::RestoredCooldown;
use crate::dht::persistence::identity::LocalIdentity;
use crate::storage::StorageError;
use futures_util::{StreamExt, future::BoxFuture, stream::FuturesUnordered};
use std::{
    future::pending,
    time::{Duration, Instant},
};
#[cfg(test)]
mod tests;

/// 预约完成后将原请求和租约结果一并交回；等待期间原请求留在 reserving future 内。
pub(super) struct ReservationResult {
    request: Request,
    lease_result: Result<CooldownLease, StorageError>,
}

/// 采样器持有的数据库工作状态；future 留在这里，单次 select 被取消不会丢失进度。
pub(super) struct Durable {
    report: Option<Box<dyn Fn(StorageError) + Send + Sync>>,
    pub(super) clock: Clock,
    storage: DhtStore,
    identity: LocalIdentity,
    /// 至多一个待确认预约，future 同时持有原请求；None 表示当前没有等待预约。
    pub(super) reserving: Option<BoxFuture<'static, ReservationResult>>,
    /// 已确认且仍属于当前启停代数的请求，等待容量与发送间隔，尚未真正发送。
    pub(super) ready: Option<Request>,
    /// 待驱动或待确认的结算/撤销 future；放入集合不代表已入数据库队列，退出时须等待结果。
    pub(super) settling: FuturesUnordered<BoxFuture<'static, Result<(), StorageError>>>,
}
impl std::fmt::Debug for Durable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DurableSampler")
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}
impl Sampler {
    /// 仅在未运行且未挂接时安装持久化状态，将剩余 UTC 毫秒恢复为当前单调期限。
    pub(in crate::dht::dispatcher) fn attach_storage(
        &mut self,
        storage: DhtStore,
        identity: LocalIdentity,
        restored: Vec<RestoredCooldown>,
    ) -> Result<(), StorageError> {
        if self.session.is_some() || self.durable.is_some() {
            return Err(StorageError::Invalid("不能替换正在使用的持久化状态"));
        }
        let now = tokio::time::Instant::now().into_std();
        for value in restored {
            let remaining = match value {
                RestoredCooldown::Id { remaining_ms, .. }
                | RestoredCooldown::Ip { remaining_ms, .. } => remaining_ms,
            };
            let duration = Duration::from_millis(
                u64::try_from(remaining).map_err(|_| StorageError::Invalid("冷却时间无效"))?,
            );
            let until = now
                .checked_add(duration)
                .ok_or(StorageError::Invalid("冷却期限溢出"))?;
            match value {
                RestoredCooldown::Id {
                    node_id, failures, ..
                } => {
                    self.ids.insert(node_id, Cooldown { until, failures });
                }
                RestoredCooldown::Ip { ip, .. } => {
                    self.ips.insert(ip, until);
                }
            }
        }
        self.durable = Some(Durable {
            report: None,
            clock: Clock::default(),
            storage,
            identity,
            reserving: None,
            ready: None,
            settling: FuturesUnordered::new(),
        });
        Ok(())
    }
    /// 先同步报告原始存储错误，再停止采样并保留错误状态；不会直接关闭数据库线程。
    pub(in crate::dht::dispatcher) fn mark_storage_fault(&mut self, error: StorageError) {
        if let Some(report) = self.durable.as_ref().and_then(|d| d.report.as_ref()) {
            // 先同步报告模块错误，再按原顺序停止采样并更新本地状态。
            report(error.clone());
        }
        self.stop(tokio::time::Instant::now().into_std());
        self.status.storage_error = Some(error);
        self.status.pause = PauseReason::Storage;
    }
    pub(in crate::dht::dispatcher) fn report_errors_to(
        &mut self,
        report: Box<dyn Fn(StorageError) + Send + Sync>,
    ) {
        if let Some(d) = &mut self.durable {
            d.report = Some(report);
        }
    }
    /// 无持久化、已有租约或联系人回退请求可直接返回；否则保存预约 future 并返回 None。
    /// None 也可能来自已停止或时钟错误，需结合状态判断，不表示请求已发出或预约被撤销。
    pub(super) fn reserve_request(&mut self, request: Request, now: Instant) -> Option<Request> {
        let Some(durable) = self.durable.as_mut() else {
            return Some(request);
        };
        if request.lease.is_some() || request.kind == RequestKind::FindNodeFallback {
            return Some(request);
        }
        let s = self.session.as_ref()?;
        let duration = s
            .config
            .minimum_interval
            .max(UNSUPPORTED_FOR)
            .as_nanos()
            .div_ceil(1_000_000);
        let at = match durable.clock.millis_at(now) {
            Ok(at) => at,
            Err(error) => {
                self.mark_storage_fault(error.into());
                return None;
            }
        };
        let Ok(duration) = i64::try_from(duration) else {
            self.mark_storage_fault(StorageError::Invalid("冷却时长溢出"));
            return None;
        };
        let mut store = durable.storage.clone();
        store.observer = request.observer.clone();
        let identity = durable.identity;
        let capacity = s.config.cooldown_capacity;
        request.observer.emit(
            crate::observation::Kind::Sampling,
            "cooldown_reservation",
            "started",
            || serde_json::json!({"duration_ms":duration}),
        );
        durable.reserving = Some(Box::pin(async move {
            let result = store
                .reserve_sampling(
                    identity,
                    request.node.id,
                    request.node.address.ip(),
                    at,
                    duration,
                    capacity,
                )
                .await;
            ReservationResult {
                request,
                lease_result: result,
            }
        }));
        self.status.pause = PauseReason::Storage;
        self.deadline = None;
        None
    }
    /// 容量、发送间隔与结算状态均允许时移出 ready；None 表示当前不能交付请求。
    /// 取出只推进采样发送节奏，实际入队、配额和 UDP 发送仍由 dispatcher 处理。
    pub(super) fn take_reserved(&mut self, capacity: usize, now: Instant) -> Option<Request> {
        let d = self.durable.as_mut()?;
        let s = self.session.as_mut()?;
        if d.ready.is_none() || !d.settling.is_empty() || capacity == 0 {
            return None;
        }
        if now < s.next_send {
            self.deadline = Some(s.next_send);
            return None;
        }
        s.next_send = now + s.config.send_spacing;
        d.ready.take()
    }
    /// 结算 future 数量不超过采样并发数，结算未完成时不再开始新采样。
    pub(super) fn settle_request(&mut self, request: &Request, now: Instant) {
        let Some(lease) = request.lease.clone() else {
            return;
        };
        let Some(cooldown) = self.ids.get(&request.node.id).copied() else {
            return;
        };
        let Some(durable) = self.durable.as_mut() else {
            return;
        };
        let at = match durable.clock.millis_at(now) {
            Ok(at) => at,
            Err(error) => {
                self.mark_storage_fault(error.into());
                return;
            }
        };
        let duration = cooldown
            .until
            .saturating_duration_since(now)
            .as_nanos()
            .div_ceil(1_000_000)
            .max(1);
        let Ok(duration) = i64::try_from(duration) else {
            self.mark_storage_fault(StorageError::Invalid("冷却结算溢出"));
            return;
        };
        let mut store = durable.storage.clone();
        store.observer = request.observer.clone();
        durable.settling.push(Box::pin(async move {
            store
                .settle_sampling(lease, at, duration, cooldown.failures)
                .await
        }));
    }
    /// socket 返回不确定的发送结果，保守保留冷却，但不计远端失败。
    pub(in crate::dht::dispatcher) fn uncertain_send(&mut self, request: Request, now: Instant) {
        self.finished(&request);
        self.cancel_request(&request, now);
    }
    /// 仅用于确认尚未发送的请求：释放内存跟踪并按租约撤销磁盘预约。
    /// 数据库撤销 future 留在 settling，返回不表示撤销事务已完成；发送不确定不能走此路径。
    pub(in crate::dht::dispatcher) fn abandon_unsent(&mut self, request: Request, now: Instant) {
        self.finished(&request);
        if request.kind == RequestKind::Sample {
            self.ids.remove(&request.node.id);
            self.ips.remove(&request.node.address.ip());
            if let Some(lease) = request.lease
                && let Some(d) = &mut self.durable
            {
                let mut store = d.storage.clone();
                store.observer = request.observer.clone();
                d.settling
                    .push(Box::pin(async move { store.abandon_sampling(lease).await }));
            }
        }
        // 下一次发送至少等待 1 秒，防止本地容量不足导致热循环。
        if let Some(s) = &mut self.session {
            s.next_send = s.next_send.max(now + Duration::from_secs(1));
            self.deadline = Some(s.next_send);
        }
    }
    /// 对发送结果未知或已进入发送阶段的请求保守延长冷却，避免取消后立刻重复采样。
    /// 与 abandon_unsent 相反，此路径保留并结算租约；本地取消不累计远端失败。
    pub(in crate::dht::dispatcher) fn cancel_request(&mut self, request: &Request, now: Instant) {
        let Some(lease) = request.lease.clone() else {
            return;
        };
        // 预约确认可能晚于 stop；内存与磁盘都从实际结算时刻等待，不能让内存先到期。
        let duration = Duration::from_millis(lease.duration_ms.max(21_600_000) as u64);
        let Some(until) = now.checked_add(duration) else {
            self.mark_storage_fault(StorageError::Invalid("取消冷却时间溢出"));
            return;
        };
        self.cooldown(request.node, until, 0);
        let Some(d) = &mut self.durable else {
            return;
        };
        let store = d.storage.clone();
        let at = d.clock.millis_at(now);
        d.settling.push(Box::pin(async move {
            let duration = lease.duration_ms.max(21_600_000);
            store.settle_sampling(lease, at?, duration, 0).await
        }));
    }
    /// 在停止调度后排空仍持有的预约与结算 future，返回保留的存储错误。
    /// 不负责业务批次消费或数据库关闭；取消等待后不能声称排空完成。
    pub(in crate::dht::dispatcher) async fn flush_storage(&mut self) -> Result<(), StorageError> {
        while self
            .durable
            .as_ref()
            .is_some_and(|d| d.reserving.is_some() || !d.settling.is_empty())
        {
            self.storage_event().await;
        }
        self.status.storage_error.clone().map_or(Ok(()), Err)
    }
    /// 取消 select 不会丢失正在排队的 SQL future，它一直保存在 durable 状态中。
    pub(in crate::dht::dispatcher) async fn storage_event(&mut self) {
        let Some(d) = self.durable.as_mut() else {
            return pending().await;
        };
        enum Event {
            Reserved(Box<Request>, Result<CooldownLease, StorageError>),
            Settled(Result<(), StorageError>),
        }
        let event = tokio::select! {
            reservation = async {
                match d.reserving.as_mut() {
                    Some(future) => future.await,
                    None => pending().await,
                }
            } => Event::Reserved(Box::new(reservation.request), reservation.lease_result),
            result = async {
                if d.settling.is_empty() {
                    pending().await
                } else {
                    d.settling.next().await.expect("仍有待结算的操作")
                }
            } => Event::Settled(result),
        };
        match event {
            Event::Reserved(mut request, result) => {
                d.reserving = None;
                match result {
                    Ok(lease) => {
                        request.lease = Some(lease);
                        if self.session.is_some() && request.generation == self.generation {
                            d.ready = Some(*request);
                        } else {
                            self.abandon_unsent(*request, tokio::time::Instant::now().into_std());
                        }
                    }
                    Err(error) => self.mark_storage_fault(error),
                }
            }
            Event::Settled(Err(error)) => self.mark_storage_fault(error),
            Event::Settled(Ok(())) => {}
        }
    }
}
