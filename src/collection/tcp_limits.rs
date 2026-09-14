//! 同 IP TCP 许可；表同时覆盖持有者与等待者，取消和最后一次释放同步回收登记。
use std::{
    collections::HashMap,
    net::IpAddr,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
/// 采集入口创建一次；整个 peer 尝试持有许可，不与 DHT 查询节奏混用。
#[derive(Default)]
pub(super) struct TcpLimits {
    tcp: Arc<TcpState>,
}
#[derive(Default)]
struct TcpState {
    /// 持有者和等待者共用每 IP 的信号量；弱引用不延长条目所指信号量的生命周期。
    ips: Mutex<HashMap<IpAddr, Weak<Semaphore>>>,
}
/// 一次许可申请在 IP 表中的登记；等待期间由申请 future 持有，成功后移入许可对象。
struct TcpTicket {
    state: Arc<TcpState>,
    ip: IpAddr,
    semaphore: Arc<Semaphore>,
}
impl Drop for TcpTicket {
    fn drop(&mut self) {
        let mut ips = self.state.ips.lock().expect("TCP 地址锁");
        // 检查和删除共用申请时的锁，避免删除期间另一个申请取得同一信号量。
        // 仅剩本登记的强引用时，已无其他持有者或等待者，可以移除弱引用条目。
        if Arc::strong_count(&self.semaphore) == 1 {
            ips.remove(&self.ip);
        }
    }
}
/// 同 IP 的单个 TCP 并发许可，不持有 socket。调用者在整个 peer 尝试期间保存它。
/// 离开作用域或持有它的 future 被丢弃时，字段自动析构，归还许可并清理登记。
pub(super) struct ConnectionPermit {
    // Rust 按字段声明顺序释放：先归还许可、释放它持有的信号量强引用，再检查登记。
    // 若交换顺序，最后一个 ticket 仍会看到许可的强引用，无法删除最后的 IP 表条目。
    _permit: OwnedSemaphorePermit,
    _ticket: TcpTicket,
}
impl TcpLimits {
    /// 申请同 IP 的单个并发许可；不同 IP 使用各自的信号量，不相互排队。
    /// worker 的 attempt 取得许可后才调用 PeerClient，后者在 fetch_peer 中连接 TCP。
    /// 等待期间也登记在 IP 表中；丢弃申请 future 会退出排队并释放登记，失去原排队位置。
    /// 此方法不监听取消 token；外层须丢弃 future 才取消等待。成功后由返回对象归还许可。
    pub(super) async fn acquire_for_ip(&self, ip: IpAddr) -> ConnectionPermit {
        let ticket = {
            let mut ips = self.tcp.ips.lock().expect("TCP 地址锁");
            let semaphore = ips.get(&ip).and_then(Weak::upgrade).unwrap_or_else(|| {
                let semaphore = Arc::new(Semaphore::new(1));
                ips.insert(ip, Arc::downgrade(&semaphore));
                semaphore
            });
            TcpTicket {
                state: self.tcp.clone(),
                ip,
                semaphore,
            }
        };
        // IP 表的同步锁已释放，等待只持有 ticket 和信号量，不跨 await 持表锁。
        let permit = ticket
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("TCP semaphore 不关闭");
        ConnectionPermit {
            _permit: permit,
            _ticket: ticket,
        }
    }

    /// 包含许可持有者或等待者的 IP 条目数，不是 socket 数；保留现有日志统计口径。
    pub(super) fn tracked_tcp_ips(&self) -> usize {
        self.tcp.ips.lock().expect("TCP 地址锁").len()
    }
}

#[cfg(test)]
mod fairness_tests {
    use super::*;
    #[tokio::test(start_paused = true)]
    async fn fifo_waiters_cancel_and_release_without_leaking_ip_entries() {
        let network = TcpLimits::default();
        let ip = "127.0.0.1".parse().unwrap();
        let first = network.acquire_for_ip(ip).await;
        let mut second = Box::pin(network.acquire_for_ip(ip));
        let mut cancelled = Box::pin(network.acquire_for_ip(ip));
        let mut last = Box::pin(network.acquire_for_ip(ip));
        assert!(futures_util::poll!(&mut second).is_pending());
        assert!(futures_util::poll!(&mut cancelled).is_pending());
        assert!(futures_util::poll!(&mut last).is_pending());
        drop(cancelled);
        drop(first);
        assert!(futures_util::poll!(&mut last).is_pending());
        let second = second.await;
        assert!(futures_util::poll!(&mut last).is_pending());
        drop(second);
        drop(last.await);
        assert_eq!(network.tracked_tcp_ips(), 0);
    }
}
