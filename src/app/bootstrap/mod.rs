//! 引导只负责找到一个已验证邻居，后续迭代查找交给 routing maintenance。
//!
//! app 持有引导任务；这里通过 DhtHandle 发命令，不直接拥有 UDP socket 或路由表。
use crate::{
    dht::dispatcher::{DhtHandle, QueryError, RemoteNode},
    net::address::AddressPolicy,
};
use futures_util::{StreamExt, future::BoxFuture, stream::FuturesUnordered};
use rand::RngExt;
use std::{collections::HashSet, io, net::SocketAddr, time::Duration};
use tokio::time::{Instant, sleep, sleep_until, timeout};

fn resolve(host: String) -> BoxFuture<'static, io::Result<Vec<SocketAddr>>> {
    Box::pin(async move { Ok(tokio::net::lookup_host(host).await?.collect()) })
}
/// 任务由 app 的 JoinSet 持有；发现邻居后继续观察，邻居耗尽时重新引导。
pub(super) async fn run(
    handle: DhtHandle,
    seeds: Vec<String>,
    policy: AddressPolicy,
) -> Result<(), QueryError> {
    run_with_resolver(handle, seeds, policy, resolve).await
}
async fn run_with_resolver(
    handle: DhtHandle,
    seeds: Vec<String>,
    policy: AddressPolicy,
    resolve: impl Fn(String) -> BoxFuture<'static, io::Result<Vec<SocketAddr>>>,
) -> Result<(), QueryError> {
    let initial = handle.status().await?;
    if initial.recovery_queued + initial.recovery_active > 0 {
        sleep(Duration::from_secs(10)).await;
    }
    let mut backoff = Duration::from_secs(60);
    loop {
        let status = handle.status().await?;
        if status.good > 0 {
            backoff = Duration::from_secs(60);
            sleep(Duration::from_secs(60)).await;
            continue;
        }
        let addresses = resolve_addresses(&seeds, status.family, policy, &resolve).await;
        match round(&handle, addresses).await? {
            Round::Connected => {
                tracing::info!(family = ?status.family, "已验证引导邻居，交由路由维护继续发现节点");
                backoff = Duration::from_secs(60);
                sleep(Duration::from_secs(60)).await;
            }
            Round::Busy => sleep(Duration::from_secs(1)).await,
            Round::Failed => {
                let delay = retry_delay(backoff, rand::rng().random_range(0..=200));
                tracing::warn!(family = ?status.family, seconds = delay.as_secs(), "当前没有可用引导邻居，稍后重试");
                sleep(delay).await;
                backoff = backoff.saturating_mul(2).min(Duration::from_secs(900));
            }
        }
    }
}
fn retry_delay(base: Duration, jitter: u32) -> Duration {
    (base + base.mul_f64(f64::from(jitter) / 1000.0)).min(Duration::from_secs(900))
}

/// DNS 结果先过滤地址族和策略，再去重；等待超时不表示系统 DNS 已停止。
async fn resolve_addresses(
    seeds: &[String],
    family: crate::dht::routing::AddressFamily,
    policy: AddressPolicy,
    resolve: &impl Fn(String) -> BoxFuture<'static, io::Result<Vec<SocketAddr>>>,
) -> Vec<SocketAddr> {
    let mut seen = HashSet::new();
    let mut addresses = Vec::new();
    for seed in seeds {
        match timeout(Duration::from_secs(5), resolve(seed.clone())).await {
            Ok(Ok(resolved)) => {
                for address in resolved {
                    if family.accepts(address) && policy.accepts(address) && seen.insert(address) {
                        addresses.push(address);
                        if addresses.len() == 8 {
                            break;
                        }
                    }
                }
            }
            Ok(Err(error)) => tracing::warn!(%seed, "引导 DNS 失败：{error}"),
            Err(_) => tracing::warn!(%seed, "引导 DNS 等待超过 5 秒"),
        }
        if addresses.len() == 8 {
            break;
        }
    }
    addresses
}
#[derive(Debug, PartialEq, Eq)]
enum Round {
    Connected,
    Busy,
    Failed,
}
/// 同时最多两条查询，发送间隔一秒；首个成功之后只排空已发出的查询。
async fn round(handle: &DhtHandle, addresses: Vec<SocketAddr>) -> Result<Round, QueryError> {
    let mut addresses = addresses.into_iter();
    let mut next_address = addresses.next();
    let mut pending = FuturesUnordered::new();
    let mut next_send = Instant::now();
    let mut connected = false;
    let mut busy = false;
    loop {
        if pending.is_empty() && (next_address.is_none() || connected) {
            break;
        }
        tokio::select! {
            result = pending.next(), if !pending.is_empty() => {
                // 分支只在集合非空时启用，next 返回的一定是某个查询的结果。
                match result.expect("仍有在途引导查询") {
                    Ok(_) => connected = true,
                    Err(QueryError::AtCapacity {..}) => busy = true,
                    Err(error @ (QueryError::DispatcherClosed | QueryError::ShuttingDown)) => return Err(error),
                    Err(error) => tracing::debug!("引导查询失败：{error}"),
                }
            }
            _ = sleep_until(next_send), if !connected && next_address.is_some() && pending.len() < 2 => {
                let address = next_address.take().unwrap();
                pending.push(handle.bootstrap_ping(RemoteNode {
                    address,
                    expected_id: None,
                }));
                next_address = addresses.next();
                next_send = Instant::now() + Duration::from_secs(1);
            }
        }
    }
    Ok(if connected {
        Round::Connected
    } else if busy {
        Round::Busy
    } else {
        Round::Failed
    })
}

#[cfg(test)]
mod tests;
