//! 通过真实 worker 验证候选切换、去重、上限及成功来源归属。
use super::peer::{
    PeerClient, VerifiedMetadata,
    compatibility_tests::{INFO, Reply, fetcher, peer},
    tests::{PeerBehavior, bind, config, hash, metadata, spawn_peer},
};
use super::{
    jobs::{ClaimClass, Job, RetryReason},
    worker::{Outcome, WorkerResources, run_job},
};
use crate::{address::AddressPolicy, dht::routing::AddressFamily, info_hash::InfoHashV1};
use sha1::{Digest, Sha1};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
async fn run_candidates(
    client: PeerClient,
    hash: InfoHashV1,
    peers: Vec<SocketAddr>,
) -> Result<VerifiedMetadata, RetryReason> {
    let network = Arc::new(WorkerResources::new(client.clone(), client.test_metrics()));
    let job = Job {
        hash,
        generation: 1,
        failed_attempts_before: 0,
        class: ClaimClass::Hint,
        had_valid_hint: true,
        peers,
    };
    match run_job(
        job,
        vec![],
        network,
        AddressPolicy::LocalUnicast,
        vec![AddressFamily::Ipv4],
        CancellationToken::new(),
    )
    .await
    {
        Outcome::Success(metadata) => Ok(metadata),
        Outcome::Retry(reason) => Err(reason),
        Outcome::Control(error) => panic!("测试控制故障：{error}"),
    }
}
/// 第一个 peer 拒绝后切换第二个；地址重复不应重复尝试，缓冲区不能沿用旧连接。
#[tokio::test]
async fn reject_then_success_uses_distinct_peers() {
    let a = bind(AddressFamily::Ipv4).await.unwrap();
    let b = bind(AddressFamily::Ipv4).await.unwrap();
    let aa = a.local_addr().unwrap();
    let ba = b.local_addr().unwrap();
    let bytes = metadata(30);
    let target = hash(&bytes);
    let first = spawn_peer(a, target, bytes.clone(), PeerBehavior::Reject);
    let second = spawn_peer(b, target, bytes.clone(), PeerBehavior::Serve);
    let result = run_candidates(PeerClient::new(config()).unwrap(), target, vec![aa, aa, ba])
        .await
        .unwrap();
    assert_eq!(result.source(), ba);
    assert_eq!(result.info(), bytes);
    first.await.unwrap();
    second.await.unwrap();
}

/// 只尝试配置允许的不同地址数，不能因为首 peer 拒绝就无限向列表后面扩散。
#[tokio::test]
async fn attempt_limit_stops_before_next_peer() {
    let target = hash(b"de");
    let mut addresses = Vec::new();
    let mut servers = Vec::new();
    for _ in 0..8 {
        let listener = bind(AddressFamily::Ipv4).await.unwrap();
        addresses.push(listener.local_addr().unwrap());
        addresses.push(listener.local_addr().unwrap());
        servers.push(spawn_peer(
            listener,
            target,
            b"de".to_vec(),
            PeerBehavior::Reject,
        ));
    }
    let next = bind(AddressFamily::Ipv4).await.unwrap();
    addresses.push(next.local_addr().unwrap());
    assert!(
        run_candidates(PeerClient::new(config()).unwrap(), target, addresses)
            .await
            .is_err()
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(20), next.accept())
            .await
            .is_err()
    );
    for server in servers {
        server.await.unwrap();
    }
}

#[tokio::test]
async fn compatible_failed_peer_does_not_claim_strict_peers_download() {
    let metrics = Arc::new(crate::collection::diagnostics::metrics::Metrics::default());
    let (failed, failure_task, _) = peer(true, Reply::Reject).await;
    let (success, success_task, _) = peer(false, Reply::Serve).await;
    let result = run_candidates(
        fetcher(metrics.clone()),
        InfoHashV1(Sha1::digest(INFO).into()),
        vec![failed, success],
    )
    .await
    .unwrap();
    failure_task.await.unwrap();
    success_task.await.unwrap();
    assert!(!result.used_extension_compatibility());
    let report = metrics.diagnostics.report();
    assert_eq!(report["extension_compatibility"]["sessions"], 1);
    assert_eq!(report["extension_compatibility"]["downloaded"], 0);
}

/// 同 IP 许可由两个真实 worker 共享；取消等待者不会建立 TCP 连接。
#[tokio::test]
async fn workers_share_ip_permit_and_cancel_waiters_without_connecting() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = bind(AddressFamily::Ipv4).await.unwrap();
    let address = listener.local_addr().unwrap();
    let client = PeerClient::new(config()).unwrap();
    let metrics = client.test_metrics();
    let network = Arc::new(WorkerResources::new(client.clone(), metrics.clone()));
    let job = Job {
        hash: hash(b"de"),
        generation: 1,
        failed_attempts_before: 0,
        class: ClaimClass::Hint,
        had_valid_hint: true,
        peers: vec![address],
    };
    let cancel = CancellationToken::new();
    let first = tokio::spawn(run_job(
        job.clone(),
        vec![],
        network.clone(),
        AddressPolicy::LocalUnicast,
        vec![AddressFamily::Ipv4],
        cancel.clone(),
    ));
    let (mut socket, _) = tokio::time::timeout(Duration::from_secs(2), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let mut handshake = [0; 68];
    socket.read_exact(&mut handshake).await.unwrap();
    let waiting_cancel = CancellationToken::new();
    let second = run_job(
        job,
        vec![],
        network.clone(),
        AddressPolicy::LocalUnicast,
        vec![AddressFamily::Ipv4],
        waiting_cancel.clone(),
    );
    tokio::pin!(second);
    assert!(futures_util::poll!(&mut second).is_pending());
    assert_eq!(metrics.report()["peer_attempts"], 1);
    waiting_cancel.cancel();
    assert!(matches!(
        second.await,
        Outcome::Retry(RetryReason::Local(super::jobs::LocalReason::Cancelled))
    ));
    cancel.cancel();
    assert!(matches!(
        first.await.unwrap(),
        Outcome::Retry(RetryReason::Local(super::jobs::LocalReason::Cancelled))
    ));
    assert_eq!(network.tcp.tracked_tcp_ips(), 0);
    let mut byte = [0];
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), socket.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    socket.shutdown().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
}
