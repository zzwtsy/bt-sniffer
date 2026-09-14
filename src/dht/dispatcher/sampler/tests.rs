//! 状态机测试不访问公网；用固定的路由节点、显式时间验证预算与冷却。
use super::*;
use crate::dht::krpc::CompactNodesV4;
use crate::dht::krpc::InfoHashSamples;
use crate::dht::peer_store::PeerAddressPolicy;
use crate::dht::routing::AddressFamily;

fn config() -> SamplerConfig {
    SamplerConfig {
        address_policy: PeerAddressPolicy::LocalUnicast,
        ..Default::default()
    }
}
fn node(n: u8) -> DiscoveredNode {
    DiscoveredNode {
        id: NodeId([n; 20]),
        address: ([127, 0, 0, n], 6881).into(),
    }
}
fn routing(now: Instant, count: u8) -> RoutingTable {
    let mut routing = RoutingTable::new(NodeId([0; 20]), AddressFamily::Ipv4, now);
    for n in 1..=count {
        routing.observe_response(node(n).id, node(n).address, now);
    }
    routing
}
fn response(interval: u64) -> SampleResponse {
    SampleResponse {
        nodes: vec![],
        interval: Duration::from_secs(interval),
        num: 1,
        samples: vec![InfoHashV1([9; 20])],
    }
}
fn begin(now: Instant, config: SamplerConfig) -> (Sampler, mpsc::Receiver<SampleBatch>) {
    let mut sampler = Sampler::default();
    let receiver = sampler.start(config, 8, now).unwrap();
    sampler.session.as_mut().unwrap().target = NodeId([0; 20]);
    (sampler, receiver)
}

/// 三条请求也必须按一秒间隔发出；一次不能占满全部额度造成突发流量。
#[test]
fn spacing_parallelism_and_xor_order_are_enforced() {
    let now = Instant::now();
    let table = routing(now, 4);
    let (mut sampler, _receiver) = begin(now, config());
    let a = sampler.next(&table, 7, now).unwrap();
    assert_eq!(a.node.id, node(1).id);
    assert!(sampler.next(&table, 7, now).is_none());
    let b = sampler
        .next(&table, 7, now + Duration::from_secs(1))
        .unwrap();
    assert_eq!(b.node.id, node(2).id);
    let c = sampler
        .next(&table, 7, now + Duration::from_secs(2))
        .unwrap();
    assert_eq!(c.node.id, node(3).id);
    assert!(
        sampler
            .next(&table, 7, now + Duration::from_secs(3))
            .is_none()
    );
    assert_eq!(sampler.status().pause, PauseReason::Transactions);
    drop((a, b, c));
}

/// 冷却从收到合法响应时开始；改变端口或 Node ID 不能绕过 IP 冷却。
#[tokio::test(start_paused = true)]
async fn interval_is_respected_across_restart_and_aliases() {
    let now = tokio::time::Instant::now().into_std();
    let table = routing(now, 1);
    let (mut sampler, mut receiver) = begin(now, config());
    let request = sampler.next(&table, 7, now).unwrap();
    sampler.success(request, response(300), &table, now);
    let batch = receiver.recv().await.unwrap();
    assert_eq!(batch.interval, Duration::from_secs(300));
    sampler.stop(now);
    let _receiver = sampler.start(config(), 8, now).unwrap();
    let mut alias = node(2);
    alias.address = ([127, 0, 0, 1], 9000).into();
    sampler.add_nodes(vec![alias], &table, now);
    tokio::time::advance(Duration::from_secs(299)).await;
    assert!(
        sampler
            .next(&table, 7, tokio::time::Instant::now().into_std())
            .is_none()
    );
    assert_eq!(sampler.deadline, Some(now + Duration::from_secs(300)));
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(
        sampler
            .next(&table, 7, tokio::time::Instant::now().into_std())
            .is_some()
    );
}

/// interval=0 仍受本地最小间隔约束；失败次数逐步增加退避，不无限增长。
#[test]
fn zero_interval_and_failure_backoff_have_local_bounds() {
    let now = Instant::now();
    let table = routing(now, 1);
    let (mut sampler, _receiver) = begin(now, config());
    let request = sampler.next(&table, 7, now).unwrap();
    sampler.success(request, response(0), &table, now);
    assert_eq!(
        sampler.ids[&node(1).id].until,
        now + Duration::from_secs(60)
    );
    for (attempt, seconds) in [60, 120, 240, 480, 900, 900].into_iter().enumerate() {
        let later = now + Duration::from_secs(1000 * (attempt as u64 + 1));
        let s = sampler.session.as_mut().unwrap();
        s.candidates.get_mut(&node(1).id).unwrap().visited = false;
        let request = sampler.next(&table, 7, later).unwrap();
        sampler.failure(request, &QueryError::Timeout, later);
        assert_eq!(
            sampler.ids[&node(1).id].until,
            later + Duration::from_secs(seconds)
        );
    }
}

/// 204 只允许一次回退，并且回退共用发包间隔；失败后不能重复 find_node。
#[test]
fn unsupported_node_has_one_budgeted_fallback() {
    let now = Instant::now();
    let table = routing(now, 1);
    let (mut sampler, _receiver) = begin(now, config());
    let request = sampler.next(&table, 7, now).unwrap();
    sampler.failure(
        request,
        &QueryError::Remote {
            code: 204,
            message: vec![].into(),
        },
        now,
    );
    assert!(sampler.next(&table, 7, now).is_none());
    let request = sampler
        .next(&table, 7, now + Duration::from_secs(1))
        .unwrap();
    assert_eq!(request.kind, RequestKind::FindNodeFallback);
    assert_eq!(sampler.session.as_ref().unwrap().queries, 2);
    sampler.failure(request, &QueryError::Timeout, now + Duration::from_secs(1));
    assert_eq!(sampler.ids[&node(1).id].until, now + UNSUPPORTED_FOR);
    for second in [2, 3, 100] {
        assert!(
            sampler
                .next(&table, 7, now + Duration::from_secs(second))
                .is_none()
        );
    }
    assert_eq!(sampler.status().unsupported, 1);
}

/// 预留结果槽位使队列满时停止新流量；读出旧结果后可恢复，不丢成功批次。
#[tokio::test]
async fn output_backpressure_reserves_space_before_query() {
    let now = Instant::now();
    let table = routing(now, 2);
    let (mut sampler, mut receiver) = begin(
        now,
        SamplerConfig {
            output_capacity: 1,
            ..config()
        },
    );
    let request = sampler.next(&table, 7, now).unwrap();
    assert!(
        sampler
            .next(&table, 7, now + Duration::from_secs(1))
            .is_none()
    );
    assert_eq!(sampler.status().pause, PauseReason::Output);
    sampler.success(request, response(300), &table, now);
    assert!(
        sampler
            .next(&table, 7, now + Duration::from_secs(1))
            .is_none()
    );
    receiver.recv().await.unwrap();
    let output = sampler.output_watch();
    assert!(matches!(output, OutputWatch::Capacity(_)));
    sampler.accept_permit(watch_output(output).await.unwrap());
    assert!(
        sampler
            .next(&table, 7, now + Duration::from_secs(1))
            .is_some()
    );
}

/// 候选淘汰不删除冷却记录；跟踪容量满时暂停接纳新地址，不能为继续扫描而提前忘记。
#[test]
fn bounded_tracking_and_inflight_stop_preserve_cooldowns() {
    let now = Instant::now();
    let table = routing(now, 2);
    let (mut sampler, _receiver) = begin(
        now,
        SamplerConfig {
            cooldown_capacity: 1,
            ..config()
        },
    );
    let request = sampler.next(&table, 7, now).unwrap();
    sampler.success(request, response(300), &table, now);
    sampler.next(&table, 7, now + Duration::from_secs(1));
    assert!(
        sampler
            .next(&table, 7, now + Duration::from_secs(2))
            .is_none()
    );
    assert_eq!(sampler.status().pause, PauseReason::TrackingCapacity);
    assert_eq!(sampler.ids.len(), 1);
    let request = sampler
        .next(&table, 7, now + Duration::from_secs(300))
        .unwrap();
    sampler.stop(now + Duration::from_secs(300));
    assert!(
        sampler.ids[&request.node.id].until >= now + Duration::from_secs(300) + UNSUPPORTED_FOR
    );
    drop(request);
}

/// 发现联系人不代表已经验证；同 ID 使用已验证地址，不访问另一地址族或非单播。
#[test]
fn candidate_limits_and_trusted_addresses_are_enforced() {
    let now = Instant::now();
    let table = routing(now, 1);
    let (mut sampler, _receiver) = begin(
        now,
        SamplerConfig {
            candidate_capacity: 8,
            ..config()
        },
    );
    let mut nodes: Vec<_> = (1..30).map(node).collect();
    nodes[0].address = ([127, 0, 0, 99], 1).into();
    nodes.push(DiscoveredNode {
        id: NodeId([90; 20]),
        address: "[::1]:1234".parse().unwrap(),
    });
    nodes.push(DiscoveredNode {
        id: NodeId([91; 20]),
        address: "224.0.0.1:1234".parse().unwrap(),
    });
    sampler.add_nodes(nodes, &table, now);
    let s = sampler.session.as_ref().unwrap();
    assert_eq!(s.candidates.len(), 8);
    assert_eq!(s.candidates[&node(1).id].node.address, node(1).address);
    assert!(!s.candidates.contains_key(&NodeId([90; 20])));
    assert!(table.contact(node(2).id).is_none());
    let (mut public, _receiver) = begin(now, SamplerConfig::default());
    assert!(public.next(&table, 7, now).is_none());
    assert_eq!(public.status().pause, PauseReason::NoSeeds);
}

/// 只有 samples 字段存在才算支持；空值合法，错误期限/计数及超大报文必须拒绝。
#[test]
fn sample_response_validation_and_deduplication() {
    let mut r = super::super::query::empty_response(NodeId([1; 20]));
    r.nodes = Some(CompactNodesV4(vec![]));
    assert!(decode_sample(&r, vec![], 100).unwrap().is_none());
    r.samples = Some(InfoHashSamples(vec![]));
    assert!(decode_sample(&r, vec![], 100).is_err());
    r.interval = Some(0);
    r.num = Some(0);
    assert!(
        decode_sample(&r, vec![], 100)
            .unwrap()
            .unwrap()
            .samples
            .is_empty()
    );
    r.samples = Some(InfoHashSamples(vec![InfoHashV1([9; 20]); 2]));
    r.num = Some(1);
    assert_eq!(
        decode_sample(&r, vec![], 100)
            .unwrap()
            .unwrap()
            .samples
            .len(),
        1
    );
    r.num = Some(0);
    assert!(decode_sample(&r, vec![], 100).is_err());
    r.num = Some(1);
    r.interval = Some(21601);
    assert!(decode_sample(&r, vec![], 100).is_err());
    r.interval = Some(21600);
    assert!(decode_sample(&r, vec![], 1025).is_err());
    assert!(decode_sample(&r, vec![], 1024).is_ok());
}

/// 没有种子或 transaction 名额时不安排立即到期的计时器，避免事件循环空转。
#[test]
fn idle_and_configuration_validation() {
    let now = Instant::now();
    let table = routing(now, 0);
    let (mut sampler, _receiver) = begin(now, config());
    assert!(sampler.next(&table, 7, now).is_none());
    assert!(sampler.deadline.is_none());
    assert_eq!(
        sampler.start(config(), 8, now).unwrap_err(),
        SamplerError::AlreadyRunning
    );
    sampler.stop(now);
    assert!(sampler.start(config(), 1, now).is_err());
    for config in [
        SamplerConfig {
            parallelism: 0,
            ..config()
        },
        SamplerConfig {
            send_spacing: Duration::ZERO,
            ..config()
        },
        SamplerConfig {
            candidate_capacity: 1,
            ..config()
        },
    ] {
        assert!(sampler.start(config, 8, now).is_err());
    }
}

/// 不断发现新联系人也不能突破单轮 64 次上限，换轮前必须留出发送间隔。
#[test]
fn each_round_has_a_hard_rpc_limit() {
    let now = Instant::now();
    let table = routing(now, 0);
    let (mut sampler, mut receiver) = begin(now, config());
    sampler.add_nodes((1..=100).map(node).collect(), &table, now);
    for second in 0..64 {
        let time = now + Duration::from_secs(second);
        let request = sampler.next(&table, 7, time).unwrap();
        sampler.success(request, response(21600), &table, time);
        receiver.try_recv().unwrap();
        assert_eq!(
            sampler.session.as_ref().unwrap().queries,
            second as usize + 1
        );
    }
    assert!(
        sampler
            .next(&table, 7, now + Duration::from_secs(64))
            .is_none()
    );
    assert_eq!(sampler.session.as_ref().unwrap().queries, 0);
    assert!(
        sampler
            .next(&table, 7, now + Duration::from_secs(65))
            .is_some()
    );
}

/// 同一 IP 的第二个 ID 不能并发占用另一条查询，即使两个端口不同。
#[test]
fn multiple_ids_on_one_ip_are_not_queried_concurrently() {
    let now = Instant::now();
    let table = routing(now, 0);
    let (mut sampler, _receiver) = begin(now, config());
    let mut alias = node(2);
    alias.address = ([127, 0, 0, 1], 9000).into();
    sampler.add_nodes(vec![node(1), alias], &table, now);
    let _request = sampler.next(&table, 7, now).unwrap();
    assert!(
        sampler
            .next(&table, 7, now + Duration::from_secs(1))
            .is_none()
    );
    assert!(
        sampler.deadline.is_none(),
        "等待在途响应，而不是反复触发已到期的计时器"
    );
}
