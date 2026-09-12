//! 临时数据库与本机节点验证监督、恢复及共同退出期限，清理失败也必须保留可观察证据。
use super::*;
use crate::{
    krpc::{
        CompactNodesV4, CompactNodesV6, InfoHashSamples, InfoHashV1, KrpcMessage, MessageType,
        NodeId, QueryArgs, QueryMethod, ResponseArgs,
    },
    net::udp::UdpTransportConfig,
    storage::SavedContact,
};
use serde_bytes::ByteBuf;

// 故障按类型分类，后到的普通写入告警不能覆盖已经记录的致命错误。
#[test]
fn fatal_fault_is_sticky() {
    let (report, errors) = watch::channel(None);
    report_fault(&report, SessionFault::DatabaseExited);
    report_fault(&report, SessionFault::StorageWrite(StorageError::Capacity));
    assert_eq!(*errors.borrow(), Some(SessionFault::DatabaseExited));
}

// 必需任务提前结束时立即报警，并保留 dispatcher 的退出结果供关闭阶段使用。
#[tokio::test]
async fn supervisor_observes_dispatcher_exit() {
    let dir = tempfile::tempdir().unwrap();
    let mut session = PersistentSession::open(StorageConfig::new(dir.path()))
        .await
        .unwrap();
    let handle = session
        .add_node(
            "node",
            udp().await,
            TransactionManager::new(Duration::from_secs(2), 8),
            config(),
            AddressPolicy::LocalUnicast,
        )
        .await
        .unwrap();
    handle.shutdown().await.unwrap();
    let fault = tokio::time::timeout(Duration::from_secs(2), session.next_fault())
        .await
        .unwrap();
    assert!(matches!(
        fault,
        SessionFault::TaskExited {
            role: TaskRole::Dispatcher(0),
            ..
        }
    ));
    assert!(session.nodes[0].exit.is_some());
    assert!(session.shutdown().await.is_err());
}

// 各种后台角色的 panic 都带有角色信息，不会悄悄留下一半运行的服务。
#[tokio::test]
async fn supervisor_identifies_panicking_roles() {
    for role in [
        TaskRole::Dispatcher(0),
        TaskRole::Snapshot(0),
        TaskRole::SampleCollector(0),
        TaskRole::FetchCoordinator,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut session = PersistentSession::open(StorageConfig::new(dir.path()))
            .await
            .unwrap();
        let task = session.tasks.spawn(async {
            panic!("测试注入任务崩溃");
        });
        session.roles.insert(task.id(), role);
        let fault = tokio::time::timeout(Duration::from_secs(2), session.next_fault())
            .await
            .unwrap();
        assert!(matches!(fault, SessionFault::TaskFailed { role: actual, .. } if actual == role));
        assert!(session.shutdown().await.is_err());
    }
}

// 数据库线程退出通过通道关闭被发现，无须等待下一次快照或主动写入。
#[tokio::test]
async fn supervisor_detects_database_thread_exit() {
    let dir = tempfile::tempdir().unwrap();
    let mut session = PersistentSession::open(StorageConfig::new(dir.path()))
        .await
        .unwrap();
    let store = session.storage.as_ref().unwrap().handle.clone();
    assert_eq!(
        store.call::<()>(|_| panic!("测试数据库线程崩溃")).await,
        Err(StorageError::Closed)
    );
    let fault = tokio::time::timeout(Duration::from_secs(2), session.next_fault())
        .await
        .unwrap();
    assert_eq!(fault, SessionFault::DatabaseExited);
    assert!(session.shutdown().await.is_err());
}

// 写入故障暂停所有节点的采样，但不会中断基础 DHT 服务，也不会因消费者排空误报致命故障。
#[tokio::test]
async fn storage_fault_pauses_all_collectors_but_keeps_udp_alive() {
    let dir = tempfile::tempdir().unwrap();
    let mut session = PersistentSession::open(StorageConfig::new(dir.path()))
        .await
        .unwrap();
    let mut addresses = Vec::new();
    for index in 0..2 {
        let transport = udp().await;
        addresses.push(transport.local_addr().unwrap());
        session
            .add_node(
                &format!("node-{index}"),
                transport,
                TransactionManager::new(Duration::from_secs(2), 8),
                config(),
                AddressPolicy::LocalUnicast,
            )
            .await
            .unwrap();
        session
            .start_sampling(index, SamplerConfig::default())
            .await
            .unwrap();
    }
    let report = session.report.clone();
    tokio::spawn(async move {
        tokio::task::yield_now().await;
        report_fault(&report, SessionFault::StorageWrite(StorageError::Capacity));
    });
    let fault = tokio::time::timeout(Duration::from_secs(2), session.next_fault())
        .await
        .unwrap();
    assert!(!fault.fatal());
    for node in &session.nodes {
        assert!(!node.handle.status().await.unwrap().sampler.running);
    }
    // next_fault 会收集已结束的消费者，然后继续等待，而不是返回“任务意外退出”。
    assert!(
        tokio::time::timeout(Duration::from_millis(30), session.next_fault())
            .await
            .is_err()
    );
    let peer = udp().await;
    for address in addresses {
        peer.send_to(address, &find_query()).await.unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), peer.recv())
                .await
                .unwrap()
                .unwrap()
                .message
                .y,
            MessageType::Response
        );
    }
    assert!(session.shutdown().await.is_err());
}

async fn udp() -> UdpTransport {
    UdpTransport::bind("127.0.0.1:0", UdpTransportConfig::default())
        .await
        .unwrap()
}
fn config() -> DhtDispatcherConfig {
    let mut config = DhtDispatcherConfig::default();
    config.maintenance.enabled = false;
    config
}
fn response(t: ByteBuf, method: QueryMethod) -> KrpcMessage {
    response_family(t, method, AddressFamily::Ipv4)
}
fn response_family(t: ByteBuf, method: QueryMethod, family: AddressFamily) -> KrpcMessage {
    let mut bytes = b"d2:id20:".to_vec();
    bytes.extend([7; 20]);
    bytes.push(b'e');
    let mut args: ResponseArgs = bendy::serde::from_bytes(&bytes).unwrap();
    if method == QueryMethod::SampleInfohashes {
        if family == AddressFamily::Ipv6 {
            args.nodes6 = Some(CompactNodesV6(vec![]));
        } else {
            args.nodes = Some(CompactNodesV4(vec![]));
        }
        args.samples = Some(InfoHashSamples(vec![InfoHashV1([9; 20]); 2]));
        args.interval = Some(300);
        args.num = Some(1);
    }
    KrpcMessage {
        t,
        y: MessageType::Response,
        q: None,
        a: None,
        r: Some(args),
        e: None,
        ro: None,
    }
}

// 磁盘联系人经过一次真实 loopback 响应验证，然后采样结果和冷却分别落盘。
#[tokio::test]
async fn recovery_sampling_and_shutdown_roundtrip() {
    recovery_roundtrip(AddressFamily::Ipv4).await;
}

// IPv6 使用 nodes6，只有环境明确不支持 IPv6 时才跳过。
#[tokio::test]
async fn ipv6_recovery_sampling_roundtrip() {
    recovery_roundtrip(AddressFamily::Ipv6).await;
}
async fn family_udp(family: AddressFamily) -> Option<UdpTransport> {
    match UdpTransport::bind(
        if family == AddressFamily::Ipv6 {
            "[::1]:0"
        } else {
            "127.0.0.1:0"
        },
        UdpTransportConfig::default(),
    )
    .await
    {
        Ok(transport) => Some(transport),
        Err(crate::net::udp::UdpTransportError::Io(error))
            if family == AddressFamily::Ipv6
                && (error.kind() == std::io::ErrorKind::AddrNotAvailable
                    || matches!(error.raw_os_error(), Some(97 | 93))) =>
        {
            None
        }
        Err(error) => panic!("本机 socket 创建失败：{error}"),
    }
}
async fn recovery_roundtrip(family: AddressFamily) {
    let dir = tempfile::tempdir().unwrap();
    let settings = StorageConfig::new(dir.path());
    let Some(peer) = family_udp(family).await else {
        return;
    };
    let store = Storage::open(settings.clone()).await.unwrap();
    let identity = identity::load_or_create(&store.handle, "node", family, 1)
        .await
        .unwrap();
    store
        .handle
        .save_contacts(
            identity,
            &[SavedContact {
                id: NodeId([7; 20]),
                address: peer.local_addr().unwrap(),
                responded_at: 1,
            }],
        )
        .await
        .unwrap();
    store.shutdown().await.unwrap();
    let mut session = PersistentSession::open(settings.clone()).await.unwrap();
    let handle = session
        .add_node(
            "node",
            family_udp(family).await.unwrap(),
            TransactionManager::new(Duration::from_secs(2), 8),
            config(),
            AddressPolicy::LocalUnicast,
        )
        .await
        .unwrap();
    let incoming = tokio::time::timeout(Duration::from_secs(3), peer.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(incoming.message.q, Some(QueryMethod::Ping));
    // 旧节点尚未回答验证 ping；此时 find_node 不能把它作为 good 节点返回。
    peer.send_to(incoming.source, &find_query()).await.unwrap();
    let answer = tokio::time::timeout(Duration::from_secs(2), peer.recv())
        .await
        .unwrap()
        .unwrap();
    let args = answer.message.r.unwrap();
    if family == AddressFamily::Ipv6 {
        assert!(args.nodes6.unwrap().0.is_empty());
    } else {
        assert!(args.nodes.unwrap().0.is_empty());
    }
    peer.send_to(
        incoming.source,
        &response_family(incoming.message.t, QueryMethod::Ping, family),
    )
    .await
    .unwrap();
    session
        .start_sampling(
            0,
            SamplerConfig {
                address_policy: AddressPolicy::LocalUnicast,
                send_spacing: Duration::from_millis(10),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let incoming = tokio::time::timeout(Duration::from_secs(3), peer.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(incoming.message.q, Some(QueryMethod::SampleInfohashes));
    let db = &session.storage.as_ref().unwrap().handle;
    // 发包之前，两个冷却键已经提交，不能仅仅排进数据库队列。
    let pending = db
        .call(|c| {
            Ok(c.query_row(
                "SELECT count(*) FROM sampling_cooldowns WHERE pending=1",
                [],
                |r| r.get::<_, i64>(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(pending, 2);
    peer.send_to(
        incoming.source,
        &response_family(incoming.message.t, QueryMethod::SampleInfohashes, family),
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let counts = db
                .call(|c| {
                    Ok((
                        c.query_row("SELECT count(*) FROM infohashes", [], |r| {
                            r.get::<_, i64>(0)
                        })?,
                        c.query_row(
                            "SELECT count(*) FROM sampling_cooldowns WHERE pending=0",
                            [],
                            |r| r.get::<_, i64>(0),
                        )?,
                    ))
                })
                .await
                .unwrap();
            if counts == (1, 2) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        handle
            .sampling_status()
            .await
            .unwrap()
            .storage_error
            .is_none()
    );
    session.shutdown().await.unwrap();
    let store = Storage::open(settings).await.unwrap();
    let contacts = store.handle.load_contacts(identity).await.unwrap();
    assert_eq!(contacts.len(), 1);
    assert!(contacts[0].responded_at > 1);
    let restored = store
        .handle
        .restore_cooldowns(identity, unix_millis(SystemTime::now()).unwrap())
        .await
        .unwrap();
    assert_eq!(restored.len(), 2);
    store.shutdown().await.unwrap();
}

fn find_query() -> KrpcMessage {
    KrpcMessage {
        t: vec![42].into(),
        y: MessageType::Query,
        q: Some(QueryMethod::FindNode),
        a: Some(QueryArgs {
            id: NodeId([8; 20]),
            target: Some(NodeId([0; 20])),
            info_hash: None,
            port: None,
            token: None,
            implied_port: None,
            want: vec![],
        }),
        r: None,
        e: None,
        ro: Some(1),
    }
}

// 第二个分段写失败时保留精确进度；重试不会漏数据，也不会重复累计观察次数。
#[tokio::test]
async fn failed_collection_retains_batch_and_resume_offset() {
    let dir = tempfile::tempdir().unwrap();
    let store = Storage::open(StorageConfig::new(dir.path())).await.unwrap();
    store.handle.call(|c| {
        c.execute_batch("CREATE TRIGGER fail_zero BEFORE INSERT ON infohashes WHEN NEW.hash=zeroblob(20) BEGIN SELECT RAISE(ABORT,'test failure'); END;")?;
        Ok(())
    }).await.unwrap();
    let (tx, rx) = mpsc::channel(1);
    let mut samples = vec![InfoHashV1([1; 20]); 1024];
    samples.extend([InfoHashV1([0; 20]); 10]);
    tx.send(SampleBatch {
        responder: crate::dht::dispatcher::DiscoveredNode {
            id: NodeId([7; 20]),
            address: "127.0.0.1:1".parse().unwrap(),
        },
        target: NodeId([0; 20]),
        received_at: std::time::Instant::now(),
        observed_at: std::time::UNIX_EPOCH + Duration::from_secs(1),
        interval: Duration::from_secs(300),
        num: 2,
        samples,
    })
    .await
    .unwrap();
    drop(tx);
    let mut state = Collection {
        receiver: rx,
        current: None,
        offset: 0,
        error: None,
    };
    assert!(collect(&store.handle, &mut state).await.is_err());
    assert_eq!(state.offset, 1024);
    assert!(state.current.is_some());
    store
        .handle
        .call(|c| {
            c.execute_batch("DROP TRIGGER fail_zero;")?;
            Ok(())
        })
        .await
        .unwrap();
    collect(&store.handle, &mut state).await.unwrap();
    assert!(state.current.is_none());
    assert_eq!(
        store
            .handle
            .call(
                |c| Ok(c.query_row("SELECT count(*) FROM infohashes", [], |r| r
                    .get::<_, i64>(0))?)
            )
            .await
            .unwrap(),
        2
    );
    store.shutdown().await.unwrap();
}

// 公网模式不自动联系磁盘里的 loopback 地址；策略过滤发生在恢复队列入口。
#[tokio::test]
async fn recovery_respects_public_address_policy() {
    let dir = tempfile::tempdir().unwrap();
    let settings = StorageConfig::new(dir.path());
    let store = Storage::open(settings.clone()).await.unwrap();
    let id = identity::load_or_create(&store.handle, "node", AddressFamily::Ipv4, 1)
        .await
        .unwrap();
    store
        .handle
        .save_contacts(
            id,
            &[SavedContact {
                id: NodeId([7; 20]),
                address: "127.0.0.1:9999".parse().unwrap(),
                responded_at: 1,
            }],
        )
        .await
        .unwrap();
    store.shutdown().await.unwrap();
    let mut session = PersistentSession::open(settings).await.unwrap();
    let handle = session
        .add_node(
            "node",
            udp().await,
            TransactionManager::new(Duration::from_secs(2), 8),
            config(),
            AddressPolicy::PublicOnly,
        )
        .await
        .unwrap();
    assert!(handle.routing_snapshot().await.unwrap().is_empty());
    session.shutdown().await.unwrap();
}

// 地址正确但节点 ID 不符时，旧联系人不能通过恢复验证。
#[tokio::test]
async fn recovery_rejects_unexpected_identity() {
    let dir = tempfile::tempdir().unwrap();
    let settings = StorageConfig::new(dir.path());
    let peer = udp().await;
    let store = Storage::open(settings.clone()).await.unwrap();
    let id = identity::load_or_create(&store.handle, "node", AddressFamily::Ipv4, 1)
        .await
        .unwrap();
    store
        .handle
        .save_contacts(
            id,
            &[SavedContact {
                id: NodeId([8; 20]),
                address: peer.local_addr().unwrap(),
                responded_at: 1,
            }],
        )
        .await
        .unwrap();
    store.shutdown().await.unwrap();
    let mut session = PersistentSession::open(settings).await.unwrap();
    let handle = session
        .add_node(
            "node",
            udp().await,
            TransactionManager::new(Duration::from_secs(2), 8),
            config(),
            AddressPolicy::LocalUnicast,
        )
        .await
        .unwrap();
    let ping = tokio::time::timeout(Duration::from_secs(2), peer.recv())
        .await
        .unwrap()
        .unwrap();
    peer.send_to(ping.source, &response(ping.message.t, QueryMethod::Ping))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while !handle.routing_snapshot().await.unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    session.shutdown().await.unwrap();
}

// 关闭超时必须返回错误；不能把“已经发出关闭请求”当作“数据已经全部保存”。
#[tokio::test(start_paused = true)]
async fn shutdown_timeout_is_reported() {
    let dir = tempfile::tempdir().unwrap();
    let session = PersistentSession::open(StorageConfig::new(dir.path()))
        .await
        .unwrap();
    session.faults.push("最初的 socket 故障".into());
    session.faults.push("后续快照失败".into());
    let handle = session.storage.as_ref().unwrap().handle.clone();
    let (entered, ready) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let job = tokio::spawn(async move {
        handle
            .call(move |_| {
                let _ = entered.send(());
                wait.recv_timeout(Duration::from_secs(5))
                    .map_err(|_| StorageError::Closed)?;
                Ok(())
            })
            .await
    });
    ready.await.unwrap();
    let error = session.shutdown().await.unwrap_err();
    assert!(
        error
            .iter()
            .any(|e| e.contains("30 秒") && e.contains("关闭数据库"))
    );
    assert!(error.iter().any(|e| e == "最初的 socket 故障"));
    assert!(error.iter().any(|e| e == "后续快照失败"));
    release.send(()).unwrap();
    job.await.unwrap().unwrap();
}

// 数据库线程被占住时，UDP 仍能及时回应；不能在 dispatcher 内 await SQL。
#[tokio::test]
async fn slow_database_does_not_block_udp() {
    let dir = tempfile::tempdir().unwrap();
    let mut session = PersistentSession::open(StorageConfig::new(dir.path()))
        .await
        .unwrap();
    let transport = udp().await;
    let address = transport.local_addr().unwrap();
    session
        .add_node(
            "node",
            transport,
            TransactionManager::new(Duration::from_secs(2), 8),
            config(),
            AddressPolicy::LocalUnicast,
        )
        .await
        .unwrap();
    let store = session.storage.as_ref().unwrap().handle.clone();
    let (entered, ready) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let blocking = tokio::spawn(async move {
        store
            .call(move |_| {
                let _ = entered.send(());
                wait.recv_timeout(Duration::from_secs(5))
                    .map_err(|_| StorageError::Closed)?;
                Ok(())
            })
            .await
    });
    ready.await.unwrap();
    let peer = udp().await;
    peer.send_to(address, &find_query()).await.unwrap();
    let reply = tokio::time::timeout(Duration::from_secs(1), peer.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply.message.y, MessageType::Response);
    release.send(()).unwrap();
    blocking.await.unwrap().unwrap();
    session.shutdown().await.unwrap();
}

// 尚未回答的恢复候选仍在最终快照中，快速重启不会把种子联系人清空。
#[tokio::test]
async fn shutdown_preserves_unverified_candidates() {
    let dir = tempfile::tempdir().unwrap();
    let settings = StorageConfig::new(dir.path());
    let peer = udp().await;
    let store = Storage::open(settings.clone()).await.unwrap();
    let identity = identity::load_or_create(&store.handle, "node", AddressFamily::Ipv4, 1)
        .await
        .unwrap();
    let saved = SavedContact {
        id: NodeId([7; 20]),
        address: peer.local_addr().unwrap(),
        responded_at: 1,
    };
    store
        .handle
        .save_contacts(identity, std::slice::from_ref(&saved))
        .await
        .unwrap();
    store.shutdown().await.unwrap();
    let mut session = PersistentSession::open(settings.clone()).await.unwrap();
    session
        .add_node(
            "node",
            udp().await,
            TransactionManager::new(Duration::from_secs(5), 8),
            config(),
            AddressPolicy::LocalUnicast,
        )
        .await
        .unwrap();
    session.shutdown().await.unwrap();
    let store = Storage::open(settings).await.unwrap();
    assert_eq!(
        store.handle.load_contacts(identity).await.unwrap(),
        vec![saved]
    );
    store.shutdown().await.unwrap();
}

/// 非正常收尾导致的任务取消必须报告故障，不能当作主动关闭。
#[tokio::test]
async fn supervisor_distinguishes_unexpected_cancellation() {
    let dir = tempfile::tempdir().unwrap();
    let mut session = PersistentSession::open(StorageConfig::new(dir.path()))
        .await
        .unwrap();
    let task = session.tasks.spawn(std::future::pending());
    session.roles.insert(task.id(), TaskRole::FetchCoordinator);
    task.abort();
    let fault = session.next_fault().await;
    assert!(matches!(
        fault,
        SessionFault::TaskFailed {
            role: TaskRole::FetchCoordinator,
            cancelled: true,
            ..
        }
    ));
    assert!(session.shutdown().await.is_err());
}

/// 清理超时不能丢失此前已报告的故障，错误必须保留在共享记录中。
#[tokio::test(start_paused = true)]
async fn timeout_preserves_faults_from_unfinished_cleanup_task() {
    let dir = tempfile::tempdir().unwrap();
    let mut session = PersistentSession::open(StorageConfig::new(dir.path()))
        .await
        .unwrap();
    let faults = session.faults.clone();
    let report = session.report.clone();
    let task = session.tasks.spawn(async move {
        faults.push("worker 的原始故障".into());
        report_fault(
            &report,
            SessionFault::CollectorFailed {
                detail: "worker 的原始故障".into(),
            },
        );
        faults.push("随后撤销入口失败".into());
        std::future::pending().await
    });
    session.roles.insert(task.id(), TaskRole::FetchCoordinator);
    let errors = session.shutdown().await.unwrap_err();
    assert!(errors.iter().any(|e| e == "worker 的原始故障"));
    assert!(errors.iter().any(|e| e == "随后撤销入口失败"));
    assert!(
        errors
            .iter()
            .any(|e| e.contains("30 秒") && e.contains("回收采集协调器"))
    );
}
