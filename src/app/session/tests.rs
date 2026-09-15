//! 临时数据库与本机节点验证监督、恢复及共同退出期限，清理失败也必须保留可观察证据。
use super::*;
use crate::dht::krpc::CompactNodesV4;
use crate::dht::krpc::CompactNodesV6;
use crate::dht::krpc::InfoHashSamples;
use crate::dht::krpc::KrpcMessage;
use crate::dht::krpc::MessageType;
use crate::dht::krpc::NodeId;
use crate::dht::krpc::QueryArgs;
use crate::dht::krpc::QueryMethod;
use crate::dht::krpc::ResponseArgs;
use crate::dht::persistence::DhtStore;
use crate::dht::persistence::SavedContact;
use crate::dht::udp::UdpTransportConfig;
use crate::info_hash::InfoHashV1;
use serde_bytes::ByteBuf;

// 故障按类型分类，后到的普通写入告警不能覆盖已经记录的致命错误。
#[test]
fn fatal_fault_is_sticky() {
    let (report, errors) = FaultReporter::new();
    report.publish(SessionFault::DatabaseExited);
    report.publish(SessionFault::StorageWrite(StorageError::Capacity));
    assert_eq!(*errors.borrow(), Some(SessionFault::DatabaseExited));
}

// 必需任务提前结束时立即报警，并保留 dispatcher 的退出结果供关闭阶段使用。
#[tokio::test]
async fn supervisor_observes_dispatcher_exit() {
    let dir = tempfile::tempdir().unwrap();
    let mut session = Session::open(StorageConfig::new(dir.path())).await.unwrap();
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
        let mut session = Session::open(StorageConfig::new(dir.path())).await.unwrap();
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
    let mut session = Session::open(StorageConfig::new(dir.path())).await.unwrap();
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
    let mut session = Session::open(StorageConfig::new(dir.path())).await.unwrap();
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
        report.publish(SessionFault::StorageWrite(StorageError::Capacity));
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
        Err(crate::dht::udp::UdpTransportError::Io(error))
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
    let identity =
        identity::load_or_create(&DhtStore::new(store.handle.clone()), "node", family, 1)
            .await
            .unwrap();
    DhtStore::new(store.handle.clone())
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
    let mut session = Session::open(settings.clone()).await.unwrap();
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
    let contacts = DhtStore::new(store.handle.clone())
        .load_contacts(identity)
        .await
        .unwrap();
    assert_eq!(contacts.len(), 1);
    assert!(contacts[0].responded_at > 1);
    let restored = DhtStore::new(store.handle.clone())
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

// 公网模式不自动联系磁盘里的 loopback 地址；策略过滤发生在恢复队列入口。
#[tokio::test]
async fn recovery_respects_public_address_policy() {
    let dir = tempfile::tempdir().unwrap();
    let settings = StorageConfig::new(dir.path());
    let store = Storage::open(settings.clone()).await.unwrap();
    let id = identity::load_or_create(
        &DhtStore::new(store.handle.clone()),
        "node",
        AddressFamily::Ipv4,
        1,
    )
    .await
    .unwrap();
    DhtStore::new(store.handle.clone())
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
    let mut session = Session::open(settings).await.unwrap();
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
    let id = identity::load_or_create(
        &DhtStore::new(store.handle.clone()),
        "node",
        AddressFamily::Ipv4,
        1,
    )
    .await
    .unwrap();
    DhtStore::new(store.handle.clone())
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
    let mut session = Session::open(settings).await.unwrap();
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
    let mut session = Session::open(StorageConfig::new(dir.path())).await.unwrap();
    let closed = session.test_close_observer();
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
    closed.await.unwrap();
    Storage::open(StorageConfig::new(dir.path()))
        .await
        .unwrap()
        .shutdown()
        .await
        .unwrap();
}

// 数据库线程被占住时，UDP 仍能及时回应；不能在 dispatcher 内 await SQL。
#[tokio::test]
async fn slow_database_does_not_block_udp() {
    let dir = tempfile::tempdir().unwrap();
    let mut session = Session::open(StorageConfig::new(dir.path())).await.unwrap();
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
    let identity = identity::load_or_create(
        &DhtStore::new(store.handle.clone()),
        "node",
        AddressFamily::Ipv4,
        1,
    )
    .await
    .unwrap();
    let saved = SavedContact {
        id: NodeId([7; 20]),
        address: peer.local_addr().unwrap(),
        responded_at: 1,
    };
    DhtStore::new(store.handle.clone())
        .save_contacts(identity, std::slice::from_ref(&saved))
        .await
        .unwrap();
    store.shutdown().await.unwrap();
    let mut session = Session::open(settings.clone()).await.unwrap();
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
        DhtStore::new(store.handle.clone())
            .load_contacts(identity)
            .await
            .unwrap(),
        vec![saved]
    );
    store.shutdown().await.unwrap();
}

/// 非正常收尾导致的任务取消必须报告故障，不能当作主动关闭。
#[tokio::test]
async fn supervisor_distinguishes_unexpected_cancellation() {
    let dir = tempfile::tempdir().unwrap();
    let mut session = Session::open(StorageConfig::new(dir.path())).await.unwrap();
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
    let mut session = Session::open(StorageConfig::new(dir.path())).await.unwrap();
    let faults = session.faults.clone();
    let report = session.report.clone();
    let task = session.tasks.spawn(async move {
        faults.push("worker 的原始故障".into());
        report.publish(SessionFault::CollectorFailed {
            detail: "worker 的原始故障".into(),
        });
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

/// collector 回调返回前已经保存原始诊断并发布故障；同一存储错误在时钟边界仍属致命错误。
#[test]
fn fault_callbacks_record_synchronously_and_preserve_classification() {
    use crate::collection::CollectorError;
    for error in [
        CollectorError::Storage(StorageError::Capacity),
        CollectorError::Clock(StorageError::Capacity),
        CollectorError::Control {
            operation: "查找 peer",
            source: crate::dht::dispatcher::QueryError::DispatcherClosed,
        },
    ] {
        let (report, errors) = FaultReporter::new();
        let faults = FaultLog::default();
        let callback = report.collector_callback(faults.clone());
        callback(&error);
        assert_eq!(faults.take(), vec![error.to_string()]);
        let expected = match &error {
            CollectorError::Storage(value) => SessionFault::StorageWrite(value.clone()),
            _ => SessionFault::CollectorFailed {
                detail: error.to_string(),
            },
        };
        assert_eq!(*errors.borrow(), Some(expected));
    }
    let (report, errors) = FaultReporter::new();
    report.storage_callback()(StorageError::Capacity);
    assert_eq!(
        *errors.borrow(),
        Some(SessionFault::StorageWrite(StorageError::Capacity))
    );
}

/// 单位通知也必须支持多次变更；消费暂停通知不应标记会话详情已读。
#[test]
fn fault_notifications_follow_sticky_state_without_consuming_details() {
    let (report, mut errors) = FaultReporter::new();
    let mut pause = report.pause_notifications().subscribe();
    assert!(!pause.has_changed().unwrap());
    for error in [StorageError::Capacity, StorageError::Closed] {
        report.publish(SessionFault::StorageWrite(error.clone()));
        assert!(pause.has_changed().unwrap());
        pause.borrow_and_update();
        assert!(errors.has_changed().unwrap());
        assert_eq!(
            *errors.borrow_and_update(),
            Some(SessionFault::StorageWrite(error.clone()))
        );
        report.publish(SessionFault::StorageWrite(error));
        assert!(!pause.has_changed().unwrap());
        assert!(!errors.has_changed().unwrap());
    }
    report.publish(SessionFault::DatabaseExited);
    assert!(pause.has_changed().unwrap());
    pause.borrow_and_update();
    errors.borrow_and_update();
    report.publish(SessionFault::StorageWrite(StorageError::Capacity));
    report.publish(SessionFault::CollectorFailed {
        detail: "后到的致命故障".into(),
    });
    assert!(!pause.has_changed().unwrap());
    assert_eq!(*errors.borrow(), Some(SessionFault::DatabaseExited));
}

/// 两个先后收尾阶段各需 20 秒，必须在总计 30 秒时超时，而不是为第二阶段重新计时。
#[tokio::test(start_paused = true)]
// 用通道建立阶段先后关系；两段各需 20 秒，但总收尾只能使用同一个 30 秒期限。
async fn shutdown_stages_share_one_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let mut session = Session::open(StorageConfig::new(dir.path())).await.unwrap();
    let (finished, next) = tokio::sync::oneshot::channel();
    let faults = session.faults.clone();
    let first = session.tasks.spawn(async move {
        tokio::time::sleep(Duration::from_secs(20)).await;
        faults.push("第一阶段已观察的错误".into());
        finished.send(()).unwrap();
        TaskOutput::Fetch(Ok(()))
    });
    session.roles.insert(first.id(), TaskRole::FetchCoordinator);
    let faults = session.faults.clone();
    let second = session.tasks.spawn(async move {
        next.await.unwrap();
        faults.push("第二阶段已观察的错误".into());
        tokio::time::sleep(Duration::from_secs(20)).await;
        TaskOutput::Snapshot(Ok(()))
    });
    session.roles.insert(second.id(), TaskRole::Snapshot(0));
    let started = tokio::time::Instant::now();
    let errors = session.shutdown().await.unwrap_err();
    assert_eq!(started.elapsed(), Duration::from_secs(30));
    assert!(errors.iter().any(|e| e == "第一阶段已观察的错误"));
    assert!(errors.iter().any(|e| e == "第二阶段已观察的错误"));
    assert!(
        errors
            .iter()
            .any(|e| e.contains("30 秒") && e.contains("回收节点任务"))
    );
}

/// 尚未完成节点停产时丢弃 next_fault，下一次必须继续处理同一故障，不能提前标记已读。
#[tokio::test]
async fn cancelled_fault_handling_keeps_detail_unread() {
    let dir = tempfile::tempdir().unwrap();
    let mut session = Session::open(StorageConfig::new(dir.path())).await.unwrap();
    let identity = identity::load_or_create(&session.dht_store, "paused", AddressFamily::Ipv4, 1)
        .await
        .unwrap();
    let table = RoutingTable::new(
        identity.node_id,
        AddressFamily::Ipv4,
        std::time::Instant::now(),
    );
    let (dispatcher, handle) = DhtDispatcher::with_config(
        udp().await,
        table,
        TransactionManager::new(Duration::from_secs(1), 8),
        config(),
    )
    .unwrap();
    session.nodes.push(Node {
        identity,
        handle,
        exit: None,
        collection: None,
        collecting: false,
    });
    session
        .report
        .publish(SessionFault::StorageWrite(StorageError::Capacity));
    let mut waiting = Box::pin(session.next_fault());
    // dispatcher 尚未运行，停产命令已经入队，但不可能收到完成响应。
    assert!(futures_util::poll!(&mut waiting).is_pending());
    drop(waiting);
    assert!(session.errors.has_changed().unwrap());
    let task = session
        .tasks
        .spawn(async move { TaskOutput::Dispatcher(dispatcher.run_persistent().await) });
    session.roles.insert(task.id(), TaskRole::Dispatcher(0));
    let fault = tokio::time::timeout(Duration::from_secs(2), session.next_fault())
        .await
        .unwrap();
    assert_eq!(fault, SessionFault::StorageWrite(StorageError::Capacity));
    assert!(!session.errors.has_changed().unwrap());
    assert!(session.shutdown().await.is_err());
}

/// 使用实际消费和 shutdown 路径验证模块 target；只访问临时 SQLite 和临时日志文件。
#[tokio::test]
async fn session_logs_use_module_targets_and_fields() {
    // 同一日志 callsite 还会被其他会话测试并发调用。隔离进程验证本地 subscriber，
    // 避免 tracing 全局 callsite/filter 缓存让目标字段断言受其他测试时序影响。
    const CHILD: &str = "BT_SNIFFER_SESSION_LOG_TEST_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "app::session::tests::session_logs_use_module_targets_and_fields",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    use tracing::instrument::WithSubscriber;
    let dir = tempfile::tempdir().unwrap();
    let session = Session::open(StorageConfig::new(dir.path())).await.unwrap();
    let logs = tempfile::NamedTempFile::new().unwrap();
    let writer = logs.reopen().unwrap();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_ansi(false)
        .with_env_filter("off,bt_sniffer::collection::ingest=debug,bt_sniffer::app::session=info")
        .with_writer(move || writer.try_clone().unwrap())
        .finish();
    let dispatch = tracing::Dispatch::new(subscriber);
    let (sender, receiver) = mpsc::channel(1);
    sender
        .send(SampleBatch {
            observer: Default::default(),
            responder: crate::dht::dispatcher::DiscoveredNode {
                id: NodeId([7; 20]),
                address: "127.0.0.1:1".parse().unwrap(),
            },
            target: NodeId([0; 20]),
            received_at: std::time::Instant::now(),
            observed_at: SystemTime::UNIX_EPOCH + Duration::from_secs(1),
            interval: Duration::from_secs(300),
            num: 1,
            samples: vec![InfoHashV1([1; 20])],
        })
        .await
        .unwrap();
    drop(sender);
    let mut collection = SampleIngest::new(receiver, crate::clock::Clock::default());
    collection
        .run(&session.test_store())
        .with_subscriber(dispatch.clone())
        .await
        .unwrap();
    session.shutdown().with_subscriber(dispatch).await.unwrap();
    let text = std::fs::read_to_string(logs.path()).unwrap();
    let events: Vec<serde_json::Value> = text
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(events.len(), 2, "{text}");
    assert_eq!(events[0]["target"], "bt_sniffer::collection::ingest");
    assert_eq!(events[1]["target"], "bt_sniffer::app::session");
    assert_eq!(events[0]["level"], "DEBUG");
    assert_eq!(events[0]["fields"]["message"], "开始保存已验证采样批次");
    assert_eq!(events[0]["fields"]["event"], "sample_batch_save_started");
    assert_eq!(events[0]["fields"]["schema_version"].as_u64(), Some(1));
    assert_eq!(events[0]["fields"]["phase"], "persist");
    assert_eq!(events[0]["fields"]["observed_at_ms"].as_i64(), Some(1000));
    assert_eq!(events[0]["fields"]["interval_secs"].as_u64(), Some(300));
    assert_eq!(events[0]["fields"]["confirmed_offset"].as_u64(), Some(0));
    assert_eq!(events[0]["fields"]["count"], 1);
    assert_eq!(events[0]["fields"]["num"], 1);
    assert_eq!(events[1]["level"], "INFO");
    assert_eq!(events[1]["fields"]["event"], "session_shutdown");
    assert_eq!(events[1]["fields"]["schema_version"], 1);
    assert_eq!(events[1]["fields"]["success"], true);
    assert_eq!(events[1]["fields"]["error_count"], 0);
}
