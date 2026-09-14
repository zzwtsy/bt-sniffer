//! 使用真实临时 SQLite 验证预约、确认与结算；线程完成靠消息确认，不靠推进虚拟时间。
use super::*;
use crate::dht::persistence::identity::load_or_create;
use crate::dht::persistence::test_storage::TestStorage as Storage;
use crate::dht::routing::AddressFamily;
use crate::storage::StorageConfig;

/// 保留目录、数据库和会话的独立所有权，测试按原顺序显式关闭数据库。
struct Fixture {
    _dir: tempfile::TempDir,
    store: Storage,
    sampler: Sampler,
    table: RoutingTable,
    receiver: mpsc::Receiver<SampleBatch>,
    now: Instant,
}

async fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let store = Storage::open(StorageConfig::new(dir.path())).await.unwrap();
    let identity = load_or_create(&store.handle, "test", AddressFamily::Ipv4, 1)
        .await
        .unwrap();
    let mut sampler = Sampler::default();
    sampler
        .attach_storage(store.handle.clone(), identity, vec![])
        .unwrap();
    let now = tokio::time::Instant::now().into_std();
    let mut table = RoutingTable::new(identity.node_id, identity.family, now);
    table.observe_response(NodeId([7; 20]), "8.8.8.8:9999".parse().unwrap(), now);
    let receiver = sampler.start(SamplerConfig::default(), 8, now).unwrap();
    Fixture {
        _dir: dir,
        store,
        sampler,
        table,
        receiver,
        now,
    }
}

// 未收到数据库确认时拿不到可发送的请求；确认后还要服从 transaction 容量。
#[tokio::test(start_paused = true)]
async fn reservation_confirmation_and_capacity_gate_sending() {
    let Fixture {
        _dir,
        store,
        mut sampler,
        table,
        receiver: _rx,
        now,
    } = fixture().await;
    let request = sampler.next(&table, 7, now).unwrap();
    assert!(sampler.reserve_request(request, now).is_none());
    assert!(sampler.take_reserved(7, now).is_none());
    sampler.storage_event().await;
    tokio::time::advance(Duration::from_secs(1)).await;
    let now = tokio::time::Instant::now().into_std();
    assert!(sampler.take_reserved(0, now).is_none());
    let request = sampler.take_reserved(7, now).unwrap();
    assert!(request.lease.is_some());
    sampler.cancel_request(&request, now);
    sampler.stop(now);
    sampler.flush_storage().await.unwrap();
    store.shutdown().await.unwrap();
}

// 停止后立即启动新会话，旧 SQL 确认也只能结算冷却，不能给新会话发包。
#[tokio::test]
async fn stale_confirmation_after_stop_never_sends() {
    let Fixture {
        _dir,
        store,
        mut sampler,
        table,
        receiver: _rx,
        now,
    } = fixture().await;
    let request = sampler.next(&table, 7, now).unwrap();
    sampler.reserve_request(request, now);
    sampler.stop(now);
    let _new_receiver = sampler.start(SamplerConfig::default(), 8, now).unwrap();
    sampler.storage_event().await;
    assert!(sampler.durable.as_ref().unwrap().ready.is_none());
    sampler.flush_storage().await.unwrap();
    let pending = store
        .handle
        .call(|c| {
            Ok(c.query_row(
                "SELECT count(*) FROM sampling_cooldowns WHERE pending=1",
                [],
                |r| r.get::<_, i64>(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(pending, 0);
    sampler.stop(now);
    store.shutdown().await.unwrap();
}

// SQLite 写入失败时立即进入可观测的暂停状态，而不是继续联网采样。
#[tokio::test]
async fn storage_failure_is_visible_and_stops_new_sampling() {
    let Fixture {
        _dir,
        store,
        mut sampler,
        table,
        receiver: _rx,
        now,
    } = fixture().await;
    store
        .handle
        .call(|c| {
            c.execute_batch("PRAGMA query_only=ON;")?;
            Ok(())
        })
        .await
        .unwrap();
    let request = sampler.next(&table, 7, now).unwrap();
    sampler.reserve_request(request, now);
    sampler.storage_event().await;
    assert!(sampler.status().storage_error.is_some());
    assert_eq!(sampler.status().pause, PauseReason::Storage);
    assert!(!sampler.status().running);
    assert!(
        sampler
            .next(&table, 7, now + Duration::from_secs(60))
            .is_none()
    );
    assert!(matches!(
        sampler.start(SamplerConfig::default(), 8, now),
        Err(SamplerError::StorageFault)
    ));
    store.shutdown().await.unwrap();
}

// 收到成功响应后，结果可以交付；同一节点必须等冷却结算提交后才能重新调度。
#[tokio::test(start_paused = true)]
async fn successful_response_settles_actual_interval() {
    let Fixture {
        _dir,
        store,
        mut sampler,
        table,
        receiver: mut rx,
        now,
    } = fixture().await;
    let request = sampler.next(&table, 7, now).unwrap();
    sampler.reserve_request(request, now);
    sampler.storage_event().await;
    tokio::time::advance(Duration::from_secs(1)).await;
    let now = tokio::time::Instant::now().into_std();
    let request = sampler.take_reserved(7, now).unwrap();
    sampler.success(
        request,
        SampleResponse {
            nodes: vec![],
            interval: Duration::from_secs(300),
            num: 0,
            samples: vec![],
        },
        &table,
        now,
    );
    assert!(rx.recv().await.unwrap().samples.is_empty());
    assert!(sampler.next(&table, 7, now).is_none());
    sampler.flush_storage().await.unwrap();
    let durations = store
        .handle
        .call(|c| {
            Ok(c.query_row(
                "SELECT min(duration_ms),max(pending) FROM sampling_cooldowns",
                [],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(durations, (300_000, 0));
    sampler.stop(now);
    store.shutdown().await.unwrap();
}

/// 事件等待被取消后，已入队预约仍能取回；Node ID/IP 各插入一次并最终结算。
#[tokio::test]
async fn cancelled_storage_wait_retains_reservation_and_settles_once() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let Fixture {
            _dir,
            store,
            mut sampler,
            table,
            receiver,
            now,
        } = fixture().await;
        // 准备：触发器观察真实写入次数，屏障让数据库在线程内停住。
        store
            .handle
            .call(|connection| {
                connection.execute_batch(
                    "CREATE TEMP TABLE reservation_writes (count INTEGER NOT NULL);
                 INSERT INTO reservation_writes VALUES (0);
                 CREATE TEMP TRIGGER count_reservation AFTER INSERT ON main.sampling_cooldowns
                 BEGIN UPDATE reservation_writes SET count = count + 1; END;",
                )?;
                Ok(())
            })
            .await
            .unwrap();
        let (started, ready) = tokio::sync::oneshot::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        let handle = store.handle.clone();
        let blocker = tokio::spawn(async move {
            handle
                .call(move |_| {
                    started.send(()).unwrap();
                    blocked.recv_timeout(Duration::from_secs(5)).unwrap();
                    Ok(())
                })
                .await
        });
        ready.await.unwrap();

        // 触发：队列有空位且 call 不申请载荷字节，首次 poll 入队后停在回复等待。
        let request = sampler.next(&table, 7, now).unwrap();
        assert!(sampler.reserve_request(request, now).is_none());
        {
            let event = sampler.storage_event();
            tokio::pin!(event);
            assert!(futures_util::poll!(&mut event).is_pending());
        }
        assert!(sampler.durable.as_ref().unwrap().reserving.is_some());
        release.send(()).unwrap();
        blocker.await.unwrap().unwrap();
        sampler.storage_event().await;
        let request = sampler.durable.as_mut().unwrap().ready.take().unwrap();
        assert!(request.lease.is_some());

        // 断言与收尾：取消请求后排空结算，不能遗留 pending 或占用批次许可。
        sampler.cancel_request(&request, now);
        drop(request);
        sampler.stop(now);
        sampler.flush_storage().await.unwrap();
        let (writes, pending): (i64, i64) = store
            .handle
            .call(|connection| {
                let writes =
                    connection
                        .query_row("SELECT count FROM reservation_writes", [], |row| row.get(0))?;
                let pending = connection.query_row(
                    "SELECT count(*) FROM sampling_cooldowns WHERE pending = 1",
                    [],
                    |row| row.get(0),
                )?;
                Ok((writes, pending))
            })
            .await
            .unwrap();
        assert_eq!(writes, 2, "一个预约只创建 Node ID 和 IP 两条记录");
        assert_eq!(pending, 0);
        assert_eq!(receiver.capacity(), receiver.max_capacity());
        store.shutdown().await.unwrap();
    })
    .await
    .expect("事件等待取消后必须能完成预约结算和数据库关闭");
}

/// 正常停止时，尚在待发阶段的预约应撤销，已经发送的仍由原取消路径保守恢复。
#[tokio::test(start_paused = true)]
async fn stopping_before_send_revokes_ready_lease() {
    let Fixture {
        _dir,
        store,
        mut sampler,
        table,
        receiver: _rx,
        now,
    } = fixture().await;
    let request = sampler.next(&table, 7, now).unwrap();
    sampler.reserve_request(request, now);
    sampler.storage_event().await;
    sampler.stop(now);
    sampler.flush_storage().await.unwrap();
    let count: i64 = store
        .handle
        .call(|c| Ok(c.query_row("SELECT count(*) FROM sampling_cooldowns", [], |r| r.get(0))?))
        .await
        .unwrap();
    assert_eq!(count, 0);
    store.shutdown().await.unwrap();
}
