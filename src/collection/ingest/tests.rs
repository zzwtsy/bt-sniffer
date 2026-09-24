//! 分段确认与取消恢复；使用真实数据库线程确认已接纳命令，不依赖固定 sleep。
use super::*;
use crate::{
    dht::NodeId,
    info_hash::SwarmKey,
    storage::{Storage, StorageConfig},
};
use std::time::Duration;
// 第二个分段写失败时保留精确进度；重试不会漏数据，也不会重复累计观察次数。
#[tokio::test]
async fn failed_collection_retains_batch_and_resume_offset() {
    use tracing::instrument::WithSubscriber;
    let logs = tempfile::NamedTempFile::new().unwrap();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_max_level(tracing::Level::DEBUG)
        .with_writer(logs.reopen().unwrap())
        .finish();
    let dispatch = tracing::Dispatch::new(subscriber);
    let dir = tempfile::tempdir().unwrap();
    let store = Storage::open(StorageConfig::new(dir.path())).await.unwrap();
    store.handle.call(|c| {
        c.execute_batch("CREATE TRIGGER fail_zero BEFORE INSERT ON infohashes WHEN NEW.hash=zeroblob(20) BEGIN SELECT RAISE(ABORT,'test failure'); END;")?;
        Ok(())
    }).await.unwrap();
    let (tx, rx) = mpsc::channel(1);
    let mut samples = vec![SwarmKey([1; 20]); 1024];
    samples.extend([SwarmKey([0; 20]); 10]);
    tx.send(SampleBatch {
        observer: Default::default(),
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
    let collection_store = CollectionStore::new(store.handle.clone());
    let mut state = SampleIngest::new(rx, crate::clock::Clock::default());
    assert!(
        state
            .run(&collection_store)
            .with_subscriber(dispatch.clone())
            .await
            .is_err()
    );
    assert_eq!(state.offset, 1024);
    assert!(state.error().is_some());
    assert_eq!(collection_store.sample_observations(), 1024);
    assert!(state.current.is_some());
    store
        .handle
        .call(|c| {
            c.execute_batch("DROP TRIGGER fail_zero;")?;
            Ok(())
        })
        .await
        .unwrap();
    state
        .run(&collection_store)
        .with_subscriber(dispatch)
        .await
        .unwrap();
    assert!(state.current.is_none());
    assert!(state.error().is_none());
    assert_eq!(collection_store.sample_observations(), 1034);
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
    let text = std::fs::read_to_string(logs.path()).unwrap();
    let events: Vec<serde_json::Value> = text
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .filter(|event| event["fields"]["event"] == "sample_batch_save_started")
        .collect();
    assert_eq!(events.len(), 2);
    for (event, offset) in events.iter().zip([0, 1024]) {
        let fields = &event["fields"];
        assert_eq!(fields["schema_version"].as_u64(), Some(1));
        assert_eq!(fields["phase"], "persist");
        assert_eq!(fields["observed_at_ms"].as_i64(), Some(1000));
        assert_eq!(fields["interval_secs"].as_u64(), Some(300));
        assert_eq!(fields["count"].as_u64(), Some(1034));
        assert_eq!(fields["confirmed_offset"].as_u64(), Some(offset));
    }
}

/// 放弃等待不能撤销队列中的保存；恢复保留的分段必须幂等。
#[tokio::test]
async fn cancelled_database_confirmation_preserves_unconfirmed_batch() {
    let directory = tempfile::tempdir().unwrap();
    let database = Storage::open(StorageConfig::new(directory.path()))
        .await
        .unwrap();
    let store = CollectionStore::new(database.handle.clone());
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
            observed_at: std::time::UNIX_EPOCH + Duration::from_secs(1),
            interval: Duration::from_secs(300),
            num: 1,
            samples: vec![SwarmKey([1; 20])],
        })
        .await
        .unwrap();
    drop(sender);
    let (started, ready) = tokio::sync::oneshot::channel();
    let (release, blocked) = std::sync::mpsc::channel();
    let handle = database.handle.clone();
    let blocking = tokio::spawn(async move {
        handle
            .call(move |_| {
                started.send(()).unwrap();
                blocked.recv().unwrap();
                Ok(())
            })
            .await
    });
    ready.await.unwrap();
    let mut state = SampleIngest::new(receiver, crate::clock::Clock::default());
    let mut saving = Box::pin(state.run(&store));
    assert!(futures_util::poll!(&mut saving).is_pending());
    drop(saving);
    assert_eq!(state.offset, 0);
    assert!(state.current.is_some());
    assert!(state.error().is_none());
    release.send(()).unwrap();
    blocking.await.unwrap().unwrap();
    // FIFO 查询完成确认此前入队写入已完成，即使接收确认的 future 已被取消。
    let count = store
        .call(|connection| {
            Ok(
                connection.query_row("SELECT count(*) FROM infohashes", [], |row| {
                    row.get::<_, i64>(0)
                })?,
            )
        })
        .await
        .unwrap();
    assert_eq!(count, 1);
    state.run(&store).await.unwrap();
    assert!(state.current.is_none());
    let count = store
        .call(|connection| {
            Ok(
                connection.query_row("SELECT count(*) FROM infohashes", [], |row| {
                    row.get::<_, i64>(0)
                })?,
            )
        })
        .await
        .unwrap();
    assert_eq!(count, 1);
    database.shutdown().await.unwrap();
}
