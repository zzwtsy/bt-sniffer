//! HTTP/SSE 资源边界和真实 socket 收尾，不启动公网。
#![cfg(test)]
use super::*;
use crate::{
    collection::catalog::ensure_catalog,
    storage::{Storage, StorageConfig},
};
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use futures_util::StreamExt;
use sha1::{Digest, Sha1};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tower::ServiceExt;

async fn fixture() -> (tempfile::TempDir, Storage, Arc<State>) {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(StorageConfig::new(dir.path())).await.unwrap();
    let observer = Observer::new("test-run".into());
    let mut store = CollectionStore::new(storage.handle.clone());
    store.observer = observer.clone();
    let state = Arc::new(State {
        observer,
        store,
        nodes: Vec::new(),
        cache: Mutex::new(json!({"nodes":[],"database":{"available":false}})),
        requests: Arc::new(Semaphore::new(4)),
        database: Arc::new(Semaphore::new(1)),
        streams: Arc::new(Semaphore::new(4)),
        rate: Mutex::new((Instant::now(), 10.0)),
        stop: CancellationToken::new(),
        finish: CancellationToken::new(),
    });
    (dir, storage, state)
}
async fn get(s: &Arc<State>, uri: &str) -> axum::response::Response {
    api::router(s.clone())
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap()
}

async fn body_json(response: axum::response::Response) -> Value {
    serde_json::from_slice(&to_bytes(response.into_body(), 1024 * 1024).await.unwrap()).unwrap()
}

async fn insert_torrent(state: &Arc<State>) -> String {
    let info =
        b"d6:lengthi42e4:name12:example-file12:piece lengthi64e6:pieces20:aaaaaaaaaaaaaaaaaaaae"
            .to_vec();
    let hash: [u8; 20] = Sha1::digest(&info).into();
    let hash_for_db = hash;
    state
        .store
        .call(move |connection| {
            let tx = connection.transaction()?;
            tx.execute(
                "INSERT INTO infohashes(hash,first_seen,last_seen) VALUES(?1,1,1)",
                [hash_for_db.as_slice()],
            )?;
            tx.execute(
                "INSERT INTO metadata(hash,info,fetched_at) VALUES(?1,?2,3)",
                rusqlite::params![hash_for_db.as_slice(), info.as_slice()],
            )?;
            ensure_catalog(&tx, &hash_for_db, &info)?;
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();
    crate::observation::hex(&hash)
}
#[tokio::test]
async fn snapshot_keeps_stable_outer_contract() {
    let (_dir, storage, state) = fixture().await;
    let snapshot = state.snapshot();
    assert_eq!(snapshot["schema_version"], 1);
    assert_eq!(snapshot["window"]["run_id"], "test-run");
    assert!(snapshot["runtime"]["sources"].is_object());
    assert_eq!(snapshot["cached"]["nodes"], json!([]));
    assert_eq!(snapshot["cached"]["database"]["available"], false);
    storage.shutdown().await.unwrap();
}

#[test]
fn database_cache_preserves_value_across_failure_and_recovers() {
    let mut cache = json!({"nodes":[],"database":{"available":false}});
    mark_database_stale(&mut cache);
    assert_eq!(cache["database"], json!({"available":false,"stale":true}));

    cache_database(&mut cache, 100, json!({"jobs":{"pending":1}}));
    assert_eq!(cache["database"]["available"], true);
    assert_eq!(cache["database"]["stale"], false);
    assert_eq!(cache["database"]["observed_at_ms"], 100);
    assert_eq!(cache["database"]["value"]["jobs"]["pending"], 1);

    mark_database_stale(&mut cache);
    assert_eq!(cache["database"]["stale"], true);
    assert_eq!(cache["database"]["value"]["jobs"]["pending"], 1);

    cache_database(&mut cache, 200, json!({"jobs":{"pending":2}}));
    assert_eq!(cache["database"]["stale"], false);
    assert_eq!(cache["database"]["observed_at_ms"], 200);
    assert_eq!(cache["database"]["value"]["jobs"]["pending"], 2);
}
#[tokio::test]
async fn minimal_read_only_api_and_removed_routes() {
    let (_dir, storage, state) = fixture().await;
    assert_eq!(get(&state, "/api/v1/health").await.status(), StatusCode::OK);
    assert_eq!(
        get(&state, "/api/v1/snapshot").await.status(),
        StatusCode::OK
    );

    for path in [
        "/api/v1/dht/nodes",
        "/api/v1/dht/nodes/0/routing",
        "/api/v1/discoveries",
        "/api/v1/discoveries/example",
        "/api/v1/hashes",
        "/api/v1/hashes/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "/api/v1/hashes/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/attempts",
        "/api/v1/jobs",
        "/api/v1/metadata",
        "/api/v1/events",
    ] {
        // 速率桶是生产资源边界；单测逐次恢复突发额度，只验证路由匹配结果。
        *state.rate.lock().unwrap() = (Instant::now(), 10.0);
        assert_eq!(
            get(&state, path).await.status(),
            StatusCode::NOT_FOUND,
            "{path}"
        );
    }

    for path in ["/api/v1/health", "/api/v1/snapshot", "/api/v1/stream"] {
        *state.rate.lock().unwrap() = (Instant::now(), 10.0);
        let response = api::router(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED, "{path}");
    }
    storage.shutdown().await.unwrap();
}

#[tokio::test]
async fn torrent_catalog_api_lists_searches_and_reads_files() {
    let (_dir, storage, state) = fixture().await;
    let hash = insert_torrent(&state).await;

    let list = get(&state, "/api/v1/torrents?limit=1").await;
    assert_eq!(list.status(), StatusCode::OK);
    let list = body_json(list).await;
    assert_eq!(list["items"][0]["hash"], hash);
    assert_eq!(list["total"], 1);
    assert_eq!(list["page"], 1);
    assert_eq!(list["index"]["complete"], true);
    assert_eq!(list["index"]["search_complete"], true);

    let search = body_json(get(&state, "/api/v1/torrents?q=AMPLE").await).await;
    assert_eq!(search["items"][0]["name"], "example-file");
    assert!(search["items"][0]["match_excerpt"].is_string());
    assert_eq!(search["total"], 1);

    let detail = body_json(get(&state, &format!("/api/v1/torrents/{hash}")).await).await;
    assert_eq!(detail["total_length"], "42");
    assert_eq!(detail["file_count"], 1);

    let files =
        body_json(get(&state, &format!("/api/v1/torrents/{hash}/files?limit=100")).await).await;
    assert_eq!(files["items"][0]["path"], "example-file");
    assert_eq!(files["items"][0]["length"], "42");

    assert_eq!(
        get(&state, "/api/v1/torrents?q=ab").await.status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        get(&state, "/api/v1/torrents?page=0").await.status(),
        StatusCode::BAD_REQUEST
    );
    // 旧游标参数不再是契约的一部分。
    assert_eq!(
        get(&state, "/api/v1/torrents?after=page2").await.status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        get(
            &state,
            "/api/v1/torrents/0000000000000000000000000000000000000000"
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );
    storage.shutdown().await.unwrap();
}
#[tokio::test(start_paused = true)]
async fn sse_replay_reset_and_last_event_id_precedence() {
    let (_dir, storage, s) = fixture().await;
    s.observer.emit(Kind::Job, "claim", "applied", || json!({}));
    let response = api::router(s.clone())
        .oneshot(
            Request::builder()
                .uri("/api/v1/stream?after=wrong:99")
                .header("Last-Event-ID", "test-run:0")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut stream = response.into_body().into_data_stream();
    assert!(
        String::from_utf8_lossy(&stream.next().await.unwrap().unwrap()).contains("event: hello")
    );
    let replay = stream.next().await.unwrap().unwrap();
    assert!(String::from_utf8_lossy(&replay).contains("id: test-run:1"));
    drop(stream);
    assert_eq!(s.streams.available_permits(), 4);
    tokio::time::advance(Duration::from_secs(901)).await;
    let mut stream = get(&s, "/api/v1/stream?after=test-run:0")
        .await
        .into_body()
        .into_data_stream();
    stream.next().await.unwrap().unwrap();
    assert!(
        String::from_utf8_lossy(&stream.next().await.unwrap().unwrap()).contains("event: reset")
    );
    assert!(stream.next().await.is_none());
    drop(stream);
    let mut stream = get(&s, "/api/v1/stream?after=old-run:1")
        .await
        .into_body()
        .into_data_stream();
    stream.next().await.unwrap().unwrap();
    assert!(
        String::from_utf8_lossy(&stream.next().await.unwrap().unwrap()).contains("event: reset")
    );
    drop(stream);
    storage.shutdown().await.unwrap();
}
#[tokio::test]
async fn stream_capacity_is_released_on_disconnect() {
    let (_dir, storage, s) = fixture().await;
    let mut bodies = Vec::new();
    for _ in 0..4 {
        let response = get(&s, "/api/v1/stream").await;
        assert_eq!(response.status(), StatusCode::OK);
        bodies.push(response.into_body());
    }
    assert_eq!(
        get(&s, "/api/v1/stream").await.status(),
        StatusCode::TOO_MANY_REQUESTS
    );
    drop(bodies);
    assert_eq!(s.streams.available_permits(), 4);
    storage.shutdown().await.unwrap();
}
#[tokio::test]
async fn actual_http_sse_shutdown_releases_listener() {
    let (_dir, storage, s) = fixture().await;
    let listener = bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut monitor = Monitor::start(listener, s.store.clone(), vec![], s.observer.clone());
    let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
    client
        .write_all(b"GET /api/v1/stream HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    let mut bytes = [0; 8192];
    let n = tokio::time::timeout(Duration::from_secs(2), client.read(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert!(String::from_utf8_lossy(&bytes[..n]).contains("200 OK"));
    monitor.begin_shutdown();
    monitor
        .state
        .observer
        .emit(Kind::Lifecycle, "shutdown", "completed", || json!({}));
    monitor
        .finish(Instant::now() + Duration::from_secs(2))
        .await;
    assert!(monitor.tasks.is_empty());
    let mut tail = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut tail))
        .await
        .unwrap()
        .unwrap();
    let listener = bind(address).await.unwrap();
    drop(listener);
    storage.shutdown().await.unwrap();
}
#[tokio::test]
async fn non_loopback_listener_is_rejected() {
    assert!(bind("0.0.0.0:0".parse().unwrap()).await.is_err());
}

#[tokio::test(start_paused = true)]
async fn continuous_events_still_publish_state_and_slow_reader_resets() {
    let (_dir, storage, s) = fixture().await;
    s.observer.emit(Kind::Job, "claim", "applied", || json!({}));
    let mut stream = get(&s, "/api/v1/stream?after=test-run:0")
        .await
        .into_body()
        .into_data_stream();
    stream.next().await.unwrap().unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    let state = stream.next().await.unwrap().unwrap();
    assert!(String::from_utf8_lossy(&state).contains("event: snapshot"));
    tokio::time::advance(Duration::from_secs(901)).await;
    s.observer.emit(Kind::Job, "claim", "applied", || json!({}));
    let reset = stream.next().await.unwrap().unwrap();
    assert!(String::from_utf8_lossy(&reset).contains("event: reset"));
    assert!(stream.next().await.is_none());
    drop(stream);
    assert_eq!(s.streams.available_permits(), 4);
    storage.shutdown().await.unwrap();
}

#[tokio::test]
async fn expired_deadline_reaps_active_and_incomplete_connections() {
    let (_dir, storage, s) = fixture().await;
    let listener = bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut monitor = Monitor::start(listener, s.store.clone(), vec![], s.observer.clone());
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream
        .write_all(b"GET /api/v1/stream HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    let mut bytes = [0; 8192];
    let count = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert!(String::from_utf8_lossy(&bytes[..count]).contains("200 OK"));
    let mut incomplete = tokio::net::TcpStream::connect(address).await.unwrap();
    incomplete
        .write_all(b"GET /api/v1/snapshot HTTP/1.1\r\n")
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while monitor.connections.lock().unwrap().len() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    monitor.finish(Instant::now()).await;
    assert!(monitor.tasks.is_empty());
    assert!(monitor.connections.lock().unwrap().is_empty());
    for client in [&mut stream, &mut incomplete] {
        let mut tail = Vec::new();
        let result = tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut tail))
            .await
            .unwrap();
        assert!(
            result.is_ok()
                || result.is_err_and(|e| e.kind() == std::io::ErrorKind::ConnectionReset)
        );
    }
    drop(bind(address).await.unwrap());
    storage.shutdown().await.unwrap();
}

#[tokio::test]
async fn incomplete_headers_expire_and_restore_connection_capacity() {
    let (_dir, storage, s) = fixture().await;
    let listener = bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let address = listener.local_addr().unwrap();
    let connections = Arc::new(Mutex::new(JoinSet::new()));
    let server = tokio::spawn(serve(listener, s.clone(), connections.clone()));
    let mut clients = Vec::new();
    for _ in 0..16 {
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        client
            .write_all(b"GET /api/v1/health HTTP/1.1\r\n")
            .await
            .unwrap();
        clients.push(client);
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        while connections.lock().unwrap().len() != 16 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::pause();
    // 让已接纳任务首次 poll，安装请求头计时器；时钟保持不动。
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(HEADER_READ_TIMEOUT - Duration::from_secs(1)).await;
    assert_eq!(connections.lock().unwrap().len(), 16);
    tokio::time::advance(Duration::from_secs(2)).await;
    for _ in 0..100 {
        if connections.lock().unwrap().is_empty() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(connections.lock().unwrap().is_empty());
    tokio::time::resume();
    for mut client in clients {
        let mut tail = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut tail))
            .await
            .unwrap()
            .unwrap();
    }
    let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
    client
        .write_all(b"GET /api/v1/health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert!(String::from_utf8_lossy(&response).contains("200 OK"));
    s.finish.cancel();
    server.await.unwrap();
    while connections.lock().unwrap().try_join_next().is_some() {}
    assert!(connections.lock().unwrap().is_empty());
    storage.shutdown().await.unwrap();
}

#[tokio::test]
async fn header_deadline_does_not_end_active_sse() {
    let (_dir, storage, s) = fixture().await;
    let listener = bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut monitor = Monitor::start(listener, s.store.clone(), vec![], s.observer.clone());
    let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
    client
        .write_all(b"GET /api/v1/stream HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    let mut bytes = [0; 8192];
    let n = tokio::time::timeout(Duration::from_secs(2), client.read(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert!(String::from_utf8_lossy(&bytes[..n]).contains("200 OK"));
    tokio::time::pause();
    tokio::time::advance(HEADER_READ_TIMEOUT + Duration::from_secs(1)).await;
    tokio::time::resume();
    monitor.state.observer.emit(
        Kind::Lifecycle,
        "after_header_deadline",
        "completed",
        || json!({}),
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut received = String::new();
        while !received.contains("after_header_deadline") {
            let n = client.read(&mut bytes).await.unwrap();
            assert!(n > 0);
            received.push_str(&String::from_utf8_lossy(&bytes[..n]));
        }
    })
    .await
    .unwrap();
    monitor.begin_shutdown();
    monitor
        .finish(Instant::now() + Duration::from_secs(1))
        .await;
    assert!(monitor.connections.lock().unwrap().is_empty());
    storage.shutdown().await.unwrap();
}

#[tokio::test]
async fn complete_v2_and_hybrid_aliases_share_catalog_and_file_contracts() {
    use crate::{collection::peer::VerifiedMetadata, info_hash::SwarmKey};
    for info in [
        include_bytes!("../collection/fixtures/v2.info").as_slice(),
        include_bytes!("../collection/fixtures/hybrid.info"),
    ] {
        let (_dir, storage, state) = fixture().await;
        let full = sha2::Sha256::digest(info);
        let key = SwarmKey(full[..20].try_into().unwrap());
        state
            .store
            .save_metadata(&VerifiedMetadata::fixture_key(info.to_vec(), key), 1)
            .await
            .unwrap();
        let v2 = crate::observation::hex(&full);
        let detail = body_json(get(&state, &format!("/api/v1/torrents/{v2}")).await).await;
        assert_eq!(detail["semantic_status"], "valid");
        assert_eq!(detail["validation_scope"], "info_only");
        let files = body_json(get(&state, &format!("/api/v1/torrents/{v2}/files")).await).await;
        assert_eq!(files["items"][0]["kind"], "file");
        assert_eq!(files["items"][1]["length"], "0");
        if detail["format"] == "hybrid" {
            let v1 = crate::observation::hex(&Sha1::digest(info));
            let alias = body_json(get(&state, &format!("/api/v1/torrents/{v1}")).await).await;
            assert_eq!(alias, detail);
            assert_eq!(detail["hash"], v1);
            assert_eq!(detail["identities"].as_array().unwrap().len(), 2);
        } else {
            assert_eq!(files["items"][0]["path"], "a");
            assert_eq!(
                get(&state, &format!("/api/v1/torrents/{}", &v2[..40]))
                    .await
                    .status(),
                StatusCode::NOT_FOUND
            );
        }
        let list = body_json(get(&state, "/api/v1/torrents").await).await;
        assert_eq!(list["total"], 1);
        storage.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn historical_detail_get_is_read_only_before_catalog_backfill() {
    let (_dir, storage, state) = fixture().await;
    let hash = insert_torrent(&state).await;
    let before = state
        .store
        .call(|c| {
            c.execute("DELETE FROM torrent_catalog", [])?;
            c.execute(
                "INSERT INTO torrent_identities SELECT 'v1',hash,id FROM metadata",
                [],
            )?;
            c.execute(
                "INSERT INTO swarm_metadata SELECT hash,id,'v1_full' FROM metadata",
                [],
            )?;
            Ok(c.total_changes())
        })
        .await
        .unwrap();
    let response = get(&state, &format!("/api/v1/torrents/{hash}")).await;
    assert_eq!(response.status(), StatusCode::OK);
    let detail = body_json(response).await;
    assert_eq!(detail["semantic_status"], "valid");
    assert_eq!(detail["identities"][0]["hash"], hash);
    let response = get(&state, &format!("/api/v1/torrents/{hash}/files")).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(body_json(response).await["available"], true);
    let after = state.store.call(|c| Ok(c.total_changes())).await.unwrap();
    assert_eq!(before, after);
    storage.shutdown().await.unwrap();
}
