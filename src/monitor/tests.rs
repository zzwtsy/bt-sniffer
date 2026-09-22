//! HTTP/SSE 资源边界和真实 socket 收尾，不启动公网。
#![cfg(test)]
use super::*;
use crate::storage::{Storage, StorageConfig};
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use futures_util::StreamExt;
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
