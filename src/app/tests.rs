//! 从应用入口验证启动、地址族和关闭；默认用临时目录，公网长时间场景单独忽略。
use super::*;
use crate::net::udp::UdpTransportConfig;
use clap::Parser;

fn local_cli(dir: &std::path::Path) -> Cli {
    Cli::try_parse_from([
        "bt-sniffer",
        "--ipv4-only",
        "--listen-v4",
        "127.0.0.1:0",
        "--allow-local",
        "--no-bootstrap",
        "--state-dir",
        dir.to_str().unwrap(),
    ])
    .unwrap()
}

/// 应用正常退出后，原状态目录可以重新打开，证明数据库和目录锁已释放。
#[tokio::test]
async fn graceful_shutdown_releases_state_directory() {
    // 同一个目录连续启动两次；首次关闭必须真正释放数据库连接和实例锁。
    let dir = tempfile::tempdir().unwrap();
    for _ in 0..2 {
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let app = tokio::spawn(run(local_cli(dir.path()), async {
            let _ = stopped.await;
        }));
        tokio::time::sleep(Duration::from_millis(100)).await;
        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), app)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(dir.path().join("state.sqlite3").exists());
        assert!(!dir.path().join("state.sqlite3-wal").exists());
        assert!(!dir.path().join("state.sqlite3-shm").exists());
    }
    let storage = crate::storage::Storage::open(StorageConfig::new(dir.path()))
        .await
        .unwrap();
    storage.shutdown().await.unwrap();
}

/// 启动时退出信号已经就绪，不应创建数据库文件。
#[tokio::test]
async fn already_cancelled_startup_does_not_create_database() {
    // 启动前就收到退出请求时，不留下半初始化数据库。
    let dir = tempfile::tempdir().unwrap();
    run(local_cli(dir.path()), async {}).await.unwrap();
    assert!(!dir.path().join("state.sqlite3").exists());
}

/// 后续启动步骤失败时，此前创建的会话资源仍须完整关闭。
#[tokio::test]
async fn partial_startup_failure_is_cleaned_up() {
    // 第二个同族节点与第一个持久化身份冲突；仍然必须关闭已经启动的节点。
    let dir = tempfile::tempdir().unwrap();
    let mut session = PersistentSession::open(StorageConfig::new(dir.path()))
        .await
        .unwrap();
    let mut handles = Vec::new();
    let mut sockets = Vec::new();
    for _ in 0..2 {
        sockets.push(
            UdpTransport::bind("127.0.0.1:0", UdpTransportConfig::default())
                .await
                .unwrap(),
        );
    }
    assert!(
        start_nodes(&mut session, &local_cli(dir.path()), sockets, &mut handles)
            .await
            .is_err()
    );
    session.shutdown().await.unwrap();
    let storage = crate::storage::Storage::open(StorageConfig::new(dir.path()))
        .await
        .unwrap();
    storage.shutdown().await.unwrap();
}

/// IPv6 回退仅适用于环境不支持，不得吞掉权限或端口占用错误。
#[test]
fn ipv6_fallback_does_not_hide_configuration_errors() {
    // 端口占用和权限不足不能被当成“机器不支持 IPv6”。
    for kind in [
        std::io::ErrorKind::PermissionDenied,
        std::io::ErrorKind::AddrInUse,
        std::io::ErrorKind::InvalidInput,
    ] {
        assert!(!sockets::ipv6_unavailable(&std::io::Error::from(kind)));
    }
    assert!(sockets::ipv6_unavailable(&std::io::Error::from(
        std::io::ErrorKind::AddrNotAvailable
    )));
}

/// 用户明确指定 IPv6 监听时，绑定失败必须向调用者报告。
#[tokio::test]
async fn explicit_ipv6_bind_failure_is_not_ignored() {
    // 显式配置即使是系统不支持的地址，也必须返回错误。
    let cli = Cli::try_parse_from([
        "bt-sniffer",
        "--listen-v4",
        "127.0.0.1:0",
        "--listen-v6",
        "[2001:db8::1234]:0",
    ])
    .unwrap();
    assert!(sockets::bind(&cli).is_err());
}

/// 两种地址族独立绑定，IPv6 socket 不能抢占 IPv4 的收包职责。
#[tokio::test]
async fn independent_dual_stack_sockets() {
    // IPv6 先绑定随机端口，再把 IPv4 绑定到相同数字端口，验证 v6-only 设置。
    let cli = Cli::try_parse_from(["bt-sniffer", "--ipv6-only", "--listen-v6", "[::]:0"]).unwrap();
    let probe = match tokio::net::UdpSocket::bind("[::1]:0").await {
        Ok(probe) => probe,
        Err(error) if sockets::ipv6_unavailable(&error) => return,
        Err(error) => panic!("IPv6 环境探测失败：{error}"),
    };
    drop(probe);
    let v6 = sockets::bind(&cli).unwrap();
    let port = v6[0].local_addr().unwrap().port();
    let cli = Cli::try_parse_from([
        "bt-sniffer",
        "--ipv4-only",
        "--listen-v4",
        &format!("0.0.0.0:{port}"),
    ])
    .unwrap();
    let v4 = sockets::bind(&cli).unwrap();
    assert_eq!(v4[0].local_addr().unwrap().port(), port);
}

/// 显式的公网互操作验收；不会随默认测试运行，状态保留在 /tmp 便于审计。
#[tokio::test]
#[ignore = "访问公共 DHT 并运行 2 小时"]
async fn public_collection_two_hours() {
    use sha1::{Digest, Sha1};
    // 在创建数据库之前注册，与正式程序共用正常退出路径。
    let signal = shutdown_signal().expect("无法注册退出信号");
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_writer(std::io::stderr)
        .try_init();
    let dir = tempfile::Builder::new()
        .prefix("bt-sniffer-public-acceptance-")
        .tempdir()
        .unwrap()
        .keep();
    eprintln!("public acceptance state: {}", dir.display());
    let cli = Cli::try_parse_from([
        "bt-sniffer",
        "--state-dir",
        dir.to_str().unwrap(),
        "--sample",
        "--fetch",
    ])
    .unwrap();
    let budget = std::sync::Arc::new(crate::dht::traffic::Budget::new(cli.traffic()).unwrap());
    let mut report = crate::acceptance::Report::new(
        "public-two-hours",
        &dir,
        serde_json::json!({"dual_stack":true,"sample":true,"fetch":true,"queries_per_sec":20,"inbound_per_sec":200,"upload_payload_bytes_per_sec":262144,"planned_seconds":7200}),
    );
    report.running();
    let timer = async {
        for minute in 1..=120 {
            tokio::time::sleep(Duration::from_secs(60)).await;
            #[cfg(target_os = "linux")]
            if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
                let rss = status
                    .lines()
                    .find(|line| line.starts_with("VmRSS:"))
                    .unwrap_or("VmRSS unavailable");
                eprintln!("public acceptance minute={minute} {rss}");
            }
        }
    };
    let mut completed = false;
    let shutdown = async {
        tokio::select! {
            biased;
            _ = signal => eprintln!("公网验收提前停止，正在保存状态并关闭数据库"),
            _ = timer => completed = true,
        }
    };
    let result = run_with_budget(cli, shutdown, budget.clone()).await;
    report.value["run_errors"] = serde_json::json!(result.as_ref().err());
    let traffic = budget.snapshot();
    report.value["families"] = serde_json::json!({"ipv4":{"validated_dht_responses":traffic.validated_v4},"ipv6":{"validated_dht_responses":traffic.validated_v6},"metadata_family":"not_attributable_in_schema_v2"});
    report.value["statistics"]["dht"] = serde_json::json!(traffic);
    if let Err(errors) = result {
        report.value["status"] = serde_json::json!(if errors.iter().any(|e| {
            let e = e.to_ascii_lowercase();
            [
                "permission denied",
                "operation not permitted",
                "address already in use",
                "cannot assign requested address",
                "network is unreachable",
            ]
            .iter()
            .any(|reason| e.contains(reason))
        }) {
            "environment_blocked"
        } else {
            "failed"
        });
        panic!("{errors:?}");
    }
    let storage = crate::storage::Storage::open(StorageConfig::new(&dir))
        .await
        .unwrap();
    let stats = storage.handle.fetch_stats().await.unwrap();
    report.value["statistics"]["storage"] = serde_json::json!(stats);
    eprintln!("public acceptance final: {stats:?}");
    let count = storage
        .handle
        .call(|c| {
            let integrity: String = c.query_row("PRAGMA integrity_check", [], |r| r.get(0))?;
            assert_eq!(integrity, "ok");
            let mut q = c.prepare("SELECT hash,info FROM metadata")?;
            let mut rows = q.query([])?;
            let mut count = 0;
            while let Some(row) = rows.next()? {
                let hash: Vec<u8> = row.get(0)?;
                let info: Vec<u8> = row.get(1)?;
                assert_eq!(Sha1::digest(&info).as_slice(), hash.as_slice());
                assert_eq!(
                    crate::peer_wire::dictionary_prefix(&info, 64).unwrap(),
                    info.as_slice()
                );
                count += 1;
            }
            Ok(count)
        })
        .await
        .unwrap();
    storage.shutdown().await.unwrap();
    report.value["verification"] = serde_json::json!([
        "normal_shutdown",
        "sqlite_integrity_check",
        "all_metadata_sha1",
        "all_metadata_complete_dictionary"
    ]);
    report.finish(completed, count > 0);
    assert!(completed, "公网验收已正常提前停止，未完成两小时验收");
    assert!(count > 0, "公网两小时没有获取 metadata，互操作闭环尚未通过");
}
