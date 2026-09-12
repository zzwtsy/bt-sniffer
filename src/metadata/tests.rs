//! TCP 模拟 peer 仅监听本机；测试不发现公网 peer，也不下载实际文件内容。
use super::*;
use crate::dht::routing::AddressFamily;
use crate::peer_wire::{self, MetadataMessage};
use futures_util::{SinkExt, StreamExt};
use sha1::{Digest, Sha1};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};
use tokio_util::codec::LengthDelimitedCodec;

fn config() -> MetadataConfig {
    MetadataConfig {
        address_policy: AddressPolicy::LocalUnicast,
        ..Default::default()
    }
}

/// 真实获取的原始 info 字节落盘再读回，不重新编码；损坏或冲突必须被拒绝。
#[tokio::test]
async fn verified_metadata_persistence_preserves_bytes_and_detects_corruption() {
    let listener = bind(AddressFamily::Ipv4).await.unwrap();
    let address = listener.local_addr().unwrap();
    let bytes = metadata(20000);
    let target = hash(&bytes);
    let peer = spawn_peer(listener, target, bytes.clone(), PeerBehavior::Serve);
    let result = MetadataFetcher::new(config())
        .unwrap()
        .fetch(target, &[address], &CancellationToken::new())
        .await
        .unwrap();
    peer.await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let store = crate::storage::Storage::open(crate::storage::StorageConfig::new(dir.path()))
        .await
        .unwrap();
    for _ in 0..2 {
        store.handle.save_metadata(&result, 100).await.unwrap();
    }
    assert_eq!(store.handle.metadata(target).await.unwrap(), Some(bytes));
    store
        .handle
        .call(move |c| {
            c.execute(
                "UPDATE metadata SET info=?1 WHERE hash=?2",
                rusqlite::params![b"de".as_slice(), target.0.as_slice()],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    assert!(store.handle.metadata(target).await.is_err());
    assert_eq!(
        store.handle.save_metadata(&result, 101).await,
        Err(crate::storage::StorageError::Conflict)
    );
    store.shutdown().await.unwrap();
}
fn hash(bytes: &[u8]) -> InfoHashV1 {
    InfoHashV1(Sha1::digest(bytes).into())
}
fn metadata(size: usize) -> Vec<u8> {
    let mut bytes = format!("d4:name{size}:").into_bytes();
    bytes.extend((0..size).map(|n| (n % 256) as u8));
    bytes.push(b'e');
    bytes
}
fn extension(id: u8, size: usize) -> bytes::Bytes {
    peer_wire::extended(
        0,
        format!("d1:md11:ut_metadatai{id}ee13:metadata_sizei{size}ee").as_bytes(),
    )
}
fn data_frame(piece: usize, total: usize, data: &[u8]) -> bytes::Bytes {
    let mut bytes = format!("d8:msg_typei1e5:piecei{piece}e10:total_sizei{total}ee").into_bytes();
    bytes.extend_from_slice(data);
    // 客户端声明接收 ID=1，即使服务端自己声明接收 ID=7，也必须向 1 发送数据。
    peer_wire::extended(1, &bytes)
}
async fn bind(family: AddressFamily) -> Option<TcpListener> {
    match TcpListener::bind(if family == AddressFamily::Ipv6 {
        "[::1]:0"
    } else {
        "127.0.0.1:0"
    })
    .await
    {
        Ok(listener) => Some(listener),
        Err(e)
            if family == AddressFamily::Ipv6
                && (e.kind() == std::io::ErrorKind::AddrNotAvailable
                    || matches!(e.raw_os_error(), Some(97 | 93))) =>
        {
            eprintln!("IPv6 环境不支持：{e}");
            None
        }
        Err(e) => panic!("本机监听失败：{e}"),
    }
}
/// 拆分标准握手，并将其尾部和首个帧连写，检查切换到 Codec 时不会丢字节。
async fn greet(
    mut socket: TcpStream,
    target: InfoHashV1,
    size: usize,
) -> tokio_util::codec::Framed<TcpStream, LengthDelimitedCodec> {
    let mut request = [0; 68];
    socket.read_exact(&mut request).await.unwrap();
    assert!(
        peer_wire::parse_handshake(&request, target)
            .unwrap()
            .supports_extensions
    );
    let response = peer_wire::handshake(target, PeerId([7; 20]));
    socket.write_all(&response[..31]).await.unwrap();
    let frame = extension(7, size);
    let mut tail = response[31..].to_vec();
    tail.extend_from_slice(&(frame.len() as u32).to_be_bytes());
    tail.extend_from_slice(&frame);
    socket.write_all(&tail).await.unwrap();
    let mut stream = LengthDelimitedCodec::builder().new_framed(socket);
    let hello = stream.next().await.unwrap().unwrap();
    assert_eq!(&hello[..2], &[20, 0]);
    assert_eq!(
        peer_wire::parse_extension(&hello[2..], 4096, 64)
            .unwrap()
            .metadata_id,
        Some(1)
    );
    stream
}
/// 模拟远端明确选择正常提供分片或拒绝请求，不改变实际协议报文格式。
#[derive(Clone, Copy, PartialEq, Eq)]
enum PeerBehavior {
    Serve,
    Reject,
}

fn spawn_peer(
    listener: TcpListener,
    target: InfoHashV1,
    bytes: Vec<u8>,
    behavior: PeerBehavior,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(3), async {
            let (socket, _) = listener.accept().await.unwrap();
            let mut stream = greet(socket, target, bytes.len()).await;
            let count = bytes.len().div_ceil(BLOCK_SIZE);
            let mut done = 0;
            while done < count {
                let batch = (count - done).min(4);
                let mut requests = Vec::new();
                for _ in 0..batch {
                    let frame = stream.next().await.unwrap().unwrap();
                    assert_eq!(&frame[..2], &[20, 7]);
                    let MetadataMessage::Request { piece } =
                        peer_wire::parse_metadata(&frame[2..], 4096, 64).unwrap()
                    else {
                        panic!("应该收到分片请求")
                    };
                    requests.push(piece);
                }
                if behavior == PeerBehavior::Reject {
                    stream
                        .send(peer_wire::metadata_control(1, 2, requests[0]))
                        .await
                        .unwrap();
                    return;
                }
                // 先发 keepalive、choke 和陌生扩展，再乱序发送数据，不要求客户端等待 unchoke。
                for ignored in [
                    bytes::Bytes::new(),
                    bytes::Bytes::from_static(&[0]),
                    peer_wire::extended(55, b"future"),
                ] {
                    stream.send(ignored).await.unwrap();
                }
                for piece in requests.into_iter().rev() {
                    let start = piece * BLOCK_SIZE;
                    let end = (start + BLOCK_SIZE).min(bytes.len());
                    stream
                        .send(data_frame(piece, bytes.len(), &bytes[start..end]))
                        .await
                        .unwrap();
                }
                done += batch;
            }
        })
        .await
        .expect("模拟 peer 不应无限等待");
    })
}

/// 多片数据含任意二进制字节，拆包、粘包、乱序和双向不同扩展 ID 后仍原样返回。
#[tokio::test]
async fn ipv4_metadata_roundtrip_preserves_raw_dictionary() {
    let listener = bind(AddressFamily::Ipv4).await.unwrap();
    let address = listener.local_addr().unwrap();
    let bytes = metadata(50000);
    let target = hash(&bytes);
    let server = spawn_peer(listener, target, bytes.clone(), PeerBehavior::Serve);
    let fetcher = MetadataFetcher::new(config()).unwrap();
    let result = fetcher
        .fetch(target, &[address], &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(result.info(), bytes);
    assert_eq!(result.info_hash(), target);
    assert_eq!(result.source(), address);
    assert_eq!(result.peer_id(), PeerId([7; 20]));
    server.await.unwrap();
}
/// IPv6 同样走 TCP peer-wire，不使用 DHT 节点 UDP 端口做隐式转换。
#[tokio::test]
async fn ipv6_metadata_roundtrip() {
    let Some(listener) = bind(AddressFamily::Ipv6).await else {
        return;
    };
    let address = listener.local_addr().unwrap();
    let bytes = b"de".to_vec();
    let server = spawn_peer(listener, hash(&bytes), bytes.clone(), PeerBehavior::Serve);
    let result = MetadataFetcher::new(config())
        .unwrap()
        .fetch(hash(&bytes), &[address], &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(result.info(), bytes);
    server.await.unwrap();
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
    let result = MetadataFetcher::new(config())
        .unwrap()
        .fetch(target, &[aa, aa, ba], &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(result.source(), ba);
    assert_eq!(result.info(), bytes);
    first.await.unwrap();
    second.await.unwrap();
}

/// 字节 hash 不匹配、根不是字典、字典后有尾随内容，即使传输完整也不能返回 VerifiedMetadata。
#[tokio::test]
async fn final_verification_rejects_untrusted_metadata() {
    for (bytes, target) in [
        (b"de".to_vec(), InfoHashV1([0; 20])),
        (b"li1ee".to_vec(), hash(b"li1ee")),
        (b"dejunk".to_vec(), hash(b"dejunk")),
    ] {
        let listener = bind(AddressFamily::Ipv4).await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = spawn_peer(listener, target, bytes, PeerBehavior::Serve);
        let error = MetadataFetcher::new(config())
            .unwrap()
            .fetch(target, &[address], &CancellationToken::new())
            .await
            .unwrap_err();
        let MetadataError::AllPeersFailed(errors) = error else {
            panic!("应有 peer 失败详情")
        };
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].address, address);
        assert!(matches!(
            errors[0].error,
            PeerError::HashMismatch | PeerError::Protocol(_)
        ));
        server.await.unwrap();
    }
}

/// SHA-1 固定向量独立于会话编解码；这里只验证协议摘要，不把 SHA-1 当作新密码方案。
#[test]
fn sha1_known_vector_and_configuration_errors() {
    let digest = hash(b"abc");
    let hex: String = digest.0.iter().map(|b| format!("{b:02x}")).collect();
    assert_eq!(hex, "a9993e364706816aba3e25717850c26c9cd0d89d");
    for invalid in [
        MetadataConfig {
            concurrency: 0,
            ..config()
        },
        MetadataConfig {
            request_window: 0,
            ..config()
        },
        MetadataConfig {
            max_frame_size: 100,
            ..config()
        },
        MetadataConfig {
            task_timeout: Duration::MAX,
            ..config()
        },
    ] {
        assert!(matches!(
            MetadataFetcher::new(invalid),
            Err(MetadataError::InvalidConfig)
        ));
    }
    assert!(matches!(
        MetadataFetcher::with_entropy(config(), |_| Err(MetadataError::Entropy)),
        Err(MetadataError::Entropy)
    ));
}

/// 默认公网策略拒绝本机；任务开始前取消不应连接网络或占用并发槽位。
#[tokio::test]
async fn address_policy_and_pre_cancelled_tasks() {
    let fetcher = MetadataFetcher::new(MetadataConfig::default()).unwrap();
    let token = CancellationToken::new();
    assert!(matches!(
        fetcher
            .fetch(hash(b"de"), &["127.0.0.1:1".parse().unwrap()], &token)
            .await,
        Err(MetadataError::NoUsablePeers)
    ));
    token.cancel();
    assert!(matches!(
        fetcher.fetch(hash(b"de"), &[], &token).await,
        Err(MetadataError::Cancelled)
    ));
    assert_eq!(fetcher.permits.available_permits(), 8);
}

/// 握手卡住时可取消；克隆 Fetcher 共用名额，取消后 socket 和名额都必须释放。
#[tokio::test]
// 握手写出信号确认连接已被占用；回收任务后检查许可，并等待服务端 EOF 证明 socket 关闭。
async fn cancellation_and_dropping_future_release_connections_and_permits() {
    for abort in [false, true] {
        let listener = bind(AddressFamily::Ipv4).await.unwrap();
        let address = listener.local_addr().unwrap();
        let (ready, started) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = [0; 68];
            socket.read_exact(&mut bytes).await.unwrap();
            ready.send(()).unwrap();
            let mut byte = [0];
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), socket.read(&mut byte))
                    .await
                    .unwrap()
                    .unwrap(),
                0,
                "取消后应关闭 socket"
            );
        });
        let fetcher = MetadataFetcher::new(MetadataConfig {
            concurrency: 1,
            ..config()
        })
        .unwrap();
        let worker = fetcher.clone();
        let token = CancellationToken::new();
        let worker_token = token.clone();
        let task =
            tokio::spawn(async move { worker.fetch(hash(b"de"), &[address], &worker_token).await });
        started.await.unwrap();
        assert!(matches!(
            fetcher
                .fetch(hash(b"de"), &[address], &CancellationToken::new())
                .await,
            Err(MetadataError::AtCapacity)
        ));
        if abort {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        } else {
            token.cancel();
            assert!(matches!(task.await.unwrap(), Err(MetadataError::Cancelled)));
        }
        assert_eq!(fetcher.permits.available_permits(), 1);
        server.await.unwrap();
    }
}

/// 暂停时钟后，标准握手、单 peer 总期限和任务总期限各自生效，不需要真实等待。
#[tokio::test]
async fn handshake_peer_and_task_deadlines_are_absolute() {
    for (peer_timeout, task_timeout, expected) in [(30, 120, 0), (1, 120, 1), (30, 1, 2)] {
        let listener = bind(AddressFamily::Ipv4).await.unwrap();
        let address = listener.local_addr().unwrap();
        let (ready, started) = tokio::sync::oneshot::channel();
        let stop = CancellationToken::new();
        let server_stop = stop.clone();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = [0; 68];
            socket.read_exact(&mut bytes).await.unwrap();
            ready.send(()).unwrap();
            server_stop.cancelled().await;
        });
        let fetcher = MetadataFetcher::new(MetadataConfig {
            peer_timeout: Duration::from_secs(peer_timeout),
            task_timeout: Duration::from_secs(task_timeout),
            ..config()
        })
        .unwrap();
        let task = tokio::spawn(async move {
            fetcher
                .fetch(hash(b"de"), &[address], &CancellationToken::new())
                .await
        });
        started.await.unwrap();
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(if expected == 0 { 6 } else { 2 })).await;
        let error = task.await.unwrap().unwrap_err();
        tokio::time::resume();
        match (expected, error) {
            (0, MetadataError::AllPeersFailed(errors)) => assert!(matches!(
                errors[0].error,
                PeerError::Timeout(Stage::Handshake)
            )),
            (1, MetadataError::AllPeersFailed(errors)) => {
                assert!(matches!(errors[0].error, PeerError::Timeout(Stage::Peer)))
            }
            (2, MetadataError::TaskTimeout) => {}
            other => panic!("错误期限结果：{other:?}"),
        }
        stop.cancel();
        server.await.unwrap();
    }
}

/// ID 增量更新只影响之后发出的请求；本地尚未验证完整 metadata 时，必须拒绝上传请求。
#[tokio::test]
async fn extension_id_updates_and_inbound_requests_follow_bep10() {
    let bytes = metadata(BLOCK_SIZE + 30);
    let target = hash(&bytes);
    let expected = bytes.clone();
    let listener = bind(AddressFamily::Ipv4).await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut stream = greet(socket, target, bytes.len()).await;
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first[1], 7);
        stream
            .send(peer_wire::metadata_control(1, 0, 0))
            .await
            .unwrap();
        let reject = stream.next().await.unwrap().unwrap();
        assert_eq!(reject[1], 7);
        assert!(matches!(
            peer_wire::parse_metadata(&reject[2..], 4096, 64).unwrap(),
            MetadataMessage::Reject { piece: 0 }
        ));
        stream
            .send(peer_wire::extended(0, b"d1:md11:ut_metadatai9eee"))
            .await
            .unwrap();
        stream
            .send(data_frame(0, bytes.len(), &bytes[..BLOCK_SIZE]))
            .await
            .unwrap();
        let next = stream.next().await.unwrap().unwrap();
        assert_eq!(next[1], 9, "发送采用更新后的远端 ID");
        stream
            .send(data_frame(1, bytes.len(), &bytes[BLOCK_SIZE..]))
            .await
            .unwrap();
    });
    let result = MetadataFetcher::new(MetadataConfig {
        request_window: 1,
        ..config()
    })
    .unwrap()
    .fetch(target, &[address], &CancellationToken::new())
    .await
    .unwrap();
    assert_eq!(result.info(), expected);
    server.await.unwrap();
}

/// 超大长度声明在读取正文前被拒绝；零长度帧也受累计帧数预算限制。
#[tokio::test]
async fn frame_and_receive_budgets_are_enforced() {
    for oversize in [false, true] {
        let listener = bind(AddressFamily::Ipv4).await.unwrap();
        let address = listener.local_addr().unwrap();
        let target = hash(b"de");
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut stream = greet(socket, target, 2).await;
            stream.next().await.unwrap().unwrap();
            if oversize {
                stream
                    .get_mut()
                    .write_all(&100000_u32.to_be_bytes())
                    .await
                    .unwrap();
            } else {
                for _ in 0..5 {
                    stream.send(bytes::Bytes::new()).await.unwrap();
                }
            }
        });
        let config = MetadataConfig {
            max_received_frames: 3,
            ..config()
        };
        let error = MetadataFetcher::new(config)
            .unwrap()
            .fetch(target, &[address], &CancellationToken::new())
            .await
            .unwrap_err();
        let MetadataError::AllPeersFailed(errors) = error else {
            panic!("应按 peer 返回预算错误")
        };
        assert!(matches!(errors[0].error, PeerError::Limit(_)));
        server.await.unwrap();
    }
}

/// metadata_size 改变、显式禁用和损坏分片必须失败，不在原连接中重新分配并重试。
#[tokio::test]
async fn changed_size_disabled_extension_and_corrupt_piece_fail() {
    for case in 0..3 {
        let listener = bind(AddressFamily::Ipv4).await.unwrap();
        let address = listener.local_addr().unwrap();
        let target = hash(b"de");
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut stream = greet(socket, target, 2).await;
            stream.next().await.unwrap().unwrap();
            let message = match case {
                0 => extension(7, 3),
                1 => extension(0, 2),
                _ => data_frame(0, 2, b"x"),
            };
            stream.send(message).await.unwrap();
        });
        let error = MetadataFetcher::new(config())
            .unwrap()
            .fetch(target, &[address], &CancellationToken::new())
            .await
            .unwrap_err();
        let MetadataError::AllPeersFailed(errors) = error else {
            panic!("应按 peer 返回协议错误")
        };
        if case == 1 {
            assert!(matches!(errors[0].error, PeerError::Unsupported));
        } else {
            assert!(matches!(errors[0].error, PeerError::Protocol(_)));
        }
        assert_eq!(errors[0].stage, Stage::Piece);
        server.await.unwrap();
    }
}

/// 持续发送 keepalive 不能给已经发出的分片请求续命；取消在分片阶段同样立即生效。
#[tokio::test]
async fn piece_deadline_and_piece_stage_cancellation() {
    for cancel in [false, true] {
        let listener = bind(AddressFamily::Ipv4).await.unwrap();
        let address = listener.local_addr().unwrap();
        let target = hash(b"de");
        let (ready, started) = tokio::sync::oneshot::channel();
        let (send_keepalive, mut signals) =
            tokio::sync::mpsc::channel::<tokio::sync::oneshot::Sender<()>>(1);
        let stop = CancellationToken::new();
        let server_stop = stop.clone();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut stream = greet(socket, target, 2).await;
            stream.next().await.unwrap().unwrap();
            ready.send(()).unwrap();
            loop {
                tokio::select! {
                    _=server_stop.cancelled()=>break,
                    signal = signals.recv() => {
                        let Some(reply) = signal else { break };
                        stream.send(bytes::Bytes::new()).await.unwrap();
                        let _ = reply.send(());
                    }
                }
            }
        });
        let token = CancellationToken::new();
        let worker_token = token.clone();
        let fetcher = MetadataFetcher::new(config()).unwrap();
        let worker = fetcher.clone();
        let task =
            tokio::spawn(async move { worker.fetch(target, &[address], &worker_token).await });
        started.await.unwrap();
        if cancel {
            token.cancel();
            assert!(matches!(task.await.unwrap(), Err(MetadataError::Cancelled)));
        } else {
            tokio::time::pause();
            for _ in 0..3 {
                tokio::time::advance(Duration::from_secs(3)).await;
                let (sent, done) = tokio::sync::oneshot::channel();
                send_keepalive.send(sent).await.unwrap();
                done.await.unwrap();
                tokio::task::yield_now().await;
            }
            tokio::time::advance(Duration::from_secs(2)).await;
            let error = task.await.unwrap().unwrap_err();
            tokio::time::resume();
            let MetadataError::AllPeersFailed(errors) = error else {
                panic!("应该是分片期限错误")
            };
            assert!(matches!(errors[0].error, PeerError::Timeout(Stage::Piece)));
        }
        stop.cancel();
        server.await.unwrap();
        assert_eq!(fetcher.permits.available_permits(), 8);
    }
}

/// 数据帧的每个字节都可以分开到达；metadata 恰好整除 16 KiB 时不应多请求一片。
#[tokio::test]
async fn bytewise_frames_and_exact_block_size() {
    let bytes = metadata(16370);
    assert_eq!(bytes.len(), BLOCK_SIZE);
    let target = hash(&bytes);
    let expected = bytes.clone();
    let listener = bind(AddressFamily::Ipv4).await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut stream = greet(socket, target, bytes.len()).await;
        stream.next().await.unwrap().unwrap();
        let frame = data_frame(0, bytes.len(), &bytes);
        let mut packet = (frame.len() as u32).to_be_bytes().to_vec();
        packet.extend_from_slice(&frame);
        for byte in &packet[..20] {
            stream.get_mut().write_all(&[*byte]).await.unwrap();
            tokio::task::yield_now().await;
        }
        stream.get_mut().write_all(&packet[20..]).await.unwrap();
        // 客户端收齐一片后关闭连接，不再请求并不存在的第二片。
        assert!(
            tokio::time::timeout(Duration::from_secs(2), stream.next())
                .await
                .unwrap()
                .is_none()
        );
    });
    let result = MetadataFetcher::new(config())
        .unwrap()
        .fetch(target, &[address], &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(result.info(), expected);
    server.await.unwrap();
}

/// metadata 缺少 size 时等待补充握手，未知字段不会清空已经协商好的扩展 ID。
#[tokio::test]
async fn optional_handshake_fields_can_arrive_later() {
    let listener = bind(AddressFamily::Ipv4).await.unwrap();
    let address = listener.local_addr().unwrap();
    let target = hash(b"de");
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0; 68];
        socket.read_exact(&mut request).await.unwrap();
        socket
            .write_all(&peer_wire::handshake(target, PeerId([7; 20])))
            .await
            .unwrap();
        let mut stream = LengthDelimitedCodec::builder().new_framed(socket);
        stream.next().await.unwrap().unwrap();
        stream
            .send(peer_wire::extended(0, b"d1:md11:ut_metadatai7eee"))
            .await
            .unwrap();
        stream
            .send(peer_wire::extended(0, b"d13:metadata_sizei2ee"))
            .await
            .unwrap();
        let request = stream.next().await.unwrap().unwrap();
        assert_eq!(request[1], 7);
        stream.send(data_frame(0, 2, b"de")).await.unwrap();
    });
    let result = MetadataFetcher::new(config())
        .unwrap()
        .fetch(target, &[address], &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(result.info(), b"de");
    server.await.unwrap();
}

/// 只尝试配置允许的不同地址数，不能因为首 peer 拒绝就无限向列表后面扩散。
#[tokio::test]
async fn attempt_limit_stops_before_next_peer() {
    let first = bind(AddressFamily::Ipv4).await.unwrap();
    let second = bind(AddressFamily::Ipv4).await.unwrap();
    let a = first.local_addr().unwrap();
    let b = second.local_addr().unwrap();
    let target = hash(b"de");
    let server = spawn_peer(first, target, b"de".to_vec(), PeerBehavior::Reject);
    let error = MetadataFetcher::new(MetadataConfig {
        max_peer_attempts: 1,
        ..config()
    })
    .unwrap()
    .fetch(target, &[a, a, b], &CancellationToken::new())
    .await
    .unwrap_err();
    let MetadataError::AllPeersFailed(errors) = error else {
        panic!("应报告尝试的 peer 失败")
    };
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].stage, Stage::Piece);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), second.accept())
            .await
            .is_err()
    );
    server.await.unwrap();
}
