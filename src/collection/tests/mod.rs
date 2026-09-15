//! 通过本机 UDP/TCP 与临时数据库验证发现到入库的闭环，以及故障、取消和恢复。
use super::{
    worker::{Outcome, run_job},
    *,
};
use crate::app::session::FaultLog;
use crate::app::session::FaultReporter;
use crate::app::session::Session;
use crate::app::session::SessionFault;
use crate::collection::peer::VerifiedMetadata;
use crate::collection::peer::wire as peer_wire;
use crate::collection::peer::wire::PeerId;
use crate::dht::NodeId;
use crate::dht::dispatcher::DhtDispatcherConfig;
use crate::dht::dispatcher::RemoteNode;
use crate::dht::krpc::CompactNodesV4;
use crate::dht::krpc::CompactNodesV6;
use crate::dht::krpc::CompactPeerAddress;
use crate::dht::krpc::InfoHashSamples;
use crate::dht::krpc::KrpcMessage;
use crate::dht::krpc::MessageType;
use crate::dht::krpc::QueryArgs;
use crate::dht::krpc::QueryMethod;
use crate::dht::krpc::ResponseArgs;
use crate::dht::krpc::Token;
use crate::dht::routing::AddressFamily;
use crate::dht::transaction::TransactionManager;
use crate::dht::udp::UdpTransport;
use crate::info_hash::InfoHashV1;
use crate::storage::StorageConfig;
use futures_util::{FutureExt, SinkExt, StreamExt};
use serde_bytes::ByteBuf;
use sha1::{Digest, Sha1};
use std::net::SocketAddr;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tokio_util::codec::LengthDelimitedCodec;

fn now() -> Result<i64, StorageError> {
    Ok(unix_millis(std::time::SystemTime::now())?)
}

const INFO: &[u8] = b"d4:name4:test6:pieces0:e";
fn hash() -> InfoHashV1 {
    InfoHashV1(Sha1::digest(INFO).into())
}
fn config(dir: &std::path::Path) -> Config {
    Config {
        metadata: MetadataConfig {
            address_policy: AddressPolicy::LocalUnicast,
            ..Default::default()
        },
        concurrency: 2,
        max_active: 16,
        sample_backpressure: SampleBackpressure::Capacity,
        state_max_bytes: 1024 * 1024 * 1024,
        directory: dir.into(),
        policy: AddressPolicy::LocalUnicast,
    }
}
async fn udp(family: AddressFamily) -> UdpTransport {
    UdpTransport::bind(
        if family == AddressFamily::Ipv6 {
            "[::1]:0"
        } else {
            "127.0.0.1:0"
        },
        Default::default(),
    )
    .await
    .unwrap()
}
async fn tcp(family: AddressFamily) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    tcp_info(family, INFO.to_vec()).await
}
async fn tcp_info(
    family: AddressFamily,
    info: Vec<u8>,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    tcp_info_with_order(family, info, false).await
}
async fn tcp_info_with_order(
    family: AddressFamily,
    info: Vec<u8>,
    compatible: bool,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let info_hash = InfoHashV1(Sha1::digest(&info).into());
    let listener = TcpListener::bind(if family == AddressFamily::Ipv6 {
        "[::1]:0"
    } else {
        "127.0.0.1:0"
    })
    .await
    .unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut hello = [0; 68];
        socket.read_exact(&mut hello).await.unwrap();
        peer_wire::parse_handshake(&hello, info_hash).unwrap();
        socket
            .write_all(&peer_wire::handshake(info_hash, PeerId([7; 20])))
            .await
            .unwrap();
        let mut stream = LengthDelimitedCodec::builder().new_framed(socket);
        stream.next().await.unwrap().unwrap();
        stream
            .send(peer_wire::extended(
                0,
                if compatible {
                    format!("d13:metadata_sizei{}e1:md11:ut_metadatai7eee", info.len())
                } else {
                    format!("d1:md11:ut_metadatai7ee13:metadata_sizei{}ee", info.len())
                }
                .as_bytes(),
            ))
            .await
            .unwrap();
        let request = stream.next().await.unwrap().unwrap();
        assert_eq!(&request[..2], &[20, 7]);
        let mut body =
            format!("d8:msg_typei1e5:piecei0e10:total_sizei{}ee", info.len()).into_bytes();
        body.extend_from_slice(&info);
        stream.send(peer_wire::extended(1, &body)).await.unwrap();
    });
    (address, task)
}
fn response(
    t: ByteBuf,
    family: AddressFamily,
    peer: Option<SocketAddr>,
    method: QueryMethod,
) -> KrpcMessage {
    KrpcMessage {
        t,
        y: MessageType::Response,
        q: None,
        a: None,
        e: None,
        ro: None,
        r: Some(ResponseArgs {
            id: NodeId([8; 20]),
            token: Some(Token(ByteBuf::from(b"token".to_vec()))),
            nodes: (family == AddressFamily::Ipv4).then(|| CompactNodesV4(vec![])),
            nodes6: (family == AddressFamily::Ipv6).then(|| CompactNodesV6(vec![])),
            values: peer.map(|a| {
                vec![match a {
                    SocketAddr::V4(a) => CompactPeerAddress::V4(a),
                    SocketAddr::V6(a) => CompactPeerAddress::V6(a),
                }]
            }),
            samples: (method == QueryMethod::SampleInfohashes)
                .then(|| InfoHashSamples(vec![hash()])),
            interval: (method == QueryMethod::SampleInfohashes).then_some(60),
            num: (method == QueryMethod::SampleInfohashes).then_some(1),
        }),
    }
}
/// 会话拥有节点与数据库；测试保留 handle 发命令，address 供模拟远端访问。
struct Fixture {
    session: Session,
    handle: DhtHandle,
    address: SocketAddr,
}

async fn fixture(dir: &std::path::Path, family: AddressFamily) -> Fixture {
    fixture_observed(dir, family, Default::default()).await
}
async fn fixture_observed(
    dir: &std::path::Path,
    family: AddressFamily,
    observer: crate::observation::Observer,
) -> Fixture {
    let mut session = Session::open_observed(StorageConfig::new(dir), Arc::default(), observer)
        .await
        .unwrap();
    let transport = udp(family).await;
    let address = transport.local_addr().unwrap();
    let mut cfg = DhtDispatcherConfig::default();
    cfg.maintenance.enabled = false;
    cfg.peer_store.address_policy = AddressPolicy::LocalUnicast;
    let handle = session
        .add_node(
            "test",
            transport,
            TransactionManager::new(Duration::from_secs(1), 32),
            cfg,
            AddressPolicy::LocalUnicast,
        )
        .await
        .unwrap();
    Fixture {
        session,
        handle,
        address,
    }
}
async fn await_metadata(store: &CollectionStore) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(bytes) = store.metadata(hash()).await.unwrap() {
                assert_eq!(bytes, INFO);
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("闭环必须自动入库");
    assert_eq!(store.fetch_stats().await.unwrap().succeeded, 1);
}

fn announce_query(token: Option<Token>, port: u16) -> KrpcMessage {
    KrpcMessage {
        t: ByteBuf::from(b"announce-test".to_vec()),
        y: MessageType::Query,
        q: Some(if token.is_some() {
            QueryMethod::AnnouncePeer
        } else {
            QueryMethod::GetPeers
        }),
        a: Some(QueryArgs {
            id: NodeId([4; 20]),
            target: None,
            info_hash: Some(hash()),
            port: token.as_ref().map(|_| port),
            token,
            implied_port: None,
            want: vec![],
        }),
        r: None,
        e: None,
        ro: Some(1),
    }
}

async fn verified() -> VerifiedMetadata {
    let (peer, task) = tcp(AddressFamily::Ipv4).await;
    let metadata = PeerClient::new(MetadataConfig {
        address_policy: AddressPolicy::LocalUnicast,
        ..Default::default()
    })
    .unwrap()
    .fetch_one(
        hash(),
        peer,
        &CancellationToken::new(),
        super::peer::PeerContext::default(),
    )
    .await
    .unwrap();
    task.await.unwrap();
    metadata
}

mod acceptance;
mod discovery;
mod limits;
mod pipeline;
mod recovery;
