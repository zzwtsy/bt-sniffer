//! 真实 loopback 验证兼容标记属于成功来源会话；取消、超时和失败仍由原 fetch 路径收尾。
use super::*;
use crate::collection::peer::wire as peer_wire;
use crate::collection::peer::wire::PeerId;
use futures_util::{SinkExt, StreamExt};
use sha1::{Digest, Sha1};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tokio_util::codec::LengthDelimitedCodec;

pub(crate) const INFO: &[u8] = b"d4:name4:teste";

#[derive(Clone, Copy)]
pub(crate) enum Reply {
    Serve,
    Reject,
    WrongHash,
    WaitForPieceTimeout,
    WaitForHandshakeTimeout,
    WaitForCancel,
}

/// 两次增量握手均使用乱序，检查会话计数去重；等待场景用信号通知测试驱动方。
pub(crate) async fn peer(
    compatible: bool,
    reply: Reply,
) -> (
    SocketAddr,
    tokio::task::JoinHandle<()>,
    tokio::sync::oneshot::Receiver<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (ready, received) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut hello = [0; 68];
        socket.read_exact(&mut hello).await.unwrap();
        let target = SwarmKey(Sha1::digest(INFO).into());
        socket
            .write_all(&peer_wire::handshake(target, PeerId([7; 20])))
            .await
            .unwrap();
        let mut frames = LengthDelimitedCodec::builder().new_framed(socket);
        frames.next().await.unwrap().unwrap();
        let extension = if matches!(reply, Reply::WaitForHandshakeTimeout) {
            b"d1:zi1e1:md11:ut_metadatai7eee".to_vec()
        } else if compatible {
            format!("d13:metadata_sizei{}e1:md11:ut_metadatai7eee", INFO.len()).into_bytes()
        } else {
            format!("d1:md11:ut_metadatai7ee13:metadata_sizei{}ee", INFO.len()).into_bytes()
        };
        frames
            .send(peer_wire::extended(0, &extension))
            .await
            .unwrap();
        frames
            .send(peer_wire::extended(0, &extension))
            .await
            .unwrap();
        if matches!(reply, Reply::WaitForHandshakeTimeout) {
            let _ = ready.send(());
            while let Some(Ok(_)) = frames.next().await {}
            return;
        }
        let request = frames.next().await.unwrap().unwrap();
        assert_eq!(&request[..2], &[20, 7]);
        let _ = ready.send(());
        match reply {
            Reply::Serve | Reply::WrongHash => {
                let mut data =
                    format!("d8:msg_typei1e5:piecei0e10:total_sizei{}ee", INFO.len()).into_bytes();
                data.extend_from_slice(INFO);
                if matches!(reply, Reply::WrongHash) {
                    *data.last_mut().unwrap() = b'x';
                }
                frames.send(peer_wire::extended(1, &data)).await.unwrap();
            }
            Reply::Reject => {
                frames
                    .send(peer_wire::metadata_control(1, 2, 0))
                    .await
                    .unwrap();
            }
            Reply::WaitForCancel | Reply::WaitForPieceTimeout => {
                while let Some(Ok(_)) = frames.next().await {}
            }
            Reply::WaitForHandshakeTimeout => unreachable!(),
        }
    });
    (address, task, received)
}

pub(crate) fn fetcher(
    metrics: Arc<crate::collection::diagnostics::metrics::Metrics>,
) -> PeerClient {
    PeerClient::new(MetadataConfig {
        address_policy: AddressPolicy::LocalUnicast,
        handshake_timeout: Duration::from_millis(300),
        piece_timeout: Duration::from_millis(300),
        ..Default::default()
    })
    .unwrap()
    .with_metrics(metrics)
}

#[tokio::test]
async fn compatible_sessions_download_or_fail_without_duplicate_samples() {
    for reply in [
        Reply::Serve,
        Reply::Reject,
        Reply::WrongHash,
        Reply::WaitForPieceTimeout,
        Reply::WaitForHandshakeTimeout,
        Reply::WaitForCancel,
    ] {
        let metrics = Arc::new(crate::collection::diagnostics::metrics::Metrics::default());
        let (address, task, ready) = peer(true, reply).await;
        let fetcher = fetcher(metrics.clone());
        let cancel = CancellationToken::new();
        let target = SwarmKey(Sha1::digest(INFO).into());
        let fetch = fetcher.fetch_one(
            target,
            address,
            &cancel,
            crate::collection::peer::PeerContext::default(),
        );
        tokio::pin!(fetch);
        let result = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::select! {
                biased;
                result = &mut fetch => return result,
                _ = ready => {}
            }
            if matches!(reply, Reply::WaitForCancel) {
                cancel.cancel();
            }
            fetch.await
        })
        .await
        .unwrap();
        task.await.unwrap();
        if matches!(reply, Reply::Serve) {
            let metadata = result.unwrap();
            assert_eq!(metadata.info(), INFO);
            assert!(metadata.used_extension_compatibility());
        } else {
            assert!(result.is_err());
        }
        let report = metrics.diagnostics.report();
        let counts = &report["extension_compatibility"];
        assert_eq!(counts["attempted_frames"], 2);
        assert_eq!(counts["accepted_frames"], 2);
        assert_eq!(counts["rejected_frames"], 0);
        assert_eq!(counts["sessions"], 1);
        assert_eq!(
            counts["downloaded"],
            u64::from(matches!(reply, Reply::Serve))
        );
        assert_eq!(counts["committed"], 0);
        assert!(
            report["failures"]
                .as_array()
                .unwrap()
                .iter()
                .all(|f| f["reason"] != "Protocol(InvalidDictionary)")
        );
    }
}
