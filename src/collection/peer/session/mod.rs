//! 一条 TCP 连接的 metadata 会话。取消/超时后整条连接丢弃，不重用半帧状态。
//!
//! fetch_peer 编排握手、协商、分片与校验；除本模块的阶段期限外，上层 fetcher 还施加 peer 和任务总期限。
use super::*;
use crate::collection::peer::wire as peer_wire;
use crate::collection::peer::wire::MetadataMessage;
use crate::collection::peer::wire::WireErrorKind;
use futures_util::{SinkExt, StreamExt};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout_at,
};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

type PeerStream = Framed<TcpStream, LengthDelimitedCodec>;

/// 分片只属于当前 peer，失败后整体丢弃，避免混合来源导致校验无法归责。
struct Pieces {
    /// 按已验证的 metadata_size 一次分配；各片写入固定偏移。
    bytes: Vec<u8>,
    /// 与分片编号一一对应；收到相同重复片不再次增加 received。
    complete: Vec<bool>,
    /// Some 表示已请求但尚未完成；期限从请求发送完成起算。
    deadlines: Vec<Option<Instant>>,
    /// 下一个尚未发送过请求的分片编号。
    next: usize,
    /// 已完成的不同分片数，不是字节数，也不包含相同重复片。
    received: usize,
}
impl Pieces {
    fn new(size: usize) -> Self {
        let count = size.div_ceil(BLOCK_SIZE);
        Self {
            bytes: vec![0; size],
            complete: vec![false; count],
            deadlines: vec![None; count],
            next: 0,
            received: 0,
        }
    }
    /// 返回最早在途分片期限；没有在途请求时才采用调用者的 fallback。
    fn deadline(&self, fallback: Instant) -> Instant {
        self.deadlines
            .iter()
            .flatten()
            .copied()
            .min()
            .unwrap_or(fallback)
    }
    /// 校验编号、长度和请求状态后接纳；相同重复片幂等，冲突片拒绝。
    fn data(&mut self, piece: usize, total_size: usize, data: &[u8]) -> Result<(), PeerError> {
        if total_size != self.bytes.len() || piece >= self.complete.len() {
            return Err(WireErrorKind::PieceOrSizeMismatch.into());
        }
        let start = piece * BLOCK_SIZE;
        let end = (start + BLOCK_SIZE).min(total_size);
        if data.len() != end - start {
            return Err(WireErrorKind::PieceLength.into());
        }
        if self.complete[piece] {
            if self.bytes[start..end] != *data {
                return Err(WireErrorKind::ConflictingPiece.into());
            }
            return Ok(());
        }
        if self.deadlines[piece].is_none() {
            return Err(WireErrorKind::UnrequestedPiece.into());
        }
        self.bytes[start..end].copy_from_slice(data);
        self.complete[piece] = true;
        self.deadlines[piece] = None;
        self.received += 1;
        Ok(())
    }
}

/// 复用调用者提供的绝对期限；成功发送不会自动延长握手或已有分片期限。
async fn send(
    stream: &mut PeerStream,
    bytes: bytes::Bytes,
    deadline: Instant,
) -> Result<(), PeerError> {
    timeout_at(deadline, stream.send(bytes))
        .await
        .map_err(|_| PeerError::Timeout(Deadline::Stage))??;
    Ok(())
}

/// 标准握手与后续扩展握手共用 deadline，不因一次成功读写重新计时。
async fn exchange_handshake(
    socket: &mut TcpStream,
    hash: SwarmKey,
    local_id: PeerId,
    handshake_deadline: Instant,
) -> Result<peer_wire::HandshakeInfo, PeerError> {
    let mut handshake = [0; 68];
    timeout_at(handshake_deadline, async {
        socket
            .write_all(&peer_wire::handshake(hash, local_id))
            .await?;
        socket.read_exact(&mut handshake).await?;
        Ok::<_, std::io::Error>(())
    })
    .await
    .map_err(|_| PeerError::Timeout(Deadline::Stage))??;
    // 协议校验和先后顺序由解析器负责；这里只转换会话需要单独统计的错误。
    match peer_wire::parse_handshake(&handshake, hash) {
        Ok(info) => Ok(info),
        Err(error) if error.kind == WireErrorKind::HandshakeHash => {
            Err(PeerError::HandshakeHashMismatch)
        }
        Err(error) => Err(error.into()),
    }
}

/// 只补足尚未请求的分片；已有在途分片的期限不会因补窗口而改变。
async fn request_pieces(
    stream: &mut PeerStream,
    pieces: &mut Pieces,
    remote_id: u8,
    config: &MetadataConfig,
    observer: &crate::observation::Observer,
) -> Result<(), PeerError> {
    while pieces.next < pieces.complete.len()
        && pieces.deadlines.iter().flatten().count() < config.request_window
    {
        let piece = pieces.next;
        let deadline = pieces.deadline(Instant::now() + config.piece_timeout);
        send(
            stream,
            peer_wire::metadata_control(remote_id, 0, piece),
            deadline,
        )
        .await?;
        pieces.deadlines[piece] = Some(Instant::now() + config.piece_timeout);
        observer.emit(crate::observation::Kind::Piece,"request","sent",||serde_json::json!({"piece":piece,"timeout_ms":config.piece_timeout.as_millis() as u64}));
        pieces.next += 1;
    }
    Ok(())
}

/// 累计预算包含握手和无关消息，防止远端用小帧持续占用资源。
struct ReceiveBudget {
    bytes: usize,
    frames: usize,
}

async fn receive_frame(
    stream: &mut PeerStream,
    deadline: Instant,
    config: &MetadataConfig,
    received: &mut ReceiveBudget,
) -> Result<bytes::BytesMut, PeerError> {
    let frame = timeout_at(deadline, stream.next())
        .await
        .map_err(|_| PeerError::Timeout(Deadline::Stage))?
        .ok_or(PeerError::Disconnected)?
        .map_err(|error| {
            if error
                .get_ref()
                .is_some_and(|e| e.is::<tokio_util::codec::LengthDelimitedCodecError>())
            {
                PeerError::Limit(super::ResourceLimit::FrameLength)
            } else {
                PeerError::Io(error)
            }
        })?;
    received.bytes = received
        .bytes
        .checked_add(frame.len() + 4)
        .ok_or(PeerError::Limit(super::ResourceLimit::ReceiveOverflow))?;
    received.frames += 1;
    if received.bytes > config.max_received_bytes || received.frames > config.max_received_frames {
        return Err(PeerError::Limit(super::ResourceLimit::ReceiveBudget));
    }
    Ok(frame)
}

/// BEP 10 字段可以分多次补齐；传输开始后不能更改已分配的 metadata 大小。
/// 缺省字段沿用旧值，ID 0 表示显式禁用并立即拒绝。
fn apply_extension(
    update: peer_wire::ExtensionUpdate,
    config: &MetadataConfig,
    remote_id: &mut Option<u8>,
    metadata_size: &mut Option<usize>,
    pieces: &mut Option<Pieces>,
) -> Result<(), PeerError> {
    if let Some(id) = update.metadata_id {
        if id == 0 {
            return Err(PeerError::Unsupported);
        }
        *remote_id = Some(id);
    }
    if let Some(size) = update.metadata_size {
        if size == 0 || size > config.max_metadata_size {
            return Err(PeerError::Limit(super::ResourceLimit::MetadataSize));
        }
        if pieces.is_some() && *metadata_size != Some(size) {
            return Err(WireErrorKind::MetadataSizeChanged.into());
        }
        *metadata_size = Some(size);
    }
    if pieces.is_none()
        && remote_id.is_some()
        && let Some(size) = *metadata_size
    {
        *pieces = Some(Pieces::new(size));
    }
    Ok(())
}

/// 原始 info 匹配 SHA-1 或 v2 SHA-256 前缀，再检查完整字典；两步均成功才移动缓冲区交给上层。
fn verify_metadata(
    state: &mut Pieces,
    hash: SwarmKey,
    address: SocketAddr,
    peer_id: PeerId,
    max_depth: usize,
    observer: &crate::observation::Observer,
) -> Result<VerifiedMetadata, PeerError> {
    let matches = crate::collection::metainfo::match_identity(&state.bytes, hash).is_some();
    observer.emit(
        crate::observation::Kind::Validation,
        "raw_info_hash",
        if matches { "verified" } else { "mismatch" },
        || serde_json::json!({"bytes":state.bytes.len()}),
    );
    if !matches {
        return Err(PeerError::HashMismatch);
    }
    let parsed = peer_wire::dictionary_prefix(&state.bytes, max_depth);
    observer.emit(
        crate::observation::Kind::Validation,
        "complete_dictionary",
        if parsed
            .as_ref()
            .is_ok_and(|raw| raw.len() == state.bytes.len())
        {
            "verified"
        } else {
            "invalid"
        },
        || serde_json::json!({}),
    );
    let raw = parsed?;
    if raw.len() != state.bytes.len() {
        return Err(WireErrorKind::InfoTrailing.into());
    }
    Ok(VerifiedMetadata {
        used_extension_compatibility: false,
        info_hash: hash,
        source: address,
        peer_id,
        info: std::mem::take(&mut state.bytes),
    })
}

/// 顺序驱动当前 peer 的连接、握手、分片和校验；唯一观察对象统一推进物理阶段与完整握手计时。
pub(super) async fn fetch_peer(
    config: &MetadataConfig,
    local_id: PeerId,
    hash: SwarmKey,
    address: SocketAddr,
    diagnostic: &mut crate::collection::diagnostics::PeerObservation,
) -> Result<VerifiedMetadata, PeerError> {
    let mut socket = tokio::time::timeout(config.connect_timeout, TcpStream::connect(address))
        .await
        .map_err(|_| PeerError::Timeout(Deadline::Stage))??;
    diagnostic.advance(Stage::StandardHandshake);
    let handshake_deadline = Instant::now() + config.handshake_timeout;
    let handshake_info =
        exchange_handshake(&mut socket, hash, local_id, handshake_deadline).await?;
    diagnostic.advance(Stage::ExtensionHandshake);
    if !handshake_info.supports_extensions {
        return Err(PeerError::Unsupported);
    }
    let mut stream = LengthDelimitedCodec::builder()
        .big_endian()
        .length_field_length(4)
        .max_frame_length(config.max_frame_size)
        .new_framed(socket);
    send(
        &mut stream,
        peer_wire::extension_handshake(),
        handshake_deadline,
    )
    .await?;
    let (mut remote_id, mut metadata_size) = (None, None);
    let mut pieces: Option<Pieces> = None;
    let mut received = ReceiveBudget {
        bytes: 68,
        frames: 0,
    };
    loop {
        if let Some(pieces) = &mut pieces {
            diagnostic.advance(Stage::Transfer);
            request_pieces(
                &mut stream,
                pieces,
                remote_id.expect("已协商扩展 ID"),
                config,
                &diagnostic.observer,
            )
            .await?;
        }
        let deadline = match &pieces {
            Some(pieces) => pieces.deadline(Instant::now() + config.piece_timeout),
            None => handshake_deadline,
        };
        // 无关消息、重复分片和 keepalive 不改变已有分片的绝对期限。
        let frame = if Instant::now() >= deadline {
            Err(PeerError::Timeout(Deadline::Stage))
        } else {
            receive_frame(&mut stream, deadline, config, &mut received).await
        };
        if matches!(&frame, Err(PeerError::Timeout(_)))
            && let Some(pieces) = &pieces
        {
            for (piece, deadline) in pieces.deadlines.iter().enumerate() {
                if deadline.is_some_and(|deadline| deadline <= Instant::now()) {
                    diagnostic.observer.emit(
                        crate::observation::Kind::Piece,
                        "receive",
                        "timeout",
                        || serde_json::json!({"piece":piece}),
                    );
                }
            }
        }
        let frame = frame?;
        if frame.is_empty() || frame[0] != 20 {
            continue;
        }
        let Some(&id) = frame.get(1) else {
            return Err(WireErrorKind::MissingExtensionId.into());
        };
        if id == 0 {
            let parsed =
                peer_wire::parse_extension(&frame[2..], config.max_header_size, config.max_depth);
            diagnostic.extension_frame(&parsed);
            let update = parsed?;
            diagnostic.observer.emit(crate::observation::Kind::Peer,"extension","parsed",||serde_json::json!({"metadata_id":update.metadata_id,"metadata_size":update.metadata_size}));
            apply_extension(
                update,
                config,
                &mut remote_id,
                &mut metadata_size,
                &mut pieces,
            )?;
            continue;
        }
        if id != peer_wire::LOCAL_METADATA_ID {
            continue;
        }
        let Some(state) = &mut pieces else {
            return Err(WireErrorKind::MetadataBeforeNegotiation.into());
        };
        match peer_wire::parse_metadata(&frame[2..], config.max_header_size, config.max_depth)? {
            MetadataMessage::Unknown => {}
            MetadataMessage::Request { piece } => {
                // 未完成整体 SHA-1 校验时不能上传任何分片，即使本地已有部分数据。
                send(
                    &mut stream,
                    peer_wire::metadata_control(
                        remote_id.expect("metadata 状态只在扩展 ID 协商后创建"),
                        2,
                        piece,
                    ),
                    deadline,
                )
                .await?;
            }
            MetadataMessage::Reject { piece } => {
                if state.deadlines.get(piece).is_some_and(Option::is_some) {
                    diagnostic.observer.emit(
                        crate::observation::Kind::Piece,
                        "receive",
                        "rejected",
                        || serde_json::json!({"piece":piece}),
                    );
                    return Err(PeerError::Rejected(piece));
                }
            }
            MetadataMessage::Data {
                piece,
                total_size,
                data,
            } => {
                let before = state.received;
                let result = state.data(piece, total_size, data);
                diagnostic.observer.emit(
                    crate::observation::Kind::Piece,
                    "receive",
                    if result.is_err() {
                        "invalid"
                    } else if state.received == before {
                        "duplicate"
                    } else {
                        "accepted"
                    },
                    || {
                        serde_json::json!({
                            "piece": piece,
                            "bytes": data.len(),
                            "received_pieces": state.received,
                            "total_pieces": state.complete.len(),
                            "metadata_bytes": state.bytes.len(),
                        })
                    },
                );
                result?;
            }
        }
        diagnostic.progress(||serde_json::json!({"peer":address.to_string(),"metadata_bytes":state.bytes.len(),"received_pieces":state.received,"complete":state.complete,"requested":state.next}));
        if state.received == state.complete.len() {
            diagnostic.advance(Stage::Verify);
            let mut validation = diagnostic
                .observer
                .span(crate::observation::Kind::Validation, "metadata");
            validation.executing();
            let result = verify_metadata(
                state,
                hash,
                address,
                handshake_info.peer_id,
                config.max_depth,
                &validation.observer,
            );
            validation.finish(
                result
                    .as_ref()
                    .map_or_else(|error| error.label(), |_| "verified"),
            );
            let mut metadata = result?;
            metadata.used_extension_compatibility = diagnostic.extension_downloaded();
            return Ok(metadata);
        }
    }
}

#[cfg(test)]
mod tests;
