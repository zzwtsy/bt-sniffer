//! 一条 TCP 连接的 metadata 会话。取消/超时后整条连接丢弃，不重用半帧状态。
//!
//! fetch_peer 编排握手、协商、分片与校验；除本模块的阶段期限外，上层 fetcher 还施加 peer 和任务总期限。
use super::*;
use crate::peer_wire::{self, MetadataMessage};
use futures_util::{SinkExt, StreamExt};
use sha1::{Digest, Sha1};
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
            return Err(WireError("分片编号或 total_size 不匹配").into());
        }
        let start = piece * BLOCK_SIZE;
        let end = (start + BLOCK_SIZE).min(total_size);
        if data.len() != end - start {
            return Err(WireError("分片长度不符合 16 KiB/末片规则").into());
        }
        if self.complete[piece] {
            if self.bytes[start..end] != *data {
                return Err(WireError("重复分片内容冲突").into());
            }
            return Ok(());
        }
        if self.deadlines[piece].is_none() {
            return Err(WireError("收到未请求的分片").into());
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
    stage: Stage,
) -> Result<(), PeerError> {
    timeout_at(deadline, stream.send(bytes))
        .await
        .map_err(|_| PeerError::Timeout(stage))??;
    Ok(())
}

/// 标准握手与后续扩展握手共用 deadline，不因一次成功读写重新计时。
async fn exchange_handshake(
    socket: &mut TcpStream,
    hash: InfoHashV1,
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
    .map_err(|_| PeerError::Timeout(Stage::Handshake))??;
    let info = peer_wire::parse_handshake(&handshake, hash)?;
    if !info.supports_extensions {
        return Err(PeerError::Unsupported);
    }
    Ok(info)
}

/// 只补足尚未请求的分片；已有在途分片的期限不会因补窗口而改变。
async fn request_pieces(
    stream: &mut PeerStream,
    pieces: &mut Pieces,
    remote_id: u8,
    config: &MetadataConfig,
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
            Stage::Piece,
        )
        .await?;
        pieces.deadlines[piece] = Some(Instant::now() + config.piece_timeout);
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
    stage: Stage,
    config: &MetadataConfig,
    received: &mut ReceiveBudget,
) -> Result<bytes::BytesMut, PeerError> {
    let frame = timeout_at(deadline, stream.next())
        .await
        .map_err(|_| PeerError::Timeout(stage))?
        .ok_or(PeerError::Disconnected)?
        .map_err(|error| {
            if error
                .get_ref()
                .is_some_and(|e| e.is::<tokio_util::codec::LengthDelimitedCodecError>())
            {
                PeerError::Limit("peer-wire 帧长度")
            } else {
                PeerError::Io(error)
            }
        })?;
    received.bytes = received
        .bytes
        .checked_add(frame.len() + 4)
        .ok_or(PeerError::Limit("接收字节计数溢出"))?;
    received.frames += 1;
    if received.bytes > config.max_received_bytes || received.frames > config.max_received_frames {
        return Err(PeerError::Limit("单 peer 累计接收预算"));
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
    phase: &mut Stage,
) -> Result<(), PeerError> {
    if let Some(id) = update.metadata_id {
        if id == 0 {
            return Err(PeerError::Unsupported);
        }
        *remote_id = Some(id);
    }
    if let Some(size) = update.metadata_size {
        if size == 0 || size > config.max_metadata_size {
            return Err(PeerError::Limit("metadata_size 必须为正且不超过上限"));
        }
        if pieces.is_some() && *metadata_size != Some(size) {
            return Err(WireError("传输中 metadata_size 改变").into());
        }
        *metadata_size = Some(size);
    }
    if pieces.is_none()
        && remote_id.is_some()
        && let Some(size) = *metadata_size
    {
        *pieces = Some(Pieces::new(size));
        *phase = Stage::Piece;
    }
    Ok(())
}

/// SHA-1 校验原始 info 字节，然后检查完整字典；两步均成功才移动缓冲区交给上层。
fn verify_metadata(
    state: &mut Pieces,
    hash: InfoHashV1,
    address: SocketAddr,
    peer_id: PeerId,
    max_depth: usize,
) -> Result<VerifiedMetadata, PeerError> {
    if Sha1::digest(&state.bytes).as_slice() != hash.0 {
        return Err(PeerError::HashMismatch);
    }
    let raw = peer_wire::dictionary_prefix(&state.bytes, max_depth)?;
    if raw.len() != state.bytes.len() {
        return Err(WireError("info 字典之后存在尾随数据").into());
    }
    Ok(VerifiedMetadata {
        info_hash: hash,
        source: address,
        peer_id,
        info: std::mem::take(&mut state.bytes),
    })
}

/// 顺序驱动当前 peer 的连接、握手、分片和校验；phase 供外层总超时报告进度。
pub(super) async fn fetch_peer(
    config: &MetadataConfig,
    local_id: PeerId,
    hash: InfoHashV1,
    address: SocketAddr,
    phase: &mut Stage,
    report: &mut crate::metrics::PhaseReport,
) -> Result<VerifiedMetadata, PeerError> {
    let mut socket = tokio::time::timeout(config.connect_timeout, TcpStream::connect(address))
        .await
        .map_err(|_| PeerError::Timeout(Stage::Connect))??;
    report.advance(1);
    let handshake_deadline = Instant::now() + config.handshake_timeout;
    *phase = Stage::Handshake;
    let handshake_info =
        exchange_handshake(&mut socket, hash, local_id, handshake_deadline).await?;
    let mut stream = LengthDelimitedCodec::builder()
        .big_endian()
        .length_field_length(4)
        .max_frame_length(config.max_frame_size)
        .new_framed(socket);
    send(
        &mut stream,
        peer_wire::extension_handshake(),
        handshake_deadline,
        Stage::Handshake,
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
            report.advance(2);
            request_pieces(
                &mut stream,
                pieces,
                remote_id.expect("已协商扩展 ID"),
                config,
            )
            .await?;
        }
        let (deadline, stage) = match &pieces {
            Some(pieces) => (
                pieces.deadline(Instant::now() + config.piece_timeout),
                Stage::Piece,
            ),
            None => (handshake_deadline, Stage::Handshake),
        };
        // 无关消息、重复分片和 keepalive 不改变已有分片的绝对期限。
        if Instant::now() >= deadline {
            return Err(PeerError::Timeout(stage));
        }
        let frame = receive_frame(&mut stream, deadline, stage, config, &mut received).await?;
        if frame.is_empty() || frame[0] != 20 {
            continue;
        }
        let Some(&id) = frame.get(1) else {
            return Err(WireError("extended 消息缺少扩展 ID").into());
        };
        if id == 0 {
            let update =
                peer_wire::parse_extension(&frame[2..], config.max_header_size, config.max_depth)?;
            apply_extension(
                update,
                config,
                &mut remote_id,
                &mut metadata_size,
                &mut pieces,
                phase,
            )?;
            continue;
        }
        if id != peer_wire::LOCAL_METADATA_ID {
            continue;
        }
        let Some(state) = &mut pieces else {
            return Err(WireError("扩展协商完成前收到 metadata 消息").into());
        };
        match peer_wire::parse_metadata(&frame[2..], config.max_header_size, config.max_depth)? {
            MetadataMessage::Unknown => {}
            MetadataMessage::Request { piece } => {
                // 未完成整体 SHA-1 校验时不能上传任何分片，即使本地已有部分数据。
                send(
                    &mut stream,
                    peer_wire::metadata_control(remote_id.unwrap(), 2, piece),
                    deadline,
                    Stage::Piece,
                )
                .await?;
            }
            MetadataMessage::Reject { piece } => {
                if state.deadlines.get(piece).is_some_and(Option::is_some) {
                    return Err(PeerError::Rejected(piece));
                }
            }
            MetadataMessage::Data {
                piece,
                total_size,
                data,
            } => state.data(piece, total_size, data)?,
        }
        if state.received == state.complete.len() {
            report.advance(3);
            *phase = Stage::Verify;
            return verify_metadata(
                state,
                hash,
                address,
                handshake_info.peer_id,
                config.max_depth,
            );
        }
    }
}

#[cfg(test)]
mod tests;
