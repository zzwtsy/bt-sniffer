//! KRPC 的 UDP 传输。
//!
//! [`UdpTransport`] 负责在 [`KrpcMessage`] 和 UDP 数据报之间转换。它不负责判断消息
//! 是查询还是响应，也不维护 transaction 状态，这些职责由 DHT 上层完成。
//!
//! dispatcher 独占 transport；本层校验报文大小与编码，响应是否匹配查询由 transaction 判断。

use bendy::serde::{Deserializer, to_bytes};
use std::error::Error;
use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
#[cfg(test)]
use tokio::net::ToSocketAddrs;
use tokio::net::UdpSocket;

use crate::dht::krpc::KrpcMessage;

mod ordering;

/// 默认允许的单条 KRPC 消息大小。
///
/// 常规 DHT 消息通常远小于这个值。设置上限可以避免恶意数据报导致无界内存分配。
pub(crate) const DEFAULT_MAX_MESSAGE_SIZE: usize = 4 * 1024;

/// IPv4 UDP 在不使用 jumbogram 时能够承载的最大 payload。
pub(crate) const MAX_UDP_PAYLOAD_SIZE: usize = 65_507;

/// UDP transport 的可调参数。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UdpTransportConfig {
    /// 允许发送或接收的最大 KRPC 数据报大小。
    pub(crate) max_message_size: usize,
}

impl Default for UdpTransportConfig {
    fn default() -> Self {
        Self {
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
        }
    }
}

/// 从 UDP socket 收到并成功解码的一条 KRPC 消息。
#[derive(Debug)]
pub(crate) struct ReceivedMessage {
    /// 数据报的真实来源地址，后续匹配 transaction 时必须校验。
    pub(crate) source: SocketAddr,
    /// 解码后的 KRPC 消息。
    pub(crate) message: KrpcMessage,
    /// 原始 UDP payload 的字节数，便于统计网络流量。
    pub(crate) encoded_len: usize,
}

/// 尚未执行 Bencode 解码的数据报，供 dispatcher 先做流量接纳。
pub(crate) struct Datagram {
    pub(crate) source: SocketAddr,
    pub(crate) bytes: Vec<u8>,
    limit: usize,
}
impl Datagram {
    pub(crate) fn decode(self) -> Result<ReceivedMessage, UdpTransportError> {
        let source = self.source;
        let size = self.bytes.len();
        if size > self.limit {
            return Err(UdpTransportError::MessageTooLarge {
                source: Some(source),
                size,
                limit: self.limit,
            });
        }
        let normalized = ordering::normalize(&self.bytes)
            .map_err(|error| UdpTransportError::Decode { source, error })?;
        let message = Deserializer::from_bytes(&normalized)
            .with_forbid_trailing_bytes(true)
            .deserialize()
            .map_err(|error| UdpTransportError::Decode { source, error })?;
        Ok(ReceivedMessage {
            source,
            message,
            encoded_len: size,
        })
    }
}

/// UDP 收发或 KRPC 编解码失败。
#[derive(Debug)]
pub(crate) enum UdpTransportError {
    /// transport 配置不合法。
    InvalidMaxMessageSize { actual: usize },
    /// 系统 UDP 操作失败。
    Io(io::Error),
    /// 待发送的 KRPC 消息无法编码。
    Encode(bendy::serde::Error),
    /// 收到的数据报不是合法 KRPC 消息。
    Decode {
        source: SocketAddr,
        error: bendy::serde::Error,
    },
    /// 数据报超过本地允许的大小。
    MessageTooLarge {
        source: Option<SocketAddr>,
        size: usize,
        limit: usize,
    },
}

impl fmt::Display for UdpTransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMaxMessageSize { actual } => write!(
                formatter,
                "UDP 消息大小上限必须在 1..={MAX_UDP_PAYLOAD_SIZE} 字节之间，实际为 {actual}"
            ),
            Self::Io(error) => write!(formatter, "UDP 收发失败：{error}"),
            Self::Encode(error) => write!(formatter, "KRPC 消息编码失败：{error}"),
            Self::Decode { source, error } => {
                write!(formatter, "来自 {source} 的 KRPC 消息解码失败：{error}")
            }
            Self::MessageTooLarge {
                source,
                size,
                limit,
            } => match source {
                Some(source) => write!(
                    formatter,
                    "来自 {source} 的 UDP 数据报至少有 {size} 字节，超过 {limit} 字节上限"
                ),
                None => write!(
                    formatter,
                    "待发送的 UDP 数据报有 {size} 字节，超过 {limit} 字节上限"
                ),
            },
        }
    }
}

impl Error for UdpTransportError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Encode(error) => Some(error),
            Self::Decode { error, .. } => Some(error),
            Self::InvalidMaxMessageSize { .. } | Self::MessageTooLarge { .. } => None,
        }
    }
}

impl From<io::Error> for UdpTransportError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// 可以在多个异步任务之间共享的 UDP transport。
///
/// 通常由一个任务持续调用 [`Self::recv_datagram`]，其他任务使用克隆的 handle 发送消息。
#[derive(Debug, Clone)]
pub(crate) struct UdpTransport {
    socket: Arc<UdpSocket>,
    config: UdpTransportConfig,
}

impl UdpTransport {
    /// 入站查询响应的预算不能超过 transport 自己的发送上限。
    pub(crate) fn max_message_size(&self) -> usize {
        self.config.max_message_size
    }

    /// 绑定本地地址并创建 transport。
    #[cfg(test)]
    pub(crate) async fn bind<A>(
        address: A,
        config: UdpTransportConfig,
    ) -> Result<Self, UdpTransportError>
    where
        A: ToSocketAddrs,
    {
        let socket = UdpSocket::bind(address).await?;
        Self::from_socket(socket, config)
    }

    /// 使用一个已经绑定的 Tokio UDP socket 创建 transport。
    pub(crate) fn from_socket(
        socket: UdpSocket,
        config: UdpTransportConfig,
    ) -> Result<Self, UdpTransportError> {
        if config.max_message_size == 0 || config.max_message_size > MAX_UDP_PAYLOAD_SIZE {
            return Err(UdpTransportError::InvalidMaxMessageSize {
                actual: config.max_message_size,
            });
        }

        Ok(Self {
            socket: Arc::new(socket),
            config,
        })
    }

    /// 返回 socket 实际绑定的本地地址。
    pub(crate) fn local_addr(&self) -> Result<SocketAddr, UdpTransportError> {
        Ok(self.socket.local_addr()?)
    }

    /// 编码并发送一条 KRPC 消息。
    #[cfg(test)]
    pub(crate) async fn send_to(
        &self,
        destination: SocketAddr,
        message: &KrpcMessage,
    ) -> Result<usize, UdpTransportError> {
        let encoded = to_bytes(message).map_err(UdpTransportError::Encode)?;
        if encoded.len() > self.config.max_message_size {
            return Err(UdpTransportError::MessageTooLarge {
                source: None,
                size: encoded.len(),
                limit: self.config.max_message_size,
            });
        }

        let sent = self.socket.send_to(&encoded, destination).await?;
        if sent != encoded.len() {
            return Err(UdpTransportError::Io(io::Error::new(
                io::ErrorKind::WriteZero,
                "UDP socket 没有发送完整数据报",
            )));
        }

        Ok(sent)
    }

    /// 非阻塞发送；直接调用 socket，不依赖 Tokio 的可写缓存，也不等待可写事件。
    pub(crate) fn try_send_to(
        &self,
        destination: SocketAddr,
        message: &KrpcMessage,
    ) -> Result<usize, UdpTransportError> {
        let bytes = to_bytes(message).map_err(UdpTransportError::Encode)?;
        if bytes.len() > self.config.max_message_size {
            return Err(UdpTransportError::MessageTooLarge {
                source: None,
                size: bytes.len(),
                limit: self.config.max_message_size,
            });
        }
        let sent = socket2::SockRef::from(&*self.socket).send_to(&bytes, &destination.into())?;
        if sent != bytes.len() {
            return Err(io::Error::new(io::ErrorKind::WriteZero, "UDP 未完整发送").into());
        }
        Ok(sent)
    }
    /// 多读一字节识别超长包；解码由接纳预算之后的 dispatcher 执行。
    pub(crate) async fn recv_datagram(&self) -> Result<Datagram, UdpTransportError> {
        let mut bytes = vec![0; self.config.max_message_size + 1];
        let (size, source) = self.socket.recv_from(&mut bytes).await?;
        bytes.truncate(size);
        Ok(Datagram {
            source,
            bytes,
            limit: self.config.max_message_size,
        })
    }
    #[cfg(test)]
    pub(crate) async fn recv(&self) -> Result<ReceivedMessage, UdpTransportError> {
        self.recv_datagram().await?.decode()
    }
}

#[cfg(test)]
mod tests;
