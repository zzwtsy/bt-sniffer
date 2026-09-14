//! 两个 socket 分别负责一种地址族，避免 IPv6 socket 抢占 IPv4 端口。
//!
//! 绑定完成后将 socket 所有权交给 app，再由每个 dispatcher 独占其地址族的收发。
use crate::app::config::Cli;
use crate::dht::udp::UdpTransport;
use crate::dht::udp::UdpTransportConfig;
use socket2::{Domain, Protocol, Socket, Type};
use std::{io, net::SocketAddr};

/// 返回的 socket 随后移入各自 dispatcher；中途失败会释放此前绑定的 socket。
/// 只有默认双栈监听且环境明确不支持 IPv6 时才允许回退，显式配置错误必须上报。
pub(super) fn bind(config: &Cli) -> Result<Vec<UdpTransport>, String> {
    let mut sockets = Vec::new();
    if !config.ipv6_only {
        sockets.push(
            bind_one(
                config
                    .listen_v4
                    .unwrap_or_else(|| "0.0.0.0:6881".parse().unwrap()),
            )
            .map_err(|e| format!("IPv4 监听失败：{e}"))?,
        );
    }
    if !config.ipv4_only {
        let address = config
            .listen_v6
            .unwrap_or_else(|| "[::]:6881".parse().unwrap());
        match bind_one(address) {
            Ok(socket) => sockets.push(socket),
            Err(error)
                if config.listen_v6.is_none() && !config.ipv6_only && ipv6_unavailable(&error) =>
            {
                tracing::warn!(
                    event = "ipv6_listen_unavailable",
                    schema_version = 1u64,
                    phase = "startup",
                    action = "continue_ipv4",
                    %error,
                    "环境不支持默认 IPv6 监听，继续 IPv4"
                );
            }
            Err(error) => return Err(format!("IPv6 监听失败：{error}")),
        }
    }
    Ok(sockets)
}
fn bind_one(address: SocketAddr) -> io::Result<UdpTransport> {
    let socket = Socket::new(
        Domain::for_address(address),
        Type::DGRAM,
        Some(Protocol::UDP),
    )?;
    if address.is_ipv6() {
        socket.set_only_v6(true)?;
    }
    socket.set_nonblocking(true)?;
    socket.bind(&address.into())?;
    let socket = tokio::net::UdpSocket::from_std(socket.into())?;
    UdpTransport::from_socket(socket, UdpTransportConfig::default()).map_err(io::Error::other)
}
pub(super) fn ipv6_unavailable(error: &io::Error) -> bool {
    if error.kind() == io::ErrorKind::AddrNotAvailable {
        return true;
    }
    // 仅列出明确的协议/地址族不支持；权限和端口占用必须报错。
    #[cfg(target_os = "linux")]
    {
        matches!(error.raw_os_error(), Some(93 | 97))
    }
    #[cfg(any(target_os = "macos", target_os = "freebsd"))]
    {
        matches!(error.raw_os_error(), Some(43 | 47))
    }
    #[cfg(windows)]
    {
        matches!(error.raw_os_error(), Some(10043 | 10047))
    }
    #[cfg(not(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "freebsd",
        windows
    )))]
    {
        false
    }
}
