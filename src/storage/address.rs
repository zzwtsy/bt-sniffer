//! SQLite 地址字段编码；不决定网络地址策略。
use super::StorageError;
use std::net::IpAddr;
/// 保存 4 或 16 字节 IP 地址，不包含端口；端口独立存储。
pub(crate) fn ip_bytes(ip: IpAddr) -> Vec<u8> {
    match ip {
        IpAddr::V4(ip) => ip.octets().to_vec(),
        IpAddr::V6(ip) => ip.octets().to_vec(),
    }
}
/// 只检查地址编码长度；地址族、端口和使用策略由调用层继续校验。
pub(crate) fn decode_ip(bytes: &[u8]) -> Result<IpAddr, StorageError> {
    match bytes.len() {
        4 => Ok(IpAddr::from(
            <[u8; 4]>::try_from(bytes).map_err(|_| StorageError::Invalid("IP 长度无效"))?,
        )),
        16 => Ok(IpAddr::from(
            <[u8; 16]>::try_from(bytes).map_err(|_| StorageError::Invalid("IP 长度无效"))?,
        )),
        _ => Err(StorageError::Invalid("IP 长度无效")),
    }
}
