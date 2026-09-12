//! KRPC（Kademlia RPC）协议。
//!
//! `message` 描述 KRPC 消息和查询参数，`compact` 负责协议中的紧凑二进制格式。
//! 程序内的其他顶层模块只需要从这里导入类型，无需了解内部文件如何拆分。
//!
//! udp 使用本层编解码；dispatcher 再检查方法所需字段、响应来源和节点身份。

mod compact;
mod message;

pub(crate) use compact::{
    CompactNodeV4, CompactNodeV6, CompactNodesV4, CompactNodesV6, CompactPeerAddress,
    InfoHashSamples,
};
pub(crate) use message::{
    InfoHashV1, KrpcErrorCode, KrpcMessage, MessageType, NodeId, QueryArgs, QueryMethod,
    ResponseArgs, Token,
};

#[cfg(test)]
mod tests;
