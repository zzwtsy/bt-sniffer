//! 任务最终远端失败的固定类别；不是期间所有 peer 失败的列表。
/// 防止已有字符串错误类别扩散为无界标签；未列出的类别统一归 Other。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum AttemptFailure {
    NoPeers,
    PeerIo,
    PeerTimeout,
    Protocol,
    HashMismatch,
    Unsupported,
    ReceiveLimit,
    Rejected,
    MetadataUnavailable,
    TaskTimeout,
    #[cfg(test)]
    Other,
}
impl AttemptFailure {
    #[cfg(test)]
    pub(crate) fn from_label(label: &str) -> Self {
        match label {
            "no_peers" => Self::NoPeers,
            "peer_io" => Self::PeerIo,
            "peer_timeout" => Self::PeerTimeout,
            "protocol" => Self::Protocol,
            "hash_mismatch" => Self::HashMismatch,
            "unsupported" => Self::Unsupported,
            "receive_limit" => Self::ReceiveLimit,
            "rejected" => Self::Rejected,
            "metadata_unavailable" => Self::MetadataUnavailable,
            "task_timeout" => Self::TaskTimeout,
            _ => Self::Other,
        }
    }
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::NoPeers => "no_peers",
            Self::PeerIo => "peer_io",
            Self::PeerTimeout => "peer_timeout",
            Self::Protocol => "protocol",
            Self::HashMismatch => "hash_mismatch",
            Self::Unsupported => "unsupported",
            Self::ReceiveLimit => "receive_limit",
            Self::Rejected => "rejected",
            Self::MetadataUnavailable => "metadata_unavailable",
            Self::TaskTimeout => "task_timeout",
            #[cfg(test)]
            Self::Other => "other",
        }
    }
}
