//! 固定协议错误类别与有界 Bendy 说明；说明只用于诊断，不参与分类或协议判断。
use std::fmt::{self, Write};

/// 有界、单行的底层说明；truncated 表示字符预算不足，文本不能作为分类键。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WireDetail {
    pub(crate) text: String,
    pub(crate) truncated: bool,
}

/// 固定错误类别始终保留；只有来自 Bendy 的失败附带底层说明。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WireError {
    pub(crate) kind: WireErrorKind,
    pub(crate) detail: Option<WireDetail>,
    pub(crate) inspection: Option<super::inspection::KeyInspection>,
}
impl WireError {
    pub(super) fn bendy(kind: WireErrorKind, error: &bendy::decoding::Error) -> Self {
        Self {
            kind,
            detail: Some(bounded_detail(error)),
            inspection: None,
        }
    }
}
impl From<WireErrorKind> for WireError {
    fn from(kind: WireErrorKind) -> Self {
        Self {
            kind,
            detail: None,
            inspection: None,
        }
    }
}
impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.kind.fmt(f)
    }
}
impl std::error::Error for WireError {}

/// 格式化期间就限制空间；控制字符转义也计入 256 字符，不保留半个转义序列。
fn bounded_detail(error: &impl fmt::Display) -> WireDetail {
    struct LimitedText {
        detail: WireDetail,
        characters: usize,
    }
    impl Write for LimitedText {
        fn write_str(&mut self, text: &str) -> fmt::Result {
            for ch in text.chars() {
                let escaped = if ch.is_control() {
                    ch.escape_default().to_string()
                } else {
                    ch.to_string()
                };
                let count = escaped.chars().count();
                if self.characters + count > 256 {
                    self.detail.truncated = true;
                    return Err(fmt::Error);
                }
                self.detail.text.push_str(&escaped);
                self.characters += count;
            }
            Ok(())
        }
    }
    let mut output = LimitedText {
        detail: WireDetail {
            text: String::new(),
            truncated: false,
        },
        characters: 0,
    };
    // 达到上限时主动中断 Display；这不是待传播的业务错误。
    let _ = write!(output, "{error}");
    output.detail
}

#[cfg(test)]
mod tests {
    use super::bounded_detail;

    #[test]
    fn detail_is_bounded_and_controls_are_escaped_without_splitting_unicode() {
        let exact = bounded_detail(&"中".repeat(256));
        assert_eq!(exact.text.chars().count(), 256);
        assert!(!exact.truncated);
        let longer = bounded_detail(&"中".repeat(257));
        assert_eq!(longer.text, exact.text);
        assert!(longer.truncated);
        let controls = bounded_detail(&"a\n\r\t\0\u{1b}中");
        assert!(!controls.text.chars().any(char::is_control));
        assert!(controls.text.contains("\\n"));
        let near_end = bounded_detail(&format!("{}\n", "a".repeat(255)));
        assert_eq!(near_end.text.len(), 255);
        assert!(near_end.truncated);
    }
}

/// 字节结构或字段约束错误；网络失败和会话资源限制由上层分类。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum WireErrorKind {
    /// peer-wire 协议名不匹配。
    ProtocolName,
    /// 标准握手 info-hash 不匹配。
    HandshakeHash,
    /// Bencode 字典损坏。
    MalformedBencode,
    /// 缺少 Bencode 字典。
    MissingDictionary,
    /// Bencode 根必须为字典。
    DictionaryRoot,
    /// Bencode 字典结构不合法。
    InvalidDictionary,
    /// 字段必须是整数。
    IntegerType,
    /// 整数溢出。
    IntegerOverflow,
    /// 扩展握手过大。
    ExtensionTooLarge,
    /// 扩展握手存在尾随字节。
    ExtensionTrailing,
    /// 扩展握手损坏。
    ExtensionMalformed,
    /// m 必须是字典。
    ExtensionMapType,
    /// m 字典损坏。
    ExtensionMapMalformed,
    /// 扩展 ID 必须在 0..255。
    ExtensionIdRange,
    /// metadata_size 不能为负。
    MetadataSizeRange,
    /// metadata 消息头损坏。
    MetadataHeaderMalformed,
    /// piece 不能为负。
    PieceRange,
    /// total_size 不能为负。
    TotalSizeRange,
    /// 缺少 piece。
    MissingPiece,
    /// 缺少 msg_type。
    MissingMessageType,
    /// data 缺少 total_size。
    MissingTotalSize,
    /// request/reject 不应带二进制数据。
    UnexpectedPayload,
    /// 分片编号或 total_size 不匹配。
    PieceOrSizeMismatch,
    /// 分片长度不符合 16 KiB/末片规则。
    PieceLength,
    /// 重复分片内容冲突。
    ConflictingPiece,
    /// 收到未请求的分片。
    UnrequestedPiece,
    /// 传输中 metadata_size 改变。
    MetadataSizeChanged,
    /// info 字典之后存在尾随数据。
    InfoTrailing,
    /// extended 消息缺少扩展 ID。
    MissingExtensionId,
    /// 扩展协商完成前收到 metadata 消息。
    MetadataBeforeNegotiation,
}
impl fmt::Display for WireErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ProtocolName => "peer-wire 协议名不匹配",
            Self::HandshakeHash => "标准握手 info-hash 不匹配",
            Self::MalformedBencode => "Bencode 字典损坏",
            Self::MissingDictionary => "缺少 Bencode 字典",
            Self::DictionaryRoot => "Bencode 根必须为字典",
            Self::InvalidDictionary => "Bencode 字典结构不合法",
            Self::IntegerType => "字段必须是整数",
            Self::IntegerOverflow => "整数溢出",
            Self::ExtensionTooLarge => "扩展握手过大",
            Self::ExtensionTrailing => "扩展握手存在尾随字节",
            Self::ExtensionMalformed => "扩展握手损坏",
            Self::ExtensionMapType => "m 必须是字典",
            Self::ExtensionMapMalformed => "m 字典损坏",
            Self::ExtensionIdRange => "扩展 ID 必须在 0..255",
            Self::MetadataSizeRange => "metadata_size 不能为负",
            Self::MetadataHeaderMalformed => "metadata 消息头损坏",
            Self::PieceRange => "piece 不能为负",
            Self::TotalSizeRange => "total_size 不能为负",
            Self::MissingPiece => "缺少 piece",
            Self::MissingMessageType => "缺少 msg_type",
            Self::MissingTotalSize => "data 缺少 total_size",
            Self::UnexpectedPayload => "request/reject 不应带二进制数据",
            Self::PieceOrSizeMismatch => "分片编号或 total_size 不匹配",
            Self::PieceLength => "分片长度不符合 16 KiB/末片规则",
            Self::ConflictingPiece => "重复分片内容冲突",
            Self::UnrequestedPiece => "收到未请求的分片",
            Self::MetadataSizeChanged => "传输中 metadata_size 改变",
            Self::InfoTrailing => "info 字典之后存在尾随数据",
            Self::MissingExtensionId => "extended 消息缺少扩展 ID",
            Self::MetadataBeforeNegotiation => "扩展协商完成前收到 metadata 消息",
        })
    }
}
