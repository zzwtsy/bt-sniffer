//! 将已校验的 v1 info 字典转换为只读目录字段；不重编码也不改变采集接纳规则。

use serde::{Deserialize, Deserializer};
use serde_bytes::ByteBuf;

fn optional<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

pub(super) const DISPLAY_TEXT_BYTES: usize = 4096;
const SEARCH_TEXT_BYTES: usize = 12 * 1024 * 1024;

#[derive(Debug, Deserialize)]
struct RawInfo {
    #[serde(default, deserialize_with = "optional")]
    name: Option<ByteBuf>,
    #[serde(rename = "name.utf-8")]
    #[serde(default, deserialize_with = "optional")]
    name_utf8: Option<ByteBuf>,
    #[serde(default, deserialize_with = "optional")]
    length: Option<i64>,
    #[serde(default, deserialize_with = "optional")]
    files: Option<Vec<RawFile>>,
    #[serde(rename = "piece length")]
    #[serde(default, deserialize_with = "optional")]
    piece_length: Option<i64>,
    #[serde(default, deserialize_with = "optional")]
    pieces: Option<ByteBuf>,
    #[serde(default, deserialize_with = "optional")]
    private: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct RawFile {
    length: i64,
    #[serde(default, deserialize_with = "optional")]
    path: Option<Vec<ByteBuf>>,
    #[serde(rename = "path.utf-8")]
    #[serde(default, deserialize_with = "optional")]
    path_utf8: Option<Vec<ByteBuf>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TorrentFile {
    pub(crate) path: String,
    pub(crate) length: u64,
    pub(crate) encoding_lossy: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedInfo {
    pub(crate) name: String,
    pub(crate) total_length: u64,
    pub(crate) piece_length: u64,
    pub(crate) piece_count: usize,
    pub(crate) private: Option<bool>,
    pub(crate) files: Vec<TorrentFile>,
    pub(crate) encoding_lossy: bool,
    pub(crate) search_text: String,
    /// 派生子串索引触及字节上限，后续完整路径没有加入索引。
    pub(crate) search_truncated: bool,
}

pub(crate) fn parse(bytes: &[u8]) -> Option<ParsedInfo> {
    let raw: RawInfo = bendy::serde::from_bytes(bytes).ok()?;
    let (name, name_lossy) = decode_preferred(
        raw.name_utf8.as_ref().map(ByteBuf::as_ref),
        raw.name.as_ref().map(ByteBuf::as_ref),
    )?;
    let piece_length = u64::try_from(raw.piece_length?).ok().filter(|n| *n > 0)?;
    let pieces = raw.pieces?;
    if pieces.len() % 20 != 0 {
        return None;
    }
    let private = match raw.private {
        None => None,
        Some(0) => Some(false),
        Some(1) => Some(true),
        Some(_) => return None,
    };
    let files = match (raw.length, raw.files) {
        (Some(length), None) => vec![TorrentFile {
            path: name.clone(),
            length: u64::try_from(length).ok()?,
            encoding_lossy: name_lossy,
        }],
        (None, Some(files)) if !files.is_empty() => {
            let mut output = Vec::with_capacity(files.len());
            for file in files {
                let segments = preferred_path(file.path_utf8.as_deref(), file.path.as_deref())?;
                let mut path = name.clone();
                let mut lossy = name_lossy;
                for segment in segments {
                    let (segment, segment_lossy) = decode(segment);
                    if segment.is_empty() {
                        return None;
                    }
                    path.push('/');
                    path.push_str(&segment);
                    lossy |= segment_lossy;
                }
                output.push(TorrentFile {
                    path,
                    length: u64::try_from(file.length).ok()?,
                    encoding_lossy: lossy,
                });
            }
            output
        }
        _ => return None,
    };
    let total_length = files
        .iter()
        .try_fold(0u64, |total, file| total.checked_add(file.length))?;
    let encoding_lossy = name_lossy || files.iter().any(|file| file.encoding_lossy);
    let mut search_text = String::new();
    let mut search_truncated = !push_bounded(&mut search_text, &name, SEARCH_TEXT_BYTES);
    for file in &files {
        if search_truncated {
            break;
        }
        if search_text
            .len()
            .saturating_add(1)
            .saturating_add(file.path.len())
            > SEARCH_TEXT_BYTES
        {
            search_truncated = true;
            break;
        }
        search_text.push('\n');
        search_text.push_str(&file.path);
    }
    Some(ParsedInfo {
        name,
        total_length,
        piece_length,
        piece_count: pieces.len() / 20,
        private,
        files,
        encoding_lossy,
        search_text,
        search_truncated,
    })
}

fn preferred_path<'a>(
    utf8: Option<&'a [ByteBuf]>,
    legacy: Option<&'a [ByteBuf]>,
) -> Option<Vec<&'a [u8]>> {
    if let Some(path) = utf8.filter(|path| {
        !path.is_empty() && path.iter().all(|part| std::str::from_utf8(part).is_ok())
    }) {
        return Some(path.iter().map(AsRef::as_ref).collect());
    }
    legacy
        .filter(|path| !path.is_empty())
        .map(|path| path.iter().map(AsRef::as_ref).collect())
}

fn decode_preferred(utf8: Option<&[u8]>, legacy: Option<&[u8]>) -> Option<(String, bool)> {
    if let Some(bytes) = utf8.filter(|bytes| std::str::from_utf8(bytes).is_ok()) {
        return Some(decode(bytes));
    }
    legacy.map(decode)
}

fn decode(bytes: &[u8]) -> (String, bool) {
    let lossy = std::str::from_utf8(bytes).is_err();
    let decoded = String::from_utf8_lossy(bytes);
    let mut output = String::with_capacity(decoded.len());
    for character in decoded.chars() {
        if character.is_control() {
            use std::fmt::Write;
            let _ = write!(output, "\\u{{{:x}}}", character as u32);
        } else {
            output.push(character);
        }
    }
    (output, lossy)
}

pub(super) fn truncate_display(value: &str) -> (String, bool) {
    if value.len() <= DISPLAY_TEXT_BYTES {
        return (value.to_owned(), false);
    }
    let mut end = DISPLAY_TEXT_BYTES.saturating_sub('…'.len_utf8());
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    let mut output = value[..end].to_owned();
    output.push('…');
    (output, true)
}

fn push_bounded(output: &mut String, value: &str, limit: usize) -> bool {
    if output.len().saturating_add(value.len()) > limit {
        return false;
    }
    output.push_str(value);
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_single_and_multi_file_info() {
        let single = parse(b"d6:lengthi42e4:name8:file.txt12:piece lengthi16e6:pieces20:aaaaaaaaaaaaaaaaaaaa7:privatei1ee").unwrap();
        assert_eq!(single.name, "file.txt");
        assert_eq!(single.total_length, 42);
        assert_eq!(single.piece_count, 1);
        assert_eq!(single.files[0].path, "file.txt");
        assert_eq!(single.private, Some(true));

        let multi = parse(b"d5:filesld6:lengthi2e4:pathl1:a1:beed6:lengthi3e10:path.utf-8l3:\xe4\xb8\xadeee4:name4:root12:piece lengthi4e6:pieces40:aaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbe").unwrap();
        assert_eq!(multi.total_length, 5);
        assert_eq!(multi.files.len(), 2);
        assert_eq!(multi.files[0].path, "root/a/b");
        assert_eq!(multi.files[1].path, "root/中");
    }

    #[test]
    fn utf8_precedence_lossy_controls_and_invalid_shapes() {
        let preferred = parse(b"d6:lengthi1e4:name6:legacy10:name.utf-83:new12:piece lengthi1e6:pieces20:aaaaaaaaaaaaaaaaaaaae").unwrap();
        assert_eq!(preferred.name, "new");

        let lossy =
            parse(b"d6:lengthi1e4:name2:\xff\n12:piece lengthi1e6:pieces20:aaaaaaaaaaaaaaaaaaaae")
                .unwrap();
        assert!(lossy.encoding_lossy);
        assert!(lossy.name.contains("\\u{a}"));

        for invalid in [
            &b"d4:name1:x6:pieces0:e"[..],
            &b"d6:lengthi-1e4:name1:x12:piece lengthi1e6:pieces0:e"[..],
            &b"d6:lengthi1e5:filesle4:name1:x12:piece lengthi1e6:pieces0:e"[..],
            &b"d6:lengthi1e4:name1:x12:piece lengthi0e6:pieces0:e"[..],
        ] {
            assert!(
                parse(invalid).is_none(),
                "{}",
                String::from_utf8_lossy(invalid)
            );
        }
    }

    #[test]
    fn display_values_are_bounded_on_utf8_boundaries() {
        let value = "中".repeat(DISPLAY_TEXT_BYTES);
        let (display, truncated) = truncate_display(&value);
        assert!(truncated);
        assert!(display.len() <= DISPLAY_TEXT_BYTES);
        assert!(display.ends_with('…'));
    }

    #[test]
    fn large_file_list_reports_path_search_truncation_without_partial_paths() {
        const FILES: usize = 50_000;
        let root = "r".repeat(255);
        let expected_path = format!("{root}/x");
        let mut info = b"d5:filesl".to_vec();
        for _ in 0..FILES {
            info.extend_from_slice(b"d6:lengthi0e4:pathl1:xee");
        }
        info.extend_from_slice(b"e4:name255:");
        info.extend_from_slice(root.as_bytes());
        info.extend_from_slice(b"12:piece lengthi1e6:pieces0:e");
        assert!(info.len() < 4 * 1024 * 1024);

        let parsed = parse(&info).unwrap();
        assert_eq!(parsed.files.len(), FILES);
        assert!(parsed.search_truncated);
        assert!(parsed.search_text.len() <= SEARCH_TEXT_BYTES);
        assert!(
            parsed
                .search_text
                .lines()
                .all(|path| path == root.as_str() || path == expected_path.as_str())
        );
        assert!(parsed.search_text.lines().count() < FILES + 1);
    }
}
