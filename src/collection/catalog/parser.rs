//! 语义解析由 metainfo 负责；此处只构造有界展示与搜索字段。
use crate::collection::metainfo;
pub(crate) use crate::collection::metainfo::TorrentFile;
pub(super) const DISPLAY_TEXT_BYTES: usize = 4096;
const SEARCH_TEXT_BYTES: usize = 12 * 1024 * 1024;
pub(crate) struct ParsedInfo {
    info: metainfo::ParsedInfo,
    pub(crate) search_text: String,
    pub(crate) search_truncated: bool,
}
impl std::ops::Deref for ParsedInfo {
    type Target = metainfo::ParsedInfo;
    fn deref(&self) -> &Self::Target {
        &self.info
    }
}
pub(crate) fn parse(bytes: &[u8]) -> Option<ParsedInfo> {
    Some(from_parsed(metainfo::analyze(bytes).parsed?))
}
pub(super) fn from_parsed(info: metainfo::ParsedInfo) -> ParsedInfo {
    let mut search_text = String::new();
    let mut search_truncated = !push_bounded(&mut search_text, &info.name, SEARCH_TEXT_BYTES);
    for file in &info.files {
        if search_truncated {
            break;
        }
        if file.kind == "padding" {
            continue;
        }
        let Some(path) = &file.path else {
            continue;
        };
        if search_text
            .len()
            .saturating_add(1)
            .saturating_add(path.len())
            > SEARCH_TEXT_BYTES
        {
            search_truncated = true;
            break;
        }
        search_text.push('\n');
        search_text.push_str(path);
    }
    ParsedInfo {
        info,
        search_text,
        search_truncated,
    }
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
        let single = parse(b"d6:lengthi42e4:name8:file.txt12:piece lengthi64e6:pieces20:aaaaaaaaaaaaaaaaaaaa7:privatei1ee").unwrap();
        assert_eq!(single.name, "file.txt");
        assert_eq!(single.total_length, 42);
        assert_eq!(single.piece_count, 1);
        assert_eq!(single.files[0].path.as_deref(), Some("file.txt"));
        assert_eq!(single.private, Some(true));

        let multi = parse(b"d5:filesld6:lengthi2e4:pathl1:a1:beed6:lengthi3e10:path.utf-8l3:\xe4\xb8\xadeee4:name4:root12:piece lengthi4e6:pieces40:aaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbe").unwrap();
        assert_eq!(multi.total_length, 5);
        assert_eq!(multi.files.len(), 2);
        assert_eq!(multi.files[0].path.as_deref(), Some("root/a/b"));
        assert_eq!(multi.files[1].path.as_deref(), Some("root/中"));
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
        let expected_prefix = format!("{root}/");
        let mut info = b"d5:filesl".to_vec();
        for index in 0..FILES {
            info.extend_from_slice(format!("d6:lengthi0e4:pathl5:{index:05}ee").as_bytes());
        }
        info.extend_from_slice(b"e4:name255:");
        info.extend_from_slice(root.as_bytes());
        info.extend_from_slice(b"12:piece lengthi1e6:pieces0:e");
        assert!(info.len() < 4 * 1024 * 1024);

        let parsed = parse(&info).unwrap();
        assert_eq!(parsed.files.len(), FILES);
        assert!(parsed.search_truncated);
        assert!(parsed.search_text.len() <= SEARCH_TEXT_BYTES);
        assert!(parsed.search_text.lines().all(|path| path == root.as_str()
            || (path.starts_with(&expected_prefix) && path.len() == expected_prefix.len() + 5)));
        assert!(parsed.search_text.lines().count() < FILES + 1);
    }
}
