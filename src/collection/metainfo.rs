//! 不重编码原始 info：语义检查与身份匹配分别报告，目录仅消费解析结果。
use crate::info_hash::{InfoHashV1, InfoHashV2, SwarmKey, TorrentIdentity};
use bendy::decoding::{Decoder, Object};
use sha1::{Digest, Sha1};
use sha2::Sha256;
use std::collections::BTreeMap;

const MAX_BYTES: usize = 4 * 1024 * 1024;
/// 结构与派生路径的保守预算；过大字典可保存，但不生成无界目录。
const PARSE_BUDGET: usize = 64 * 1024 * 1024;
#[derive(Debug)]
enum Value<'a> {
    Bytes(&'a [u8]),
    Integer(i64),
    List(Vec<Value<'a>>),
    Dict(BTreeMap<&'a [u8], Value<'a>>),
}
type Dict<'a> = BTreeMap<&'a [u8], Value<'a>>;
type Result<T> = std::result::Result<T, &'static str>;
fn charge(budget: &mut usize, amount: usize) -> Result<()> {
    *budget = budget.checked_sub(amount).ok_or("semantic_capacity")?;
    Ok(())
}
fn value<'a>(object: Object<'_, 'a>, budget: &mut usize) -> Result<Value<'a>> {
    charge(budget, 96)?;
    Ok(match object {
        Object::Bytes(bytes) => Value::Bytes(bytes),
        Object::Integer(text) => Value::Integer(text.parse().map_err(|_| "integer_range")?),
        Object::List(mut list) => {
            let mut values = Vec::new();
            while let Some(item) = list.next_object().map_err(|_| "invalid_bencode")? {
                values.push(value(item, budget)?);
            }
            Value::List(values)
        }
        Object::Dict(mut dict) => {
            let mut values = BTreeMap::new();
            while let Some((key, item)) = dict.next_pair().map_err(|_| "invalid_bencode")? {
                if values.insert(key, value(item, budget)?).is_some() {
                    return Err("duplicate_key");
                }
            }
            Value::Dict(values)
        }
    })
}
fn dictionary<'a>(v: &'a Value<'a>) -> Result<&'a Dict<'a>> {
    if let Value::Dict(d) = v {
        Ok(d)
    } else {
        Err("dictionary_type")
    }
}
fn bytes<'a>(v: &'a Value<'a>) -> Result<&'a [u8]> {
    if let Value::Bytes(b) = v {
        Ok(b)
    } else {
        Err("bytes_type")
    }
}
fn integer(v: &Value<'_>) -> Result<i64> {
    if let Value::Integer(n) = v {
        Ok(*n)
    } else {
        Err("integer_type")
    }
}
fn required<'a>(d: &'a Dict<'a>, key: &[u8]) -> Result<&'a Value<'a>> {
    d.get(key).ok_or("missing_field")
}
fn length(d: &Dict<'_>, key: &[u8]) -> Result<u64> {
    u64::try_from(integer(required(d, key)?)?).map_err(|_| "negative_length")
}
fn optional_bytes<'a>(d: &'a Dict<'a>, key: &[u8]) -> Result<Option<&'a [u8]>> {
    d.get(key).map(bytes).transpose()
}
fn path(v: &Value<'_>) -> Result<Vec<Vec<u8>>> {
    let Value::List(items) = v else {
        return Err("path_type");
    };
    if items.is_empty() {
        return Err("empty_path");
    }
    items
        .iter()
        .map(|item| {
            let b = bytes(item)?;
            segment(b)?;
            Ok(b.to_vec())
        })
        .collect()
}
fn segment(b: &[u8]) -> Result<()> {
    if b.is_empty()
        || b == b"."
        || b == b".."
        || b.contains(&b'/')
        || b.contains(&b'\\')
        || b.contains(&0)
    {
        Err("invalid_path")
    } else {
        Ok(())
    }
}
pub(crate) fn display(bytes: &[u8]) -> (String, bool) {
    use std::fmt::Write;
    let lossy = std::str::from_utf8(bytes).is_err();
    let mut text = String::new();
    for c in String::from_utf8_lossy(bytes).chars() {
        if c.is_control() {
            let _ = write!(text, "\\u{{{:x}}}", c as u32);
        } else {
            text.push(c);
        }
    }
    (text, lossy)
}
fn display_path(parts: &[Vec<u8>]) -> (String, bool) {
    let mut lossy = false;
    let path = parts
        .iter()
        .map(|part| {
            let (text, bad) = display(part);
            lossy |= bad;
            text
        })
        .collect::<Vec<_>>()
        .join("/");
    (path, lossy)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TorrentFile {
    pub(crate) path: Option<String>,
    pub(crate) length: u64,
    pub(crate) encoding_lossy: bool,
    pub(crate) kind: &'static str,
    pub(crate) hidden: bool,
    pub(crate) executable: bool,
    pub(crate) symlink_path: Option<String>,
    pub(crate) sha1: Option<String>,
    raw_path: Vec<Vec<u8>>,
    raw_target: Option<Vec<Vec<u8>>>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedInfo {
    pub(crate) format: &'static str,
    pub(crate) name: String,
    pub(crate) total_length: u64,
    pub(crate) piece_space_length: u64,
    pub(crate) padding_length: u64,
    pub(crate) file_count: usize,
    pub(crate) piece_length: u64,
    pub(crate) piece_count: usize,
    pub(crate) private: Option<bool>,
    pub(crate) files: Vec<TorrentFile>,
    pub(crate) encoding_lossy: bool,
}
#[derive(Debug)]
pub(crate) struct Analysis {
    pub(crate) format: &'static str,
    pub(crate) status: &'static str,
    pub(crate) reason: Option<&'static str>,
    pub(crate) parsed: Option<ParsedInfo>,
}
/// 原始字典边界由下载层另行严格验证；本入口也独立执行相同大小和深度约束。
pub(crate) fn analyze(info: &[u8]) -> Analysis {
    let mut format = "unknown";
    let parsed = (|| {
        if info.is_empty() || info.len() > MAX_BYTES {
            return Err("metadata_size");
        }
        let mut budget = PARSE_BUDGET;
        let mut decoder = Decoder::new(info).with_max_depth(64);
        let root = value(
            decoder
                .next_object()
                .map_err(|_| "invalid_bencode")?
                .ok_or("missing_dictionary")?,
            &mut budget,
        )?;
        if decoder
            .next_object()
            .map_err(|_| "invalid_bencode")?
            .is_some()
        {
            return Err("trailing_data");
        }
        let d = dictionary(&root)?;
        let version = d.get(b"meta version".as_slice()).map(integer).transpose()?;
        if version.is_some_and(|v| v != 2) {
            return Err("unsupported_meta_version");
        }
        let has_v1 = d.contains_key(b"pieces".as_slice());
        format = if version == Some(2) {
            if has_v1 { "hybrid" } else { "v2" }
        } else {
            "v1"
        };
        let name = optional_bytes(d, b"name.utf-8")?
            .filter(|v| std::str::from_utf8(v).is_ok())
            .or(optional_bytes(d, b"name")?);
        let (name, name_lossy) = display(name.unwrap_or_default());
        let piece_length = length(d, b"piece length")?;
        if piece_length == 0
            || (version == Some(2) && (piece_length < 16384 || !piece_length.is_power_of_two()))
        {
            return Err("piece_length");
        }
        let private = d
            .get(b"private".as_slice())
            .map(|v| match integer(v)? {
                0 => Ok(false),
                1 => Ok(true),
                _ => Err("private_flag"),
            })
            .transpose()?;
        let mut files = if version == Some(2) {
            let mut files = Vec::new();
            tree(
                dictionary(required(d, b"file tree")?)?,
                &mut Vec::new(),
                &mut files,
                &mut budget,
            )?;
            if files.is_empty() {
                return Err("empty_file_tree");
            }
            files
        } else {
            v1_files(d, &mut budget)?
        };
        if format == "hybrid" {
            let legacy = v1_files(d, &mut budget)?;
            check_v1(d, &legacy, piece_length)?;
            let regular: Vec<_> = legacy.iter().filter(|f| f.kind != "padding").collect();
            if regular.len() != files.len() {
                return Err("hybrid_layout");
            }
            let mut offset = 0u64;
            let mut v2_end = 0u64;
            let mut index = 0;
            for file in &legacy {
                if file.kind != "padding" {
                    let v2 = &files[index];
                    if file.length > 0 {
                        let aligned = v2_end
                            .checked_add((piece_length - v2_end % piece_length) % piece_length)
                            .ok_or("length_overflow")?;
                        if offset != aligned {
                            return Err("hybrid_layout");
                        }
                        v2_end = aligned.checked_add(file.length).ok_or("length_overflow")?;
                    }
                    if file.raw_path != v2.raw_path
                        || file.length != v2.length
                        || file.kind != v2.kind
                        || file.raw_target != v2.raw_target
                        || (file.length > 0 && !offset.is_multiple_of(piece_length))
                    {
                        return Err("hybrid_layout");
                    }
                    index += 1;
                }
                offset = offset.checked_add(file.length).ok_or("length_overflow")?;
            }
            let end_boundary = v2_end
                .checked_add((piece_length - v2_end % piece_length) % piece_length)
                .ok_or("length_overflow")?;
            if offset > end_boundary {
                return Err("hybrid_layout");
            }
            files = legacy;
        }
        let mut total_length = 0u64;
        let mut padding_length = 0u64;
        let mut piece_space_length = 0u64;
        let mut piece_count = 0u64;
        for file in &files {
            if file.kind == "padding" {
                padding_length = padding_length
                    .checked_add(file.length)
                    .ok_or("length_overflow")?;
            } else {
                total_length = total_length
                    .checked_add(file.length)
                    .ok_or("length_overflow")?;
            }
            if format == "v2" && file.length > 0 {
                piece_space_length = piece_space_length
                    .checked_add((piece_length - piece_space_length % piece_length) % piece_length)
                    .ok_or("length_overflow")?;
                piece_count = piece_count
                    .checked_add(file.length.div_ceil(piece_length))
                    .ok_or("length_overflow")?;
            }
            piece_space_length = piece_space_length
                .checked_add(file.length)
                .ok_or("length_overflow")?;
        }
        if format != "v2" {
            piece_count = check_v1(d, &files, piece_length)?;
        }
        // 原始字节参与重复、层级和链接比较，显示替换字符不参与协议判断。
        let mut paths: Vec<_> = files
            .iter()
            .filter(|f| f.kind != "padding")
            .map(|f| &f.raw_path)
            .collect();
        paths.sort_unstable();
        if paths.windows(2).any(|pair| pair[1].starts_with(pair[0])) {
            return Err("path_conflict");
        }
        for file in &files {
            if let Some(target) = &file.raw_target {
                match paths.binary_search(&target) {
                    Ok(_) => {}
                    Err(index) if paths.get(index).is_some_and(|p| p.starts_with(target)) => {}
                    Err(_) => return Err("dangling_symlink"),
                }
            }
        }
        Ok(ParsedInfo {
            format,
            name,
            total_length,
            piece_space_length,
            padding_length,
            file_count: files.iter().filter(|f| f.kind != "padding").count(),
            piece_length,
            piece_count: usize::try_from(piece_count).map_err(|_| "piece_count_range")?,
            private,
            encoding_lossy: name_lossy || files.iter().any(|f| f.encoding_lossy),
            files,
        })
    })();
    match parsed {
        Ok(parsed) => Analysis {
            format,
            status: "valid",
            reason: None,
            parsed: Some(parsed),
        },
        Err(reason) => Analysis {
            format,
            status: if reason == "unsupported_meta_version" {
                "unsupported"
            } else {
                "invalid"
            },
            reason: Some(reason),
            parsed: None,
        },
    }
}
fn file(
    d: &Dict<'_>,
    raw_path: Vec<Vec<u8>>,
    shown: Option<Vec<Vec<u8>>>,
    budget: &mut usize,
) -> Result<TorrentFile> {
    let attr = optional_bytes(d, b"attr")?.unwrap_or_default();
    let kind = if attr.contains(&b'p') {
        "padding"
    } else if attr.contains(&b'l') {
        "symlink"
    } else {
        "file"
    };
    if attr.contains(&b'p') && attr.contains(&b'l') {
        return Err("file_attributes");
    }
    let length = if kind == "symlink" && !d.contains_key(b"length".as_slice()) {
        0
    } else {
        length(d, b"length")?
    };
    if kind == "symlink" && length != 0 {
        return Err("symlink_length");
    }
    let raw_target = if kind == "symlink" {
        Some(path(required(d, b"symlink path")?)?)
    } else {
        None
    };
    let sha1 = optional_bytes(d, b"sha1")?
        .map(|b| {
            if b.len() == 20 {
                Ok(crate::observation::hex(b))
            } else {
                Err("file_sha1_length")
            }
        })
        .transpose()?;
    charge(
        budget,
        256 + raw_path.len() * 32
            + raw_path.iter().map(Vec::len).sum::<usize>() * 8
            + raw_target
                .as_ref()
                .map_or(0, |p| p.iter().map(Vec::len).sum::<usize>() * 8),
    )?;
    charge(
        budget,
        shown
            .as_ref()
            .map_or(0, |p| p.iter().map(|s| s.len() + 1).sum::<usize>() * 2),
    )?;
    let (path, encoding_lossy) = shown
        .as_ref()
        .map(|p| {
            let (s, l) = display_path(p);
            (Some(s), l)
        })
        .unwrap_or((None, false));
    Ok(TorrentFile {
        path,
        length,
        encoding_lossy,
        kind,
        hidden: attr.contains(&b'h'),
        executable: attr.contains(&b'x'),
        symlink_path: raw_target.as_ref().map(|p| display_path(p).0),
        sha1,
        raw_path,
        raw_target,
    })
}
fn v1_files(d: &Dict<'_>, budget: &mut usize) -> Result<Vec<TorrentFile>> {
    let raw_name = bytes(required(d, b"name")?)?;
    segment(raw_name)?;
    let display_name = optional_bytes(d, b"name.utf-8")?
        .filter(|b| std::str::from_utf8(b).is_ok())
        .unwrap_or(raw_name);
    match (d.get(b"length".as_slice()), d.get(b"files".as_slice())) {
        (_, Some(Value::List(items)))
            if !d.contains_key(b"length".as_slice()) && !items.is_empty() =>
        {
            let mut out = Vec::new();
            for item in items {
                let f = dictionary(item)?;
                let padding = optional_bytes(f, b"attr")?.is_some_and(|a| a.contains(&b'p'));
                let raw = f
                    .get(b"path".as_slice())
                    .or(f.get(b"path.utf-8".as_slice()))
                    .map(path)
                    .transpose()?;
                if raw.is_none() && !padding {
                    return Err("missing_path");
                }
                let preferred = f
                    .get(b"path.utf-8".as_slice())
                    .map(path)
                    .transpose()?
                    .filter(|p| p.iter().all(|s| std::str::from_utf8(s).is_ok()))
                    .or_else(|| raw.clone());
                let shown = preferred.map(|p| {
                    let mut full = vec![display_name.to_vec()];
                    full.extend(p);
                    full
                });
                // v2 文件树不包含 v1 的展示根名，比较使用 torrent 内相对路径。
                out.push(file(f, raw.unwrap_or_default(), shown, budget)?);
            }
            Ok(out)
        }
        (_, None) => Ok(vec![file(
            d,
            vec![raw_name.to_vec()],
            Some(vec![display_name.to_vec()]),
            budget,
        )?]),
        _ => Err("file_layout"),
    }
}
fn check_v1(d: &Dict<'_>, files: &[TorrentFile], piece_length: u64) -> Result<u64> {
    let pieces = bytes(required(d, b"pieces")?)?;
    let total = files.iter().try_fold(0u64, |sum, f| {
        sum.checked_add(f.length).ok_or("length_overflow")
    })?;
    if pieces.len() % 20 != 0 || pieces.len() as u64 / 20 != total.div_ceil(piece_length) {
        return Err("piece_count");
    }
    Ok(pieces.len() as u64 / 20)
}
fn tree(
    d: &Dict<'_>,
    path: &mut Vec<Vec<u8>>,
    out: &mut Vec<TorrentFile>,
    budget: &mut usize,
) -> Result<()> {
    if let Some(leaf) = d.get(b"".as_slice()) {
        if path.is_empty() || d.len() != 1 {
            return Err("file_tree_conflict");
        }
        let properties = dictionary(leaf)?;
        let f = file(properties, path.clone(), Some(path.clone()), budget)?;
        if f.kind == "padding" {
            return Err("v2_padding_file");
        }
        let root = optional_bytes(properties, b"pieces root")?;
        if (f.length > 0 && root.is_none()) || root.is_some_and(|r| r.len() != 32) {
            return Err("pieces_root");
        }
        out.push(f);
    } else {
        for (name, child) in d {
            segment(name)?;
            path.push(name.to_vec());
            tree(dictionary(child)?, path, out, budget)?;
            path.pop();
        }
    }
    Ok(())
}
/// 仅读取版本，不要求目录语义有效；完整字典验证仍由调用者负责。
pub(crate) fn is_v2(info: &[u8]) -> bool {
    let mut decoder = Decoder::new(info).with_max_depth(64);
    let Ok(Some(Object::Dict(mut dict))) = decoder.next_object() else {
        return false;
    };
    while let Ok(Some((key, object))) = dict.next_pair() {
        if key == b"meta version" {
            return matches!(object, Object::Integer("2"));
        }
    }
    false
}
/// 返回实际匹配的完整身份；v2 路径仅承诺前 20 字节与发现键相等。
pub(crate) fn match_identity(info: &[u8], key: SwarmKey) -> Option<TorrentIdentity> {
    let sha1: [u8; 20] = Sha1::digest(info).into();
    if sha1 == key.0 {
        return Some(TorrentIdentity::V1(InfoHashV1(sha1)));
    }
    if is_v2(info) {
        let sha256: [u8; 32] = Sha256::digest(info).into();
        if sha256[..20] == key.0 {
            return Some(TorrentIdentity::V2(InfoHashV2(sha256)));
        }
    }
    None
}
/// 只有有效 hybrid 才主动拓展另一身份；单独匹配来源不证明双布局一致。
pub(crate) fn identities(
    info: &[u8],
    source: TorrentIdentity,
    analysis: &Analysis,
) -> Vec<TorrentIdentity> {
    if analysis.status == "valid" && analysis.format == "hybrid" {
        vec![
            TorrentIdentity::V1(InfoHashV1(Sha1::digest(info).into())),
            TorrentIdentity::V2(InfoHashV2(Sha256::digest(info).into())),
        ]
    } else {
        vec![source]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn independent_protocol_fixtures_and_digests() {
        let cases: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/manifest.json")).unwrap();
        for case in cases.as_array().unwrap() {
            let name = case["name"].as_str().unwrap();
            let info = std::fs::read(format!(
                "{}/src/collection/fixtures/{name}.info",
                env!("CARGO_MANIFEST_DIR")
            ))
            .unwrap();
            assert_eq!(
                crate::observation::hex(&Sha1::digest(&info)),
                case["sha1"],
                "{name}"
            );
            assert_eq!(
                crate::observation::hex(&Sha256::digest(&info)),
                case["sha256"],
                "{name}"
            );
            let result = analyze(&info);
            assert_eq!(result.status, case["status"], "{name}: {result:?}");
            assert_eq!(
                serde_json::to_value(result.reason).unwrap(),
                case["reason"],
                "{name}"
            );
        }
    }
    #[test]
    fn source_matching_and_hybrid_aliases_are_distinct() {
        for info in [
            include_bytes!("fixtures/v2.info").as_slice(),
            include_bytes!("fixtures/hybrid.info"),
            include_bytes!("fixtures/hybrid-mismatch.info"),
        ] {
            let full = InfoHashV2(Sha256::digest(info).into());
            let source = TorrentIdentity::V2(full);
            assert_eq!(match_identity(info, source.swarm_key()), Some(source));
            assert_eq!(match_identity(info, SwarmKey([0; 20])), None);
            let analysis = analyze(info);
            let aliases = identities(info, source, &analysis);
            assert_eq!(
                aliases.len(),
                if analysis.format == "hybrid" && analysis.status == "valid" {
                    2
                } else {
                    1
                }
            );
        }
    }
    #[test]
    fn padding_stats_and_stable_order() {
        let parsed = analyze(include_bytes!("fixtures/bep47.info"))
            .parsed
            .unwrap();
        assert_eq!(
            (
                parsed.total_length,
                parsed.piece_space_length,
                parsed.padding_length,
                parsed.file_count
            ),
            (1, 16384, 16383, 2)
        );
        assert_eq!(parsed.files[1].path, None);
        assert_eq!(parsed.files[2].kind, "symlink");
        assert!(parsed.files[0].hidden && parsed.files[0].executable);
        assert_eq!(
            analyze(&vec![0; MAX_BYTES + 1]).reason,
            Some("metadata_size")
        );
        let nested = [b"d1:a".as_slice(), &[b'l'; 65], &[b'e'; 66]].concat();
        assert_eq!(analyze(&nested).status, "invalid");
    }
    #[test]
    fn empty_directories_do_not_contribute_files_or_piece_space() {
        for info in [
            include_bytes!("fixtures/v2-empty-directory.info").as_slice(),
            include_bytes!("fixtures/hybrid-empty-directory.info"),
        ] {
            let parsed = analyze(info).parsed.unwrap();
            assert_eq!(parsed.file_count, 1);
            assert_eq!(parsed.total_length, 1);
            assert_eq!(parsed.piece_space_length, 1);
            assert_eq!(parsed.piece_count, 1);
        }
        assert_eq!(
            analyze(b"d9:file treed5:emptydee12:meta versioni2e12:piece lengthi16384ee").reason,
            Some("empty_file_tree")
        );
        assert_eq!(
            analyze(include_bytes!("fixtures/v2-tree-conflict.info")).reason,
            Some("file_tree_conflict")
        );
    }
}
