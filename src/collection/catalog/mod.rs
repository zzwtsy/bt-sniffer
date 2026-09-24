//! 已保存 metadata 的可重建查询目录；原始 info 仍由 metadata 表持有。

mod parser;
mod queries;

pub(crate) use parser::{TorrentFile, parse};

use rusqlite::{OptionalExtension, Transaction, params};

/// 在 metadata 所属事务内补齐目录。语义字段不可展示时仍写占位行，避免回填永久卡住。
pub(crate) fn ensure_catalog(
    transaction: &Transaction<'_>,
    hash: &[u8],
    info: &[u8],
) -> rusqlite::Result<bool> {
    let existing = transaction
        .query_row(
            "SELECT CASE WHEN semantic_status='pending' OR (semantic_status='invalid' AND semantic_reason='empty_directory') THEN NULL ELSE search_incomplete END FROM torrent_catalog WHERE hash=?1",
            [hash],
            |row| row.get::<_, Option<i64>>(0),
        )
        .optional()?;
    if matches!(existing, Some(Some(_))) {
        return Ok(false);
    }
    let mut analysis = super::metainfo::analyze(info);
    let piece_space = analysis
        .parsed
        .as_ref()
        .map(|p| p.piece_space_length.to_string());
    let padding = analysis
        .parsed
        .as_ref()
        .map(|p| p.padding_length.to_string());
    let parsed = analysis.parsed.take().map(parser::from_parsed);
    let search_incomplete = parsed.as_ref().is_none_or(|parsed| parsed.search_truncated);
    let (
        status,
        name,
        name_truncated,
        encoding_lossy,
        total,
        files,
        piece_length,
        pieces,
        private,
        search_text,
    ) = if let Some(parsed) = parsed {
        let (name, name_truncated) = parser::truncate_display(&parsed.name);
        (
            "parsed",
            Some(name),
            name_truncated,
            parsed.encoding_lossy,
            Some(parsed.total_length.to_string()),
            Some(i64::try_from(parsed.file_count).unwrap_or(i64::MAX)),
            Some(parsed.piece_length.to_string()),
            Some(i64::try_from(parsed.piece_count).unwrap_or(i64::MAX)),
            parsed.private,
            parsed.search_text,
        )
    } else {
        (
            "unavailable",
            None,
            false,
            false,
            None,
            None,
            None,
            None,
            None,
            String::new(),
        )
    };
    if existing.is_some() {
        transaction.execute(
            "UPDATE torrent_catalog SET
                parse_status=?2,name=?3,name_truncated=?4,encoding_lossy=?5,total_length=?6,
                file_count=?7,piece_length=?8,piece_count=?9,private=?10,search_text=?11,
                search_incomplete=?12
             WHERE hash=?1",
            params![
                hash,
                status,
                name,
                name_truncated,
                encoding_lossy,
                total,
                files,
                piece_length,
                pieces,
                private,
                search_text,
                search_incomplete,
            ],
        )?;
    } else {
        transaction.execute(
            "INSERT INTO torrent_catalog(
                hash,parse_status,name,name_truncated,encoding_lossy,total_length,
                file_count,piece_length,piece_count,private,search_text,search_incomplete
             ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
            params![
                hash,
                status,
                name,
                name_truncated,
                encoding_lossy,
                total,
                files,
                piece_length,
                pieces,
                private,
                search_text,
                search_incomplete,
            ],
        )?;
    }
    transaction.execute("UPDATE torrent_catalog SET format=?2,semantic_status=?3,semantic_reason=?4,piece_space_length=?5,padding_length=?6 WHERE hash=?1",
        params![hash, analysis.format, analysis.status, analysis.reason,
            piece_space, padding])?;
    Ok(true)
}
