//! 目录分页、详情读取与单条后台回填；所有 SQL 仍串行经过唯一数据库线程。

use super::{TorrentFile, ensure_catalog, parse, parser::truncate_display};
use crate::{
    collection::{inspection::ReadError, store::CollectionStore},
    info_hash::InfoHashV1,
    observation::hex,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::Serialize;
use sha1::{Digest, Sha1};
use std::time::{Duration, Instant};
use tokio::sync::OwnedSemaphorePermit;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize)]
pub(crate) struct IndexState {
    indexed: i64,
    total: i64,
    complete: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct CatalogItem {
    hash: String,
    parse_status: String,
    name: Option<String>,
    name_truncated: bool,
    encoding_lossy: bool,
    total_length: Option<String>,
    file_count: Option<i64>,
    piece_length: Option<String>,
    piece_count: Option<i64>,
    private: Option<bool>,
    fetched_at_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    match_excerpt: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct CatalogPage {
    items: Vec<CatalogItem>,
    next: Option<String>,
    index: IndexState,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct TorrentDetail {
    hash: String,
    parse_status: &'static str,
    name: Option<String>,
    name_truncated: bool,
    encoding_lossy: bool,
    total_length: Option<String>,
    file_count: Option<usize>,
    piece_length: Option<String>,
    piece_count: Option<usize>,
    private: Option<bool>,
    fetched_at_ms: i64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct FileItem {
    index: usize,
    path: String,
    path_truncated: bool,
    encoding_lossy: bool,
    length: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct FilePage {
    available: bool,
    items: Vec<FileItem>,
    next: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct BackfillStep {
    pub(crate) cursor: Option<[u8; 20]>,
    pub(crate) complete: bool,
}

struct Progress<'a>(&'a Connection);
impl<'a> Progress<'a> {
    fn install(connection: &'a Connection, cancel: CancellationToken) -> rusqlite::Result<Self> {
        let until = Instant::now() + Duration::from_millis(100);
        connection.progress_handler(
            1000,
            Some(move || cancel.is_cancelled() || Instant::now() >= until),
        )?;
        Ok(Self(connection))
    }
}
impl Drop for Progress<'_> {
    fn drop(&mut self) {
        let _ = self.0.progress_handler(0, None::<fn() -> bool>);
    }
}

impl CollectionStore {
    pub(crate) async fn catalog_page(
        &self,
        query: Option<String>,
        after: Option<String>,
        limit: usize,
        permit: OwnedSemaphorePermit,
        cancel: CancellationToken,
    ) -> Result<CatalogPage, ReadError> {
        self.call(move |connection| {
            let _permit = permit;
            if cancel.is_cancelled() {
                return Ok(Err(ReadError::Cancelled));
            }
            let _progress = Progress::install(connection, cancel)?;
            Ok(read_catalog_page(connection, query, after, limit))
        })
        .await
        .map_err(|_| ReadError::Unavailable)?
    }

    pub(crate) async fn torrent_detail(
        &self,
        hash: InfoHashV1,
        permit: OwnedSemaphorePermit,
        cancel: CancellationToken,
    ) -> Result<TorrentDetail, ReadError> {
        self.call(move |connection| {
            let _permit = permit;
            if cancel.is_cancelled() {
                return Ok(Err(ReadError::Cancelled));
            }
            let _progress = Progress::install(connection, cancel)?;
            Ok(read_detail(connection, hash))
        })
        .await
        .map_err(|_| ReadError::Unavailable)?
    }

    pub(crate) async fn torrent_files(
        &self,
        hash: InfoHashV1,
        after: Option<String>,
        limit: usize,
        permit: OwnedSemaphorePermit,
        cancel: CancellationToken,
    ) -> Result<FilePage, ReadError> {
        self.call(move |connection| {
            let _permit = permit;
            if cancel.is_cancelled() {
                return Ok(Err(ReadError::Cancelled));
            }
            let _progress = Progress::install(connection, cancel)?;
            Ok(read_files(connection, hash, after, limit))
        })
        .await
        .map_err(|_| ReadError::Unavailable)?
    }

    /// 单次只补一条；等待 future 被取消不会撤销已经进入数据库线程的事务。
    pub(crate) async fn backfill_catalog_one(
        &self,
        cursor: Option<[u8; 20]>,
        permit: OwnedSemaphorePermit,
        cancel: CancellationToken,
    ) -> Result<BackfillStep, ReadError> {
        self.call(move |connection| {
            let _permit = permit;
            if cancel.is_cancelled() {
                return Ok(Err(ReadError::Cancelled));
            }
            Ok(backfill_one(connection, cursor))
        })
        .await
        .map_err(|_| ReadError::Unavailable)?
    }
}

fn read_catalog_page(
    connection: &Connection,
    query: Option<String>,
    after: Option<String>,
    limit: usize,
) -> Result<CatalogPage, ReadError> {
    if !(1..=100).contains(&limit) {
        return Err(ReadError::Invalid);
    }
    let query = query.map(|q| q.trim().to_owned()).filter(|q| !q.is_empty());
    if query
        .as_ref()
        .is_some_and(|q| !(3..=200).contains(&q.chars().count()))
    {
        return Err(ReadError::Invalid);
    }
    let cursor = after.map(|value| parse_cursor(&value)).transpose()?;
    let tx = connection.unchecked_transaction().map_err(sql_error)?;
    let items = if let Some(query) = query {
        let literal = format!("\"{}\"", query.replace('"', "\"\""));
        let mut statement = tx
            .prepare(
                "SELECT c.hash,c.parse_status,c.name,c.name_truncated,c.encoding_lossy,
                        c.total_length,c.file_count,c.piece_length,c.piece_count,c.private,
                        m.fetched_at,snippet(torrent_catalog_fts,0,'','', ' … ',24)
                 FROM torrent_catalog_fts
                 JOIN torrent_catalog c ON c.id=torrent_catalog_fts.rowid
                 JOIN metadata m ON m.hash=c.hash
                 WHERE torrent_catalog_fts MATCH ?1
                   AND (?2 IS NULL OR m.fetched_at<?2 OR (m.fetched_at=?2 AND c.hash<?3))
                 ORDER BY m.fetched_at DESC,c.hash DESC LIMIT ?4",
            )
            .map_err(sql_error)?;
        statement
            .query_map(
                params![
                    literal,
                    cursor.as_ref().map(|c| c.0),
                    cursor.as_ref().map(|c| c.1.as_slice()),
                    (limit + 1) as i64,
                ],
                |row| catalog_item(row, true),
            )
            .map_err(sql_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql_error)?
    } else {
        let mut statement = tx
            .prepare(
                "SELECT c.hash,c.parse_status,c.name,c.name_truncated,c.encoding_lossy,
                        c.total_length,c.file_count,c.piece_length,c.piece_count,c.private,
                        m.fetched_at,NULL
                 FROM torrent_catalog c JOIN metadata m ON m.hash=c.hash
                 WHERE (?1 IS NULL OR m.fetched_at<?1 OR (m.fetched_at=?1 AND c.hash<?2))
                 ORDER BY m.fetched_at DESC,c.hash DESC LIMIT ?3",
            )
            .map_err(sql_error)?;
        statement
            .query_map(
                params![
                    cursor.as_ref().map(|c| c.0),
                    cursor.as_ref().map(|c| c.1.as_slice()),
                    (limit + 1) as i64,
                ],
                |row| catalog_item(row, false),
            )
            .map_err(sql_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql_error)?
    };
    let mut items = items;
    let more = items.len() > limit;
    items.truncate(limit);
    let next = if more {
        items.last().and_then(|item| {
            parse_hash(&item.hash).map(|hash| encode_catalog_cursor(item.fetched_at_ms, hash))
        })
    } else {
        None
    };
    let index = index_state(&tx)?;
    tx.commit().map_err(sql_error)?;
    Ok(CatalogPage { items, next, index })
}

fn catalog_item(row: &rusqlite::Row<'_>, matched: bool) -> rusqlite::Result<CatalogItem> {
    let hash = row.get::<_, Vec<u8>>(0)?;
    let excerpt = row.get::<_, Option<String>>(11)?;
    let excerpt = if matched {
        excerpt.map(|value| truncate_display(&value).0)
    } else {
        None
    };
    Ok(CatalogItem {
        hash: hex(&hash),
        parse_status: row.get(1)?,
        name: row.get(2)?,
        name_truncated: row.get(3)?,
        encoding_lossy: row.get(4)?,
        total_length: row.get(5)?,
        file_count: row.get(6)?,
        piece_length: row.get(7)?,
        piece_count: row.get(8)?,
        private: row.get(9)?,
        fetched_at_ms: row.get(10)?,
        match_excerpt: excerpt,
    })
}

fn read_detail(connection: &Connection, hash: InfoHashV1) -> Result<TorrentDetail, ReadError> {
    let (info, fetched_at) = metadata(connection, hash)?;
    let parsed = parse(&info);
    let Some(parsed) = parsed else {
        return Ok(TorrentDetail {
            hash: hex(&hash.0),
            parse_status: "unavailable",
            name: None,
            name_truncated: false,
            encoding_lossy: false,
            total_length: None,
            file_count: None,
            piece_length: None,
            piece_count: None,
            private: None,
            fetched_at_ms: fetched_at,
        });
    };
    let (name, name_truncated) = truncate_display(&parsed.name);
    Ok(TorrentDetail {
        hash: hex(&hash.0),
        parse_status: "parsed",
        name: Some(name),
        name_truncated,
        encoding_lossy: parsed.encoding_lossy,
        total_length: Some(parsed.total_length.to_string()),
        file_count: Some(parsed.files.len()),
        piece_length: Some(parsed.piece_length.to_string()),
        piece_count: Some(parsed.piece_count),
        private: parsed.private,
        fetched_at_ms: fetched_at,
    })
}

fn read_files(
    connection: &Connection,
    hash: InfoHashV1,
    after: Option<String>,
    limit: usize,
) -> Result<FilePage, ReadError> {
    if !(1..=100).contains(&limit) {
        return Err(ReadError::Invalid);
    }
    let start = after
        .map(|value| parse_file_cursor(&value))
        .transpose()?
        .unwrap_or(0);
    let (info, _) = metadata(connection, hash)?;
    let Some(parsed) = parse(&info) else {
        return Ok(FilePage {
            available: false,
            items: Vec::new(),
            next: None,
        });
    };
    if start > parsed.files.len() {
        return Err(ReadError::Invalid);
    }
    let end = start.saturating_add(limit).min(parsed.files.len());
    let items = parsed.files[start..end]
        .iter()
        .enumerate()
        .map(|(offset, file)| file_item(start + offset, file))
        .collect();
    Ok(FilePage {
        available: true,
        items,
        next: (end < parsed.files.len()).then(|| format!("{end:016x}")),
    })
}

fn file_item(index: usize, file: &TorrentFile) -> FileItem {
    let (path, path_truncated) = truncate_display(&file.path);
    FileItem {
        index,
        path,
        path_truncated,
        encoding_lossy: file.encoding_lossy,
        length: file.length.to_string(),
    }
}

fn metadata(connection: &Connection, hash: InfoHashV1) -> Result<(Vec<u8>, i64), ReadError> {
    let result = connection
        .query_row(
            "SELECT info,fetched_at FROM metadata WHERE hash=?1",
            [hash.0.as_slice()],
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()
        .map_err(sql_error)?
        .ok_or(ReadError::Missing)?;
    if result.0.is_empty()
        || result.0.len() > 4 * 1024 * 1024
        || Sha1::digest(&result.0).as_slice() != hash.0
        || !matches!(
            crate::collection::peer::wire::dictionary_prefix(&result.0, 64),
            Ok(prefix) if prefix.len() == result.0.len()
        )
    {
        return Err(ReadError::Unavailable);
    }
    Ok(result)
}

fn backfill_one(
    connection: &mut Connection,
    cursor: Option<[u8; 20]>,
) -> Result<BackfillStep, ReadError> {
    let tx = connection.transaction().map_err(sql_error)?;
    let after = cursor.map_or_else(Vec::new, |value| value.to_vec());
    let row = tx
        .query_row(
            "SELECT m.hash,m.info FROM metadata m
             WHERE m.hash>?1 AND NOT EXISTS(
                 SELECT 1 FROM torrent_catalog c WHERE c.hash=m.hash
             )
             ORDER BY m.hash LIMIT 1",
            [after],
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )
        .optional()
        .map_err(sql_error)?;
    let Some((hash, info)) = row else {
        let state = index_state(&tx)?;
        tx.commit().map_err(sql_error)?;
        return Ok(BackfillStep {
            cursor,
            complete: state.complete,
        });
    };
    let hash_array: [u8; 20] = hash.try_into().map_err(|_| ReadError::Unavailable)?;
    ensure_catalog(&tx, &hash_array, &info).map_err(sql_error)?;
    let state = index_state(&tx)?;
    tx.commit().map_err(sql_error)?;
    Ok(BackfillStep {
        cursor: Some(hash_array),
        complete: state.complete,
    })
}

fn index_state(transaction: &Transaction<'_>) -> Result<IndexState, ReadError> {
    let (indexed, total) = transaction
        .query_row(
            "SELECT indexed,total FROM torrent_catalog_state WHERE singleton=1",
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .map_err(sql_error)?;
    Ok(IndexState {
        indexed,
        total,
        complete: indexed == total,
    })
}

fn parse_cursor(value: &str) -> Result<(i64, [u8; 20]), ReadError> {
    if value.len() != 56 {
        return Err(ReadError::Invalid);
    }
    let mut at = [0; 8];
    for (index, byte) in at.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| ReadError::Invalid)?;
    }
    let at = i64::try_from(u64::from_be_bytes(at)).map_err(|_| ReadError::Invalid)?;
    let hash = parse_hash(&value[16..]).ok_or(ReadError::Invalid)?;
    Ok((at, hash))
}

fn encode_catalog_cursor(at: i64, hash: [u8; 20]) -> String {
    let mut bytes = Vec::with_capacity(28);
    bytes.extend_from_slice(&(at as u64).to_be_bytes());
    bytes.extend_from_slice(&hash);
    hex(&bytes)
}

fn parse_file_cursor(value: &str) -> Result<usize, ReadError> {
    if value.len() != 16 {
        return Err(ReadError::Invalid);
    }
    let offset = u64::from_str_radix(value, 16).map_err(|_| ReadError::Invalid)?;
    usize::try_from(offset).map_err(|_| ReadError::Invalid)
}

fn parse_hash(value: &str) -> Option<[u8; 20]> {
    if value.len() != 40 {
        return None;
    }
    let mut output = [0; 20];
    for (index, byte) in output.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some(output)
}

fn sql_error(error: rusqlite::Error) -> ReadError {
    if matches!(error,rusqlite::Error::SqliteFailure(ref error,_) if error.code==rusqlite::ErrorCode::OperationInterrupted)
    {
        ReadError::Cancelled
    } else {
        ReadError::Unavailable
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::schema;

    const MULTI: &[u8] = b"d5:filesld6:lengthi2e4:pathl6:folder9:movie.mkveed6:lengthi3e4:pathl9:notes.txteee4:name9:\xe6\xb5\x8b\xe8\xaf\x95\xe9\x9b\x8612:piece lengthi4e6:pieces40:aaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbe";

    fn connection() -> Connection {
        let mut connection = Connection::open_in_memory().unwrap();
        connection.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
        schema::migrate(&mut connection).unwrap();
        connection
    }

    fn insert(connection: &mut Connection, info: &[u8], fetched_at: i64) -> InfoHashV1 {
        let hash = InfoHashV1(Sha1::digest(info).into());
        let tx = connection.transaction().unwrap();
        tx.execute(
            "INSERT INTO infohashes(hash,first_seen,last_seen) VALUES(?1,?2,?2)",
            params![hash.0.as_slice(), fetched_at],
        )
        .unwrap();
        tx.execute(
            "INSERT INTO metadata(hash,info,fetched_at) VALUES(?1,?2,?3)",
            params![hash.0.as_slice(), info, fetched_at],
        )
        .unwrap();
        ensure_catalog(&tx, &hash.0, info).unwrap();
        tx.commit().unwrap();
        hash
    }

    fn insert_without_catalog(
        connection: &mut Connection,
        hash: InfoHashV1,
        info: &[u8],
        fetched_at: i64,
    ) {
        connection
            .execute(
                "INSERT INTO infohashes(hash,first_seen,last_seen) VALUES(?1,?2,?2)",
                params![hash.0.as_slice(), fetched_at],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO metadata(hash,info,fetched_at) VALUES(?1,?2,?3)",
                params![hash.0.as_slice(), info, fetched_at],
            )
            .unwrap();
    }

    #[test]
    fn recent_search_cursor_detail_and_files_share_validated_source() {
        let mut connection = connection();
        let older = insert(
            &mut connection,
            b"d6:lengthi42e4:name8:file.txt12:piece lengthi16e6:pieces20:aaaaaaaaaaaaaaaaaaaae",
            10,
        );
        let newer = insert(&mut connection, MULTI, 20);

        let first = read_catalog_page(&connection, None, None, 1).unwrap();
        assert_eq!(first.items[0].hash, hex(&newer.0));
        assert_eq!(first.index.indexed, 2);
        assert!(first.index.complete);
        let second = read_catalog_page(&connection, None, first.next, 1).unwrap();
        assert_eq!(second.items[0].hash, hex(&older.0));

        for query in ["测试集", "folder/movie", "MOVIE.MKV"] {
            let page = read_catalog_page(&connection, Some(query.into()), None, 50).unwrap();
            assert_eq!(page.items.len(), 1, "{query}");
            assert_eq!(page.items[0].hash, hex(&newer.0));
            assert!(page.items[0].match_excerpt.is_some());
        }

        let detail = read_detail(&connection, newer).unwrap();
        assert_eq!(detail.total_length.as_deref(), Some("5"));
        assert_eq!(detail.file_count, Some(2));
        let first_files = read_files(&connection, newer, None, 1).unwrap();
        assert_eq!(first_files.items[0].path, "测试集/folder/movie.mkv");
        let second_files = read_files(&connection, newer, first_files.next, 1).unwrap();
        assert_eq!(second_files.items[0].path, "测试集/notes.txt");
    }

    #[test]
    fn invalid_queries_missing_hash_and_unavailable_metadata_are_distinct() {
        let mut connection = connection();
        let invalid = insert(&mut connection, b"de", 10);
        assert!(matches!(
            read_catalog_page(&connection, Some("ab".into()), None, 50),
            Err(ReadError::Invalid)
        ));
        assert!(matches!(
            read_detail(&connection, InfoHashV1([9; 20])),
            Err(ReadError::Missing)
        ));
        assert_eq!(
            read_detail(&connection, invalid).unwrap().parse_status,
            "unavailable"
        );
        assert!(
            !read_files(&connection, invalid, None, 100)
                .unwrap()
                .available
        );
    }

    #[test]
    fn backfill_resumes_from_missing_rows_and_keeps_counts_consistent() {
        let mut connection = connection();
        let info = b"d6:lengthi1e4:name3:one12:piece lengthi1e6:pieces20:aaaaaaaaaaaaaaaaaaaae";
        insert_without_catalog(&mut connection, InfoHashV1([1; 20]), info, 1);
        insert_without_catalog(&mut connection, InfoHashV1([2; 20]), info, 2);

        let first = backfill_one(&mut connection, None).unwrap();
        assert!(!first.complete);
        // 模拟进程重启：游标不持久化，从头扫描仍只处理缺失行。
        let second = backfill_one(&mut connection, None).unwrap();
        assert!(second.complete);
        let state = connection
            .query_row(
                "SELECT indexed,total FROM torrent_catalog_state WHERE singleton=1",
                [],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )
            .unwrap();
        assert_eq!(state, (2, 2));
        assert!(backfill_one(&mut connection, None).unwrap().complete);
    }
}
