//! 目录分页、详情读取与单条后台回填；所有 SQL 仍串行经过唯一数据库线程。

#[cfg(test)]
use crate::info_hash::SwarmKey;

use super::{TorrentFile, ensure_catalog, parse, parser::truncate_display};
use crate::{
    collection::{inspection::ReadError, store::CollectionStore},
    info_hash::TorrentIdentity,
    observation::hex,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::Serialize;
use sha1::{Digest, Sha1};
use std::time::{Duration, Instant};
use tokio::sync::OwnedSemaphorePermit;
use tokio_util::sync::CancellationToken;

const RECENT_PAGE_SQL: &str =
    "SELECT c.hash,c.parse_status,c.name,c.name_truncated,c.encoding_lossy,
    c.total_length,c.file_count,c.piece_length,c.piece_count,c.private,m.fetched_at,NULL
    FROM metadata AS m INDEXED BY metadata_fetched_at_hash
    JOIN torrent_catalog c ON c.hash=m.hash
    ORDER BY m.fetched_at DESC,m.hash DESC LIMIT ?1 OFFSET ?2";
// 外键保证目录行关联 metadata；直接计数避免每次读取首页都逐行联表。
const RECENT_TOTAL_SQL: &str = "SELECT COUNT(*) FROM torrent_catalog";

#[derive(Debug, Clone, Serialize)]
struct IdentityView {
    kind: String,
    hash: String,
}
#[derive(Debug, Clone, Serialize)]
struct ProtocolSummary {
    format: String,
    semantic_status: String,
    semantic_reason: Option<String>,
    identities: Vec<IdentityView>,
    verification: Vec<String>,
    validation_scope: &'static str,
    piece_layers: &'static str,
    piece_space_length: Option<String>,
    padding_length: Option<String>,
}
fn protocol(connection: &Connection, hash: &[u8]) -> rusqlite::Result<ProtocolSummary> {
    let mut result = connection.query_row("SELECT format,semantic_status,semantic_reason,piece_space_length,padding_length FROM torrent_catalog WHERE hash=?1",[hash],|r|Ok(ProtocolSummary {
        format:r.get(0)?,semantic_status:r.get(1)?,semantic_reason:r.get(2)?,piece_space_length:r.get(3)?,padding_length:r.get(4)?,identities:Vec::new(),verification:Vec::new(),validation_scope:"info_only",piece_layers:"not_fetched",
    }))?;
    load_identity_summary(connection, hash, &mut result)?;
    Ok(result)
}

/// 只读取已持久化的身份依据，详情即时解析不创建别名或目录。
fn load_identity_summary(
    connection: &Connection,
    hash: &[u8],
    result: &mut ProtocolSummary,
) -> rusqlite::Result<()> {
    let mut statement = connection.prepare("SELECT i.kind,i.hash FROM torrent_identities i JOIN metadata m ON m.id=i.metadata_id WHERE m.hash=?1 ORDER BY i.kind")?;
    result.identities = statement
        .query_map([hash], |r| {
            Ok(IdentityView {
                kind: r.get(0)?,
                hash: hex(&r.get::<_, Vec<u8>>(1)?),
            })
        })?
        .collect::<rusqlite::Result<_>>()?;
    let mut statement = connection.prepare("SELECT DISTINCT s.verification FROM swarm_metadata s JOIN metadata m ON m.id=s.metadata_id WHERE m.hash=?1 ORDER BY s.verification")?;
    result.verification = statement
        .query_map([hash], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    Ok(())
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct IndexState {
    indexed: i64,
    total: i64,
    /// 每条 metadata 都有已确认搜索覆盖状态的目录行。
    complete: bool,
    /// 名称及完整文件路径都可检索；不可解析或触及搜索上限时为 false。
    search_complete: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct CatalogItem {
    hash: String,
    #[serde(flatten)]
    protocol: ProtocolSummary,
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
    /// 当前查询条件下的结果总条数，不是本页条数；页越界时 items 为空、total 不变。
    total: i64,
    /// 回显请求的 1 起始页码，便于客户端核对响应归属。
    page: usize,
    index: IndexState,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct TorrentDetail {
    hash: String,
    #[serde(flatten)]
    protocol: ProtocolSummary,
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
    path: Option<String>,
    kind: &'static str,
    hidden: bool,
    executable: bool,
    symlink_path: Option<String>,
    sha1: Option<String>,
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
    pub(crate) cursor: Option<i64>,
    /// 没有剩余缺失目录或旧版未知路径状态；已知搜索截断不要求重复回填。
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
        page: usize,
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
            Ok(read_catalog_page(connection, query, page, limit))
        })
        .await
        .map_err(|_| ReadError::Unavailable)?
    }

    pub(crate) async fn torrent_detail(
        &self,
        hash: TorrentIdentity,
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
        hash: TorrentIdentity,
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
        cursor: Option<i64>,
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
    page: usize,
    limit: usize,
) -> Result<CatalogPage, ReadError> {
    if !(1..=100).contains(&limit) || page == 0 {
        return Err(ReadError::Invalid);
    }
    let query = query.map(|q| q.trim().to_owned()).filter(|q| !q.is_empty());
    if query
        .as_ref()
        .is_some_and(|q| !(3..=200).contains(&q.chars().count()))
    {
        return Err(ReadError::Invalid);
    }
    // 页码先在本类型内饱和再换算 i64，极端页码不会溢出，只是返回空页。
    let offset = i64::try_from(page.saturating_sub(1).saturating_mul(limit)).unwrap_or(i64::MAX);
    let tx = connection.unchecked_transaction().map_err(sql_error)?;
    let (items, total) = if let Some(query) = query {
        let literal = format!("\"{}\"", query.replace('"', "\"\""));
        let total = tx
            .query_row(
                "SELECT COUNT(*) FROM torrent_catalog_fts
                 JOIN torrent_catalog c ON c.id=torrent_catalog_fts.rowid
                 JOIN metadata m ON m.hash=c.hash
                 WHERE torrent_catalog_fts MATCH ?1",
                [&literal],
                |row| row.get::<_, i64>(0),
            )
            .map_err(sql_error)?;
        let mut statement = tx
            .prepare(
                "SELECT c.hash,c.parse_status,c.name,c.name_truncated,c.encoding_lossy,
                        c.total_length,c.file_count,c.piece_length,c.piece_count,c.private,
                        m.fetched_at,snippet(torrent_catalog_fts,0,'','', ' … ',24)
                 FROM torrent_catalog_fts
                 JOIN torrent_catalog c ON c.id=torrent_catalog_fts.rowid
                 JOIN metadata m ON m.hash=c.hash
                 WHERE torrent_catalog_fts MATCH ?1
                 ORDER BY m.fetched_at DESC,m.hash DESC LIMIT ?2 OFFSET ?3",
            )
            .map_err(sql_error)?;
        let items = statement
            .query_map(params![literal, limit as i64, offset], |row| {
                catalog_item(&tx, row, true)
            })
            .map_err(sql_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql_error)?;
        (items, total)
    } else {
        let total = tx
            .query_row(RECENT_TOTAL_SQL, [], |row| row.get::<_, i64>(0))
            .map_err(sql_error)?;
        let mut statement = tx.prepare(RECENT_PAGE_SQL).map_err(sql_error)?;
        let items = statement
            .query_map(params![limit as i64, offset], |row| {
                catalog_item(&tx, row, false)
            })
            .map_err(sql_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql_error)?;
        (items, total)
    };
    let index = index_state(&tx)?;
    tx.commit().map_err(sql_error)?;
    Ok(CatalogPage {
        items,
        total,
        page,
        index,
    })
}

fn catalog_item(
    connection: &Connection,
    row: &rusqlite::Row<'_>,
    matched: bool,
) -> rusqlite::Result<CatalogItem> {
    let hash = row.get::<_, Vec<u8>>(0)?;
    let excerpt = row.get::<_, Option<String>>(11)?;
    let excerpt = if matched {
        excerpt.map(|value| truncate_display(&value).0)
    } else {
        None
    };
    Ok(CatalogItem {
        protocol: protocol(connection, &hash)?,
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

fn read_detail(
    connection: &Connection,
    hash: impl Into<TorrentIdentity>,
) -> Result<TorrentDetail, ReadError> {
    let (info, fetched_at, canonical) = metadata(connection, hash.into())?;
    // 详情不依赖派生目录是否完成回填；同一次分析提供语义和展示统计。
    let analysis = super::super::metainfo::analyze(&info);
    let mut protocol = ProtocolSummary {
        format: analysis.format.into(),
        semantic_status: analysis.status.into(),
        semantic_reason: analysis.reason.map(str::to_owned),
        piece_space_length: analysis
            .parsed
            .as_ref()
            .map(|p| p.piece_space_length.to_string()),
        padding_length: analysis
            .parsed
            .as_ref()
            .map(|p| p.padding_length.to_string()),
        identities: Vec::new(),
        verification: Vec::new(),
        validation_scope: "info_only",
        piece_layers: "not_fetched",
    };
    load_identity_summary(connection, &canonical, &mut protocol).map_err(sql_error)?;
    let parsed = analysis.parsed;
    let Some(parsed) = parsed else {
        return Ok(TorrentDetail {
            hash: hex(&canonical),
            protocol,
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
        hash: hex(&canonical),
        protocol,
        parse_status: "parsed",
        name: Some(name),
        name_truncated,
        encoding_lossy: parsed.encoding_lossy,
        total_length: Some(parsed.total_length.to_string()),
        file_count: Some(parsed.file_count),
        piece_length: Some(parsed.piece_length.to_string()),
        piece_count: Some(parsed.piece_count),
        private: parsed.private,
        fetched_at_ms: fetched_at,
    })
}

fn read_files(
    connection: &Connection,
    hash: impl Into<TorrentIdentity>,
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
    let (info, _, _) = metadata(connection, hash.into())?;
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
    let (path, path_truncated) = truncate_display(file.path.as_deref().unwrap_or(""));
    FileItem {
        index,
        path: file.path.as_ref().map(|_| path),
        kind: file.kind,
        hidden: file.hidden,
        executable: file.executable,
        symlink_path: file.symlink_path.as_deref().map(|p| truncate_display(p).0),
        sha1: file.sha1.clone(),
        path_truncated,
        encoding_lossy: file.encoding_lossy,
        length: file.length.to_string(),
    }
}

fn metadata(
    connection: &Connection,
    hash: TorrentIdentity,
) -> Result<(Vec<u8>, i64, Vec<u8>), ReadError> {
    let result = connection.query_row(
        "SELECT CASE WHEN length(m.info) BETWEEN 1 AND 4194304 THEN m.info ELSE NULL END,m.fetched_at,m.hash FROM metadata m WHERE m.id=(SELECT metadata_id FROM torrent_identities WHERE kind=?1 AND hash=?2) OR m.hash=?2 LIMIT 1",
        params![hash.kind(),hash.bytes()], |r|Ok((r.get::<_,Vec<u8>>(0)?,r.get::<_,i64>(1)?,r.get::<_,Vec<u8>>(2)?)))
        .optional().map_err(sql_error)?.ok_or(ReadError::Missing)?;
    let digest_matches = match hash {
        TorrentIdentity::V1(_) => Sha1::digest(&result.0).as_slice() == hash.bytes(),
        TorrentIdentity::V2(_) => sha2::Sha256::digest(&result.0).as_slice() == hash.bytes(),
    };
    if result.0.is_empty()
        || result.0.len() > 4 * 1024 * 1024
        || !digest_matches
        || !matches!(crate::collection::peer::wire::dictionary_prefix(&result.0,64),Ok(raw) if raw.len()==result.0.len())
    {
        return Err(ReadError::Unavailable);
    }
    Ok(result)
}

fn backfill_one(
    connection: &mut Connection,
    cursor: Option<i64>,
) -> Result<BackfillStep, ReadError> {
    let tx = connection.transaction().map_err(sql_error)?;
    let after = cursor.unwrap_or(0);
    let row = tx
        .query_row(
            "SELECT m.id,m.hash,CASE WHEN length(m.info) BETWEEN 1 AND 4194304 THEN m.info ELSE X'' END FROM metadata m
             LEFT JOIN torrent_catalog c ON c.hash=m.hash
             WHERE m.id>?1 AND (c.hash IS NULL OR c.search_incomplete IS NULL OR c.semantic_status='pending' OR (c.semantic_status='invalid' AND c.semantic_reason='empty_directory'))
             ORDER BY m.id LIMIT 1",
            [after],
            |row| Ok((row.get::<_,i64>(0)?,row.get::<_, Vec<u8>>(1)?, row.get::<_, Vec<u8>>(2)?)),
        )
        .optional()
        .map_err(sql_error)?;
    let Some((id, hash, info)) = row else {
        let state = index_state(&tx)?;
        tx.commit().map_err(sql_error)?;
        return Ok(BackfillStep {
            cursor,
            complete: state.complete,
        });
    };
    let identity = if hash.len() == 20 {
        TorrentIdentity::V1(crate::info_hash::InfoHashV1(
            hash.as_slice()
                .try_into()
                .map_err(|_| ReadError::Unavailable)?,
        ))
    } else {
        TorrentIdentity::V2(crate::info_hash::InfoHashV2(
            hash.as_slice()
                .try_into()
                .map_err(|_| ReadError::Unavailable)?,
        ))
    };
    // 损坏原始数据保留为不可解析目录，不能据此添加身份别名。
    if super::super::metainfo::match_identity(&info, identity.swarm_key()) == Some(identity) {
        let at: i64 = tx
            .query_row("SELECT fetched_at FROM metadata WHERE id=?1", [id], |r| {
                r.get(0)
            })
            .map_err(sql_error)?;
        super::super::metadata_store::save(&tx, identity.swarm_key(), &info, at)
            .map_err(|_| ReadError::Unavailable)?;
    } else {
        ensure_catalog(&tx, &hash, b"").map_err(sql_error)?;
        tx.execute("UPDATE torrent_catalog SET semantic_status='invalid',semantic_reason='identity_mismatch' WHERE hash=?1",[&hash]).map_err(sql_error)?;
    }
    tx.commit().map_err(sql_error)?;
    Ok(BackfillStep {
        cursor: Some(id),
        // 至少存在一条尚未检查的记录；下一轮才能确认扫描完成。
        complete: false,
    })
}

fn index_state(transaction: &Transaction<'_>) -> Result<IndexState, ReadError> {
    let (indexed, total, search_incomplete) = transaction
        .query_row(
            "SELECT indexed,total,search_incomplete FROM torrent_catalog_state WHERE singleton=1",
            [],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )
        .map_err(sql_error)?;
    Ok(IndexState {
        indexed,
        total,
        complete: indexed == total,
        search_complete: search_incomplete == 0 && indexed == total,
    })
}

fn parse_file_cursor(value: &str) -> Result<usize, ReadError> {
    if value.len() != 16 {
        return Err(ReadError::Invalid);
    }
    let offset = u64::from_str_radix(value, 16).map_err(|_| ReadError::Invalid)?;
    usize::try_from(offset).map_err(|_| ReadError::Invalid)
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

    fn insert(connection: &mut Connection, info: &[u8], fetched_at: i64) -> SwarmKey {
        let hash = SwarmKey(Sha1::digest(info).into());
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
        hash: SwarmKey,
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
    fn recent_total_counts_catalog_rows_independently_of_backfill_status() {
        let mut connection = connection();
        assert_eq!(
            read_catalog_page(&connection, None, 1, 50).unwrap().total,
            0
        );
        insert_without_catalog(&mut connection, SwarmKey([9; 20]), b"de", 1);
        assert_eq!(
            read_catalog_page(&connection, None, 1, 50).unwrap().total,
            0
        );
        let parsed = insert(&mut connection, MULTI, 2);
        insert(&mut connection, b"de", 3);
        let page = read_catalog_page(&connection, None, 1, 50).unwrap();
        assert_eq!(page.total, 2);
        assert_eq!(page.index.total, 3);
        assert_eq!(page.index.indexed, 2);
        connection
            .execute(
                "UPDATE torrent_catalog SET search_incomplete=NULL WHERE hash=?1",
                [parsed.0.as_slice()],
            )
            .unwrap();
        let page = read_catalog_page(&connection, None, 1, 50).unwrap();
        assert_eq!(page.total, 2);
        assert_eq!(page.index.indexed, 1);
    }

    #[test]
    fn recent_total_does_not_execute_per_row_joins() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        let mut connection = connection();
        let tx = connection.transaction().unwrap();
        for number in 0_u64..10_000 {
            let mut hash = [0; 20];
            hash[..8].copy_from_slice(&number.to_be_bytes());
            tx.execute("INSERT INTO infohashes VALUES(?1,0,0)", [hash.as_slice()])
                .unwrap();
            tx.execute(
                "INSERT INTO metadata(hash,info,fetched_at) VALUES(?1,X'6465',0)",
                [hash.as_slice()],
            )
            .unwrap();
            ensure_catalog(&tx, &hash, b"de").unwrap();
        }
        tx.commit().unwrap();
        let callbacks = Arc::new(AtomicUsize::new(0));
        let observed = callbacks.clone();
        connection
            .progress_handler(
                1000,
                Some(move || {
                    observed.fetch_add(1, Ordering::Relaxed);
                    false
                }),
            )
            .unwrap();
        let total: i64 = connection
            .query_row(RECENT_TOTAL_SQL, [], |row| row.get(0))
            .unwrap();
        connection
            .progress_handler(0, None::<fn() -> bool>)
            .unwrap();
        assert_eq!(total, 10_000);
        // 不依赖机器耗时或精确 opcode 数；逐行 JOIN 会超过这个宽松上限。
        assert!(callbacks.load(Ordering::Relaxed) < 5);
    }

    #[test]
    fn recent_search_pages_detail_and_files_share_validated_source() {
        let mut connection = connection();
        let older = insert(
            &mut connection,
            b"d6:lengthi42e4:name8:file.txt12:piece lengthi16e6:pieces20:aaaaaaaaaaaaaaaaaaaae",
            10,
        );
        let newer = insert(&mut connection, MULTI, 20);

        let first = read_catalog_page(&connection, None, 1, 1).unwrap();
        assert_eq!(first.items[0].hash, hex(&newer.0));
        assert_eq!(first.items[0].file_count, Some(2));
        assert_eq!(first.total, 2);
        assert_eq!(first.page, 1);
        assert_eq!(first.index.indexed, 2);
        assert!(first.index.complete);
        let second = read_catalog_page(&connection, None, 2, 1).unwrap();
        assert_eq!(second.items[0].hash, hex(&older.0));

        for query in ["测试集", "folder/movie", "MOVIE.MKV"] {
            let page = read_catalog_page(&connection, Some(query.into()), 1, 50).unwrap();
            assert_eq!(page.items.len(), 1, "{query}");
            assert_eq!(page.items[0].hash, hex(&newer.0));
            assert_eq!(page.total, 1, "{query}");
            assert!(page.items[0].match_excerpt.is_some());
        }

        let detail = read_detail(&connection, newer).unwrap();
        assert_eq!(detail.total_length.as_deref(), Some("5"));
        assert_eq!(detail.file_count, Some(2));
        let first_files = read_files(&connection, newer, None, 1).unwrap();
        assert_eq!(
            first_files.items[0].path.as_deref(),
            Some("测试集/folder/movie.mkv")
        );
        let second_files = read_files(&connection, newer, first_files.next, 1).unwrap();
        assert_eq!(
            second_files.items[0].path.as_deref(),
            Some("测试集/notes.txt")
        );
    }

    #[test]
    fn invalid_queries_missing_hash_and_unavailable_metadata_are_distinct() {
        let mut connection = connection();
        let invalid = insert(&mut connection, b"de", 10);
        assert!(matches!(
            read_catalog_page(&connection, Some("ab".into()), 1, 50),
            Err(ReadError::Invalid)
        ));
        assert!(matches!(
            read_detail(&connection, SwarmKey([9; 20])),
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
        insert_without_catalog(&mut connection, SwarmKey([1; 20]), info, 1);
        insert_without_catalog(&mut connection, SwarmKey([2; 20]), info, 2);

        let first = backfill_one(&mut connection, None).unwrap();
        assert!(!first.complete);
        // 模拟进程重启：游标不持久化，从头扫描仍只处理缺失行。
        let second = backfill_one(&mut connection, None).unwrap();
        assert!(!second.complete);
        assert!(
            backfill_one(&mut connection, second.cursor)
                .unwrap()
                .complete
        );
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

    #[test]
    fn backfill_rebuilds_legacy_rows_with_unknown_search_coverage_once() {
        let mut connection = connection();
        let info = b"d6:lengthi1e4:name3:one12:piece lengthi1e6:pieces20:aaaaaaaaaaaaaaaaaaaae";
        let hash = insert(&mut connection, info, 1);
        connection
            .execute(
                "UPDATE torrent_catalog SET search_incomplete=NULL WHERE hash=?1",
                [hash.0.as_slice()],
            )
            .unwrap();

        let state = read_catalog_page(&connection, None, 1, 1).unwrap().index;
        assert!(!state.complete);
        assert!(!state.search_complete);

        let rebuilt = backfill_one(&mut connection, None).unwrap();
        assert!(!rebuilt.complete);
        let finished = backfill_one(&mut connection, rebuilt.cursor).unwrap();
        assert!(finished.complete);
        assert!(backfill_one(&mut connection, None).unwrap().complete);

        let state = read_catalog_page(&connection, None, 1, 1).unwrap().index;
        assert!(state.complete);
        assert!(state.search_complete);
        let counts: (i64, i64) = connection
            .query_row(
                "SELECT indexed,search_incomplete FROM torrent_catalog_state WHERE singleton=1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(counts, (1, 0));
    }

    #[test]
    fn recent_catalog_pages_use_ordered_metadata_index() {
        let connection = connection();
        let mut statement = connection
            .prepare(&format!("EXPLAIN QUERY PLAN {RECENT_PAGE_SQL}"))
            .unwrap();
        let details = statement
            .query_map(params![50_i64, 50_i64], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(
            details
                .iter()
                .any(|detail| detail.contains("metadata_fetched_at_hash")),
            "{details:?}"
        );
        assert!(
            details
                .iter()
                .all(|detail| !detail.contains("USE TEMP B-TREE FOR ORDER BY")),
            "{details:?}"
        );
    }

    #[test]
    fn equal_timestamps_page_stably_by_hash() {
        let mut connection = connection();
        let mut hashes = Vec::new();
        for name in ["one", "two", "six"] {
            let info = format!(
                "d6:lengthi1e4:name3:{name}12:piece lengthi1e6:pieces20:aaaaaaaaaaaaaaaaaaaae"
            );
            hashes.push(insert(&mut connection, info.as_bytes(), 7));
        }
        hashes.sort_by_key(|hash| std::cmp::Reverse(hash.0));

        let first = read_catalog_page(&connection, None, 1, 1).unwrap();
        let second = read_catalog_page(&connection, None, 2, 1).unwrap();
        let third = read_catalog_page(&connection, None, 3, 1).unwrap();
        assert_eq!(first.items[0].hash, hex(&hashes[0].0));
        assert_eq!(second.items[0].hash, hex(&hashes[1].0));
        assert_eq!(third.items[0].hash, hex(&hashes[2].0));
        assert_eq!(third.total, 3);
    }

    #[test]
    fn search_pages_share_the_same_offset_order() {
        let mut connection = connection();
        let newer = insert(
            &mut connection,
            b"d5:filesld6:lengthi1e4:pathl14:search-hit.txteee4:name3:one12:piece lengthi1e6:pieces20:aaaaaaaaaaaaaaaaaaaae",
            9,
        );
        let older = insert(
            &mut connection,
            b"d5:filesld6:lengthi1e4:pathl14:search-hit.txteee4:name3:two12:piece lengthi1e6:pieces20:aaaaaaaaaaaaaaaaaaaae",
            8,
        );

        let first = read_catalog_page(&connection, Some("hit".into()), 1, 1).unwrap();
        let second = read_catalog_page(&connection, Some("hit".into()), 2, 1).unwrap();
        assert_eq!(first.items[0].hash, hex(&newer.0));
        assert_eq!(second.items[0].hash, hex(&older.0));
        assert_eq!(first.total, 2);
        assert_eq!(second.total, 2);
    }

    #[test]
    fn out_of_range_page_returns_empty_items_with_total() {
        let mut connection = connection();
        insert(
            &mut connection,
            b"d6:lengthi1e4:name3:one12:piece lengthi1e6:pieces20:aaaaaaaaaaaaaaaaaaaae",
            1,
        );
        insert(
            &mut connection,
            b"d6:lengthi1e4:name3:two12:piece lengthi1e6:pieces20:aaaaaaaaaaaaaaaaaaaae",
            2,
        );

        // 越界页不 clamp：空 items + 真实 total，由调用方决定纠正方向。
        let page = read_catalog_page(&connection, None, 9, 50).unwrap();
        assert!(page.items.is_empty());
        assert_eq!(page.total, 2);
        assert_eq!(page.page, 9);
        let search = read_catalog_page(&connection, Some("one".into()), 9, 50).unwrap();
        assert!(search.items.is_empty());
        assert_eq!(search.total, 1);
    }

    #[test]
    fn page_zero_and_limit_bounds_are_rejected() {
        let connection = connection();
        for (page, limit) in [(0_usize, 50_usize), (1, 0), (1, 101)] {
            assert!(
                matches!(
                    read_catalog_page(&connection, None, page, limit),
                    Err(ReadError::Invalid)
                ),
                "{page}/{limit}"
            );
        }
    }
    #[test]
    fn details_do_not_require_catalog_or_write_derived_state() {
        let mut c = connection();
        for info in [
            include_bytes!("../fixtures/v1.info").as_slice(),
            include_bytes!("../fixtures/v1-bad-pieces.info"),
            include_bytes!("../fixtures/unknown-version.info"),
        ] {
            let hash = SwarmKey(Sha1::digest(info).into());
            insert_without_catalog(&mut c, hash, info, 1);
            let id = c.last_insert_rowid();
            c.execute(
                "INSERT INTO torrent_identities VALUES('v1',?1,?2)",
                params![hash.0.as_slice(), id],
            )
            .unwrap();
            c.execute(
                "INSERT INTO swarm_metadata VALUES(?1,?2,'v1_full')",
                params![hash.0.as_slice(), id],
            )
            .unwrap();
            let expected = super::super::super::metainfo::analyze(info);
            for phase in 0..3 {
                if phase == 1 {
                    let tx = c.transaction().unwrap();
                    ensure_catalog(&tx, &hash.0, info).unwrap();
                    tx.execute("UPDATE torrent_catalog SET semantic_status='pending',semantic_reason=NULL WHERE hash=?1", [hash.0.as_slice()]).unwrap();
                    tx.commit().unwrap();
                } else if phase == 2 {
                    let tx = c.transaction().unwrap();
                    ensure_catalog(&tx, &hash.0, info).unwrap();
                    tx.commit().unwrap();
                }
                let before = c.total_changes();
                let detail = read_detail(&c, hash).unwrap();
                assert_eq!(detail.protocol.semantic_status, expected.status);
                assert_eq!(detail.protocol.semantic_reason.as_deref(), expected.reason);
                assert_eq!(detail.protocol.identities.len(), 1);
                assert_eq!(detail.protocol.verification, ["v1_full"]);
                assert_eq!(
                    detail.parse_status,
                    if expected.parsed.is_some() {
                        "parsed"
                    } else {
                        "unavailable"
                    }
                );
                assert_eq!(
                    read_files(&c, hash, None, 100).unwrap().available,
                    expected.parsed.is_some()
                );
                assert_eq!(c.total_changes(), before);
            }
        }
    }

    #[test]
    fn empty_directory_backfill_is_atomic_and_resumes() {
        let mut c = connection();
        let info = include_bytes!("../fixtures/hybrid-empty-directory.info");
        let key = insert(&mut c, info, 1);
        let v2: [u8; 32] = sha2::Sha256::digest(info).into();
        c.execute("UPDATE torrent_catalog SET semantic_status='invalid',semantic_reason='empty_directory',parse_status='unavailable'", []).unwrap();
        c.execute(
            "INSERT INTO torrent_identities SELECT 'v1',hash,id FROM metadata",
            [],
        )
        .unwrap();
        c.execute(
            "INSERT INTO swarm_metadata SELECT hash,id,'v1_full' FROM metadata",
            [],
        )
        .unwrap();
        c.execute("INSERT INTO infohashes VALUES(?1,1,1)", [&v2[..20]])
            .unwrap();
        c.execute("INSERT INTO fetch_jobs(hash,state,due_at,generation,updated_at) VALUES(?1,'running',1,7,1)", [&v2[..20]]).unwrap();
        c.execute(
            "INSERT INTO peer_hints VALUES(?1,X'7F000001',6881,1)",
            [&v2[..20]],
        )
        .unwrap();
        c.execute_batch("CREATE TRIGGER fail_rebuild BEFORE UPDATE ON torrent_catalog BEGIN SELECT RAISE(ABORT,'test'); END;").unwrap();
        assert!(backfill_one(&mut c, None).is_err());
        assert_eq!(
            c.query_row("SELECT count(*) FROM torrent_identities", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(
            c.query_row("SELECT generation FROM fetch_jobs", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            7
        );
        assert_eq!(
            c.query_row("SELECT count(*) FROM peer_hints", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            1
        );
        c.execute_batch("DROP TRIGGER fail_rebuild").unwrap();
        backfill_one(&mut c, None).unwrap();
        assert_eq!(
            c.query_row("SELECT info FROM metadata", [], |r| r.get::<_, Vec<u8>>(0))
                .unwrap(),
            info
        );
        assert_eq!(
            c.query_row("SELECT generation FROM fetch_jobs", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            8
        );
        assert_eq!(
            c.query_row("SELECT state FROM fetch_jobs", [], |r| r
                .get::<_, String>(0))
                .unwrap(),
            "succeeded"
        );
        assert_eq!(
            c.query_row("SELECT count(*) FROM peer_hints", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
        let detail = read_detail(&c, key).unwrap();
        assert_eq!(detail.protocol.semantic_status, "valid");
        assert_eq!(detail.protocol.identities.len(), 2);
        let other = read_detail(&c, TorrentIdentity::V2(crate::info_hash::InfoHashV2(v2))).unwrap();
        assert_eq!(detail.hash, other.hash);
        let before = c.total_changes();
        assert!(backfill_one(&mut c, None).unwrap().complete);
        assert_eq!(c.total_changes(), before);

        let invalid = insert(&mut c, include_bytes!("../fixtures/v1-bad-pieces.info"), 2);
        c.execute("UPDATE torrent_catalog SET semantic_status='invalid',semantic_reason='empty_directory' WHERE hash=?1",[invalid.0.as_slice()]).unwrap();
        backfill_one(&mut c, None).unwrap();
        assert_eq!(
            read_detail(&c, invalid)
                .unwrap()
                .protocol
                .semantic_reason
                .as_deref(),
            Some("piece_count")
        );
        assert!(backfill_one(&mut c, None).unwrap().complete);
    }
    #[test]
    fn large_catalog_identity_queries_use_bounded_work() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        let c = connection();
        c.execute_batch("BEGIN;
            WITH RECURSIVE seq(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM seq WHERE x<100000)
            INSERT INTO metadata(hash,info,fetched_at) SELECT CAST(printf('%020d',x) AS BLOB),X'6465',x FROM seq;
            INSERT INTO infohashes SELECT hash,1,1 FROM metadata;
            INSERT INTO torrent_identities SELECT 'v1',hash,id FROM metadata;
            INSERT INTO swarm_metadata SELECT hash,id,'v1_full' FROM metadata;
            INSERT INTO torrent_catalog(hash,parse_status,name,name_truncated,encoding_lossy,search_text,search_incomplete,format,semantic_status)
            SELECT hash,'unavailable',NULL,0,0,CASE WHEN id>99900 THEN 'needle' ELSE '' END,1,'v1','invalid' FROM metadata;
            INSERT INTO torrent_identities VALUES('v2',zeroblob(32),100000);
            INSERT INTO infohashes VALUES(zeroblob(20),1,1);
            INSERT INTO swarm_metadata VALUES(zeroblob(20),100000,'hybrid_derived');
            COMMIT;").unwrap();
        let mut measurements = Vec::new();
        for search in [false, true] {
            for limit in [50, 100] {
                let query = search.then(|| "needle".to_owned());
                let calls = Arc::new(AtomicUsize::new(0));
                let counter = calls.clone();
                c.progress_handler(
                    1,
                    Some(move || {
                        counter.fetch_add(1, Ordering::Relaxed);
                        false
                    }),
                )
                .unwrap();
                let page = read_catalog_page(&c, query.clone(), 1, limit).unwrap();
                c.progress_handler(0, None::<fn() -> bool>).unwrap();
                let steps = calls.load(Ordering::Relaxed);
                assert!(
                    steps < 100_000,
                    "search={search} limit={limit} steps={steps}"
                );
                assert_eq!(page.items.len(), limit);
                assert_eq!(page.total, if search { 100 } else { 100000 });
                assert_eq!(page.items[0].protocol.identities.len(), 2);
                assert_eq!(page.items[0].protocol.verification.len(), 2);
                let started = Instant::now();
                let progress = Progress::install(&c, CancellationToken::new()).unwrap();
                read_catalog_page(&c, query, 1, limit).unwrap();
                drop(progress);
                measurements.push(serde_json::json!({"search":search,"limit":limit,"vm_steps":steps,"elapsed_us":started.elapsed().as_micros()}));
            }
        }
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target/checks/catalog-query-benchmark.json");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            path,
            serde_json::to_vec_pretty(
                &serde_json::json!({"metadata_count":100000,"measurements":measurements}),
            )
            .unwrap(),
        )
        .unwrap();
    }
}
