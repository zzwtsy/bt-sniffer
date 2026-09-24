//! 完整身份归并与原始 metadata 提交；调用者拥有领取检查和事务边界。
use super::{catalog::ensure_catalog, metainfo};
use crate::{
    info_hash::{SwarmKey, TorrentIdentity},
    storage::StorageError,
};
use rusqlite::{OptionalExtension, Transaction, params};

pub(super) fn save(
    tx: &Transaction<'_>,
    key: SwarmKey,
    info: &[u8],
    at: i64,
) -> Result<i64, StorageError> {
    if info.is_empty() || info.len() > 4 * 1024 * 1024 {
        return Err(StorageError::Invalid("metadata 大小超限"));
    }
    let source = metainfo::match_identity(info, key).ok_or(StorageError::Conflict)?;
    if crate::collection::peer::wire::dictionary_prefix(info, 64)
        .map_err(|_| StorageError::Invalid("metadata 字典不合法"))?
        .len()
        != info.len()
    {
        return Err(StorageError::Invalid("metadata 尾随数据"));
    }
    let analysis = metainfo::analyze(info);
    let identities = metainfo::identities(info, source, &analysis);
    drop(analysis);
    let mut existing = None;
    for identity in &identities {
        let row: Option<(i64,bool)> = tx.query_row(
            "SELECT m.id,m.info=?3 FROM metadata m WHERE m.id=(SELECT metadata_id FROM torrent_identities WHERE kind=?1 AND hash=?2) OR m.hash=?2",
            params![identity.kind(),identity.bytes(),info], |r| Ok((r.get(0)?,r.get(1)?))).optional()?;
        if let Some((id, old)) = row {
            if !old {
                return Err(StorageError::Conflict);
            }
            if let Some(keep) = existing {
                if keep != id {
                    merge_identical(tx, keep, id)?;
                }
            } else {
                existing = Some(id);
            }
        }
    }
    let id = match existing {
        Some(id) => id,
        None => {
            tx.execute(
                "INSERT INTO metadata(hash,info,fetched_at) VALUES(?1,?2,?3)",
                params![identities[0].bytes(), info, at],
            )?;
            tx.last_insert_rowid()
        }
    };
    let canonical: Vec<u8> =
        tx.query_row("SELECT hash FROM metadata WHERE id=?1", [id], |r| r.get(0))?;
    if canonical != identities[0].bytes() {
        tx.execute("DELETE FROM torrent_catalog WHERE hash=?1", [&canonical])?;
        tx.execute(
            "UPDATE metadata SET hash=?2 WHERE id=?1",
            params![id, identities[0].bytes()],
        )?;
    }
    for identity in identities {
        tx.execute("INSERT INTO torrent_identities(kind,hash,metadata_id) VALUES(?1,?2,?3) ON CONFLICT(kind,hash) DO NOTHING", params![identity.kind(),identity.bytes(),id])?;
        let swarm = identity.swarm_key();
        super::records::upsert_hash(tx, swarm, at)?;
        let verification = if identity == source {
            match source {
                TorrentIdentity::V1(_) => "v1_full",
                TorrentIdentity::V2(_) => "v2_prefix",
            }
        } else {
            "hybrid_derived"
        };
        tx.execute("INSERT INTO swarm_metadata(hash,metadata_id,verification) VALUES(?1,?2,?3) ON CONFLICT(hash,metadata_id) DO UPDATE SET verification=CASE WHEN excluded.verification='hybrid_derived' THEN swarm_metadata.verification ELSE excluded.verification END",params![swarm.0.as_slice(),id,verification])?;
        if swarm != key {
            // 单次归并失效化另一查找任务；迟到 worker 仍必须通过 generation 检查。
            tx.execute("UPDATE fetch_jobs SET state='succeeded',generation=generation+1,updated_at=max(updated_at,?2),error=NULL WHERE hash=?1 AND state!='succeeded'",params![swarm.0.as_slice(),at])?;
            tx.execute("DELETE FROM peer_hints WHERE hash=?1", [swarm.0.as_slice()])?;
        }
    }
    let canonical: Vec<u8> =
        tx.query_row("SELECT hash FROM metadata WHERE id=?1", [id], |r| r.get(0))?;
    ensure_catalog(tx, &canonical, info)?;
    Ok(id)
}

/// 仅在调用方逐条确认原始字节相同之后归并；发现历史与领取记录仍保留。
fn merge_identical(tx: &Transaction<'_>, keep: i64, remove: i64) -> Result<(), StorageError> {
    tx.execute(
        "UPDATE torrent_identities SET metadata_id=?1 WHERE metadata_id=?2",
        params![keep, remove],
    )?;
    tx.execute("INSERT OR IGNORE INTO swarm_metadata(hash,metadata_id,verification) SELECT hash,?1,verification FROM swarm_metadata WHERE metadata_id=?2",params![keep,remove])?;
    tx.execute("DELETE FROM swarm_metadata WHERE metadata_id=?1", [remove])?;
    tx.execute("UPDATE metadata SET fetched_at=min(fetched_at,(SELECT fetched_at FROM metadata WHERE id=?2)) WHERE id=?1",params![keep,remove])?;
    tx.execute(
        "DELETE FROM torrent_catalog WHERE hash=(SELECT hash FROM metadata WHERE id=?1)",
        [remove],
    )?;
    tx.execute("DELETE FROM metadata WHERE id=?1", [remove])?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha1::{Digest, Sha1};
    use sha2::Sha256;
    fn connection() -> rusqlite::Connection {
        let mut c = rusqlite::Connection::open_in_memory().unwrap();
        c.execute_batch("PRAGMA foreign_keys=ON").unwrap();
        crate::storage::schema::migrate(&mut c).unwrap();
        c
    }
    #[test]
    fn hybrid_aliases_merge_and_conflicts_do_not_overwrite() {
        let mut c = connection();
        let info = include_bytes!("fixtures/hybrid.info");
        let v1 = SwarmKey(Sha1::digest(info).into());
        let full: [u8; 32] = Sha256::digest(info).into();
        let v2 = SwarmKey(full[..20].try_into().unwrap());
        let tx = c.transaction().unwrap();
        let a = save(&tx, v2, info, 2).unwrap();
        let b = save(&tx, v1, info, 3).unwrap();
        assert_eq!(a, b);
        assert_eq!(
            tx.query_row("SELECT count(*) FROM metadata", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(
            tx.query_row("SELECT count(*) FROM torrent_identities", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            2
        );
        assert_eq!(
            tx.query_row("SELECT hash FROM metadata", [], |r| r.get::<_, Vec<u8>>(0))
                .unwrap(),
            v1.0
        );
        tx.execute("UPDATE metadata SET info=X'6465'", []).unwrap();
        assert!(matches!(
            save(&tx, v1, info, 4),
            Err(StorageError::Conflict)
        ));
        assert_eq!(
            tx.query_row("SELECT info FROM metadata", [], |r| r.get::<_, Vec<u8>>(0))
                .unwrap(),
            b"de"
        );
    }
    #[test]
    fn identical_historical_rows_merge_atomically() {
        let mut c = connection();
        let info = include_bytes!("fixtures/hybrid.info");
        let v1 = SwarmKey(Sha1::digest(info).into());
        let full: [u8; 32] = Sha256::digest(info).into();
        let tx = c.transaction().unwrap();
        for hash in [v1.0.as_slice(), full.as_slice()] {
            tx.execute(
                "INSERT INTO metadata(hash,info,fetched_at) VALUES(?1,?2,1)",
                params![hash, info.as_slice()],
            )
            .unwrap();
        }
        save(&tx, v1, info, 2).unwrap();
        assert_eq!(
            tx.query_row("SELECT count(*) FROM metadata", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            1
        );
        tx.commit().unwrap();
        assert_eq!(
            c.query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |r| r
                .get::<_, i64>(
                0
            ))
            .unwrap(),
            0
        );
    }
    #[test]
    fn truncated_key_relation_is_not_a_unique_identity() {
        let mut c = connection();
        let tx = c.transaction().unwrap();
        let key = SwarmKey([7; 20]);
        super::super::records::upsert_hash(&tx, key, 1).unwrap();
        for suffix in [1, 2] {
            let mut hash = [7; 32];
            hash[31] = suffix;
            tx.execute(
                "INSERT INTO metadata(hash,info,fetched_at) VALUES(?1,X'6465',1)",
                [hash.as_slice()],
            )
            .unwrap();
            let id = tx.last_insert_rowid();
            tx.execute(
                "INSERT INTO torrent_identities VALUES('v2',?1,?2)",
                params![hash.as_slice(), id],
            )
            .unwrap();
            tx.execute(
                "INSERT INTO swarm_metadata VALUES(?1,?2,'v2_prefix')",
                params![key.0.as_slice(), id],
            )
            .unwrap();
        }
        assert_eq!(
            tx.query_row(
                "SELECT count(*) FROM swarm_metadata WHERE hash=?1",
                [key.0.as_slice()],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            2
        );
    }
    #[test]
    fn invalid_semantics_are_saved_without_hybrid_alias() {
        let mut c = connection();
        let info = include_bytes!("fixtures/hybrid-mismatch.info");
        let tx = c.transaction().unwrap();
        let v1 = SwarmKey(Sha1::digest(info).into());
        save(&tx, v1, info, 1).unwrap();
        assert_eq!(
            tx.query_row("SELECT count(*) FROM torrent_identities", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(
            tx.query_row("SELECT semantic_reason FROM torrent_catalog", [], |r| {
                r.get::<_, String>(0)
            })
            .unwrap(),
            "hybrid_layout"
        );
        tx.rollback().unwrap();
        assert_eq!(
            c.query_row("SELECT count(*) FROM metadata", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0
        );
    }
}
