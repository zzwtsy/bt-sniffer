//! Storage 打开连接时执行版本检查、版本迁移，再补充领取索引。
//! v1-v5 的表、对应索引和版本号在同一迁移事务中提交；补充查询索引单独执行。
//! 拒绝较新版本；迁移事务失败会回滚，但补充索引失败不撤销已提交的版本迁移。
use super::StorageError;
use rusqlite::{Connection, Transaction};

/// 新库顺序执行全部版本，旧库只执行缺少的版本；迁移提交后再补查询索引。
/// 已是 v5 的数据库也会补索引；该步失败会返回错误，已有数据和版本迁移仍保留。
pub(crate) fn migrate(connection: &mut Connection) -> Result<(), StorageError> {
    let version: i64 = connection.pragma_query_value(None, "user_version", |r| r.get(0))?;
    if version > 5 {
        return Err(StorageError::Invalid("数据库由更新版本程序创建"));
    }
    if version == 5 {
        return claim_index(connection);
    }
    if version == 4 {
        let tx = connection.transaction()?;
        upgrade_v5(&tx)?;
        tx.commit()?;
        return claim_index(connection);
    }
    if version == 3 {
        let tx = connection.transaction()?;
        upgrade_v4(&tx)?;
        upgrade_v5(&tx)?;
        tx.commit()?;
        return claim_index(connection);
    }
    let tx = connection.transaction()?;
    // v1 保存身份、联系人、hash、metadata 和采样冷却。
    if version == 0 {
        tx.execute_batch(
            r#"
        CREATE TABLE node_identities (
            identity INTEGER PRIMARY KEY,
            instance TEXT NOT NULL CHECK(length(instance) BETWEEN 1 AND 128),
            family INTEGER NOT NULL CHECK(family IN (4,6)),
            node_id BLOB NOT NULL CHECK(length(node_id)=20),
            created_at INTEGER NOT NULL CHECK(created_at>=0),
            method TEXT NOT NULL CHECK(method='random-v1'),
            UNIQUE(instance,family)
        ) STRICT;
        CREATE TABLE routing_contacts (
            identity INTEGER NOT NULL REFERENCES node_identities(identity),
            node_id BLOB NOT NULL CHECK(length(node_id)=20),
            ip BLOB NOT NULL CHECK(length(ip) IN (4,16)),
            port INTEGER NOT NULL CHECK(port BETWEEN 1 AND 65535),
            responded_at INTEGER NOT NULL CHECK(responded_at>=0),
            PRIMARY KEY(identity,node_id)
        ) STRICT;
        CREATE TABLE infohashes (
            hash BLOB PRIMARY KEY NOT NULL CHECK(length(hash)=20),
            first_seen INTEGER NOT NULL CHECK(first_seen>=0),
            last_seen INTEGER NOT NULL CHECK(last_seen>=first_seen)
        ) STRICT;
        CREATE TABLE metadata (
            hash BLOB PRIMARY KEY NOT NULL REFERENCES infohashes(hash),
            info BLOB NOT NULL CHECK(length(info) BETWEEN 1 AND 4194304),
            fetched_at INTEGER NOT NULL CHECK(fetched_at>=0)
        ) STRICT;
        CREATE TABLE sampling_cooldowns (
            identity INTEGER NOT NULL REFERENCES node_identities(identity),
            kind INTEGER NOT NULL CHECK(kind IN (0,1)),
            key BLOB NOT NULL,
            lease BLOB NOT NULL CHECK(length(lease)=16),
            pending INTEGER NOT NULL CHECK(pending IN (0,1)),
            until_at INTEGER NOT NULL CHECK(until_at>=0),
            duration_ms INTEGER NOT NULL CHECK(duration_ms>0),
            failures INTEGER NOT NULL CHECK(failures BETWEEN 0 AND 4294967295),
            CHECK((kind=0 AND length(key)=20) OR (kind=1 AND length(key) IN (4,16))),
            PRIMARY KEY(identity,kind,key)
        ) STRICT;
        CREATE INDEX cooldown_expiry ON sampling_cooldowns(identity,pending,until_at);
        PRAGMA user_version=1;
    "#,
        )?;
    }
    // v2 增加任务和 peer 提示；历史 hash 是否回填任务由显式启用采集决定。
    if version < 2 {
        tx.execute_batch(
            r#"
        CREATE TABLE fetch_jobs (
            hash BLOB PRIMARY KEY NOT NULL REFERENCES infohashes(hash),
            state TEXT NOT NULL CHECK(state IN ('pending','running','succeeded','retry_wait','dormant')),
            attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts BETWEEN 0 AND 6),
            due_at INTEGER NOT NULL CHECK(due_at>=0),
            generation INTEGER NOT NULL DEFAULT 0 CHECK(generation>=0),
            updated_at INTEGER NOT NULL CHECK(updated_at>=0),
            error TEXT
        ) STRICT;
        CREATE INDEX fetch_due ON fetch_jobs(state,due_at,hash);
        CREATE TABLE peer_hints (
            hash BLOB NOT NULL REFERENCES infohashes(hash),
            ip BLOB NOT NULL CHECK(length(ip) IN (4,16)),
            port INTEGER NOT NULL CHECK(port BETWEEN 1 AND 65535),
            observed_at INTEGER NOT NULL CHECK(observed_at>=0),
            PRIMARY KEY(hash,ip,port)
        ) STRICT;
        CREATE INDEX hint_expiry ON peer_hints(observed_at);
        PRAGMA user_version=2;
    "#,
        )?;
    }
    // v3 保存可重建的 torrent 展示目录；原始 info 仍是唯一持久事实。
    tx.execute_batch(
        r#"
        CREATE TABLE torrent_catalog (
            id INTEGER PRIMARY KEY,
            hash BLOB NOT NULL UNIQUE REFERENCES metadata(hash) ON DELETE CASCADE,
            parse_status TEXT NOT NULL CHECK(parse_status IN ('parsed','unavailable')),
            name TEXT,
            name_truncated INTEGER NOT NULL CHECK(name_truncated IN (0,1)),
            encoding_lossy INTEGER NOT NULL CHECK(encoding_lossy IN (0,1)),
            total_length TEXT,
            file_count INTEGER,
            piece_length TEXT,
            piece_count INTEGER,
            private INTEGER CHECK(private IN (0,1)),
            search_text TEXT NOT NULL
        ) STRICT;
        CREATE INDEX torrent_catalog_hash ON torrent_catalog(hash);
        CREATE VIRTUAL TABLE torrent_catalog_fts USING fts5(
            search_text,
            content='torrent_catalog',
            content_rowid='id',
            tokenize='trigram'
        );
        CREATE TRIGGER torrent_catalog_ai AFTER INSERT ON torrent_catalog BEGIN
            INSERT INTO torrent_catalog_fts(rowid,search_text) VALUES(new.id,new.search_text);
        END;
        CREATE TRIGGER torrent_catalog_ad AFTER DELETE ON torrent_catalog BEGIN
            INSERT INTO torrent_catalog_fts(torrent_catalog_fts,rowid,search_text)
            VALUES('delete',old.id,old.search_text);
        END;
        CREATE TRIGGER torrent_catalog_au AFTER UPDATE ON torrent_catalog BEGIN
            INSERT INTO torrent_catalog_fts(torrent_catalog_fts,rowid,search_text)
            VALUES('delete',old.id,old.search_text);
            INSERT INTO torrent_catalog_fts(rowid,search_text) VALUES(new.id,new.search_text);
        END;
        CREATE TABLE torrent_catalog_state (
            singleton INTEGER PRIMARY KEY CHECK(singleton=1),
            indexed INTEGER NOT NULL CHECK(indexed>=0),
            total INTEGER NOT NULL CHECK(total>=indexed)
        ) STRICT;
        INSERT INTO torrent_catalog_state(singleton,indexed,total)
        SELECT 1,0,count(*) FROM metadata;
        CREATE TRIGGER metadata_catalog_total_ai AFTER INSERT ON metadata BEGIN
            UPDATE torrent_catalog_state SET total=total+1 WHERE singleton=1;
        END;
        CREATE TRIGGER metadata_catalog_total_ad AFTER DELETE ON metadata BEGIN
            UPDATE torrent_catalog_state SET total=total-1 WHERE singleton=1;
        END;
        CREATE TRIGGER torrent_catalog_indexed_ai AFTER INSERT ON torrent_catalog BEGIN
            UPDATE torrent_catalog_state SET indexed=indexed+1 WHERE singleton=1;
        END;
        CREATE TRIGGER torrent_catalog_indexed_ad AFTER DELETE ON torrent_catalog BEGIN
            UPDATE torrent_catalog_state SET indexed=indexed-1 WHERE singleton=1;
        END;
        PRAGMA user_version=3;
    "#,
    )?;
    upgrade_v4(&tx)?;
    upgrade_v5(&tx)?;
    tx.commit()?;
    claim_index(connection)
}

/// v4 记录路径搜索覆盖状态；旧目录行置为未知，等待 Monitor 从原始 metadata 重算。
fn upgrade_v4(transaction: &Transaction<'_>) -> Result<(), StorageError> {
    transaction.execute_batch(
        r#"
        ALTER TABLE torrent_catalog
            ADD COLUMN search_incomplete INTEGER CHECK(search_incomplete IN (0,1));
        ALTER TABLE torrent_catalog_state
            ADD COLUMN search_incomplete INTEGER NOT NULL DEFAULT 0 CHECK(search_incomplete>=0);
        UPDATE torrent_catalog_state
        SET indexed=(
                SELECT count(*) FROM torrent_catalog WHERE search_incomplete IS NOT NULL
            ),
            search_incomplete=(
                SELECT count(*) FROM torrent_catalog
                WHERE search_incomplete IS NULL OR search_incomplete=1
            )
        WHERE singleton=1;
        DROP TRIGGER torrent_catalog_indexed_ai;
        DROP TRIGGER torrent_catalog_indexed_ad;
        CREATE TRIGGER torrent_catalog_indexed_ai AFTER INSERT ON torrent_catalog BEGIN
            UPDATE torrent_catalog_state
            SET indexed=indexed+CASE WHEN new.search_incomplete IS NULL THEN 0 ELSE 1 END
            WHERE singleton=1;
        END;
        CREATE TRIGGER torrent_catalog_indexed_ad AFTER DELETE ON torrent_catalog BEGIN
            UPDATE torrent_catalog_state
            SET indexed=indexed-CASE WHEN old.search_incomplete IS NULL THEN 0 ELSE 1 END
            WHERE singleton=1;
        END;
        CREATE TRIGGER torrent_catalog_indexed_au AFTER UPDATE OF search_incomplete ON torrent_catalog BEGIN
            UPDATE torrent_catalog_state
            SET indexed=indexed
                + CASE WHEN new.search_incomplete IS NULL THEN 0 ELSE 1 END
                - CASE WHEN old.search_incomplete IS NULL THEN 0 ELSE 1 END
            WHERE singleton=1;
        END;
        CREATE TRIGGER torrent_catalog_search_ai AFTER INSERT ON torrent_catalog BEGIN
            UPDATE torrent_catalog_state
            SET search_incomplete=search_incomplete+
                CASE WHEN new.search_incomplete IS NULL OR new.search_incomplete=1 THEN 1 ELSE 0 END
            WHERE singleton=1;
        END;
        CREATE TRIGGER torrent_catalog_search_ad AFTER DELETE ON torrent_catalog BEGIN
            UPDATE torrent_catalog_state
            SET search_incomplete=search_incomplete-
                CASE WHEN old.search_incomplete IS NULL OR old.search_incomplete=1 THEN 1 ELSE 0 END
            WHERE singleton=1;
        END;
        CREATE TRIGGER torrent_catalog_search_au AFTER UPDATE OF search_incomplete ON torrent_catalog BEGIN
            UPDATE torrent_catalog_state
            SET search_incomplete=search_incomplete
                + CASE WHEN new.search_incomplete IS NULL OR new.search_incomplete=1 THEN 1 ELSE 0 END
                - CASE WHEN old.search_incomplete IS NULL OR old.search_incomplete=1 THEN 1 ELSE 0 END
            WHERE singleton=1;
        END;
        PRAGMA user_version=4;
    "#,
    )?;
    Ok(())
}

/// 仅补充索引，不改变 schema 版本、记录或已有 metadata。
fn claim_index(connection: &Connection) -> Result<(), StorageError> {
    connection.execute_batch("CREATE INDEX IF NOT EXISTS swarm_metadata_by_metadata ON swarm_metadata(metadata_id,verification);")?;
    connection.execute_batch(
        "CREATE INDEX IF NOT EXISTS infohash_first_seen ON infohashes(first_seen,hash);",
    )?;
    connection.execute_batch(
        "CREATE INDEX IF NOT EXISTS metadata_fetched_at_hash ON metadata(fetched_at DESC,hash DESC);",
    )?;
    connection.execute_batch("CREATE INDEX IF NOT EXISTS fetch_claim_due ON fetch_jobs(due_at,hash) WHERE state IN ('pending','retry_wait');")?;
    connection.execute_batch(
        "CREATE INDEX IF NOT EXISTS fetch_retry_due ON fetch_jobs(due_at,hash)
         WHERE state IN ('pending','retry_wait') AND generation > 0;",
    )?;
    Ok(())
}

/// v5 将线上查找键与完整身份分离；目录是可重建数据，原始 info 逐字节迁移。
fn upgrade_v5(tx: &Transaction<'_>) -> Result<(), StorageError> {
    tx.execute_batch(r#"
        CREATE TEMP TABLE catalog_v4_backup AS SELECT * FROM torrent_catalog;
        DROP TABLE torrent_catalog_fts;
        DROP TABLE torrent_catalog;
        DROP TABLE torrent_catalog_state;
        DROP TRIGGER metadata_catalog_total_ai;
        DROP TRIGGER metadata_catalog_total_ad;
        ALTER TABLE metadata RENAME TO metadata_v4;
        CREATE TABLE metadata (
            id INTEGER PRIMARY KEY,
            hash BLOB NOT NULL UNIQUE CHECK(length(hash) IN (20,32)),
            info BLOB NOT NULL CHECK(length(info) BETWEEN 1 AND 4194304),
            fetched_at INTEGER NOT NULL CHECK(fetched_at>=0)
        ) STRICT;
        INSERT INTO metadata(hash,info,fetched_at) SELECT hash,info,fetched_at FROM metadata_v4;
        CREATE TABLE torrent_identities (
            kind TEXT NOT NULL CHECK(kind IN ('v1','v2')),
            hash BLOB NOT NULL,
            metadata_id INTEGER NOT NULL REFERENCES metadata(id),
            CHECK((kind='v1' AND length(hash)=20) OR (kind='v2' AND length(hash)=32)),
            PRIMARY KEY(kind,hash),
            UNIQUE(metadata_id,kind)
        ) STRICT;
        CREATE TABLE swarm_metadata (
            hash BLOB NOT NULL REFERENCES infohashes(hash) CHECK(length(hash)=20),
            metadata_id INTEGER NOT NULL REFERENCES metadata(id),
            verification TEXT NOT NULL CHECK(verification IN ('v1_full','v2_prefix','hybrid_derived')),
            PRIMARY KEY(hash,metadata_id)
        ) STRICT;
        INSERT INTO torrent_identities SELECT 'v1',hash,id FROM metadata;
        INSERT INTO swarm_metadata SELECT hash,id,'v1_full' FROM metadata;
        DROP TABLE metadata_v4;
        CREATE TABLE torrent_catalog (
            id INTEGER PRIMARY KEY,
            hash BLOB NOT NULL UNIQUE REFERENCES metadata(hash) ON DELETE CASCADE,
            parse_status TEXT NOT NULL CHECK(parse_status IN ('parsed','unavailable')),
            name TEXT,
            name_truncated INTEGER NOT NULL CHECK(name_truncated IN (0,1)),
            encoding_lossy INTEGER NOT NULL CHECK(encoding_lossy IN (0,1)),
            total_length TEXT,
            file_count INTEGER,
            piece_length TEXT,
            piece_count INTEGER,
            private INTEGER CHECK(private IN (0,1)),
            search_text TEXT NOT NULL
        ) STRICT;
        CREATE INDEX torrent_catalog_hash ON torrent_catalog(hash);
        CREATE VIRTUAL TABLE torrent_catalog_fts USING fts5(
            search_text,
            content='torrent_catalog',
            content_rowid='id',
            tokenize='trigram'
        );
        CREATE TRIGGER torrent_catalog_ai AFTER INSERT ON torrent_catalog BEGIN
            INSERT INTO torrent_catalog_fts(rowid,search_text) VALUES(new.id,new.search_text);
        END;
        CREATE TRIGGER torrent_catalog_ad AFTER DELETE ON torrent_catalog BEGIN
            INSERT INTO torrent_catalog_fts(torrent_catalog_fts,rowid,search_text)
            VALUES('delete',old.id,old.search_text);
        END;
        CREATE TRIGGER torrent_catalog_au AFTER UPDATE ON torrent_catalog BEGIN
            INSERT INTO torrent_catalog_fts(torrent_catalog_fts,rowid,search_text)
            VALUES('delete',old.id,old.search_text);
            INSERT INTO torrent_catalog_fts(rowid,search_text) VALUES(new.id,new.search_text);
        END;
        CREATE TABLE torrent_catalog_state (
            singleton INTEGER PRIMARY KEY CHECK(singleton=1),
            indexed INTEGER NOT NULL CHECK(indexed>=0),
            total INTEGER NOT NULL CHECK(total>=indexed)
        ) STRICT;
        INSERT INTO torrent_catalog_state(singleton,indexed,total)
        SELECT 1,0,count(*) FROM metadata;
        CREATE TRIGGER metadata_catalog_total_ai AFTER INSERT ON metadata BEGIN
            UPDATE torrent_catalog_state SET total=total+1 WHERE singleton=1;
        END;
        CREATE TRIGGER metadata_catalog_total_ad AFTER DELETE ON metadata BEGIN
            UPDATE torrent_catalog_state SET total=total-1 WHERE singleton=1;
        END;
        CREATE TRIGGER torrent_catalog_indexed_ai AFTER INSERT ON torrent_catalog BEGIN
            UPDATE torrent_catalog_state SET indexed=indexed+1 WHERE singleton=1;
        END;
        CREATE TRIGGER torrent_catalog_indexed_ad AFTER DELETE ON torrent_catalog BEGIN
            UPDATE torrent_catalog_state SET indexed=indexed-1 WHERE singleton=1;
        END;

    "#)?;
    upgrade_v4(tx)?;
    tx.execute_batch(r#"
        ALTER TABLE torrent_catalog ADD COLUMN format TEXT NOT NULL DEFAULT 'unknown';
        ALTER TABLE torrent_catalog ADD COLUMN semantic_status TEXT NOT NULL DEFAULT 'pending';
        ALTER TABLE torrent_catalog ADD COLUMN semantic_reason TEXT;
        ALTER TABLE torrent_catalog ADD COLUMN piece_space_length TEXT;
        ALTER TABLE torrent_catalog ADD COLUMN padding_length TEXT;
        INSERT INTO torrent_catalog(hash,parse_status,name,name_truncated,encoding_lossy,total_length,file_count,piece_length,piece_count,private,search_text,search_incomplete)
            SELECT hash,parse_status,name,name_truncated,encoding_lossy,total_length,file_count,piece_length,piece_count,private,search_text,NULL FROM catalog_v4_backup;
        DROP TABLE catalog_v4_backup;
        ALTER TABLE node_identities RENAME TO node_identities_v4;
        CREATE TABLE node_identities (
            identity INTEGER PRIMARY KEY,
            instance TEXT NOT NULL CHECK(length(instance) BETWEEN 1 AND 128),
            family INTEGER NOT NULL CHECK(family IN (4,6)),
            node_id BLOB NOT NULL CHECK(length(node_id)=20),
            created_at INTEGER NOT NULL CHECK(created_at>=0),
            method TEXT NOT NULL CHECK(method IN ('random-v1','bep42')),
            external_ip BLOB CHECK(external_ip IS NULL OR length(external_ip) IN (4,16)),
            external_changed_at INTEGER CHECK(external_changed_at IS NULL OR external_changed_at>=0),
            UNIQUE(instance,family)
        ) STRICT;
        INSERT INTO node_identities(identity,instance,family,node_id,created_at,method)
            SELECT identity,instance,family,node_id,created_at,method FROM node_identities_v4;
        PRAGMA user_version=5;
    "#)?;
    // SQLite 重命名会重写外键；联系人和采样预约也在同一事务内改回新身份表。
    for table in ["routing_contacts", "sampling_cooldowns"] {
        let sql: String = tx.query_row(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?1",
            [table],
            |r| r.get(0),
        )?;
        tx.execute_batch(&format!("ALTER TABLE {table} RENAME TO {table}_v4;"))?;
        tx.execute_batch(&sql.replace("node_identities_v4", "node_identities"))?;
        tx.execute_batch(&format!(
            "INSERT INTO {table} SELECT * FROM {table}_v4; DROP TABLE {table}_v4;"
        ))?;
    }
    tx.execute_batch("DROP TABLE node_identities_v4; CREATE INDEX cooldown_expiry ON sampling_cooldowns(identity,pending,until_at);")?;
    Ok(())
}
