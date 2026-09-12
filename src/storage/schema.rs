//! Storage 打开连接时执行版本检查与迁移；表、索引和版本号在同一事务中提交。
//! 拒绝未知的较新版本，迁移失败保留原库，不能靠删除数据重新初始化。
use super::StorageError;
use rusqlite::Connection;

/// 新库顺序执行 v1 和 v2；旧库只执行尚未完成的版本，最后统一提交。
pub(super) fn migrate(connection: &mut Connection) -> Result<(), StorageError> {
    let version: i64 = connection.pragma_query_value(None, "user_version", |r| r.get(0))?;
    if version > 2 {
        return Err(StorageError::Invalid("数据库由更新版本程序创建"));
    }
    if version == 2 {
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
    tx.commit()?;
    claim_index(connection)
}

/// 仅补充索引，不改变 schema 版本、记录或已有 metadata。
fn claim_index(connection: &Connection) -> Result<(), StorageError> {
    connection.execute_batch("CREATE INDEX IF NOT EXISTS fetch_claim_due ON fetch_jobs(due_at,hash) WHERE state IN ('pending','retry_wait');")?;
    Ok(())
}
