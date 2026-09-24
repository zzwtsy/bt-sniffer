-- 固定历史 schema v4；来源 d75a224，迁移测试不得用当前 schema 冒充旧库。

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
