use rusqlite::{Connection, params};

use crate::error::{Result, StorageError};

struct Migration {
    version: i64,
    sql: &'static str,
}

const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        sql: r#"
            CREATE TABLE schema_migrations (
                version     INTEGER PRIMARY KEY,
                applied_at  INTEGER NOT NULL
            );

            CREATE TABLE settings (
                key         TEXT PRIMARY KEY NOT NULL,
                kind        TEXT NOT NULL CHECK (kind IN ('string', 'bytes')),
                value       BLOB NOT NULL
            );

            CREATE TABLE devices (
                device_id   TEXT PRIMARY KEY NOT NULL,
                name        TEXT NOT NULL,
                public_key  BLOB,
                address     TEXT NOT NULL,
                first_seen  INTEGER NOT NULL,
                last_seen   INTEGER NOT NULL
            );

            CREATE TABLE trusted_peers (
                device_id   TEXT PRIMARY KEY NOT NULL,
                name        TEXT NOT NULL,
                public_key  BLOB NOT NULL,
                address     TEXT NOT NULL,
                trusted_at  INTEGER NOT NULL,
                last_seen   INTEGER NOT NULL
            );

            CREATE TABLE jobs (
                id                  TEXT PRIMARY KEY NOT NULL,
                payload             BLOB NOT NULL,
                status              TEXT NOT NULL CHECK (
                    status IN (
                        'pending', 'scanning', 'transferring', 'verifying', 'paused',
                        'completed', 'completed_with_errors', 'failed', 'cancelled', 'interrupted'
                    )
                ),
                progress_cbor       BLOB NOT NULL,
                bytes_transferred   INTEGER NOT NULL,
                total_bytes         INTEGER NOT NULL,
                files_completed     INTEGER NOT NULL,
                total_files         INTEGER NOT NULL,
                error               TEXT,
                created_at          INTEGER NOT NULL,
                updated_at          INTEGER NOT NULL
            );

            CREATE TABLE job_files (
                job_id              TEXT NOT NULL,
                path                TEXT NOT NULL,
                size                INTEGER NOT NULL,
                status              TEXT NOT NULL CHECK (
                    status IN (
                        'pending', 'running', 'completed', 'failed',
                        'skipped', 'interrupted'
                    )
                ),
                bytes_transferred   INTEGER NOT NULL,
                error               TEXT,
                updated_at          INTEGER NOT NULL,
                PRIMARY KEY (job_id, path),
                FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
            );

            CREATE TABLE chunks (
                job_id          TEXT NOT NULL,
                path            TEXT NOT NULL,
                chunk_index     INTEGER NOT NULL,
                source_hash     BLOB NOT NULL CHECK (length(source_hash) = 32),
                completed_at    INTEGER NOT NULL,
                PRIMARY KEY (job_id, path, chunk_index),
                FOREIGN KEY (job_id, path)
                    REFERENCES job_files(job_id, path) ON DELETE CASCADE
            );

            CREATE TABLE hash_cache (
                path            TEXT NOT NULL,
                size            INTEGER NOT NULL,
                mtime_ns        INTEGER NOT NULL,
                chunk_size      INTEGER NOT NULL,
                full_hash       BLOB NOT NULL CHECK (length(full_hash) = 32),
                chunks_cbor     BLOB NOT NULL,
                updated_at      INTEGER NOT NULL,
                PRIMARY KEY (path, size, mtime_ns, chunk_size)
            );
        "#,
    },
    Migration {
        version: 2,
        sql: r#"
            CREATE INDEX devices_last_seen_idx
                ON devices(last_seen DESC);
            CREATE INDEX trusted_peers_name_idx
                ON trusted_peers(name COLLATE NOCASE);
            CREATE INDEX jobs_created_at_idx
                ON jobs(created_at DESC);
            CREATE INDEX jobs_status_idx
                ON jobs(status);
            CREATE INDEX job_files_status_idx
                ON job_files(job_id, status);
            CREATE INDEX chunks_source_idx
                ON chunks(job_id, path, source_hash);
        "#,
    },
];

pub(crate) fn migrate(connection: &mut Connection, applied_at: i64) -> Result<()> {
    let current_version = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    let supported_version = MIGRATIONS.last().map_or(0, |migration| migration.version);
    if current_version > supported_version {
        return Err(StorageError::UnsupportedSchemaVersion {
            found: current_version,
            supported: supported_version,
        });
    }

    for migration in MIGRATIONS {
        if migration.version <= current_version {
            continue;
        }

        let transaction = connection.transaction()?;
        transaction.execute_batch(migration.sql)?;
        transaction.execute(
            "INSERT INTO schema_migrations (version, applied_at) VALUES (?1, ?2)",
            params![migration.version, applied_at],
        )?;
        transaction.pragma_update(None, "user_version", migration.version)?;
        transaction.commit()?;
    }

    Ok(())
}
