use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fastsync_core::{JobStatus as CoreJobStatus, TransferJob, TransferProgress};
use parking_lot::Mutex;
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use crate::error::{Result, StorageError};
use crate::migrations::migrate;
use crate::models::{
    CompletedChunk, DiscoveredDevice, HashCacheEntry, HashCacheKey, JobFileRecord, JobFileStatus,
    JobRecord, StoredJobStatus, TrustedPeer,
};

const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

struct DatabaseInner {
    connection: Mutex<Connection>,
}

/// A clonable handle to FastSync's SQLite database.
///
/// SQLite access is serialized by a `parking_lot::Mutex`. Values are encoded
/// before taking that lock and decoded after releasing it.
#[derive(Clone)]
pub struct Database {
    inner: Arc<DatabaseInner>,
}

impl Database {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent).map_err(|source| StorageError::CreateDirectory {
                path: parent.to_path_buf(),
                source,
            })?;
            secure_directory(parent)?;
        }

        secure_database_file(path)?;

        Self::initialize(Connection::open(path)?)
    }

    pub fn open_in_memory() -> Result<Self> {
        Self::initialize(Connection::open_in_memory()?)
    }

    fn initialize(mut connection: Connection) -> Result<Self> {
        connection.busy_timeout(BUSY_TIMEOUT)?;
        connection.pragma_update(None, "foreign_keys", true)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "NORMAL")?;

        let now = now_millis()?;
        migrate(&mut connection, now)?;
        mark_running_jobs_interrupted(&mut connection, now)?;

        Ok(Self {
            inner: Arc::new(DatabaseInner {
                connection: Mutex::new(connection),
            }),
        })
    }

    pub fn set_string(&self, key: &str, value: &str) -> Result<()> {
        self.set_setting(key, "string", value.as_bytes())
    }

    pub fn get_string(&self, key: &str) -> Result<Option<String>> {
        let Some((kind, value)) = self.get_setting(key)? else {
            return Ok(None);
        };
        if kind != "string" {
            return Err(StorageError::SettingType {
                key: key.to_owned(),
                expected: "string",
                actual: kind,
            });
        }

        String::from_utf8(value)
            .map(Some)
            .map_err(|source| StorageError::InvalidSettingString {
                key: key.to_owned(),
                source,
            })
    }

    pub fn get_or_insert_string(&self, key: &str, value: &str) -> Result<String> {
        let connection = self.inner.connection.lock();
        connection.execute(
            "INSERT OR IGNORE INTO settings (key, kind, value) VALUES (?1, 'string', ?2)",
            params![key, value.as_bytes()],
        )?;
        let (kind, value): (String, Vec<u8>) = connection.query_row(
            "SELECT kind, value FROM settings WHERE key = ?1",
            params![key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if kind != "string" {
            return Err(StorageError::SettingType {
                key: key.to_owned(),
                expected: "string",
                actual: kind,
            });
        }
        String::from_utf8(value).map_err(|source| StorageError::InvalidSettingString {
            key: key.to_owned(),
            source,
        })
    }

    pub fn set_bytes(&self, key: &str, value: &[u8]) -> Result<()> {
        self.set_setting(key, "bytes", value)
    }

    pub fn set_bytes_batch(&self, values: &[(&str, &[u8])]) -> Result<()> {
        let mut connection = self.inner.connection.lock();
        let transaction = connection.transaction()?;
        for (key, value) in values {
            transaction.execute(
                r#"
                    INSERT INTO settings (key, kind, value)
                    VALUES (?1, 'bytes', ?2)
                    ON CONFLICT(key) DO UPDATE SET
                        kind = excluded.kind,
                        value = excluded.value
                "#,
                params![key, value],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn get_bytes(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let Some((kind, value)) = self.get_setting(key)? else {
            return Ok(None);
        };
        if kind != "bytes" {
            return Err(StorageError::SettingType {
                key: key.to_owned(),
                expected: "bytes",
                actual: kind,
            });
        }

        Ok(Some(value))
    }

    pub fn remove_setting(&self, key: &str) -> Result<bool> {
        let connection = self.inner.connection.lock();
        Ok(connection.execute("DELETE FROM settings WHERE key = ?1", params![key])? != 0)
    }

    fn set_setting(&self, key: &str, kind: &str, value: &[u8]) -> Result<()> {
        let connection = self.inner.connection.lock();
        connection.execute(
            r#"
                INSERT INTO settings (key, kind, value)
                VALUES (?1, ?2, ?3)
                ON CONFLICT(key) DO UPDATE SET
                    kind = excluded.kind,
                    value = excluded.value
            "#,
            params![key, kind, value],
        )?;
        Ok(())
    }

    fn get_setting(&self, key: &str) -> Result<Option<(String, Vec<u8>)>> {
        let connection = self.inner.connection.lock();
        Ok(connection
            .query_row(
                "SELECT kind, value FROM settings WHERE key = ?1",
                params![key],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?)
    }

    pub fn upsert_discovered_device(&self, device: &DiscoveredDevice) -> Result<()> {
        let connection = self.inner.connection.lock();
        connection.execute(
            r#"
                INSERT INTO devices (
                    device_id, name, public_key, address, first_seen, last_seen
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                ON CONFLICT(device_id) DO UPDATE SET
                    name = CASE
                        WHEN excluded.public_key IS NOT NULL THEN excluded.name
                        WHEN devices.public_key IS NOT NULL THEN devices.name
                        ELSE excluded.name
                    END,
                    public_key = COALESCE(excluded.public_key, devices.public_key),
                    address = CASE
                        WHEN excluded.public_key IS NOT NULL THEN excluded.address
                        WHEN devices.public_key IS NOT NULL THEN devices.address
                        ELSE excluded.address
                    END,
                    first_seen = MIN(devices.first_seen, excluded.first_seen),
                    last_seen = excluded.last_seen
            "#,
            params![
                device.device_id.to_string(),
                device.name,
                device.public_key,
                device.address,
                device.first_seen,
                device.last_seen,
            ],
        )?;
        Ok(())
    }

    pub fn get_discovered_device(&self, device_id: Uuid) -> Result<Option<DiscoveredDevice>> {
        let raw = {
            let connection = self.inner.connection.lock();
            connection
                .query_row(
                    r#"
                        SELECT device_id, name, public_key, address, first_seen, last_seen
                        FROM devices
                        WHERE device_id = ?1
                    "#,
                    params![device_id.to_string()],
                    raw_device,
                )
                .optional()?
        };
        raw.map(device_from_raw).transpose()
    }

    pub fn list_discovered_devices(&self) -> Result<Vec<DiscoveredDevice>> {
        let raw_devices = {
            let connection = self.inner.connection.lock();
            let mut statement = connection.prepare(
                r#"
                    SELECT device_id, name, public_key, address, first_seen, last_seen
                    FROM devices
                    ORDER BY last_seen DESC, name COLLATE NOCASE
                "#,
            )?;
            let rows = statement
                .query_map([], raw_device)?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            rows
        };

        raw_devices.into_iter().map(device_from_raw).collect()
    }

    pub fn remove_discovered_device(&self, device_id: Uuid) -> Result<bool> {
        let connection = self.inner.connection.lock();
        Ok(connection.execute(
            "DELETE FROM devices WHERE device_id = ?1",
            params![device_id.to_string()],
        )? != 0)
    }

    pub fn upsert_trusted_peer(&self, peer: &TrustedPeer) -> Result<()> {
        let connection = self.inner.connection.lock();
        connection.execute(
            r#"
                INSERT INTO trusted_peers (
                    device_id, name, public_key, address, trusted_at, last_seen
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                ON CONFLICT(device_id) DO UPDATE SET
                    name = excluded.name,
                    public_key = excluded.public_key,
                    address = excluded.address,
                    trusted_at = excluded.trusted_at,
                    last_seen = excluded.last_seen
            "#,
            params![
                peer.device_id.to_string(),
                peer.name,
                peer.public_key,
                peer.address,
                peer.trusted_at,
                peer.last_seen,
            ],
        )?;
        Ok(())
    }

    pub fn get_trusted_peer(&self, device_id: Uuid) -> Result<Option<TrustedPeer>> {
        let raw = {
            let connection = self.inner.connection.lock();
            connection
                .query_row(
                    r#"
                        SELECT device_id, name, public_key, address, trusted_at, last_seen
                        FROM trusted_peers
                        WHERE device_id = ?1
                    "#,
                    params![device_id.to_string()],
                    raw_trusted_peer,
                )
                .optional()?
        };
        raw.map(trusted_peer_from_raw).transpose()
    }

    pub fn list_trusted_peers(&self) -> Result<Vec<TrustedPeer>> {
        let raw_peers = {
            let connection = self.inner.connection.lock();
            let mut statement = connection.prepare(
                r#"
                    SELECT device_id, name, public_key, address, trusted_at, last_seen
                    FROM trusted_peers
                    ORDER BY name COLLATE NOCASE, device_id
                "#,
            )?;
            let rows = statement
                .query_map([], raw_trusted_peer)?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            rows
        };

        raw_peers.into_iter().map(trusted_peer_from_raw).collect()
    }

    pub fn remove_trusted_peer(&self, device_id: Uuid) -> Result<bool> {
        let connection = self.inner.connection.lock();
        Ok(connection.execute(
            "DELETE FROM trusted_peers WHERE device_id = ?1",
            params![device_id.to_string()],
        )? != 0)
    }

    pub fn create_job(&self, job: &TransferJob) -> Result<()> {
        let now = now_millis()?;
        self.create_job_record(&JobRecord {
            job: job.clone(),
            status: job.status.into(),
            error: None,
            created_at: now,
            updated_at: now,
        })
    }

    pub fn create_job_if_absent(&self, job: &TransferJob) -> Result<bool> {
        let now = now_millis()?;
        self.create_job_record_if_absent(&JobRecord {
            job: job.clone(),
            status: job.status.into(),
            error: None,
            created_at: now,
            updated_at: now,
        })
    }

    pub fn create_job_record_if_absent(&self, job: &JobRecord) -> Result<bool> {
        let prepared = PreparedJob::from_record(job)?;
        let connection = self.inner.connection.lock();
        Ok(connection.execute(
            r#"
                INSERT INTO jobs (
                    id, payload, status, progress_cbor, bytes_transferred,
                    total_bytes, files_completed, total_files, error,
                    created_at, updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
                ON CONFLICT(id) DO NOTHING
            "#,
            params![
                prepared.id,
                prepared.payload,
                prepared.status,
                prepared.progress.cbor,
                prepared.progress.bytes_transferred,
                prepared.progress.total_bytes,
                prepared.progress.files_completed,
                prepared.progress.total_files,
                prepared.error,
                prepared.created_at,
                prepared.updated_at,
            ],
        )? != 0)
    }

    pub fn create_job_record(&self, job: &JobRecord) -> Result<()> {
        let prepared = PreparedJob::from_record(job)?;
        let connection = self.inner.connection.lock();
        connection.execute(
            r#"
                INSERT INTO jobs (
                    id, payload, status, progress_cbor, bytes_transferred,
                    total_bytes, files_completed, total_files, error,
                    created_at, updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
            "#,
            params![
                prepared.id,
                prepared.payload,
                prepared.status,
                prepared.progress.cbor,
                prepared.progress.bytes_transferred,
                prepared.progress.total_bytes,
                prepared.progress.files_completed,
                prepared.progress.total_files,
                prepared.error,
                prepared.created_at,
                prepared.updated_at,
            ],
        )?;
        Ok(())
    }

    pub fn upsert_job(&self, job: &TransferJob) -> Result<()> {
        let now = now_millis()?;
        self.upsert_job_record(&JobRecord {
            job: job.clone(),
            status: job.status.into(),
            error: None,
            created_at: now,
            updated_at: now,
        })
    }

    pub fn upsert_job_record(&self, job: &JobRecord) -> Result<()> {
        let prepared = PreparedJob::from_record(job)?;
        let connection = self.inner.connection.lock();
        connection.execute(
            r#"
                INSERT INTO jobs (
                    id, payload, status, progress_cbor, bytes_transferred,
                    total_bytes, files_completed, total_files, error,
                    created_at, updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
                ON CONFLICT(id) DO UPDATE SET
                    payload = excluded.payload,
                    status = excluded.status,
                    progress_cbor = excluded.progress_cbor,
                    bytes_transferred = excluded.bytes_transferred,
                    total_bytes = excluded.total_bytes,
                    files_completed = excluded.files_completed,
                    total_files = excluded.total_files,
                    error = excluded.error,
                    updated_at = excluded.updated_at
            "#,
            params![
                prepared.id,
                prepared.payload,
                prepared.status,
                prepared.progress.cbor,
                prepared.progress.bytes_transferred,
                prepared.progress.total_bytes,
                prepared.progress.files_completed,
                prepared.progress.total_files,
                prepared.error,
                prepared.created_at,
                prepared.updated_at,
            ],
        )?;
        Ok(())
    }

    pub fn get_job(&self, job_id: Uuid) -> Result<Option<JobRecord>> {
        let raw = {
            let connection = self.inner.connection.lock();
            connection
                .query_row(
                    r#"
                        SELECT id, payload, status, progress_cbor,
                               bytes_transferred, total_bytes, files_completed,
                               total_files, error, created_at, updated_at
                        FROM jobs
                        WHERE id = ?1
                    "#,
                    params![job_id.to_string()],
                    raw_job,
                )
                .optional()?
        };
        raw.map(job_from_raw).transpose()
    }

    pub fn list_jobs(&self) -> Result<Vec<JobRecord>> {
        let raw_jobs = {
            let connection = self.inner.connection.lock();
            let mut statement = connection.prepare(
                r#"
                    SELECT id, payload, status, progress_cbor,
                           bytes_transferred, total_bytes, files_completed,
                           total_files, error, created_at, updated_at
                    FROM jobs
                    ORDER BY created_at DESC, id
                "#,
            )?;
            let rows = statement
                .query_map([], raw_job)?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            rows
        };

        raw_jobs.into_iter().map(job_from_raw).collect()
    }

    pub fn update_job_status(&self, job_id: Uuid, status: CoreJobStatus) -> Result<bool> {
        self.update_stored_job_status(job_id, status.into())
    }

    pub fn update_job_status_with_error(
        &self,
        job_id: Uuid,
        status: CoreJobStatus,
        error: Option<&str>,
    ) -> Result<bool> {
        let updated_at = now_millis()?;
        let connection = self.inner.connection.lock();
        Ok(connection.execute(
            r#"
                UPDATE jobs
                SET status = ?2, error = ?3, updated_at = ?4
                WHERE id = ?1
            "#,
            params![
                job_id.to_string(),
                StoredJobStatus::from(status).as_str(),
                error,
                updated_at
            ],
        )? != 0)
    }

    pub fn mark_job_interrupted(&self, job_id: Uuid) -> Result<bool> {
        self.update_stored_job_status(job_id, StoredJobStatus::Interrupted)
    }

    fn update_stored_job_status(&self, job_id: Uuid, status: StoredJobStatus) -> Result<bool> {
        let updated_at = now_millis()?;
        let connection = self.inner.connection.lock();
        Ok(connection.execute(
            "UPDATE jobs SET status = ?2, updated_at = ?3 WHERE id = ?1",
            params![job_id.to_string(), status.as_str(), updated_at],
        )? != 0)
    }

    pub fn update_job_progress(&self, job_id: Uuid, progress: &TransferProgress) -> Result<bool> {
        let progress = PreparedProgress::new(progress)?;
        let updated_at = now_millis()?;
        let connection = self.inner.connection.lock();
        Ok(connection.execute(
            r#"
                UPDATE jobs
                SET progress_cbor = ?2,
                    bytes_transferred = ?3,
                    total_bytes = ?4,
                    files_completed = ?5,
                    total_files = ?6,
                    updated_at = ?7
                WHERE id = ?1
            "#,
            params![
                job_id.to_string(),
                progress.cbor,
                progress.bytes_transferred,
                progress.total_bytes,
                progress.files_completed,
                progress.total_files,
                updated_at,
            ],
        )? != 0)
    }

    pub fn update_job_status_and_progress(
        &self,
        job_id: Uuid,
        status: CoreJobStatus,
        progress: &TransferProgress,
    ) -> Result<bool> {
        let progress = PreparedProgress::new(progress)?;
        let updated_at = now_millis()?;
        let connection = self.inner.connection.lock();
        Ok(connection.execute(
            r#"
                UPDATE jobs
                SET status = ?2,
                    progress_cbor = ?3,
                    bytes_transferred = ?4,
                    total_bytes = ?5,
                    files_completed = ?6,
                    total_files = ?7,
                    updated_at = ?8
                WHERE id = ?1
            "#,
            params![
                job_id.to_string(),
                StoredJobStatus::from(status).as_str(),
                progress.cbor,
                progress.bytes_transferred,
                progress.total_bytes,
                progress.files_completed,
                progress.total_files,
                updated_at,
            ],
        )? != 0)
    }

    pub fn remove_job(&self, job_id: Uuid) -> Result<bool> {
        let connection = self.inner.connection.lock();
        Ok(connection.execute(
            "DELETE FROM jobs WHERE id = ?1",
            params![job_id.to_string()],
        )? != 0)
    }

    pub fn upsert_job_file(&self, file: &JobFileRecord) -> Result<()> {
        let prepared = PreparedJobFile::from_record(file)?;
        let connection = self.inner.connection.lock();
        upsert_prepared_job_file(&connection, &prepared)?;
        Ok(())
    }

    pub fn upsert_job_files(&self, files: &[JobFileRecord]) -> Result<()> {
        let prepared = files
            .iter()
            .map(PreparedJobFile::from_record)
            .collect::<Result<Vec<_>>>()?;
        let mut connection = self.inner.connection.lock();
        let transaction = connection.transaction()?;
        for file in &prepared {
            upsert_prepared_job_file(&transaction, file)?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn replace_job_files(&self, job_id: Uuid, files: &[JobFileRecord]) -> Result<()> {
        if let Some(file) = files.iter().find(|file| file.job_id != job_id) {
            return Err(StorageError::InvalidData {
                field: "job_files.job_id",
                message: format!(
                    "file {} belongs to {}, not {}",
                    file.path, file.job_id, job_id
                ),
            });
        }
        let prepared = files
            .iter()
            .map(PreparedJobFile::from_record)
            .collect::<Result<Vec<_>>>()?;

        let mut connection = self.inner.connection.lock();
        let transaction = connection.transaction()?;
        transaction.execute(
            "DELETE FROM job_files WHERE job_id = ?1",
            params![job_id.to_string()],
        )?;
        for file in &prepared {
            upsert_prepared_job_file(&transaction, file)?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn get_job_file(&self, job_id: Uuid, path: &str) -> Result<Option<JobFileRecord>> {
        let raw = {
            let connection = self.inner.connection.lock();
            connection
                .query_row(
                    r#"
                        SELECT job_id, path, size, status, bytes_transferred, error, updated_at
                        FROM job_files
                        WHERE job_id = ?1 AND path = ?2
                    "#,
                    params![job_id.to_string(), path],
                    raw_job_file,
                )
                .optional()?
        };
        raw.map(job_file_from_raw).transpose()
    }

    pub fn list_job_files(&self, job_id: Uuid) -> Result<Vec<JobFileRecord>> {
        let raw_files = {
            let connection = self.inner.connection.lock();
            let mut statement = connection.prepare(
                r#"
                    SELECT job_id, path, size, status, bytes_transferred, error, updated_at
                    FROM job_files
                    WHERE job_id = ?1
                    ORDER BY path
                "#,
            )?;
            let rows = statement
                .query_map(params![job_id.to_string()], raw_job_file)?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            rows
        };

        raw_files.into_iter().map(job_file_from_raw).collect()
    }

    pub fn retain_job_files(
        &self,
        job_id: Uuid,
        retained_paths: &HashSet<String>,
    ) -> Result<Vec<String>> {
        let mut connection = self.inner.connection.lock();
        let transaction = connection.transaction()?;
        let existing = {
            let mut statement = transaction
                .prepare("SELECT path FROM job_files WHERE job_id = ?1 ORDER BY path")?;
            statement
                .query_map(params![job_id.to_string()], |row| row.get::<_, String>(0))?
                .collect::<std::result::Result<Vec<_>, _>>()?
        };
        let removed: Vec<_> = existing
            .into_iter()
            .filter(|path| !retained_paths.contains(path))
            .collect();
        for path in &removed {
            transaction.execute(
                "DELETE FROM job_files WHERE job_id = ?1 AND path = ?2",
                params![job_id.to_string(), path],
            )?;
        }
        transaction.commit()?;
        Ok(removed)
    }

    pub fn update_job_file_status(
        &self,
        job_id: Uuid,
        path: &str,
        status: JobFileStatus,
        error: Option<&str>,
    ) -> Result<bool> {
        let updated_at = now_millis()?;
        let connection = self.inner.connection.lock();
        Ok(connection.execute(
            r#"
                UPDATE job_files
                SET status = ?3, error = ?4, updated_at = ?5
                WHERE job_id = ?1 AND path = ?2
            "#,
            params![job_id.to_string(), path, status.as_str(), error, updated_at,],
        )? != 0)
    }

    pub fn update_job_file_progress(
        &self,
        job_id: Uuid,
        path: &str,
        bytes_transferred: u64,
    ) -> Result<bool> {
        let bytes_transferred = sqlite_integer(bytes_transferred, "job_files.bytes_transferred")?;
        let updated_at = now_millis()?;
        let connection = self.inner.connection.lock();
        Ok(connection.execute(
            r#"
                UPDATE job_files
                SET bytes_transferred = ?3, updated_at = ?4
                WHERE job_id = ?1 AND path = ?2
            "#,
            params![job_id.to_string(), path, bytes_transferred, updated_at],
        )? != 0)
    }

    pub fn record_completed_chunk(&self, chunk: &CompletedChunk) -> Result<()> {
        let prepared = PreparedChunk::from_record(chunk)?;
        let connection = self.inner.connection.lock();
        upsert_prepared_chunk(&connection, &prepared)?;
        Ok(())
    }

    pub fn record_completed_chunks(&self, chunks: &[CompletedChunk]) -> Result<()> {
        let prepared = chunks
            .iter()
            .map(PreparedChunk::from_record)
            .collect::<Result<Vec<_>>>()?;
        let mut connection = self.inner.connection.lock();
        let transaction = connection.transaction()?;
        for chunk in &prepared {
            upsert_prepared_chunk(&transaction, chunk)?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn is_chunk_completed(
        &self,
        job_id: Uuid,
        path: &str,
        index: u64,
        source_hash: &[u8; 32],
    ) -> Result<bool> {
        let index = sqlite_integer(index, "chunks.chunk_index")?;
        let connection = self.inner.connection.lock();
        connection
            .query_row(
                r#"
                    SELECT EXISTS(
                        SELECT 1 FROM chunks
                        WHERE job_id = ?1 AND path = ?2
                          AND chunk_index = ?3 AND source_hash = ?4
                    )
                "#,
                params![job_id.to_string(), path, index, source_hash.as_slice()],
                |row| row.get(0),
            )
            .map_err(StorageError::from)
    }

    pub fn completed_chunks(
        &self,
        job_id: Uuid,
        path: &str,
        source_hash: &[u8; 32],
    ) -> Result<Vec<CompletedChunk>> {
        let raw_chunks = {
            let connection = self.inner.connection.lock();
            let mut statement = connection.prepare(
                r#"
                    SELECT job_id, path, chunk_index, source_hash, completed_at
                    FROM chunks
                    WHERE job_id = ?1 AND path = ?2 AND source_hash = ?3
                    ORDER BY chunk_index
                "#,
            )?;
            let rows = statement
                .query_map(
                    params![job_id.to_string(), path, source_hash.as_slice()],
                    raw_chunk,
                )?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            rows
        };

        raw_chunks.into_iter().map(chunk_from_raw).collect()
    }

    pub fn completed_chunk_indices(
        &self,
        job_id: Uuid,
        path: &str,
        source_hash: &[u8; 32],
    ) -> Result<Vec<u64>> {
        Ok(self
            .completed_chunks(job_id, path, source_hash)?
            .into_iter()
            .map(|chunk| chunk.index)
            .collect())
    }

    pub fn remove_completed_chunk(
        &self,
        job_id: Uuid,
        path: &str,
        index: u64,
        source_hash: &[u8; 32],
    ) -> Result<bool> {
        let index = sqlite_integer(index, "chunks.chunk_index")?;
        let connection = self.inner.connection.lock();
        Ok(connection.execute(
            r#"
                DELETE FROM chunks
                WHERE job_id = ?1 AND path = ?2
                  AND chunk_index = ?3 AND source_hash = ?4
            "#,
            params![job_id.to_string(), path, index, source_hash.as_slice()],
        )? != 0)
    }

    pub fn remove_completed_chunks(
        &self,
        job_id: Uuid,
        path: &str,
        source_hash: &[u8; 32],
    ) -> Result<usize> {
        let connection = self.inner.connection.lock();
        Ok(connection.execute(
            r#"
                DELETE FROM chunks
                WHERE job_id = ?1 AND path = ?2 AND source_hash = ?3
            "#,
            params![job_id.to_string(), path, source_hash.as_slice()],
        )?)
    }

    pub fn clear_completed_chunks(&self, job_id: Uuid, path: &str) -> Result<usize> {
        let connection = self.inner.connection.lock();
        Ok(connection.execute(
            "DELETE FROM chunks WHERE job_id = ?1 AND path = ?2",
            params![job_id.to_string(), path],
        )?)
    }

    pub fn put_hash_cache(&self, entry: &HashCacheEntry) -> Result<()> {
        let chunks_cbor = encode_cbor(&entry.chunks, "hash-cache chunk descriptors")?;
        let size = sqlite_integer(entry.key.size, "hash_cache.size")?;
        let chunk_size = sqlite_integer(entry.key.chunk_size, "hash_cache.chunk_size")?;

        let mut connection = self.inner.connection.lock();
        let transaction = connection.transaction()?;
        transaction.execute(
            "DELETE FROM hash_cache WHERE path = ?1",
            params![entry.key.path],
        )?;
        transaction.execute(
            r#"
                INSERT INTO hash_cache (
                    path, size, mtime_ns, chunk_size, full_hash, chunks_cbor, updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            "#,
            params![
                entry.key.path,
                size,
                entry.key.mtime_ns,
                chunk_size,
                entry.full_hash.as_slice(),
                chunks_cbor,
                entry.updated_at,
            ],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn get_hash_cache(&self, key: &HashCacheKey) -> Result<Option<HashCacheEntry>> {
        let size = sqlite_integer(key.size, "hash_cache.size")?;
        let chunk_size = sqlite_integer(key.chunk_size, "hash_cache.chunk_size")?;
        let raw = {
            let connection = self.inner.connection.lock();
            connection
                .query_row(
                    r#"
                        SELECT full_hash, chunks_cbor, updated_at
                        FROM hash_cache
                        WHERE path = ?1 AND size = ?2
                          AND mtime_ns = ?3 AND chunk_size = ?4
                    "#,
                    params![key.path, size, key.mtime_ns, chunk_size],
                    |row| {
                        Ok(RawHashCache {
                            full_hash: row.get(0)?,
                            chunks_cbor: row.get(1)?,
                            updated_at: row.get(2)?,
                        })
                    },
                )
                .optional()?
        };

        let Some(raw) = raw else {
            return Ok(None);
        };
        let full_hash = hash_from_blob(raw.full_hash, "hash_cache.full_hash")?;
        let chunks = decode_cbor(&raw.chunks_cbor, "hash-cache chunk descriptors")?;
        Ok(Some(HashCacheEntry {
            key: key.clone(),
            full_hash,
            chunks,
            updated_at: raw.updated_at,
        }))
    }

    pub fn remove_hash_cache(&self, path: &str) -> Result<usize> {
        let connection = self.inner.connection.lock();
        Ok(connection.execute("DELETE FROM hash_cache WHERE path = ?1", params![path])?)
    }
}

#[cfg(unix)]
fn secure_directory(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(|source| {
        StorageError::SecurePath {
            path: path.to_path_buf(),
            source,
        }
    })
}

#[cfg(not(unix))]
fn secure_directory(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn secure_database_file(path: &Path) -> Result<()> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .mode(0o600)
        .open(path)
        .map_err(|source| StorageError::SecurePath {
            path: path.to_path_buf(),
            source,
        })?;
    file.set_permissions(fs::Permissions::from_mode(0o600))
        .map_err(|source| StorageError::SecurePath {
            path: path.to_path_buf(),
            source,
        })
}

#[cfg(not(unix))]
fn secure_database_file(_path: &Path) -> Result<()> {
    Ok(())
}

fn mark_running_jobs_interrupted(connection: &mut Connection, updated_at: i64) -> Result<()> {
    let transaction = connection.transaction()?;
    transaction.execute(
        r#"
            UPDATE job_files
            SET status = 'interrupted', updated_at = ?1
            WHERE status = 'running'
        "#,
        params![updated_at],
    )?;
    transaction.execute(
        r#"
            UPDATE jobs
            SET status = 'interrupted', updated_at = ?1
            WHERE status IN ('scanning', 'transferring', 'verifying')
        "#,
        params![updated_at],
    )?;
    transaction.commit()?;
    Ok(())
}

fn now_millis() -> Result<i64> {
    let millis = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    i64::try_from(millis).map_err(|_| StorageError::InvalidData {
        field: "timestamp",
        message: format!("{millis} milliseconds does not fit in SQLite INTEGER"),
    })
}

fn sqlite_integer(value: u64, field: &'static str) -> Result<i64> {
    i64::try_from(value).map_err(|_| StorageError::IntegerOutOfRange { field, value })
}

fn unsigned_integer(value: i64, field: &'static str) -> Result<u64> {
    u64::try_from(value).map_err(|_| StorageError::InvalidData {
        field,
        message: format!("negative value {value}"),
    })
}

fn parse_uuid(value: &str, field: &'static str) -> Result<Uuid> {
    Uuid::parse_str(value).map_err(|source| StorageError::InvalidData {
        field,
        message: source.to_string(),
    })
}

fn hash_from_blob(value: Vec<u8>, field: &'static str) -> Result<[u8; 32]> {
    let length = value.len();
    value.try_into().map_err(|_| StorageError::InvalidData {
        field,
        message: format!("expected 32 bytes, found {length}"),
    })
}

fn encode_cbor<T: Serialize + ?Sized>(value: &T, kind: &'static str) -> Result<Vec<u8>> {
    let mut encoded = Vec::new();
    ciborium::ser::into_writer(value, &mut encoded).map_err(|source| StorageError::Encode {
        kind,
        message: source.to_string(),
    })?;
    Ok(encoded)
}

fn decode_cbor<T: DeserializeOwned>(value: &[u8], kind: &'static str) -> Result<T> {
    ciborium::de::from_reader(value).map_err(|source| StorageError::Decode {
        kind,
        message: source.to_string(),
    })
}

type RawDevice = (String, String, Option<Vec<u8>>, String, i64, i64);

fn raw_device(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawDevice> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
    ))
}

fn device_from_raw(raw: RawDevice) -> Result<DiscoveredDevice> {
    Ok(DiscoveredDevice {
        device_id: parse_uuid(&raw.0, "devices.device_id")?,
        name: raw.1,
        public_key: raw.2,
        address: raw.3,
        first_seen: raw.4,
        last_seen: raw.5,
    })
}

type RawTrustedPeer = (String, String, Vec<u8>, String, i64, i64);

fn raw_trusted_peer(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawTrustedPeer> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
    ))
}

fn trusted_peer_from_raw(raw: RawTrustedPeer) -> Result<TrustedPeer> {
    Ok(TrustedPeer {
        device_id: parse_uuid(&raw.0, "trusted_peers.device_id")?,
        name: raw.1,
        public_key: raw.2,
        address: raw.3,
        trusted_at: raw.4,
        last_seen: raw.5,
    })
}

struct PreparedProgress {
    cbor: Vec<u8>,
    bytes_transferred: i64,
    total_bytes: i64,
    files_completed: i64,
    total_files: i64,
}

impl PreparedProgress {
    fn new(progress: &TransferProgress) -> Result<Self> {
        Ok(Self {
            cbor: encode_cbor(progress, "transfer progress")?,
            bytes_transferred: sqlite_integer(
                progress.transferred_bytes,
                "jobs.bytes_transferred",
            )?,
            total_bytes: sqlite_integer(progress.total_bytes, "jobs.total_bytes")?,
            files_completed: sqlite_integer(progress.completed_files, "jobs.files_completed")?,
            total_files: sqlite_integer(progress.total_files, "jobs.total_files")?,
        })
    }
}

struct PreparedJob {
    id: String,
    payload: Vec<u8>,
    status: &'static str,
    progress: PreparedProgress,
    error: Option<String>,
    created_at: i64,
    updated_at: i64,
}

impl PreparedJob {
    fn from_record(job: &JobRecord) -> Result<Self> {
        if let Some(core_status) = job.status.core_status() {
            if core_status != job.job.status {
                return Err(StorageError::InvalidData {
                    field: "jobs.status",
                    message: format!(
                        "stored status {:?} does not match transfer job status {:?}",
                        job.status, job.job.status
                    ),
                });
            }
        }

        Ok(Self {
            id: job.job.id.to_string(),
            payload: encode_cbor(&job.job, "transfer job")?,
            status: job.status.as_str(),
            progress: PreparedProgress::new(&job.job.progress)?,
            error: job.error.clone(),
            created_at: job.created_at,
            updated_at: job.updated_at,
        })
    }
}

struct RawJob {
    id: String,
    payload: Vec<u8>,
    status: String,
    progress_cbor: Vec<u8>,
    bytes_transferred: i64,
    total_bytes: i64,
    files_completed: i64,
    total_files: i64,
    error: Option<String>,
    created_at: i64,
    updated_at: i64,
}

fn raw_job(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawJob> {
    Ok(RawJob {
        id: row.get(0)?,
        payload: row.get(1)?,
        status: row.get(2)?,
        progress_cbor: row.get(3)?,
        bytes_transferred: row.get(4)?,
        total_bytes: row.get(5)?,
        files_completed: row.get(6)?,
        total_files: row.get(7)?,
        error: row.get(8)?,
        created_at: row.get(9)?,
        updated_at: row.get(10)?,
    })
}

fn job_from_raw(raw: RawJob) -> Result<JobRecord> {
    let status =
        StoredJobStatus::from_str(&raw.status).ok_or_else(|| StorageError::InvalidData {
            field: "jobs.status",
            message: format!("unknown status {:?}", raw.status),
        })?;
    let id = parse_uuid(&raw.id, "jobs.id")?;
    let mut job: TransferJob = decode_cbor(&raw.payload, "transfer job")?;
    if job.id != id {
        return Err(StorageError::InvalidData {
            field: "jobs.payload",
            message: format!("job ID {} does not match row ID {id}", job.id),
        });
    }
    let progress: TransferProgress = decode_cbor(&raw.progress_cbor, "transfer progress")?;
    validate_progress_columns(&raw, &progress)?;
    job.progress = progress;
    if let Some(core_status) = status.core_status() {
        job.status = core_status;
    }

    Ok(JobRecord {
        job,
        status,
        error: raw.error,
        created_at: raw.created_at,
        updated_at: raw.updated_at,
    })
}

fn validate_progress_columns(raw: &RawJob, progress: &TransferProgress) -> Result<()> {
    let stored = (
        unsigned_integer(raw.bytes_transferred, "jobs.bytes_transferred")?,
        unsigned_integer(raw.total_bytes, "jobs.total_bytes")?,
        unsigned_integer(raw.files_completed, "jobs.files_completed")?,
        unsigned_integer(raw.total_files, "jobs.total_files")?,
    );
    let encoded = (
        progress.transferred_bytes,
        progress.total_bytes,
        progress.completed_files,
        progress.total_files,
    );
    if stored != encoded {
        return Err(StorageError::InvalidData {
            field: "jobs.progress_cbor",
            message: "encoded progress does not match indexed progress columns".to_owned(),
        });
    }
    Ok(())
}

struct PreparedJobFile {
    job_id: String,
    path: String,
    size: i64,
    status: &'static str,
    bytes_transferred: i64,
    error: Option<String>,
    updated_at: i64,
}

impl PreparedJobFile {
    fn from_record(file: &JobFileRecord) -> Result<Self> {
        Ok(Self {
            job_id: file.job_id.to_string(),
            path: file.path.clone(),
            size: sqlite_integer(file.size, "job_files.size")?,
            status: file.status.as_str(),
            bytes_transferred: sqlite_integer(
                file.bytes_transferred,
                "job_files.bytes_transferred",
            )?,
            error: file.error.clone(),
            updated_at: file.updated_at,
        })
    }
}

fn upsert_prepared_job_file(
    connection: &Connection,
    file: &PreparedJobFile,
) -> rusqlite::Result<usize> {
    connection.execute(
        r#"
            INSERT INTO job_files (
                job_id, path, size, status, bytes_transferred, error, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ON CONFLICT(job_id, path) DO UPDATE SET
                size = excluded.size,
                status = excluded.status,
                bytes_transferred = excluded.bytes_transferred,
                error = excluded.error,
                updated_at = excluded.updated_at
        "#,
        params![
            file.job_id,
            file.path,
            file.size,
            file.status,
            file.bytes_transferred,
            file.error,
            file.updated_at,
        ],
    )
}

struct RawJobFile {
    job_id: String,
    path: String,
    size: i64,
    status: String,
    bytes_transferred: i64,
    error: Option<String>,
    updated_at: i64,
}

fn raw_job_file(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawJobFile> {
    Ok(RawJobFile {
        job_id: row.get(0)?,
        path: row.get(1)?,
        size: row.get(2)?,
        status: row.get(3)?,
        bytes_transferred: row.get(4)?,
        error: row.get(5)?,
        updated_at: row.get(6)?,
    })
}

fn job_file_from_raw(raw: RawJobFile) -> Result<JobFileRecord> {
    let status = JobFileStatus::from_str(&raw.status).ok_or_else(|| StorageError::InvalidData {
        field: "job_files.status",
        message: format!("unknown status {:?}", raw.status),
    })?;
    Ok(JobFileRecord {
        job_id: parse_uuid(&raw.job_id, "job_files.job_id")?,
        path: raw.path,
        size: unsigned_integer(raw.size, "job_files.size")?,
        status,
        bytes_transferred: unsigned_integer(raw.bytes_transferred, "job_files.bytes_transferred")?,
        error: raw.error,
        updated_at: raw.updated_at,
    })
}

struct PreparedChunk {
    job_id: String,
    path: String,
    index: i64,
    source_hash: [u8; 32],
    completed_at: i64,
}

impl PreparedChunk {
    fn from_record(chunk: &CompletedChunk) -> Result<Self> {
        Ok(Self {
            job_id: chunk.job_id.to_string(),
            path: chunk.path.clone(),
            index: sqlite_integer(chunk.index, "chunks.chunk_index")?,
            source_hash: chunk.source_hash,
            completed_at: chunk.completed_at,
        })
    }
}

fn upsert_prepared_chunk(
    connection: &Connection,
    chunk: &PreparedChunk,
) -> rusqlite::Result<usize> {
    connection.execute(
        r#"
            INSERT INTO chunks (
                job_id, path, chunk_index, source_hash, completed_at
            ) VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(job_id, path, chunk_index) DO UPDATE SET
                source_hash = excluded.source_hash,
                completed_at = excluded.completed_at
        "#,
        params![
            chunk.job_id,
            chunk.path,
            chunk.index,
            chunk.source_hash.as_slice(),
            chunk.completed_at,
        ],
    )
}

struct RawChunk {
    job_id: String,
    path: String,
    index: i64,
    source_hash: Vec<u8>,
    completed_at: i64,
}

fn raw_chunk(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawChunk> {
    Ok(RawChunk {
        job_id: row.get(0)?,
        path: row.get(1)?,
        index: row.get(2)?,
        source_hash: row.get(3)?,
        completed_at: row.get(4)?,
    })
}

fn chunk_from_raw(raw: RawChunk) -> Result<CompletedChunk> {
    Ok(CompletedChunk {
        job_id: parse_uuid(&raw.job_id, "chunks.job_id")?,
        path: raw.path,
        index: unsigned_integer(raw.index, "chunks.chunk_index")?,
        source_hash: hash_from_blob(raw.source_hash, "chunks.source_hash")?,
        completed_at: raw.completed_at,
    })
}

struct RawHashCache {
    full_hash: Vec<u8>,
    chunks_cbor: Vec<u8>,
    updated_at: i64,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::error::Error;

    use fastsync_core::{
        ChunkDescriptor, JobStatus, TransferConfig, TransferJob, TransferProgress,
    };
    use tempfile::TempDir;

    use super::*;

    fn file_database() -> Result<(TempDir, std::path::PathBuf, Database)> {
        let directory = tempfile::tempdir().map_err(|source| StorageError::CreateDirectory {
            path: std::env::temp_dir(),
            source,
        })?;
        let path = directory.path().join("nested").join("fastsync.sqlite3");
        let database = Database::open(&path)?;
        Ok((directory, path, database))
    }

    fn test_job(id: Uuid, status: JobStatus) -> JobRecord {
        let mut job = TransferJob::new("source", "destination", TransferConfig::default());
        job.id = id;
        job.status = status;
        job.progress = TransferProgress {
            transferred_bytes: 20,
            total_bytes: 100,
            completed_files: 0,
            total_files: 1,
            ..TransferProgress::default()
        };
        JobRecord {
            job,
            status: status.into(),
            error: None,
            created_at: 100,
            updated_at: 101,
        }
    }

    fn transfer_job(id: Uuid, status: JobStatus) -> TransferJob {
        let mut job = TransferJob::new("source", "destination", TransferConfig::default());
        job.id = id;
        job.status = status;
        job.progress = TransferProgress {
            transferred_bytes: 20,
            total_bytes: 100,
            completed_files: 0,
            total_files: 1,
            ..TransferProgress::default()
        };
        job
    }

    #[test]
    fn settings_preserve_value_types() -> std::result::Result<(), Box<dyn Error>> {
        let database = Database::open_in_memory()?;
        database.set_string("name", "FastSync")?;
        database.set_bytes("identity", &[0, 1, 255])?;
        database.set_bytes_batch(&[("certificate", &[3, 4]), ("private-key", &[5, 6])])?;
        assert_eq!(database.get_string("name")?.as_deref(), Some("FastSync"));
        assert_eq!(database.get_bytes("identity")?, Some(vec![0, 1, 255]));
        assert_eq!(database.get_bytes("certificate")?, Some(vec![3, 4]));
        assert_eq!(database.get_bytes("private-key")?, Some(vec![5, 6]));
        assert_eq!(database.get_or_insert_string("token", "first")?, "first");
        assert_eq!(database.get_or_insert_string("token", "second")?, "first");
        assert!(matches!(
            database.get_bytes("name"),
            Err(StorageError::SettingType { .. })
        ));
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn on_disk_database_and_directory_are_private() -> std::result::Result<(), Box<dyn Error>> {
        use std::os::unix::fs::PermissionsExt;

        let temporary = tempfile::tempdir()?;
        let data_directory = temporary.path().join("private-data");
        let database_path = data_directory.join("fastsync.sqlite3");
        let _database = Database::open(&database_path)?;

        assert_eq!(
            fs::metadata(&data_directory)?.permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(&database_path)?.permissions().mode() & 0o777,
            0o600
        );
        Ok(())
    }

    #[test]
    fn discovered_devices_upsert_by_id() -> std::result::Result<(), Box<dyn Error>> {
        let database = Database::open_in_memory()?;
        let device_id = Uuid::new_v4();
        database.upsert_discovered_device(&DiscoveredDevice {
            device_id,
            name: "first".to_owned(),
            public_key: Some(vec![1, 2]),
            address: "192.0.2.1:1".to_owned(),
            first_seen: 10,
            last_seen: 20,
        })?;
        database.upsert_discovered_device(&DiscoveredDevice {
            device_id,
            name: "updated".to_owned(),
            public_key: None,
            address: "192.0.2.1:2".to_owned(),
            first_seen: 15,
            last_seen: 30,
        })?;
        let devices = database.list_discovered_devices()?;
        assert_eq!(devices.len(), 1);
        assert_eq!(devices[0].name, "first");
        assert_eq!(devices[0].public_key, Some(vec![1, 2]));
        assert_eq!(devices[0].address, "192.0.2.1:1");
        assert_eq!(devices[0].first_seen, 10);
        assert_eq!(devices[0].last_seen, 30);
        Ok(())
    }

    #[test]
    fn migration_creates_tables_and_configures_connection()
    -> std::result::Result<(), Box<dyn Error>> {
        let (_directory, path, database) = file_database()?;
        let connection = database.inner.connection.lock();
        let tables = {
            let mut statement = connection
                .prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")?;
            let rows = statement
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<std::result::Result<BTreeSet<_>, _>>()?;
            rows
        };
        for expected in [
            "settings",
            "devices",
            "trusted_peers",
            "jobs",
            "job_files",
            "chunks",
            "hash_cache",
        ] {
            assert!(tables.contains(expected), "missing table {expected}");
        }
        let foreign_keys: i64 =
            connection.query_row("PRAGMA foreign_keys", [], |row| row.get(0))?;
        let synchronous: i64 = connection.query_row("PRAGMA synchronous", [], |row| row.get(0))?;
        let journal_mode: String =
            connection.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
        let version: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        let busy_timeout: i64 =
            connection.query_row("PRAGMA busy_timeout", [], |row| row.get(0))?;
        assert_eq!(foreign_keys, 1);
        assert_eq!(synchronous, 1);
        assert_eq!(journal_mode, "wal");
        assert_eq!(version, 2);
        assert_eq!(busy_timeout, 5_000);
        drop(connection);
        assert!(path.exists());
        Ok(())
    }

    #[test]
    fn hash_cache_requires_exact_file_metadata() -> std::result::Result<(), Box<dyn Error>> {
        let database = Database::open_in_memory()?;
        let original = HashCacheEntry {
            key: HashCacheKey {
                path: "data.bin".to_owned(),
                size: 128,
                mtime_ns: 42,
                chunk_size: 64,
            },
            full_hash: [7; 32],
            chunks: vec![ChunkDescriptor {
                index: 0,
                offset: 0,
                size: 64,
                hash: [8; 32],
            }],
            updated_at: 50,
        };
        database.put_hash_cache(&original)?;
        assert_eq!(
            database.get_hash_cache(&original.key)?,
            Some(original.clone())
        );

        let stale_key = HashCacheKey {
            size: 129,
            ..original.key.clone()
        };
        assert_eq!(database.get_hash_cache(&stale_key)?, None);

        let replacement = HashCacheEntry {
            key: stale_key.clone(),
            full_hash: [9; 32],
            chunks: Vec::new(),
            updated_at: 51,
        };
        database.put_hash_cache(&replacement)?;
        assert_eq!(database.get_hash_cache(&original.key)?, None);
        assert_eq!(database.get_hash_cache(&stale_key)?, Some(replacement));
        Ok(())
    }

    #[test]
    fn trusted_peer_roundtrip_and_remove() -> std::result::Result<(), Box<dyn Error>> {
        let database = Database::open_in_memory()?;
        let peer = TrustedPeer {
            device_id: Uuid::new_v4(),
            name: "workstation".to_owned(),
            public_key: vec![1, 2, 3, 4],
            address: "192.0.2.4:43721".to_owned(),
            trusted_at: 100,
            last_seen: 200,
        };
        database.upsert_trusted_peer(&peer)?;
        assert_eq!(
            database.get_trusted_peer(peer.device_id)?,
            Some(peer.clone())
        );
        assert_eq!(database.list_trusted_peers()?, vec![peer.clone()]);
        assert!(database.remove_trusted_peer(peer.device_id)?);
        assert_eq!(database.get_trusted_peer(peer.device_id)?, None);
        Ok(())
    }

    #[test]
    fn updates_core_job_status_and_progress() -> std::result::Result<(), Box<dyn Error>> {
        let database = Database::open_in_memory()?;
        let job_id = Uuid::new_v4();
        database.create_job(&transfer_job(job_id, JobStatus::Pending))?;

        let progress = TransferProgress {
            total_files: 4,
            completed_files: 2,
            total_bytes: 1_000,
            transferred_bytes: 600,
            skipped_files: 1,
            ..TransferProgress::default()
        };
        assert!(database.update_job_status_and_progress(
            job_id,
            JobStatus::Verifying,
            &progress
        )?);
        let stored = database.get_job(job_id)?.ok_or("job was not persisted")?;
        assert_eq!(stored.status, StoredJobStatus::Verifying);
        assert_eq!(stored.job.status, JobStatus::Verifying);
        assert_eq!(stored.job.progress, progress);
        Ok(())
    }

    #[test]
    fn create_job_if_absent_never_overwrites_existing_owner()
    -> std::result::Result<(), Box<dyn Error>> {
        let database = Database::open_in_memory()?;
        let job_id = Uuid::new_v4();
        let original = transfer_job(job_id, JobStatus::Transferring);
        assert!(database.create_job_if_absent(&original)?);

        let mut conflicting = original.clone();
        conflicting.source_root = "other-source".into();
        conflicting.destination_root = "other-destination".into();
        assert!(!database.create_job_if_absent(&conflicting)?);

        let stored = database.get_job(job_id)?.ok_or("job was not persisted")?;
        assert_eq!(stored.job.source_root, original.source_root);
        assert_eq!(stored.job.destination_root, original.destination_root);
        Ok(())
    }

    #[test]
    fn persists_jobs_files_and_resume_chunks() -> std::result::Result<(), Box<dyn Error>> {
        let (_directory, path, database) = file_database()?;
        let job_id = Uuid::new_v4();
        let job = test_job(job_id, JobStatus::Pending);
        database.create_job_record(&job)?;
        database.upsert_job_file(&JobFileRecord {
            job_id,
            path: "folder/file.bin".to_owned(),
            size: 100,
            status: JobFileStatus::Running,
            bytes_transferred: 50,
            error: None,
            updated_at: 102,
        })?;
        assert!(database.update_job_file_status(
            job_id,
            "folder/file.bin",
            JobFileStatus::Failed,
            Some("connection lost")
        )?);
        let source_hash = [3; 32];
        database.record_completed_chunks(&[
            CompletedChunk {
                job_id,
                path: "folder/file.bin".to_owned(),
                index: 0,
                source_hash,
                completed_at: 103,
            },
            CompletedChunk {
                job_id,
                path: "folder/file.bin".to_owned(),
                index: 1,
                source_hash,
                completed_at: 104,
            },
        ])?;
        drop(database);

        let reopened = Database::open(path)?;
        assert_eq!(reopened.get_job(job_id)?, Some(job));
        let file = reopened
            .get_job_file(job_id, "folder/file.bin")?
            .ok_or("job file was not persisted")?;
        assert_eq!(file.status, JobFileStatus::Failed);
        assert_eq!(file.error.as_deref(), Some("connection lost"));
        assert_eq!(
            reopened.completed_chunk_indices(job_id, "folder/file.bin", &source_hash)?,
            vec![0, 1]
        );
        assert!(!reopened.is_chunk_completed(job_id, "folder/file.bin", 0, &[4; 32])?);
        assert!(reopened.remove_completed_chunk(job_id, "folder/file.bin", 0, &source_hash)?);
        assert_eq!(
            reopened.completed_chunk_indices(job_id, "folder/file.bin", &source_hash)?,
            vec![1]
        );
        Ok(())
    }

    #[test]
    fn startup_marks_running_jobs_interrupted() -> std::result::Result<(), Box<dyn Error>> {
        let (_directory, path, database) = file_database()?;
        let running_id = Uuid::new_v4();
        let pending_id = Uuid::new_v4();
        database.create_job(&transfer_job(running_id, JobStatus::Transferring))?;
        database.create_job(&transfer_job(pending_id, JobStatus::Pending))?;
        drop(database);

        let reopened = Database::open(path)?;
        let running = reopened
            .get_job(running_id)?
            .ok_or("running job was not persisted")?;
        let pending = reopened
            .get_job(pending_id)?
            .ok_or("pending job was not persisted")?;
        assert_eq!(running.status, StoredJobStatus::Interrupted);
        assert_eq!(running.job.status, JobStatus::Transferring);
        assert_eq!(pending.status, StoredJobStatus::Pending);
        Ok(())
    }
}
