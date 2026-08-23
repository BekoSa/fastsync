use std::path::PathBuf;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

use crate::path::{PathValidationError, validate_wire_relative_path};

pub const DEFAULT_CHUNK_SIZE: u64 = 4 * 1024 * 1024;

pub type ContentHash = [u8; 32];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum VerificationMode {
    Fast,
    #[default]
    Verified,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    #[default]
    Pending,
    Scanning,
    Transferring,
    Verifying,
    Paused,
    Completed,
    CompletedWithErrors,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferConfig {
    pub verification_mode: VerificationMode,
    pub chunk_size: u64,
    pub concurrency: u32,
    pub retry_limit: u32,
}

impl Default for TransferConfig {
    fn default() -> Self {
        Self {
            verification_mode: VerificationMode::Verified,
            chunk_size: DEFAULT_CHUNK_SIZE,
            concurrency: 32,
            retry_limit: 6,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferJob {
    pub id: Uuid,
    pub peer_id: Option<Uuid>,
    pub source_root: PathBuf,
    pub destination_root: PathBuf,
    pub config: TransferConfig,
    pub status: JobStatus,
    pub progress: TransferProgress,
    pub errors: Vec<FileError>,
}

impl TransferJob {
    pub fn new(
        source_root: impl Into<PathBuf>,
        destination_root: impl Into<PathBuf>,
        config: TransferConfig,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            peer_id: None,
            source_root: source_root.into(),
            destination_root: destination_root.into(),
            config,
            status: JobStatus::Pending,
            progress: TransferProgress::default(),
            errors: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TransferProgress {
    pub total_files: u64,
    pub completed_files: u64,
    pub total_bytes: u64,
    pub transferred_bytes: u64,
    pub reused_bytes: u64,
    pub skipped_files: u64,
    pub transferred_files: u64,
    pub failed_files: u64,
    pub symlinks_skipped: u64,
    pub network_bytes_per_second: u64,
    pub read_bytes_per_second: u64,
    pub write_bytes_per_second: u64,
    pub hash_bytes_per_second: u64,
    pub active_file_streams: u32,
    pub active_chunk_streams: u32,
    pub queued_chunks: u64,
    pub current_concurrency: u32,
    pub current_file: Option<FileProgress>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileProgress {
    #[serde(
        serialize_with = "serialize_wire_path",
        deserialize_with = "deserialize_wire_path"
    )]
    pub relative_path: String,
    pub size: u64,
    pub transferred: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileOperation {
    Scan,
    Metadata,
    Read,
    Write,
    Verify,
    Finalize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileError {
    pub relative_path: Option<String>,
    pub operation: FileOperation,
    pub message: String,
}

impl FileError {
    pub fn new(
        relative_path: Option<String>,
        operation: FileOperation,
        message: impl Into<String>,
    ) -> Self {
        Self {
            relative_path,
            operation,
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileType {
    File,
    Directory,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestEntry {
    #[serde(
        serialize_with = "serialize_wire_path",
        deserialize_with = "deserialize_wire_path"
    )]
    pub relative_path: String,
    pub size: u64,
    pub mtime_ns: i64,
    pub file_type: FileType,
    pub read_only: bool,
}

impl ManifestEntry {
    pub fn new(
        relative_path: impl Into<String>,
        size: u64,
        mtime_ns: i64,
        file_type: FileType,
        read_only: bool,
    ) -> std::result::Result<Self, PathValidationError> {
        let relative_path = relative_path.into();
        validate_wire_relative_path(&relative_path)?;

        Ok(Self {
            relative_path,
            size,
            mtime_ns,
            file_type,
            read_only,
        })
    }

    pub fn source_metadata(&self) -> SourceMetadata {
        SourceMetadata {
            size: self.size,
            mtime_ns: self.mtime_ns,
            file_type: self.file_type,
            read_only: self.read_only,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceMetadata {
    pub size: u64,
    pub mtime_ns: i64,
    pub file_type: FileType,
    pub read_only: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkDescriptor {
    pub index: u64,
    pub offset: u64,
    pub size: u64,
    pub hash: ContentHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HashedFile {
    pub entry: ManifestEntry,
    pub hash: ContentHash,
    pub chunks: Vec<ChunkDescriptor>,
}

fn serialize_wire_path<S>(path: &String, serializer: S) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    validate_wire_relative_path(path).map_err(serde::ser::Error::custom)?;
    serializer.serialize_str(path)
}

fn deserialize_wire_path<'de, D>(deserializer: D) -> std::result::Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let path = String::deserialize(deserializer)?;
    validate_wire_relative_path(&path).map_err(serde::de::Error::custom)?;
    Ok(path)
}
