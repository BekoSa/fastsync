use fastsync_core::{ChunkDescriptor, ContentHash, JobStatus as CoreJobStatus, TransferJob};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveredDevice {
    pub device_id: Uuid,
    pub name: String,
    pub public_key: Option<Vec<u8>>,
    pub address: String,
    pub first_seen: i64,
    pub last_seen: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrustedPeer {
    pub device_id: Uuid,
    pub name: String,
    pub public_key: Vec<u8>,
    pub address: String,
    pub trusted_at: i64,
    pub last_seen: i64,
}

/// Persisted job state, extending the core states with crash interruption.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoredJobStatus {
    Pending,
    Scanning,
    Transferring,
    Verifying,
    Paused,
    Completed,
    CompletedWithErrors,
    Failed,
    Cancelled,
    Interrupted,
}

impl StoredJobStatus {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Scanning => "scanning",
            Self::Transferring => "transferring",
            Self::Verifying => "verifying",
            Self::Paused => "paused",
            Self::Completed => "completed",
            Self::CompletedWithErrors => "completed_with_errors",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Interrupted => "interrupted",
        }
    }

    pub(crate) fn from_str(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "scanning" => Some(Self::Scanning),
            "transferring" => Some(Self::Transferring),
            "verifying" => Some(Self::Verifying),
            "paused" => Some(Self::Paused),
            "completed" => Some(Self::Completed),
            "completed_with_errors" => Some(Self::CompletedWithErrors),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            "interrupted" => Some(Self::Interrupted),
            _ => None,
        }
    }

    pub fn core_status(self) -> Option<CoreJobStatus> {
        match self {
            Self::Pending => Some(CoreJobStatus::Pending),
            Self::Scanning => Some(CoreJobStatus::Scanning),
            Self::Transferring => Some(CoreJobStatus::Transferring),
            Self::Verifying => Some(CoreJobStatus::Verifying),
            Self::Paused => Some(CoreJobStatus::Paused),
            Self::Completed => Some(CoreJobStatus::Completed),
            Self::CompletedWithErrors => Some(CoreJobStatus::CompletedWithErrors),
            Self::Failed => Some(CoreJobStatus::Failed),
            Self::Cancelled => Some(CoreJobStatus::Cancelled),
            Self::Interrupted => None,
        }
    }
}

impl From<CoreJobStatus> for StoredJobStatus {
    fn from(value: CoreJobStatus) -> Self {
        match value {
            CoreJobStatus::Pending => Self::Pending,
            CoreJobStatus::Scanning => Self::Scanning,
            CoreJobStatus::Transferring => Self::Transferring,
            CoreJobStatus::Verifying => Self::Verifying,
            CoreJobStatus::Paused => Self::Paused,
            CoreJobStatus::Completed => Self::Completed,
            CoreJobStatus::CompletedWithErrors => Self::CompletedWithErrors,
            CoreJobStatus::Failed => Self::Failed,
            CoreJobStatus::Cancelled => Self::Cancelled,
        }
    }
}

/// A core transfer job plus persistence-only state and Unix-millisecond timestamps.
///
/// For an interrupted record, `job.status` retains its prior active core state
/// so the transfer layer can determine which phase should be resumed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobRecord {
    pub job: TransferJob,
    pub status: StoredJobStatus,
    pub error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobFileStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Skipped,
    Interrupted,
}

impl JobFileStatus {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
            Self::Interrupted => "interrupted",
        }
    }

    pub(crate) fn from_str(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "running" => Some(Self::Running),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "skipped" => Some(Self::Skipped),
            "interrupted" => Some(Self::Interrupted),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobFileRecord {
    pub job_id: Uuid,
    pub path: String,
    pub size: u64,
    pub status: JobFileStatus,
    pub bytes_transferred: u64,
    pub error: Option<String>,
    pub updated_at: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletedChunk {
    pub job_id: Uuid,
    pub path: String,
    pub index: u64,
    pub source_hash: ContentHash,
    pub completed_at: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HashCacheKey {
    pub path: String,
    pub size: u64,
    pub mtime_ns: i64,
    pub chunk_size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HashCacheEntry {
    pub key: HashCacheKey,
    pub full_hash: ContentHash,
    pub chunks: Vec<ChunkDescriptor>,
    pub updated_at: i64,
}
