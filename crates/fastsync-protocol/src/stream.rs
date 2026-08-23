use fastsync_core::{ContentHash, HashedFile, ManifestEntry, VerificationMode};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A client request carried in a [`crate::RequestFrame`].
///
/// For `UploadChunk`, exactly `ChunkHeader::size` raw bytes follow the CBOR
/// frame. Those bytes are intentionally not represented by this enum.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum Request {
    Compare(CompareBatch),
    Negotiate(HashedFileBatch),
    UploadChunk(ChunkHeader),
    FinalizeFile(FinalizeFileRequest),
    CompleteJob(CompleteJobRequest),
    Ping(PingRequest),
}

/// A server response carried in a [`crate::ResponseFrame`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum Response {
    Compare(CompareDecisionBatch),
    Negotiate(FilePlanBatch),
    Acknowledged(Acknowledgement),
    Pong(PongResponse),
    Error(RemoteError),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompareBatch {
    pub job_id: Uuid,
    pub manifest_id: Uuid,
    /// Absolute path interpreted only by the receiving agent.
    pub destination_root: String,
    pub verification_mode: VerificationMode,
    pub chunk_size: u64,
    pub sequence: u32,
    pub is_last: bool,
    pub entries: Vec<ManifestEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompareAction {
    Unchanged,
    NeedHash,
    Transfer,
    Delete,
    Conflict,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompareDecision {
    pub path: String,
    pub action: CompareAction,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompareDecisionBatch {
    pub job_id: Uuid,
    pub manifest_id: Uuid,
    pub sequence: u32,
    pub is_last: bool,
    pub decisions: Vec<CompareDecision>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HashedFileBatch {
    pub job_id: Uuid,
    pub manifest_id: Uuid,
    pub sequence: u32,
    pub is_last: bool,
    pub files: Vec<HashedFile>,
}

/// The receiver's chunk-level transfer plan for one file.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilePlan {
    pub path: String,
    pub missing_chunks: Vec<u64>,
    pub missing_bytes: u64,
    pub resumed_chunks: Vec<u64>,
    pub resumed_bytes: u64,
    pub reused_chunks: Vec<u64>,
    pub reused_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilePlanBatch {
    pub job_id: Uuid,
    pub manifest_id: Uuid,
    pub sequence: u32,
    pub is_last: bool,
    pub plans: Vec<FilePlan>,
}

/// Metadata for a raw chunk payload that immediately follows this CBOR value.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkHeader {
    pub job_id: Uuid,
    pub manifest_id: Uuid,
    pub path: String,
    pub index: u64,
    pub offset: u64,
    pub size: u64,
    pub hash: ContentHash,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinalizeFileRequest {
    pub job_id: Uuid,
    pub manifest_id: Uuid,
    pub file: HashedFile,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompleteJobRequest {
    pub job_id: Uuid,
    pub manifest_id: Uuid,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PingRequest {
    pub nonce: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PongResponse {
    pub nonce: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkAcknowledgement {
    pub job_id: Uuid,
    pub manifest_id: Uuid,
    pub path: String,
    pub index: u64,
    pub size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileAcknowledgement {
    pub job_id: Uuid,
    pub manifest_id: Uuid,
    pub path: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobAcknowledgement {
    pub job_id: Uuid,
    pub manifest_id: Uuid,
    pub completed_with_errors: bool,
    pub failed_files: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum Acknowledgement {
    Chunk(ChunkAcknowledgement),
    File(FileAcknowledgement),
    Job(JobAcknowledgement),
}

/// Stable categories for errors returned by a remote peer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteErrorCode {
    InvalidRequest,
    UnsupportedProtocol,
    Unauthorized,
    JobNotFound,
    PathNotFound,
    InvalidChunk,
    HashMismatch,
    Conflict,
    Storage,
    Internal,
}

/// A machine-readable remote failure with optional request context.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteError {
    pub code: RemoteErrorCode,
    pub message: String,
    pub retryable: bool,
    pub job_id: Option<Uuid>,
    pub path: Option<String>,
}
