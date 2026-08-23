use std::io;
use std::path::{Path, PathBuf};

use fastsync_protocol::{ProtocolError, RemoteError, RemoteErrorCode};
use thiserror::Error;
use uuid::Uuid;

pub type Result<T> = std::result::Result<T, TransferError>;

#[derive(Debug, Error)]
pub enum TransferError {
    #[error(transparent)]
    Storage(#[from] fastsync_storage::StorageError),

    #[error(transparent)]
    Core(#[from] fastsync_core::CoreError),

    #[error(transparent)]
    Protocol(#[from] ProtocolError),

    #[error("I/O error while {operation} `{path}`: {source}")]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    #[error("TLS configuration error: {0}")]
    Tls(String),

    #[error("invalid persistent device identity: {0}")]
    Identity(String),

    #[error("authentication failed: {0}")]
    Authentication(String),

    #[error("peer rejected the handshake: {0}")]
    HandshakeRejected(String),

    #[error("protocol version mismatch: expected {expected}, received {received}")]
    ProtocolVersionMismatch { expected: u16, received: u16 },

    #[error("peer {device_id} is not trusted for transfers")]
    UntrustedPeer { device_id: Uuid },

    #[error("remote {code:?} error: {message}")]
    Remote {
        code: RemoteErrorCode,
        message: String,
        retryable: bool,
        job_id: Option<Uuid>,
        path: Option<String>,
    },

    #[error("network error: {0}")]
    Network(String),

    #[error("invalid transfer data: {0}")]
    InvalidData(String),

    #[error("transfer was cancelled")]
    Cancelled,

    #[error("background task failed: {0}")]
    Task(String),
}

impl TransferError {
    pub(crate) fn io(operation: &'static str, path: impl AsRef<Path>, source: io::Error) -> Self {
        Self::Io {
            operation,
            path: path.as_ref().to_path_buf(),
            source,
        }
    }

    /// Whether retrying after reconnecting and renegotiating can succeed.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Network(_) => true,
            Self::Remote { retryable, .. } => *retryable,
            Self::Protocol(
                ProtocolError::Io(_)
                | ProtocolError::TruncatedLengthPrefix { .. }
                | ProtocolError::TruncatedFrame { .. },
            ) => true,
            _ => false,
        }
    }

    pub(crate) fn remote_path(&self) -> Option<&str> {
        match self {
            Self::Remote { path, .. } => path.as_deref(),
            _ => None,
        }
    }
}

impl From<RemoteError> for TransferError {
    fn from(error: RemoteError) -> Self {
        Self::Remote {
            code: error.code,
            message: error.message,
            retryable: error.retryable,
            job_id: error.job_id,
            path: error.path,
        }
    }
}
