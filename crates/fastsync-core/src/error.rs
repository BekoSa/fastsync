use std::io;
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::model::{ContentHash, SourceMetadata};
use crate::path::RelativePathError;

pub type Result<T> = std::result::Result<T, CoreError>;

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("I/O error while {operation} `{path}`: {source}")]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("`{path}` is not a directory")]
    NotDirectory { path: PathBuf },
    #[error("`{path}` is not a regular file")]
    NotRegularFile { path: PathBuf },
    #[error("unsupported filesystem entry type at `{path}`")]
    UnsupportedFileType { path: PathBuf },
    #[error("invalid relative path for `{path}`: {source}")]
    InvalidRelativePath {
        path: PathBuf,
        #[source]
        source: RelativePathError,
    },
    #[error("modification time for `{path}` cannot be represented in nanoseconds")]
    MetadataTimeOutOfRange { path: PathBuf },
    #[error("chunk size must be non-zero and fit in memory, got {chunk_size}")]
    InvalidChunkSize { chunk_size: u64 },
    #[error("unable to allocate a {chunk_size}-byte chunk buffer")]
    ChunkBufferAllocation { chunk_size: u64 },
    #[error("file size or chunk index exceeds supported limits for `{path}`")]
    FileTooLarge { path: PathBuf },
    #[error("source metadata changed while reading `{path}`")]
    SourceChanged {
        path: PathBuf,
        expected: SourceMetadata,
        actual: SourceMetadata,
    },
    #[error("read length changed while reading `{path}`: expected {expected}, got {actual}")]
    ReadSizeChanged {
        path: PathBuf,
        expected: u64,
        actual: u64,
    },
    #[error("content hash does not match for `{path}`")]
    HashMismatch {
        path: PathBuf,
        expected: ContentHash,
        actual: ContentHash,
    },
    #[error("file size does not match for `{path}`: expected {expected}, got {actual}")]
    SizeMismatch {
        path: PathBuf,
        expected: u64,
        actual: u64,
    },
    #[error("operation was cancelled while processing `{path}`")]
    OperationCancelled { path: PathBuf },
}

impl CoreError {
    pub(crate) fn io(operation: &'static str, path: impl AsRef<Path>, source: io::Error) -> Self {
        Self::Io {
            operation,
            path: path.as_ref().to_path_buf(),
            source,
        }
    }
}
