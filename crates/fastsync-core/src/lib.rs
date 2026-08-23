//! Shared domain models and filesystem primitives for FastSync.

mod error;
mod hashing;
mod metadata;
mod model;
mod partial;
mod path;
mod scan;

pub use error::{CoreError, Result};
pub use hashing::hash_file;
pub use metadata::{
    apply_manifest_metadata, capture_source_metadata, ensure_source_unchanged,
    manifest_entry_from_path, metadata_matches, path_matches_manifest, source_metadata_matches,
};
pub use model::{
    ChunkDescriptor, ContentHash, DEFAULT_CHUNK_SIZE, FileError, FileOperation, FileProgress,
    FileType, HashedFile, JobStatus, ManifestEntry, SourceMetadata, TransferConfig, TransferJob,
    TransferProgress, VerificationMode,
};
pub use partial::{
    finalize_hashed_file, finalize_hashed_file_from, finalize_hashed_file_from_cancellable,
    finalize_partial_file, initialize_partial_file, initialize_partial_file_at,
    verify_partial_file,
};
pub use path::{
    PARTIAL_FILE_SUFFIX, PathValidationError, RelativePathError, is_fastsync_staging_component,
    is_receiver_absolute_path, partial_file_path, safe_join, to_wire_relative_path,
    validate_wire_relative_path,
};
pub use scan::{ScanResult, scan_directory};
