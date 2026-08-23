use std::path::PathBuf;

use thiserror::Error;

pub type Result<T> = std::result::Result<T, StorageError>;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("SQLite storage error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("failed to create database directory {path}: {source}")]
    CreateDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to secure database path {path}: {source}")]
    SecurePath {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to encode {kind} as CBOR: {message}")]
    Encode { kind: &'static str, message: String },

    #[error("failed to decode {kind} from CBOR: {message}")]
    Decode { kind: &'static str, message: String },

    #[error("setting {key:?} contains {actual} data, not {expected} data")]
    SettingType {
        key: String,
        expected: &'static str,
        actual: String,
    },

    #[error("setting {key:?} is not valid UTF-8: {source}")]
    InvalidSettingString {
        key: String,
        #[source]
        source: std::string::FromUtf8Error,
    },

    #[error("invalid persisted data in {field}: {message}")]
    InvalidData {
        field: &'static str,
        message: String,
    },

    #[error("{field} value {value} is too large for SQLite")]
    IntegerOutOfRange { field: &'static str, value: u64 },

    #[error("system clock is before the Unix epoch: {0}")]
    SystemTime(#[from] std::time::SystemTimeError),

    #[error("database schema version {found} is newer than supported version {supported}")]
    UnsupportedSchemaVersion { found: i64, supported: i64 },
}
