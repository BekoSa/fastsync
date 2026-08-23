use std::fs::{self, Metadata};
use std::path::Path;
use std::time::UNIX_EPOCH;

use filetime::FileTime;

use crate::error::{CoreError, Result};
use crate::model::{FileType, ManifestEntry, SourceMetadata};
use crate::path::to_wire_relative_path;

pub fn metadata_matches(expected: &ManifestEntry, actual: &ManifestEntry) -> bool {
    expected.file_type == actual.file_type
        && expected.size == actual.size
        && expected.mtime_ns == actual.mtime_ns
        && expected.read_only == actual.read_only
}

pub fn source_metadata_matches(expected: &SourceMetadata, actual: &SourceMetadata) -> bool {
    expected == actual
}

pub fn capture_source_metadata(path: impl AsRef<Path>) -> Result<SourceMetadata> {
    let path = path.as_ref();
    let metadata = fs::symlink_metadata(path)
        .map_err(|source| CoreError::io("reading metadata for", path, source))?;
    source_metadata_from_metadata(path, &metadata)
}

pub fn ensure_source_unchanged(path: impl AsRef<Path>, expected: &SourceMetadata) -> Result<()> {
    let path = path.as_ref();
    let actual = capture_source_metadata(path)?;
    if source_metadata_matches(expected, &actual) {
        return Ok(());
    }

    Err(CoreError::SourceChanged {
        path: path.to_path_buf(),
        expected: *expected,
        actual,
    })
}

pub fn path_matches_manifest(path: impl AsRef<Path>, expected: &ManifestEntry) -> Result<bool> {
    let actual = capture_source_metadata(path)?;
    Ok(source_metadata_matches(
        &expected.source_metadata(),
        &actual,
    ))
}

pub fn manifest_entry_from_path(
    root: impl AsRef<Path>,
    path: impl AsRef<Path>,
) -> Result<ManifestEntry> {
    let root = root.as_ref();
    let path = path.as_ref();
    let relative_path =
        to_wire_relative_path(root, path).map_err(|source| CoreError::InvalidRelativePath {
            path: path.to_path_buf(),
            source,
        })?;
    let metadata = fs::symlink_metadata(path)
        .map_err(|source| CoreError::io("reading metadata for", path, source))?;
    manifest_entry_from_metadata(path, relative_path, &metadata)
}

pub fn apply_manifest_metadata(path: impl AsRef<Path>, entry: &ManifestEntry) -> Result<()> {
    let path = path.as_ref();
    let current = capture_source_metadata(path)?;
    if current.file_type != entry.file_type {
        return Err(CoreError::UnsupportedFileType {
            path: path.to_path_buf(),
        });
    }

    let seconds = entry.mtime_ns.div_euclid(1_000_000_000);
    let nanoseconds = entry.mtime_ns.rem_euclid(1_000_000_000) as u32;
    let mtime = FileTime::from_unix_time(seconds, nanoseconds);
    filetime::set_file_mtime(path, mtime)
        .map_err(|source| CoreError::io("setting modification time on", path, source))?;

    let metadata = fs::symlink_metadata(path)
        .map_err(|source| CoreError::io("reading permissions for", path, source))?;
    let mut permissions = metadata.permissions();
    set_read_only(&mut permissions, entry.read_only);
    fs::set_permissions(path, permissions)
        .map_err(|source| CoreError::io("setting permissions on", path, source))?;

    Ok(())
}

pub(crate) fn manifest_entry_from_metadata(
    path: &Path,
    relative_path: String,
    metadata: &Metadata,
) -> Result<ManifestEntry> {
    let source = source_metadata_from_metadata(path, metadata)?;
    Ok(ManifestEntry {
        relative_path,
        size: source.size,
        mtime_ns: source.mtime_ns,
        file_type: source.file_type,
        read_only: source.read_only,
    })
}

pub(crate) fn source_metadata_from_metadata(
    path: &Path,
    metadata: &Metadata,
) -> Result<SourceMetadata> {
    let file_type = metadata.file_type();
    let file_type = if file_type.is_file() {
        FileType::File
    } else if file_type.is_dir() {
        FileType::Directory
    } else {
        return Err(CoreError::UnsupportedFileType {
            path: path.to_path_buf(),
        });
    };

    let modified = metadata
        .modified()
        .map_err(|source| CoreError::io("reading modification time for", path, source))?;
    let mtime_ns = match modified.duration_since(UNIX_EPOCH) {
        Ok(duration) => {
            i128::from(duration.as_secs()) * 1_000_000_000 + i128::from(duration.subsec_nanos())
        }
        Err(error) => {
            let duration = error.duration();
            -(i128::from(duration.as_secs()) * 1_000_000_000 + i128::from(duration.subsec_nanos()))
        }
    };
    let mtime_ns = i64::try_from(mtime_ns).map_err(|_| CoreError::MetadataTimeOutOfRange {
        path: path.to_path_buf(),
    })?;

    Ok(SourceMetadata {
        size: if file_type == FileType::File {
            metadata.len()
        } else {
            0
        },
        mtime_ns,
        file_type,
        read_only: metadata.permissions().readonly(),
    })
}

#[cfg(unix)]
fn set_read_only(permissions: &mut fs::Permissions, read_only: bool) {
    use std::os::unix::fs::PermissionsExt;

    let mode = permissions.mode();
    if read_only {
        permissions.set_mode(mode & !0o222);
    } else {
        permissions.set_mode(mode | 0o200);
    }
}

#[cfg(not(unix))]
fn set_read_only(permissions: &mut fs::Permissions, read_only: bool) {
    permissions.set_readonly(read_only);
}
