use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

use crate::error::{CoreError, Result};
use crate::metadata::manifest_entry_from_metadata;
use crate::model::{FileError, FileOperation, ManifestEntry};
use crate::path::to_wire_relative_path;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ScanResult {
    pub entries: Vec<ManifestEntry>,
    pub symlinks_skipped: u64,
    pub unsupported_entries_skipped: u64,
    pub errors: Vec<FileError>,
}

/// Recursively scans `root` without following symlinks.
///
/// Root failures are returned. Recoverable per-entry failures are retained in
/// [`ScanResult::errors`] so callers cannot mistake an incomplete manifest for a complete scan.
pub fn scan_directory(root: impl AsRef<Path>) -> Result<ScanResult> {
    let root = root.as_ref();
    let root_metadata = fs::symlink_metadata(root)
        .map_err(|source| CoreError::io("reading scan root metadata for", root, source))?;

    if root_metadata.file_type().is_symlink() {
        return Err(CoreError::NotDirectory {
            path: root.to_path_buf(),
        });
    }
    if !root_metadata.is_dir() {
        return Err(CoreError::NotDirectory {
            path: root.to_path_buf(),
        });
    }

    let mut result = ScanResult::default();
    for item in WalkDir::new(root).follow_links(false) {
        let entry = match item {
            Ok(entry) => entry,
            Err(error) => {
                let relative_path = error
                    .path()
                    .and_then(|path| to_wire_relative_path(root, path).ok());
                result.errors.push(FileError::new(
                    relative_path,
                    FileOperation::Scan,
                    error.to_string(),
                ));
                continue;
            }
        };

        if entry.file_type().is_symlink() {
            result.symlinks_skipped += 1;
            continue;
        }
        if entry.depth() == 0 {
            continue;
        }

        let relative_path = match to_wire_relative_path(root, entry.path()) {
            Ok(relative_path) => relative_path,
            Err(error) => {
                result.errors.push(FileError::new(
                    None,
                    FileOperation::Scan,
                    format!("{}: {error}", entry.path().display()),
                ));
                continue;
            }
        };

        let metadata = match fs::symlink_metadata(entry.path()) {
            Ok(metadata) => metadata,
            Err(error) => {
                result.errors.push(FileError::new(
                    Some(relative_path),
                    FileOperation::Metadata,
                    error.to_string(),
                ));
                continue;
            }
        };

        if metadata.file_type().is_symlink() {
            result.symlinks_skipped += 1;
            continue;
        }
        if !metadata.is_file() && !metadata.is_dir() {
            result.unsupported_entries_skipped += 1;
            continue;
        }

        match manifest_entry_from_metadata(entry.path(), relative_path.clone(), &metadata) {
            Ok(manifest_entry) => result.entries.push(manifest_entry),
            Err(error) => result.errors.push(FileError::new(
                Some(relative_path),
                FileOperation::Metadata,
                error.to_string(),
            )),
        }
    }

    result
        .entries
        .sort_unstable_by(|left, right| left.relative_path.cmp(&right.relative_path));
    Ok(result)
}
