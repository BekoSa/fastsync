use std::fs::{self, File, OpenOptions};
use std::io::{self, Read};
use std::path::Path;

use crate::error::{CoreError, Result};
use crate::metadata::{
    apply_manifest_metadata, capture_source_metadata, source_metadata_from_metadata,
};
use crate::model::{ContentHash, FileType, HashedFile, SourceMetadata};
use crate::path::partial_file_path;

const VERIFY_BUFFER_SIZE: usize = 64 * 1024;

/// Creates or truncates the sibling `.fastsync-part` file and sets its final length.
pub fn initialize_partial_file(destination: impl AsRef<Path>, size: u64) -> Result<File> {
    let partial_path = partial_file_path(destination);
    initialize_partial_file_at(partial_path, size)
}

/// Creates or truncates an explicit staging file and sets its final length.
pub fn initialize_partial_file_at(partial_path: impl AsRef<Path>, size: u64) -> Result<File> {
    let partial_path = partial_path.as_ref();
    match fs::symlink_metadata(partial_path) {
        Ok(metadata) if !metadata.is_file() => {
            return Err(CoreError::NotRegularFile {
                path: partial_path.to_path_buf(),
            });
        }
        Ok(_) => make_staging_writable(partial_path)?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(CoreError::io(
                "checking existing partial file at",
                partial_path,
                source,
            ));
        }
    }

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(partial_path)
        .map_err(|source| CoreError::io("creating partial file at", partial_path, source))?;
    file.set_len(size)
        .map_err(|source| CoreError::io("sizing partial file at", partial_path, source))?;
    Ok(file)
}

/// Verifies the complete sibling partial file without changing the destination.
pub fn verify_partial_file(
    destination: impl AsRef<Path>,
    expected_hash: &ContentHash,
) -> Result<()> {
    let destination = destination.as_ref();
    let partial_path = partial_file_path(destination);
    let (_file, actual_hash, _metadata) = open_verified_partial(&partial_path, expected_hash)?;
    debug_assert_eq!(&actual_hash, expected_hash);
    Ok(())
}

/// Verifies and atomically replaces `destination` with its sibling partial file.
pub fn finalize_partial_file(
    destination: impl AsRef<Path>,
    expected_hash: &ContentHash,
) -> Result<()> {
    let destination = destination.as_ref();
    let partial_path = partial_file_path(destination);
    let (file, _actual_hash, _metadata) = open_verified_partial(&partial_path, expected_hash)?;
    file.sync_all()
        .map_err(|source| CoreError::io("syncing partial file at", &partial_path, source))?;
    drop(file);
    atomic_replace(&partial_path, destination)?;
    Ok(())
}

/// Finalizes a verified partial file and applies the manifest mtime and read-only state.
pub fn finalize_hashed_file(destination: impl AsRef<Path>, expected: &HashedFile) -> Result<()> {
    let destination = destination.as_ref();
    let partial_path = partial_file_path(destination);
    finalize_hashed_file_from(partial_path, destination, expected)
}

/// Verifies an explicit staging file and atomically replaces `destination` with it.
pub fn finalize_hashed_file_from(
    partial_path: impl AsRef<Path>,
    destination: impl AsRef<Path>,
    expected: &HashedFile,
) -> Result<()> {
    finalize_hashed_file_from_cancellable(partial_path, destination, expected, || false)
}

/// Cancellable variant of [`finalize_hashed_file_from`].
pub fn finalize_hashed_file_from_cancellable<F>(
    partial_path: impl AsRef<Path>,
    destination: impl AsRef<Path>,
    expected: &HashedFile,
    is_cancelled: F,
) -> Result<()>
where
    F: Fn() -> bool,
{
    let destination = destination.as_ref();
    let partial_path = partial_path.as_ref();
    if expected.entry.file_type != FileType::File {
        return Err(CoreError::NotRegularFile {
            path: partial_path.to_path_buf(),
        });
    }

    let (file, _actual_hash, metadata) =
        open_verified_partial_cancellable(partial_path, &expected.hash, &is_cancelled)?;
    if metadata.size != expected.entry.size {
        return Err(CoreError::SizeMismatch {
            path: partial_path.to_path_buf(),
            expected: expected.entry.size,
            actual: metadata.size,
        });
    }

    file.sync_all()
        .map_err(|source| CoreError::io("syncing partial file at", partial_path, source))?;
    if let Err(error) = apply_manifest_metadata(partial_path, &expected.entry) {
        drop(file);
        let _ = make_staging_writable(partial_path);
        return Err(error);
    }
    if let Err(source) = file.sync_all() {
        drop(file);
        let _ = make_staging_writable(partial_path);
        return Err(CoreError::io(
            "syncing partial metadata at",
            partial_path,
            source,
        ));
    }
    drop(file);
    if is_cancelled() {
        make_staging_writable(partial_path)?;
        return Err(CoreError::OperationCancelled {
            path: partial_path.to_path_buf(),
        });
    }
    if let Err(error) = atomic_replace(partial_path, destination) {
        let _ = make_staging_writable(partial_path);
        return Err(error);
    }
    Ok(())
}

fn make_staging_writable(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|source| CoreError::io("reading staging permissions for", path, source))?;
    let mut permissions = metadata.permissions();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        permissions.set_mode(permissions.mode() | 0o200);
    }
    #[cfg(not(unix))]
    permissions.set_readonly(false);
    fs::set_permissions(path, permissions)
        .map_err(|source| CoreError::io("restoring staging permissions on", path, source))
}

fn open_verified_partial(
    partial_path: &Path,
    expected_hash: &ContentHash,
) -> Result<(File, ContentHash, SourceMetadata)> {
    open_verified_partial_cancellable(partial_path, expected_hash, &|| false)
}

fn open_verified_partial_cancellable<F>(
    partial_path: &Path,
    expected_hash: &ContentHash,
    is_cancelled: &F,
) -> Result<(File, ContentHash, SourceMetadata)>
where
    F: Fn() -> bool,
{
    if is_cancelled() {
        return Err(CoreError::OperationCancelled {
            path: partial_path.to_path_buf(),
        });
    }
    let initial = capture_source_metadata(partial_path)?;
    if initial.file_type != FileType::File {
        return Err(CoreError::NotRegularFile {
            path: partial_path.to_path_buf(),
        });
    }

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(partial_path)
        .map_err(|source| CoreError::io("opening partial file at", partial_path, source))?;
    let opened_metadata = file.metadata().map_err(|source| {
        CoreError::io("reading open partial metadata for", partial_path, source)
    })?;
    let opened = source_metadata_from_metadata(partial_path, &opened_metadata)?;
    ensure_unchanged(partial_path, &initial, &opened)?;

    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0_u8; VERIFY_BUFFER_SIZE];
    loop {
        if is_cancelled() {
            return Err(CoreError::OperationCancelled {
                path: partial_path.to_path_buf(),
            });
        }
        match file.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => hasher.update(&buffer[..read]),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(source) => {
                return Err(CoreError::io(
                    "reading partial file at",
                    partial_path,
                    source,
                ));
            }
        };
    }
    let actual_hash = *hasher.finalize().as_bytes();

    let final_metadata = file.metadata().map_err(|source| {
        CoreError::io("re-reading open partial metadata for", partial_path, source)
    })?;
    let final_opened = source_metadata_from_metadata(partial_path, &final_metadata)?;
    ensure_unchanged(partial_path, &opened, &final_opened)?;
    let final_path = capture_source_metadata(partial_path)?;
    ensure_unchanged(partial_path, &initial, &final_path)?;

    if &actual_hash != expected_hash {
        return Err(CoreError::HashMismatch {
            path: partial_path.to_path_buf(),
            expected: *expected_hash,
            actual: actual_hash,
        });
    }

    Ok((file, actual_hash, initial))
}

fn ensure_unchanged(path: &Path, expected: &SourceMetadata, actual: &SourceMetadata) -> Result<()> {
    if expected == actual {
        return Ok(());
    }

    Err(CoreError::SourceChanged {
        path: path.to_path_buf(),
        expected: *expected,
        actual: *actual,
    })
}

#[cfg(not(windows))]
fn atomic_replace(source: &Path, destination: &Path) -> Result<()> {
    fs::rename(source, destination)
        .map_err(|error| CoreError::io("atomically replacing destination at", destination, error))
}

#[cfg(windows)]
fn atomic_replace(source: &Path, destination: &Path) -> Result<()> {
    windows_atomic_replace(source, destination)
        .map_err(|error| CoreError::io("atomically replacing destination at", destination, error))
}

#[cfg(windows)]
fn windows_atomic_replace(source: &Path, destination: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    };

    fn wide_null_terminated(path: &Path) -> io::Result<Vec<u16>> {
        let absolute = match fs::canonicalize(path) {
            Ok(path) => path,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let parent = path.parent().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "path has no parent directory")
                })?;
                let name = path.file_name().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "path has no file name")
                })?;
                fs::canonicalize(parent)?.join(name)
            }
            Err(error) => return Err(error),
        };
        let slash = u16::from(b'\\');
        let forward_slash = u16::from(b'/');
        let question = u16::from(b'?');
        let mut original: Vec<u16> = absolute.as_os_str().encode_wide().collect();
        for character in &mut original {
            if *character == forward_slash {
                *character = slash;
            }
        }
        let mut wide = if original.starts_with(&[slash, slash, question, slash]) {
            original
        } else if original.starts_with(&[slash, slash]) {
            let mut prefixed: Vec<u16> = r"\\?\UNC\".encode_utf16().collect();
            prefixed.extend_from_slice(&original[2..]);
            prefixed
        } else {
            let mut prefixed: Vec<u16> = r"\\?\".encode_utf16().collect();
            prefixed.extend_from_slice(&original);
            prefixed
        };
        if wide.contains(&0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Windows paths cannot contain NUL",
            ));
        }
        wide.push(0);
        Ok(wide)
    }

    let destination_path = destination;
    let source_wide = wide_null_terminated(source)?;
    let destination_wide = wide_null_terminated(destination_path)?;

    let read_only_permissions = match fs::metadata(destination_path) {
        Ok(metadata) if metadata.permissions().readonly() => {
            let original = metadata.permissions();
            let mut writable = original.clone();
            writable.set_readonly(false);
            fs::set_permissions(destination_path, writable)?;
            Some(original)
        }
        Ok(_) => None,
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(error),
    };

    // SAFETY: both pointers reference live, NUL-terminated UTF-16 buffers for the call.
    let result = unsafe {
        MoveFileExW(
            source_wide.as_ptr(),
            destination_wide.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if result == 0 {
        let move_error = io::Error::last_os_error();
        if let Some(permissions) = read_only_permissions {
            let _ = fs::set_permissions(destination_path, permissions);
        }
        Err(move_error)
    } else {
        Ok(())
    }
}
