use std::io;
use std::path::{Component, Path, PathBuf};

use fastsync_core::{
    ChunkDescriptor, CoreError, FileType, HashedFile, ManifestEntry, apply_manifest_metadata,
    finalize_hashed_file_from_cancellable, initialize_partial_file_at,
    is_fastsync_staging_component, manifest_entry_from_path, safe_join,
};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio_util::sync::CancellationToken;

use crate::TRANSFER_BUFFER_SIZE;
use crate::error::{Result, TransferError};

pub(crate) async fn ensure_destination_root(root: &Path) -> Result<PathBuf> {
    ensure_root(root, false).await
}

async fn ensure_root(root: &Path, allow_staging_namespace: bool) -> Result<PathBuf> {
    if !root.is_absolute() {
        return Err(TransferError::InvalidData(format!(
            "destination root `{}` is not absolute",
            root.display()
        )));
    }
    if root
        .components()
        .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(TransferError::InvalidData(format!(
            "destination root `{}` contains dot components",
            root.display()
        )));
    }
    if !allow_staging_namespace
        && root.components().any(|component| {
            matches!(
                component,
                Component::Normal(name)
                    if name.to_str().is_some_and(is_fastsync_staging_component)
            )
        })
    {
        return Err(TransferError::InvalidData(format!(
            "destination root `{}` is inside FastSync's staging namespace",
            root.display()
        )));
    }
    let ancestors: Vec<_> = root.ancestors().collect();
    for ancestor in ancestors.into_iter().rev() {
        if !ancestor.as_os_str().is_empty() {
            ensure_one_directory(ancestor).await?;
        }
    }
    fs::canonicalize(root)
        .await
        .map_err(|source| TransferError::io("canonicalizing destination root", root, source))
}

pub(crate) async fn destination_file_path(root: &Path, relative: &str) -> Result<PathBuf> {
    let path = destination_candidate_path(root, relative)?;
    ensure_parents(root, relative, false).await?;
    match fs::symlink_metadata(&path).await {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(TransferError::InvalidData(
            format!("destination `{}` is a symbolic link", path.display()),
        )),
        Ok(_) => canonical_child_path(root, &path).await,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            canonical_child_path(root, &path).await
        }
        Err(source) => Err(TransferError::io("checking destination", path, source)),
    }
}

pub(crate) fn destination_candidate_path(root: &Path, relative: &str) -> Result<PathBuf> {
    safe_join(root, relative)
        .map_err(|error| TransferError::InvalidData(format!("invalid path {relative:?}: {error}")))
}

pub(crate) async fn create_manifest_directory(
    root: &Path,
    entry: &ManifestEntry,
) -> Result<PathBuf> {
    if entry.file_type != FileType::Directory {
        return Err(TransferError::InvalidData(format!(
            "{} is not a directory manifest entry",
            entry.relative_path
        )));
    }
    let path = safe_join(root, &entry.relative_path).map_err(|error| {
        TransferError::InvalidData(format!("invalid path {:?}: {error}", entry.relative_path))
    })?;
    ensure_parents(root, &entry.relative_path, false).await?;
    ensure_one_directory(&path).await?;
    canonical_child_path(root, &path).await
}

pub(crate) async fn existing_manifest(
    root: PathBuf,
    path: PathBuf,
) -> Result<Option<ManifestEntry>> {
    match fs::symlink_metadata(&path).await {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(TransferError::InvalidData(format!(
                "destination `{}` is a symbolic link",
                path.display()
            )));
        }
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(TransferError::io(
                "reading destination metadata",
                path,
                source,
            ));
        }
    }
    tokio::task::spawn_blocking(move || manifest_entry_from_path(root, path))
        .await
        .map_err(|error| TransferError::Task(error.to_string()))?
        .map(Some)
        .map_err(TransferError::from)
}

pub(crate) async fn staging_file_path(root: &Path, relative: &str) -> Result<PathBuf> {
    let path = safe_join(root, relative).map_err(|error| {
        TransferError::InvalidData(format!("invalid staging path {relative:?}: {error}"))
    })?;
    ensure_parents(root, relative, true).await?;
    canonical_child_path(root, &path).await
}

pub(crate) async fn prepare_partial(partial: PathBuf, size: u64) -> Result<bool> {
    let usable = match fs::symlink_metadata(&partial).await {
        Ok(metadata) => {
            metadata.is_file() && !metadata.file_type().is_symlink() && metadata.len() == size
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => false,
        Err(source) => {
            return Err(TransferError::io("checking partial file", partial, source));
        }
    };
    if usable {
        make_partial_writable(&partial).await?;
    }
    if !usable {
        let initialize_path = partial.clone();
        tokio::task::spawn_blocking(move || {
            initialize_partial_file_at(initialize_path, size).map(drop)
        })
        .await
        .map_err(|error| TransferError::Task(error.to_string()))??;
    }
    Ok(usable)
}

async fn make_partial_writable(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .await
        .map_err(|error| TransferError::io("reading staging permissions", path, error))?;
    let mut permissions = metadata.permissions();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        permissions.set_mode(permissions.mode() | 0o200);
    }
    #[cfg(not(unix))]
    permissions.set_readonly(false);
    fs::set_permissions(path, permissions)
        .await
        .map_err(|error| TransferError::io("restoring staging permissions", path, error))
}

pub(crate) async fn hash_region(path: &Path, offset: u64, size: u64) -> Result<[u8; 32]> {
    let mut file = File::open(path)
        .await
        .map_err(|source| TransferError::io("opening file region", path, source))?;
    file.seek(SeekFrom::Start(offset))
        .await
        .map_err(|source| TransferError::io("seeking file region", path, source))?;
    let mut remaining = size;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0_u8; TRANSFER_BUFFER_SIZE];
    while remaining != 0 {
        let wanted = usize::try_from(remaining.min(TRANSFER_BUFFER_SIZE as u64))
            .map_err(|_| TransferError::InvalidData("region size is out of range".to_owned()))?;
        let read = file
            .read(&mut buffer[..wanted])
            .await
            .map_err(|source| TransferError::io("reading file region", path, source))?;
        if read == 0 {
            return Err(TransferError::InvalidData(format!(
                "file `{}` ended while reading a {size}-byte region at {offset}",
                path.display()
            )));
        }
        hasher.update(&buffer[..read]);
        remaining -= read as u64;
    }
    Ok(*hasher.finalize().as_bytes())
}

pub(crate) async fn hash_region_cancellable(
    path: &Path,
    offset: u64,
    size: u64,
    cancellation: &CancellationToken,
) -> Result<[u8; 32]> {
    tokio::select! {
        _ = cancellation.cancelled() => Err(TransferError::Cancelled),
        result = hash_region(path, offset, size) => result,
    }
}

pub(crate) async fn copy_verified_chunk(
    source: &Path,
    partial: &Path,
    chunk: &ChunkDescriptor,
) -> Result<bool> {
    let mut reader = File::open(source)
        .await
        .map_err(|error| TransferError::io("opening reuse source", source, error))?;
    let mut writer = OpenOptions::new()
        .write(true)
        .open(partial)
        .await
        .map_err(|error| TransferError::io("opening partial file", partial, error))?;
    reader
        .seek(SeekFrom::Start(chunk.offset))
        .await
        .map_err(|error| TransferError::io("seeking reuse source", source, error))?;
    writer
        .seek(SeekFrom::Start(chunk.offset))
        .await
        .map_err(|error| TransferError::io("seeking partial file", partial, error))?;

    let mut remaining = chunk.size;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0_u8; TRANSFER_BUFFER_SIZE];
    while remaining != 0 {
        let wanted = usize::try_from(remaining.min(TRANSFER_BUFFER_SIZE as u64))
            .map_err(|_| TransferError::InvalidData("chunk size is out of range".to_owned()))?;
        let read = reader
            .read(&mut buffer[..wanted])
            .await
            .map_err(|error| TransferError::io("reading reuse source", source, error))?;
        if read == 0 {
            return Ok(false);
        }
        hasher.update(&buffer[..read]);
        writer
            .write_all(&buffer[..read])
            .await
            .map_err(|error| TransferError::io("writing reused chunk", partial, error))?;
        remaining -= read as u64;
    }
    writer
        .flush()
        .await
        .map_err(|error| TransferError::io("flushing reused chunk", partial, error))?;
    Ok(hasher.finalize().as_bytes() == &chunk.hash)
}

pub(crate) async fn finalize_file(
    staging: PathBuf,
    destination: PathBuf,
    file: HashedFile,
    cancellation: CancellationToken,
) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        finalize_hashed_file_from_cancellable(staging, destination, &file, || {
            cancellation.is_cancelled()
        })
        .map_err(|error| match error {
            CoreError::OperationCancelled { .. } => TransferError::Cancelled,
            error => TransferError::Core(error),
        })
    })
    .await
    .map_err(|error| TransferError::Task(error.to_string()))??;
    Ok(())
}

pub(crate) fn destination_lease_key(path: &Path) -> String {
    #[cfg(windows)]
    {
        let normalized = path.to_string_lossy().replace('/', "\\").to_lowercase();
        if let Some(unc) = normalized.strip_prefix("\\\\?\\unc\\") {
            format!("\\\\{unc}")
        } else if let Some(local) = normalized.strip_prefix("\\\\?\\") {
            local.to_owned()
        } else {
            normalized
        }
    }
    #[cfg(not(windows))]
    {
        path.to_string_lossy().to_lowercase()
    }
}

pub(crate) async fn apply_directory(path: PathBuf, entry: ManifestEntry) -> Result<()> {
    tokio::task::spawn_blocking(move || apply_manifest_metadata(path, &entry))
        .await
        .map_err(|error| TransferError::Task(error.to_string()))??;
    Ok(())
}

async fn ensure_parents(root: &Path, relative: &str, staging_root: bool) -> Result<()> {
    ensure_root(root, staging_root).await?;
    let components: Vec<&str> = relative.split('/').collect();
    let parent_count = components.len().saturating_sub(1);
    let mut current = root.to_path_buf();
    for component in components.into_iter().take(parent_count) {
        current.push(component);
        ensure_one_directory(&current).await?;
    }
    Ok(())
}

async fn canonical_child_path(root: &Path, path: &Path) -> Result<PathBuf> {
    let canonical_root = fs::canonicalize(root)
        .await
        .map_err(|source| TransferError::io("canonicalizing destination root", root, source))?;
    let parent = path.parent().ok_or_else(|| {
        TransferError::InvalidData(format!("path `{}` has no parent", path.display()))
    })?;
    let name = path.file_name().ok_or_else(|| {
        TransferError::InvalidData(format!("path `{}` has no file name", path.display()))
    })?;
    let parent = fs::canonicalize(parent)
        .await
        .map_err(|source| TransferError::io("canonicalizing destination parent", parent, source))?;
    if parent.starts_with(&canonical_root) {
        Ok(parent.join(name))
    } else {
        Err(TransferError::InvalidData(format!(
            "destination `{}` resolves outside root `{}`",
            path.display(),
            canonical_root.display()
        )))
    }
}

async fn ensure_one_directory(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            Err(TransferError::InvalidData(format!(
                "path component `{}` is not a real directory",
                path.display()
            )))
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => match fs::create_dir(path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let metadata = fs::symlink_metadata(path).await.map_err(|source| {
                    TransferError::io("checking concurrently-created directory", path, source)
                })?;
                if metadata.file_type().is_symlink() || !metadata.is_dir() {
                    Err(TransferError::InvalidData(format!(
                        "path component `{}` is not a real directory",
                        path.display()
                    )))
                } else {
                    Ok(())
                }
            }
            Err(source) => Err(TransferError::io("creating directory", path, source)),
        },
        Err(source) => Err(TransferError::io("checking directory", path, source)),
    }
}
