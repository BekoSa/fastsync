use std::fs::{File, Metadata};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use fastsync_core::{
    ChunkDescriptor, FileType, HashedFile, ManifestEntry, SourceMetadata, capture_source_metadata,
    ensure_source_unchanged,
};
use fastsync_storage::{Database, HashCacheEntry, HashCacheKey};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::TRANSFER_BUFFER_SIZE;
use crate::error::{Result, TransferError};

const MAX_CHUNKS_PER_FILE: u64 = 131_072;

pub(crate) async fn hash_file_cached(
    database: Database,
    semaphore: Arc<Semaphore>,
    path: PathBuf,
    entry: ManifestEntry,
    chunk_size: u64,
    use_cache: bool,
    cancellation: CancellationToken,
) -> Result<HashedFile> {
    if cancellation.is_cancelled() {
        return Err(TransferError::Cancelled);
    }
    let key = HashCacheKey {
        path: cache_path(&path),
        size: entry.size,
        mtime_ns: entry.mtime_ns,
        chunk_size,
    };

    if use_cache && let Some(cached) = database.get_hash_cache(&key)? {
        let file = HashedFile {
            entry: entry.clone(),
            hash: cached.full_hash,
            chunks: cached.chunks,
        };
        if validate_hashed_file(&file, chunk_size).is_ok()
            && ensure_source_unchanged(&path, &entry.source_metadata()).is_ok()
        {
            return Ok(file);
        }
        database.remove_hash_cache(&key.path)?;
    }

    let permit = tokio::select! {
        _ = cancellation.cancelled() => return Err(TransferError::Cancelled),
        permit = semaphore.acquire_owned() => {
            permit.map_err(|_| TransferError::Task("hash semaphore was closed".to_owned()))?
        }
    };
    let hash_path = path.clone();
    let hash_entry = entry.clone();
    let file = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        hash_file_streaming(&hash_path, &hash_entry, chunk_size, &cancellation)
    })
    .await
    .map_err(|error| TransferError::Task(error.to_string()))??;
    validate_hashed_file(&file, chunk_size)?;

    if use_cache {
        database.put_hash_cache(&HashCacheEntry {
            key,
            full_hash: file.hash,
            chunks: file.chunks.clone(),
            updated_at: now_millis()?,
        })?;
    }
    Ok(file)
}

pub(crate) fn validate_hashed_file(file: &HashedFile, chunk_size: u64) -> Result<()> {
    if chunk_size == 0 {
        return Err(TransferError::InvalidData(
            "chunk size must be non-zero".to_owned(),
        ));
    }
    if file.entry.file_type != FileType::File {
        return Err(TransferError::InvalidData(format!(
            "{} is not a regular file",
            file.entry.relative_path
        )));
    }
    let chunk_count = u64::try_from(file.chunks.len()).map_err(|_| {
        TransferError::InvalidData(format!("{} has too many chunks", file.entry.relative_path))
    })?;
    if chunk_count > MAX_CHUNKS_PER_FILE {
        return Err(TransferError::InvalidData(format!(
            "{} has {chunk_count} chunks; the limit is {MAX_CHUNKS_PER_FILE}",
            file.entry.relative_path
        )));
    }

    let mut expected_offset = 0_u64;
    for (position, chunk) in file.chunks.iter().enumerate() {
        let expected_index = u64::try_from(position).map_err(|_| {
            TransferError::InvalidData(format!("{} has too many chunks", file.entry.relative_path))
        })?;
        if chunk.index != expected_index || chunk.offset != expected_offset {
            return Err(TransferError::InvalidData(format!(
                "{} has non-contiguous chunk descriptors",
                file.entry.relative_path
            )));
        }
        if chunk.size == 0 || chunk.size > chunk_size {
            return Err(TransferError::InvalidData(format!(
                "{} has invalid chunk {} size {}",
                file.entry.relative_path, chunk.index, chunk.size
            )));
        }
        if chunk.size != chunk_size
            && chunk
                .offset
                .checked_add(chunk.size)
                .is_some_and(|end| end != file.entry.size)
        {
            return Err(TransferError::InvalidData(format!(
                "{} has a short non-final chunk",
                file.entry.relative_path
            )));
        }
        expected_offset = expected_offset.checked_add(chunk.size).ok_or_else(|| {
            TransferError::InvalidData(format!(
                "{} chunk offsets overflow",
                file.entry.relative_path
            ))
        })?;
    }

    if expected_offset != file.entry.size {
        return Err(TransferError::InvalidData(format!(
            "{} chunk sizes total {}, expected {}",
            file.entry.relative_path, expected_offset, file.entry.size
        )));
    }
    if file.entry.size == 0 && !file.chunks.is_empty() {
        return Err(TransferError::InvalidData(format!(
            "empty file {} contains chunk descriptors",
            file.entry.relative_path
        )));
    }
    Ok(())
}

pub(crate) fn now_millis() -> Result<i64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| TransferError::InvalidData(error.to_string()))?
        .as_millis();
    i64::try_from(millis)
        .map_err(|_| TransferError::InvalidData("system time is out of range".to_owned()))
}

fn cache_path(path: &Path) -> String {
    format!(
        "path-blake3:{}",
        blake3::hash(path.as_os_str().as_encoded_bytes()).to_hex()
    )
}

fn hash_file_streaming(
    path: &Path,
    entry: &ManifestEntry,
    chunk_size: u64,
    cancellation: &CancellationToken,
) -> Result<HashedFile> {
    if chunk_size == 0 {
        return Err(TransferError::InvalidData(
            "chunk size must be non-zero".to_owned(),
        ));
    }
    if entry.file_type != FileType::File {
        return Err(TransferError::InvalidData(format!(
            "`{}` is not a regular file",
            path.display()
        )));
    }
    let expected_chunks = entry.size.div_ceil(chunk_size);
    if expected_chunks > MAX_CHUNKS_PER_FILE {
        let minimum_chunk_size = entry.size.div_ceil(MAX_CHUNKS_PER_FILE);
        return Err(TransferError::InvalidData(format!(
            "file `{}` needs {expected_chunks} chunks; increase chunk size to at least {minimum_chunk_size} bytes",
            path.display()
        )));
    }

    let expected = entry.source_metadata();
    let initial = capture_source_metadata(path)?;
    ensure_metadata_unchanged(path, &expected, &initial)?;
    let mut input = File::open(path)
        .map_err(|error| TransferError::io("opening source for hashing", path, error))?;
    let opened = metadata_snapshot(
        path,
        &input
            .metadata()
            .map_err(|error| TransferError::io("reading open source metadata", path, error))?,
    )?;
    ensure_metadata_unchanged(path, &initial, &opened)?;

    let mut buffer = vec![0_u8; TRANSFER_BUFFER_SIZE];
    let mut full_hasher = blake3::Hasher::new();
    let mut chunk_hasher = blake3::Hasher::new();
    let mut chunk_bytes = 0_u64;
    let mut total_bytes = 0_u64;
    let chunk_capacity = usize::try_from(expected_chunks)
        .map_err(|_| TransferError::InvalidData("chunk count is out of range".to_owned()))?;
    let mut chunks = Vec::with_capacity(chunk_capacity);
    loop {
        if cancellation.is_cancelled() {
            return Err(TransferError::Cancelled);
        }
        let remaining_in_chunk = chunk_size.saturating_sub(chunk_bytes);
        let wanted = usize::try_from(remaining_in_chunk.min(TRANSFER_BUFFER_SIZE as u64))
            .map_err(|_| TransferError::InvalidData("chunk size is out of range".to_owned()))?;
        let read = match input.read(&mut buffer[..wanted]) {
            Ok(0) => break,
            Ok(read) => read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => {
                return Err(TransferError::io("reading source for hashing", path, error));
            }
        };
        full_hasher.update(&buffer[..read]);
        chunk_hasher.update(&buffer[..read]);
        let read = u64::try_from(read)
            .map_err(|_| TransferError::InvalidData("read size is out of range".to_owned()))?;
        chunk_bytes = chunk_bytes.checked_add(read).ok_or_else(|| {
            TransferError::InvalidData(format!("file `{}` is too large", path.display()))
        })?;
        total_bytes = total_bytes.checked_add(read).ok_or_else(|| {
            TransferError::InvalidData(format!("file `{}` is too large", path.display()))
        })?;
        if chunk_bytes == chunk_size {
            push_chunk(
                path,
                &mut chunks,
                total_bytes - chunk_bytes,
                chunk_bytes,
                &chunk_hasher,
            )?;
            chunk_hasher = blake3::Hasher::new();
            chunk_bytes = 0;
        }
    }
    if chunk_bytes != 0 {
        push_chunk(
            path,
            &mut chunks,
            total_bytes - chunk_bytes,
            chunk_bytes,
            &chunk_hasher,
        )?;
    }

    let final_opened = metadata_snapshot(
        path,
        &input
            .metadata()
            .map_err(|error| TransferError::io("re-reading open source metadata", path, error))?,
    )?;
    ensure_metadata_unchanged(path, &opened, &final_opened)?;
    let final_path = capture_source_metadata(path)?;
    ensure_metadata_unchanged(path, &initial, &final_path)?;
    if total_bytes != expected.size {
        return Err(fastsync_core::CoreError::ReadSizeChanged {
            path: path.to_path_buf(),
            expected: expected.size,
            actual: total_bytes,
        }
        .into());
    }

    Ok(HashedFile {
        entry: entry.clone(),
        hash: *full_hasher.finalize().as_bytes(),
        chunks,
    })
}

fn push_chunk(
    path: &Path,
    chunks: &mut Vec<ChunkDescriptor>,
    offset: u64,
    size: u64,
    hasher: &blake3::Hasher,
) -> Result<()> {
    let index = u64::try_from(chunks.len()).map_err(|_| {
        TransferError::InvalidData(format!("file `{}` has too many chunks", path.display()))
    })?;
    chunks.push(ChunkDescriptor {
        index,
        offset,
        size,
        hash: *hasher.finalize().as_bytes(),
    });
    Ok(())
}

fn metadata_snapshot(path: &Path, metadata: &Metadata) -> Result<SourceMetadata> {
    let file_type = if metadata.is_file() {
        FileType::File
    } else if metadata.is_dir() {
        FileType::Directory
    } else {
        return Err(TransferError::InvalidData(format!(
            "unsupported file type at `{}`",
            path.display()
        )));
    };
    let modified = metadata
        .modified()
        .map_err(|error| TransferError::io("reading source modification time", path, error))?;
    let mtime_ns = match modified.duration_since(UNIX_EPOCH) {
        Ok(duration) => {
            i128::from(duration.as_secs()) * 1_000_000_000 + i128::from(duration.subsec_nanos())
        }
        Err(error) => {
            let duration = error.duration();
            -(i128::from(duration.as_secs()) * 1_000_000_000 + i128::from(duration.subsec_nanos()))
        }
    };
    let mtime_ns = i64::try_from(mtime_ns).map_err(|_| {
        TransferError::InvalidData(format!(
            "modification time for `{}` is out of range",
            path.display()
        ))
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

fn ensure_metadata_unchanged(
    path: &Path,
    expected: &SourceMetadata,
    actual: &SourceMetadata,
) -> Result<()> {
    if expected == actual {
        return Ok(());
    }
    Err(fastsync_core::CoreError::SourceChanged {
        path: path.to_path_buf(),
        expected: *expected,
        actual: *actual,
    }
    .into())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    #[test]
    fn streaming_hash_matches_core_chunk_descriptors() -> Result<()> {
        let directory = tempfile::tempdir()
            .map_err(|error| TransferError::io("creating hash test directory", ".", error))?;
        let path = directory.path().join("data.bin");
        let contents: Vec<u8> = (0..900_000)
            .map(|position| ((position * 19 + position / 11) % 253) as u8)
            .collect();
        fs::write(&path, contents)
            .map_err(|error| TransferError::io("writing hash test file", &path, error))?;
        let entry = fastsync_core::manifest_entry_from_path(directory.path(), &path)?;
        let chunk_size = 350_000;
        let expected = fastsync_core::hash_file(&path, &entry, chunk_size)?;
        let actual = hash_file_streaming(&path, &entry, chunk_size, &CancellationToken::new())?;
        assert_eq!(actual, expected);
        Ok(())
    }
}
