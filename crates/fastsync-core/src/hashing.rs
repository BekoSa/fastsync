use std::fs::File;
use std::io::{self, Read};
use std::path::Path;

use crate::error::{CoreError, Result};
use crate::metadata::{capture_source_metadata, source_metadata_from_metadata};
use crate::model::{ChunkDescriptor, ContentHash, FileType, HashedFile, ManifestEntry};

/// Computes full-file and fixed-size chunk hashes in one sequential read.
///
/// Metadata is compared before and after the read, including against the scanned manifest entry.
pub fn hash_file(
    path: impl AsRef<Path>,
    entry: &ManifestEntry,
    chunk_size: u64,
) -> Result<HashedFile> {
    let path = path.as_ref();
    if chunk_size == 0 || usize::try_from(chunk_size).is_err() {
        return Err(CoreError::InvalidChunkSize { chunk_size });
    }
    if entry.file_type != FileType::File {
        return Err(CoreError::NotRegularFile {
            path: path.to_path_buf(),
        });
    }

    let expected = entry.source_metadata();
    let initial = capture_source_metadata(path)?;
    ensure_snapshot_matches(path, &expected, &initial)?;

    let mut file = File::open(path).map_err(|source| CoreError::io("opening", path, source))?;
    let opened_metadata = file
        .metadata()
        .map_err(|source| CoreError::io("reading open file metadata for", path, source))?;
    let opened = source_metadata_from_metadata(path, &opened_metadata)?;
    ensure_snapshot_matches(path, &initial, &opened)?;

    let (hash, chunks, bytes_read) = hash_chunks(&mut file, path, chunk_size)?;

    let final_metadata = file
        .metadata()
        .map_err(|source| CoreError::io("re-reading open file metadata for", path, source))?;
    let final_opened = source_metadata_from_metadata(path, &final_metadata)?;
    ensure_snapshot_matches(path, &opened, &final_opened)?;

    let final_path = capture_source_metadata(path)?;
    ensure_snapshot_matches(path, &initial, &final_path)?;
    if bytes_read != expected.size {
        return Err(CoreError::ReadSizeChanged {
            path: path.to_path_buf(),
            expected: expected.size,
            actual: bytes_read,
        });
    }

    Ok(HashedFile {
        entry: entry.clone(),
        hash,
        chunks,
    })
}

fn hash_chunks(
    reader: &mut impl Read,
    path: &Path,
    chunk_size: u64,
) -> Result<(ContentHash, Vec<ChunkDescriptor>, u64)> {
    if chunk_size == 0 {
        return Err(CoreError::InvalidChunkSize { chunk_size });
    }
    let chunk_size_usize =
        usize::try_from(chunk_size).map_err(|_| CoreError::InvalidChunkSize { chunk_size })?;
    let mut buffer = Vec::new();
    buffer
        .try_reserve_exact(chunk_size_usize)
        .map_err(|_| CoreError::ChunkBufferAllocation { chunk_size })?;
    buffer.resize(chunk_size_usize, 0);

    let mut full_hasher = blake3::Hasher::new();
    let mut chunks = Vec::new();
    let mut offset = 0_u64;

    loop {
        let mut chunk_hasher = blake3::Hasher::new();
        let mut filled = 0_usize;
        let mut reached_eof = false;

        while filled < buffer.len() {
            match reader.read(&mut buffer[filled..]) {
                Ok(0) => {
                    reached_eof = true;
                    break;
                }
                Ok(read) => {
                    let end = filled + read;
                    full_hasher.update(&buffer[filled..end]);
                    chunk_hasher.update(&buffer[filled..end]);
                    filled = end;
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(source) => return Err(CoreError::io("reading", path, source)),
            }
        }

        if filled == 0 {
            break;
        }

        let size = u64::try_from(filled).map_err(|_| CoreError::FileTooLarge {
            path: path.to_path_buf(),
        })?;
        let index = u64::try_from(chunks.len()).map_err(|_| CoreError::FileTooLarge {
            path: path.to_path_buf(),
        })?;
        chunks.push(ChunkDescriptor {
            index,
            offset,
            size,
            hash: *chunk_hasher.finalize().as_bytes(),
        });
        offset = offset
            .checked_add(size)
            .ok_or_else(|| CoreError::FileTooLarge {
                path: path.to_path_buf(),
            })?;

        if reached_eof {
            break;
        }
    }

    Ok((*full_hasher.finalize().as_bytes(), chunks, offset))
}

fn ensure_snapshot_matches(
    path: &Path,
    expected: &crate::model::SourceMetadata,
    actual: &crate::model::SourceMetadata,
) -> Result<()> {
    if expected == actual {
        return Ok(());
    }

    Err(CoreError::SourceChanged {
        path: path.to_path_buf(),
        expected: *expected,
        actual: *actual,
    })
}
