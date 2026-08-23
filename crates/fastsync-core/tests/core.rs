use std::error::Error;
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

use fastsync_core::{
    CoreError, FileType, ManifestEntry, capture_source_metadata, ensure_source_unchanged,
    finalize_partial_file, hash_file, initialize_partial_file, metadata_matches, partial_file_path,
    safe_join, scan_directory, validate_wire_relative_path,
};
use tempfile::tempdir;

#[test]
fn scanner_normalizes_unicode_paths_and_skips_symlinks() -> Result<(), Box<dyn Error>> {
    let root = tempdir()?;
    let directory = root.path().join("папка с пробелом");
    fs::create_dir(&directory)?;
    fs::write(directory.join("данные file.txt"), b"payload")?;

    #[cfg(unix)]
    std::os::unix::fs::symlink(&directory, root.path().join("ignored-link"))?;

    let scan = scan_directory(root.path())?;
    assert!(scan.errors.is_empty(), "scan errors: {:?}", scan.errors);
    assert!(scan.entries.iter().any(|entry| {
        entry.relative_path == "папка с пробелом" && entry.file_type == FileType::Directory
    }));
    assert!(scan.entries.iter().any(|entry| {
        entry.relative_path == "папка с пробелом/данные file.txt"
            && entry.file_type == FileType::File
    }));
    assert!(
        !scan
            .entries
            .iter()
            .any(|entry| entry.relative_path.contains("ignored-link"))
    );

    #[cfg(unix)]
    assert_eq!(scan.symlinks_skipped, 1);

    Ok(())
}

#[test]
fn wire_paths_reject_traversal_and_windows_forms() {
    let invalid = [
        "",
        "/absolute",
        "//server/share",
        "C:/Windows",
        "folder/D:relative",
        "folder\\file",
        ".",
        "./file",
        "..",
        "folder/../file",
        "folder//file",
        "folder/file/",
        "nul\0byte",
        "folder/name:stream",
        "folder/trailing.",
        "folder/trailing ",
        "CON",
        "con.txt",
        "aux.bin",
        "LPT9",
        ".fastsync-stage-deadbeef/file.bin",
        "nested/.FASTSYNC-STAGE-token/file.bin",
    ];

    for path in invalid {
        assert!(
            validate_wire_relative_path(path).is_err(),
            "accepted invalid path {path:?}"
        );
    }

    let root = Path::new("destination");
    assert!(safe_join(root, "../escape").is_err());
    assert_eq!(
        safe_join(root, "Каталог с местом/файл.bin"),
        Ok(root.join("Каталог с местом").join("файл.bin"))
    );
}

#[cfg(unix)]
#[test]
fn scanner_rejects_a_symlink_root() -> Result<(), Box<dyn Error>> {
    let parent = tempdir()?;
    let real = parent.path().join("real");
    fs::create_dir(&real)?;
    let linked = parent.path().join("linked");
    std::os::unix::fs::symlink(&real, &linked)?;

    assert!(matches!(
        scan_directory(&linked),
        Err(CoreError::NotDirectory { .. })
    ));
    Ok(())
}

#[test]
fn hashing_produces_full_hash_and_fixed_chunks() -> Result<(), Box<dyn Error>> {
    let root = tempdir()?;
    let path = root.path().join("chunked.bin");
    let content = b"abcdefghij";
    fs::write(&path, content)?;
    let entry = scan_directory(root.path())?
        .entries
        .into_iter()
        .find(|entry| entry.relative_path == "chunked.bin")
        .ok_or("file missing from scan")?;

    let hashed = hash_file(&path, &entry, 4)?;
    assert_eq!(hashed.hash, *blake3::hash(content).as_bytes());
    assert_eq!(hashed.chunks.len(), 3);
    assert_eq!(
        hashed
            .chunks
            .iter()
            .map(|chunk| (chunk.index, chunk.offset, chunk.size))
            .collect::<Vec<_>>(),
        vec![(0, 0, 4), (1, 4, 4), (2, 8, 2)]
    );
    assert_eq!(
        hashed.chunks[0].hash,
        *blake3::hash(&content[0..4]).as_bytes()
    );
    assert_eq!(
        hashed.chunks[1].hash,
        *blake3::hash(&content[4..8]).as_bytes()
    );
    assert_eq!(
        hashed.chunks[2].hash,
        *blake3::hash(&content[8..10]).as_bytes()
    );

    Ok(())
}

#[test]
fn changed_source_metadata_is_detected() -> Result<(), Box<dyn Error>> {
    let root = tempdir()?;
    let path = root.path().join("source.bin");
    fs::write(&path, b"before")?;
    let entry = scan_directory(root.path())?
        .entries
        .into_iter()
        .find(|entry| entry.relative_path == "source.bin")
        .ok_or("file missing from scan")?;
    let original = capture_source_metadata(&path)?;

    OpenOptions::new()
        .append(true)
        .open(&path)?
        .write_all(b"-changed")?;

    assert!(matches!(
        ensure_source_unchanged(&path, &original),
        Err(CoreError::SourceChanged { .. })
    ));
    assert!(matches!(
        hash_file(&path, &entry, 4),
        Err(CoreError::SourceChanged { .. })
    ));

    let mut changed_entry = entry.clone();
    changed_entry.size += 1;
    assert!(!metadata_matches(&entry, &changed_entry));

    Ok(())
}

#[test]
fn partial_file_is_named_and_atomically_replaces_destination() -> Result<(), Box<dyn Error>> {
    let root = tempdir()?;
    let destination = root.path().join("итоговый file.bin");
    fs::write(&destination, b"old")?;
    let mut old_permissions = fs::metadata(&destination)?.permissions();
    old_permissions.set_readonly(true);
    fs::set_permissions(&destination, old_permissions)?;
    let content = b"replacement payload";

    assert_eq!(
        partial_file_path(&destination),
        root.path().join("итоговый file.bin.fastsync-part")
    );

    let mut partial = initialize_partial_file(&destination, content.len() as u64)?;
    partial.seek(SeekFrom::Start(0))?;
    partial.write_all(content)?;
    partial.flush()?;
    drop(partial);

    let expected_hash = *blake3::hash(content).as_bytes();
    finalize_partial_file(&destination, &expected_hash)?;
    assert_eq!(fs::read(&destination)?, content);
    assert!(!partial_file_path(&destination).exists());

    Ok(())
}

#[test]
fn failed_partial_verification_preserves_destination() -> Result<(), Box<dyn Error>> {
    let root = tempdir()?;
    let destination = root.path().join("result.bin");
    fs::write(&destination, b"old")?;

    let mut partial = initialize_partial_file(&destination, 3)?;
    partial.write_all(b"bad")?;
    drop(partial);

    let expected_hash = *blake3::hash(b"good").as_bytes();
    assert!(matches!(
        finalize_partial_file(&destination, &expected_hash),
        Err(CoreError::HashMismatch { .. })
    ));
    assert_eq!(fs::read(&destination)?, b"old");
    assert!(partial_file_path(&destination).exists());

    Ok(())
}

#[test]
fn manifest_constructor_enforces_normalized_wire_paths() {
    assert!(ManifestEntry::new("a/../b", 0, 0, FileType::File, false).is_err());
    assert!(ManifestEntry::new("кириллица/file name", 0, 0, FileType::File, false).is_ok());
}
