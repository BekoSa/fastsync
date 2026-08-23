use std::error::Error;
use std::fs;
use std::io::{Seek, SeekFrom, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use fastsync_core::{
    FileType, JobStatus, TransferConfig, TransferJob, VerificationMode, hash_file,
    initialize_partial_file_at, scan_directory,
};
use fastsync_storage::{CompletedChunk, Database, JobFileRecord, JobFileStatus, TrustedPeer};
use fastsync_transfer::{DeviceIdentity, TransferEngine};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transfers_mixed_tree_with_reuse_and_resume() -> Result<(), Box<dyn Error>> {
    let temporary = tempfile::tempdir()?;
    let source_root = temporary.path().join("source");
    let destination_root = temporary.path().join("destination");
    fs::create_dir_all(source_root.join("unicodé"))?;
    fs::create_dir_all(&destination_root)?;

    fs::write(source_root.join("alpha.txt"), b"already present")?;
    fs::write(source_root.join("paired.bin"), b"primary")?;
    fs::write(
        source_root.join("paired.bin.fastsync-part"),
        b"real suffix file",
    )?;
    fs::write(source_root.join("empty.bin"), b"")?;
    fs::write(
        source_root.join("unicodé").join("数据.txt"),
        "FastSync handles Unicode paths\n".as_bytes(),
    )?;
    let large: Vec<u8> = (0..230_000)
        .map(|position| ((position * 31 + position / 17) % 251) as u8)
        .collect();
    fs::write(source_root.join("large.bin"), &large)?;
    fs::write(destination_root.join("alpha.txt"), b"already present")?;
    fs::write(
        destination_root.join("large.bin.fastsync-part"),
        b"unrelated destination data",
    )?;

    let source_database = Database::open(temporary.path().join("source.sqlite3"))?;
    let destination_database = Database::open(temporary.path().join("destination.sqlite3"))?;
    let source_identity = DeviceIdentity::load_or_create(source_database.clone(), "source")?;
    let destination_identity =
        DeviceIdentity::load_or_create(destination_database.clone(), "destination")?;
    source_database.upsert_trusted_peer(&trusted_peer(&destination_identity))?;
    destination_database.upsert_trusted_peer(&trusted_peer(&source_identity))?;

    let source_engine = TransferEngine::bind(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        source_database.clone(),
        source_identity.clone(),
    )?;
    let destination_engine = TransferEngine::bind(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        destination_database.clone(),
        destination_identity.clone(),
    )?;
    let destination_address = destination_engine.local_addr()?;
    let server_cancellation = CancellationToken::new();
    let server = {
        let engine = destination_engine.clone();
        let cancellation = server_cancellation.clone();
        tokio::spawn(async move { engine.serve(cancellation).await })
    };

    let probed = source_engine
        .probe(destination_address, Some(destination_identity.id()))
        .await?;
    assert_eq!(probed.device_id, destination_identity.id());
    assert!(probed.trusted);

    let config = TransferConfig {
        verification_mode: VerificationMode::Verified,
        chunk_size: 64 * 1024,
        concurrency: 4,
        retry_limit: 2,
    };
    let mut job = TransferJob::new(&source_root, &destination_root, config.clone());
    job.peer_id = Some(destination_identity.id());

    let scan = scan_directory(&source_root)?;
    let large_entry = scan
        .entries
        .iter()
        .find(|entry| entry.relative_path == "large.bin" && entry.file_type == FileType::File)
        .ok_or("large file was not scanned")?
        .clone();
    let hashed_large = hash_file(
        source_root.join("large.bin"),
        &large_entry,
        config.chunk_size,
    )?;
    let first_chunk = hashed_large
        .chunks
        .first()
        .copied()
        .ok_or("large file did not have a first chunk")?;

    let mut receiver_job = TransferJob::new("", &destination_root, config);
    receiver_job.id = job.id;
    receiver_job.peer_id = Some(source_identity.id());
    receiver_job.status = JobStatus::Transferring;
    destination_database.upsert_job(&receiver_job)?;
    destination_database.upsert_job_file(&JobFileRecord {
        job_id: job.id,
        path: "large.bin".to_owned(),
        size: large_entry.size,
        status: JobFileStatus::Running,
        bytes_transferred: first_chunk.size,
        error: None,
        updated_at: 1,
    })?;
    let staging_token = uuid::Uuid::new_v4();
    destination_database.set_string(
        &format!(
            "receiver_staging_token_v1:{}:{}",
            source_identity.id(),
            job.id
        ),
        &staging_token.to_string(),
    )?;
    let staging_root = destination_root.join(format!(".fastsync-stage-{staging_token}"));
    fs::create_dir_all(&staging_root)?;
    let mut partial = initialize_partial_file_at(staging_root.join("large.bin"), large_entry.size)?;
    partial.seek(SeekFrom::Start(first_chunk.offset))?;
    let first_end = usize::try_from(first_chunk.size)?;
    partial.write_all(&large[..first_end])?;
    partial.flush()?;
    drop(partial);
    destination_database.record_completed_chunk(&CompletedChunk {
        job_id: job.id,
        path: "large.bin".to_owned(),
        index: first_chunk.index,
        source_hash: hashed_large.hash,
        completed_at: 2,
    })?;

    let (_pause_sender, pause_receiver) = watch::channel(false);
    let (progress_sender, _progress_receiver) = watch::channel(job.progress.clone());
    let result = source_engine
        .transfer_job(
            job,
            destination_address,
            CancellationToken::new(),
            pause_receiver,
            Some(progress_sender),
        )
        .await?;

    assert_eq!(result.status, JobStatus::Completed);
    assert!(result.errors.is_empty());
    assert!(result.progress.skipped_files >= 1);
    assert!(result.progress.reused_bytes >= first_chunk.size);
    assert!(result.progress.transferred_bytes < result.progress.total_bytes);
    assert_eq!(
        fs::read(destination_root.join("alpha.txt"))?,
        fs::read(source_root.join("alpha.txt"))?
    );
    assert_eq!(
        fs::read(destination_root.join("empty.bin"))?,
        Vec::<u8>::new()
    );
    assert_eq!(
        fs::read(destination_root.join("unicodé").join("数据.txt"))?,
        fs::read(source_root.join("unicodé").join("数据.txt"))?
    );
    assert_eq!(fs::read(destination_root.join("paired.bin"))?, b"primary");
    assert_eq!(
        fs::read(destination_root.join("paired.bin.fastsync-part"))?,
        b"real suffix file"
    );
    assert_eq!(
        fs::read(destination_root.join("large.bin.fastsync-part"))?,
        b"unrelated destination data"
    );
    assert_eq!(
        blake3::hash(&fs::read(destination_root.join("large.bin"))?),
        blake3::hash(&large)
    );
    assert!(!staging_root.exists());
    assert!(
        destination_database
            .completed_chunk_indices(job_id(&result), "large.bin", &hashed_large.hash)?
            .is_empty()
    );

    server_cancellation.cancel();
    server.await??;
    source_engine.close();
    destination_engine.close();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reports_an_empty_directory_conflict() -> Result<(), Box<dyn Error>> {
    let temporary = tempfile::tempdir()?;
    let source_root = temporary.path().join("source-conflict");
    let destination_root = temporary.path().join("destination-conflict");
    fs::create_dir_all(source_root.join("blocked"))?;
    fs::create_dir_all(&destination_root)?;
    fs::write(destination_root.join("blocked"), b"must remain a file")?;

    let source_database = Database::open(temporary.path().join("source-conflict.sqlite3"))?;
    let destination_database =
        Database::open(temporary.path().join("destination-conflict.sqlite3"))?;
    let source_identity = DeviceIdentity::load_or_create(source_database.clone(), "source")?;
    let destination_identity =
        DeviceIdentity::load_or_create(destination_database.clone(), "destination")?;
    source_database.upsert_trusted_peer(&trusted_peer(&destination_identity))?;
    destination_database.upsert_trusted_peer(&trusted_peer(&source_identity))?;

    let source_engine = TransferEngine::bind(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        source_database,
        source_identity,
    )?;
    let destination_engine = TransferEngine::bind(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        destination_database,
        destination_identity.clone(),
    )?;
    let destination_address = destination_engine.local_addr()?;
    let server_cancellation = CancellationToken::new();
    let server = {
        let engine = destination_engine.clone();
        let cancellation = server_cancellation.clone();
        tokio::spawn(async move { engine.serve(cancellation).await })
    };

    let mut job = TransferJob::new(&source_root, &destination_root, TransferConfig::default());
    job.peer_id = Some(destination_identity.id());
    let (_pause_sender, pause_receiver) = watch::channel(false);
    let result = source_engine
        .transfer_job(
            job,
            destination_address,
            CancellationToken::new(),
            pause_receiver,
            None,
        )
        .await?;

    assert_eq!(result.status, JobStatus::CompletedWithErrors);
    assert!(result.errors.iter().any(|error| {
        error.relative_path.as_deref() == Some("blocked") && error.message.contains("Conflict")
    }));
    assert_eq!(
        fs::read(destination_root.join("blocked"))?,
        b"must remain a file"
    );

    server_cancellation.cancel();
    server.await??;
    source_engine.close();
    destination_engine.close();
    Ok(())
}

fn trusted_peer(identity: &DeviceIdentity) -> TrustedPeer {
    TrustedPeer {
        device_id: identity.id(),
        name: identity.name().to_owned(),
        public_key: identity.public_key().to_vec(),
        address: String::new(),
        trusted_at: 1,
        last_seen: 1,
    }
}

fn job_id(job: &TransferJob) -> uuid::Uuid {
    job.id
}
