use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use dashmap::DashSet;
use fastsync_core::{
    ChunkDescriptor, FileError, FileOperation, FileProgress, FileType, HashedFile, JobStatus,
    ManifestEntry, TransferJob, TransferProgress, VerificationMode, ensure_source_unchanged,
    is_receiver_absolute_path, safe_join, scan_directory,
};
use fastsync_protocol::{
    Acknowledgement, ChunkHeader, CompareAction, CompareBatch, CompleteJobRequest, FilePlan,
    FinalizeFileRequest, HashedFileBatch, Request, RequestFrame, Response, ResponseFrame,
    encode_frame, read_frame, write_frame,
};
use fastsync_storage::{JobFileRecord, JobFileStatus};
use futures::{StreamExt, stream};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::TRANSFER_BUFFER_SIZE;
use crate::auth::rpc;
use crate::engine::TransferEngine;
use crate::error::{Result, TransferError};
use crate::filesystem::hash_region_cancellable;
use crate::hashing::{hash_file_cached, now_millis, validate_hashed_file};

const COMPARE_BATCH_SIZE: usize = 512;
const HASH_BATCH_SIZE: usize = 64;

#[derive(Clone)]
struct UploadWork {
    file: Arc<HashedFile>,
    chunk: ChunkDescriptor,
}

impl TransferEngine {
    /// Transfers a job over one authenticated QUIC connection.
    ///
    /// `pause` is `true` while paused. Progress snapshots are sent through
    /// `progress` when supplied. The returned job contains final progress and
    /// all recoverable per-file errors.
    pub async fn transfer_job(
        &self,
        mut job: TransferJob,
        peer_address: SocketAddr,
        cancellation: CancellationToken,
        pause: watch::Receiver<bool>,
        progress: Option<watch::Sender<TransferProgress>>,
    ) -> Result<TransferJob> {
        self.database.upsert_job(&job)?;
        let result = self
            .run_transfer_job(
                &mut job,
                peer_address,
                cancellation.clone(),
                pause,
                progress.as_ref(),
            )
            .await;
        match result {
            Ok(()) => {
                self.database.upsert_job(&job)?;
                publish_progress(progress.as_ref(), &job.progress);
                Ok(job)
            }
            Err(error) => {
                job.status = if matches!(&error, TransferError::Cancelled) {
                    JobStatus::Cancelled
                } else {
                    JobStatus::Failed
                };
                let message = error.to_string();
                if let Err(storage_error) = self.database.upsert_job(&job) {
                    tracing::warn!(%storage_error, "failed to persist failed transfer job");
                }
                if let Err(storage_error) =
                    self.database
                        .update_job_status_with_error(job.id, job.status, Some(&message))
                {
                    tracing::warn!(%storage_error, "failed to persist transfer failure message");
                }
                publish_progress(progress.as_ref(), &job.progress);
                Err(error)
            }
        }
    }

    async fn run_transfer_job(
        &self,
        job: &mut TransferJob,
        peer_address: SocketAddr,
        cancellation: CancellationToken,
        mut pause: watch::Receiver<bool>,
        progress: Option<&watch::Sender<TransferProgress>>,
    ) -> Result<()> {
        validate_config(job)?;
        wait_until_active(&cancellation, &mut pause).await?;
        job.status = JobStatus::Scanning;
        self.persist_job(job, progress)?;

        let source_root = job.source_root.clone();
        let scan_task = tokio::task::spawn_blocking(move || scan_directory(source_root));
        let scan = tokio::select! {
            _ = cancellation.cancelled() => return Err(TransferError::Cancelled),
            scan = scan_task => scan.map_err(|error| TransferError::Task(error.to_string()))??,
        };
        job.errors.extend(scan.errors);
        job.progress = TransferProgress {
            total_files: scan
                .entries
                .iter()
                .filter(|entry| entry.file_type == FileType::File)
                .count() as u64,
            total_bytes: scan
                .entries
                .iter()
                .filter(|entry| entry.file_type == FileType::File)
                .map(|entry| entry.size)
                .sum(),
            symlinks_skipped: scan.symlinks_skipped,
            current_concurrency: job.config.concurrency,
            ..TransferProgress::default()
        };
        let file_records = scan
            .entries
            .iter()
            .filter(|entry| entry.file_type == FileType::File)
            .map(|entry| {
                Ok(JobFileRecord {
                    job_id: job.id,
                    path: entry.relative_path.clone(),
                    size: entry.size,
                    status: JobFileStatus::Pending,
                    bytes_transferred: 0,
                    error: None,
                    updated_at: now_millis()?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        self.database.replace_job_files(job.id, &file_records)?;
        self.persist_job(job, progress)?;

        wait_until_active(&cancellation, &mut pause).await?;
        let authenticated = tokio::select! {
            _ = cancellation.cancelled() => return Err(TransferError::Cancelled),
            connection = self.connect_authenticated(peer_address, job.peer_id) => connection?,
        };
        if !authenticated.peer.trusted {
            return Err(TransferError::UntrustedPeer {
                device_id: authenticated.peer.device_id,
            });
        }
        job.peer_id = Some(authenticated.peer.device_id);
        job.status = JobStatus::Transferring;
        self.persist_job(job, progress)?;

        let destination_root = job.destination_root.to_str().ok_or_else(|| {
            TransferError::InvalidData("destination root is not valid UTF-8".to_owned())
        })?;
        let (manifest_id, decisions) = self
            .compare_manifest(
                &authenticated.connection,
                job,
                destination_root,
                &scan.entries,
                &cancellation,
                &mut pause,
            )
            .await?;

        let mut failed_paths = HashSet::new();
        let mut to_hash = Vec::new();
        for entry in scan
            .entries
            .iter()
            .filter(|entry| entry.file_type == FileType::Directory)
        {
            let action = decisions.get(&entry.relative_path).ok_or_else(|| {
                TransferError::InvalidData(format!(
                    "receiver omitted comparison result for {:?}",
                    entry.relative_path
                ))
            })?;
            if *action != CompareAction::Unchanged {
                job.errors.push(FileError::new(
                    Some(entry.relative_path.clone()),
                    FileOperation::Write,
                    format!("receiver reported {action:?} for directory"),
                ));
            }
        }
        for entry in scan
            .entries
            .iter()
            .filter(|entry| entry.file_type == FileType::File)
        {
            let action = decisions.get(&entry.relative_path).ok_or_else(|| {
                TransferError::InvalidData(format!(
                    "receiver omitted comparison result for {:?}",
                    entry.relative_path
                ))
            })?;
            match action {
                CompareAction::Unchanged => {
                    job.progress.completed_files += 1;
                    job.progress.skipped_files += 1;
                    job.progress.reused_bytes =
                        job.progress.reused_bytes.saturating_add(entry.size);
                    self.database.update_job_file_status(
                        job.id,
                        &entry.relative_path,
                        JobFileStatus::Skipped,
                        None,
                    )?;
                }
                CompareAction::NeedHash | CompareAction::Transfer => to_hash.push(entry.clone()),
                CompareAction::Conflict | CompareAction::Delete => {
                    record_file_failure(
                        job,
                        &mut failed_paths,
                        &entry.relative_path,
                        FileOperation::Write,
                        format!("receiver reported {action:?}"),
                    );
                    self.database.update_job_file_status(
                        job.id,
                        &entry.relative_path,
                        JobFileStatus::Failed,
                        Some("destination conflict"),
                    )?;
                }
            }
        }
        self.persist_job(job, progress)?;

        let hashed_files = self
            .hash_source_files(
                job,
                to_hash,
                &cancellation,
                &pause,
                &mut failed_paths,
                progress,
            )
            .await?;
        let (plans, negotiated_files) = self
            .negotiate_files(
                &authenticated.connection,
                job,
                manifest_id,
                hashed_files,
                &cancellation,
                &mut pause,
                &mut failed_paths,
                progress,
            )
            .await?;

        let mut queued_chunks = 0_u64;
        for file in negotiated_files.values() {
            let plan = plans.get(&file.entry.relative_path).ok_or_else(|| {
                TransferError::InvalidData(format!(
                    "receiver omitted file plan for {:?}",
                    file.entry.relative_path
                ))
            })?;
            validate_file_plan(file, plan)?;
            job.progress.reused_bytes = job
                .progress
                .reused_bytes
                .saturating_add(plan.resumed_bytes)
                .saturating_add(plan.reused_bytes);
            queued_chunks = queued_chunks
                .checked_add(u64::try_from(plan.missing_chunks.len()).map_err(|_| {
                    TransferError::InvalidData("chunk queue length is out of range".to_owned())
                })?)
                .ok_or_else(|| {
                    TransferError::InvalidData("chunk queue length overflow".to_owned())
                })?;
        }
        job.progress.queued_chunks = queued_chunks;
        self.persist_job(job, progress)?;

        self.upload_chunks(
            &authenticated.connection,
            job,
            manifest_id,
            &negotiated_files,
            &plans,
            &cancellation,
            &pause,
            &mut failed_paths,
            progress,
        )
        .await?;

        job.status = JobStatus::Verifying;
        self.persist_job(job, progress)?;
        for (path, file) in &negotiated_files {
            if failed_paths.contains(path) {
                continue;
            }
            wait_until_active(&cancellation, &mut pause).await?;
            let source = safe_join(&job.source_root, path).map_err(|error| {
                TransferError::InvalidData(format!("invalid source path {path:?}: {error}"))
            })?;
            let source_check = if job.config.verification_mode == VerificationMode::Verified {
                verify_source_snapshot(source, file.as_ref(), &cancellation).await
            } else {
                source_unchanged(source, file.entry.clone()).await
            };
            if let Err(error) = source_check {
                record_file_failure(
                    job,
                    &mut failed_paths,
                    path,
                    FileOperation::Verify,
                    error.to_string(),
                );
                self.database.update_job_file_status(
                    job.id,
                    path,
                    JobFileStatus::Failed,
                    Some(&error.to_string()),
                )?;
                self.persist_job(job, progress)?;
                continue;
            }
            let response = cancellable_rpc(
                &authenticated.connection,
                Request::FinalizeFile(FinalizeFileRequest {
                    job_id: job.id,
                    manifest_id,
                    file: file.as_ref().clone(),
                }),
                &cancellation,
            )
            .await;
            match response {
                Ok(Response::Acknowledged(Acknowledgement::File(acknowledgement)))
                    if acknowledgement.job_id == job.id
                        && acknowledgement.manifest_id == manifest_id
                        && acknowledgement.path == *path =>
                {
                    job.progress.completed_files += 1;
                    let plan = plans.get(path).ok_or_else(|| {
                        TransferError::InvalidData(format!("missing plan for {path:?}"))
                    })?;
                    if plan.missing_chunks.is_empty() {
                        job.progress.skipped_files += 1;
                        self.database.update_job_file_status(
                            job.id,
                            path,
                            JobFileStatus::Skipped,
                            None,
                        )?;
                    } else {
                        job.progress.transferred_files += 1;
                        self.database.update_job_file_status(
                            job.id,
                            path,
                            JobFileStatus::Completed,
                            None,
                        )?;
                    }
                    self.database
                        .update_job_file_progress(job.id, path, file.entry.size)?;
                }
                Ok(other) => {
                    let error = TransferError::InvalidData(format!(
                        "unexpected finalize response for {path:?}: {other:?}"
                    ));
                    record_file_failure(
                        job,
                        &mut failed_paths,
                        path,
                        FileOperation::Finalize,
                        error.to_string(),
                    );
                    self.database.update_job_file_status(
                        job.id,
                        path,
                        JobFileStatus::Failed,
                        Some(&error.to_string()),
                    )?;
                }
                Err(error) if error.is_retryable() => return Err(error),
                Err(error) => {
                    record_file_failure(
                        job,
                        &mut failed_paths,
                        path,
                        FileOperation::Finalize,
                        error.to_string(),
                    );
                    self.database.update_job_file_status(
                        job.id,
                        path,
                        JobFileStatus::Failed,
                        Some(&error.to_string()),
                    )?;
                }
            }
            self.persist_job(job, progress)?;
        }

        wait_until_active(&cancellation, &mut pause).await?;
        let response = cancellable_rpc(
            &authenticated.connection,
            Request::CompleteJob(CompleteJobRequest {
                job_id: job.id,
                manifest_id,
            }),
            &cancellation,
        )
        .await?;
        let receiver_failed_files = match response {
            Response::Acknowledged(Acknowledgement::Job(acknowledgement))
                if acknowledgement.job_id == job.id
                    && acknowledgement.manifest_id == manifest_id =>
            {
                if acknowledgement.completed_with_errors {
                    acknowledgement.failed_files.max(1)
                } else {
                    0
                }
            }
            other => {
                return Err(TransferError::InvalidData(format!(
                    "unexpected complete-job response: {other:?}"
                )));
            }
        };
        if receiver_failed_files != 0 && job.errors.is_empty() {
            job.errors.push(FileError::new(
                None,
                FileOperation::Finalize,
                format!("receiver reported {receiver_failed_files} failed files"),
            ));
            job.progress.failed_files = job.progress.failed_files.max(receiver_failed_files);
        }

        job.progress.active_chunk_streams = 0;
        job.progress.active_file_streams = 0;
        job.progress.queued_chunks = 0;
        job.progress.current_file = None;
        job.status = if job.errors.is_empty() {
            JobStatus::Completed
        } else {
            JobStatus::CompletedWithErrors
        };
        self.persist_job(job, progress)?;
        authenticated
            .connection
            .close(quinn::VarInt::from_u32(0), b"transfer complete");
        Ok(())
    }

    async fn compare_manifest(
        &self,
        connection: &quinn::Connection,
        job: &TransferJob,
        destination_root: &str,
        entries: &[ManifestEntry],
        cancellation: &CancellationToken,
        pause: &mut watch::Receiver<bool>,
    ) -> Result<(uuid::Uuid, HashMap<String, CompareAction>)> {
        let batch_count = entries.len().div_ceil(COMPARE_BATCH_SIZE).max(1);
        let manifest_id = uuid::Uuid::new_v4();
        let mut decisions = HashMap::new();
        for sequence in 0..batch_count {
            wait_until_active(cancellation, pause).await?;
            let start = sequence * COMPARE_BATCH_SIZE;
            let end = (start + COMPARE_BATCH_SIZE).min(entries.len());
            let batch_entries = if start < end {
                entries[start..end].to_vec()
            } else {
                Vec::new()
            };
            let sequence_u32 = u32::try_from(sequence).map_err(|_| {
                TransferError::InvalidData("manifest has too many batches".to_owned())
            })?;
            let response = cancellable_rpc(
                connection,
                Request::Compare(CompareBatch {
                    job_id: job.id,
                    manifest_id,
                    destination_root: destination_root.to_owned(),
                    verification_mode: job.config.verification_mode,
                    chunk_size: job.config.chunk_size,
                    sequence: sequence_u32,
                    is_last: sequence + 1 == batch_count,
                    entries: batch_entries.clone(),
                }),
                cancellation,
            )
            .await?;
            let Response::Compare(response) = response else {
                return Err(TransferError::InvalidData(
                    "receiver returned the wrong comparison response".to_owned(),
                ));
            };
            if response.job_id != job.id
                || response.manifest_id != manifest_id
                || response.sequence != sequence_u32
                || response.is_last != (sequence + 1 == batch_count)
                || response.decisions.len() != batch_entries.len()
            {
                return Err(TransferError::InvalidData(
                    "receiver returned a mismatched comparison batch".to_owned(),
                ));
            }
            let expected: HashSet<_> = batch_entries
                .iter()
                .map(|entry| entry.relative_path.as_str())
                .collect();
            for decision in response.decisions {
                if !expected.contains(decision.path.as_str())
                    || decisions
                        .insert(decision.path.clone(), decision.action)
                        .is_some()
                {
                    return Err(TransferError::InvalidData(format!(
                        "receiver returned an unexpected comparison path {:?}",
                        decision.path
                    )));
                }
            }
        }
        Ok((manifest_id, decisions))
    }

    #[allow(clippy::too_many_arguments)]
    async fn hash_source_files(
        &self,
        job: &mut TransferJob,
        entries: Vec<ManifestEntry>,
        cancellation: &CancellationToken,
        pause: &watch::Receiver<bool>,
        failed_paths: &mut HashSet<String>,
        progress: Option<&watch::Sender<TransferProgress>>,
    ) -> Result<Vec<HashedFile>> {
        let concurrency = usize::try_from(job.config.concurrency).unwrap_or(1).max(1);
        let hash_started = Instant::now();
        let entry_count = entries.len();
        let mut hashed_bytes = 0_u64;
        let mut processed_entries = 0_usize;
        job.progress.active_file_streams =
            u32::try_from(entry_count.min(concurrency)).unwrap_or(u32::MAX);
        let source_root = job.source_root.clone();
        let chunk_size = job.config.chunk_size;
        let use_cache = job.config.verification_mode != VerificationMode::Verified;
        let database = self.database.clone();
        let semaphore = Arc::clone(&self.hash_semaphore);
        let work = stream::iter(entries.into_iter().map(move |entry| {
            let database = database.clone();
            let semaphore = Arc::clone(&semaphore);
            let source_root = source_root.clone();
            let cancellation = cancellation.clone();
            let mut pause = (*pause).clone();
            async move {
                let path_name = entry.relative_path.clone();
                let result = async {
                    wait_until_active(&cancellation, &mut pause).await?;
                    let path = safe_join(source_root, &entry.relative_path).map_err(|error| {
                        TransferError::InvalidData(format!(
                            "invalid source path {:?}: {error}",
                            entry.relative_path
                        ))
                    })?;
                    let hashing = hash_file_cached(
                        database,
                        semaphore,
                        path,
                        entry,
                        chunk_size,
                        use_cache,
                        cancellation.clone(),
                    );
                    tokio::select! {
                        _ = cancellation.cancelled() => Err(TransferError::Cancelled),
                        result = hashing => result,
                    }
                }
                .await;
                (path_name, result)
            }
        }))
        .buffer_unordered(concurrency);
        tokio::pin!(work);

        let mut hashed = Vec::new();
        while let Some((path, result)) = work.next().await {
            processed_entries = processed_entries.saturating_add(1);
            match result {
                Ok(file) => {
                    hashed_bytes = hashed_bytes.saturating_add(file.entry.size);
                    hashed.push(file);
                }
                Err(TransferError::Cancelled) => return Err(TransferError::Cancelled),
                Err(error) => {
                    record_file_failure(
                        job,
                        failed_paths,
                        &path,
                        FileOperation::Verify,
                        error.to_string(),
                    );
                    self.database.update_job_file_status(
                        job.id,
                        &path,
                        JobFileStatus::Failed,
                        Some(&error.to_string()),
                    )?;
                }
            }
            let elapsed = hash_started.elapsed().as_secs_f64();
            if elapsed > 0.0 {
                job.progress.hash_bytes_per_second =
                    (hashed_bytes as f64 / elapsed).min(u64::MAX as f64) as u64;
            }
            job.progress.active_file_streams = u32::try_from(
                entry_count
                    .saturating_sub(processed_entries)
                    .min(concurrency),
            )
            .unwrap_or(u32::MAX);
            self.persist_job(job, progress)?;
        }
        job.progress.active_file_streams = 0;
        hashed.sort_unstable_by(|left, right| {
            left.entry.relative_path.cmp(&right.entry.relative_path)
        });
        Ok(hashed)
    }

    #[allow(clippy::too_many_arguments)]
    async fn negotiate_files(
        &self,
        connection: &quinn::Connection,
        job: &mut TransferJob,
        manifest_id: uuid::Uuid,
        files: Vec<HashedFile>,
        cancellation: &CancellationToken,
        pause: &mut watch::Receiver<bool>,
        failed_paths: &mut HashSet<String>,
        progress: Option<&watch::Sender<TransferProgress>>,
    ) -> Result<(HashMap<String, FilePlan>, HashMap<String, Arc<HashedFile>>)> {
        let mut encodable = Vec::new();
        for file in files {
            validate_hashed_file(&file, job.config.chunk_size)?;
            let probe = RequestFrame::new(Request::Negotiate(HashedFileBatch {
                job_id: job.id,
                manifest_id,
                sequence: 0,
                is_last: true,
                files: vec![file.clone()],
            }));
            if let Err(error) = encode_frame(&probe) {
                let path = file.entry.relative_path.clone();
                record_file_failure(
                    job,
                    failed_paths,
                    &path,
                    FileOperation::Verify,
                    format!("hashed file descriptor is too large for the protocol: {error}"),
                );
                self.database.update_job_file_status(
                    job.id,
                    &path,
                    JobFileStatus::Failed,
                    Some("hashed descriptor exceeds protocol frame limit"),
                )?;
            } else {
                encodable.push(file);
            }
        }

        let batches = hash_batches(job.id, manifest_id, encodable)?;
        let batch_count = batches.len();
        let mut plans = HashMap::new();
        let mut negotiated = HashMap::new();
        for (sequence, batch) in batches.into_iter().enumerate() {
            let mut pending = batch;
            while !pending.is_empty() {
                wait_until_active(cancellation, pause).await?;
                let sequence_u32 = u32::try_from(sequence).map_err(|_| {
                    TransferError::InvalidData("hash manifest has too many batches".to_owned())
                })?;
                let response = cancellable_rpc(
                    connection,
                    Request::Negotiate(HashedFileBatch {
                        job_id: job.id,
                        manifest_id,
                        sequence: sequence_u32,
                        is_last: sequence + 1 == batch_count,
                        files: pending.clone(),
                    }),
                    cancellation,
                )
                .await;
                let response = match response {
                    Ok(response) => response,
                    Err(error) if !error.is_retryable() => {
                        let Some(path) = error.remote_path().map(ToOwned::to_owned) else {
                            return Err(error);
                        };
                        let Some(position) = pending
                            .iter()
                            .position(|file| file.entry.relative_path == path)
                        else {
                            return Err(error);
                        };
                        pending.remove(position);
                        record_file_failure(
                            job,
                            failed_paths,
                            &path,
                            FileOperation::Write,
                            error.to_string(),
                        );
                        self.database.update_job_file_status(
                            job.id,
                            &path,
                            JobFileStatus::Failed,
                            Some(&error.to_string()),
                        )?;
                        self.persist_job(job, progress)?;
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                let Response::Negotiate(response) = response else {
                    return Err(TransferError::InvalidData(
                        "receiver returned the wrong negotiation response".to_owned(),
                    ));
                };
                if response.job_id != job.id
                    || response.manifest_id != manifest_id
                    || response.sequence != sequence_u32
                    || response.plans.len() != pending.len()
                {
                    return Err(TransferError::InvalidData(
                        "receiver returned a mismatched negotiation batch".to_owned(),
                    ));
                }
                let pending_by_path: HashMap<_, _> = pending
                    .drain(..)
                    .map(|file| (file.entry.relative_path.clone(), Arc::new(file)))
                    .collect();
                for plan in response.plans {
                    let file = pending_by_path.get(&plan.path).ok_or_else(|| {
                        TransferError::InvalidData(format!(
                            "receiver planned unexpected file {:?}",
                            plan.path
                        ))
                    })?;
                    validate_file_plan(file, &plan)?;
                    if plans.insert(plan.path.clone(), plan.clone()).is_some() {
                        return Err(TransferError::InvalidData(format!(
                            "receiver returned duplicate plan for {:?}",
                            plan.path
                        )));
                    }
                    negotiated.insert(plan.path.clone(), Arc::clone(file));
                }
            }
        }
        self.persist_job(job, progress)?;
        Ok((plans, negotiated))
    }

    #[allow(clippy::too_many_arguments)]
    async fn upload_chunks(
        &self,
        connection: &quinn::Connection,
        job: &mut TransferJob,
        manifest_id: uuid::Uuid,
        files: &HashMap<String, Arc<HashedFile>>,
        plans: &HashMap<String, FilePlan>,
        cancellation: &CancellationToken,
        pause: &watch::Receiver<bool>,
        failed_paths: &mut HashSet<String>,
        progress: Option<&watch::Sender<TransferProgress>>,
    ) -> Result<()> {
        let concurrency = usize::try_from(job.config.concurrency).unwrap_or(1).max(1);
        let source_root = job.source_root.clone();
        let job_id = job.id;
        let terminal_paths = Arc::new(DashSet::new());
        let upload_started = Instant::now();
        let mut phase_bytes = 0_u64;
        let initial_active = job
            .progress
            .queued_chunks
            .min(u64::from(job.config.concurrency));
        job.progress.active_chunk_streams = u32::try_from(initial_active).unwrap_or(u32::MAX);
        job.progress.active_file_streams = job.progress.active_chunk_streams;
        self.persist_job(job, progress)?;
        let work = stream::iter(upload_work(files, plans).map(|(path, work)| {
            let connection = connection.clone();
            let cancellation = cancellation.clone();
            let mut pause = (*pause).clone();
            let source_root = source_root.clone();
            let terminal_paths = Arc::clone(&terminal_paths);
            let file_size = work.as_ref().map_or(0, |work| work.file.entry.size);
            let chunk_size = work.as_ref().map_or(0, |work| work.chunk.size);
            async move {
                let result = async {
                    if terminal_paths.contains(&path) {
                        return Ok(false);
                    }
                    let work = work?;
                    wait_until_active(&cancellation, &mut pause).await?;
                    let upload = upload_chunk(
                        &connection,
                        job_id,
                        manifest_id,
                        &source_root,
                        &work.file,
                        work.chunk,
                    );
                    tokio::select! {
                        _ = cancellation.cancelled() => {
                            connection.close(quinn::VarInt::from_u32(1), b"transfer cancelled");
                            Err(TransferError::Cancelled)
                        },
                        result = upload => result.map(|()| true),
                    }
                }
                .await;
                (path, file_size, chunk_size, result)
            }
        }))
        .buffer_unordered(concurrency);
        tokio::pin!(work);

        let mut transferred_by_file: HashMap<String, u64> = HashMap::new();
        while let Some((path, file_size, chunk_size, result)) = work.next().await {
            job.progress.queued_chunks = job.progress.queued_chunks.saturating_sub(1);
            match result {
                Ok(true) => {
                    phase_bytes = phase_bytes.saturating_add(chunk_size);
                    job.progress.transferred_bytes =
                        job.progress.transferred_bytes.saturating_add(chunk_size);
                    let transferred = transferred_by_file.entry(path.clone()).or_default();
                    *transferred = transferred.saturating_add(chunk_size);
                    self.database
                        .update_job_file_progress(job.id, &path, *transferred)?;
                }
                Ok(false) => {}
                Err(TransferError::Cancelled) => return Err(TransferError::Cancelled),
                Err(error) if error.is_retryable() => return Err(error),
                Err(error) => {
                    terminal_paths.insert(path.clone());
                    record_file_failure(
                        job,
                        failed_paths,
                        &path,
                        FileOperation::Read,
                        error.to_string(),
                    );
                    self.database.update_job_file_status(
                        job.id,
                        &path,
                        JobFileStatus::Failed,
                        Some(&error.to_string()),
                    )?;
                }
            }
            let elapsed = upload_started.elapsed().as_secs_f64();
            if elapsed > 0.0 {
                let rate = (phase_bytes as f64 / elapsed).min(u64::MAX as f64) as u64;
                job.progress.network_bytes_per_second = rate;
                job.progress.read_bytes_per_second = rate;
                job.progress.write_bytes_per_second = rate;
            }
            let active = job
                .progress
                .queued_chunks
                .min(u64::from(job.config.concurrency));
            job.progress.active_chunk_streams = u32::try_from(active).unwrap_or(u32::MAX);
            job.progress.active_file_streams = job.progress.active_chunk_streams;
            job.progress.current_file = Some(FileProgress {
                relative_path: path.clone(),
                size: file_size,
                transferred: transferred_by_file.get(&path).copied().unwrap_or(0),
            });
            self.persist_job(job, progress)?;
        }
        job.progress.active_chunk_streams = 0;
        job.progress.active_file_streams = 0;
        Ok(())
    }

    fn persist_job(
        &self,
        job: &TransferJob,
        progress: Option<&watch::Sender<TransferProgress>>,
    ) -> Result<()> {
        self.database.upsert_job(job)?;
        publish_progress(progress, &job.progress);
        Ok(())
    }
}

fn upload_work<'a>(
    files: &'a HashMap<String, Arc<HashedFile>>,
    plans: &'a HashMap<String, FilePlan>,
) -> impl Iterator<Item = (String, Result<UploadWork>)> + 'a {
    plans.iter().flat_map(move |(path, plan)| {
        let file = files.get(path).cloned();
        plan.missing_chunks.iter().map(move |index| {
            let work = file
                .clone()
                .ok_or_else(|| {
                    TransferError::InvalidData(format!(
                        "missing negotiated descriptor for {path:?}"
                    ))
                })
                .and_then(|file| {
                    let position = usize::try_from(*index).map_err(|_| {
                        TransferError::InvalidData(format!(
                            "chunk index {index} for {path:?} is out of range"
                        ))
                    })?;
                    let chunk = file.chunks.get(position).copied().ok_or_else(|| {
                        TransferError::InvalidData(format!(
                            "receiver requested unknown chunk {index} for {path:?}"
                        ))
                    })?;
                    Ok(UploadWork { file, chunk })
                });
            (path.clone(), work)
        })
    })
}

async fn upload_chunk(
    connection: &quinn::Connection,
    job_id: uuid::Uuid,
    manifest_id: uuid::Uuid,
    source_root: &Path,
    file: &HashedFile,
    chunk: ChunkDescriptor,
) -> Result<()> {
    let source = safe_join(source_root, &file.entry.relative_path).map_err(|error| {
        TransferError::InvalidData(format!(
            "invalid source path {:?}: {error}",
            file.entry.relative_path
        ))
    })?;
    source_unchanged(source.clone(), file.entry.clone()).await?;
    let mut input = File::open(&source)
        .await
        .map_err(|error| TransferError::io("opening source file", &source, error))?;
    input
        .seek(SeekFrom::Start(chunk.offset))
        .await
        .map_err(|error| TransferError::io("seeking source file", &source, error))?;
    let (mut send, mut receive) = connection
        .open_bi()
        .await
        .map_err(|error| TransferError::Network(error.to_string()))?;
    write_frame(
        &mut send,
        &RequestFrame::new(Request::UploadChunk(ChunkHeader {
            job_id,
            manifest_id,
            path: file.entry.relative_path.clone(),
            index: chunk.index,
            offset: chunk.offset,
            size: chunk.size,
            hash: chunk.hash,
        })),
    )
    .await?;

    let mut remaining = chunk.size;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0_u8; TRANSFER_BUFFER_SIZE];
    while remaining != 0 {
        let wanted = usize::try_from(remaining.min(TRANSFER_BUFFER_SIZE as u64))
            .map_err(|_| TransferError::InvalidData("chunk size is out of range".to_owned()))?;
        let read = input
            .read(&mut buffer[..wanted])
            .await
            .map_err(|error| TransferError::io("reading source file", &source, error))?;
        if read == 0 {
            return Err(TransferError::InvalidData(format!(
                "source {:?} ended while reading chunk {}",
                file.entry.relative_path, chunk.index
            )));
        }
        hasher.update(&buffer[..read]);
        send.write_all(&buffer[..read])
            .await
            .map_err(|error| TransferError::Network(error.to_string()))?;
        remaining -= read as u64;
    }
    send.finish()
        .map_err(|error| TransferError::Network(error.to_string()))?;
    let outgoing_hash = *hasher.finalize().as_bytes();
    let response: ResponseFrame = read_frame(&mut receive).await?;
    let response = response.into_payload().map_err(TransferError::from)?;
    if outgoing_hash != chunk.hash {
        return Err(TransferError::InvalidData(format!(
            "source {:?} changed while reading chunk {}",
            file.entry.relative_path, chunk.index
        )));
    }
    match response {
        Response::Acknowledged(Acknowledgement::Chunk(acknowledgement))
            if acknowledgement.job_id == job_id
                && acknowledgement.manifest_id == manifest_id
                && acknowledgement.path == file.entry.relative_path
                && acknowledgement.index == chunk.index
                && acknowledgement.size == chunk.size =>
        {
            Ok(())
        }
        Response::Error(error) => Err(error.into()),
        other => Err(TransferError::InvalidData(format!(
            "unexpected chunk acknowledgement: {other:?}"
        ))),
    }
}

async fn cancellable_rpc(
    connection: &quinn::Connection,
    request: Request,
    cancellation: &CancellationToken,
) -> Result<Response> {
    tokio::select! {
        _ = cancellation.cancelled() => {
            connection.close(quinn::VarInt::from_u32(1), b"transfer cancelled");
            Err(TransferError::Cancelled)
        },
        response = rpc(connection, request) => response,
    }
}

async fn source_unchanged(path: PathBuf, entry: ManifestEntry) -> Result<()> {
    tokio::task::spawn_blocking(move || ensure_source_unchanged(path, &entry.source_metadata()))
        .await
        .map_err(|error| TransferError::Task(error.to_string()))??;
    Ok(())
}

async fn verify_source_snapshot(
    path: PathBuf,
    file: &HashedFile,
    cancellation: &CancellationToken,
) -> Result<()> {
    source_unchanged(path.clone(), file.entry.clone()).await?;
    let actual = hash_region_cancellable(&path, 0, file.entry.size, cancellation).await?;
    source_unchanged(path.clone(), file.entry.clone()).await?;
    if actual != file.hash {
        return Err(fastsync_core::CoreError::HashMismatch {
            path,
            expected: file.hash,
            actual,
        }
        .into());
    }
    Ok(())
}

async fn wait_until_active(
    cancellation: &CancellationToken,
    pause: &mut watch::Receiver<bool>,
) -> Result<()> {
    loop {
        if cancellation.is_cancelled() {
            return Err(TransferError::Cancelled);
        }
        if !*pause.borrow() {
            return Ok(());
        }
        tokio::select! {
            _ = cancellation.cancelled() => return Err(TransferError::Cancelled),
            changed = pause.changed() => {
                if changed.is_err() {
                    return Ok(());
                }
            }
        }
    }
}

fn validate_config(job: &TransferJob) -> Result<()> {
    if job.config.chunk_size == 0 || usize::try_from(job.config.chunk_size).is_err() {
        return Err(TransferError::InvalidData(format!(
            "invalid chunk size {}",
            job.config.chunk_size
        )));
    }
    if job.config.concurrency == 0 {
        return Err(TransferError::InvalidData(
            "transfer concurrency must be non-zero".to_owned(),
        ));
    }
    if !is_receiver_absolute_path(&job.destination_root) {
        return Err(TransferError::InvalidData(format!(
            "destination root `{}` must be absolute",
            job.destination_root.display()
        )));
    }
    Ok(())
}

fn hash_batches(
    job_id: uuid::Uuid,
    manifest_id: uuid::Uuid,
    files: Vec<HashedFile>,
) -> Result<Vec<Vec<HashedFile>>> {
    let mut batches = Vec::new();
    let mut current = Vec::new();
    for file in files {
        if current.len() == HASH_BATCH_SIZE {
            batches.push(std::mem::take(&mut current));
        }
        let mut candidate = current.clone();
        candidate.push(file.clone());
        let frame = RequestFrame::new(Request::Negotiate(HashedFileBatch {
            job_id,
            manifest_id,
            sequence: 0,
            is_last: false,
            files: candidate,
        }));
        match encode_frame(&frame) {
            Ok(_) => current.push(file),
            Err(fastsync_protocol::ProtocolError::FrameTooLarge { .. }) if !current.is_empty() => {
                batches.push(std::mem::take(&mut current));
                current.push(file);
            }
            Err(error) => return Err(error.into()),
        }
    }
    if !current.is_empty() {
        batches.push(current);
    }
    Ok(batches)
}

fn validate_file_plan(file: &HashedFile, plan: &FilePlan) -> Result<()> {
    if plan.path != file.entry.relative_path {
        return Err(TransferError::InvalidData(format!(
            "file plan path {:?} does not match {:?}",
            plan.path, file.entry.relative_path
        )));
    }
    let mut indices = HashSet::new();
    let mut totals = [0_u64; 3];
    for (category, values) in [
        (0_usize, &plan.missing_chunks),
        (1_usize, &plan.resumed_chunks),
        (2_usize, &plan.reused_chunks),
    ] {
        for index in values {
            let position = usize::try_from(*index).map_err(|_| {
                TransferError::InvalidData(format!("chunk index {index} is out of range"))
            })?;
            let chunk = file
                .chunks
                .get(position)
                .filter(|chunk| chunk.index == *index)
                .ok_or_else(|| {
                    TransferError::InvalidData(format!(
                        "file plan for {:?} contains unknown chunk {index}",
                        plan.path
                    ))
                })?;
            if !indices.insert(*index) {
                return Err(TransferError::InvalidData(format!(
                    "file plan for {:?} repeats chunk {index}",
                    plan.path
                )));
            }
            totals[category] = totals[category].checked_add(chunk.size).ok_or_else(|| {
                TransferError::InvalidData("file plan byte count overflow".to_owned())
            })?;
        }
    }
    if indices.len() != file.chunks.len()
        || totals[0] != plan.missing_bytes
        || totals[1] != plan.resumed_bytes
        || totals[2] != plan.reused_bytes
    {
        return Err(TransferError::InvalidData(format!(
            "file plan totals for {:?} are inconsistent",
            plan.path
        )));
    }
    Ok(())
}

fn record_file_failure(
    job: &mut TransferJob,
    failed_paths: &mut HashSet<String>,
    path: &str,
    operation: FileOperation,
    message: String,
) {
    if failed_paths.insert(path.to_owned()) {
        job.progress.failed_files += 1;
        job.errors
            .push(FileError::new(Some(path.to_owned()), operation, message));
    }
}

fn publish_progress(sender: Option<&watch::Sender<TransferProgress>>, progress: &TransferProgress) {
    if let Some(sender) = sender {
        sender.send_replace(progress.clone());
    }
}
