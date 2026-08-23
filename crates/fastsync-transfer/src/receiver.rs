use std::collections::HashSet;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use dashmap::{DashMap, mapref::entry::Entry};
use fastsync_core::{
    FileType, HashedFile, JobStatus, TransferConfig, TransferJob, TransferProgress,
    VerificationMode, metadata_matches,
};
use fastsync_protocol::{
    Acknowledgement, ChunkAcknowledgement, ChunkHeader, CompareAction, CompareBatch,
    CompareDecision, CompareDecisionBatch, CompleteJobRequest, FileAcknowledgement, FilePlan,
    FilePlanBatch, FinalizeFileRequest, HashedFileBatch, JobAcknowledgement, MAX_FRAME_SIZE,
    PingRequest, PongResponse, RemoteError, RemoteErrorCode, Request, RequestFrame, Response,
    ResponseFrame, read_frame_with_limit, write_frame,
};
use fastsync_storage::{CompletedChunk, JobFileRecord, JobFileStatus, JobRecord, StoredJobStatus};
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio_util::sync::CancellationToken;

use crate::TRANSFER_BUFFER_SIZE;
use crate::engine::{PeerInfo, TransferEngine};
use crate::error::{Result, TransferError};
use crate::filesystem::{
    apply_directory, copy_verified_chunk, create_manifest_directory, destination_candidate_path,
    destination_file_path, destination_lease_key, ensure_destination_root, existing_manifest,
    finalize_file, hash_region_cancellable, prepare_partial, staging_file_path,
};
use crate::hashing::{hash_file_cached, now_millis, validate_hashed_file};
use crate::state::{
    CompletedManifest, DestinationLease, IncomingFile, IncomingJob, IncomingJobKey,
};

const SERVER_JOB_CONCURRENCY: u32 = 32;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);
const REQUEST_HEADER_TIMEOUT: Duration = Duration::from_secs(30);
const CHUNK_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

impl TransferEngine {
    /// Runs the QUIC accept loop until cancellation or endpoint shutdown.
    pub async fn serve(&self, cancellation: CancellationToken) -> Result<()> {
        loop {
            let connection_permit = tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                permit = Arc::clone(&self.connection_semaphore).acquire_owned() => {
                    permit.map_err(|_| TransferError::Task("connection semaphore was closed".to_owned()))?
                }
            };
            let incoming = tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                incoming = self.endpoint.accept() => incoming,
            };
            let Some(incoming) = incoming else {
                return Ok(());
            };
            let engine = self.clone();
            let connection_cancellation = cancellation.clone();
            tokio::spawn(async move {
                let _connection_permit = connection_permit;
                if let Err(error) = engine
                    .handle_incoming_connection(incoming, connection_cancellation)
                    .await
                {
                    if !matches!(error, TransferError::Cancelled) {
                        tracing::debug!(%error, "FastSync QUIC connection ended");
                    }
                }
            });
        }
    }

    pub async fn accept_loop(&self, cancellation: CancellationToken) -> Result<()> {
        self.serve(cancellation).await
    }

    pub async fn run_server(&self, cancellation: CancellationToken) -> Result<()> {
        self.serve(cancellation).await
    }

    async fn handle_incoming_connection(
        &self,
        incoming: quinn::Incoming,
        cancellation: CancellationToken,
    ) -> Result<()> {
        let connection = tokio::select! {
            _ = cancellation.cancelled() => return Err(TransferError::Cancelled),
            connection = incoming => connection.map_err(|error| TransferError::Network(error.to_string()))?,
        };
        let stream = tokio::select! {
            _ = cancellation.cancelled() => return Err(TransferError::Cancelled),
            stream = tokio::time::timeout(HANDSHAKE_TIMEOUT, connection.accept_bi()) => {
                stream
                    .map_err(|_| TransferError::Network("waiting for handshake stream timed out".to_owned()))?
                    .map_err(|error| TransferError::Network(error.to_string()))?
            },
        };
        let (send, receive) = stream;
        let peer = tokio::time::timeout(
            HANDSHAKE_TIMEOUT,
            self.accept_handshake(&connection, send, receive),
        )
        .await
        .map_err(|_| TransferError::Network("handshake timed out".to_owned()))??;

        let connection_id = uuid::Uuid::new_v4();
        self.active_connections
            .insert((peer.device_id, connection_id), connection.clone());
        let connection_jobs = Arc::new(DashMap::new());
        let result = self
            .handle_authenticated_connection(
                &connection,
                peer.clone(),
                cancellation,
                Arc::clone(&connection_jobs),
            )
            .await;
        self.active_connections
            .remove(&(peer.device_id, connection_id));
        if let Err(error) = self.connection_closed(&connection_jobs).await {
            tracing::warn!(peer_id = %peer.device_id, %error, "cannot release abandoned incoming jobs");
        }
        result
    }

    async fn handle_authenticated_connection(
        &self,
        connection: &quinn::Connection,
        peer: PeerInfo,
        cancellation: CancellationToken,
        connection_jobs: Arc<DashMap<IncomingJobKey, ()>>,
    ) -> Result<()> {
        let mut requests = tokio::task::JoinSet::new();
        let request_cancellation = cancellation.child_token();
        let result = 'accept: loop {
            let stream = tokio::select! {
                _ = cancellation.cancelled() => {
                    connection.close(quinn::VarInt::from_u32(0), b"server shutting down");
                    break Ok(());
                }
                stream = connection.accept_bi() => stream,
                completed = requests.join_next(), if !requests.is_empty() => {
                    if let Some(Err(error)) = completed {
                        tracing::debug!(%error, "FastSync request task failed");
                    }
                    continue;
                }
            };
            let (send, receive) = match stream {
                Ok(stream) => stream,
                Err(
                    quinn::ConnectionError::ApplicationClosed(_)
                    | quinn::ConnectionError::LocallyClosed,
                ) => break Ok(()),
                Err(error) => break Err(TransferError::Network(error.to_string())),
            };
            let request_permit = tokio::select! {
                _ = cancellation.cancelled() => {
                    connection.close(quinn::VarInt::from_u32(0), b"server shutting down");
                    break 'accept Ok(());
                },
                permit = Arc::clone(&self.request_semaphore).acquire_owned() => {
                    match permit {
                        Ok(permit) => permit,
                        Err(_) => break 'accept Err(TransferError::Task("request semaphore was closed".to_owned())),
                    }
                }
            };
            let engine = self.clone();
            let peer = peer.clone();
            let stream_cancellation = request_cancellation.clone();
            let connection_jobs = Arc::clone(&connection_jobs);
            requests.spawn(async move {
                let _request_permit = request_permit;
                if let Err(error) = engine
                    .handle_request_stream(
                        peer,
                        send,
                        receive,
                        stream_cancellation,
                        connection_jobs,
                    )
                    .await
                {
                    tracing::debug!(%error, "FastSync request stream failed");
                }
            });
        };
        request_cancellation.cancel();
        while let Some(completed) = requests.join_next().await {
            if let Err(error) = completed {
                tracing::debug!(%error, "FastSync request task failed while draining connection");
            }
        }
        result
    }

    async fn handle_request_stream(
        &self,
        peer: PeerInfo,
        mut send: quinn::SendStream,
        mut receive: quinn::RecvStream,
        cancellation: CancellationToken,
        connection_jobs: Arc<DashMap<IncomingJobKey, ()>>,
    ) -> Result<()> {
        let frame_limit = if peer.trusted {
            MAX_FRAME_SIZE
        } else {
            64 * 1024
        };
        let frame: RequestFrame = match tokio::time::timeout(
            REQUEST_HEADER_TIMEOUT,
            read_frame_with_limit(&mut receive, frame_limit),
        )
        .await
        {
            Ok(Ok(frame)) => frame,
            Ok(Err(error)) => return Err(error.into()),
            Err(_) => {
                return Err(TransferError::Network(
                    "request header timed out".to_owned(),
                ));
            }
        };
        if let Err(error) = frame.validate() {
            let response = Response::Error(RemoteError {
                code: RemoteErrorCode::UnsupportedProtocol,
                message: error.to_string(),
                retryable: false,
                job_id: None,
                path: None,
            });
            return write_response(&mut send, response).await;
        }
        let request = frame.payload;

        if !matches!(&request, Request::Ping(_)) {
            match self.validate_trusted_key(peer.device_id, &peer.public_key) {
                Ok(true) => {}
                Ok(false) => {
                    return write_response(
                        &mut send,
                        Response::Error(RemoteError {
                            code: RemoteErrorCode::Unauthorized,
                            message: format!(
                                "device {} is authenticated but not trusted",
                                peer.device_id
                            ),
                            retryable: false,
                            job_id: request_job_id(&request),
                            path: request_path(&request),
                        }),
                    )
                    .await;
                }
                Err(error) => {
                    return write_response(
                        &mut send,
                        Response::Error(remote_error(
                            error,
                            request_job_id(&request),
                            request_path(&request),
                        )),
                    )
                    .await;
                }
            }
        }

        let job_id = request_job_id(&request);
        let path = request_path(&request);
        let job_key = job_id.map(|job_id| IncomingJobKey {
            peer_id: peer.device_id,
            job_id,
        });
        let newly_registered = if let Some(key) = job_key {
            self.register_job_connection(key, &connection_jobs).await
        } else {
            false
        };
        let processed = if cancellation.is_cancelled() {
            Err(TransferError::Cancelled)
        } else {
            self.process_request(&peer, request, &mut receive, &cancellation)
                .await
        };
        if processed.is_err()
            && newly_registered
            && let Some(key) = job_key
        {
            connection_jobs.remove(&key);
            let failed_request = DashMap::new();
            failed_request.insert(key, ());
            if let Err(error) = self.connection_closed(&failed_request).await {
                tracing::warn!(job_id = %key.job_id, %error, "cannot retire failed incoming request");
            }
        }
        let response = match processed {
            Ok(response) => response,
            Err(error) => Response::Error(remote_error(error, job_id, path)),
        };
        write_response(&mut send, response).await
    }

    async fn process_request(
        &self,
        peer: &PeerInfo,
        request: Request,
        receive: &mut quinn::RecvStream,
        cancellation: &CancellationToken,
    ) -> Result<Response> {
        match request {
            Request::Ping(PingRequest { nonce }) => Ok(Response::Pong(PongResponse { nonce })),
            Request::Compare(batch) => self.process_compare(peer, batch).await,
            Request::Negotiate(batch) => self.process_negotiate(peer, batch, cancellation).await,
            Request::UploadChunk(header) => {
                self.process_upload(peer, header, receive, cancellation)
                    .await
            }
            Request::FinalizeFile(request) => {
                self.process_finalize(peer, request, cancellation).await
            }
            Request::CompleteJob(request) => self.process_complete(peer, request).await,
        }
    }

    async fn process_compare(&self, peer: &PeerInfo, batch: CompareBatch) -> Result<Response> {
        validate_chunk_size(batch.chunk_size)?;
        let requested_destination_root = PathBuf::from(&batch.destination_root);
        let key = IncomingJobKey {
            peer_id: peer.device_id,
            job_id: batch.job_id,
        };
        let previously_committed = self
            .database
            .get_string(&manifest_setting_key(key))?
            .and_then(|value| uuid::Uuid::parse_str(&value).ok());
        if previously_committed == Some(batch.manifest_id) {
            return Err(TransferError::InvalidData(format!(
                "manifest ID {} was already committed for job {}",
                batch.manifest_id, batch.job_id
            )));
        }
        let root_lifecycle = self.destination_root_lifecycle.lock().await;
        if let Some(existing) = self.incoming_jobs.get(&key)
            && (destination_lease_key(&existing.requested_destination_root)
                != destination_lease_key(&requested_destination_root)
                || existing.verification_mode != batch.verification_mode
                || existing.chunk_size != batch.chunk_size)
        {
            return Err(TransferError::InvalidData(format!(
                "job {} parameters changed during transfer",
                batch.job_id
            )));
        }
        self.ensure_destination_available(key, &requested_destination_root)?;
        let destination_root = ensure_destination_root(&requested_destination_root).await?;
        let staging_root = self.staging_root(&destination_root, key)?;
        let candidate_state = Arc::new(IncomingJob::new(
            key,
            requested_destination_root.clone(),
            destination_root.clone(),
            staging_root,
            batch.verification_mode,
            batch.chunk_size,
        ));
        let (state, inserted) = match self.incoming_jobs.entry(key) {
            Entry::Occupied(existing) => (Arc::clone(existing.get()), false),
            Entry::Vacant(entry) => {
                entry.insert(Arc::clone(&candidate_state));
                (candidate_state, true)
            }
        };
        if let Err(error) = self.acquire_destination_lease(&state, &destination_root, "") {
            if inserted {
                self.discard_incoming_state(&state);
            }
            return Err(error);
        }
        drop(root_lifecycle);
        if let Err(error) =
            state.validate_parameters(&destination_root, batch.verification_mode, batch.chunk_size)
        {
            if inserted {
                self.discard_incoming_state(&state);
            }
            return Err(error);
        }
        let _operation = match state.begin_exclusive_operation().await {
            Ok(operation) => operation,
            Err(error) => {
                if inserted {
                    self.discard_incoming_state(&state);
                }
                return Err(error);
            }
        };
        if let Err(error) = self.ensure_receiver_job(&state).await {
            if inserted {
                self.discard_incoming_state(&state);
            }
            return Err(error);
        }
        let _compare = state.compare_lock.lock().await;
        state.begin_manifest_batch(batch.manifest_id, batch.sequence)?;

        let mut decisions = Vec::with_capacity(batch.entries.len());
        let mut file_records = Vec::new();
        for entry in batch.entries {
            let path = entry.relative_path.clone();
            state.validate_manifest_path(&path)?;
            state.record_manifest_entry(&entry)?;
            let candidate = destination_candidate_path(&destination_root, &path)?;
            self.acquire_destination_lease(&state, &candidate, &path)?;
            let decision = match entry.file_type {
                FileType::Directory => {
                    match create_manifest_directory(&destination_root, &entry).await {
                        Ok(_) => {
                            state.remember_directory(entry.clone())?;
                            CompareAction::Unchanged
                        }
                        Err(error) => {
                            tracing::warn!(path = %path, %error, "cannot create destination directory");
                            state.remember_directory_failure(path.clone(), error.to_string())?;
                            CompareAction::Conflict
                        }
                    }
                }
                FileType::File => {
                    let destination = match destination_file_path(&destination_root, &path).await {
                        Ok(destination) => destination,
                        Err(error) => {
                            file_records.push(job_file_record(
                                batch.job_id,
                                &path,
                                entry.size,
                                JobFileStatus::Failed,
                                Some(error.to_string()),
                            )?);
                            decisions.push(CompareDecision {
                                path,
                                action: CompareAction::Conflict,
                            });
                            continue;
                        }
                    };
                    match existing_manifest(destination_root.clone(), destination).await {
                        Ok(Some(actual)) if actual.file_type != FileType::File => {
                            CompareAction::Conflict
                        }
                        Ok(Some(actual))
                            if batch.verification_mode == VerificationMode::Fast
                                && metadata_matches(&entry, &actual) =>
                        {
                            CompareAction::Unchanged
                        }
                        Ok(_) => CompareAction::NeedHash,
                        Err(error) => {
                            tracing::warn!(path = %path, %error, "cannot compare destination file");
                            CompareAction::Conflict
                        }
                    }
                }
            };
            if entry.file_type == FileType::File {
                let (status, error) = match &decision {
                    CompareAction::Unchanged => (JobFileStatus::Skipped, None),
                    CompareAction::Conflict => (
                        JobFileStatus::Failed,
                        Some("destination conflict".to_owned()),
                    ),
                    _ => (JobFileStatus::Pending, None),
                };
                file_records.push(job_file_record(
                    batch.job_id,
                    &path,
                    entry.size,
                    status,
                    error,
                )?);
            }
            decisions.push(CompareDecision {
                path,
                action: decision,
            });
        }
        if !file_records.is_empty() {
            self.database.upsert_job_files(&file_records)?;
        }
        if let Some(manifest) =
            state.finish_manifest_batch(batch.manifest_id, batch.sequence, batch.is_last)?
        {
            self.reconcile_manifest(&state, &manifest).await?;
            self.database.set_string(
                &manifest_setting_key(state.key),
                &batch.manifest_id.to_string(),
            )?;
            state.commit_manifest(batch.manifest_id, manifest.entries)?;
        }
        Ok(Response::Compare(CompareDecisionBatch {
            job_id: batch.job_id,
            manifest_id: batch.manifest_id,
            sequence: batch.sequence,
            is_last: batch.is_last,
            decisions,
        }))
    }

    async fn process_negotiate(
        &self,
        peer: &PeerInfo,
        batch: HashedFileBatch,
        cancellation: &CancellationToken,
    ) -> Result<Response> {
        let state = self.incoming_state(peer.device_id, batch.job_id)?;
        let _operation = state.begin_exclusive_operation().await?;
        state.validate_manifest_id(batch.manifest_id)?;
        let mut plans = Vec::with_capacity(batch.files.len());
        for file in batch.files {
            let path = file.entry.relative_path.clone();
            validate_hashed_file(&file, state.chunk_size).map_err(|error| {
                contextual_remote(
                    error,
                    batch.job_id,
                    path.clone(),
                    RemoteErrorCode::InvalidRequest,
                )
            })?;
            state.validate_negotiated_entry(&file.entry)?;
            let plan = self
                .prepare_incoming_file(&state, file, cancellation)
                .await
                .map_err(|error| {
                    contextual_remote(error, batch.job_id, path, RemoteErrorCode::Storage)
                })?;
            plans.push(plan);
        }
        Ok(Response::Negotiate(FilePlanBatch {
            job_id: batch.job_id,
            manifest_id: batch.manifest_id,
            sequence: batch.sequence,
            is_last: batch.is_last,
            plans,
        }))
    }

    async fn prepare_incoming_file(
        &self,
        state: &Arc<IncomingJob>,
        file: HashedFile,
        cancellation: &CancellationToken,
    ) -> Result<FilePlan> {
        let path = file.entry.relative_path.clone();
        let candidate = destination_candidate_path(&state.destination_root, &path)?;
        self.ensure_destination_lease(state, &candidate, &path)?;
        let destination = destination_file_path(&state.destination_root, &path).await?;
        let staging = staging_file_path(&state.staging_root, &path).await?;
        let existing = state
            .files
            .get(&path)
            .map(|existing| Arc::clone(existing.value()));
        if let Some(existing) = &existing {
            if existing.file != file
                || existing.destination != destination
                || existing.staging != staging
            {
                return Err(TransferError::InvalidData(format!(
                    "negotiated content for {path:?} changed"
                )));
            }
            if !existing.inflight_chunks.is_empty() {
                return Err(TransferError::Network(format!(
                    "prior uploads for {path:?} have not stopped yet"
                )));
            }
            if existing.is_finalized() {
                let metadata = fs::symlink_metadata(&destination).await.map_err(|error| {
                    TransferError::io("checking finalized destination", &destination, error)
                })?;
                if metadata.file_type().is_symlink()
                    || !metadata.is_file()
                    || metadata.len() != file.entry.size
                    || hash_region_cancellable(&destination, 0, file.entry.size, cancellation)
                        .await?
                        != file.hash
                {
                    return Err(TransferError::InvalidData(format!(
                        "previously finalized destination for {path:?} changed"
                    )));
                }
                self.database.upsert_job_file(&job_file_record(
                    state.key.job_id,
                    &path,
                    file.entry.size,
                    JobFileStatus::Running,
                    None,
                )?)?;
                return Ok(FilePlan {
                    path,
                    missing_chunks: Vec::new(),
                    missing_bytes: 0,
                    resumed_chunks: Vec::new(),
                    resumed_bytes: 0,
                    reused_chunks: file.chunks.iter().map(|chunk| chunk.index).collect(),
                    reused_bytes: file.entry.size,
                });
            }
        }

        let actual = existing_manifest(state.destination_root.clone(), destination.clone()).await?;
        if let Some(actual) = &actual {
            if actual.file_type != FileType::File {
                return Err(TransferError::InvalidData(format!(
                    "destination `{}` is not a regular file",
                    destination.display()
                )));
            }
        }

        let existing_hash =
            if let Some(actual) = actual.filter(|actual| actual.size == file.entry.size) {
                Some(
                    hash_file_cached(
                        self.database.clone(),
                        Arc::clone(&self.hash_semaphore),
                        destination.clone(),
                        actual,
                        state.chunk_size,
                        state.verification_mode != VerificationMode::Verified,
                        cancellation.clone(),
                    )
                    .await?,
                )
            } else {
                None
            };

        // Identical content needs neither network traffic nor a full local copy into a partial.
        if existing_hash
            .as_ref()
            .is_some_and(|actual| actual.hash == file.hash)
        {
            apply_directory(destination.clone(), file.entry.clone()).await?;
            let incoming = Arc::new(IncomingFile::new(file.clone(), destination, staging));
            incoming.mark_finalized();
            state.files.insert(path.clone(), incoming);
            self.database.upsert_job_file(&job_file_record(
                state.key.job_id,
                &path,
                file.entry.size,
                JobFileStatus::Running,
                None,
            )?)?;
            return Ok(FilePlan {
                path,
                missing_chunks: Vec::new(),
                missing_bytes: 0,
                resumed_chunks: Vec::new(),
                resumed_bytes: 0,
                reused_chunks: file.chunks.iter().map(|chunk| chunk.index).collect(),
                reused_bytes: file.entry.size,
            });
        }

        let preserved_partial = prepare_partial(staging.clone(), file.entry.size).await?;
        if !preserved_partial {
            self.database
                .clear_completed_chunks(state.key.job_id, &path)?;
        }

        let mut resumed = HashSet::new();
        let completed =
            self.database
                .completed_chunk_indices(state.key.job_id, &path, &file.hash)?;
        for index in completed {
            if cancellation.is_cancelled() {
                return Err(TransferError::Cancelled);
            }
            let Some(chunk) = usize::try_from(index)
                .ok()
                .and_then(|position| file.chunks.get(position))
                .filter(|chunk| chunk.index == index)
            else {
                self.database
                    .remove_completed_chunk(state.key.job_id, &path, index, &file.hash)?;
                continue;
            };
            match hash_region_cancellable(&staging, chunk.offset, chunk.size, cancellation).await {
                Ok(hash) if hash == chunk.hash => {
                    resumed.insert(index);
                }
                _ => {
                    self.database.remove_completed_chunk(
                        state.key.job_id,
                        &path,
                        index,
                        &file.hash,
                    )?;
                }
            }
        }

        let mut reused = HashSet::new();
        if let Some(existing_hash) = existing_hash {
            for (expected, available) in file.chunks.iter().zip(existing_hash.chunks.iter()) {
                if cancellation.is_cancelled() {
                    return Err(TransferError::Cancelled);
                }
                if resumed.contains(&expected.index) || expected.hash != available.hash {
                    continue;
                }
                let copied = tokio::select! {
                    _ = cancellation.cancelled() => return Err(TransferError::Cancelled),
                    copied = copy_verified_chunk(&destination, &staging, expected) => copied?,
                };
                if copied {
                    reused.insert(expected.index);
                }
            }
        }

        let completed_at = now_millis()?;
        let reused_records: Vec<_> = reused
            .iter()
            .map(|index| CompletedChunk {
                job_id: state.key.job_id,
                path: path.clone(),
                index: *index,
                source_hash: file.hash,
                completed_at,
            })
            .collect();
        if !reused_records.is_empty() {
            self.database.record_completed_chunks(&reused_records)?;
        }

        let mut missing_chunks = Vec::new();
        let mut resumed_chunks = Vec::new();
        let mut reused_chunks = Vec::new();
        let mut missing_bytes = 0_u64;
        let mut resumed_bytes = 0_u64;
        let mut reused_bytes = 0_u64;
        for chunk in &file.chunks {
            if resumed.contains(&chunk.index) {
                resumed_chunks.push(chunk.index);
                resumed_bytes = resumed_bytes.saturating_add(chunk.size);
            } else if reused.contains(&chunk.index) {
                reused_chunks.push(chunk.index);
                reused_bytes = reused_bytes.saturating_add(chunk.size);
            } else {
                missing_chunks.push(chunk.index);
                missing_bytes = missing_bytes.saturating_add(chunk.size);
            }
        }

        if existing.is_none() {
            state.files.insert(
                path.clone(),
                Arc::new(IncomingFile::new(file.clone(), destination, staging)),
            );
        }
        self.database.upsert_job_file(&job_file_record(
            state.key.job_id,
            &path,
            file.entry.size,
            JobFileStatus::Running,
            None,
        )?)?;
        Ok(FilePlan {
            path,
            missing_chunks,
            missing_bytes,
            resumed_chunks,
            resumed_bytes,
            reused_chunks,
            reused_bytes,
        })
    }

    async fn process_upload(
        &self,
        peer: &PeerInfo,
        header: ChunkHeader,
        receive: &mut quinn::RecvStream,
        cancellation: &CancellationToken,
    ) -> Result<Response> {
        let state = self.incoming_state(peer.device_id, header.job_id)?;
        let _job_operation = state.begin_operation().await?;
        state.validate_manifest_id(header.manifest_id)?;
        let incoming = state
            .files
            .get(&header.path)
            .map(|file| Arc::clone(file.value()))
            .ok_or_else(|| {
                TransferError::InvalidData(format!("file {:?} was not negotiated", header.path))
            })?;
        self.ensure_destination_lease(&state, &incoming.destination, &header.path)?;
        let _file_operation = incoming.operation_lock.read().await;
        if incoming.is_finalized() {
            return Err(TransferError::InvalidData(format!(
                "file {:?} is already finalized",
                header.path
            )));
        }
        let descriptor_index = usize::try_from(header.index).map_err(|_| {
            TransferError::InvalidData(format!("chunk index {} is out of range", header.index))
        })?;
        let descriptor = incoming
            .file
            .chunks
            .get(descriptor_index)
            .filter(|chunk| {
                chunk.index == header.index
                    && chunk.offset == header.offset
                    && chunk.size == header.size
                    && chunk.hash == header.hash
            })
            .copied()
            .ok_or_else(|| {
                TransferError::InvalidData(format!(
                    "chunk {} for {:?} does not match its negotiated descriptor",
                    header.index, header.path
                ))
            })?;
        if incoming.inflight_chunks.insert(header.index, ()).is_some() {
            return Err(TransferError::InvalidData(format!(
                "chunk {} for {:?} is already being uploaded",
                header.index, header.path
            )));
        }

        let result = self
            .receive_chunk(&state, &incoming, descriptor, receive, cancellation)
            .await;
        incoming.inflight_chunks.remove(&header.index);
        result?;
        Ok(Response::Acknowledged(Acknowledgement::Chunk(
            ChunkAcknowledgement {
                job_id: header.job_id,
                manifest_id: header.manifest_id,
                path: header.path,
                index: header.index,
                size: header.size,
            },
        )))
    }

    async fn receive_chunk(
        &self,
        state: &IncomingJob,
        incoming: &IncomingFile,
        descriptor: fastsync_core::ChunkDescriptor,
        receive: &mut quinn::RecvStream,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        let file = &incoming.file;
        let partial = &incoming.staging;
        let metadata = fs::symlink_metadata(&partial)
            .await
            .map_err(|error| TransferError::io("checking partial file", &partial, error))?;
        if metadata.file_type().is_symlink()
            || !metadata.is_file()
            || metadata.len() != file.entry.size
        {
            return Err(TransferError::InvalidData(format!(
                "partial file `{}` is missing or has the wrong size",
                partial.display()
            )));
        }
        let mut output = OpenOptions::new()
            .write(true)
            .open(partial)
            .await
            .map_err(|error| TransferError::io("opening partial file", partial, error))?;
        output
            .seek(SeekFrom::Start(descriptor.offset))
            .await
            .map_err(|error| TransferError::io("seeking partial file", partial, error))?;

        let mut remaining = descriptor.size;
        let mut hasher = blake3::Hasher::new();
        let mut buffer = vec![0_u8; TRANSFER_BUFFER_SIZE];
        while remaining != 0 {
            let wanted = usize::try_from(remaining.min(TRANSFER_BUFFER_SIZE as u64))
                .map_err(|_| TransferError::InvalidData("chunk size is out of range".to_owned()))?;
            let read = tokio::select! {
                _ = cancellation.cancelled() => return Err(TransferError::Cancelled),
                read = tokio::time::timeout(
                    CHUNK_IDLE_TIMEOUT,
                    AsyncReadExt::read(receive, &mut buffer[..wanted]),
                ) => read
                    .map_err(|_| TransferError::Network("chunk body read timed out".to_owned()))?
                    .map_err(|error| TransferError::Network(error.to_string()))?,
            };
            if read == 0 {
                return Err(TransferError::Network(format!(
                    "chunk {} for {:?} ended with {remaining} bytes missing",
                    descriptor.index, file.entry.relative_path
                )));
            }
            hasher.update(&buffer[..read]);
            tokio::select! {
                _ = cancellation.cancelled() => return Err(TransferError::Cancelled),
                written = output.write_all(&buffer[..read]) => {
                    written.map_err(|error| TransferError::io("writing partial file", partial, error))?;
                }
            }
            remaining -= read as u64;
        }
        let mut extra = [0_u8; 1];
        let trailing = tokio::select! {
            _ = cancellation.cancelled() => return Err(TransferError::Cancelled),
            read = tokio::time::timeout(CHUNK_IDLE_TIMEOUT, AsyncReadExt::read(receive, &mut extra)) => {
                read
                    .map_err(|_| TransferError::Network("waiting for chunk end timed out".to_owned()))?
                    .map_err(|error| TransferError::Network(error.to_string()))?
            }
        };
        if trailing != 0 {
            return Err(TransferError::InvalidData(format!(
                "chunk {} for {:?} contains trailing bytes",
                descriptor.index, file.entry.relative_path
            )));
        }
        let actual_hash = *hasher.finalize().as_bytes();
        if actual_hash != descriptor.hash {
            return Err(TransferError::InvalidData(format!(
                "chunk {} hash mismatch for {:?}",
                descriptor.index, file.entry.relative_path
            )));
        }
        tokio::select! {
            _ = cancellation.cancelled() => return Err(TransferError::Cancelled),
            flushed = output.flush() => {
                flushed.map_err(|error| TransferError::io("flushing partial file", partial, error))?;
            }
        }
        self.database.record_completed_chunk(&CompletedChunk {
            job_id: state.key.job_id,
            path: file.entry.relative_path.clone(),
            index: descriptor.index,
            source_hash: file.hash,
            completed_at: now_millis()?,
        })?;
        Ok(())
    }

    async fn process_finalize(
        &self,
        peer: &PeerInfo,
        request: FinalizeFileRequest,
        cancellation: &CancellationToken,
    ) -> Result<Response> {
        let state = self.incoming_state(peer.device_id, request.job_id)?;
        let _job_operation = state.begin_operation().await?;
        state.validate_manifest_id(request.manifest_id)?;
        validate_hashed_file(&request.file, state.chunk_size)?;
        let path = request.file.entry.relative_path.clone();
        let incoming = state
            .files
            .get(&path)
            .map(|file| Arc::clone(file.value()))
            .ok_or_else(|| {
                TransferError::InvalidData(format!("file {path:?} was not negotiated"))
            })?;
        if incoming.file != request.file {
            return Err(TransferError::InvalidData(format!(
                "finalized descriptor for {path:?} differs from negotiation"
            )));
        }
        self.ensure_destination_lease(&state, &incoming.destination, &path)?;
        let _file_operation = incoming.operation_lock.write().await;
        if !incoming.inflight_chunks.is_empty() {
            return Err(TransferError::InvalidData(format!(
                "cannot finalize {path:?} while chunks are still uploading"
            )));
        }
        let _verification_permit = tokio::select! {
            _ = cancellation.cancelled() => return Err(TransferError::Cancelled),
            permit = Arc::clone(&self.hash_semaphore).acquire_owned() => {
                permit.map_err(|_| TransferError::Task("hash semaphore was closed".to_owned()))?
            }
        };
        if !incoming.is_finalized() {
            for chunk in &request.file.chunks {
                if !self.database.is_chunk_completed(
                    request.job_id,
                    &path,
                    chunk.index,
                    &request.file.hash,
                )? {
                    return Err(TransferError::InvalidData(format!(
                        "chunk {} for {path:?} is incomplete",
                        chunk.index
                    )));
                }
            }
            let destination = &incoming.destination;
            let staging = &incoming.staging;
            match fs::symlink_metadata(staging).await {
                Ok(_) => {
                    finalize_file(
                        staging.clone(),
                        destination.clone(),
                        request.file.clone(),
                        cancellation.clone(),
                    )
                    .await?
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    let actual =
                        existing_manifest(state.destination_root.clone(), destination.clone())
                            .await?
                            .ok_or_else(|| {
                                TransferError::InvalidData(format!(
                                    "partial and destination file for {path:?} are missing"
                                ))
                            })?;
                    if !metadata_matches(&request.file.entry, &actual)
                        || hash_region_cancellable(
                            destination,
                            0,
                            request.file.entry.size,
                            cancellation,
                        )
                        .await?
                            != request.file.hash
                    {
                        return Err(TransferError::InvalidData(format!(
                            "partial file for {path:?} is missing"
                        )));
                    }
                }
                Err(source) => {
                    return Err(TransferError::io("checking staging file", staging, source));
                }
            }
            incoming.mark_finalized();
        } else {
            let actual =
                existing_manifest(state.destination_root.clone(), incoming.destination.clone())
                    .await?
                    .ok_or_else(|| {
                        TransferError::InvalidData(format!(
                            "reused destination for {path:?} disappeared before finalize"
                        ))
                    })?;
            if !metadata_matches(&request.file.entry, &actual)
                || hash_region_cancellable(
                    &incoming.destination,
                    0,
                    request.file.entry.size,
                    cancellation,
                )
                .await?
                    != request.file.hash
            {
                return Err(TransferError::InvalidData(format!(
                    "reused destination for {path:?} changed before finalize"
                )));
            }
        }
        self.database
            .clear_completed_chunks(request.job_id, &path)?;
        if !self.database.update_job_file_progress(
            request.job_id,
            &path,
            request.file.entry.size,
        )? {
            return Err(TransferError::InvalidData(format!(
                "job file row for {path:?} disappeared during finalize"
            )));
        }
        if !self.database.update_job_file_status(
            request.job_id,
            &path,
            JobFileStatus::Completed,
            None,
        )? {
            return Err(TransferError::InvalidData(format!(
                "job file row for {path:?} disappeared during finalize"
            )));
        }
        Ok(Response::Acknowledged(Acknowledgement::File(
            FileAcknowledgement {
                job_id: request.job_id,
                manifest_id: request.manifest_id,
                path,
            },
        )))
    }

    async fn process_complete(
        &self,
        peer: &PeerInfo,
        request: CompleteJobRequest,
    ) -> Result<Response> {
        let key = IncomingJobKey {
            peer_id: peer.device_id,
            job_id: request.job_id,
        };
        let completed_with_errors;
        let failed_files;
        let state = if let Some(state) = self.incoming_jobs.get(&key) {
            Some(Arc::clone(state.value()))
        } else {
            None
        };
        if let Some(state) = state {
            let completion = state.begin_completion().await?;
            state.validate_manifest_id(request.manifest_id)?;
            for directory in state.directories_deepest_first()? {
                let path =
                    fastsync_core::safe_join(&state.destination_root, &directory.relative_path)
                        .map_err(|error| TransferError::InvalidData(error.to_string()))?;
                self.ensure_destination_lease(&state, &path, &directory.relative_path)?;
                apply_directory(path, directory).await?;
            }
            let unfinished = self.database.list_job_files(request.job_id)?;
            for file in unfinished.iter().filter(|file| {
                matches!(file.status, JobFileStatus::Pending | JobFileStatus::Running)
            }) {
                self.database.update_job_file_status(
                    request.job_id,
                    &file.path,
                    JobFileStatus::Failed,
                    Some("source did not finalize file"),
                )?;
            }
            let files = self.database.list_job_files(request.job_id)?;
            let failed_file_count = files
                .iter()
                .filter(|file| file.status == JobFileStatus::Failed)
                .count() as u64;
            let directory_failure_count = state.directory_failure_count()?;
            let failed_count = failed_file_count.saturating_add(directory_failure_count);
            let failed = failed_count != 0;
            let progress = TransferProgress {
                total_files: files.len() as u64,
                completed_files: files
                    .iter()
                    .filter(|file| {
                        matches!(
                            file.status,
                            JobFileStatus::Completed | JobFileStatus::Skipped
                        )
                    })
                    .count() as u64,
                total_bytes: files.iter().map(|file| file.size).sum(),
                transferred_bytes: files.iter().map(|file| file.bytes_transferred).sum(),
                failed_files: failed_count,
                ..TransferProgress::default()
            };
            completed_with_errors = failed;
            failed_files = progress.failed_files;
            self.database.update_job_status_and_progress(
                request.job_id,
                if failed {
                    JobStatus::CompletedWithErrors
                } else {
                    JobStatus::Completed
                },
                &progress,
            )?;
            if !failed && let Err(error) = cleanup_staging_root(&state.staging_root).await {
                tracing::warn!(%error, job_id = %request.job_id, "cannot clean completed staging directory");
            }
            self.release_destination_leases(&state)?;
            self.incoming_jobs.remove(&key);
            completion.commit();
        } else {
            let completed = self.database.get_job(request.job_id)?.filter(|job| {
                job.job.peer_id == Some(peer.device_id)
                    && job.job.source_root.as_os_str().is_empty()
                    && matches!(
                        job.status,
                        StoredJobStatus::Completed | StoredJobStatus::CompletedWithErrors
                    )
            });
            let Some(completed) = completed else {
                return Err(TransferError::InvalidData(format!(
                    "job {} was not found",
                    request.job_id
                )));
            };
            let stored_manifest_id = self
                .database
                .get_string(&manifest_setting_key(key))?
                .and_then(|value| uuid::Uuid::parse_str(&value).ok());
            if stored_manifest_id != Some(request.manifest_id) {
                return Err(TransferError::InvalidData(format!(
                    "manifest {} is not the completed generation for job {}",
                    request.manifest_id, request.job_id
                )));
            }
            completed_with_errors = completed.status == StoredJobStatus::CompletedWithErrors;
            failed_files = completed.job.progress.failed_files;
        }
        Ok(Response::Acknowledged(Acknowledgement::Job(
            JobAcknowledgement {
                job_id: request.job_id,
                manifest_id: request.manifest_id,
                completed_with_errors,
                failed_files,
            },
        )))
    }

    async fn ensure_receiver_job(&self, state: &IncomingJob) -> Result<()> {
        let mut job = TransferJob::new(
            PathBuf::new(),
            state.destination_root.clone(),
            TransferConfig {
                verification_mode: state.verification_mode,
                chunk_size: state.chunk_size,
                concurrency: SERVER_JOB_CONCURRENCY,
                retry_limit: 0,
            },
        );
        job.id = state.key.job_id;
        job.peer_id = Some(state.key.peer_id);
        job.status = JobStatus::Transferring;
        if self.database.create_job_if_absent(&job)? {
            return Ok(());
        }
        let existing = self.database.get_job(state.key.job_id)?.ok_or_else(|| {
            TransferError::Task(format!(
                "job {} disappeared while validating receiver ownership",
                state.key.job_id
            ))
        })?;
        if !receiver_job_matches(&existing, state) {
            return Err(TransferError::InvalidData(format!(
                "job {} already exists with different peer or transfer parameters",
                state.key.job_id
            )));
        }
        self.database
            .update_job_status(state.key.job_id, JobStatus::Transferring)?;
        Ok(())
    }

    fn incoming_state(&self, peer_id: uuid::Uuid, job_id: uuid::Uuid) -> Result<Arc<IncomingJob>> {
        self.incoming_jobs
            .get(&IncomingJobKey { peer_id, job_id })
            .map(|state| Arc::clone(state.value()))
            .ok_or_else(|| TransferError::InvalidData(format!("job {job_id} was not found")))
    }

    fn discard_incoming_state(&self, state: &Arc<IncomingJob>) {
        if let Err(error) = self.release_destination_leases(state) {
            tracing::warn!(job_id = %state.key.job_id, %error, "cannot release discarded job leases");
        }
        match self.incoming_jobs.entry(state.key) {
            Entry::Occupied(entry) if Arc::ptr_eq(entry.get(), state) => {
                entry.remove();
            }
            Entry::Occupied(_) | Entry::Vacant(_) => {}
        }
    }

    fn ensure_destination_available(
        &self,
        owner: IncomingJobKey,
        destination: &std::path::Path,
    ) -> Result<()> {
        let _lifecycle = self.destination_lease_lifecycle.lock().map_err(|_| {
            TransferError::Task("destination lease lifecycle lock was poisoned".to_owned())
        })?;
        let normalized = PathBuf::from(destination_lease_key(destination));
        if let Some(existing) = self.destination_leases.iter().find(|existing| {
            existing.owner != owner
                && (normalized.starts_with(&existing.native_path)
                    || existing.native_path.starts_with(&normalized))
        }) {
            Err(TransferError::Network(format!(
                "destination root `{}` overlaps job {} path {:?}",
                destination.display(),
                existing.owner.job_id,
                existing.wire_path,
            )))
        } else {
            Ok(())
        }
    }

    fn staging_root(
        &self,
        destination_root: &std::path::Path,
        key: IncomingJobKey,
    ) -> Result<PathBuf> {
        let setting_key = format!("receiver_staging_token_v1:{}:{}", key.peer_id, key.job_id);
        let candidate = uuid::Uuid::new_v4().to_string();
        let stored = self
            .database
            .get_or_insert_string(&setting_key, &candidate)?;
        let token = uuid::Uuid::parse_str(&stored).map_err(|error| {
            TransferError::InvalidData(format!(
                "stored staging token for job {} is invalid: {error}",
                key.job_id
            ))
        })?;
        Ok(destination_root.join(format!(".fastsync-stage-{token}")))
    }

    fn acquire_destination_lease(
        &self,
        state: &IncomingJob,
        destination: &std::path::Path,
        wire_path: &str,
    ) -> Result<()> {
        let _lifecycle = self.destination_lease_lifecycle.lock().map_err(|_| {
            TransferError::Task("destination lease lifecycle lock was poisoned".to_owned())
        })?;
        let lease_key = destination_lease_key(destination);
        let lease_path = PathBuf::from(&lease_key);
        if let Some(existing) = self.destination_leases.iter().find(|existing| {
            existing.owner != state.key
                && (lease_path.starts_with(&existing.native_path)
                    || existing.native_path.starts_with(&lease_path))
        }) {
            return Err(TransferError::Network(format!(
                "destination `{}` overlaps job {} path {:?}",
                destination.display(),
                existing.owner.job_id,
                existing.wire_path,
            )));
        }
        match self.destination_leases.entry(lease_key.clone()) {
            Entry::Occupied(existing) if existing.get().owner != state.key => {
                return Err(TransferError::Network(format!(
                    "destination `{}` is busy in job {}",
                    destination.display(),
                    existing.get().owner.job_id,
                )));
            }
            Entry::Occupied(existing) if existing.get().wire_path != wire_path => {
                return Err(TransferError::InvalidData(format!(
                    "destination `{}` is already reserved by job {} for path {:?}",
                    destination.display(),
                    existing.get().owner.job_id,
                    existing.get().wire_path,
                )));
            }
            Entry::Occupied(_) => {}
            Entry::Vacant(entry) => {
                entry.insert(DestinationLease {
                    owner: state.key,
                    wire_path: wire_path.to_owned(),
                    native_path: lease_path,
                });
            }
        }
        state.remember_lease(lease_key)
    }

    fn ensure_destination_lease(
        &self,
        state: &IncomingJob,
        destination: &std::path::Path,
        wire_path: &str,
    ) -> Result<()> {
        let lease_key = destination_lease_key(destination);
        let valid = self
            .destination_leases
            .get(&lease_key)
            .is_some_and(|lease| lease.owner == state.key && lease.wire_path == wire_path);
        if valid {
            Ok(())
        } else {
            Err(TransferError::InvalidData(format!(
                "job {} no longer owns destination path {wire_path:?}",
                state.key.job_id
            )))
        }
    }

    fn release_destination_lease(&self, state: &IncomingJob, key: &str) -> Result<()> {
        let _lifecycle = self.destination_lease_lifecycle.lock().map_err(|_| {
            TransferError::Task("destination lease lifecycle lock was poisoned".to_owned())
        })?;
        match self.destination_leases.entry(key.to_owned()) {
            Entry::Occupied(entry) if entry.get().owner == state.key => {
                entry.remove();
            }
            Entry::Occupied(_) | Entry::Vacant(_) => {}
        }
        Ok(())
    }

    fn release_destination_leases(&self, state: &IncomingJob) -> Result<()> {
        for key in state.lease_keys()? {
            self.release_destination_lease(state, &key)?;
        }
        Ok(())
    }

    async fn register_job_connection(
        &self,
        key: IncomingJobKey,
        connection_jobs: &DashMap<IncomingJobKey, ()>,
    ) -> bool {
        let _lifecycle = self.peer_connection_lifecycle.lock().await;
        if connection_jobs.insert(key, ()).is_some() {
            return false;
        }
        match self.active_job_connections.entry(key) {
            Entry::Occupied(mut count) => {
                let next = count.get().saturating_add(1);
                *count.get_mut() = next;
            }
            Entry::Vacant(entry) => {
                entry.insert(1);
            }
        }
        true
    }

    async fn connection_closed(&self, connection_jobs: &DashMap<IncomingJobKey, ()>) -> Result<()> {
        let _lifecycle = self.peer_connection_lifecycle.lock().await;
        let mut abandoned = Vec::new();
        for key in connection_jobs.iter().map(|entry| *entry.key()) {
            match self.active_job_connections.entry(key) {
                Entry::Occupied(mut count) if *count.get() > 1 => {
                    *count.get_mut() -= 1;
                }
                Entry::Occupied(count) => {
                    count.remove();
                    abandoned.push(key);
                }
                Entry::Vacant(_) => {}
            }
        }
        let mut first_error = None;
        for key in abandoned {
            let result = self.retire_incoming_job(key).await;
            if let Err(error) = result {
                self.schedule_retirement_retry(key);
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    async fn retire_incoming_job(&self, key: IncomingJobKey) -> Result<()> {
        let Some(state) = self
            .incoming_jobs
            .get(&key)
            .map(|state| Arc::clone(state.value()))
        else {
            return Ok(());
        };
        let retirement = state.begin_completion().await?;
        let belongs_to_state = self
            .database
            .get_job(state.key.job_id)?
            .is_some_and(|job| receiver_job_matches(&job, &state));
        if !belongs_to_state {
            return Err(TransferError::InvalidData(format!(
                "receiver job {} no longer owns its database record",
                state.key.job_id
            )));
        }
        self.database.mark_job_interrupted(state.key.job_id)?;
        self.release_destination_leases(&state)?;
        match self.incoming_jobs.entry(state.key) {
            Entry::Occupied(entry) if Arc::ptr_eq(entry.get(), &state) => {
                entry.remove();
            }
            Entry::Occupied(_) | Entry::Vacant(_) => {}
        }
        retirement.commit();
        Ok(())
    }

    fn schedule_retirement_retry(&self, key: IncomingJobKey) {
        let engine = self.clone();
        tokio::spawn(async move {
            for delay in [1, 5, 30] {
                tokio::time::sleep(Duration::from_secs(delay)).await;
                let _lifecycle = engine.peer_connection_lifecycle.lock().await;
                if engine.active_job_connections.contains_key(&key) {
                    return;
                }
                match engine.retire_incoming_job(key).await {
                    Ok(()) => return,
                    Err(error) => {
                        tracing::warn!(job_id = %key.job_id, %error, "retrying abandoned job cleanup");
                    }
                }
            }
        });
    }

    async fn reconcile_manifest(
        &self,
        state: &IncomingJob,
        manifest: &CompletedManifest,
    ) -> Result<()> {
        let mut stale_paths = self
            .database
            .retain_job_files(state.key.job_id, &manifest.file_paths)?;
        stale_paths.extend(state.files.iter().map(|entry| entry.key().clone()));
        stale_paths.sort_unstable();
        stale_paths.dedup();

        for path in stale_paths {
            if let Some(file) = state.files.get(&path)
                && !file.inflight_chunks.is_empty()
            {
                return Err(TransferError::Network(format!(
                    "uploads for removed path {path:?} have not stopped yet"
                )));
            }
            let removed = state.files.remove(&path).map(|(_, file)| file);
            let destination = if let Some(file) = &removed {
                file.destination.clone()
            } else {
                fastsync_core::safe_join(&state.destination_root, &path)
                    .map_err(|error| TransferError::InvalidData(error.to_string()))?
            };
            let lease_key = destination_lease_key(&destination);
            if !manifest.all_paths.contains(&path) {
                self.release_destination_lease(state, &lease_key)?;
                state.forget_lease(&lease_key)?;
            }

            if manifest.file_paths.contains(&path) {
                continue;
            }

            let staging = if let Some(file) = removed {
                file.staging.clone()
            } else {
                fastsync_core::safe_join(&state.staging_root, &path)
                    .map_err(|error| TransferError::InvalidData(error.to_string()))?
            };
            match fs::symlink_metadata(&staging).await {
                Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
                    fs::remove_file(&staging).await.map_err(|error| {
                        TransferError::io("removing stale staging file", &staging, error)
                    })?;
                }
                Ok(_) => {
                    return Err(TransferError::InvalidData(format!(
                        "stale staging path `{}` is not a regular file",
                        staging.display()
                    )));
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(TransferError::io(
                        "checking stale staging file",
                        &staging,
                        error,
                    ));
                }
            }
        }
        for lease_key in state.lease_keys()? {
            let stale = self
                .destination_leases
                .get(&lease_key)
                .is_some_and(|lease| {
                    lease.owner == state.key
                        && !lease.wire_path.is_empty()
                        && !manifest.all_paths.contains(&lease.wire_path)
                });
            if stale {
                self.release_destination_lease(state, &lease_key)?;
                state.forget_lease(&lease_key)?;
            }
        }
        Ok(())
    }
}

fn receiver_job_matches(record: &JobRecord, state: &IncomingJob) -> bool {
    record.job.peer_id == Some(state.key.peer_id)
        && record.job.destination_root == state.destination_root
        && record.job.source_root.as_os_str().is_empty()
        && record.job.config.verification_mode == state.verification_mode
        && record.job.config.chunk_size == state.chunk_size
}

fn manifest_setting_key(key: IncomingJobKey) -> String {
    format!("receiver_manifest_id_v2:{}:{}", key.peer_id, key.job_id)
}

async fn cleanup_staging_root(root: &std::path::Path) -> Result<()> {
    match fs::symlink_metadata(root).await {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
            fs::remove_dir_all(root)
                .await
                .map_err(|error| TransferError::io("removing staging directory", root, error))
        }
        Ok(_) => Err(TransferError::InvalidData(format!(
            "staging root `{}` is not a real directory",
            root.display()
        ))),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(TransferError::io("checking staging directory", root, error)),
    }
}

async fn write_response(send: &mut quinn::SendStream, response: Response) -> Result<()> {
    write_frame(send, &ResponseFrame::new(response)).await?;
    send.finish()
        .map_err(|error| TransferError::Network(error.to_string()))?;
    Ok(())
}

fn validate_chunk_size(chunk_size: u64) -> Result<()> {
    if chunk_size == 0 || usize::try_from(chunk_size).is_err() {
        Err(TransferError::InvalidData(format!(
            "invalid chunk size {chunk_size}"
        )))
    } else {
        Ok(())
    }
}

fn job_file_record(
    job_id: uuid::Uuid,
    path: &str,
    size: u64,
    status: JobFileStatus,
    error: Option<String>,
) -> Result<JobFileRecord> {
    Ok(JobFileRecord {
        job_id,
        path: path.to_owned(),
        size,
        status,
        bytes_transferred: 0,
        error,
        updated_at: now_millis()?,
    })
}

fn request_job_id(request: &Request) -> Option<uuid::Uuid> {
    match request {
        Request::Compare(request) => Some(request.job_id),
        Request::Negotiate(request) => Some(request.job_id),
        Request::UploadChunk(request) => Some(request.job_id),
        Request::FinalizeFile(request) => Some(request.job_id),
        Request::CompleteJob(request) => Some(request.job_id),
        Request::Ping(_) => None,
    }
}

fn request_path(request: &Request) -> Option<String> {
    match request {
        Request::UploadChunk(request) => Some(request.path.clone()),
        Request::FinalizeFile(request) => Some(request.file.entry.relative_path.clone()),
        _ => None,
    }
}

fn contextual_remote(
    error: TransferError,
    job_id: uuid::Uuid,
    path: String,
    default_code: RemoteErrorCode,
) -> TransferError {
    if matches!(&error, TransferError::Remote { .. }) {
        return error;
    }
    let code = match &error {
        TransferError::Core(fastsync_core::CoreError::HashMismatch { .. }) => {
            RemoteErrorCode::HashMismatch
        }
        TransferError::InvalidData(_) => RemoteErrorCode::InvalidRequest,
        _ => default_code,
    };
    TransferError::Remote {
        code,
        message: error.to_string(),
        retryable: error.is_retryable(),
        job_id: Some(job_id),
        path: Some(path),
    }
}

fn remote_error(
    error: TransferError,
    job_id: Option<uuid::Uuid>,
    path: Option<String>,
) -> RemoteError {
    let error = match error {
        TransferError::Remote {
            code,
            message,
            retryable,
            job_id: remote_job_id,
            path: remote_path,
        } => {
            return RemoteError {
                code,
                message,
                retryable,
                job_id: remote_job_id.or(job_id),
                path: remote_path.or(path),
            };
        }
        error => error,
    };
    let code = match &error {
        TransferError::ProtocolVersionMismatch { .. } => RemoteErrorCode::UnsupportedProtocol,
        TransferError::UntrustedPeer { .. } | TransferError::Authentication(_) => {
            RemoteErrorCode::Unauthorized
        }
        TransferError::Storage(_) => RemoteErrorCode::Storage,
        TransferError::Core(fastsync_core::CoreError::HashMismatch { .. }) => {
            RemoteErrorCode::HashMismatch
        }
        TransferError::InvalidData(message) if message.contains("hash mismatch") => {
            RemoteErrorCode::HashMismatch
        }
        TransferError::InvalidData(message) if message.contains("not found") => {
            RemoteErrorCode::JobNotFound
        }
        TransferError::InvalidData(_) | TransferError::Protocol(_) => {
            RemoteErrorCode::InvalidRequest
        }
        _ => RemoteErrorCode::Internal,
    };
    RemoteError {
        code,
        message: error.to_string(),
        retryable: error.is_retryable(),
        job_id,
        path,
    }
}
