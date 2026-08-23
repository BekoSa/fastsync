use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::body::Body;
use axum::extract::rejection::JsonRejection;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::http::{Request, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use dashmap::DashMap;
use fastsync_core::{
    DEFAULT_CHUNK_SIZE, FileError, JobStatus, TransferConfig, TransferJob, TransferProgress,
    VerificationMode, is_receiver_absolute_path,
};
use fastsync_discovery::DiscoveredPeer;
use fastsync_protocol::{AGENT_VERSION, PROTOCOL_VERSION};
use fastsync_storage::{
    Database, DiscoveredDevice, JobFileStatus, JobRecord, StorageError, StoredJobStatus,
    TrustedPeer,
};
use fastsync_transfer::{PeerInfo, TransferEngine, device_id_from_public_key};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::net::lookup_host;
use tokio_util::sync::CancellationToken;
use tower_http::trace::TraceLayer;
use uuid::Uuid;

use crate::DEFAULT_QUIC_PORT;
use crate::events::{AppEvent, EventBus, unix_millis};
use crate::jobs::{JobManager, ManagerError};

const INDEX_HTML: &str = include_str!("../../../web/index.html");
const MIN_CHUNK_SIZE: u64 = 1024 * 1024;
const MAX_CHUNK_SIZE: u64 = 1024 * 1024 * 1024;
const MAX_CONCURRENCY: u32 = 64;
const MAX_RETRY_LIMIT: u32 = 20;
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

pub const AGENT_FEATURES: &[&str] = &[
    "blake3",
    "fixed-chunk-resume",
    "tls-certificate-binding",
    "http-api",
    "websocket-events",
];

#[derive(Clone)]
pub struct AppState {
    pub database: Database,
    pub engine: TransferEngine,
    pub jobs: JobManager,
    pub events: EventBus,
    observed_peers: Arc<DashMap<Uuid, ObservedPeer>>,
    online_ttl: Duration,
    http_address: SocketAddr,
    quic_address: SocketAddr,
    data_directory: PathBuf,
    shutdown: CancellationToken,
}

#[derive(Clone, Debug)]
struct ObservedPeer {
    http_address: Option<SocketAddr>,
    protocol_version: u16,
    agent_version: String,
    features: Vec<String>,
}

impl AppState {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        database: Database,
        engine: TransferEngine,
        jobs: JobManager,
        events: EventBus,
        online_ttl: Duration,
        http_address: SocketAddr,
        quic_address: SocketAddr,
        data_directory: PathBuf,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            database,
            engine,
            jobs,
            events,
            observed_peers: Arc::new(DashMap::new()),
            online_ttl,
            http_address,
            quic_address,
            data_directory,
            shutdown,
        }
    }

    pub fn record_discovery(&self, peer: DiscoveredPeer) -> Result<(), StorageError> {
        let now = unix_millis();
        let device_id = peer.announcement.device_id;
        self.database.upsert_discovered_device(&DiscoveredDevice {
            device_id,
            name: peer.announcement.device_name.clone(),
            public_key: None,
            address: peer.transfer_addr.to_string(),
            first_seen: now,
            last_seen: now,
        })?;
        self.observed_peers.insert(
            device_id,
            ObservedPeer {
                http_address: Some(peer.http_addr),
                protocol_version: peer.announcement.protocol_version,
                agent_version: peer.announcement.agent_version,
                features: peer.announcement.features,
            },
        );
        self.events.send(AppEvent::device_updated(device_id));
        Ok(())
    }

    fn record_authenticated(&self, peer: &PeerInfo) {
        self.observed_peers.insert(
            peer.device_id,
            ObservedPeer {
                http_address: None,
                protocol_version: PROTOCOL_VERSION,
                agent_version: peer.agent_version.clone(),
                features: peer.features.clone(),
            },
        );
        self.events.send(AppEvent::device_updated(peer.device_id));
    }

    fn device_views(&self) -> Result<Vec<DeviceView>, StorageError> {
        let mut merged = HashMap::<Uuid, MergedDevice>::new();
        for device in self.database.list_discovered_devices()? {
            merged.insert(
                device.device_id,
                MergedDevice {
                    id: device.device_id,
                    name: device.name,
                    address: device.address,
                    public_key: device.public_key,
                    first_seen: device.first_seen,
                    last_seen: device.last_seen,
                    trusted: false,
                },
            );
        }
        for peer in self.database.list_trusted_peers()? {
            merged
                .entry(peer.device_id)
                .and_modify(|device| {
                    device.name.clone_from(&peer.name);
                    device.address.clone_from(&peer.address);
                    device.public_key = Some(peer.public_key.clone());
                    device.first_seen = device.first_seen.min(peer.trusted_at);
                    device.last_seen = device.last_seen.max(peer.last_seen);
                    device.trusted = true;
                })
                .or_insert_with(|| MergedDevice {
                    id: peer.device_id,
                    name: peer.name,
                    address: peer.address,
                    public_key: Some(peer.public_key),
                    first_seen: peer.trusted_at,
                    last_seen: peer.last_seen,
                    trusted: true,
                });
        }

        let now = unix_millis();
        let ttl_millis = self.online_ttl.as_millis().min(i64::MAX as u128) as i64;
        let mut views = merged
            .into_values()
            .map(|device| {
                let observed = self
                    .observed_peers
                    .get(&device.id)
                    .map(|entry| entry.value().clone());
                let identity_verified =
                    validated_public_key(device.id, device.public_key.as_deref()).is_some();
                let online =
                    device.last_seen > 0 && now.saturating_sub(device.last_seen) <= ttl_millis;
                DeviceView {
                    id: device.id,
                    name: device.name,
                    address: device.address,
                    http_address: observed
                        .as_ref()
                        .and_then(|peer| peer.http_address)
                        .map(|address| address.to_string()),
                    online,
                    trusted: device.trusted,
                    pending: !device.trusted,
                    identity_verified,
                    status: if device.trusted {
                        "trusted".to_owned()
                    } else {
                        "pending".to_owned()
                    },
                    public_key: device.public_key.as_deref().map(hex),
                    first_seen: device.first_seen,
                    last_seen: device.last_seen,
                    protocol_version: observed.as_ref().map(|peer| peer.protocol_version),
                    agent_version: observed.as_ref().map(|peer| peer.agent_version.clone()),
                    features: observed.map_or_else(Vec::new, |peer| peer.features),
                }
            })
            .collect::<Vec<_>>();
        views.sort_unstable_by(|left, right| {
            right
                .online
                .cmp(&left.online)
                .then_with(|| right.trusted.cmp(&left.trusted))
                .then_with(|| left.name.to_lowercase().cmp(&right.name.to_lowercase()))
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(views)
    }

    fn job_view(&self, record: JobRecord) -> Result<JobView, StorageError> {
        let mut progress = record.job.progress.clone();
        let mut bytes_per_second = 0.0;
        if let Some(live) = self.jobs.live_progress(record.job.id) {
            progress = live.progress;
            if matches!(
                record.status,
                StoredJobStatus::Scanning
                    | StoredJobStatus::Transferring
                    | StoredJobStatus::Verifying
            ) {
                bytes_per_second = live.bytes_per_second;
            }
        }
        let accounted_bytes = progress
            .transferred_bytes
            .saturating_add(progress.reused_bytes)
            .min(progress.total_bytes);
        let completed = matches!(
            record.status,
            StoredJobStatus::Completed | StoredJobStatus::CompletedWithErrors
        );
        let percent = if progress.total_bytes > 0 {
            accounted_bytes as f64 * 100.0 / progress.total_bytes as f64
        } else if completed {
            100.0
        } else {
            0.0
        };
        let remaining = progress.total_bytes.saturating_sub(accounted_bytes);
        let eta_seconds =
            (bytes_per_second > 0.0 && remaining > 0).then(|| remaining as f64 / bytes_per_second);
        let files = self.database.list_job_files(record.job.id)?;
        let file_errors = files
            .into_iter()
            .filter_map(|file| {
                file.error.map(|message| FileErrorView {
                    path: file.path,
                    status: file.status,
                    message,
                })
            })
            .collect();
        let direction = if record.job.source_root.as_os_str().is_empty() {
            "incoming"
        } else {
            "outgoing"
        };
        let pipeline = PipelineView {
            active_file_streams: progress.active_file_streams,
            active_chunk_streams: progress.active_chunk_streams,
            queued_chunks: progress.queued_chunks,
            current_concurrency: progress.current_concurrency,
            configured_concurrency: record.job.config.concurrency,
        };
        Ok(JobView {
            id: record.job.id,
            peer_id: record.job.peer_id,
            runner_active: self.jobs.is_active(record.job.id),
            direction,
            source_path: record.job.source_root.to_string_lossy().into_owned(),
            destination_path: record.job.destination_root.to_string_lossy().into_owned(),
            verification: record.job.config.verification_mode,
            chunk_size_bytes: record.job.config.chunk_size,
            concurrency: record.job.config.concurrency,
            retry_limit: record.job.config.retry_limit,
            status: record.status,
            error: record.error,
            created_at: record.created_at,
            updated_at: record.updated_at,
            progress,
            percent: percent.clamp(0.0, 100.0),
            accounted_bytes,
            throughput_bytes_per_second: bytes_per_second,
            throughput_mbps: bytes_per_second * 8.0 / 1_000_000.0,
            throughput_megabytes_per_second: bytes_per_second / 1_000_000.0,
            eta_seconds,
            pipeline,
            errors: record.job.errors,
            file_errors,
        })
    }

    fn job_view_by_id(&self, job_id: Uuid) -> Result<Option<JobView>, StorageError> {
        self.database
            .get_job(job_id)?
            .map(|record| self.job_view(record))
            .transpose()
    }
}

#[derive(Debug)]
struct MergedDevice {
    id: Uuid,
    name: String,
    address: String,
    public_key: Option<Vec<u8>>,
    first_seen: i64,
    last_seen: i64,
    trusted: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct DeviceView {
    pub id: Uuid,
    pub name: String,
    pub address: String,
    pub http_address: Option<String>,
    pub online: bool,
    pub trusted: bool,
    pub pending: bool,
    pub identity_verified: bool,
    pub status: String,
    pub public_key: Option<String>,
    pub first_seen: i64,
    pub last_seen: i64,
    pub protocol_version: Option<u16>,
    pub agent_version: Option<String>,
    pub features: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
struct LocalDeviceView {
    id: Uuid,
    name: String,
    public_key: String,
    certificate_fingerprint: String,
    protocol_version: u16,
    agent_version: &'static str,
    features: Vec<&'static str>,
    http_address: String,
    quic_address: String,
    data_directory: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct JobView {
    pub id: Uuid,
    pub peer_id: Option<Uuid>,
    pub runner_active: bool,
    pub direction: &'static str,
    pub source_path: String,
    pub destination_path: String,
    pub verification: VerificationMode,
    pub chunk_size_bytes: u64,
    pub concurrency: u32,
    pub retry_limit: u32,
    pub status: StoredJobStatus,
    pub error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub progress: TransferProgress,
    pub percent: f64,
    pub accounted_bytes: u64,
    pub throughput_bytes_per_second: f64,
    pub throughput_mbps: f64,
    pub throughput_megabytes_per_second: f64,
    pub eta_seconds: Option<f64>,
    pub pipeline: PipelineView,
    pub errors: Vec<FileError>,
    pub file_errors: Vec<FileErrorView>,
}

#[derive(Clone, Debug, Serialize)]
pub struct PipelineView {
    pub active_file_streams: u32,
    pub active_chunk_streams: u32,
    pub queued_chunks: u64,
    pub current_concurrency: u32,
    pub configured_concurrency: u32,
}

#[derive(Clone, Debug, Serialize)]
pub struct FileErrorView {
    pub path: String,
    pub status: JobFileStatus,
    pub message: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConnectRequest {
    address: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateJobRequest {
    peer_id: Uuid,
    source_path: String,
    destination_path: String,
    #[serde(default, alias = "verification_mode")]
    verification: Option<VerificationMode>,
    #[serde(default)]
    chunk_size: Option<u64>,
    #[serde(default)]
    chunk_size_bytes: Option<u64>,
    #[serde(default)]
    chunk_size_mib: Option<u64>,
    #[serde(default)]
    concurrency: Option<u32>,
    #[serde(default)]
    retry_limit: Option<u32>,
}

#[derive(Debug, Serialize)]
struct HealthView {
    status: &'static str,
    agent_version: &'static str,
    protocol_version: u16,
}

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

#[derive(Serialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Serialize)]
struct ErrorBody {
    code: &'static str,
    message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "bad_request", message)
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", message)
    }

    fn conflict(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, "conflict", message)
    }

    fn bad_gateway(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_GATEWAY, "peer_unreachable", message)
    }

    fn gateway_timeout(message: impl Into<String>) -> Self {
        Self::new(StatusCode::GATEWAY_TIMEOUT, "peer_timeout", message)
    }

    fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", message)
    }

    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }

    fn from_storage(error: StorageError) -> Self {
        tracing::error!(%error, "API storage operation failed");
        Self::internal(error.to_string())
    }

    fn from_manager(error: ManagerError) -> Self {
        match error {
            ManagerError::NotFound(id) => Self::not_found(format!("job {id} was not found")),
            ManagerError::Conflict(message) => Self::conflict(message),
            ManagerError::ShuttingDown => Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "shutting_down",
                "the agent is shutting down",
            ),
            ManagerError::Storage(error) => Self::from_storage(error),
        }
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ApiError {}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorEnvelope {
                error: ErrorBody {
                    code: self.code,
                    message: self.message,
                },
            }),
        )
            .into_response()
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/health", get(health))
        .route("/api/health", get(health))
        .route("/api/local", get(local_device))
        .route("/api/identity", get(local_device))
        .route("/api/devices/local", get(local_device))
        .route("/api/devices", get(list_devices))
        .route("/api/devices/connect", post(connect_device))
        .route(
            "/api/devices/{id}/trust",
            post(trust_device).delete(untrust_device),
        )
        .route("/api/devices/{id}/untrust", post(untrust_device))
        .route("/api/jobs", get(list_jobs).post(create_job))
        .route("/api/jobs/{id}", get(get_job))
        .route("/api/jobs/{id}/pause", post(pause_job))
        .route("/api/jobs/{id}/resume", post(resume_job))
        .route("/api/jobs/{id}/cancel", post(cancel_job))
        .route("/api/events", get(events))
        .fallback(not_found)
        .layer(middleware::from_fn(reject_cross_origin))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn reject_cross_origin(request: Request<Body>, next: Next) -> Response {
    let headers = request.headers();
    let Some(origin) = headers.get(header::ORIGIN) else {
        return next.run(request).await;
    };
    let same_origin = origin
        .to_str()
        .ok()
        .and_then(|origin| origin.parse::<axum::http::Uri>().ok())
        .and_then(|origin| {
            origin
                .authority()
                .map(|authority| authority.as_str().to_owned())
        })
        .zip(
            headers
                .get(header::HOST)
                .and_then(|host| host.to_str().ok())
                .map(str::to_owned),
        )
        .is_some_and(|(origin, host)| origin.eq_ignore_ascii_case(&host));
    if same_origin {
        next.run(request).await
    } else {
        (
            StatusCode::FORBIDDEN,
            "cross-origin requests are not allowed",
        )
            .into_response()
    }
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn health() -> Json<HealthView> {
    Json(HealthView {
        status: "ok",
        agent_version: AGENT_VERSION,
        protocol_version: PROTOCOL_VERSION,
    })
}

async fn local_device(State(state): State<AppState>) -> Json<LocalDeviceView> {
    let identity = state.engine.identity();
    Json(LocalDeviceView {
        id: identity.id(),
        name: identity.name().to_owned(),
        public_key: hex(&identity.public_key()),
        certificate_fingerprint: hex(&identity.certificate_fingerprint()),
        protocol_version: PROTOCOL_VERSION,
        agent_version: AGENT_VERSION,
        features: AGENT_FEATURES.to_vec(),
        http_address: state.http_address.to_string(),
        quic_address: state.quic_address.to_string(),
        data_directory: state.data_directory.to_string_lossy().into_owned(),
    })
}

async fn list_devices(State(state): State<AppState>) -> Result<Json<Vec<DeviceView>>, ApiError> {
    state
        .device_views()
        .map(Json)
        .map_err(ApiError::from_storage)
}

async fn connect_device(
    State(state): State<AppState>,
    payload: Result<Json<ConnectRequest>, JsonRejection>,
) -> Result<Json<DeviceView>, ApiError> {
    let request = payload.map_err(json_rejection)?.0;
    let address = resolve_peer_address(&request.address).await?;
    let peer = probe_with_timeout(&state.engine, address, None).await?;
    state.record_authenticated(&peer);
    find_device_view(&state, peer.device_id)
}

async fn trust_device(
    State(state): State<AppState>,
    Path(raw_id): Path<String>,
) -> Result<Json<DeviceView>, ApiError> {
    let device_id = parse_uuid(&raw_id, "device ID")?;
    if let Some(trusted) = state
        .database
        .get_trusted_peer(device_id)
        .map_err(ApiError::from_storage)?
    {
        if validated_public_key(device_id, Some(trusted.public_key.as_slice())).is_none() {
            return Err(ApiError::conflict(format!(
                "trusted record for device {device_id} contains an invalid public key"
            )));
        }
        return find_device_view(&state, device_id);
    }
    let discovered = state
        .database
        .get_discovered_device(device_id)
        .map_err(ApiError::from_storage)?
        .ok_or_else(|| ApiError::not_found(format!("device {device_id} was not found")))?;
    let address = discovered.address.parse::<SocketAddr>().map_err(|error| {
        ApiError::conflict(format!(
            "stored address {:?} for device {device_id} is invalid: {error}",
            discovered.address
        ))
    })?;

    let probe =
        tokio::time::timeout(PROBE_TIMEOUT, state.engine.probe(address, Some(device_id))).await;
    let (name, public_key, trusted_address, last_seen) = match probe {
        Ok(Ok(peer)) => {
            state.record_authenticated(&peer);
            (
                peer.device_name,
                peer.public_key,
                peer.address.to_string(),
                unix_millis(),
            )
        }
        Ok(Err(error)) => {
            if !error.is_retryable() {
                return Err(ApiError::bad_gateway(format!(
                    "authenticated probe for device {device_id} was rejected: {error}"
                )));
            }
            let public_key = validated_public_key(device_id, discovered.public_key.as_deref())
                .ok_or_else(|| {
                    ApiError::bad_gateway(format!(
                        "device {device_id} has not completed a cryptographic probe: {error}"
                    ))
                })?;
            tracing::warn!(%device_id, %error, "trusting previously authenticated peer while it is unreachable");
            (
                discovered.name,
                public_key,
                discovered.address,
                discovered.last_seen,
            )
        }
        Err(_) => {
            let public_key = validated_public_key(device_id, discovered.public_key.as_deref())
                .ok_or_else(|| {
                    ApiError::gateway_timeout(format!(
                        "timed out probing device {device_id}; no authenticated public key is stored"
                    ))
                })?;
            tracing::warn!(%device_id, "trusting previously authenticated peer after probe timeout");
            (
                discovered.name,
                public_key,
                discovered.address,
                discovered.last_seen,
            )
        }
    };
    if device_id_from_public_key(&public_key) != device_id {
        return Err(ApiError::conflict(format!(
            "stored public key does not derive device ID {device_id}"
        )));
    }
    state
        .database
        .upsert_trusted_peer(&TrustedPeer {
            device_id,
            name,
            public_key: public_key.to_vec(),
            address: trusted_address,
            trusted_at: unix_millis(),
            last_seen,
        })
        .map_err(ApiError::from_storage)?;
    state.events.send(AppEvent::device_updated(device_id));
    find_device_view(&state, device_id)
}

async fn untrust_device(
    State(state): State<AppState>,
    Path(raw_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let device_id = parse_uuid(&raw_id, "device ID")?;
    let removed = state
        .database
        .remove_trusted_peer(device_id)
        .map_err(ApiError::from_storage)?;
    if !removed {
        return Err(ApiError::not_found(format!(
            "trusted device {device_id} was not found"
        )));
    }
    state.engine.disconnect_peer(device_id);
    state
        .jobs
        .cancel_peer(device_id)
        .map_err(ApiError::from_manager)?;
    state.events.send(AppEvent::device_updated(device_id));
    Ok(StatusCode::NO_CONTENT)
}

async fn list_jobs(State(state): State<AppState>) -> Result<Json<Vec<JobView>>, ApiError> {
    let records = state.database.list_jobs().map_err(ApiError::from_storage)?;
    let jobs = records
        .into_iter()
        .map(|record| state.job_view(record))
        .collect::<Result<Vec<_>, _>>()
        .map_err(ApiError::from_storage)?;
    Ok(Json(jobs))
}

async fn get_job(
    State(state): State<AppState>,
    Path(raw_id): Path<String>,
) -> Result<Json<JobView>, ApiError> {
    let job_id = parse_uuid(&raw_id, "job ID")?;
    state
        .job_view_by_id(job_id)
        .map_err(ApiError::from_storage)?
        .map(Json)
        .ok_or_else(|| ApiError::not_found(format!("job {job_id} was not found")))
}

async fn create_job(
    State(state): State<AppState>,
    payload: Result<Json<CreateJobRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<JobView>), ApiError> {
    let request = payload.map_err(json_rejection)?.0;
    let source = PathBuf::from(request.source_path.trim());
    if !source.is_absolute() {
        return Err(ApiError::bad_request("source_path must be absolute"));
    }
    let source_metadata = tokio::fs::symlink_metadata(&source)
        .await
        .map_err(|error| {
            ApiError::bad_request(format!(
                "source_path `{}` cannot be read: {error}",
                source.display()
            ))
        })?;
    if source_metadata.file_type().is_symlink() || !source_metadata.is_dir() {
        return Err(ApiError::bad_request(
            "source_path must be an existing directory, not a symlink",
        ));
    }
    let destination = PathBuf::from(request.destination_path.trim());
    if !is_receiver_absolute_path(&destination) {
        return Err(ApiError::bad_request(
            "destination_path must be absolute on the remote device",
        ));
    }

    let trusted = state
        .database
        .get_trusted_peer(request.peer_id)
        .map_err(ApiError::from_storage)?
        .ok_or_else(|| ApiError::conflict(format!("peer {} is not trusted", request.peer_id)))?;
    trusted.address.parse::<SocketAddr>().map_err(|error| {
        ApiError::conflict(format!(
            "trusted peer address {:?} is invalid: {error}",
            trusted.address
        ))
    })?;

    let chunk_size = resolve_chunk_size(&request)?;
    let concurrency = request.concurrency.unwrap_or(32);
    if !(1..=MAX_CONCURRENCY).contains(&concurrency) {
        return Err(ApiError::bad_request(format!(
            "concurrency must be between 1 and {MAX_CONCURRENCY}"
        )));
    }
    let retry_limit = request.retry_limit.unwrap_or(6);
    if retry_limit > MAX_RETRY_LIMIT {
        return Err(ApiError::bad_request(format!(
            "retry_limit must not exceed {MAX_RETRY_LIMIT}"
        )));
    }
    let config = TransferConfig {
        verification_mode: request.verification.unwrap_or(VerificationMode::Verified),
        chunk_size,
        concurrency,
        retry_limit,
    };
    let mut job = TransferJob::new(source, destination, config);
    job.peer_id = Some(request.peer_id);
    state
        .database
        .create_job(&job)
        .map_err(ApiError::from_storage)?;
    if let Err(error) = state.jobs.start(job.id) {
        let message = error.to_string();
        if let Err(storage_error) =
            state
                .database
                .update_job_status_with_error(job.id, JobStatus::Failed, Some(&message))
        {
            tracing::error!(job_id = %job.id, %storage_error, "failed to persist job start failure");
        }
        return Err(ApiError::from_manager(error));
    }
    let view = state
        .job_view_by_id(job.id)
        .map_err(ApiError::from_storage)?
        .ok_or_else(|| ApiError::internal("newly created job disappeared from storage"))?;
    Ok((StatusCode::CREATED, Json(view)))
}

async fn pause_job(
    State(state): State<AppState>,
    Path(raw_id): Path<String>,
) -> Result<Json<JobView>, ApiError> {
    control_job(&state, &raw_id, JobAction::Pause).await
}

async fn resume_job(
    State(state): State<AppState>,
    Path(raw_id): Path<String>,
) -> Result<Json<JobView>, ApiError> {
    control_job(&state, &raw_id, JobAction::Resume).await
}

async fn cancel_job(
    State(state): State<AppState>,
    Path(raw_id): Path<String>,
) -> Result<Json<JobView>, ApiError> {
    control_job(&state, &raw_id, JobAction::Cancel).await
}

enum JobAction {
    Pause,
    Resume,
    Cancel,
}

async fn control_job(
    state: &AppState,
    raw_id: &str,
    action: JobAction,
) -> Result<Json<JobView>, ApiError> {
    let job_id = parse_uuid(raw_id, "job ID")?;
    let result = match action {
        JobAction::Pause => state.jobs.pause(job_id),
        JobAction::Resume => state.jobs.resume(job_id),
        JobAction::Cancel => state.jobs.cancel(job_id),
    };
    result.map_err(ApiError::from_manager)?;
    state
        .job_view_by_id(job_id)
        .map_err(ApiError::from_storage)?
        .map(Json)
        .ok_or_else(|| ApiError::not_found(format!("job {job_id} was not found")))
}

async fn events(websocket: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    websocket.on_upgrade(move |socket| event_socket(socket, state.events, state.shutdown))
}

async fn event_socket(socket: WebSocket, events: EventBus, shutdown: CancellationToken) {
    let (mut sender, mut receiver) = socket.split();
    let mut subscription = events.subscribe();
    if sender
        .send(Message::Text(r#"{"type":"ready"}"#.into()))
        .await
        .is_err()
    {
        return;
    }
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                let _ = sender.send(Message::Close(None)).await;
                return;
            }
            event = subscription.recv() => {
                let text = match event {
                    Ok(event) => match serde_json::to_string(&event) {
                        Ok(text) => text,
                        Err(error) => {
                            tracing::warn!(%error, "failed to serialize websocket event");
                            continue;
                        }
                    },
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        r#"{"type":"resync"}"#.to_owned()
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                };
                if sender.send(Message::Text(text.into())).await.is_err() {
                    return;
                }
            }
            incoming = receiver.next() => {
                match incoming {
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return,
                    Some(Ok(Message::Ping(payload))) => {
                        if sender.send(Message::Pong(payload)).await.is_err() {
                            return;
                        }
                    }
                    Some(Ok(_)) => {}
                }
            }
        }
    }
}

async fn not_found() -> ApiError {
    ApiError::not_found("route not found")
}

fn find_device_view(state: &AppState, device_id: Uuid) -> Result<Json<DeviceView>, ApiError> {
    state
        .device_views()
        .map_err(ApiError::from_storage)?
        .into_iter()
        .find(|device| device.id == device_id)
        .map(Json)
        .ok_or_else(|| ApiError::internal("authenticated device was not persisted"))
}

async fn probe_with_timeout(
    engine: &TransferEngine,
    address: SocketAddr,
    expected_device_id: Option<Uuid>,
) -> Result<PeerInfo, ApiError> {
    match tokio::time::timeout(PROBE_TIMEOUT, engine.probe(address, expected_device_id)).await {
        Ok(Ok(peer)) => Ok(peer),
        Ok(Err(error)) => Err(ApiError::bad_gateway(format!(
            "authenticated probe to {address} failed: {error}"
        ))),
        Err(_) => Err(ApiError::gateway_timeout(format!(
            "authenticated probe to {address} timed out"
        ))),
    }
}

async fn resolve_peer_address(raw: &str) -> Result<SocketAddr, ApiError> {
    let value = raw.trim();
    if value.is_empty() {
        return Err(ApiError::bad_request("address must not be empty"));
    }
    if let Ok(address) = value.parse::<SocketAddr>() {
        return Ok(address);
    }
    if let Ok(ip) = value.parse::<IpAddr>() {
        return Ok(SocketAddr::new(ip, DEFAULT_QUIC_PORT));
    }
    let target = if value.contains(':') {
        value.to_owned()
    } else {
        format!("{value}:{DEFAULT_QUIC_PORT}")
    };
    let resolved = lookup_host(&target)
        .await
        .map_err(|error| ApiError::bad_request(format!("cannot resolve {value:?}: {error}")))?
        .collect::<Vec<_>>();
    resolved
        .iter()
        .copied()
        .find(SocketAddr::is_ipv4)
        .or_else(|| resolved.first().copied())
        .ok_or_else(|| ApiError::bad_request(format!("address {value:?} resolved to no endpoints")))
}

fn resolve_chunk_size(request: &CreateJobRequest) -> Result<u64, ApiError> {
    let supplied = [
        request.chunk_size,
        request.chunk_size_bytes,
        request.chunk_size_mib,
    ]
    .into_iter()
    .flatten()
    .count();
    if supplied > 1 {
        return Err(ApiError::bad_request(
            "provide only one of chunk_size, chunk_size_bytes, or chunk_size_mib",
        ));
    }
    let chunk_size = if let Some(mib) = request.chunk_size_mib {
        mib.checked_mul(1024 * 1024)
            .ok_or_else(|| ApiError::bad_request("chunk_size_mib is too large"))?
    } else {
        request
            .chunk_size_bytes
            .or(request.chunk_size)
            .unwrap_or(DEFAULT_CHUNK_SIZE)
    };
    if !(MIN_CHUNK_SIZE..=MAX_CHUNK_SIZE).contains(&chunk_size) {
        return Err(ApiError::bad_request(format!(
            "chunk size must be between {MIN_CHUNK_SIZE} and {MAX_CHUNK_SIZE} bytes"
        )));
    }
    Ok(chunk_size)
}

fn parse_uuid(value: &str, kind: &str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(value)
        .map_err(|error| ApiError::bad_request(format!("invalid {kind} {value:?}: {error}")))
}

fn json_rejection(rejection: JsonRejection) -> ApiError {
    ApiError::bad_request(format!("invalid JSON request: {rejection}"))
}

fn validated_public_key(device_id: Uuid, public_key: Option<&[u8]>) -> Option<[u8; 32]> {
    let public_key: [u8; 32] = public_key?.try_into().ok()?;
    (device_id_from_public_key(&public_key) == device_id).then_some(public_key)
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        encoded.push(DIGITS[usize::from(byte >> 4)] as char);
        encoded.push(DIGITS[usize::from(byte & 0x0f)] as char);
    }
    encoded
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::net::{Ipv4Addr, SocketAddrV4};

    use fastsync_transfer::DeviceIdentity;

    use super::*;

    #[tokio::test]
    async fn device_state_merges_discovery_and_trust() -> Result<(), Box<dyn Error>> {
        let database = Database::open_in_memory()?;
        let local_identity = DeviceIdentity::load_or_create(database.clone(), "local")?;
        let engine = TransferEngine::bind(
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)),
            database.clone(),
            local_identity,
        )?;
        let events = EventBus::new();
        let jobs = JobManager::new(database.clone(), engine.clone(), events.clone());
        let state = AppState::new(
            database.clone(),
            engine.clone(),
            jobs,
            events,
            Duration::from_secs(30),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8765)),
            engine.local_addr()?,
            PathBuf::from("test-data"),
            CancellationToken::new(),
        );

        let peer_database = Database::open_in_memory()?;
        let peer_identity = DeviceIdentity::load_or_create(peer_database, "peer")?;
        let now = unix_millis();
        database.upsert_discovered_device(&DiscoveredDevice {
            device_id: peer_identity.id(),
            name: "beacon name".to_owned(),
            public_key: None,
            address: "127.0.0.1:39463".to_owned(),
            first_seen: now - 10,
            last_seen: now,
        })?;
        database.upsert_trusted_peer(&TrustedPeer {
            device_id: peer_identity.id(),
            name: "trusted name".to_owned(),
            public_key: peer_identity.public_key().to_vec(),
            address: "127.0.0.1:39463".to_owned(),
            trusted_at: now - 5,
            last_seen: now - 1,
        })?;

        let views = state.device_views()?;
        assert_eq!(views.len(), 1);
        assert_eq!(views[0].name, "trusted name");
        assert!(views[0].trusted);
        assert!(views[0].online);
        assert!(views[0].identity_verified);
        engine.close();
        Ok(())
    }

    #[test]
    fn chunk_size_accepts_bytes_or_mib_but_not_both() -> Result<(), Box<dyn Error>> {
        let mut request = CreateJobRequest {
            peer_id: Uuid::nil(),
            source_path: "/source".to_owned(),
            destination_path: "/destination".to_owned(),
            verification: None,
            chunk_size: None,
            chunk_size_bytes: None,
            chunk_size_mib: Some(8),
            concurrency: None,
            retry_limit: None,
        };
        assert_eq!(resolve_chunk_size(&request)?, 8 * 1024 * 1024);
        request.chunk_size_bytes = Some(1024);
        assert!(resolve_chunk_size(&request).is_err());
        Ok(())
    }

    #[test]
    fn public_key_must_derive_the_claimed_device_id() -> Result<(), Box<dyn Error>> {
        let database = Database::open_in_memory()?;
        let identity = DeviceIdentity::load_or_create(database, "peer")?;
        let public_key = identity.public_key();
        assert_eq!(
            validated_public_key(identity.id(), Some(&public_key)),
            Some(public_key)
        );
        assert_eq!(validated_public_key(Uuid::nil(), Some(&public_key)), None);
        Ok(())
    }
}
