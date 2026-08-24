use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use fastsync_core::{JobStatus, TransferProgress};
use fastsync_storage::{Database, StorageError, StoredJobStatus};
use fastsync_transfer::{TransferEngine, TransferError};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::events::{AppEvent, EventBus};

const INITIAL_RETRY_DELAY: Duration = Duration::from_millis(500);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(15);
const THROUGHPUT_STALE_AFTER: Duration = Duration::from_secs(4);
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub enum ManagerError {
    NotFound(Uuid),
    Conflict(String),
    ShuttingDown,
    Storage(StorageError),
}

impl fmt::Display for ManagerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound(id) => write!(formatter, "job {id} was not found"),
            Self::Conflict(message) => formatter.write_str(message),
            Self::ShuttingDown => formatter.write_str("the agent is shutting down"),
            Self::Storage(error) => fmt::Display::fmt(error, formatter),
        }
    }
}

impl std::error::Error for ManagerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Storage(error) => Some(error),
            _ => None,
        }
    }
}

impl From<StorageError> for ManagerError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

#[derive(Clone)]
pub struct JobManager {
    inner: Arc<JobManagerInner>,
}

struct JobManagerInner {
    database: Database,
    engine: TransferEngine,
    events: EventBus,
    controls: DashMap<Uuid, Arc<RunnerControl>>,
    snapshots: DashMap<Uuid, ProgressSnapshot>,
    shutdown: CancellationToken,
}

struct RunnerControl {
    cancellation: CancellationToken,
    pause: watch::Sender<bool>,
}

#[derive(Clone)]
struct ProgressSnapshot {
    progress: TransferProgress,
    last_bytes: u64,
    byte_sampled_at: Instant,
    bytes_per_second: f64,
}

#[derive(Clone, Debug)]
pub struct LiveProgress {
    pub progress: TransferProgress,
    pub bytes_per_second: f64,
}

impl ProgressSnapshot {
    fn new(progress: TransferProgress, now: Instant) -> Self {
        Self {
            last_bytes: progress.transferred_bytes,
            progress,
            byte_sampled_at: now,
            bytes_per_second: 0.0,
        }
    }

    fn update(&mut self, mut progress: TransferProgress, now: Instant) {
        if progress.transferred_bytes < self.last_bytes {
            self.last_bytes = progress.transferred_bytes;
            self.byte_sampled_at = now;
            self.bytes_per_second = 0.0;
        } else if progress.transferred_bytes > self.last_bytes {
            let elapsed = now.saturating_duration_since(self.byte_sampled_at);
            if !elapsed.is_zero() {
                let delta = progress.transferred_bytes - self.last_bytes;
                self.bytes_per_second = delta as f64 / elapsed.as_secs_f64();
                let rate = self.bytes_per_second.min(u64::MAX as f64) as u64;
                progress.network_bytes_per_second = rate;
                progress.read_bytes_per_second = rate;
                progress.write_bytes_per_second = rate;
            }
            self.last_bytes = progress.transferred_bytes;
            self.byte_sampled_at = now;
        }
        self.progress = progress;
    }

    fn view(&self, now: Instant) -> LiveProgress {
        let bytes_per_second =
            if now.saturating_duration_since(self.byte_sampled_at) > THROUGHPUT_STALE_AFTER {
                0.0
            } else {
                self.bytes_per_second
            };
        LiveProgress {
            progress: self.progress.clone(),
            bytes_per_second,
        }
    }
}

impl JobManager {
    pub fn new(database: Database, engine: TransferEngine, events: EventBus) -> Self {
        Self {
            inner: Arc::new(JobManagerInner {
                database,
                engine,
                events,
                controls: DashMap::new(),
                snapshots: DashMap::new(),
                shutdown: CancellationToken::new(),
            }),
        }
    }

    pub fn start(&self, job_id: Uuid) -> Result<(), ManagerError> {
        if self.inner.shutdown.is_cancelled() {
            return Err(ManagerError::ShuttingDown);
        }
        if self.inner.database.get_job(job_id)?.is_none() {
            return Err(ManagerError::NotFound(job_id));
        }

        let (pause, _) = watch::channel(false);
        let control = Arc::new(RunnerControl {
            cancellation: self.inner.shutdown.child_token(),
            pause,
        });
        match self.inner.controls.entry(job_id) {
            Entry::Occupied(_) => {
                return Err(ManagerError::Conflict(format!(
                    "job {job_id} already has an active runner"
                )));
            }
            Entry::Vacant(entry) => {
                entry.insert(Arc::clone(&control));
            }
        }

        let manager = self.clone();
        tokio::spawn(async move {
            manager.run(job_id, Arc::clone(&control)).await;
            let should_remove = manager
                .inner
                .controls
                .get(&job_id)
                .is_some_and(|active| Arc::ptr_eq(active.value(), &control));
            if should_remove {
                manager.inner.controls.remove(&job_id);
                manager.inner.events.send(AppEvent::job_updated(job_id));
            }
        });
        self.inner.events.send(AppEvent::job_updated(job_id));
        Ok(())
    }

    pub fn pause(&self, job_id: Uuid) -> Result<(), ManagerError> {
        let record = self
            .inner
            .database
            .get_job(job_id)?
            .ok_or(ManagerError::NotFound(job_id))?;
        if matches!(
            record.status,
            StoredJobStatus::Completed
                | StoredJobStatus::CompletedWithErrors
                | StoredJobStatus::Cancelled
        ) {
            return Err(ManagerError::Conflict(format!(
                "job {job_id} cannot be paused from {:?}",
                record.status
            )));
        }
        let Some(control) = self.inner.controls.get(&job_id) else {
            if record.status == StoredJobStatus::Paused {
                return Ok(());
            }
            return Err(ManagerError::Conflict(format!(
                "job {job_id} does not have an active runner"
            )));
        };
        control.pause.send_replace(true);
        drop(control);
        self.inner
            .database
            .update_job_status(job_id, JobStatus::Paused)?;
        self.inner.events.send(AppEvent::job_updated(job_id));
        Ok(())
    }

    pub fn resume(&self, job_id: Uuid) -> Result<(), ManagerError> {
        let record = self
            .inner
            .database
            .get_job(job_id)?
            .ok_or(ManagerError::NotFound(job_id))?;
        if let Some(control) = self.inner.controls.get(&job_id) {
            if *control.pause.borrow() {
                control.pause.send_replace(false);
                drop(control);
                self.inner.database.update_job_status_with_error(
                    job_id,
                    JobStatus::Transferring,
                    None,
                )?;
                self.inner.events.send(AppEvent::job_updated(job_id));
                return Ok(());
            }
            if !matches!(record.status, StoredJobStatus::Paused) {
                if matches!(
                    record.status,
                    StoredJobStatus::Pending
                        | StoredJobStatus::Scanning
                        | StoredJobStatus::Transferring
                        | StoredJobStatus::Verifying
                ) {
                    return Ok(());
                }
                return Err(ManagerError::Conflict(format!(
                    "job {job_id} cannot be resumed from {:?}",
                    record.status
                )));
            }
            return Err(ManagerError::Conflict(format!(
                "job {job_id} is marked paused but its runner is active"
            )));
        }

        if !matches!(
            record.status,
            StoredJobStatus::Paused
                | StoredJobStatus::Interrupted
                | StoredJobStatus::Failed
                | StoredJobStatus::CompletedWithErrors
                | StoredJobStatus::Pending
        ) {
            return Err(ManagerError::Conflict(format!(
                "job {job_id} cannot be resumed from {:?}",
                record.status
            )));
        }
        if record.job.source_root.as_os_str().is_empty() {
            return Err(ManagerError::Conflict(
                "incoming jobs are controlled by their sender".to_owned(),
            ));
        }
        self.start(job_id)
    }

    pub fn cancel(&self, job_id: Uuid) -> Result<(), ManagerError> {
        let record = self
            .inner
            .database
            .get_job(job_id)?
            .ok_or(ManagerError::NotFound(job_id))?;
        if matches!(
            record.status,
            StoredJobStatus::Completed | StoredJobStatus::CompletedWithErrors
        ) {
            return Err(ManagerError::Conflict(format!(
                "completed job {job_id} cannot be cancelled"
            )));
        }
        if record.status == StoredJobStatus::Cancelled {
            return Ok(());
        }
        if let Some(control) = self.inner.controls.get(&job_id) {
            control.cancellation.cancel();
        } else if record.job.source_root.as_os_str().is_empty() {
            return Err(ManagerError::Conflict(
                "incoming jobs are controlled by their sender".to_owned(),
            ));
        }
        self.inner
            .database
            .update_job_status(job_id, JobStatus::Cancelled)?;
        self.inner.events.send(AppEvent::job_updated(job_id));
        Ok(())
    }

    pub fn cancel_peer(&self, peer_id: Uuid) -> Result<(), ManagerError> {
        for record in self.inner.database.list_jobs()? {
            if record.job.peer_id != Some(peer_id) || record.job.source_root.as_os_str().is_empty()
            {
                continue;
            }
            let Some(control) = self.inner.controls.get(&record.job.id) else {
                continue;
            };
            control.cancellation.cancel();
            drop(control);
            self.inner
                .database
                .update_job_status(record.job.id, JobStatus::Cancelled)?;
            self.inner.events.send(AppEvent::job_updated(record.job.id));
        }
        Ok(())
    }

    pub fn live_progress(&self, job_id: Uuid) -> Option<LiveProgress> {
        self.inner
            .snapshots
            .get(&job_id)
            .map(|snapshot| snapshot.view(Instant::now()))
    }

    pub fn is_active(&self, job_id: Uuid) -> bool {
        self.inner.controls.contains_key(&job_id)
    }

    pub async fn shutdown(&self) {
        self.inner.shutdown.cancel();
        for control in &self.inner.controls {
            control.value().cancellation.cancel();
        }
        let wait_for_runners = async {
            while !self.inner.controls.is_empty() {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        };
        let _ = tokio::time::timeout(SHUTDOWN_GRACE, wait_for_runners).await;
    }

    async fn run(&self, job_id: Uuid, control: Arc<RunnerControl>) {
        let mut retry_number = 0_u32;
        loop {
            if self.wait_until_resumed(&control).await.is_err() {
                self.persist_stopped(job_id);
                break;
            }

            let record = match self.inner.database.get_job(job_id) {
                Ok(Some(record)) => record,
                Ok(None) => {
                    tracing::error!(%job_id, "active transfer job disappeared from storage");
                    break;
                }
                Err(error) => {
                    tracing::error!(%job_id, %error, "failed to reload transfer job");
                    break;
                }
            };
            let Some(peer_id) = record.job.peer_id else {
                self.persist_failure(job_id, "transfer job has no peer ID");
                break;
            };
            let peer = match self.inner.database.get_trusted_peer(peer_id) {
                Ok(Some(peer)) => peer,
                Ok(None) => {
                    self.persist_failure(job_id, "the transfer peer is no longer trusted");
                    break;
                }
                Err(error) => {
                    self.persist_failure(job_id, &format!("failed to load trusted peer: {error}"));
                    break;
                }
            };
            let peer_address = match peer.address.parse::<SocketAddr>() {
                Ok(address) => address,
                Err(error) => {
                    self.persist_failure(
                        job_id,
                        &format!(
                            "trusted peer address {:?} is invalid: {error}",
                            peer.address
                        ),
                    );
                    break;
                }
            };

            let mut job = record.job;
            job.status = JobStatus::Pending;
            job.errors.clear();
            let retry_limit = job.config.retry_limit;
            let initial_progress = job.progress.clone();
            self.record_progress(job_id, initial_progress.clone());
            if let Err(error) = self.inner.database.upsert_job(&job) {
                tracing::error!(%job_id, %error, "failed to prepare transfer attempt");
                break;
            }
            if *control.pause.borrow() {
                if let Err(error) = self
                    .inner
                    .database
                    .update_job_status(job_id, JobStatus::Paused)
                {
                    tracing::warn!(%job_id, %error, "failed to restore paused status");
                }
                continue;
            }
            self.inner.events.send(AppEvent::job_updated(job_id));

            let (progress_sender, mut progress_receiver) = watch::channel(initial_progress);
            let transfer = self.inner.engine.transfer_job(
                job,
                peer_address,
                control.cancellation.clone(),
                control.pause.subscribe(),
                Some(progress_sender),
            );
            tokio::pin!(transfer);
            let mut progress_open = true;
            let result = loop {
                tokio::select! {
                    result = &mut transfer => break result,
                    changed = progress_receiver.changed(), if progress_open => {
                        if changed.is_err() {
                            progress_open = false;
                            continue;
                        }
                        let progress = progress_receiver.borrow_and_update().clone();
                        self.record_progress(job_id, progress);
                        if *control.pause.borrow()
                            && let Err(error) = self
                                .inner
                                .database
                                .update_job_status(job_id, JobStatus::Paused)
                        {
                            tracing::warn!(%job_id, %error, "failed to persist paused status");
                        }
                        self.inner.events.send(AppEvent::job_progress(job_id));
                    }
                }
            };

            match result {
                Ok(final_job) => {
                    self.record_progress(job_id, final_job.progress.clone());
                    if let Err(error) = self.inner.database.upsert_job(&final_job) {
                        tracing::error!(%job_id, %error, "failed to persist completed transfer");
                    }
                    self.inner.events.send(AppEvent::job_updated(job_id));
                    break;
                }
                Err(_) if control.cancellation.is_cancelled() => {
                    self.persist_stopped(job_id);
                    break;
                }
                Err(TransferError::Cancelled) => {
                    self.persist_stopped(job_id);
                    break;
                }
                Err(error) if error.is_retryable() && retry_number < retry_limit => {
                    retry_number = retry_number.saturating_add(1);
                    let delay = retry_delay(retry_number);
                    let message = format!(
                        "retry {retry_number}/{retry_limit} in {:.1}s after: {error}",
                        delay.as_secs_f64()
                    );
                    if let Err(storage_error) = self.inner.database.update_job_status_with_error(
                        job_id,
                        JobStatus::Pending,
                        Some(&message),
                    ) {
                        tracing::warn!(%job_id, %storage_error, "failed to persist retry state");
                    }
                    tracing::warn!(%job_id, retry_number, retry_limit, %error, "retrying transfer");
                    self.inner.events.send(AppEvent::job_updated(job_id));
                    tokio::select! {
                        _ = control.cancellation.cancelled() => {
                            self.persist_stopped(job_id);
                            break;
                        }
                        _ = tokio::time::sleep(delay) => {}
                    }
                }
                Err(error) => {
                    tracing::warn!(%job_id, %error, "transfer job failed");
                    self.inner.events.send(AppEvent::job_updated(job_id));
                    break;
                }
            }
        }
    }

    async fn wait_until_resumed(&self, control: &RunnerControl) -> Result<(), ()> {
        let mut pause = control.pause.subscribe();
        loop {
            if control.cancellation.is_cancelled() {
                return Err(());
            }
            if !*pause.borrow() {
                return Ok(());
            }
            tokio::select! {
                _ = control.cancellation.cancelled() => return Err(()),
                changed = pause.changed() => {
                    if changed.is_err() {
                        return Ok(());
                    }
                }
            }
        }
    }

    fn record_progress(&self, job_id: Uuid, progress: TransferProgress) {
        let now = Instant::now();
        match self.inner.snapshots.entry(job_id) {
            Entry::Occupied(mut entry) => entry.get_mut().update(progress, now),
            Entry::Vacant(entry) => {
                entry.insert(ProgressSnapshot::new(progress, now));
            }
        }
    }

    fn persist_failure(&self, job_id: Uuid, message: &str) {
        if let Err(error) = self.inner.database.update_job_status_with_error(
            job_id,
            JobStatus::Failed,
            Some(message),
        ) {
            tracing::error!(%job_id, %error, "failed to persist transfer failure");
        }
        self.inner.events.send(AppEvent::job_updated(job_id));
    }

    fn persist_stopped(&self, job_id: Uuid) {
        let result = if self.inner.shutdown.is_cancelled() {
            self.inner.database.mark_job_interrupted(job_id)
        } else {
            self.inner
                .database
                .update_job_status(job_id, JobStatus::Cancelled)
        };
        if let Err(error) = result {
            tracing::warn!(%job_id, %error, "failed to persist stopped transfer");
        }
        self.inner.events.send(AppEvent::job_updated(job_id));
    }
}

fn retry_delay(retry_number: u32) -> Duration {
    let exponent = retry_number.saturating_sub(1).min(10);
    INITIAL_RETRY_DELAY
        .saturating_mul(1_u32 << exponent)
        .min(MAX_RETRY_DELAY)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progress_snapshot_calculates_delta_rate_and_expires() {
        let start = Instant::now();
        let mut snapshot = ProgressSnapshot::new(TransferProgress::default(), start);
        snapshot.update(
            TransferProgress {
                transferred_bytes: 2_000,
                ..TransferProgress::default()
            },
            start + Duration::from_secs(2),
        );
        assert!(
            (snapshot
                .view(start + Duration::from_secs(2))
                .bytes_per_second
                - 1_000.0)
                .abs()
                < 0.1
        );
        assert_eq!(
            snapshot
                .view(start + Duration::from_secs(7))
                .bytes_per_second,
            0.0
        );
    }

    #[test]
    fn retry_delay_is_bounded() {
        assert_eq!(retry_delay(1), Duration::from_millis(500));
        assert_eq!(retry_delay(2), Duration::from_secs(1));
        assert_eq!(retry_delay(100), MAX_RETRY_DELAY);
    }
}
