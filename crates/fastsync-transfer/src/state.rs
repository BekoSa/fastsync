use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

use dashmap::DashMap;
use fastsync_core::{HashedFile, ManifestEntry, VerificationMode};
use uuid::Uuid;

use crate::error::{Result, TransferError};

const JOB_ACTIVE: u8 = 0;
const JOB_COMPLETING: u8 = 1;
const JOB_COMPLETED: u8 = 2;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub(crate) struct IncomingJobKey {
    pub peer_id: Uuid,
    pub job_id: Uuid,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DestinationLease {
    pub owner: IncomingJobKey,
    pub wire_path: String,
    pub native_path: PathBuf,
}

pub(crate) struct CompletedManifest {
    pub all_paths: HashSet<String>,
    pub file_paths: HashSet<String>,
    pub entries: HashMap<String, ManifestEntry>,
}

pub(crate) struct IncomingJob {
    pub key: IncomingJobKey,
    pub requested_destination_root: PathBuf,
    pub destination_root: PathBuf,
    pub staging_root: PathBuf,
    pub verification_mode: VerificationMode,
    pub chunk_size: u64,
    pub files: DashMap<String, Arc<IncomingFile>>,
    directories: Mutex<Vec<ManifestEntry>>,
    directory_failures: Mutex<HashMap<String, String>>,
    path_identities: Mutex<HashMap<String, String>>,
    manifest_entries: Mutex<HashMap<String, ManifestEntry>>,
    committed_manifest_id: Mutex<Option<Uuid>>,
    manifest_generation: Mutex<Option<ManifestGeneration>>,
    started_manifest_ids: Mutex<HashSet<Uuid>>,
    pub compare_lock: tokio::sync::Mutex<()>,
    manifest_complete: AtomicBool,
    lease_keys: Mutex<HashSet<String>>,
    lifecycle: tokio::sync::RwLock<()>,
    phase: AtomicU8,
}

impl IncomingJob {
    pub fn new(
        key: IncomingJobKey,
        requested_destination_root: PathBuf,
        destination_root: PathBuf,
        staging_root: PathBuf,
        verification_mode: VerificationMode,
        chunk_size: u64,
    ) -> Self {
        Self {
            key,
            requested_destination_root,
            destination_root,
            staging_root,
            verification_mode,
            chunk_size,
            files: DashMap::new(),
            directories: Mutex::new(Vec::new()),
            directory_failures: Mutex::new(HashMap::new()),
            path_identities: Mutex::new(HashMap::new()),
            manifest_entries: Mutex::new(HashMap::new()),
            committed_manifest_id: Mutex::new(None),
            manifest_generation: Mutex::new(None),
            started_manifest_ids: Mutex::new(HashSet::new()),
            compare_lock: tokio::sync::Mutex::new(()),
            manifest_complete: AtomicBool::new(false),
            lease_keys: Mutex::new(HashSet::new()),
            lifecycle: tokio::sync::RwLock::new(()),
            phase: AtomicU8::new(JOB_ACTIVE),
        }
    }

    pub fn validate_parameters(
        &self,
        destination_root: &std::path::Path,
        verification_mode: VerificationMode,
        chunk_size: u64,
    ) -> Result<()> {
        if self.destination_root == destination_root
            && self.verification_mode == verification_mode
            && self.chunk_size == chunk_size
        {
            Ok(())
        } else {
            Err(TransferError::InvalidData(format!(
                "job {} parameters changed during transfer",
                self.key.job_id
            )))
        }
    }

    pub fn remember_directory(&self, entry: ManifestEntry) -> Result<()> {
        let path = entry.relative_path.clone();
        let mut directories = self
            .directories
            .lock()
            .map_err(|_| TransferError::Task("directory state lock was poisoned".to_owned()))?;
        if let Some(existing) = directories
            .iter_mut()
            .find(|existing| existing.relative_path == entry.relative_path)
        {
            *existing = entry;
        } else {
            directories.push(entry);
        }
        self.directory_failures
            .lock()
            .map_err(|_| TransferError::Task("directory failure lock was poisoned".to_owned()))?
            .remove(&path);
        Ok(())
    }

    pub fn remember_directory_failure(&self, path: String, error: String) -> Result<()> {
        self.directory_failures
            .lock()
            .map_err(|_| TransferError::Task("directory failure lock was poisoned".to_owned()))?
            .insert(path, error);
        Ok(())
    }

    pub fn directory_failure_count(&self) -> Result<u64> {
        u64::try_from(
            self.directory_failures
                .lock()
                .map_err(|_| TransferError::Task("directory failure lock was poisoned".to_owned()))?
                .len(),
        )
        .map_err(|_| TransferError::InvalidData("directory failure count overflow".to_owned()))
    }

    pub fn validate_manifest_path(&self, path: &str) -> Result<()> {
        let staging_component = self
            .staging_root
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                TransferError::InvalidData("staging root is not valid UTF-8".to_owned())
            })?;
        let uses_staging_namespace = path.split('/').next().is_some_and(|component| {
            #[cfg(windows)]
            {
                component.eq_ignore_ascii_case(staging_component)
            }
            #[cfg(not(windows))]
            {
                component == staging_component
            }
        });
        if uses_staging_namespace {
            return Err(TransferError::InvalidData(format!(
                "path {path:?} conflicts with FastSync's staging namespace"
            )));
        }

        let identity = path.to_lowercase();
        let mut identities = self.path_identities.lock().map_err(|_| {
            TransferError::Task("manifest path identity lock was poisoned".to_owned())
        })?;
        if let Some(existing) = identities.get(&identity) {
            if existing != path {
                return Err(TransferError::InvalidData(format!(
                    "paths {existing:?} differ only by case and are not portable"
                )));
            }
        } else {
            identities.insert(identity, path.to_owned());
        }
        Ok(())
    }

    pub fn begin_manifest_batch(&self, manifest_id: Uuid, sequence: u32) -> Result<()> {
        let mut generation = self
            .manifest_generation
            .lock()
            .map_err(|_| TransferError::Task("manifest generation lock was poisoned".to_owned()))?;
        if sequence == 0 {
            let mut started = self.started_manifest_ids.lock().map_err(|_| {
                TransferError::Task("manifest attempt lock was poisoned".to_owned())
            })?;
            if !started.insert(manifest_id) {
                return Err(TransferError::InvalidData(format!(
                    "manifest ID {manifest_id} was already used for job {}",
                    self.key.job_id
                )));
            }
            *generation = Some(ManifestGeneration {
                manifest_id,
                next_sequence: 0,
                paths: HashSet::new(),
                file_paths: HashSet::new(),
                entries: HashMap::new(),
                required_directories: HashSet::new(),
            });
            self.manifest_complete.store(false, Ordering::Release);
            self.directories
                .lock()
                .map_err(|_| TransferError::Task("directory state lock was poisoned".to_owned()))?
                .clear();
            self.directory_failures
                .lock()
                .map_err(|_| TransferError::Task("directory failure lock was poisoned".to_owned()))?
                .clear();
            self.path_identities
                .lock()
                .map_err(|_| {
                    TransferError::Task("manifest path identity lock was poisoned".to_owned())
                })?
                .clear();
        }
        let active = generation.as_ref().ok_or_else(|| {
            TransferError::InvalidData(format!(
                "manifest batch {sequence} arrived before sequence 0"
            ))
        })?;
        if active.manifest_id != manifest_id {
            return Err(TransferError::InvalidData(format!(
                "manifest batch {sequence} belongs to an obsolete generation"
            )));
        }
        if active.next_sequence != sequence {
            return Err(TransferError::InvalidData(format!(
                "expected manifest batch {}, received {sequence}",
                active.next_sequence
            )));
        }
        Ok(())
    }

    pub fn record_manifest_entry(&self, entry: &ManifestEntry) -> Result<()> {
        let mut generation = self
            .manifest_generation
            .lock()
            .map_err(|_| TransferError::Task("manifest generation lock was poisoned".to_owned()))?;
        let active = generation.as_mut().ok_or_else(|| {
            TransferError::InvalidData("manifest generation is not active".to_owned())
        })?;
        let path = &entry.relative_path;
        if active.paths.contains(path) {
            return Err(TransferError::InvalidData(format!(
                "manifest contains duplicate path {path:?}"
            )));
        }
        let mut offset = 0;
        let mut parents = Vec::new();
        for component in path.split('/').take(path.matches('/').count()) {
            offset += component.len();
            let parent = &path[..offset];
            if active
                .entries
                .get(parent)
                .is_some_and(|parent| parent.file_type != fastsync_core::FileType::Directory)
            {
                return Err(TransferError::InvalidData(format!(
                    "manifest path {path:?} has file ancestor {parent:?}"
                )));
            }
            parents.push(parent.to_owned());
            offset += 1;
        }
        if entry.file_type == fastsync_core::FileType::File {
            if active.required_directories.contains(path) {
                return Err(TransferError::InvalidData(format!(
                    "manifest file {path:?} is an ancestor of another path"
                )));
            }
        }
        active.paths.insert(path.clone());
        active.required_directories.extend(parents);
        if entry.file_type == fastsync_core::FileType::File {
            active.file_paths.insert(path.clone());
        }
        active.entries.insert(path.clone(), entry.clone());
        Ok(())
    }

    pub fn finish_manifest_batch(
        &self,
        manifest_id: Uuid,
        sequence: u32,
        is_last: bool,
    ) -> Result<Option<CompletedManifest>> {
        let mut generation = self
            .manifest_generation
            .lock()
            .map_err(|_| TransferError::Task("manifest generation lock was poisoned".to_owned()))?;
        let active = generation.as_mut().ok_or_else(|| {
            TransferError::InvalidData("manifest generation is not active".to_owned())
        })?;
        if active.manifest_id != manifest_id {
            return Err(TransferError::InvalidData(format!(
                "manifest batch {sequence} belongs to an obsolete generation"
            )));
        }
        if active.next_sequence != sequence {
            return Err(TransferError::InvalidData(format!(
                "manifest batch {sequence} is not active"
            )));
        }
        active.next_sequence = active
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| TransferError::InvalidData("manifest sequence overflow".to_owned()))?;
        if !is_last {
            return Ok(None);
        }
        let paths = generation.take().map(|generation| CompletedManifest {
            all_paths: generation.paths,
            file_paths: generation.file_paths,
            entries: generation.entries,
        });
        Ok(paths)
    }

    pub fn commit_manifest(
        &self,
        manifest_id: Uuid,
        entries: HashMap<String, ManifestEntry>,
    ) -> Result<()> {
        *self
            .manifest_entries
            .lock()
            .map_err(|_| TransferError::Task("manifest entry lock was poisoned".to_owned()))? =
            entries;
        *self.committed_manifest_id.lock().map_err(|_| {
            TransferError::Task("committed manifest ID lock was poisoned".to_owned())
        })? = Some(manifest_id);
        self.manifest_complete.store(true, Ordering::Release);
        Ok(())
    }

    pub fn validate_manifest_id(&self, manifest_id: Uuid) -> Result<()> {
        let expected = *self.committed_manifest_id.lock().map_err(|_| {
            TransferError::Task("committed manifest ID lock was poisoned".to_owned())
        })?;
        if self.manifest_complete.load(Ordering::Acquire) && expected == Some(manifest_id) {
            Ok(())
        } else {
            Err(TransferError::InvalidData(format!(
                "manifest {manifest_id} is not active for job {}",
                self.key.job_id
            )))
        }
    }

    pub fn validate_negotiated_entry(&self, entry: &ManifestEntry) -> Result<()> {
        let entries = self
            .manifest_entries
            .lock()
            .map_err(|_| TransferError::Task("manifest entry lock was poisoned".to_owned()))?;
        if entries.get(&entry.relative_path) == Some(entry)
            && entry.file_type == fastsync_core::FileType::File
        {
            Ok(())
        } else {
            Err(TransferError::InvalidData(format!(
                "negotiated file {:?} does not match the completed manifest",
                entry.relative_path
            )))
        }
    }

    pub fn directories_deepest_first(&self) -> Result<Vec<ManifestEntry>> {
        let mut directories = self
            .directories
            .lock()
            .map_err(|_| TransferError::Task("directory state lock was poisoned".to_owned()))?
            .clone();
        directories.sort_unstable_by(|left, right| {
            right
                .relative_path
                .matches('/')
                .count()
                .cmp(&left.relative_path.matches('/').count())
                .then_with(|| right.relative_path.cmp(&left.relative_path))
        });
        Ok(directories)
    }

    pub async fn begin_operation(&self) -> Result<tokio::sync::RwLockReadGuard<'_, ()>> {
        let guard = self.lifecycle.read().await;
        if self.phase.load(Ordering::Acquire) == JOB_ACTIVE {
            Ok(guard)
        } else {
            Err(TransferError::Network(format!(
                "job {} is currently retiring",
                self.key.job_id
            )))
        }
    }

    pub async fn begin_exclusive_operation(&self) -> Result<tokio::sync::RwLockWriteGuard<'_, ()>> {
        let guard = self.lifecycle.write().await;
        if self.phase.load(Ordering::Acquire) == JOB_ACTIVE {
            Ok(guard)
        } else {
            Err(TransferError::Network(format!(
                "job {} is currently retiring",
                self.key.job_id
            )))
        }
    }

    pub async fn begin_completion(&self) -> Result<IncomingJobCompletion<'_>> {
        let guard = self.lifecycle.write().await;
        self.phase
            .compare_exchange(
                JOB_ACTIVE,
                JOB_COMPLETING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|_| {
                TransferError::InvalidData(format!(
                    "job {} is already completing or completed",
                    self.key.job_id
                ))
            })?;
        Ok(IncomingJobCompletion {
            job: self,
            _guard: guard,
            committed: false,
        })
    }

    pub fn remember_lease(&self, key: String) -> Result<()> {
        self.lease_keys
            .lock()
            .map_err(|_| TransferError::Task("destination lease lock was poisoned".to_owned()))?
            .insert(key);
        Ok(())
    }

    pub fn lease_keys(&self) -> Result<Vec<String>> {
        Ok(self
            .lease_keys
            .lock()
            .map_err(|_| TransferError::Task("destination lease lock was poisoned".to_owned()))?
            .iter()
            .cloned()
            .collect())
    }

    pub fn forget_lease(&self, key: &str) -> Result<()> {
        self.lease_keys
            .lock()
            .map_err(|_| TransferError::Task("destination lease lock was poisoned".to_owned()))?
            .remove(key);
        Ok(())
    }
}

struct ManifestGeneration {
    manifest_id: Uuid,
    next_sequence: u32,
    paths: HashSet<String>,
    file_paths: HashSet<String>,
    entries: HashMap<String, ManifestEntry>,
    required_directories: HashSet<String>,
}

pub(crate) struct IncomingJobCompletion<'a> {
    job: &'a IncomingJob,
    _guard: tokio::sync::RwLockWriteGuard<'a, ()>,
    committed: bool,
}

impl IncomingJobCompletion<'_> {
    pub fn commit(mut self) {
        self.job.phase.store(JOB_COMPLETED, Ordering::Release);
        self.committed = true;
    }
}

impl Drop for IncomingJobCompletion<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.job.phase.store(JOB_ACTIVE, Ordering::Release);
        }
    }
}

pub(crate) struct IncomingFile {
    pub file: HashedFile,
    pub destination: PathBuf,
    pub staging: PathBuf,
    pub inflight_chunks: DashMap<u64, ()>,
    pub operation_lock: tokio::sync::RwLock<()>,
    finalized: AtomicBool,
}

impl IncomingFile {
    pub fn new(file: HashedFile, destination: PathBuf, staging: PathBuf) -> Self {
        Self {
            file,
            destination,
            staging,
            inflight_chunks: DashMap::new(),
            operation_lock: tokio::sync::RwLock::new(()),
            finalized: AtomicBool::new(false),
        }
    }

    pub fn is_finalized(&self) -> bool {
        self.finalized.load(Ordering::Acquire)
    }

    pub fn mark_finalized(&self) {
        self.finalized.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use fastsync_core::{FileType, ManifestEntry, VerificationMode};
    use uuid::Uuid;

    use super::{IncomingJob, IncomingJobKey};

    fn state() -> IncomingJob {
        IncomingJob::new(
            IncomingJobKey {
                peer_id: Uuid::from_u128(1),
                job_id: Uuid::from_u128(2),
            },
            "/destination".into(),
            "/destination".into(),
            "/destination/.fastsync-stage-test".into(),
            VerificationMode::Verified,
            4 * 1024 * 1024,
        )
    }

    #[test]
    fn committed_manifest_binds_negotiation_to_id_and_metadata()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let state = state();
        let manifest_id = Uuid::from_u128(3);
        let entry = ManifestEntry::new("file.bin", 10, 20, FileType::File, false)?;
        state.begin_manifest_batch(manifest_id, 0)?;
        state.record_manifest_entry(&entry)?;
        let manifest = state
            .finish_manifest_batch(manifest_id, 0, true)?
            .ok_or("manifest was not completed")?;
        state.commit_manifest(manifest_id, manifest.entries)?;

        state.validate_manifest_id(manifest_id)?;
        state.validate_negotiated_entry(&entry)?;
        assert!(state.validate_manifest_id(Uuid::from_u128(4)).is_err());
        let changed = ManifestEntry::new("file.bin", 11, 20, FileType::File, false)?;
        assert!(state.validate_negotiated_entry(&changed).is_err());
        Ok(())
    }

    #[test]
    fn manifest_rejects_case_aliases_and_file_ancestors()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let state = state();
        state.validate_manifest_path("Folder/File")?;
        assert!(state.validate_manifest_path("folder/file").is_err());

        let manifest_id = Uuid::from_u128(5);
        state.begin_manifest_batch(manifest_id, 0)?;
        state.record_manifest_entry(&ManifestEntry::new("parent", 1, 1, FileType::File, false)?)?;
        assert!(
            state
                .record_manifest_entry(&ManifestEntry::new(
                    "parent/child",
                    1,
                    1,
                    FileType::File,
                    false,
                )?)
                .is_err()
        );
        Ok(())
    }
}
