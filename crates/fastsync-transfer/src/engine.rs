use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use dashmap::DashMap;
use fastsync_storage::Database;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use uuid::Uuid;

use crate::error::{Result, TransferError};
use crate::identity::DeviceIdentity;
use crate::state::{DestinationLease, IncomingJob, IncomingJobKey};
use crate::tls;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerInfo {
    pub device_id: Uuid,
    pub device_name: String,
    pub public_key: [u8; 32],
    pub certificate_fingerprint: [u8; 32],
    pub address: SocketAddr,
    pub agent_version: String,
    pub features: Vec<String>,
    pub trusted: bool,
}

pub(crate) struct AuthenticatedConnection {
    pub connection: quinn::Connection,
    pub peer: PeerInfo,
}

#[derive(Clone)]
pub struct TransferEngine {
    pub(crate) endpoint: quinn::Endpoint,
    pub(crate) identity: DeviceIdentity,
    pub(crate) database: Database,
    pub(crate) incoming_jobs: Arc<DashMap<IncomingJobKey, Arc<IncomingJob>>>,
    pub(crate) destination_leases: Arc<DashMap<String, DestinationLease>>,
    pub(crate) destination_lease_lifecycle: Arc<Mutex<()>>,
    pub(crate) destination_root_lifecycle: Arc<tokio::sync::Mutex<()>>,
    pub(crate) hash_semaphore: Arc<Semaphore>,
    pub(crate) connection_semaphore: Arc<Semaphore>,
    pub(crate) request_semaphore: Arc<Semaphore>,
    pub(crate) active_job_connections: Arc<DashMap<IncomingJobKey, usize>>,
    pub(crate) active_connections: Arc<DashMap<(Uuid, Uuid), quinn::Connection>>,
    pub(crate) peer_connection_lifecycle: Arc<tokio::sync::Mutex<()>>,
}

impl TransferEngine {
    pub fn bind(
        bind_address: SocketAddr,
        database: Database,
        identity: DeviceIdentity,
    ) -> Result<Self> {
        let server_config = tls::server_config(&identity)?;
        let mut endpoint =
            quinn::Endpoint::server(server_config, bind_address).map_err(|error| {
                TransferError::io("binding QUIC endpoint", bind_address.to_string(), error)
            })?;
        endpoint.set_default_client_config(tls::client_config()?);
        let hash_workers = std::thread::available_parallelism()
            .map(|workers| workers.get())
            .unwrap_or(2)
            .clamp(1, 8);

        Ok(Self {
            endpoint,
            identity,
            database,
            incoming_jobs: Arc::new(DashMap::new()),
            destination_leases: Arc::new(DashMap::new()),
            destination_lease_lifecycle: Arc::new(Mutex::new(())),
            destination_root_lifecycle: Arc::new(tokio::sync::Mutex::new(())),
            hash_semaphore: Arc::new(Semaphore::new(hash_workers)),
            connection_semaphore: Arc::new(Semaphore::new(32)),
            request_semaphore: Arc::new(Semaphore::new(64)),
            active_job_connections: Arc::new(DashMap::new()),
            active_connections: Arc::new(DashMap::new()),
            peer_connection_lifecycle: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    pub fn new(
        bind_address: SocketAddr,
        database: Database,
        identity: DeviceIdentity,
    ) -> Result<Self> {
        Self::bind(bind_address, database, identity)
    }

    pub fn load_and_bind(
        bind_address: SocketAddr,
        database: Database,
        device_name: impl Into<String>,
    ) -> Result<Self> {
        let identity = DeviceIdentity::load_or_create(database.clone(), device_name)?;
        Self::bind(bind_address, database, identity)
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.endpoint.local_addr()
    }

    pub const fn identity(&self) -> &DeviceIdentity {
        &self.identity
    }

    pub fn close(&self) {
        self.endpoint
            .close(quinn::VarInt::from_u32(0), b"FastSync endpoint closed");
    }

    pub fn disconnect_peer(&self, peer_id: Uuid) {
        let connections: Vec<_> = self
            .active_connections
            .iter()
            .filter(|entry| entry.key().0 == peer_id)
            .map(|entry| entry.value().clone())
            .collect();
        for connection in connections {
            connection.close(quinn::VarInt::from_u32(0), b"peer trust revoked");
        }
    }
}
