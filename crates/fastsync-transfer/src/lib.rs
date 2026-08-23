//! Authenticated QUIC transfer engine for FastSync.

#![forbid(unsafe_code)]

mod auth;
mod engine;
mod error;
mod filesystem;
mod hashing;
mod identity;
mod receiver;
mod sender;
mod state;
mod tls;

pub use engine::{PeerInfo, TransferEngine};
pub use error::{Result, TransferError};
pub use identity::{DeviceIdentity, device_id_from_public_key};

pub type AuthenticatedPeerInfo = PeerInfo;

/// Largest buffer used while streaming file contents over QUIC.
pub const TRANSFER_BUFFER_SIZE: usize = 256 * 1024;
