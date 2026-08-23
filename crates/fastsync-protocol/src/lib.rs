//! Versioned wire types and CBOR framing for FastSync.
//!
//! A framed CBOR value is prefixed by its payload length as a big-endian
//! `u32`. Stream messages use [`Versioned`] so the negotiated protocol version
//! remains explicit on the wire. Chunk payload bytes are not part of a CBOR
//! value; they immediately follow a [`Request::UploadChunk`] frame.

#![forbid(unsafe_code)]

mod codec;
mod discovery;
mod handshake;
mod stream;

pub use codec::{
    LENGTH_PREFIX_SIZE, MAX_FRAME_SIZE, ProtocolError, ProtocolResult, decode_cbor, decode_frame,
    encode_cbor, encode_frame, read_frame, read_frame_with_limit, write_frame,
};
pub use discovery::DiscoveryAnnouncement;
pub use fastsync_core as core;
pub use fastsync_core::{ChunkDescriptor, ContentHash, FileType, HashedFile, ManifestEntry};
pub use handshake::{
    ClientHandshake, HandshakeRejection, HandshakeResponse, RejectionReason, ServerHandshake,
};
pub use stream::{
    Acknowledgement, ChunkAcknowledgement, ChunkHeader, CompareAction, CompareBatch,
    CompareDecision, CompareDecisionBatch, CompleteJobRequest, FileAcknowledgement, FilePlan,
    FilePlanBatch, FinalizeFileRequest, HashedFileBatch, JobAcknowledgement, PingRequest,
    PongResponse, RemoteError, RemoteErrorCode, Request, Response,
};

use serde::{Deserialize, Serialize};

/// The only protocol version implemented by this crate.
pub const PROTOCOL_VERSION: u16 = 2;

/// The FastSync agent version advertised by this build.
pub const AGENT_VERSION: &str = env!("CARGO_PKG_VERSION");

/// An explicitly versioned logical message.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Versioned<T> {
    pub protocol_version: u16,
    pub payload: T,
}

impl<T> Versioned<T> {
    /// Wraps a payload in a v1 envelope.
    pub const fn new(payload: T) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            payload,
        }
    }

    /// Checks that this envelope is a protocol version supported locally.
    pub fn validate(&self) -> ProtocolResult<()> {
        validate_protocol_version(self.protocol_version)
    }

    /// Validates the envelope and returns its payload.
    pub fn into_payload(self) -> ProtocolResult<T> {
        self.validate()?;
        Ok(self.payload)
    }
}

/// A versioned client-to-server stream frame.
pub type RequestFrame = Versioned<Request>;

/// A versioned server-to-client stream frame.
pub type ResponseFrame = Versioned<Response>;

/// Rejects protocol versions other than [`PROTOCOL_VERSION`].
pub fn validate_protocol_version(received: u16) -> ProtocolResult<()> {
    if received == PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(ProtocolError::ProtocolVersionMismatch {
            expected: PROTOCOL_VERSION,
            received,
        })
    }
}
