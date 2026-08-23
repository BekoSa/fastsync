use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The first authenticated message sent by a client.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientHandshake {
    pub protocol_version: u16,
    pub device_id: Uuid,
    pub device_name: String,
    pub agent_version: String,
    #[serde(default)]
    pub features: Vec<String>,
    pub nonce: Vec<u8>,
    pub public_key: Vec<u8>,
    pub tls_certificate_binding: Vec<u8>,
    pub signature: Vec<u8>,
}

/// The authenticated handshake returned by an accepting server.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerHandshake {
    pub protocol_version: u16,
    pub device_id: Uuid,
    pub device_name: String,
    pub agent_version: String,
    #[serde(default)]
    pub features: Vec<String>,
    pub nonce: Vec<u8>,
    pub public_key: Vec<u8>,
    pub tls_certificate_binding: Vec<u8>,
    pub signature: Vec<u8>,
}

/// A server's answer to a client handshake.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", content = "data", rename_all = "snake_case")]
pub enum HandshakeResponse {
    Accepted(ServerHandshake),
    Rejected(HandshakeRejection),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandshakeRejection {
    pub reason: RejectionReason,
    pub message: String,
}

impl HandshakeRejection {
    pub fn protocol_version_mismatch(expected: u16, received: u16) -> Self {
        Self {
            reason: RejectionReason::ProtocolVersionMismatch { expected, received },
            message: format!("protocol version mismatch: expected {expected}, received {received}"),
        }
    }
}

/// Machine-readable reasons for rejecting a handshake.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "code", content = "details", rename_all = "snake_case")]
pub enum RejectionReason {
    ProtocolVersionMismatch { expected: u16, received: u16 },
    InvalidSignature,
    InvalidPublicKey,
    TlsCertificateBindingMismatch,
    UntrustedDevice,
    MalformedHandshake,
    Other { code: String },
}
