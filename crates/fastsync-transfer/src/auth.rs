use std::net::SocketAddr;

use ed25519_dalek::{Signature, VerifyingKey};
use fastsync_protocol::{
    AGENT_VERSION, ClientHandshake, HandshakeRejection, HandshakeResponse, PROTOCOL_VERSION,
    PingRequest, PongResponse, RejectionReason, Request, RequestFrame, Response, ResponseFrame,
    ServerHandshake, read_frame, write_frame,
};
use fastsync_storage::{DiscoveredDevice, TrustedPeer};
use rand::{RngCore, rngs::OsRng};
use uuid::Uuid;

use crate::engine::{AuthenticatedConnection, PeerInfo, TransferEngine};
use crate::error::{Result, TransferError};
use crate::hashing::now_millis;
use crate::identity::device_id_from_public_key;
use crate::tls::peer_certificate_fingerprint;

const NONCE_SIZE: usize = 32;
const MAX_IDENTITY_TEXT: usize = 1024;
const MAX_FEATURES: usize = 64;
const CLIENT_DOMAIN: &[u8] = b"FastSync authenticated client handshake v2";
const SERVER_DOMAIN: &[u8] = b"FastSync authenticated server handshake v2";

impl TransferEngine {
    pub async fn probe(
        &self,
        address: SocketAddr,
        expected_device_id: Option<Uuid>,
    ) -> Result<PeerInfo> {
        let authenticated = self
            .connect_authenticated(address, expected_device_id)
            .await?;
        let nonce = OsRng.next_u64();
        let response = rpc(
            &authenticated.connection,
            Request::Ping(PingRequest { nonce }),
        )
        .await?;
        match response {
            Response::Pong(PongResponse { nonce: received }) if received == nonce => {}
            Response::Pong(_) => {
                return Err(TransferError::Authentication(
                    "probe nonce did not match".to_owned(),
                ));
            }
            other => {
                return Err(TransferError::InvalidData(format!(
                    "unexpected probe response {other:?}"
                )));
            }
        }
        authenticated
            .connection
            .close(quinn::VarInt::from_u32(0), b"probe complete");
        Ok(authenticated.peer)
    }

    pub(crate) async fn connect_authenticated(
        &self,
        address: SocketAddr,
        expected_device_id: Option<Uuid>,
    ) -> Result<AuthenticatedConnection> {
        let connecting = self
            .endpoint
            .connect(address, "fastsync.local")
            .map_err(|error| TransferError::Network(error.to_string()))?;
        let connection = connecting
            .await
            .map_err(|error| TransferError::Network(error.to_string()))?;
        let certificate_fingerprint = peer_certificate_fingerprint(&connection)?;
        let (mut send, mut receive) = connection
            .open_bi()
            .await
            .map_err(|error| TransferError::Network(error.to_string()))?;

        let mut nonce = vec![0_u8; NONCE_SIZE];
        OsRng.fill_bytes(&mut nonce);
        let mut handshake = ClientHandshake {
            protocol_version: PROTOCOL_VERSION,
            device_id: self.identity.id(),
            device_name: self.identity.name().to_owned(),
            agent_version: AGENT_VERSION.to_owned(),
            features: vec![
                "blake3".to_owned(),
                "fixed-chunk-resume".to_owned(),
                "tls-certificate-binding".to_owned(),
            ],
            nonce,
            public_key: self.identity.public_key().to_vec(),
            tls_certificate_binding: certificate_fingerprint.to_vec(),
            signature: Vec::new(),
        };
        handshake.signature = self
            .identity
            .sign(&client_transcript(&handshake))
            .to_bytes()
            .to_vec();
        write_frame(&mut send, &handshake).await?;
        send.finish()
            .map_err(|error| TransferError::Network(error.to_string()))?;

        let response: HandshakeResponse = read_frame(&mut receive).await?;
        let server = match response {
            HandshakeResponse::Accepted(server) => server,
            HandshakeResponse::Rejected(rejection) => {
                if let RejectionReason::ProtocolVersionMismatch { expected, received } =
                    rejection.reason
                {
                    return Err(TransferError::ProtocolVersionMismatch { expected, received });
                }
                return Err(TransferError::HandshakeRejected(rejection.message));
            }
        };
        let public_key = validate_server_handshake(&server, &handshake, &certificate_fingerprint)?;
        if let Some(expected) = expected_device_id
            && server.device_id != expected
        {
            return Err(TransferError::Authentication(format!(
                "connected to device {}, expected {expected}",
                server.device_id
            )));
        }
        let trusted = self.validate_trusted_key(server.device_id, &public_key)?;
        self.record_peer(server.device_id, &server.device_name, &public_key, address)?;

        Ok(AuthenticatedConnection {
            connection,
            peer: PeerInfo {
                device_id: server.device_id,
                device_name: server.device_name,
                public_key,
                certificate_fingerprint,
                address,
                agent_version: server.agent_version,
                features: server.features,
                trusted,
            },
        })
    }

    pub(crate) async fn accept_handshake(
        &self,
        connection: &quinn::Connection,
        mut send: quinn::SendStream,
        mut receive: quinn::RecvStream,
    ) -> Result<PeerInfo> {
        let client: ClientHandshake = read_frame(&mut receive).await?;
        if client.protocol_version != PROTOCOL_VERSION {
            let rejection = HandshakeRejection::protocol_version_mismatch(
                PROTOCOL_VERSION,
                client.protocol_version,
            );
            write_frame(&mut send, &HandshakeResponse::Rejected(rejection)).await?;
            send.finish()
                .map_err(|error| TransferError::Network(error.to_string()))?;
            return Err(TransferError::ProtocolVersionMismatch {
                expected: PROTOCOL_VERSION,
                received: client.protocol_version,
            });
        }

        let public_key =
            match validate_client_handshake(&client, &self.identity.certificate_fingerprint()) {
                Ok(key) => key,
                Err(error) => {
                    let reason = rejection_reason(&error);
                    let rejection = HandshakeRejection {
                        reason,
                        message: error.to_string(),
                    };
                    write_frame(&mut send, &HandshakeResponse::Rejected(rejection)).await?;
                    send.finish()
                        .map_err(|finish| TransferError::Network(finish.to_string()))?;
                    return Err(error);
                }
            };
        let trusted = match self.validate_trusted_key(client.device_id, &public_key) {
            Ok(trusted) => trusted,
            Err(error) => {
                let rejection = HandshakeRejection {
                    reason: RejectionReason::InvalidPublicKey,
                    message: error.to_string(),
                };
                write_frame(&mut send, &HandshakeResponse::Rejected(rejection)).await?;
                send.finish()
                    .map_err(|finish| TransferError::Network(finish.to_string()))?;
                return Err(error);
            }
        };

        let mut server_nonce = vec![0_u8; NONCE_SIZE];
        OsRng.fill_bytes(&mut server_nonce);
        let mut server = ServerHandshake {
            protocol_version: PROTOCOL_VERSION,
            device_id: self.identity.id(),
            device_name: self.identity.name().to_owned(),
            agent_version: AGENT_VERSION.to_owned(),
            features: vec![
                "blake3".to_owned(),
                "fixed-chunk-resume".to_owned(),
                "tls-certificate-binding".to_owned(),
            ],
            nonce: server_nonce,
            public_key: self.identity.public_key().to_vec(),
            tls_certificate_binding: self.identity.certificate_fingerprint().to_vec(),
            signature: Vec::new(),
        };
        server.signature = self
            .identity
            .sign(&server_transcript(&client, &server))
            .to_bytes()
            .to_vec();
        write_frame(&mut send, &HandshakeResponse::Accepted(server.clone())).await?;
        send.finish()
            .map_err(|error| TransferError::Network(error.to_string()))?;

        let address = connection.remote_address();
        self.record_peer(client.device_id, &client.device_name, &public_key, address)?;
        Ok(PeerInfo {
            device_id: client.device_id,
            device_name: client.device_name,
            public_key,
            certificate_fingerprint: self.identity.certificate_fingerprint(),
            address,
            agent_version: client.agent_version,
            features: client.features,
            trusted,
        })
    }

    pub(crate) fn validate_trusted_key(
        &self,
        device_id: Uuid,
        public_key: &[u8; 32],
    ) -> Result<bool> {
        let Some(peer) = self.database.get_trusted_peer(device_id)? else {
            return Ok(false);
        };
        if peer.public_key.as_slice() != public_key {
            return Err(TransferError::Authentication(format!(
                "trusted public key for device {device_id} does not match"
            )));
        }
        Ok(true)
    }

    pub(crate) fn record_peer(
        &self,
        device_id: Uuid,
        name: &str,
        public_key: &[u8; 32],
        address: SocketAddr,
    ) -> Result<()> {
        let now = now_millis()?;
        self.database.upsert_discovered_device(&DiscoveredDevice {
            device_id,
            name: name.to_owned(),
            public_key: Some(public_key.to_vec()),
            address: address.to_string(),
            first_seen: now,
            last_seen: now,
        })?;
        if let Some(existing) = self.database.get_trusted_peer(device_id)? {
            if existing.public_key.as_slice() != public_key {
                return Err(TransferError::Authentication(format!(
                    "trusted public key for device {device_id} does not match"
                )));
            }
            self.database.upsert_trusted_peer(&TrustedPeer {
                device_id,
                name: name.to_owned(),
                public_key: public_key.to_vec(),
                address: address.to_string(),
                trusted_at: existing.trusted_at,
                last_seen: now,
            })?;
        }
        Ok(())
    }
}

pub(crate) async fn rpc(connection: &quinn::Connection, request: Request) -> Result<Response> {
    let (mut send, mut receive) = connection
        .open_bi()
        .await
        .map_err(|error| TransferError::Network(error.to_string()))?;
    write_frame(&mut send, &RequestFrame::new(request)).await?;
    send.finish()
        .map_err(|error| TransferError::Network(error.to_string()))?;
    let response: ResponseFrame = read_frame(&mut receive).await?;
    let response = response.into_payload().map_err(|error| match error {
        fastsync_protocol::ProtocolError::ProtocolVersionMismatch { expected, received } => {
            TransferError::ProtocolVersionMismatch { expected, received }
        }
        other => TransferError::Protocol(other),
    })?;
    match response {
        Response::Error(error) => Err(error.into()),
        response => Ok(response),
    }
}

fn validate_client_handshake(
    handshake: &ClientHandshake,
    expected_binding: &[u8; 32],
) -> Result<[u8; 32]> {
    validate_handshake_fields(
        handshake.protocol_version,
        handshake.device_id,
        &handshake.device_name,
        &handshake.agent_version,
        &handshake.features,
        &handshake.nonce,
        &handshake.public_key,
        &handshake.tls_certificate_binding,
        expected_binding,
    )?;
    let public_key = array_32(&handshake.public_key, "client public key")?;
    verify_identity_signature(
        handshake.device_id,
        &public_key,
        &handshake.signature,
        &client_transcript(handshake),
    )?;
    Ok(public_key)
}

fn validate_server_handshake(
    handshake: &ServerHandshake,
    client: &ClientHandshake,
    expected_binding: &[u8; 32],
) -> Result<[u8; 32]> {
    validate_handshake_fields(
        handshake.protocol_version,
        handshake.device_id,
        &handshake.device_name,
        &handshake.agent_version,
        &handshake.features,
        &handshake.nonce,
        &handshake.public_key,
        &handshake.tls_certificate_binding,
        expected_binding,
    )?;
    let public_key = array_32(&handshake.public_key, "server public key")?;
    verify_identity_signature(
        handshake.device_id,
        &public_key,
        &handshake.signature,
        &server_transcript(client, handshake),
    )?;
    Ok(public_key)
}

#[allow(clippy::too_many_arguments)]
fn validate_handshake_fields(
    protocol_version: u16,
    device_id: Uuid,
    device_name: &str,
    agent_version: &str,
    features: &[String],
    nonce: &[u8],
    public_key: &[u8],
    binding: &[u8],
    expected_binding: &[u8; 32],
) -> Result<()> {
    if protocol_version != PROTOCOL_VERSION {
        return Err(TransferError::ProtocolVersionMismatch {
            expected: PROTOCOL_VERSION,
            received: protocol_version,
        });
    }
    if device_name.is_empty()
        || device_name.len() > MAX_IDENTITY_TEXT
        || agent_version.len() > MAX_IDENTITY_TEXT
        || features.len() > MAX_FEATURES
        || features
            .iter()
            .any(|feature| feature.len() > MAX_IDENTITY_TEXT)
    {
        return Err(TransferError::Authentication(
            "invalid handshake identity fields".to_owned(),
        ));
    }
    if nonce.len() != NONCE_SIZE || public_key.len() != 32 || binding.len() != 32 {
        return Err(TransferError::Authentication(
            "invalid handshake cryptographic field length".to_owned(),
        ));
    }
    if binding != expected_binding {
        return Err(TransferError::Authentication(
            "TLS certificate channel binding does not match".to_owned(),
        ));
    }
    let key = array_32(public_key, "public key")?;
    if device_id_from_public_key(&key) != device_id {
        return Err(TransferError::Authentication(
            "device ID is not derived from the supplied public key".to_owned(),
        ));
    }
    Ok(())
}

fn verify_identity_signature(
    device_id: Uuid,
    public_key: &[u8; 32],
    signature: &[u8],
    transcript: &[u8],
) -> Result<()> {
    let verifying_key = VerifyingKey::from_bytes(public_key)
        .map_err(|error| TransferError::Authentication(error.to_string()))?;
    let signature = Signature::from_slice(signature)
        .map_err(|error| TransferError::Authentication(error.to_string()))?;
    verifying_key
        .verify_strict(transcript, &signature)
        .map_err(|_| {
            TransferError::Authentication(format!(
                "invalid handshake signature from device {device_id}"
            ))
        })
}

fn client_transcript(handshake: &ClientHandshake) -> Vec<u8> {
    let mut transcript = Vec::new();
    append_field(&mut transcript, CLIENT_DOMAIN);
    transcript.extend_from_slice(&handshake.protocol_version.to_be_bytes());
    transcript.extend_from_slice(handshake.device_id.as_bytes());
    append_field(&mut transcript, handshake.device_name.as_bytes());
    append_field(&mut transcript, handshake.agent_version.as_bytes());
    append_features(&mut transcript, &handshake.features);
    append_field(&mut transcript, &handshake.nonce);
    append_field(&mut transcript, &handshake.public_key);
    append_field(&mut transcript, &handshake.tls_certificate_binding);
    transcript
}

fn server_transcript(client: &ClientHandshake, server: &ServerHandshake) -> Vec<u8> {
    let mut transcript = Vec::new();
    append_field(&mut transcript, SERVER_DOMAIN);
    append_field(&mut transcript, &client_transcript(client));
    append_field(&mut transcript, &client.signature);
    transcript.extend_from_slice(&server.protocol_version.to_be_bytes());
    transcript.extend_from_slice(client.device_id.as_bytes());
    transcript.extend_from_slice(server.device_id.as_bytes());
    append_field(&mut transcript, &client.nonce);
    append_field(&mut transcript, &server.nonce);
    append_field(&mut transcript, &client.public_key);
    append_field(&mut transcript, &server.public_key);
    append_field(&mut transcript, server.device_name.as_bytes());
    append_field(&mut transcript, server.agent_version.as_bytes());
    append_features(&mut transcript, &server.features);
    append_field(&mut transcript, &server.tls_certificate_binding);
    transcript
}

fn append_features(transcript: &mut Vec<u8>, features: &[String]) {
    transcript.extend_from_slice(&(features.len() as u64).to_be_bytes());
    for feature in features {
        append_field(transcript, feature.as_bytes());
    }
}

fn append_field(transcript: &mut Vec<u8>, field: &[u8]) {
    transcript.extend_from_slice(&(field.len() as u64).to_be_bytes());
    transcript.extend_from_slice(field);
}

fn array_32(value: &[u8], kind: &str) -> Result<[u8; 32]> {
    value
        .try_into()
        .map_err(|_| TransferError::Authentication(format!("{kind} must contain exactly 32 bytes")))
}

fn rejection_reason(error: &TransferError) -> RejectionReason {
    match error {
        TransferError::ProtocolVersionMismatch { expected, received } => {
            RejectionReason::ProtocolVersionMismatch {
                expected: *expected,
                received: *received,
            }
        }
        TransferError::Authentication(message) if message.contains("channel binding") => {
            RejectionReason::TlsCertificateBindingMismatch
        }
        TransferError::Authentication(message) if message.contains("signature") => {
            RejectionReason::InvalidSignature
        }
        TransferError::Authentication(_) => RejectionReason::InvalidPublicKey,
        _ => RejectionReason::MalformedHandshake,
    }
}
