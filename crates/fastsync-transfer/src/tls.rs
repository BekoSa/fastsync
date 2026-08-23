use std::sync::Arc;
use std::time::Duration;

use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};

use crate::error::{Result, TransferError};
use crate::identity::DeviceIdentity;

const QUIC_IDLE_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const QUIC_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(10);

pub(crate) fn server_config(identity: &DeviceIdentity) -> Result<quinn::ServerConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let rustls_config = rustls::ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|error| TransferError::Tls(error.to_string()))?
        .with_no_client_auth()
        .with_single_cert(
            vec![CertificateDer::from(
                identity.tls_certificate_der().to_vec(),
            )],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
                identity.tls_private_key_der().to_vec(),
            )),
        )
        .map_err(|error| TransferError::Tls(error.to_string()))?;
    let quic_config = QuicServerConfig::try_from(rustls_config)
        .map_err(|error| TransferError::Tls(error.to_string()))?;
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(quic_config));
    config.transport_config(transport_config()?);
    Ok(config)
}

pub(crate) fn client_config() -> Result<quinn::ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let rustls_config = rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|error| TransferError::Tls(error.to_string()))?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptSelfSignedCertificate(provider)))
        .with_no_client_auth();
    let quic_config = QuicClientConfig::try_from(rustls_config)
        .map_err(|error| TransferError::Tls(error.to_string()))?;
    let mut config = quinn::ClientConfig::new(Arc::new(quic_config));
    config.transport_config(transport_config()?);
    Ok(config)
}

fn transport_config() -> Result<Arc<quinn::TransportConfig>> {
    let idle_timeout = quinn::IdleTimeout::try_from(QUIC_IDLE_TIMEOUT)
        .map_err(|error| TransferError::Tls(format!("invalid QUIC idle timeout: {error}")))?;
    let mut config = quinn::TransportConfig::default();
    config
        .max_idle_timeout(Some(idle_timeout))
        .keep_alive_interval(Some(QUIC_KEEP_ALIVE_INTERVAL))
        .max_concurrent_bidi_streams(quinn::VarInt::from_u32(512));
    Ok(Arc::new(config))
}

pub(crate) fn peer_certificate_fingerprint(connection: &quinn::Connection) -> Result<[u8; 32]> {
    let identity = connection.peer_identity().ok_or_else(|| {
        TransferError::Authentication("QUIC peer did not provide a TLS certificate".to_owned())
    })?;
    let certificates = identity
        .downcast::<Vec<CertificateDer<'static>>>()
        .map_err(|_| {
            TransferError::Authentication(
                "QUIC peer identity is not a rustls certificate chain".to_owned(),
            )
        })?;
    let certificate = certificates.first().ok_or_else(|| {
        TransferError::Authentication("TLS certificate chain is empty".to_owned())
    })?;
    Ok(*blake3::hash(certificate.as_ref()).as_bytes())
}

/// Certificate trust is established by the signed, channel-bound application handshake.
#[derive(Debug)]
struct AcceptSelfSignedCertificate(Arc<rustls::crypto::CryptoProvider>);

impl ServerCertVerifier for AcceptSelfSignedCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signed: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            certificate,
            signed,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signed: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            certificate,
            signed,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}
