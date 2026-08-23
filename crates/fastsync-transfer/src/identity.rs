use std::sync::Arc;

use ed25519_dalek::{Signature, Signer, SigningKey};
use fastsync_storage::Database;
use rand::rngs::OsRng;
use uuid::Uuid;

use crate::error::{Result, TransferError};

const SIGNING_KEY_SETTING: &str = "transfer.identity.ed25519_signing_key";
const DEVICE_NAME_SETTING: &str = "transfer.identity.device_name";
const TLS_CERTIFICATE_SETTING: &str = "transfer.identity.tls_certificate_der";
const TLS_PRIVATE_KEY_SETTING: &str = "transfer.identity.tls_private_key_der";

#[derive(Clone)]
pub struct DeviceIdentity {
    id: Uuid,
    name: Arc<str>,
    signing_key: Arc<SigningKey>,
    public_key: [u8; 32],
    tls_certificate_der: Arc<[u8]>,
    tls_private_key_der: Arc<[u8]>,
    certificate_fingerprint: [u8; 32],
}

impl std::fmt::Debug for DeviceIdentity {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DeviceIdentity")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("public_key", &self.public_key)
            .field("certificate_fingerprint", &self.certificate_fingerprint)
            .finish_non_exhaustive()
    }
}

impl DeviceIdentity {
    /// Loads an identity from SQLite, creating and persisting it on first use.
    pub fn load_or_create(database: Database, device_name: impl Into<String>) -> Result<Self> {
        let device_name = device_name.into();
        if device_name.trim().is_empty() {
            return Err(TransferError::Identity(
                "device name must not be empty".to_owned(),
            ));
        }

        let signing_key = match database.get_bytes(SIGNING_KEY_SETTING)? {
            Some(encoded) => {
                let length = encoded.len();
                let bytes: [u8; 32] = encoded.try_into().map_err(|_| {
                    TransferError::Identity(format!(
                        "stored Ed25519 signing key has {length} bytes, expected 32"
                    ))
                })?;
                SigningKey::from_bytes(&bytes)
            }
            None => {
                let key = SigningKey::generate(&mut OsRng);
                database.set_bytes(SIGNING_KEY_SETTING, &key.to_bytes())?;
                key
            }
        };

        let (certificate_der, private_key_der) = match (
            database.get_bytes(TLS_CERTIFICATE_SETTING)?,
            database.get_bytes(TLS_PRIVATE_KEY_SETTING)?,
        ) {
            (Some(certificate), Some(private_key))
                if !certificate.is_empty() && !private_key.is_empty() =>
            {
                (certificate, private_key)
            }
            _ => {
                let certified =
                    rcgen::generate_simple_self_signed(vec!["fastsync.local".to_owned()])
                        .map_err(|error| TransferError::Tls(error.to_string()))?;
                let certificate = certified.cert.der().to_vec();
                let private_key = certified.key_pair.serialize_der();
                database.set_bytes_batch(&[
                    (TLS_CERTIFICATE_SETTING, &certificate),
                    (TLS_PRIVATE_KEY_SETTING, &private_key),
                ])?;
                (certificate, private_key)
            }
        };

        database.set_string(DEVICE_NAME_SETTING, &device_name)?;
        let public_key = signing_key.verifying_key().to_bytes();
        let id = device_id_from_public_key(&public_key);
        let certificate_fingerprint = *blake3::hash(&certificate_der).as_bytes();

        Ok(Self {
            id,
            name: Arc::from(device_name),
            signing_key: Arc::new(signing_key),
            public_key,
            tls_certificate_der: Arc::from(certificate_der),
            tls_private_key_der: Arc::from(private_key_der),
            certificate_fingerprint,
        })
    }

    pub const fn id(&self) -> Uuid {
        self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub const fn public_key(&self) -> [u8; 32] {
        self.public_key
    }

    pub const fn certificate_fingerprint(&self) -> [u8; 32] {
        self.certificate_fingerprint
    }

    pub const fn cert_fingerprint(&self) -> [u8; 32] {
        self.certificate_fingerprint
    }

    pub fn tls_certificate_der(&self) -> &[u8] {
        &self.tls_certificate_der
    }

    pub(crate) fn tls_private_key_der(&self) -> &[u8] {
        &self.tls_private_key_der
    }

    pub(crate) fn sign(&self, message: &[u8]) -> Signature {
        self.signing_key.sign(message)
    }
}

/// Derives a stable RFC 9562 UUIDv8 from an Ed25519 public key.
pub fn device_id_from_public_key(public_key: &[u8; 32]) -> Uuid {
    let digest = blake3::hash(public_key);
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_is_persistent_and_uuid_is_well_formed() -> Result<()> {
        let database = Database::open_in_memory()?;
        let first = DeviceIdentity::load_or_create(database.clone(), "first")?;
        let second = DeviceIdentity::load_or_create(database, "renamed")?;

        assert_eq!(first.id(), second.id());
        assert_eq!(first.public_key(), second.public_key());
        assert_eq!(
            first.certificate_fingerprint(),
            second.certificate_fingerprint()
        );
        assert_eq!(second.id().as_bytes()[6] >> 4, 8);
        assert_eq!(second.id().as_bytes()[8] & 0xc0, 0x80);
        assert_eq!(second.name(), "renamed");
        Ok(())
    }
}
