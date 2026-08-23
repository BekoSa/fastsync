use std::{
    io::{self, Cursor},
    mem::size_of,
};

use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Number of bytes in the big-endian frame-length prefix.
pub const LENGTH_PREFIX_SIZE: usize = size_of::<u32>();

/// Maximum permitted CBOR payload size (16 MiB).
pub const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

pub type ProtocolResult<T> = Result<T, ProtocolError>;

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("failed to encode CBOR: {0}")]
    CborEncode(String),

    #[error("failed to decode CBOR: {0}")]
    CborDecode(String),

    #[error("CBOR payload has {remaining} trailing bytes")]
    TrailingCborData { remaining: usize },

    #[error("frame payload is {size} bytes; maximum is {maximum} bytes")]
    FrameTooLarge { size: usize, maximum: usize },

    #[error("frame length prefix is truncated: received {received} of 4 bytes")]
    TruncatedLengthPrefix { received: usize },

    #[error("frame payload is truncated: expected {expected} bytes, received {received}")]
    TruncatedFrame { expected: usize, received: usize },

    #[error("frame has {extra} trailing bytes after its declared payload")]
    TrailingFrameData { extra: usize },

    #[error("protocol version mismatch: expected {expected}, received {received}")]
    ProtocolVersionMismatch { expected: u16, received: u16 },

    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

/// Encodes a value as one CBOR value without a length prefix.
pub fn encode_cbor<T>(value: &T) -> ProtocolResult<Vec<u8>>
where
    T: Serialize + ?Sized,
{
    let mut payload = Vec::new();
    ciborium::ser::into_writer(value, &mut payload)
        .map_err(|error| ProtocolError::CborEncode(error.to_string()))?;
    Ok(payload)
}

/// Decodes exactly one CBOR value and rejects trailing data.
pub fn decode_cbor<T>(payload: &[u8]) -> ProtocolResult<T>
where
    T: DeserializeOwned,
{
    let mut cursor = Cursor::new(payload);
    let value = ciborium::de::from_reader(&mut cursor)
        .map_err(|error| ProtocolError::CborDecode(error.to_string()))?;
    let consumed = cursor.position() as usize;

    if consumed != payload.len() {
        return Err(ProtocolError::TrailingCborData {
            remaining: payload.len() - consumed,
        });
    }

    Ok(value)
}

/// Encodes a complete length-prefixed frame.
pub fn encode_frame<T>(value: &T) -> ProtocolResult<Vec<u8>>
where
    T: Serialize + ?Sized,
{
    let payload = encode_cbor(value)?;
    let length = frame_length(payload.len())?;
    let mut frame = Vec::with_capacity(LENGTH_PREFIX_SIZE + payload.len());
    frame.extend_from_slice(&length.to_be_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

/// Decodes one complete length-prefixed frame from memory.
pub fn decode_frame<T>(frame: &[u8]) -> ProtocolResult<T>
where
    T: DeserializeOwned,
{
    if frame.len() < LENGTH_PREFIX_SIZE {
        return Err(ProtocolError::TruncatedLengthPrefix {
            received: frame.len(),
        });
    }

    let mut prefix = [0_u8; LENGTH_PREFIX_SIZE];
    prefix.copy_from_slice(&frame[..LENGTH_PREFIX_SIZE]);
    let length = u32::from_be_bytes(prefix) as usize;
    ensure_frame_size(length)?;

    let payload = &frame[LENGTH_PREFIX_SIZE..];
    if payload.len() < length {
        return Err(ProtocolError::TruncatedFrame {
            expected: length,
            received: payload.len(),
        });
    }
    if payload.len() > length {
        return Err(ProtocolError::TrailingFrameData {
            extra: payload.len() - length,
        });
    }

    decode_cbor(payload)
}

/// Writes one CBOR frame to an asynchronous byte stream.
pub async fn write_frame<W, T>(writer: &mut W, value: &T) -> ProtocolResult<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize + ?Sized,
{
    let payload = encode_cbor(value)?;
    let length = frame_length(payload.len())?;
    writer.write_all(&length.to_be_bytes()).await?;
    writer.write_all(&payload).await?;
    Ok(())
}

/// Reads one CBOR frame from an asynchronous byte stream.
///
/// The declared length is checked before allocating the payload buffer.
pub async fn read_frame<R, T>(reader: &mut R) -> ProtocolResult<T>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    read_frame_with_limit(reader, MAX_FRAME_SIZE).await
}

/// Reads one CBOR frame while applying a caller-specific allocation limit.
pub async fn read_frame_with_limit<R, T>(reader: &mut R, maximum: usize) -> ProtocolResult<T>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let maximum = maximum.min(MAX_FRAME_SIZE);
    let mut prefix = [0_u8; LENGTH_PREFIX_SIZE];
    let mut prefix_read = 0;
    while prefix_read < prefix.len() {
        match reader.read(&mut prefix[prefix_read..]).await {
            Ok(0) => {
                return Err(ProtocolError::TruncatedLengthPrefix {
                    received: prefix_read,
                });
            }
            Ok(read) => prefix_read += read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(ProtocolError::Io(error)),
        }
    }

    let length = u32::from_be_bytes(prefix) as usize;
    if length > maximum {
        return Err(ProtocolError::FrameTooLarge {
            size: length,
            maximum,
        });
    }

    let mut payload = vec![0_u8; length];
    let mut payload_read = 0;
    while payload_read < payload.len() {
        match reader.read(&mut payload[payload_read..]).await {
            Ok(0) => {
                return Err(ProtocolError::TruncatedFrame {
                    expected: length,
                    received: payload_read,
                });
            }
            Ok(read) => payload_read += read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(ProtocolError::Io(error)),
        }
    }

    decode_cbor(&payload)
}

fn ensure_frame_size(size: usize) -> ProtocolResult<()> {
    if size > MAX_FRAME_SIZE {
        Err(ProtocolError::FrameTooLarge {
            size,
            maximum: MAX_FRAME_SIZE,
        })
    } else {
        Ok(())
    }
}

fn frame_length(size: usize) -> ProtocolResult<u32> {
    ensure_frame_size(size)?;
    u32::try_from(size).map_err(|_| ProtocolError::FrameTooLarge {
        size,
        maximum: MAX_FRAME_SIZE,
    })
}
