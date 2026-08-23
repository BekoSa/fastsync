use std::error::Error;

use fastsync_protocol::core::VerificationMode;
use fastsync_protocol::{
    ChunkHeader, CompareAction, CompareBatch, CompareDecision, CompareDecisionBatch, FileType,
    HandshakeRejection, HandshakeResponse, MAX_FRAME_SIZE, ManifestEntry, PROTOCOL_VERSION,
    ProtocolError, RejectionReason, Request, RequestFrame, Versioned, decode_cbor, encode_cbor,
    encode_frame, read_frame, read_frame_with_limit, write_frame,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

#[tokio::test]
async fn request_batch_roundtrips_through_async_framing() -> Result<(), Box<dyn Error>> {
    let request = RequestFrame::new(Request::Compare(CompareBatch {
        job_id: Uuid::from_u128(1),
        manifest_id: Uuid::from_u128(10),
        destination_root: "D:/Games".to_owned(),
        verification_mode: VerificationMode::Verified,
        chunk_size: 4 * 1024 * 1024,
        sequence: 7,
        is_last: true,
        entries: vec![
            ManifestEntry::new("docs/guide.txt", 42, 123_456, FileType::File, false)?,
            ManifestEntry::new("images", 0, 234_567, FileType::Directory, false)?,
        ],
    }));

    let (mut sender, mut receiver) = tokio::io::duplex(4096);
    write_frame(&mut sender, &request).await?;
    let decoded: RequestFrame = read_frame(&mut receiver).await?;

    assert_eq!(decoded, request);
    Ok(())
}

#[test]
fn compare_decision_batch_roundtrips_as_cbor() -> Result<(), Box<dyn Error>> {
    let decisions = CompareDecisionBatch {
        job_id: Uuid::from_u128(2),
        manifest_id: Uuid::from_u128(20),
        sequence: 3,
        is_last: false,
        decisions: vec![
            CompareDecision {
                path: "same.txt".to_owned(),
                action: CompareAction::Unchanged,
            },
            CompareDecision {
                path: "changed.bin".to_owned(),
                action: CompareAction::NeedHash,
            },
        ],
    };

    let encoded = encode_cbor(&decisions)?;
    let decoded: CompareDecisionBatch = decode_cbor(&encoded)?;
    assert_eq!(decoded, decisions);
    Ok(())
}

#[test]
fn frame_length_prefix_is_big_endian() -> Result<(), Box<dyn Error>> {
    let frame = encode_frame(&())?;
    assert_eq!(&frame[..4], &[0, 0, 0, 1]);
    Ok(())
}

#[tokio::test]
async fn raw_chunk_bytes_remain_after_the_header_frame() -> Result<(), Box<dyn Error>> {
    let header = ChunkHeader {
        job_id: Uuid::from_u128(3),
        manifest_id: Uuid::from_u128(30),
        path: "payload.bin".to_owned(),
        index: 0,
        offset: 0,
        size: 4,
        hash: [7; 32],
    };
    let request = RequestFrame::new(Request::UploadChunk(header));
    let raw = [1_u8, 2, 3, 4];
    let (mut sender, mut receiver) = tokio::io::duplex(4096);

    write_frame(&mut sender, &request).await?;
    sender.write_all(&raw).await?;

    let decoded: RequestFrame = read_frame(&mut receiver).await?;
    let mut received_raw = [0_u8; 4];
    receiver.read_exact(&mut received_raw).await?;

    assert_eq!(decoded, request);
    assert_eq!(received_raw, raw);
    Ok(())
}

#[tokio::test]
async fn write_rejects_oversized_frame() -> Result<(), Box<dyn Error>> {
    let oversized = vec![0_u8; MAX_FRAME_SIZE + 1];
    let (mut sender, _receiver) = tokio::io::duplex(64);
    let error = write_frame(&mut sender, &oversized)
        .await
        .err()
        .ok_or("oversized frame was accepted")?;

    assert!(matches!(error, ProtocolError::FrameTooLarge { .. }));
    Ok(())
}

#[tokio::test]
async fn read_rejects_oversized_declared_length() -> Result<(), Box<dyn Error>> {
    let (mut sender, mut receiver) = tokio::io::duplex(64);
    let length = u32::try_from(MAX_FRAME_SIZE + 1)?;
    sender.write_all(&length.to_be_bytes()).await?;

    let error = read_frame::<_, Vec<u8>>(&mut receiver)
        .await
        .err()
        .ok_or("oversized declared length was accepted")?;
    assert!(matches!(error, ProtocolError::FrameTooLarge { .. }));
    Ok(())
}

#[tokio::test]
async fn read_applies_a_smaller_caller_limit_before_allocating() -> Result<(), Box<dyn Error>> {
    let (mut sender, mut receiver) = tokio::io::duplex(64);
    sender.write_all(&65_u32.to_be_bytes()).await?;

    let error = read_frame_with_limit::<_, Vec<u8>>(&mut receiver, 64)
        .await
        .err()
        .ok_or("caller frame limit was ignored")?;
    assert!(matches!(
        error,
        ProtocolError::FrameTooLarge {
            size: 65,
            maximum: 64
        }
    ));
    Ok(())
}

#[tokio::test]
async fn truncated_prefix_and_payload_are_reported() -> Result<(), Box<dyn Error>> {
    let (mut prefix_sender, mut prefix_receiver) = tokio::io::duplex(64);
    prefix_sender.write_all(&[0, 0]).await?;
    prefix_sender.shutdown().await?;

    let prefix_error = read_frame::<_, Vec<u8>>(&mut prefix_receiver)
        .await
        .err()
        .ok_or("truncated prefix was accepted")?;
    assert!(matches!(
        prefix_error,
        ProtocolError::TruncatedLengthPrefix { received: 2 }
    ));

    let (mut payload_sender, mut payload_receiver) = tokio::io::duplex(64);
    payload_sender.write_all(&10_u32.to_be_bytes()).await?;
    payload_sender.write_all(&[1, 2, 3]).await?;
    payload_sender.shutdown().await?;

    let payload_error = read_frame::<_, Vec<u8>>(&mut payload_receiver)
        .await
        .err()
        .ok_or("truncated payload was accepted")?;
    assert!(matches!(
        payload_error,
        ProtocolError::TruncatedFrame {
            expected: 10,
            received: 3
        }
    ));
    Ok(())
}

#[test]
fn protocol_version_rejection_has_stable_representation() -> Result<(), Box<dyn Error>> {
    let rejection = HandshakeResponse::Rejected(HandshakeRejection::protocol_version_mismatch(
        PROTOCOL_VERSION,
        2,
    ));
    let json = serde_json::to_value(&rejection)?;

    assert_eq!(json["status"], "rejected");
    assert_eq!(json["data"]["reason"]["code"], "protocol_version_mismatch");
    assert_eq!(
        json["data"]["reason"]["details"]["expected"],
        PROTOCOL_VERSION
    );
    assert_eq!(json["data"]["reason"]["details"]["received"], 2);

    let encoded = encode_cbor(&rejection)?;
    let decoded: HandshakeResponse = decode_cbor(&encoded)?;
    assert_eq!(decoded, rejection);
    assert!(matches!(
        decoded,
        HandshakeResponse::Rejected(HandshakeRejection {
            reason: RejectionReason::ProtocolVersionMismatch {
                expected: PROTOCOL_VERSION,
                received: 2
            },
            ..
        })
    ));
    Ok(())
}

#[test]
fn versioned_payload_rejects_an_unknown_version() -> Result<(), Box<dyn Error>> {
    let frame = Versioned {
        protocol_version: PROTOCOL_VERSION + 1,
        payload: (),
    };
    let error = frame
        .into_payload()
        .err()
        .ok_or("unknown protocol version was accepted")?;

    assert!(matches!(
        error,
        ProtocolError::ProtocolVersionMismatch {
            expected: PROTOCOL_VERSION,
            received
        } if received == PROTOCOL_VERSION + 1
    ));
    Ok(())
}
