//! Wire protocol: message types + postcard (de)serialization + framing.
//!
//! One bidirectional QUIC stream per request. Frames are a `u32` little-endian
//! length prefix followed by a postcard-encoded body.

use serde::{Deserialize, Serialize, de::DeserializeOwned};

/// ALPN identifier for the popfusion protocol, version 1.
pub const ALPN: &[u8] = b"popfusion/1";

/// Maximum payload of a single `GetRange` request (client chunks larger reads).
pub const MAX_DATA_LEN: u64 = 16 * 1024 * 1024;

// AIDEV-NOTE: frame cap is MAX_DATA_LEN plus generous envelope headroom for the
// postcard enum tag / varint length / error strings. Every inbound frame length is
// validated against this before any allocation — a malicious 4 GiB length prefix
// must not cause a 4 GiB allocation.
/// Maximum size of one frame body on the wire.
pub const MAX_FRAME_LEN: u32 = MAX_DATA_LEN as u32 + 1024;

/// A single listing entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    /// Path relative to the served root, `/`-separated.
    pub path: String,
    /// Object size in bytes.
    pub size: u64,
}

/// Client → server messages.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Request {
    /// Recursive listing under `prefix` (empty prefix lists everything).
    List { prefix: String },
    /// Size + last-modified for one object.
    Stat { path: String },
    /// Read `len` bytes at `offset`. `len` must be ≤ [`MAX_DATA_LEN`].
    GetRange { path: String, offset: u64, len: u64 },
}

/// Server → client messages.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Response {
    List {
        entries: Vec<Entry>,
    },
    Stat {
        size: u64,
        last_modified: i64,
    },
    /// Exactly the requested `len` bytes, or an `Err` response instead.
    Data {
        bytes: Vec<u8>,
    },
    Err {
        code: ErrCode,
        msg: String,
    },
}

/// Error codes carried on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrCode {
    NotFound,
    Forbidden,
    RangeInvalid,
    Internal,
}

impl std::fmt::Display for ErrCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            ErrCode::NotFound => "NotFound",
            ErrCode::Forbidden => "Forbidden",
            ErrCode::RangeInvalid => "RangeInvalid",
            ErrCode::Internal => "Internal",
        };
        f.write_str(s)
    }
}

/// Protocol-level failures (framing / codec).
#[derive(Debug, thiserror::Error)]
pub enum ProtoError {
    #[error("frame of {len} bytes exceeds maximum {MAX_FRAME_LEN}")]
    FrameTooLarge { len: u64 },
    #[error("failed to encode message: {0}")]
    Encode(postcard::Error),
    #[error("failed to decode message: {0}")]
    Decode(postcard::Error),
}

/// Encode a message as a length-prefixed frame ready to write to a stream.
pub fn encode_frame<T: Serialize>(msg: &T) -> Result<Vec<u8>, ProtoError> {
    let body = postcard::to_stdvec(msg).map_err(ProtoError::Encode)?;
    let len = body.len() as u64;
    if len > MAX_FRAME_LEN as u64 {
        return Err(ProtoError::FrameTooLarge { len });
    }
    let mut frame = Vec::with_capacity(4 + body.len());
    frame.extend_from_slice(&(body.len() as u32).to_le_bytes());
    frame.extend_from_slice(&body);
    Ok(frame)
}

/// Validate a length prefix and return the body length to read.
pub fn body_len(header: [u8; 4]) -> Result<usize, ProtoError> {
    let len = u32::from_le_bytes(header);
    if len > MAX_FRAME_LEN {
        return Err(ProtoError::FrameTooLarge { len: len as u64 });
    }
    Ok(len as usize)
}

/// Decode a frame body previously sized via [`body_len`].
pub fn decode_body<T: DeserializeOwned>(body: &[u8]) -> Result<T, ProtoError> {
    postcard::from_bytes(body).map_err(ProtoError::Decode)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip<T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug>(msg: &T) {
        let frame = encode_frame(msg).expect("encode");
        let header: [u8; 4] = frame[..4].try_into().unwrap();
        let len = body_len(header).expect("valid length");
        assert_eq!(len, frame.len() - 4);
        let back: T = decode_body(&frame[4..]).expect("decode");
        assert_eq!(&back, msg);
    }

    #[test]
    fn request_round_trip() {
        round_trip(&Request::List {
            prefix: String::new(),
        });
        round_trip(&Request::Stat {
            path: "a/b.parquet".into(),
        });
        round_trip(&Request::GetRange {
            path: "x.parquet".into(),
            offset: 42,
            len: 4096,
        });
    }

    #[test]
    fn response_round_trip() {
        round_trip(&Response::List {
            entries: vec![Entry {
                path: "wfi_catalog.parquet".into(),
                size: 41_700_000_000,
            }],
        });
        round_trip(&Response::Stat {
            size: 7,
            last_modified: 1_756_000_000,
        });
        round_trip(&Response::Data {
            bytes: vec![1, 2, 3],
        });
        round_trip(&Response::Err {
            code: ErrCode::RangeInvalid,
            msg: "offset+len beyond object".into(),
        });
    }

    #[test]
    fn oversize_frame_rejected_on_encode() {
        let msg = Response::Data {
            bytes: vec![0u8; MAX_FRAME_LEN as usize + 1],
        };
        assert!(matches!(
            encode_frame(&msg),
            Err(ProtoError::FrameTooLarge { .. })
        ));
    }

    #[test]
    fn oversize_frame_rejected_on_read() {
        let header = u32::MAX.to_le_bytes();
        assert!(matches!(
            body_len(header),
            Err(ProtoError::FrameTooLarge { .. })
        ));
    }

    #[test]
    fn malformed_body_is_an_error_not_a_panic() {
        let garbage = [0xFFu8; 16];
        assert!(decode_body::<Request>(&garbage).is_err());
        assert!(decode_body::<Response>(&garbage).is_err());
    }
}
