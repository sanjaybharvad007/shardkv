//! Wire protocol.
//!
//! Every message, in both directions, is one length-prefixed frame:
//!
//! ```text
//! +----------------+--------+---------------------+
//! | body_len: u32  | tag    | payload             |
//! | big-endian     | 1 byte | body_len - 1 bytes  |
//! +----------------+--------+---------------------+
//! ```
//!
//! `tag` is an opcode in a request and a status in a response.
//!
//! | Request | Opcode | Payload                               |
//! |---------|--------|---------------------------------------|
//! | PING    | 0x01   | empty                                 |
//! | GET     | 0x02   | key                                   |
//! | SET     | 0x03   | key_len: u16 big-endian, key, value   |
//! | DEL     | 0x04   | key                                   |
//! | STATS   | 0x05   | empty                                 |
//!
//! | Response  | Status | Payload                                   |
//! |-----------|--------|-------------------------------------------|
//! | OK        | 0x00   | value for GET, text for STATS, else empty |
//! | NOT_FOUND | 0x01   | empty                                     |
//! | ERROR     | 0x02   | UTF-8 message                             |
//!
//! Responses come back in request order, so a client may pipeline: send many
//! requests before reading any response.
//!
//! Parsing borrows from the input buffer. A decoded [`Request`] holds slices
//! into the connection's read buffer, so decoding a request allocates nothing.

use std::fmt;

use bytes::{BufMut, BytesMut};

/// Bytes in the length prefix.
pub const HEADER_LEN: usize = 4;
/// Default cap on `body_len`. Larger frames are refused before being buffered.
pub const DEFAULT_MAX_FRAME: usize = 8 * 1024 * 1024;
/// Keys are length-prefixed with a u16 inside SET.
pub const MAX_KEY_LEN: usize = u16::MAX as usize;

const OP_PING: u8 = 0x01;
const OP_GET: u8 = 0x02;
const OP_SET: u8 = 0x03;
const OP_DEL: u8 = 0x04;
const OP_STATS: u8 = 0x05;

/// A decoded request that borrows its key and value from the read buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Request<'a> {
    Ping,
    Get { key: &'a [u8] },
    Set { key: &'a [u8], value: &'a [u8] },
    Del { key: &'a [u8] },
    Stats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Status {
    Ok = 0x00,
    NotFound = 0x01,
    Error = 0x02,
}

/// A decoded response that borrows its payload from the read buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Response<'a> {
    pub status: Status,
    pub body: &'a [u8],
}

/// The byte stream itself is unusable: the peer must be disconnected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameError {
    /// `body_len` was zero; a frame always carries at least the tag byte.
    Empty,
    /// `body_len` exceeded the configured maximum.
    TooLarge { len: usize, max: usize },
}

/// The frame was well delimited but its contents were invalid. The stream is
/// still in sync, so the server answers with ERROR and keeps the connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestError {
    UnknownOpcode(u8),
    EmptyKey,
    KeyTooLong,
    /// The payload was shorter than its own length fields claim.
    Truncated,
    /// A request that takes no payload carried one.
    UnexpectedPayload,
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrameError::Empty => write!(f, "empty frame"),
            FrameError::TooLarge { len, max } => {
                write!(f, "frame of {len} bytes exceeds the {max} byte limit")
            }
        }
    }
}

impl fmt::Display for RequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RequestError::UnknownOpcode(op) => write!(f, "unknown opcode 0x{op:02x}"),
            RequestError::EmptyKey => write!(f, "key must not be empty"),
            RequestError::KeyTooLong => write!(f, "key longer than {MAX_KEY_LEN} bytes"),
            RequestError::Truncated => write!(f, "payload shorter than its length field"),
            RequestError::UnexpectedPayload => write!(f, "request takes no payload"),
        }
    }
}

impl std::error::Error for FrameError {}
impl std::error::Error for RequestError {}

/// One frame split off the front of a buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Frame<'a> {
    pub tag: u8,
    pub payload: &'a [u8],
    /// Bytes this frame occupies in the buffer, header included.
    pub consumed: usize,
}

/// Splits one frame off the front of `buf`.
///
/// Returns `Ok(None)` when `buf` does not yet hold a complete frame. An
/// oversized frame is reported as soon as its header arrives, before any of
/// its body is buffered.
pub fn split_frame(buf: &[u8], max_frame: usize) -> Result<Option<Frame<'_>>, FrameError> {
    let Some(header) = buf.first_chunk::<HEADER_LEN>() else {
        return Ok(None);
    };
    let len = u32::from_be_bytes(*header) as usize;
    if len == 0 {
        return Err(FrameError::Empty);
    }
    if len > max_frame {
        return Err(FrameError::TooLarge {
            len,
            max: max_frame,
        });
    }
    let Some(body) = buf.get(HEADER_LEN..HEADER_LEN + len) else {
        return Ok(None);
    };
    Ok(Some(Frame {
        tag: body[0],
        payload: &body[1..],
        consumed: HEADER_LEN + len,
    }))
}

impl<'a> Request<'a> {
    /// Decodes the contents of one frame.
    pub fn decode(frame: &Frame<'a>) -> Result<Self, RequestError> {
        let payload = frame.payload;
        let key_only = |key: &'a [u8]| match key.len() {
            0 => Err(RequestError::EmptyKey),
            n if n > MAX_KEY_LEN => Err(RequestError::KeyTooLong),
            _ => Ok(key),
        };
        let no_payload = |req| {
            if payload.is_empty() {
                Ok(req)
            } else {
                Err(RequestError::UnexpectedPayload)
            }
        };
        match frame.tag {
            OP_PING => no_payload(Request::Ping),
            OP_STATS => no_payload(Request::Stats),
            OP_GET => Ok(Request::Get {
                key: key_only(payload)?,
            }),
            OP_DEL => Ok(Request::Del {
                key: key_only(payload)?,
            }),
            OP_SET => {
                let Some((len, rest)) = payload.split_first_chunk::<2>() else {
                    return Err(RequestError::Truncated);
                };
                let key_len = u16::from_be_bytes(*len) as usize;
                if key_len == 0 {
                    return Err(RequestError::EmptyKey);
                }
                if rest.len() < key_len {
                    return Err(RequestError::Truncated);
                }
                let (key, value) = rest.split_at(key_len);
                Ok(Request::Set { key, value })
            }
            other => Err(RequestError::UnknownOpcode(other)),
        }
    }

    /// Appends this request to `out` as one frame.
    ///
    /// # Panics
    /// Panics if a key is empty or longer than [`MAX_KEY_LEN`], or if the
    /// frame body would not fit in a u32. Those are caller bugs, not data
    /// errors; [`crate::Client`] validates before calling.
    pub fn encode(&self, out: &mut BytesMut) {
        let check = |key: &[u8]| {
            assert!(
                !key.is_empty() && key.len() <= MAX_KEY_LEN,
                "invalid key length"
            );
        };
        match *self {
            Request::Ping => put_frame(out, OP_PING, &[]),
            Request::Stats => put_frame(out, OP_STATS, &[]),
            Request::Get { key } => {
                check(key);
                put_frame(out, OP_GET, &[key]);
            }
            Request::Del { key } => {
                check(key);
                put_frame(out, OP_DEL, &[key]);
            }
            Request::Set { key, value } => {
                check(key);
                let key_len = (key.len() as u16).to_be_bytes();
                put_frame(out, OP_SET, &[&key_len, key, value]);
            }
        }
    }
}

impl<'a> Response<'a> {
    pub fn decode(frame: &Frame<'a>) -> Option<Self> {
        let status = match frame.tag {
            0x00 => Status::Ok,
            0x01 => Status::NotFound,
            0x02 => Status::Error,
            _ => return None,
        };
        Some(Response {
            status,
            body: frame.payload,
        })
    }
}

/// Appends a response frame to `out`.
pub fn encode_response(out: &mut BytesMut, status: Status, body: &[u8]) {
    put_frame(out, status as u8, &[body]);
}

fn put_frame(out: &mut BytesMut, tag: u8, parts: &[&[u8]]) {
    let payload: usize = parts.iter().map(|p| p.len()).sum();
    let body_len = u32::try_from(payload + 1).expect("frame body exceeds u32");
    out.reserve(HEADER_LEN + 1 + payload);
    out.put_u32(body_len);
    out.put_u8(tag);
    for part in parts {
        out.put_slice(part);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encoded(req: Request<'_>) -> BytesMut {
        let mut buf = BytesMut::new();
        req.encode(&mut buf);
        buf
    }

    fn roundtrip(req: Request<'_>) {
        let buf = encoded(req);
        let frame = split_frame(&buf, DEFAULT_MAX_FRAME).unwrap().unwrap();
        assert_eq!(frame.consumed, buf.len());
        assert_eq!(Request::decode(&frame).unwrap(), req);
    }

    #[test]
    fn requests_roundtrip() {
        roundtrip(Request::Ping);
        roundtrip(Request::Stats);
        roundtrip(Request::Get { key: b"k" });
        roundtrip(Request::Del { key: b"some key" });
        roundtrip(Request::Set {
            key: b"k",
            value: b"",
        });
        roundtrip(Request::Set {
            key: b"key",
            value: b"value with \0 bytes \xff",
        });
        let big = vec![7u8; 100_000];
        roundtrip(Request::Set {
            key: &[1u8; MAX_KEY_LEN],
            value: &big,
        });
    }

    #[test]
    fn response_roundtrip() {
        let mut buf = BytesMut::new();
        encode_response(&mut buf, Status::Ok, b"hello");
        encode_response(&mut buf, Status::NotFound, b"");
        let f1 = split_frame(&buf, DEFAULT_MAX_FRAME).unwrap().unwrap();
        let r1 = Response::decode(&f1).unwrap();
        assert_eq!((r1.status, r1.body), (Status::Ok, &b"hello"[..]));
        let f2 = split_frame(&buf[f1.consumed..], DEFAULT_MAX_FRAME)
            .unwrap()
            .unwrap();
        assert_eq!(Response::decode(&f2).unwrap().status, Status::NotFound);
        assert_eq!(f1.consumed + f2.consumed, buf.len());
    }

    /// Every strict prefix of a frame is "incomplete", never an error and
    /// never a wrong frame. This is what a slow or fragmenting peer produces.
    #[test]
    fn every_prefix_is_incomplete() {
        let buf = encoded(Request::Set {
            key: b"key",
            value: b"some value",
        });
        for cut in 0..buf.len() {
            assert_eq!(
                split_frame(&buf[..cut], DEFAULT_MAX_FRAME),
                Ok(None),
                "cut at {cut}"
            );
        }
        assert!(split_frame(&buf, DEFAULT_MAX_FRAME).unwrap().is_some());
    }

    #[test]
    fn several_frames_in_one_buffer() {
        let mut buf = BytesMut::new();
        let reqs = [
            Request::Get { key: b"a" },
            Request::Set {
                key: b"b",
                value: b"1",
            },
            Request::Ping,
            Request::Del { key: b"c" },
        ];
        for r in &reqs {
            r.encode(&mut buf);
        }
        let mut at = 0;
        for want in &reqs {
            let frame = split_frame(&buf[at..], DEFAULT_MAX_FRAME).unwrap().unwrap();
            assert_eq!(&Request::decode(&frame).unwrap(), want);
            at += frame.consumed;
        }
        assert_eq!(at, buf.len());
    }

    #[test]
    fn oversized_frame_is_rejected_from_the_header_alone() {
        let header = 1025u32.to_be_bytes();
        assert_eq!(
            split_frame(&header, 1024),
            Err(FrameError::TooLarge {
                len: 1025,
                max: 1024
            })
        );
        assert_eq!(split_frame(&[0, 0, 0, 0], 1024), Err(FrameError::Empty));
    }

    fn decode_raw(tag: u8, payload: &[u8]) -> Result<Request<'_>, RequestError> {
        Request::decode(&Frame {
            tag,
            payload,
            consumed: 0,
        })
    }

    #[test]
    fn malformed_requests() {
        assert_eq!(
            decode_raw(0x7f, b""),
            Err(RequestError::UnknownOpcode(0x7f))
        );
        assert_eq!(decode_raw(OP_GET, b""), Err(RequestError::EmptyKey));
        assert_eq!(decode_raw(OP_DEL, b""), Err(RequestError::EmptyKey));
        assert_eq!(
            decode_raw(OP_PING, b"x"),
            Err(RequestError::UnexpectedPayload)
        );
        assert_eq!(decode_raw(OP_SET, b"\x00"), Err(RequestError::Truncated));
        assert_eq!(
            decode_raw(OP_SET, b"\x00\x00v"),
            Err(RequestError::EmptyKey)
        );
        // key_len says 5 but only 2 bytes follow
        assert_eq!(
            decode_raw(OP_SET, b"\x00\x05ab"),
            Err(RequestError::Truncated)
        );
        let long_key = vec![b'k'; MAX_KEY_LEN + 1];
        assert_eq!(decode_raw(OP_GET, &long_key), Err(RequestError::KeyTooLong));
    }
}
