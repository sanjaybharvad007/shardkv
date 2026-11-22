//! Async client.

use std::fmt;
use std::io;

use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, ToSocketAddrs};

use crate::protocol::{
    split_frame, FrameError, Request, Response, Status, DEFAULT_MAX_FRAME, MAX_KEY_LEN,
};

#[derive(Debug)]
pub enum ClientError {
    Io(io::Error),
    /// The server closed the connection while a response was outstanding.
    ConnectionClosed,
    /// The server sent bytes that are not a valid response.
    BadResponse(String),
    /// The server answered with an ERROR status.
    Server(String),
    /// The request was invalid and was not sent.
    InvalidKey,
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::Io(e) => write!(f, "i/o error: {e}"),
            ClientError::ConnectionClosed => write!(f, "connection closed by server"),
            ClientError::BadResponse(why) => write!(f, "invalid response: {why}"),
            ClientError::Server(msg) => write!(f, "server error: {msg}"),
            ClientError::InvalidKey => {
                write!(f, "key must be between 1 and {MAX_KEY_LEN} bytes")
            }
        }
    }
}

impl std::error::Error for ClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ClientError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for ClientError {
    fn from(e: io::Error) -> Self {
        ClientError::Io(e)
    }
}

impl From<FrameError> for ClientError {
    fn from(e: FrameError) -> Self {
        ClientError::BadResponse(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, ClientError>;

/// One response, owning its payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reply {
    pub status: Status,
    pub body: Bytes,
}

/// A connection to a shardkv server. Not `Clone`: one connection carries one
/// ordered stream of requests. Open several for concurrency.
pub struct Client {
    stream: TcpStream,
    rbuf: BytesMut,
    wbuf: BytesMut,
    /// Requests encoded into `wbuf` whose responses have not been read.
    queued: usize,
    max_frame: usize,
}

fn check_key(key: &[u8]) -> Result<()> {
    if key.is_empty() || key.len() > MAX_KEY_LEN {
        Err(ClientError::InvalidKey)
    } else {
        Ok(())
    }
}

impl Client {
    pub async fn connect(addr: impl ToSocketAddrs) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(Client {
            stream,
            rbuf: BytesMut::with_capacity(16 * 1024),
            wbuf: BytesMut::with_capacity(16 * 1024),
            queued: 0,
            max_frame: DEFAULT_MAX_FRAME,
        })
    }

    pub async fn ping(&mut self) -> Result<()> {
        self.queue(Request::Ping);
        self.one_ok().await.map(drop)
    }

    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Bytes>> {
        check_key(key)?;
        self.queue(Request::Get { key });
        self.one_ok().await
    }

    pub async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        check_key(key)?;
        self.queue(Request::Set { key, value });
        self.one_ok().await.map(drop)
    }

    /// Returns `true` if the key existed.
    pub async fn del(&mut self, key: &[u8]) -> Result<bool> {
        check_key(key)?;
        self.queue(Request::Del { key });
        Ok(self.one_ok().await?.is_some())
    }

    pub async fn stats(&mut self) -> Result<String> {
        self.queue(Request::Stats);
        let body = self.one_ok().await?.unwrap_or_default();
        String::from_utf8(body.to_vec()).map_err(|e| ClientError::BadResponse(e.to_string()))
    }

    // ---- pipelining ------------------------------------------------------
    // Queue any number of requests, then `flush` sends them in one write and
    // returns the replies in the same order.

    pub fn queue_get(&mut self, key: &[u8]) -> Result<()> {
        check_key(key)?;
        self.queue(Request::Get { key });
        Ok(())
    }

    pub fn queue_set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        check_key(key)?;
        self.queue(Request::Set { key, value });
        Ok(())
    }

    pub fn queue_del(&mut self, key: &[u8]) -> Result<()> {
        check_key(key)?;
        self.queue(Request::Del { key });
        Ok(())
    }

    /// Sends every queued request and reads one reply per request.
    pub async fn flush(&mut self) -> Result<Vec<Reply>> {
        let n = std::mem::take(&mut self.queued);
        self.stream.write_all(&self.wbuf).await?;
        self.wbuf.clear();
        let mut replies = Vec::with_capacity(n);
        for _ in 0..n {
            replies.push(self.read_reply().await?);
        }
        Ok(replies)
    }

    fn queue(&mut self, request: Request<'_>) {
        request.encode(&mut self.wbuf);
        self.queued += 1;
    }

    /// Flushes exactly one request: `Some(body)` for OK, `None` for NOT_FOUND.
    async fn one_ok(&mut self) -> Result<Option<Bytes>> {
        debug_assert_eq!(
            self.queued, 1,
            "single-request call with a pipeline pending"
        );
        let reply = self
            .flush()
            .await?
            .pop()
            .ok_or(ClientError::ConnectionClosed)?;
        match reply.status {
            Status::Ok => Ok(Some(reply.body)),
            Status::NotFound => Ok(None),
            Status::Error => Err(ClientError::Server(
                String::from_utf8_lossy(&reply.body).into_owned(),
            )),
        }
    }

    async fn read_reply(&mut self) -> Result<Reply> {
        loop {
            if let Some(frame) = split_frame(&self.rbuf, self.max_frame)? {
                let consumed = frame.consumed;
                let response = Response::decode(&frame).ok_or_else(|| {
                    ClientError::BadResponse(format!("unknown status 0x{:02x}", frame.tag))
                })?;
                let reply = Reply {
                    status: response.status,
                    body: Bytes::copy_from_slice(response.body),
                };
                self.rbuf.advance(consumed);
                return Ok(reply);
            }
            if self.stream.read_buf(&mut self.rbuf).await? == 0 {
                return Err(ClientError::ConnectionClosed);
            }
        }
    }
}
