//! The TCP server.
//!
//! One Tokio task per connection. Each task loops over three steps: execute
//! every complete request already in its read buffer, write all the responses
//! with one `write_all`, then read more bytes. A client that pipelines N
//! requests therefore costs about one read and one write system call, not N.
//!
//! Store operations take a shard lock (`parking_lot`, not an async lock) and
//! never await while holding it. The critical sections are a hash-table probe
//! long, so blocking the worker thread for that time is cheaper than an async
//! mutex's bookkeeping.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch, OwnedSemaphorePermit, Semaphore};

use crate::protocol::{
    encode_response, split_frame, FrameError, Request, Status, DEFAULT_MAX_FRAME,
};
use crate::store::Store;

/// Responses are flushed once this many bytes are pending, even if more
/// requests are already buffered, so a pipeline of large GETs cannot grow the
/// write buffer without bound.
const WRITE_HIGH_WATER: usize = 256 * 1024;
const READ_BUFFER: usize = 16 * 1024;
/// How long `run` waits for connections to finish after shutdown is requested.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct Config {
    /// Number of store shards; rounded up to a power of two.
    pub shards: usize,
    /// Largest accepted frame body, in bytes.
    pub max_frame: usize,
    /// Connections beyond this are closed immediately on accept.
    pub max_connections: usize,
}

impl Default for Config {
    fn default() -> Self {
        let cores = std::thread::available_parallelism().map_or(4, |n| n.get());
        Config {
            // Several shards per core keeps the chance that two busy threads
            // want the same shard low, at 128 bytes plus a table per shard.
            shards: (cores * 16).next_power_of_two(),
            max_frame: DEFAULT_MAX_FRAME,
            max_connections: 10_000,
        }
    }
}

/// Server-wide counters.
///
/// Connection tasks count operations in plain local integers and add them to
/// these atomics once per batch. Incrementing a shared atomic on every request
/// would make every core fight over one cache line.
#[derive(Default)]
pub struct Stats {
    connections_accepted: AtomicU64,
    connections_refused: AtomicU64,
    connections_open: AtomicU64,
    gets: AtomicU64,
    get_hits: AtomicU64,
    sets: AtomicU64,
    dels: AtomicU64,
    bad_requests: AtomicU64,
}

#[derive(Default)]
struct LocalStats {
    gets: u64,
    get_hits: u64,
    sets: u64,
    dels: u64,
    bad_requests: u64,
}

impl LocalStats {
    fn flush_into(&mut self, s: &Stats) {
        let add = |counter: &AtomicU64, n: &mut u64| {
            if *n != 0 {
                counter.fetch_add(std::mem::take(n), Ordering::Relaxed);
            }
        };
        add(&s.gets, &mut self.gets);
        add(&s.get_hits, &mut self.get_hits);
        add(&s.sets, &mut self.sets);
        add(&s.dels, &mut self.dels);
        add(&s.bad_requests, &mut self.bad_requests);
    }
}

impl Stats {
    fn render(&self, store: &Store) -> String {
        let v = |c: &AtomicU64| c.load(Ordering::Relaxed);
        format!(
            "keys={}\nshards={}\nconnections_open={}\nconnections_accepted={}\n\
             connections_refused={}\ngets={}\nget_hits={}\nsets={}\ndels={}\nbad_requests={}\n",
            store.len(),
            store.shard_count(),
            v(&self.connections_open),
            v(&self.connections_accepted),
            v(&self.connections_refused),
            v(&self.gets),
            v(&self.get_hits),
            v(&self.sets),
            v(&self.dels),
            v(&self.bad_requests),
        )
    }
}

pub struct Server {
    listener: TcpListener,
    store: Arc<Store>,
    stats: Arc<Stats>,
    config: Config,
}

impl Server {
    pub async fn bind(addr: impl tokio::net::ToSocketAddrs, config: Config) -> io::Result<Self> {
        Ok(Server {
            listener: TcpListener::bind(addr).await?,
            store: Arc::new(Store::new(config.shards)),
            stats: Arc::new(Stats::default()),
            config,
        })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub fn store(&self) -> Arc<Store> {
        Arc::clone(&self.store)
    }

    /// Accepts connections until `shutdown` completes, then stops accepting,
    /// tells every connection to finish its current batch, and waits (up to
    /// five seconds) for them to close.
    pub async fn run(self, shutdown: impl Future<Output = ()>) -> io::Result<()> {
        let Server {
            listener,
            store,
            stats,
            config,
        } = self;
        let limit = Arc::new(Semaphore::new(config.max_connections));
        let (stop_tx, stop_rx) = watch::channel(false);
        // Every connection task holds a clone of `alive_tx`. `recv` on the
        // other end returns `None` only when all of them have been dropped.
        let (alive_tx, mut alive_rx) = mpsc::channel::<()>(1);
        tokio::pin!(shutdown);

        loop {
            let accepted = tokio::select! {
                _ = &mut shutdown => break,
                accepted = listener.accept() => accepted,
            };
            let socket = match accepted {
                Ok((socket, _peer)) => socket,
                Err(_) => {
                    // Usually file-descriptor exhaustion. Back off instead of
                    // spinning on the same error.
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            };
            let Ok(permit) = Arc::clone(&limit).try_acquire_owned() else {
                stats.connections_refused.fetch_add(1, Ordering::Relaxed);
                continue; // dropping `socket` closes it
            };
            stats.connections_accepted.fetch_add(1, Ordering::Relaxed);
            let conn = Connection {
                socket,
                store: Arc::clone(&store),
                stats: Arc::clone(&stats),
                max_frame: config.max_frame,
                stop: stop_rx.clone(),
                _permit: permit,
                _alive: alive_tx.clone(),
            };
            tokio::spawn(conn.run());
        }

        drop(listener);
        let _ = stop_tx.send(true);
        drop(alive_tx);
        let _ = tokio::time::timeout(DRAIN_TIMEOUT, alive_rx.recv()).await;
        Ok(())
    }
}

struct Connection {
    socket: TcpStream,
    store: Arc<Store>,
    stats: Arc<Stats>,
    max_frame: usize,
    stop: watch::Receiver<bool>,
    _permit: OwnedSemaphorePermit,
    _alive: mpsc::Sender<()>,
}

/// What the request loop should do after draining the read buffer.
enum Next {
    ReadMore,
    Close,
}

impl Connection {
    async fn run(mut self) {
        self.stats.connections_open.fetch_add(1, Ordering::Relaxed);
        // I/O errors mean the peer went away; there is nobody to report to.
        let _ = self.serve().await;
        self.stats.connections_open.fetch_sub(1, Ordering::Relaxed);
    }

    async fn serve(&mut self) -> io::Result<()> {
        let _ = self.socket.set_nodelay(true);
        let mut rbuf = BytesMut::with_capacity(READ_BUFFER);
        let mut wbuf = BytesMut::with_capacity(READ_BUFFER);
        let mut local = LocalStats::default();

        loop {
            let next = loop {
                // Synchronous section: no `.await` between taking a shard
                // lock inside `execute` and releasing it.
                let (consumed, next) = self.drain(&rbuf, &mut wbuf, &mut local);
                rbuf.advance(consumed);
                match next {
                    // `drain` stopped early only because the write buffer
                    // filled; flush and continue with the same read buffer.
                    None => {
                        self.socket.write_all(&wbuf).await?;
                        wbuf.clear();
                    }
                    Some(next) => break next,
                }
            };
            local.flush_into(&self.stats);
            if !wbuf.is_empty() {
                self.socket.write_all(&wbuf).await?;
                wbuf.clear();
            }
            if matches!(next, Next::Close) {
                return self.socket.shutdown().await;
            }
            rbuf.reserve(READ_BUFFER);
            tokio::select! {
                read = self.socket.read_buf(&mut rbuf) => {
                    if read? == 0 {
                        return Ok(()); // peer closed
                    }
                }
                // Shutdown: every response for a request already received has
                // been written above, so closing here loses nothing.
                _ = self.stop.changed() => return self.socket.shutdown().await,
            }
        }
    }

    /// Executes complete requests from the front of `rbuf`, appending the
    /// responses to `wbuf`. Returns how many bytes were consumed and what to
    /// do next; `None` means the write buffer hit its high-water mark with
    /// requests still pending.
    fn drain(
        &self,
        rbuf: &[u8],
        wbuf: &mut BytesMut,
        local: &mut LocalStats,
    ) -> (usize, Option<Next>) {
        let mut at = 0;
        loop {
            if wbuf.len() >= WRITE_HIGH_WATER {
                return (at, None);
            }
            let frame = match split_frame(&rbuf[at..], self.max_frame) {
                Ok(Some(frame)) => frame,
                Ok(None) => return (at, Some(Next::ReadMore)),
                Err(e @ (FrameError::Empty | FrameError::TooLarge { .. })) => {
                    // The stream cannot be resynchronised: say why and close.
                    local.bad_requests += 1;
                    encode_response(wbuf, Status::Error, e.to_string().as_bytes());
                    return (rbuf.len(), Some(Next::Close));
                }
            };
            at += frame.consumed;
            match Request::decode(&frame) {
                Ok(request) => self.execute(request, wbuf, local),
                Err(e) => {
                    // Frame boundaries are intact, so the connection survives.
                    local.bad_requests += 1;
                    encode_response(wbuf, Status::Error, e.to_string().as_bytes());
                }
            }
        }
    }

    fn execute(&self, request: Request<'_>, wbuf: &mut BytesMut, local: &mut LocalStats) {
        match request {
            Request::Ping => encode_response(wbuf, Status::Ok, b""),
            Request::Get { key } => {
                local.gets += 1;
                match self.store.get(key) {
                    Some(value) => {
                        local.get_hits += 1;
                        encode_response(wbuf, Status::Ok, &value);
                    }
                    None => encode_response(wbuf, Status::NotFound, b""),
                }
            }
            Request::Set { key, value } => {
                local.sets += 1;
                self.store.set(key, value);
                encode_response(wbuf, Status::Ok, b"");
            }
            Request::Del { key } => {
                local.dels += 1;
                let status = if self.store.del(key) {
                    Status::Ok
                } else {
                    Status::NotFound
                };
                encode_response(wbuf, status, b"");
            }
            Request::Stats => {
                // Include this connection's own operations up to this point.
                local.flush_into(&self.stats);
                encode_response(wbuf, Status::Ok, self.stats.render(&self.store).as_bytes());
            }
        }
    }
}
