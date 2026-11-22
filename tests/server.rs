//! End-to-end tests: a real server on an ephemeral port, real TCP clients.

use std::net::SocketAddr;
use std::time::Duration;

use shardkv::{Client, ClientError, Config, Server, Status};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

struct Running {
    addr: SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<std::io::Result<()>>,
}

impl Running {
    async fn shutdown(mut self) {
        let _ = self.stop.take().unwrap().send(());
        tokio::time::timeout(Duration::from_secs(10), self.task)
            .await
            .expect("server did not stop in time")
            .unwrap()
            .unwrap();
    }
}

async fn start(config: Config) -> Running {
    let server = Server::bind("127.0.0.1:0", config).await.unwrap();
    let addr = server.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    let task = tokio::spawn(server.run(async {
        let _ = stopped.await;
    }));
    Running {
        addr,
        stop: Some(stop),
        task,
    }
}

fn small() -> Config {
    Config {
        shards: 8,
        ..Config::default()
    }
}

/// Reads one raw response frame: (status byte, payload).
async fn read_raw(sock: &mut TcpStream) -> (u8, Vec<u8>) {
    let len = sock.read_u32().await.unwrap() as usize;
    let mut body = vec![0u8; len];
    sock.read_exact(&mut body).await.unwrap();
    (body[0], body[1..].to_vec())
}

#[tokio::test]
async fn basic_operations_and_stats() {
    let srv = start(small()).await;
    let mut c = Client::connect(srv.addr).await.unwrap();
    c.ping().await.unwrap();
    assert_eq!(c.get(b"missing").await.unwrap(), None);
    c.set(b"k", b"v1").await.unwrap();
    assert_eq!(c.get(b"k").await.unwrap().as_deref(), Some(&b"v1"[..]));
    c.set(b"k", b"").await.unwrap(); // an empty value is a value
    assert_eq!(c.get(b"k").await.unwrap().as_deref(), Some(&b""[..]));
    assert!(c.del(b"k").await.unwrap());
    assert!(!c.del(b"k").await.unwrap());
    c.set(b"other", b"x").await.unwrap();

    let stats = c.stats().await.unwrap();
    for line in [
        "keys=1",
        "shards=8",
        "gets=3",
        "get_hits=2",
        "sets=3",
        "dels=2",
        "connections_open=1",
    ] {
        assert!(
            stats.lines().any(|l| l == line),
            "missing `{line}` in:\n{stats}"
        );
    }
    assert!(matches!(c.get(b"").await, Err(ClientError::InvalidKey)));
    srv.shutdown().await;
}

#[tokio::test]
async fn pipelined_replies_arrive_in_request_order() {
    let srv = start(small()).await;
    let mut c = Client::connect(srv.addr).await.unwrap();
    const N: usize = 2000;
    for i in 0..N {
        c.queue_set(format!("key{i}").as_bytes(), format!("value{i}").as_bytes())
            .unwrap();
        c.queue_get(format!("key{i}").as_bytes()).unwrap();
        if i % 3 == 0 {
            c.queue_del(format!("key{i}").as_bytes()).unwrap();
            c.queue_get(format!("key{i}").as_bytes()).unwrap();
        }
    }
    let replies = c.flush().await.unwrap();
    let mut it = replies.into_iter();
    for i in 0..N {
        assert_eq!(it.next().unwrap().status, Status::Ok); // set
        let got = it.next().unwrap();
        assert_eq!(
            (got.status, &got.body[..]),
            (Status::Ok, format!("value{i}").as_bytes())
        );
        if i % 3 == 0 {
            assert_eq!(it.next().unwrap().status, Status::Ok); // del
            assert_eq!(it.next().unwrap().status, Status::NotFound); // get after del
        }
    }
    assert!(it.next().is_none());
    srv.shutdown().await;
}

#[tokio::test]
async fn large_values_and_large_pipelines() {
    let srv = start(small()).await;
    let mut c = Client::connect(srv.addr).await.unwrap();
    let big: Vec<u8> = (0..1_000_000u32).map(|i| (i % 251) as u8).collect();
    c.set(b"big", &big).await.unwrap();
    assert_eq!(c.get(b"big").await.unwrap().unwrap(), &big[..]);

    // 200 GETs of 64 KiB is about 13 MiB of responses: far past the server's
    // write high-water mark, so it must flush in the middle of the batch.
    let chunk = vec![0xabu8; 64 * 1024];
    c.set(b"chunk", &chunk).await.unwrap();
    for _ in 0..200 {
        c.queue_get(b"chunk").unwrap();
    }
    let replies = c.flush().await.unwrap();
    assert_eq!(replies.len(), 200);
    assert!(replies
        .iter()
        .all(|r| r.status == Status::Ok && r.body == chunk));
    srv.shutdown().await;
}

#[tokio::test]
async fn request_split_across_many_tcp_segments() {
    let srv = start(small()).await;
    let mut sock = TcpStream::connect(srv.addr).await.unwrap();
    sock.set_nodelay(true).unwrap();
    // SET "ab" = "xyz", then GET "ab", delivered one byte at a time.
    let mut bytes = vec![0, 0, 0, 8, 0x03, 0, 2, b'a', b'b', b'x', b'y', b'z'];
    bytes.extend_from_slice(&[0, 0, 0, 3, 0x02, b'a', b'b']);
    for b in bytes {
        sock.write_all(&[b]).await.unwrap();
        sock.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert_eq!(read_raw(&mut sock).await, (0x00, vec![]));
    assert_eq!(read_raw(&mut sock).await, (0x00, b"xyz".to_vec()));
    srv.shutdown().await;
}

#[tokio::test]
async fn malformed_request_gets_an_error_and_the_connection_survives() {
    let srv = start(small()).await;
    let mut sock = TcpStream::connect(srv.addr).await.unwrap();
    // unknown opcode 0x7f, GET with an empty key, then a valid PING
    sock.write_all(&[0, 0, 0, 1, 0x7f, 0, 0, 0, 1, 0x02, 0, 0, 0, 1, 0x01])
        .await
        .unwrap();
    let (status, msg) = read_raw(&mut sock).await;
    assert_eq!(status, 0x02);
    assert!(String::from_utf8(msg).unwrap().contains("unknown opcode"));
    assert_eq!(read_raw(&mut sock).await.0, 0x02);
    assert_eq!(read_raw(&mut sock).await, (0x00, vec![]));
    srv.shutdown().await;
}

#[tokio::test]
async fn oversized_frame_gets_an_error_and_the_connection_is_closed() {
    let srv = start(Config {
        max_frame: 1024,
        ..small()
    })
    .await;
    let mut sock = TcpStream::connect(srv.addr).await.unwrap();
    // A valid PING first, then a header announcing 1 MiB. Only the header is
    // sent: the server must refuse without waiting for the body.
    sock.write_all(&[0, 0, 0, 1, 0x01]).await.unwrap();
    sock.write_all(&(1u32 << 20).to_be_bytes()).await.unwrap();
    assert_eq!(read_raw(&mut sock).await, (0x00, vec![]));
    let (status, msg) = read_raw(&mut sock).await;
    assert_eq!(status, 0x02);
    assert!(String::from_utf8(msg).unwrap().contains("exceeds"));
    let mut rest = Vec::new();
    assert_eq!(
        sock.read_to_end(&mut rest).await.unwrap(),
        0,
        "server should close"
    );
    srv.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_concurrent_clients() {
    let srv = start(small()).await;
    let addr = srv.addr;
    let mut tasks = Vec::new();
    for client in 0..32u32 {
        tasks.push(tokio::spawn(async move {
            let mut c = Client::connect(addr).await.unwrap();
            for i in 0..300u32 {
                let key = format!("c{client}:k{}", i % 50);
                let value = format!("c{client}:v{i}");
                c.set(key.as_bytes(), value.as_bytes()).await.unwrap();
                // Nobody else writes this key, so the read must see our write.
                assert_eq!(
                    c.get(key.as_bytes()).await.unwrap().unwrap(),
                    value.as_bytes()
                );
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let mut c = Client::connect(addr).await.unwrap();
    assert!(c.stats().await.unwrap().lines().any(|l| l == "keys=1600"));
    srv.shutdown().await;
}

#[tokio::test]
async fn connections_over_the_limit_are_refused() {
    let srv = start(Config {
        max_connections: 2,
        ..small()
    })
    .await;
    let mut a = Client::connect(srv.addr).await.unwrap();
    let mut b = Client::connect(srv.addr).await.unwrap();
    a.ping().await.unwrap();
    b.ping().await.unwrap();
    let mut c = Client::connect(srv.addr).await.unwrap(); // TCP accept succeeds
    assert!(c.ping().await.is_err(), "third connection should be closed");
    drop(a);
    // The slot is released when the server notices the close.
    let mut ok = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        if let Ok(mut d) = Client::connect(srv.addr).await {
            if d.ping().await.is_ok() {
                ok = true;
                break;
            }
        }
    }
    assert!(ok, "a freed slot should be reusable");
    srv.shutdown().await;
}

#[tokio::test]
async fn graceful_shutdown_closes_idle_connections() {
    let srv = start(small()).await;
    let addr = srv.addr;
    let mut c = Client::connect(addr).await.unwrap();
    c.set(b"k", b"v").await.unwrap();
    srv.shutdown().await; // returns only after the connection task has ended
    assert!(c.ping().await.is_err());
    assert!(
        TcpStream::connect(addr).await.is_err(),
        "listener should be closed"
    );
}
