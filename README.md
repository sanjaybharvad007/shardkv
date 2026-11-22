# shardkv

A sharded in-memory key-value store in Rust, served over TCP with a small
binary protocol on Tokio.

```
client ──TCP──► connection task ──► parse (borrowed, no allocation)
                 (one per socket)         │
                                          ▼
                                   hash key once
                                          │
                       ┌──────────┬───────┴──┬──────────┐
                       ▼          ▼          ▼          ▼
                    shard 0    shard 1     ...      shard N-1
                    RwLock +   RwLock +             RwLock +
                    table      table                table
```

## What it does

- **GET / SET / DEL / PING / STATS** over a length-prefixed binary protocol
  with pipelining (`src/protocol.rs` has the full format).
- **Sharded store.** Keys are spread over a power-of-two number of hash
  tables, each behind its own reader-writer lock, so two operations contend
  only when they hit the same shard and one of them writes.
- **Bounded resources.** Frames over a size limit are refused from the header
  alone, before the body is buffered. Connections over a limit are closed on
  accept. Pending responses are flushed at a high-water mark.
- **Graceful shutdown.** On Ctrl-C the server stops accepting, lets each
  connection finish the requests it has already received, then exits.

## Build and run

Requires stable Rust (tested with 1.97).

```bash
cargo test                       # 11 unit tests, 9 end-to-end tests
cargo build --release

./target/release/shardkv-server --addr 127.0.0.1:7878
./target/release/shardkv-bench  --addr 127.0.0.1:7878 --pipeline 16   # in another terminal
./target/release/storebench                                           # store only, no network
```

Every binary takes `--help`. As a library:

```rust
let mut client = shardkv::Client::connect("127.0.0.1:7878").await?;
client.set(b"greeting", b"hello").await?;
assert_eq!(client.get(b"greeting").await?.as_deref(), Some(&b"hello"[..]));

// Pipelining: one write, replies in request order.
client.queue_get(b"a")?;
client.queue_get(b"b")?;
let replies = client.flush().await?;
```

## Design decisions

**Parsing borrows; it does not copy.** `Request<'a>` holds slices into the
connection's read buffer, so decoding a request allocates nothing. The
lifetime ties each request to the buffer it came from, and the compiler
rejects any attempt to keep a request after the buffer is advanced.

**One hash per operation.** The key is hashed once and the same hash selects
the shard and probes the table (`hashbrown::HashTable` accepts a precomputed
hash; the standard `HashMap` would hash again).

**The allocator is kept out of the critical section.** `set` allocates the
value before taking the write lock. A replaced or deleted entry is moved out
under the lock and freed after it is released.

**Synchronous locks inside async tasks.** Shard locks are `parking_lot`
locks, never held across an `.await`. The critical section is one hash-table
probe; an async mutex would cost more than the work it protects.

**Two kinds of protocol error.** A frame with a bad length means the byte
stream can no longer be trusted, so the server reports the error and closes.
A well-delimited frame with bad contents (unknown opcode, empty key) gets an
ERROR response and the connection continues.

**Statistics are batched.** Each connection counts operations in local
integers and adds them to the shared atomic counters once per batch of
requests, so cores do not contend on a counter cache line for every request.

## Measurements

Finding 2 was measured on a 10-core Apple Silicon MacBook Air. Everything
else is from a 2-vCPU shared cloud VM with client and server on the same
machine over loopback. That VM is small and noisy: repeated runs differ by
tens of percent. Treat its numbers as evidence of behaviour, and rerun on
your own hardware for numbers worth quoting. Raw output is in `results/`.

### Finding 1: the first read of every value was allocating under a lock

`storebench` wraps the global allocator and counts heap allocations per
operation. The first version stored values as `bytes::Bytes`:

| Operation | Allocations, `Bytes` | Allocations, `Arc<[u8]>` |
|---|---:|---:|
| GET (hit) | 1 on first read of each value | 0 |
| GET (miss) | 0 | 0 |
| SET (overwrite) | 1 | 1 |
| SET (new key) | 2 | 2 |
| DEL | 0 | 0 |

`Bytes::copy_from_slice` defers allocating its shared reference count until
the first clone, and that clone happened inside `get`, while holding the
shard's read lock. Storing `Arc<[u8]>` puts the count and the data in a
single allocation at write time. Single-threaded read-heavy throughput went
from about 16 to about 21 million operations per second.

### Finding 2: one lock collapses as threads are added; shards scale

In-process, 131,072 keys, 128-byte values, million operations per second, on
a 10-core Apple Silicon MacBook Air (`results/storebench-m-series.txt`).

95% reads:

| Shards | 1 thread | 2 | 4 | 8 | 16 |
|---:|---:|---:|---:|---:|---:|
| 1 | 61.1 | 28.5 | 20.0 | 7.1 | 6.9 |
| 16 | 61.9 | 51.5 | 65.3 | 48.3 | 48.7 |
| 256 | 57.7 | 70.9 | 105.4 | 101.5 | 111.4 |

50% reads:

| Shards | 1 thread | 2 | 4 | 8 | 16 |
|---:|---:|---:|---:|---:|---:|
| 1 | 24.7 | 16.3 | 10.2 | 6.3 | 4.9 |
| 16 | 24.0 | 26.1 | 39.3 | 30.7 | 30.3 |
| 256 | 22.7 | 26.2 | 40.9 | 34.8 | 35.2 |

With a single shard, every added thread makes the store slower: at 16
threads the read-heavy workload runs at about a ninth of its single-thread
speed, because every operation bounces the same lock word between cores.
With 256 shards the same workload goes from 57.7 to 105.4 million operations
per second at four threads, and at 16 threads it is 16 times faster than the
single-shard store.

Scaling is well short of linear: four threads give about 1.8 times one
thread, and more threads add little. The cause has not been isolated.
Candidates are the machine's mix of performance and efficiency cores, memory
latency (the keys and values do not fit in cache), contention on shard lock
words that even readers must write, and, for the write-heavy workload, the
system allocator, since every SET allocates. The 16-shard rows sitting well
below the 256-shard rows suggest lock-word contention is part of it.

### Finding 3: over the network, the store is not the bottleneck

32 connections, 90% reads, 128-byte values, closed loop:

| Setup | Requests/s | p50 | p99 |
|---|---:|---:|---:|
| 256 shards, no pipelining | 161,000 | 0.19 ms | 0.33 ms |
| 256 shards, pipeline of 16 | 1,783,000 | 0.28 ms | 0.51 ms |
| 1 shard, pipeline of 16 | 1,760,000 | 0.28 ms | 0.63 ms |

Pipelining raised throughput about elevenfold, because the server answers a
whole batch with one read and one write. The shard count made no measurable
difference to throughput here: a store operation takes tens of nanoseconds
and a request's share of the network path takes hundreds, so on two cores
the lock is never the limit. Sharding matters once there are enough cores
for the store to be contended, which Finding 2 shows in isolation.

### Tail latency at a fixed request rate

Open loop: requests are sent on a schedule regardless of replies, and
latency is measured from each request's scheduled time, so server stalls are
not hidden. 32 connections, 256 shards:

| Offered rate | p50 | p99 | p99.9 | max |
|---:|---:|---:|---:|---:|
| 20,000/s | 0.16 ms | 0.22 ms | 0.42 ms | 0.69 ms |
| 100,000/s | 0.14 ms | 0.21 ms | 0.86 ms | 2.08 ms |
| 200,000/s | 0.15 ms | 0.26 ms | 1.84 ms | 3.43 ms |
| 400,000/s | 0.16 ms | 0.23 ms | 1.06 ms | 3.01 ms |

About 0.1 ms of every figure is the load generator's own timer error, which
is counted as latency by design. A first version of the generator used
Tokio's timers and reported a p50 above 1 ms; the extra millisecond was the
timer's resolution, not the server. The schedule is now driven by an OS
thread (see `ticker` in `src/bin/shardkv-bench.rs`).

## Limitations

- **In-memory only.** No persistence, replication, or eviction; memory grows
  until keys are deleted.
- **No TTLs, no transactions, no multi-key operations.**
- **No authentication or TLS.** Bind to localhost or a trusted network.
- **Network benchmarks ran on one small VM**, with the client competing with
  the server for the same two vCPUs. They have not been repeated on larger
  hardware.
- **An error reply before a forced close can be lost** if the client is
  still sending when the server closes the socket.

## Layout

| Path | Contents |
|---|---|
| `src/protocol.rs` | Wire format, zero-copy parser, encoder |
| `src/store.rs` | Sharded concurrent map |
| `src/server.rs` | Tokio server, connection loop, shutdown |
| `src/client.rs` | Async client with pipelining |
| `src/bin/shardkv-server.rs` | Server binary |
| `src/bin/shardkv-bench.rs` | Network load generator (closed and open loop) |
| `src/bin/storebench.rs` | In-process throughput and allocation benchmark |
| `tests/server.rs` | End-to-end tests against a real server |

## License

MIT
