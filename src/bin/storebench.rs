//! In-process benchmark of the store alone: no sockets, no runtime.
//!
//! It answers two questions about the data structure:
//!   1. How does throughput change with thread count and shard count?
//!   2. How many heap allocations does each operation make?
//!
//! Allocations are counted exactly by wrapping the global allocator, which is
//! why this is its own binary: the counter costs one atomic add per call.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use shardkv::Store;

struct CountingAllocator;

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: forwarded unchanged; the caller upholds `alloc`'s contract.
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` came from `System.alloc` with this same layout.
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: forwarded unchanged; the caller upholds `realloc`'s contract.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

const KEYS: u64 = 1 << 17;
const VALUE: [u8; 128] = [0x5a; 128];

fn key(i: u64) -> [u8; 16] {
    let mut k = *b"key:000000000000";
    k[8..].copy_from_slice(&i.to_be_bytes());
    k
}

/// xorshift64*: small, fast, and good enough to pick keys.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
}

fn filled(shards: usize) -> Arc<Store> {
    let store = Arc::new(Store::new(shards));
    for i in 0..KEYS {
        store.set(&key(i), &VALUE);
    }
    store
}

/// Runs `threads` threads for `duration`; each does `read_pct`% GETs and the
/// rest SETs on uniformly random keys. Returns operations per second.
fn throughput(shards: usize, threads: usize, read_pct: u64, duration: Duration) -> f64 {
    let store = filled(shards);
    let stop = Arc::new(AtomicBool::new(false));
    let start = Arc::new(Barrier::new(threads + 1));
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let (store, stop, start) = (store.clone(), stop.clone(), start.clone());
            std::thread::spawn(move || {
                let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ (t as u64 + 1));
                let mut ops = 0u64;
                start.wait();
                while !stop.load(Ordering::Relaxed) {
                    for _ in 0..256 {
                        let r = rng.next();
                        let k = key(r % KEYS);
                        if (r >> 32) % 100 < read_pct {
                            std::hint::black_box(store.get(&k));
                        } else {
                            store.set(&k, &VALUE);
                        }
                    }
                    ops += 256;
                }
                ops
            })
        })
        .collect();
    start.wait();
    let began = Instant::now();
    std::thread::sleep(duration);
    stop.store(true, Ordering::Relaxed);
    let total: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
    total as f64 / began.elapsed().as_secs_f64()
}

fn allocations_per_op(label: &str, n: u64, mut op: impl FnMut(u64)) {
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    for i in 0..n {
        op(i);
    }
    let count = ALLOCATIONS.load(Ordering::Relaxed) - before;
    println!("  {label:<28} {:>6.3}", count as f64 / n as f64);
}

fn main() {
    let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
    let seconds: f64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(2.0);
    let duration = Duration::from_secs_f64(seconds);
    let mut thread_counts = vec![1, 2, 4, 8, 16];
    thread_counts.retain(|&t| t <= cores.max(2) * 2);

    println!("store benchmark: {KEYS} keys, 128-byte values, {cores} cores, {seconds} s per cell");
    for read_pct in [95u64, 50] {
        println!("\nthroughput, million ops/s, {read_pct}% reads");
        print!("  {:>8}", "shards");
        for t in &thread_counts {
            print!(" {:>9}", format!("{t} thr"));
        }
        println!();
        for shards in [1usize, 16, 256] {
            print!("  {shards:>8}");
            for &t in &thread_counts {
                print!(" {:>9.2}", throughput(shards, t, read_pct, duration) / 1e6);
            }
            println!();
        }
    }

    println!("\nheap allocations per operation");
    let store = filled(64);
    let n = 100_000;
    allocations_per_op("get (hit)", n, |i| {
        std::hint::black_box(store.get(&key(i % KEYS)));
    });
    allocations_per_op("get (miss)", n, |i| {
        std::hint::black_box(store.get(&key(KEYS + i)));
    });
    allocations_per_op("set (overwrite)", n, |i| {
        store.set(&key(i % KEYS), &VALUE);
    });
    allocations_per_op("set (new key)", n, |i| {
        store.set(&key(KEYS + i), &VALUE);
    });
    allocations_per_op("del (hit)", n, |i| {
        store.del(&key(KEYS + i));
    });
}
