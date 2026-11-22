//! The sharded map.
//!
//! Keys are spread over a power-of-two number of shards, each an independent
//! hash table behind its own reader-writer lock. Two operations contend only
//! when they hit the same shard and at least one is a write, so contention
//! falls as the shard count rises.
//!
//! Decisions that keep critical sections short:
//!
//! * **One hash per operation.** The key is hashed once; the same 64-bit hash
//!   picks the shard and probes the table (`hashbrown::HashTable` accepts a
//!   precomputed hash, which `std::collections::HashMap` does not).
//! * **Allocate and free outside the lock.** `set` copies the value into its
//!   own allocation before taking the write lock, and a replaced or deleted
//!   entry is moved out and dropped after the lock is released. The allocator
//!   is never called while a shard is write-locked, except for the key of a
//!   brand-new entry and table growth.
//! * **Reads copy nothing and allocate nothing.** Values are `Arc<[u8]>`, so
//!   `get` returns a handle that shares the stored allocation (one atomic
//!   increment) and the caller writes it to a socket after the lock is gone.
//!   An earlier version stored `bytes::Bytes` built with `copy_from_slice`;
//!   the allocation counter in `storebench` showed one heap allocation on the
//!   first read of every value, made while holding the shard's read lock,
//!   because `Bytes` only allocates its shared reference count on first
//!   clone. `Arc<[u8]>` puts the count and the data in one allocation at
//!   write time.
//! * **Values own exact-size allocations.** Storing a zero-copy slice of the
//!   connection's read buffer would be cheaper per SET, but one small value
//!   would then keep its whole read buffer alive for as long as it is stored.
//! * **Shards are padded to 128 bytes** so the lock words of neighbouring
//!   shards never share a cache line.

use std::sync::Arc;

use hashbrown::hash_table::{Entry as TableEntry, HashTable};
use parking_lot::RwLock;

struct Entry {
    key: Box<[u8]>,
    value: Arc<[u8]>,
}

#[repr(align(128))]
struct Shard {
    table: RwLock<HashTable<Entry>>,
}

/// A concurrent map from byte-string keys to byte-string values.
pub struct Store {
    shards: Box<[Shard]>,
    /// `shards.len() - 1`; valid as a mask because the length is a power of two.
    mask: usize,
    hasher: ahash::RandomState,
}

impl Store {
    /// Creates a store with `shards` shards, rounded up to a power of two
    /// (minimum 1).
    pub fn new(shards: usize) -> Self {
        let n = shards.max(1).next_power_of_two();
        Store {
            shards: (0..n)
                .map(|_| Shard {
                    table: RwLock::new(HashTable::new()),
                })
                .collect(),
            mask: n - 1,
            hasher: ahash::RandomState::new(),
        }
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    #[inline]
    fn hash(&self, key: &[u8]) -> u64 {
        self.hasher.hash_one(key)
    }

    /// hashbrown indexes buckets with the low bits of the hash and tags them
    /// with the top seven, so the shard is chosen from bits in between. Using
    /// the low bits for both would leave every table in a shard with the same
    /// low bits and waste part of the hash.
    #[inline]
    fn shard(&self, hash: u64) -> &Shard {
        &self.shards[(hash >> 40) as usize & self.mask]
    }

    /// Returns the value stored under `key`. The result shares the stored
    /// allocation; it stays valid if the key is later overwritten or deleted.
    pub fn get(&self, key: &[u8]) -> Option<Arc<[u8]>> {
        let hash = self.hash(key);
        let table = self.shard(hash).table.read();
        table
            .find(hash, |e| &*e.key == key)
            .map(|e| e.value.clone())
    }

    /// Stores `value` under `key`. Returns `true` if the key already existed.
    pub fn set(&self, key: &[u8], value: &[u8]) -> bool {
        let hash = self.hash(key);
        let value: Arc<[u8]> = Arc::from(value); // allocated before locking
        let replaced: Option<Arc<[u8]>>;
        {
            let mut table = self.shard(hash).table.write();
            match table.entry(hash, |e| &*e.key == key, |e| self.hash(&e.key)) {
                TableEntry::Occupied(mut slot) => {
                    replaced = Some(std::mem::replace(&mut slot.get_mut().value, value));
                }
                TableEntry::Vacant(slot) => {
                    slot.insert(Entry {
                        key: key.into(),
                        value,
                    });
                    replaced = None;
                }
            }
        }
        replaced.is_some() // the old value is freed here, after the unlock
    }

    /// Removes `key`. Returns `true` if it existed.
    pub fn del(&self, key: &[u8]) -> bool {
        let hash = self.hash(key);
        let removed: Option<Entry>;
        {
            let mut table = self.shard(hash).table.write();
            removed = match table.find_entry(hash, |e| &*e.key == key) {
                Ok(slot) => Some(slot.remove().0),
                Err(_) => None,
            };
        }
        removed.is_some()
    }

    /// Number of keys. Locks one shard at a time, so under concurrent writes
    /// the result is a count that was never exactly true at any one instant.
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.table.read().len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_operations() {
        let s = Store::new(4);
        assert!(s.is_empty());
        assert_eq!(s.get(b"a"), None);
        assert!(!s.set(b"a", b"1"));
        assert_eq!(s.get(b"a").as_deref(), Some(&b"1"[..]));
        assert!(s.set(b"a", b"2")); // overwrite reports the key existed
        assert_eq!(s.get(b"a").as_deref(), Some(&b"2"[..]));
        assert_eq!(s.len(), 1);
        assert!(s.del(b"a"));
        assert!(!s.del(b"a"));
        assert_eq!(s.get(b"a"), None);
        assert!(s.is_empty());
    }

    #[test]
    fn shard_count_rounds_up() {
        assert_eq!(Store::new(0).shard_count(), 1);
        assert_eq!(Store::new(1).shard_count(), 1);
        assert_eq!(Store::new(5).shard_count(), 8);
        assert_eq!(Store::new(64).shard_count(), 64);
    }

    #[test]
    fn a_read_handle_outlives_overwrite_and_delete() {
        let s = Store::new(1);
        s.set(b"k", b"first");
        let held = s.get(b"k").unwrap();
        s.set(b"k", b"second");
        s.del(b"k");
        assert_eq!(&held[..], b"first");
    }

    #[test]
    fn many_keys_spread_over_shards() {
        let s = Store::new(16);
        for i in 0..10_000u32 {
            s.set(&i.to_be_bytes(), &(i * 2).to_be_bytes());
        }
        assert_eq!(s.len(), 10_000);
        for i in 0..10_000u32 {
            assert_eq!(
                s.get(&i.to_be_bytes()).as_deref(),
                Some(&(i * 2).to_be_bytes()[..])
            );
        }
        let used = s
            .shards
            .iter()
            .filter(|sh| !sh.table.read().is_empty())
            .count();
        assert_eq!(used, 16, "keys should land in every shard");
    }

    /// Writers own disjoint key ranges and overwrite them repeatedly while
    /// readers scan everything. A reader must only ever see a value that some
    /// writer stored for exactly that key: never a torn or misplaced one.
    #[test]
    fn concurrent_readers_and_writers() {
        const WRITERS: u32 = 4;
        const KEYS_PER_WRITER: u32 = 500;
        const ROUNDS: u32 = 40;
        let store = Arc::new(Store::new(8));
        let value = |key: u32, round: u32| {
            let mut v = Vec::with_capacity(24);
            v.extend_from_slice(&key.to_be_bytes());
            v.extend_from_slice(&round.to_be_bytes());
            v.extend_from_slice(&[key as u8; 16]);
            v
        };
        std::thread::scope(|scope| {
            for w in 0..WRITERS {
                let store = &store;
                scope.spawn(move || {
                    for round in 0..ROUNDS {
                        for i in 0..KEYS_PER_WRITER {
                            let key = w * KEYS_PER_WRITER + i;
                            store.set(&key.to_be_bytes(), &value(key, round));
                            if round % 7 == 3 && i % 5 == 0 {
                                store.del(&key.to_be_bytes());
                            }
                        }
                    }
                });
            }
            for _ in 0..3 {
                let store = &store;
                scope.spawn(move || {
                    for _ in 0..20 {
                        for key in 0..WRITERS * KEYS_PER_WRITER {
                            if let Some(v) = store.get(&key.to_be_bytes()) {
                                assert_eq!(v.len(), 24);
                                assert_eq!(&v[..4], &key.to_be_bytes());
                                let round = u32::from_be_bytes(v[4..8].try_into().unwrap());
                                assert!(round < ROUNDS);
                                assert!(v[8..].iter().all(|&b| b == key as u8));
                            }
                        }
                    }
                });
            }
        });
        // The last round (39) deletes nothing, so every key holds its final value.
        assert_eq!(store.len(), (WRITERS * KEYS_PER_WRITER) as usize);
        for key in 0..WRITERS * KEYS_PER_WRITER {
            assert_eq!(
                &*store.get(&key.to_be_bytes()).unwrap(),
                &value(key, ROUNDS - 1)[..]
            );
        }
    }
}
