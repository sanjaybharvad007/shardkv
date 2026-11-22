//! shardkv: a sharded in-memory key-value store served over TCP.
//!
//! * [`store`]: the concurrent map.
//! * [`protocol`]: the binary wire format and its zero-copy parser.
//! * [`server`]: the Tokio server.
//! * [`client`]: an async client with pipelining.

pub mod client;
pub mod protocol;
pub mod server;
pub mod store;

pub use client::{Client, ClientError, Reply};
pub use protocol::Status;
pub use server::{Config, Server};
pub use store::Store;
