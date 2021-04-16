//! **cdrs** is a native Cassandra DB client written in Rust.

#[macro_use]
pub mod macros;

pub mod cluster;
pub mod frame;
pub mod load_balancing;
pub mod query;
pub mod types;

pub mod authenticators;
pub mod compression;
pub mod consistency;
pub mod error;
pub mod events;
pub mod retry;
pub mod transport;

pub type Error = error::Error;
pub type Result<T> = error::Result<T>;
