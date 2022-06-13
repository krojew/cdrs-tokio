//! A generic cassandra protocol crate.
//! Built in coordination with cdrs-tokio but is flexible for many usecases.

extern crate core;

#[macro_use]
mod macros;

pub mod frame;
pub mod query;
pub mod types;

pub mod authenticators;
pub mod compression;
pub mod consistency;
pub mod crc;
pub mod error;
pub mod events;
pub mod token;

pub type Error = error::Error;
pub type Result<T> = error::Result<T>;
