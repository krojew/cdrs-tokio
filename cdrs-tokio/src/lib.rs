//! **cdrs** is a native Cassandra DB client written in Rust.
//!
//! ## Getting started
//!
//! This example configures a cluster consisting of a single node, and uses round-robin load balancing.
//!
//! ```no_run
//! use cdrs_tokio::cluster::session::{TcpSessionBuilder, SessionBuilder};
//! use cdrs_tokio::cluster::NodeTcpConfigBuilder;
//! use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() {
//!     let cluster_config = NodeTcpConfigBuilder::new()
//!         .with_contact_point("127.0.0.1:9042".into())
//!         .build()
//!         .await
//!         .unwrap();
//!     let session = TcpSessionBuilder::new(RoundRobinLoadBalancingStrategy::new(), cluster_config)
//!         .build()
//!         .await
//!         .unwrap();
//!
//!     let create_ks = "CREATE KEYSPACE IF NOT EXISTS test_ks WITH REPLICATION = { \
//!                      'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
//!     session
//!         .query(create_ks)
//!         .await
//!         .expect("Keyspace create error");
//! }
//! ```
//!
//! ## Nodes and load balancing
//!
//! In order to maximize efficiency, the driver needs to be appropriately configured for given use
//! case. Please look at available [load balancers](crate::load_balancing) and
//! [node distance evaluators](crate::load_balancing::node_distance_evaluator) to pick the optimal
//! solution when building the [`Session`](crate::cluster::session::Session). Topology-aware load
//! balancing is preferred when dealing with multi-node clusters, otherwise simpler strategies might
//! prove more efficient.

#[macro_use]
mod macros;

pub mod cluster;
pub mod envelope_parser;
pub mod load_balancing;

pub mod frame_encoding;
pub mod future;
pub mod retry;
pub mod speculative_execution;
pub mod statement;
pub mod transport;

pub use cassandra_protocol::authenticators;
pub use cassandra_protocol::compression;
pub use cassandra_protocol::consistency;
pub use cassandra_protocol::error;
pub use cassandra_protocol::frame;
pub use cassandra_protocol::query;
pub use cassandra_protocol::types;

pub type Error = error::Error;
pub type Result<T> = error::Result<T>;

#[cfg(feature = "derive")]
pub use cdrs_tokio_helpers_derive::{DbMirror, IntoCdrsValue, TryFromRow, TryFromUdt};
