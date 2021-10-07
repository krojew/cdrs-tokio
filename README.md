# CDRS tokio [![crates.io version](https://img.shields.io/crates/v/cdrs-tokio.svg)](https://crates.io/crates/cdrs-tokio) ![build status](https://github.com/krojew/cdrs-tokio/actions/workflows/rust.yml/badge.svg)

<p align="center">
  <img src="./cdrs-logo.png" alt="CDRS tokio - async Apache Cassandra driver using tokio"/>
</p>

CDRS is production-ready Apache **C**assandra **d**river written in pure **R**u**s**t.

## Features

- Asynchronous API;
- TCP/TLS connection;
- Configurable load balancing;
- Connection pooling;
- Configurable connection strategies;
- LZ4, Snappy compression;
- Cassandra-to-Rust data deserialization;
- Pluggable authentication strategies;
- [ScyllaDB](https://www.scylladb.com/) support;
- Server events listening;
- Multiple CQL version support (3, 4), full spec implementation;
- Query tracing information;
- Prepared statements;
- Query paging;
- Batch statements;
- Configurable retry and reconnection policy;
- Support for interleaved queries;

## Documentation and examples

- [User guide](./documentation).
- [Examples](./cdrs-tokio/examples).
- API docs (release).
- Using ScyllaDB with RUST [lesson](https://university.scylladb.com/courses/using-scylla-drivers/lessons/rust-and-scylla/).

## Getting started

This example configures a cluster consisting of a single node, and uses round-robin load balancing.

```rust
use cdrs_tokio::cluster::session::{TcpSessionBuilder, SessionBuilder};
use cdrs_tokio::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder};
use cdrs_tokio::load_balancing::RoundRobinBalancingStrategy;
use cdrs_tokio::query::*;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let nodes = NodeTcpConfigBuilder::new()
        .with_node_address("127.0.0.1:9042".into())
        .build()
        .await
        .unwrap();
    let cluster_config = ClusterTcpConfig(nodes);
    let session = TcpSessionBuilder::new(RoundRobinBalancingStrategy::new(), cluster_config).build();

    let create_ks = "CREATE KEYSPACE IF NOT EXISTS test_ks WITH REPLICATION = { \
                     'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
    session
        .query(create_ks)
        .await
        .expect("Keyspace create error");
}
```

## License

This project is licensed under either of

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0))
- MIT license ([LICENSE-MIT](LICENSE-MIT) or [http://opensource.org/licenses/MIT](http://opensource.org/licenses/MIT))

at your option.
