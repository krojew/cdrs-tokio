# CDRS tokio [![crates.io version](https://img.shields.io/crates/v/cdrs-tokio.svg)](https://crates.io/crates/cdrs-tokio) ![build status](https://github.com/krojew/cdrs-tokio/actions/workflows/rust.yml/badge.svg)

![CDRS tokio - async Apache Cassandra driver using tokio](./cdrs-logo.png)

CDRS is production-ready Apache **C**assandra **d**river written in pure **R**u**s**t. Focuses on providing high
level of configurability to suit most use cases at any scale, as its Java counterpart, while also leveraging the
safety and performance of Rust.

## Features

- Asynchronous API;
- TCP/TLS connection (rustls);
- Topology-aware dynamic and configurable load balancing;
- Configurable connection strategies and pools;
- Configurable speculative execution;
- LZ4, Snappy compression;
- Cassandra-to-Rust data serialization/deserialization with custom type support;
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
- Support for Yugabyte YCQL JSONB;

## Performance

Due to high configurability of **CDRS**, the performance will vary depending on use case. The following benchmarks
have been made against the latest (master as of 03-12-2012) versions of respective libraries (except 
cassandra-cpp: 2.16.0).

- `cdrs-tokio-large-pool` - **CDRS** with node connection pool equal to double of physical CPU cores
- `cdrs-tokio-small-pool` - **CDRS** with a single connection per node
- `scylladb-rust-large-pool` - `scylla` crate with node connection pool equal to double of physical CPU cores
- `scylladb-rust-small-pool` - `scylla` crate with a single connection per node
- `cassandra-cpp` - Rust bindings for Datastax C++ Driver, running on multiple threads using Tokio
- `gocql` - a driver written in Go

![insert benchmark](./perf-inserts.png)
![select benchmark](./perf-selects.png)
![mixed benchmark](./perf-mixed.png)

Knowing given use case, CDRS can be optimized for peak performance.

## Documentation and examples

- [User guide](./documentation).
- [Examples](./cdrs-tokio/examples).
- [API docs](https://docs.rs/cdrs-tokio/latest/cdrs_tokio/).
- Using ScyllaDB with RUST [lesson](https://university.scylladb.com/courses/using-scylla-drivers/lessons/rust-and-scylla/).

## Getting started

This example configures a cluster consisting of a single node without authentication, and uses round-robin 
load balancing. Other options are kept as default.

```rust
use cdrs_tokio::cluster::session::{TcpSessionBuilder, SessionBuilder};
use cdrs_tokio::cluster::NodeTcpConfigBuilder;
use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
use cdrs_tokio::query::*;

#[tokio::main]
async fn main() {
    let cluster_config = NodeTcpConfigBuilder::new()
        .with_contact_point("127.0.0.1:9042".into())
        .build()
        .await
        .unwrap();
    let session = TcpSessionBuilder::new(RoundRobinLoadBalancingStrategy::new(), cluster_config).build();

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
