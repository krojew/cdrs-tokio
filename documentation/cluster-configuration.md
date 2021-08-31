### Cluster configuration

Apache Cassandra is designed to be a scalable and higly available database. So most often developers work with multi node Cassandra clusters. For instance Apple's setup includes 75k nodes, Netflix 2.5k nodes, Ebay >100 nodes.

That's why CDRS driver was designed with multi-node support in mind. In order to connect to Cassandra cluster via CDRS connection configuration should be provided:

```rust
use cdrs_tokio::authenticators::NoneAuthenticatorProvider;
use cdrs_tokio::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder};

fn main() {
  let node = NodeTcpConfigBuilder::new("127.0.0.1:9042".parse().unwrap(), Arc::new(NoneAuthenticatorProvider)).build();
  let cluster_config = ClusterTcpConfig(vec![node]);
}
```

For each node configuration, `SaslAuthenticatorProvider` should be provided. `SaslAuthenticatorProvider` is a trait that the structure should implement so it can be used by CDRS session for authentication. Out of the box CDRS provides two types of authenticators:

- `cdrs_tokio::authenticators::NoneAuthenticatorProvider` that should be used if authentication is disabled by a node ([Cassandra authenticator](http://cassandra.apache.org/doc/latest/configuration/cassandra_config_file.html#authenticator) is set to `AllowAllAuthenticator`) on server.

- `cdrs_tokio::authenticators::StaticPasswordAuthenticatorProvider` that should be used if authentication is enabled on the server and [authenticator](http://cassandra.apache.org/doc/latest/configuration/cassandra_config_file.html#authenticator) is `PasswordAuthenticator`.

```rust
use cdrs_tokio::authenticators::StaticPasswordAuthenticatorProvider;
let authenticator = StaticPasswordAuthenticatorProvider::new("user", "pass");
```

If a node has a custom authentication strategy, corresponded `SaslAuthenticatorProvider` should be implemented by a developer and further used in `NodeTcpConfigBuilder`.

### Reference

1. Cassandra cluster configuration https://docs.datastax.com/en/cassandra/3.0/cassandra/initialize/initTOC.html.

2. ScyllaDB cluster configuration https://docs.scylladb.com/operating-scylla/ (see Cluster Management section).
