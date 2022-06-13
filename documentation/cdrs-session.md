# CDRS Session

`Session` is a structure that holds as set pools of connections authorised by a Cluster. As well, it provides data decompressing and load balancing mechanisms used for Cassandra frame exchange, or querying in other words.

In order to create new session a [cluster config](./cluster-configuration.md) and a load balancing strategy must be provided. Load balancing strategy is used when some query should be performed by driver. At that moment load balancer returns a connection for a node that was picked up in accordance to a strategy.
Such logic guarantees that nodes' loads are balanced and there is no need to establish new connection if there is a one that is released after previous query.

## Load balancing

Any structure that implements `LoadBalancingStrategy` trait can be used in `Session` as a load balancer.

CDRS provides few strategies out of the box so no additional development may not be needed:

- `RandomLoadBalancingStrategy` randomly picks up a node from a cluster.

- `RoundRobinLoadBalancingStrategy` thread safe round-robin balancing strategy.

- `TopologyAwareLoadBalancingStrategy` policy taking dynamic cluster topology into account.

Along with that any custom load balancing strategy may be implemented and used with CDRS. The only requirement is the structure must implement `LoadBalancingStrategy` trait.

## Data compression

CQL binary protocol allows using LZ4 and Snappy (for protocol version < 5) data compression in order to reduce traffic between Node and Client.

CDRS provides methods for creating `Session` with different compression contexts: LZ4 and Snappy.

### Reference

1. LZ4 compression algorithm https://en.wikipedia.org/wiki/LZ4_(compression_algorithm).

2. Snappy compression algorithm https://en.wikipedia.org/wiki/Snappy_(compression).
