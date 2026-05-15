use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;
use std::sync::Arc;

use crate::cluster::topology::{Node, NodeMap};
use crate::cluster::ConnectionManager;
use crate::cluster::Murmur3Token;
use crate::transport::CdrsTransport;

/// Map of tokens to nodes.
pub struct TokenMap<T: CdrsTransport + 'static, CM: ConnectionManager<T> + 'static> {
    token_ring: BTreeMap<Murmur3Token, Arc<Node<T, CM>>>,
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> Clone for TokenMap<T, CM> {
    fn clone(&self) -> Self {
        TokenMap {
            token_ring: self.token_ring.clone(),
        }
    }
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> Debug for TokenMap<T, CM> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenMap")
            .field("token_ring", &self.token_ring)
            .finish()
    }
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> Default for TokenMap<T, CM> {
    fn default() -> Self {
        TokenMap {
            token_ring: Default::default(),
        }
    }
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> TokenMap<T, CM> {
    pub fn new(nodes: &NodeMap<T, CM>) -> Self {
        TokenMap {
            token_ring: nodes
                .iter()
                .flat_map(|(_, node)| {
                    node.tokens()
                        .iter()
                        .map(move |token| (*token, node.clone()))
                })
                .collect(),
        }
    }

    /// Returns up to `replica_count` distinct nodes starting at the given token
    /// and walking the token ring in the direction of replicas.
    ///
    /// With vnodes, a single physical node owns many tokens, so a naive walk
    /// can return the same node many times in a row. Cassandra's replication
    /// algorithm picks DISTINCT endpoints, so we dedup on each node's
    /// broadcast RPC address as we walk and stop once we've collected enough.
    pub fn nodes_for_token_capped(
        &self,
        token: Murmur3Token,
        replica_count: usize,
    ) -> impl Iterator<Item = Arc<Node<T, CM>>> + '_ {
        // Iterate ring positions starting from `token` and wrap around. We
        // can't use `Iterator::take(replica_count)` directly because
        // `replica_count` counts unique nodes, not ring positions.
        //
        // The set tracks broadcast RPC addresses we've already yielded; this
        // matches the identity used elsewhere in the load balancer
        // (`unique_by(|node| node.broadcast_rpc_address())`).
        let mut seen = std::collections::HashSet::with_capacity(replica_count);
        self.token_ring
            .range(token..)
            .chain(self.token_ring.iter())
            .filter_map(move |(_, node)| {
                if seen.len() >= replica_count {
                    // already collected the requested number of replicas
                    return None;
                }
                if seen.insert(node.broadcast_rpc_address()) {
                    Some(node.clone())
                } else {
                    None
                }
            })
    }

    /// Returns all distinct nodes starting at the given token and walking the
    /// token ring in the direction of replicas.
    ///
    /// As with `nodes_for_token_capped`, the dedup is on broadcast RPC
    /// address so a node owning many vnode tokens still appears only once.
    pub fn nodes_for_token(
        &self,
        token: Murmur3Token,
    ) -> impl Iterator<Item = Arc<Node<T, CM>>> + '_ {
        let mut seen = std::collections::HashSet::new();
        self.token_ring
            .range(token..)
            .chain(self.token_ring.iter())
            .filter_map(move |(_, node)| {
                if seen.insert(node.broadcast_rpc_address()) {
                    Some(node.clone())
                } else {
                    None
                }
            })
    }

    /// Creates a new map with a new node inserted.
    #[must_use]
    pub fn clone_with_node(&self, node: Arc<Node<T, CM>>) -> Self {
        let mut map = self.clone();
        for token in node.tokens() {
            map.token_ring.insert(*token, node.clone());
        }

        map
    }

    /// Creates a new map with a node removed.
    #[must_use]
    pub fn clone_without_node(&self, broadcast_rpc_address: SocketAddr) -> Self {
        let token_ring = self
            .token_ring
            .iter()
            .filter_map(|(token, node)| {
                if node.broadcast_rpc_address() == broadcast_rpc_address {
                    None
                } else {
                    Some((*token, node.clone()))
                }
            })
            .collect();

        TokenMap { token_ring }
    }
}

#[cfg(test)]
mod tests {
    use cassandra_protocol::frame::Version;
    use itertools::Itertools;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::{Arc, LazyLock};
    use tokio::sync::watch;
    use uuid::Uuid;

    use crate::cluster::connection_manager::MockConnectionManager;
    use crate::cluster::connection_pool::ConnectionPoolFactory;
    use crate::cluster::topology::{Node, NodeMap};
    use crate::cluster::Murmur3Token;
    use crate::cluster::TokenMap;
    use crate::retry::MockReconnectionPolicy;
    use crate::transport::MockCdrsTransport;

    static HOST_ID_1: LazyLock<Uuid> = LazyLock::new(Uuid::new_v4);
    static HOST_ID_2: LazyLock<Uuid> = LazyLock::new(Uuid::new_v4);
    static HOST_ID_3: LazyLock<Uuid> = LazyLock::new(Uuid::new_v4);

    fn prepare_nodes() -> NodeMap<MockCdrsTransport, MockConnectionManager<MockCdrsTransport>> {
        let (_, keyspace_receiver) = watch::channel(None);
        let connection_manager = MockConnectionManager::<MockCdrsTransport>::new();
        let reconnection_policy = MockReconnectionPolicy::new();
        let connection_pool_factory = Arc::new(ConnectionPoolFactory::new(
            Default::default(),
            Version::V4,
            connection_manager,
            keyspace_receiver,
            Arc::new(reconnection_policy),
        ));

        // each node gets a distinct broadcast RPC address so that dedup by
        // endpoint inside the token map can distinguish them
        let mut nodes = NodeMap::default();
        nodes.insert(
            *HOST_ID_1,
            Arc::new(Node::new(
                connection_pool_factory.clone(),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
                None,
                Some(*HOST_ID_1),
                None,
                vec![
                    Murmur3Token::new(-2),
                    Murmur3Token::new(-1),
                    Murmur3Token::new(0),
                ],
                "".into(),
                "".into(),
            )),
        );
        nodes.insert(
            *HOST_ID_2,
            Arc::new(Node::new(
                connection_pool_factory.clone(),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 8080),
                None,
                Some(*HOST_ID_2),
                None,
                vec![Murmur3Token::new(20)],
                "".into(),
                "".into(),
            )),
        );
        nodes.insert(
            *HOST_ID_3,
            Arc::new(Node::new(
                connection_pool_factory,
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 3)), 8080),
                None,
                Some(*HOST_ID_3),
                None,
                vec![
                    Murmur3Token::new(2),
                    Murmur3Token::new(1),
                    Murmur3Token::new(10),
                ],
                "".into(),
                "".into(),
            )),
        );

        nodes
    }

    fn verify_tokens(host_ids: &[Uuid], token: Murmur3Token) {
        let token_map = TokenMap::new(&prepare_nodes());
        let nodes = token_map
            .nodes_for_token_capped(token, host_ids.len())
            .collect_vec();

        assert_eq!(nodes.len(), host_ids.len());
        for (index, node) in nodes.iter().enumerate() {
            assert_eq!(node.host_id().unwrap(), host_ids[index]);
        }
    }

    #[test]
    fn should_return_replicas_in_order() {
        // ring walk from token 0 visits HOST_ID_1's primary token (0), then
        // HOST_ID_3's tokens (1, 2, 10), then HOST_ID_2 (20). Replicas must be
        // distinct so we expect each node exactly once.
        verify_tokens(
            &[*HOST_ID_1, *HOST_ID_3, *HOST_ID_2],
            Murmur3Token::new(0),
        );
    }

    #[test]
    fn should_return_replicas_in_order_for_non_primary_token() {
        verify_tokens(&[*HOST_ID_3, *HOST_ID_2], Murmur3Token::new(3));
    }

    // Cassandra's replication algorithm picks replicas as DISTINCT endpoints
    // walking the ring. With vnodes, a single physical node owns many tokens
    // and the same node can appear multiple consecutive ring positions, so
    // simply taking the first N tokens would return the same physical replica
    // multiple times - which is wrong: a "replica" is a copy on a different
    // node, not on the same one.
    #[test]
    fn should_return_distinct_nodes_as_replicas() {
        let token_map = TokenMap::new(&prepare_nodes());

        // there are only 3 distinct nodes in the ring - asking for 5 replicas
        // must yield those 3 nodes once each, not the same node padded out.
        let nodes = token_map
            .nodes_for_token_capped(Murmur3Token::new(0), 5)
            .collect_vec();
        let host_ids: Vec<_> = nodes.iter().map(|n| n.host_id().unwrap()).collect();
        assert_eq!(host_ids, vec![*HOST_ID_1, *HOST_ID_3, *HOST_ID_2]);
    }

    #[test]
    fn should_cap_replica_count_when_smaller_than_distinct_nodes() {
        let token_map = TokenMap::new(&prepare_nodes());

        // when fewer replicas than distinct nodes are requested, the iterator
        // should stop at exactly that count (no need to enumerate the rest).
        let nodes = token_map
            .nodes_for_token_capped(Murmur3Token::new(0), 2)
            .collect_vec();
        let host_ids: Vec<_> = nodes.iter().map(|n| n.host_id().unwrap()).collect();
        assert_eq!(host_ids, vec![*HOST_ID_1, *HOST_ID_3]);
    }

    #[test]
    fn should_return_replicas_in_a_ring() {
        // starting at token 20 we hit HOST_ID_2 first, then wrap around to the
        // smaller tokens which belong to HOST_ID_1, then to HOST_ID_3.
        verify_tokens(
            &[*HOST_ID_2, *HOST_ID_1, *HOST_ID_3],
            Murmur3Token::new(20),
        );
    }
}
