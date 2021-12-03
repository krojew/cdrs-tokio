use cassandra_protocol::token::Murmur3Token;
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;
use std::sync::Arc;

use crate::cluster::topology::{Node, NodeMap};
use crate::cluster::ConnectionManager;
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

    /// Returns local nodes starting at given token and going in the direction of replicas.
    pub fn nodes_for_token_capped(
        &self,
        token: Murmur3Token,
        replica_count: usize,
    ) -> impl Iterator<Item = Arc<Node<T, CM>>> + '_ {
        self.token_ring
            .range(token..)
            .chain(self.token_ring.iter())
            .take(replica_count)
            .map(|(_, node)| node.clone())
    }

    /// Returns local nodes starting at given token and going in the direction of replicas.
    pub fn nodes_for_token(
        &self,
        token: Murmur3Token,
    ) -> impl Iterator<Item = Arc<Node<T, CM>>> + '_ {
        self.token_ring
            .range(token..)
            .chain(self.token_ring.iter())
            .take(self.token_ring.len())
            .map(|(_, node)| node.clone())
    }

    /// Creates a new map with a new node inserted.
    pub fn clone_with_node(&self, node: Arc<Node<T, CM>>) -> Self {
        let mut map = self.clone();
        for token in node.tokens() {
            map.token_ring.insert(*token, node.clone());
        }

        map
    }

    /// Creates a new map with a node removed.
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
    use cassandra_protocol::token::Murmur3Token;
    use itertools::Itertools;
    use lazy_static::lazy_static;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::Arc;
    use tokio::sync::watch;
    use uuid::Uuid;

    use crate::cluster::connection_manager::MockConnectionManager;
    use crate::cluster::connection_pool::ConnectionPoolFactory;
    use crate::cluster::topology::{Node, NodeMap};
    use crate::cluster::TokenMap;
    use crate::transport::MockCdrsTransport;

    lazy_static! {
        static ref HOST_ID_1: Uuid = Uuid::new_v4();
        static ref HOST_ID_2: Uuid = Uuid::new_v4();
        static ref HOST_ID_3: Uuid = Uuid::new_v4();
    }

    fn prepare_nodes() -> NodeMap<MockCdrsTransport, MockConnectionManager<MockCdrsTransport>> {
        let (_, keyspace_receiver) = watch::channel(None);
        let connection_manager = MockConnectionManager::<MockCdrsTransport>::new();
        let connection_pool_factory = Arc::new(ConnectionPoolFactory::new(
            Default::default(),
            Version::V4,
            connection_manager,
            keyspace_receiver,
        ));

        let mut nodes = NodeMap::default();
        nodes.insert(
            *HOST_ID_1,
            Arc::new(Node::new(
                connection_pool_factory.clone(),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 8080),
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
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 8080),
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
        verify_tokens(
            &[*HOST_ID_1, *HOST_ID_3, *HOST_ID_3, *HOST_ID_3, *HOST_ID_2],
            Murmur3Token::new(0),
        );
    }

    #[test]
    fn should_return_replicas_in_order_for_non_primary_token() {
        verify_tokens(&[*HOST_ID_3, *HOST_ID_2], Murmur3Token::new(3));
    }

    #[test]
    fn should_return_replicas_in_a_ring() {
        verify_tokens(
            &[*HOST_ID_2, *HOST_ID_1, *HOST_ID_1, *HOST_ID_1, *HOST_ID_3],
            Murmur3Token::new(20),
        );
    }
}
