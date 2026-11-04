use fxhash::FxHashMap;
use itertools::Itertools;
use rand::prelude::*;
use rand::rng;
use std::net::SocketAddr;
use std::sync::Arc;
use uuid::Uuid;

use crate::cluster::topology::keyspace_metadata::KeyspaceMetadata;
use crate::cluster::topology::node::Node;
use crate::cluster::topology::{DatacenterMetadata, NodeMap};
use crate::cluster::{ConnectionManager, TokenMap};
use crate::transport::CdrsTransport;

fn build_datacenter_info<T: CdrsTransport, CM: ConnectionManager<T>>(
    nodes: &NodeMap<T, CM>,
) -> FxHashMap<String, DatacenterMetadata> {
    let grouped_by_dc = nodes
        .values()
        .sorted_unstable_by_key(|node| node.datacenter())
        .chunk_by(|node| node.datacenter());

    (&grouped_by_dc)
        .into_iter()
        .map(|(dc, nodes)| {
            (
                dc.into(),
                DatacenterMetadata::new(nodes.unique_by(|node| node.rack()).count()),
            )
        })
        .collect()
}

// Only nodes owning tokens appear in the token ring, so only they can be replicas.
fn build_datacenter_node_counts<T: CdrsTransport, CM: ConnectionManager<T>>(
    nodes: &NodeMap<T, CM>,
) -> FxHashMap<String, usize> {
    let mut node_counts: FxHashMap<String, usize> = Default::default();
    for node in nodes.values().filter(|node| !node.tokens().is_empty()) {
        *node_counts.entry(node.datacenter().into()).or_default() += 1;
    }

    node_counts
}

/// Immutable metadata of the Cassandra cluster that this driver instance is connected to.
#[derive(Debug, Clone)]
pub struct ClusterMetadata<T: CdrsTransport + 'static, CM: ConnectionManager<T> + 'static> {
    nodes: NodeMap<T, CM>,
    token_map: TokenMap<T, CM>,
    keyspaces: FxHashMap<String, KeyspaceMetadata>,
    datacenters: FxHashMap<String, DatacenterMetadata>,
    datacenter_node_counts: FxHashMap<String, usize>,
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> ClusterMetadata<T, CM> {
    pub fn new(nodes: NodeMap<T, CM>, keyspaces: FxHashMap<String, KeyspaceMetadata>) -> Self {
        let token_map = TokenMap::new(&nodes);
        let datacenters = build_datacenter_info(&nodes);
        let datacenter_node_counts = build_datacenter_node_counts(&nodes);
        ClusterMetadata {
            nodes,
            token_map,
            keyspaces,
            datacenters,
            datacenter_node_counts,
        }
    }

    /// Returns current token map.
    #[inline]
    pub fn token_map(&self) -> &TokenMap<T, CM> {
        &self.token_map
    }

    /// Creates a new metadata with a keyspace replaced/added.
    #[must_use]
    pub fn clone_with_keyspace(&self, keyspace_name: String, keyspace: KeyspaceMetadata) -> Self {
        let mut keyspaces = self.keyspaces.clone();
        keyspaces.insert(keyspace_name, keyspace);

        ClusterMetadata {
            nodes: self.nodes.clone(),
            token_map: self.token_map.clone(),
            keyspaces,
            datacenters: self.datacenters.clone(),
            datacenter_node_counts: self.datacenter_node_counts.clone(),
        }
    }

    /// Creates a new metadata with a keyspace removed.
    #[must_use]
    pub fn clone_without_keyspace(&self, keyspace: &str) -> Self {
        let mut keyspaces = self.keyspaces.clone();
        keyspaces.remove(keyspace);

        ClusterMetadata {
            nodes: self.nodes.clone(),
            token_map: self.token_map.clone(),
            keyspaces,
            datacenters: self.datacenters.clone(),
            datacenter_node_counts: self.datacenter_node_counts.clone(),
        }
    }

    /// Creates a new metadata with a node replaced/added. The node must have a host id.
    #[must_use]
    pub fn clone_with_node(&self, node: Node<T, CM>) -> Self {
        let node = Arc::new(node);

        let mut nodes = self.nodes.clone();
        let old_node = nodes.insert(
            node.host_id().expect("Adding a node without host id!"),
            node.clone(),
        );

        // inserting only the new tokens would leave the replaced node's old tokens in the ring
        let token_map = match old_node {
            Some(old_node) if old_node.tokens() != node.tokens() => TokenMap::new(&nodes),
            _ => self.token_map.clone_with_node(node),
        };

        let datacenters = build_datacenter_info(&nodes);
        let datacenter_node_counts = build_datacenter_node_counts(&nodes);

        ClusterMetadata {
            nodes,
            token_map,
            keyspaces: self.keyspaces.clone(),
            datacenters,
            datacenter_node_counts,
        }
    }

    /// Creates a new metadata with a node removed.
    #[must_use]
    pub fn clone_without_node(&self, broadcast_rpc_address: SocketAddr) -> Self {
        let nodes = self
            .nodes
            .iter()
            .filter_map(|(host_id, node)| {
                if node.broadcast_rpc_address() != broadcast_rpc_address {
                    Some((*host_id, node.clone()))
                } else {
                    None
                }
            })
            .collect();

        Self::new(nodes, self.keyspaces.clone())
    }

    /// Returns all known nodes.
    #[inline]
    pub fn nodes(&self) -> &NodeMap<T, CM> {
        &self.nodes
    }

    /// Returns known keyspaces.
    #[inline]
    pub fn keyspaces(&self) -> &FxHashMap<String, KeyspaceMetadata> {
        &self.keyspaces
    }

    /// Returns known keyspace, if present.
    #[inline]
    pub fn keyspace(&self, keyspace: &str) -> Option<&KeyspaceMetadata> {
        self.keyspaces.get(keyspace)
    }

    /// Returns known datacenters.
    #[inline]
    pub fn datacenters(&self) -> &FxHashMap<String, DatacenterMetadata> {
        &self.datacenters
    }

    /// Returns known datacenter, if present.
    #[inline]
    pub fn datacenter(&self, name: &str) -> Option<&DatacenterMetadata> {
        self.datacenters.get(name)
    }

    /// Returns the number of nodes owning tokens in the given datacenter.
    #[inline]
    pub(crate) fn datacenter_node_count(&self, name: &str) -> usize {
        self.datacenter_node_counts.get(name).copied().unwrap_or(0)
    }

    /// Returns node that are not ignored for load balancing.
    #[inline]
    pub fn unignored_nodes(&self) -> Vec<Arc<Node<T, CM>>> {
        self.nodes
            .iter()
            .filter_map(|(_, node)| {
                if node.is_ignored() {
                    None
                } else {
                    Some(node.clone())
                }
            })
            .collect()
    }

    /// Returns node that are not ignored for load balancing and are local.
    #[inline]
    pub fn unignored_local_nodes(&self) -> Vec<Arc<Node<T, CM>>> {
        self.nodes
            .iter()
            .filter_map(|(_, node)| {
                if node.is_ignored() || !node.is_local() {
                    None
                } else {
                    Some(node.clone())
                }
            })
            .collect()
    }

    /// Returns node that are not ignored for load balancing and are remote, up to `max_count`
    /// randomly chosen nodes per datacenter.
    pub fn unignored_remote_nodes_capped(&self, max_count: usize) -> Vec<Arc<Node<T, CM>>> {
        let mut nodes_by_dc: FxHashMap<&str, Vec<Arc<Node<T, CM>>>> = Default::default();
        for node in self.nodes.values() {
            if !node.is_ignored() && node.is_remote() {
                nodes_by_dc
                    .entry(node.datacenter())
                    .or_default()
                    .push(node.clone());
            }
        }

        let mut rng = rng();
        nodes_by_dc
            .into_values()
            .flat_map(|mut nodes| {
                nodes.shuffle(&mut rng);
                nodes.truncate(max_count);
                nodes
            })
            .collect()
    }

    /// Checks if any nodes are known.
    #[inline]
    pub fn has_nodes(&self) -> bool {
        !self.nodes.is_empty()
    }

    /// Checks if a node with a given address is present.
    #[inline]
    pub fn has_node_by_rpc_address(&self, broadcast_rpc_address: SocketAddr) -> bool {
        self.nodes
            .iter()
            .any(|(_, node)| node.broadcast_rpc_address() == broadcast_rpc_address)
    }

    /// Finds a node by its address.
    #[inline]
    pub fn find_node_by_rpc_address(
        &self,
        broadcast_rpc_address: SocketAddr,
    ) -> Option<Arc<Node<T, CM>>> {
        self.nodes
            .iter()
            .find(|(_, node)| node.broadcast_rpc_address() == broadcast_rpc_address)
            .map(|(_, node)| node.clone())
    }

    /// Finds a node by its host id.
    #[inline]
    pub fn find_node_by_host_id(&self, host_id: &Uuid) -> Option<Arc<Node<T, CM>>> {
        self.nodes.get(host_id).cloned()
    }
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> Default for ClusterMetadata<T, CM> {
    fn default() -> Self {
        ClusterMetadata {
            nodes: Default::default(),
            token_map: Default::default(),
            keyspaces: Default::default(),
            datacenters: Default::default(),
            datacenter_node_counts: Default::default(),
        }
    }
}

//noinspection DuplicatedCode
#[cfg(test)]
mod tests {
    use cassandra_protocol::frame::Version;
    use fxhash::FxHashMap;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::Arc;
    use tokio::sync::watch;
    use uuid::Uuid;

    use crate::cluster::connection_manager::MockConnectionManager;
    use crate::cluster::connection_pool::ConnectionPoolFactory;
    use crate::cluster::topology::cluster_metadata::build_datacenter_info;
    use crate::cluster::topology::{Node, NodeDistance, NodeState};
    use crate::cluster::{ClusterMetadata, Murmur3Token};
    use crate::retry::MockReconnectionPolicy;
    use crate::transport::MockCdrsTransport;
    use fxhash::FxHashSet;

    #[test]
    fn should_build_datacenter_info() {
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

        let mut nodes = FxHashMap::default();
        nodes.insert(
            Uuid::new_v4(),
            Arc::new(Node::new(
                connection_pool_factory.clone(),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 8080),
                None,
                None,
                None,
                Default::default(),
                "r1".into(),
                "dc1".into(),
            )),
        );
        nodes.insert(
            Uuid::new_v4(),
            Arc::new(Node::new(
                connection_pool_factory.clone(),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 8080),
                None,
                None,
                None,
                Default::default(),
                "r1".into(),
                "dc1".into(),
            )),
        );
        nodes.insert(
            Uuid::new_v4(),
            Arc::new(Node::new(
                connection_pool_factory.clone(),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 8080),
                None,
                None,
                None,
                Default::default(),
                "r2".into(),
                "dc1".into(),
            )),
        );
        nodes.insert(
            Uuid::new_v4(),
            Arc::new(Node::new(
                connection_pool_factory,
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 8080),
                None,
                None,
                None,
                Default::default(),
                "r1".into(),
                "dc2".into(),
            )),
        );

        let dc_info = build_datacenter_info(&nodes);
        assert_eq!(dc_info.get("dc1").unwrap().rack_count, 2);
        assert_eq!(dc_info.get("dc2").unwrap().rack_count, 1);
    }

    #[test]
    fn should_cap_random_remote_nodes_per_datacenter() {
        let (_, keyspace_receiver) = watch::channel(None);
        let connection_pool_factory = Arc::new(ConnectionPoolFactory::new(
            Default::default(),
            Version::V4,
            MockConnectionManager::<MockCdrsTransport>::new(),
            keyspace_receiver,
            Arc::new(MockReconnectionPolicy::new()),
        ));

        let mut nodes = FxHashMap::default();
        for (port, dc, distance) in [
            (1, "dc1", NodeDistance::Local),
            (2, "dc2", NodeDistance::Remote),
            (3, "dc2", NodeDistance::Remote),
            (4, "dc2", NodeDistance::Remote),
            (5, "dc3", NodeDistance::Remote),
            (6, "dc3", NodeDistance::Remote),
            (7, "dc3", NodeDistance::Remote),
        ]
        .iter()
        {
            let host_id = Uuid::new_v4();
            nodes.insert(
                host_id,
                Arc::new(Node::new_with_state(
                    connection_pool_factory.clone(),
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), *port),
                    None,
                    Some(host_id),
                    Some(*distance),
                    NodeState::Up,
                    Default::default(),
                    "".into(),
                    dc.to_string(),
                )),
            );
        }

        let metadata = ClusterMetadata::new(nodes, Default::default());

        let mut seen_ports = FxHashSet::default();
        for _ in 0..50 {
            let remote_nodes = metadata.unignored_remote_nodes_capped(2);
            assert_eq!(remote_nodes.len(), 4);
            for dc in ["dc2", "dc3"].iter() {
                assert_eq!(
                    remote_nodes
                        .iter()
                        .filter(|node| node.datacenter() == *dc)
                        .count(),
                    2
                );
            }

            seen_ports.extend(
                remote_nodes
                    .iter()
                    .map(|node| node.broadcast_rpc_address().port()),
            );
        }

        assert_eq!(seen_ports.len(), 6, "remote nodes were not chosen randomly");
    }

    #[test]
    fn should_drop_old_tokens_when_replacing_node() {
        let (_, keyspace_receiver) = watch::channel(None);
        let connection_pool_factory = Arc::new(ConnectionPoolFactory::new(
            Default::default(),
            Version::V4,
            MockConnectionManager::<MockCdrsTransport>::new(),
            keyspace_receiver,
            Arc::new(MockReconnectionPolicy::new()),
        ));

        let host_id = Uuid::new_v4();
        let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9042);
        let create_node = |tokens| {
            Node::new(
                connection_pool_factory.clone(),
                address,
                None,
                Some(host_id),
                None,
                tokens,
                "".into(),
                "".into(),
            )
        };

        let mut nodes = FxHashMap::default();
        nodes.insert(host_id, Arc::new(create_node(vec![Murmur3Token::new(1)])));
        let metadata = ClusterMetadata::new(nodes, Default::default());

        let metadata = metadata.clone_with_node(create_node(vec![Murmur3Token::new(50)]));

        let ring_tokens: Vec<_> = metadata
            .token_map()
            .nodes_for_token(Murmur3Token::new(0))
            .map(|node| node.tokens().to_vec())
            .collect();
        assert_eq!(ring_tokens, vec![vec![Murmur3Token::new(50)]]);
    }
}
