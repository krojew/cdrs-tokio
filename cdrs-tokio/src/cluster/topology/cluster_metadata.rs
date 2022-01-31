use fxhash::FxHashMap;
use itertools::Itertools;
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
        .group_by(|node| node.datacenter());

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

/// Immutable metadata of the Cassandra cluster that this driver instance is connected to.
#[derive(Debug, Clone)]
pub struct ClusterMetadata<T: CdrsTransport + 'static, CM: ConnectionManager<T> + 'static> {
    nodes: NodeMap<T, CM>,
    token_map: TokenMap<T, CM>,
    keyspaces: FxHashMap<String, KeyspaceMetadata>,
    datacenters: FxHashMap<String, DatacenterMetadata>,
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> ClusterMetadata<T, CM> {
    pub fn new(nodes: NodeMap<T, CM>, keyspaces: FxHashMap<String, KeyspaceMetadata>) -> Self {
        let token_map = TokenMap::new(&nodes);
        let datacenters = build_datacenter_info(&nodes);
        ClusterMetadata {
            nodes,
            token_map,
            keyspaces,
            datacenters,
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
        }
    }

    /// Creates a new metadata with a node replaced/added. The node must have a host id.
    #[must_use]
    pub fn clone_with_node(&self, node: Node<T, CM>) -> Self {
        let node = Arc::new(node);
        let token_map = self.token_map.clone_with_node(node.clone());

        let mut nodes = self.nodes.clone();
        nodes.insert(
            node.host_id().expect("Adding a node without host id!"),
            node,
        );

        let datacenters = build_datacenter_info(&nodes);

        ClusterMetadata {
            nodes,
            token_map,
            keyspaces: self.keyspaces.clone(),
            datacenters,
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

    /// Returns node that are not ignored for load balancing and are remote.
    #[inline]
    pub fn unignored_remote_nodes_capped(&self, max_count: usize) -> Vec<Arc<Node<T, CM>>> {
        self.nodes
            .iter()
            .filter_map(|(_, node)| {
                if node.is_ignored() || !node.is_remote() {
                    None
                } else {
                    Some(node.clone())
                }
            })
            .take(max_count)
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
    use crate::cluster::topology::Node;
    use crate::transport::MockCdrsTransport;

    #[test]
    fn should_build_datacenter_info() {
        let (_, keyspace_receiver) = watch::channel(None);
        let connection_manager = MockConnectionManager::<MockCdrsTransport>::new();
        let connection_pool_factory = Arc::new(ConnectionPoolFactory::new(
            Default::default(),
            Version::V4,
            connection_manager,
            keyspace_receiver,
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
}
