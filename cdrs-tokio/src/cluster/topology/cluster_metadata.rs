use derive_more::Constructor;
use fxhash::FxHashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use uuid::Uuid;

use crate::cluster::topology::node::Node;
use crate::cluster::ConnectionManager;
use crate::transport::CdrsTransport;

/// Map from host id to a node.
pub type NodeMap<T, CM> = FxHashMap<Uuid, Arc<Node<T, CM>>>;

/// Immutable metadata of the Cassandra cluster that this driver instance is connected to.
// TODO: add token map
#[derive(Constructor)]
pub struct ClusterMetadata<T: CdrsTransport, CM: ConnectionManager<T>> {
    nodes: NodeMap<T, CM>,
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> ClusterMetadata<T, CM> {
    /// Creates a new metadata with a new set of nodes.
    pub fn clone_with_nodes(&self, nodes: NodeMap<T, CM>) -> Self {
        ClusterMetadata { nodes }
    }

    /// Creates a new metadata with a node replaced/added. The node must have a host id.
    pub fn clone_with_node(&self, node: Node<T, CM>) -> Self {
        let mut nodes = self.nodes.clone();
        nodes.insert(
            node.host_id().expect("Adding a node without host id!"),
            Arc::new(node),
        );

        ClusterMetadata { nodes }
    }

    /// Creates a new metadata with a node removed.
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

        ClusterMetadata { nodes }
    }

    /// Returns all known nodes.
    #[inline]
    pub fn nodes(&self) -> &NodeMap<T, CM> {
        &self.nodes
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

impl<T: CdrsTransport, CM: ConnectionManager<T>> Clone for ClusterMetadata<T, CM> {
    fn clone(&self) -> Self {
        ClusterMetadata {
            nodes: self.nodes.clone(),
        }
    }
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> Default for ClusterMetadata<T, CM> {
    fn default() -> Self {
        ClusterMetadata {
            nodes: Default::default(),
        }
    }
}
