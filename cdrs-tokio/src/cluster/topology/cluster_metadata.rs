use fxhash::FxHashMap;
use std::sync::Arc;
use uuid::Uuid;

use crate::cluster::topology::node::Node;
use crate::cluster::ConnectionManager;
use crate::transport::CdrsTransport;

/// Map from host id to a node.
pub type NodeMap<T, CM> = FxHashMap<Uuid, Arc<Node<T, CM>>>;

/// Immutable metadata of the Cassandra cluster that this driver instance is connected to.
// TODO: add token map
pub struct ClusterMetadata<T: CdrsTransport, CM: ConnectionManager<T>> {
    nodes: NodeMap<T, CM>,
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> ClusterMetadata<T, CM> {
    /// Creates initial metadata.
    pub fn new(nodes: NodeMap<T, CM>) -> Self {
        ClusterMetadata { nodes }
    }

    /// Creates a new metadata with a new set of nodes.
    pub fn clone_with_nodes(&self, nodes: NodeMap<T, CM>) -> Self {
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
