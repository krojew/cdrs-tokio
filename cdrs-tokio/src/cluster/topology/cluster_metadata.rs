use std::sync::Arc;

use crate::cluster::topology::node::Node;
use crate::cluster::ConnectionManager;
use crate::transport::CdrsTransport;

/// Immutable metadata of the Cassandra cluster that this driver instance is connected to.
pub struct ClusterMetadata<T: CdrsTransport, CM: ConnectionManager<T>> {
    all_nodes: Vec<Arc<Node<T, CM>>>,
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> ClusterMetadata<T, CM> {
    pub fn new(contact_points: Vec<Arc<Node<T, CM>>>) -> Self {
        ClusterMetadata {
            all_nodes: contact_points,
        }
    }

    #[inline]
    pub fn all_nodes(&self) -> &Vec<Arc<Node<T, CM>>> {
        &self.all_nodes
    }
}
