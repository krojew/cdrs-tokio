use std::sync::Arc;

use crate::cluster::{ConnectionManager, Node};
use crate::transport::CdrsTransport;

/// Immutable metadata of the Cassandra cluster that this driver instance is connected to.
// TODO: add dynamic cluster
pub struct ClusterMetadata<T: CdrsTransport, CM: ConnectionManager<T>> {
    all_nodes: Vec<Arc<Node<T, CM>>>,
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> ClusterMetadata<T, CM> {
    pub fn new(all_nodes: Vec<Arc<Node<T, CM>>>) -> Self {
        ClusterMetadata { all_nodes }
    }

    #[inline]
    pub fn all_nodes(&self) -> &Vec<Arc<Node<T, CM>>> {
        &self.all_nodes
    }
}
