use std::marker::PhantomData;
use std::sync::Arc;

use crate::cluster::connection_manager::ThreadSafeReconnectionPolicy;
use crate::cluster::ConnectionManager;
use crate::error::Result;
use crate::transport::CdrsTransport;

/// Metadata about a Cassandra node in the cluster, along with a connection.
pub struct Node<T: CdrsTransport, CM: ConnectionManager<T>> {
    connection_manager: CM,
    _transport: PhantomData<T>,
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> Node<T, CM> {
    pub fn new(connection_manager: CM) -> Self {
        Node {
            connection_manager,
            _transport: Default::default(),
        }
    }

    /// Returns connection to given node.
    #[inline]
    pub async fn connection(
        &self,
        reconnection_policy: &ThreadSafeReconnectionPolicy,
    ) -> Result<Arc<T>> {
        self.connection_manager
            .connection(reconnection_policy)
            .await
    }
}
