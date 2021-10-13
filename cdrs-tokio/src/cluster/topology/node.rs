use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;
use tracing::*;

use crate::cluster::topology::NodeDistance;
use crate::cluster::{ConnectionManager, NodeInfo};
use crate::error::Result;
use crate::frame::Frame;
use crate::transport::CdrsTransport;

/// Metadata about a Cassandra node in the cluster, along with a connection.
pub struct Node<T: CdrsTransport, CM: ConnectionManager<T>> {
    connection_manager: Arc<CM>,
    connection: RwLock<Option<Arc<T>>>,
    broadcast_rpc_address: SocketAddr,
    distance: Option<NodeDistance>,
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> Debug for Node<T, CM> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("broadcast_rpc_address", &self.broadcast_rpc_address)
            .field("distance", &self.distance)
            .finish()
    }
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> Node<T, CM> {
    /// Creates a node from an point address. The node is considered ignored until a distance can
    /// be computed.
    pub fn new(connection_manager: Arc<CM>, broadcast_rpc_address: SocketAddr) -> Self {
        Node {
            connection_manager,
            connection: Default::default(),
            broadcast_rpc_address,
            distance: None,
        }
    }

    /// Return node address.
    #[inline]
    pub fn broadcast_rpc_address(&self) -> SocketAddr {
        self.broadcast_rpc_address
    }

    /// Returns connection to given node.
    pub async fn persistent_connection(&self) -> Result<Arc<T>> {
        {
            let connection = self.connection.read().await;
            if let Some(connection) = connection.deref() {
                if !connection.is_broken() {
                    return Ok(connection.clone());
                }
            }
        }

        let mut connection = self.connection.write().await;
        if let Some(connection) = connection.deref() {
            if !connection.is_broken() {
                // somebody established connection in the meantime
                return Ok(connection.clone());
            }
        }

        let new_connection = Arc::new(self.new_connection(None).await?);
        *connection = Some(new_connection.clone());

        Ok(new_connection)
    }

    /// Creates a new connection to the node with optional event sender.
    pub async fn new_connection(&self, event_handler: Option<Sender<Frame>>) -> Result<T> {
        debug!("Establishing new connection to node...");
        self.connection_manager
            .connection(event_handler, self.broadcast_rpc_address)
            .await
    }

    #[inline]
    pub fn distance(&self) -> Option<NodeDistance> {
        self.distance
    }

    /// This node should be ignored from establishing connections.
    #[inline]
    pub fn is_ignored(&self) -> bool {
        self.distance.is_none()
    }

    pub(crate) fn clone_with_node_info(&self, node_info: &NodeInfo) -> Self {
        Node {
            connection_manager: self.connection_manager.clone(),
            connection: Default::default(),
            broadcast_rpc_address: node_info.broadcast_rpc_address,
            distance: self.distance,
        }
    }
}
