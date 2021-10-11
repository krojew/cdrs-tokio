use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;
use tracing::*;

use crate::cluster::topology::NodeDistance;
use crate::cluster::ConnectionManager;
use crate::error::Result;
use crate::frame::Frame;
use crate::transport::CdrsTransport;

/// Metadata about a Cassandra node in the cluster, along with a connection.
pub struct Node<T: CdrsTransport, CM: ConnectionManager<T>> {
    connection_manager: Arc<CM>,
    connection: RwLock<Option<Arc<T>>>,
    addr: SocketAddr,
    distance: NodeDistance,
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> Node<T, CM> {
    pub fn new_contact_point(connection_manager: Arc<CM>, addr: SocketAddr) -> Self {
        Node {
            connection_manager,
            connection: Default::default(),
            addr,
            // let's assume contact points are local, until first topology refresh
            distance: NodeDistance::Local,
        }
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
            .connection(event_handler, self.addr)
            .await
    }

    #[inline]
    pub fn distance(&self) -> NodeDistance {
        self.distance
    }

    /// This node should be ignored from establishing connections.
    #[inline]
    pub fn is_ignored(&self) -> bool {
        self.distance == NodeDistance::Ignored
    }
}
