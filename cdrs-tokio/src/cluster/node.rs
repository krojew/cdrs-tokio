use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;

use crate::cluster::ConnectionManager;
use crate::error::Result;
use crate::frame::Frame;
use crate::transport::CdrsTransport;

/// Metadata about a Cassandra node in the cluster, along with a connection.
pub struct Node<T: CdrsTransport, CM: ConnectionManager<T>> {
    connection_manager: CM,
    connection: RwLock<Option<Arc<T>>>,
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> Node<T, CM> {
    pub fn new(connection_manager: CM) -> Self {
        Node {
            connection_manager,
            connection: Default::default(),
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
        self.connection_manager.connection(event_handler).await
    }
}
