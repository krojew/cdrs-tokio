use atomic::Atomic;
use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;
use tracing::*;
use uuid::Uuid;

use crate::cluster::topology::{NodeDistance, NodeState};
use crate::cluster::{ConnectionManager, Murmur3Token, NodeInfo};
use crate::error::{Error, Result};
use crate::frame::Frame;
use crate::transport::CdrsTransport;

/// Metadata about a Cassandra node in the cluster, along with a connection.
pub struct Node<T: CdrsTransport, CM: ConnectionManager<T>> {
    connection_manager: Arc<CM>,
    connection: RwLock<Option<Arc<T>>>,
    broadcast_rpc_address: SocketAddr,
    broadcast_address: Option<SocketAddr>,
    distance: Option<NodeDistance>,
    state: Atomic<NodeState>,
    host_id: Option<Uuid>,
    tokens: Vec<Murmur3Token>,
    rack: String,
    datacenter: String,
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> Debug for Node<T, CM> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("broadcast_rpc_address", &self.broadcast_rpc_address)
            .field("broadcast_address", &self.broadcast_address)
            .field("distance", &self.distance)
            .field("state", &self.state)
            .field("host_id", &self.host_id)
            .field("tokens", &self.tokens)
            .field("rack", &self.rack)
            .field("datacenter", &self.datacenter)
            .finish()
    }
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> Node<T, CM> {
    /// Creates a node from an address. The node state nad distance is unknown at this time.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        connection_manager: Arc<CM>,
        broadcast_rpc_address: SocketAddr,
        broadcast_address: Option<SocketAddr>,
        host_id: Option<Uuid>,
        distance: Option<NodeDistance>,
        tokens: Vec<Murmur3Token>,
        rack: String,
        datacenter: String,
    ) -> Self {
        Node {
            connection_manager,
            connection: Default::default(),
            broadcast_rpc_address,
            broadcast_address,
            distance,
            state: Atomic::new(NodeState::Unknown),
            host_id,
            tokens,
            rack,
            datacenter,
        }
    }

    /// Creates a node from an address. The node state nad distance is unknown at this time.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_state(
        connection_manager: Arc<CM>,
        broadcast_rpc_address: SocketAddr,
        broadcast_address: Option<SocketAddr>,
        host_id: Option<Uuid>,
        distance: Option<NodeDistance>,
        state: NodeState,
        tokens: Vec<Murmur3Token>,
        rack: String,
        datacenter: String,
    ) -> Self {
        Node {
            connection_manager,
            connection: Default::default(),
            broadcast_rpc_address,
            broadcast_address,
            distance,
            state: Atomic::new(state),
            host_id,
            tokens,
            rack,
            datacenter,
        }
    }

    /// Creates a node from an address. The node distance is unknown at this time.
    #[allow(clippy::too_many_arguments)]
    pub fn with_state(
        connection_manager: Arc<CM>,
        broadcast_rpc_address: SocketAddr,
        broadcast_address: Option<SocketAddr>,
        host_id: Option<Uuid>,
        state: NodeState,
        tokens: Vec<Murmur3Token>,
        rack: String,
        datacenter: String,
    ) -> Self {
        Node {
            connection_manager,
            connection: Default::default(),
            broadcast_rpc_address,
            broadcast_address,
            distance: None,
            state: Atomic::new(state),
            host_id,
            tokens,
            rack,
            datacenter,
        }
    }

    #[cfg(test)]
    pub fn with_distance(
        connection_manager: Arc<CM>,
        broadcast_rpc_address: SocketAddr,
        broadcast_address: Option<SocketAddr>,
        host_id: Option<Uuid>,
        distance: NodeDistance,
    ) -> Self {
        Node {
            connection_manager,
            connection: Default::default(),
            broadcast_rpc_address,
            broadcast_address,
            distance: Some(distance),
            state: Atomic::new(NodeState::Unknown),
            host_id,
            tokens: Default::default(),
            rack: Default::default(),
            datacenter: Default::default(),
        }
    }

    #[inline]
    pub fn state(&self) -> NodeState {
        self.state.load(Ordering::Relaxed)
    }

    /// The host ID that is assigned to this node by Cassandra. This value can be used to uniquely
    /// identify a node even when the underling IP address changes.
    #[inline]
    pub fn host_id(&self) -> Option<Uuid> {
        self.host_id
    }

    /// The node's broadcast RPC address. That is, the address that the node expects clients to
    /// connect to.
    #[inline]
    pub fn broadcast_rpc_address(&self) -> SocketAddr {
        self.broadcast_rpc_address
    }

    /// The node's broadcast address. That is, the address that other nodes use to communicate with
    /// that node.
    #[inline]
    pub fn broadcast_address(&self) -> Option<SocketAddr> {
        self.broadcast_address
    }

    /// Returns tokens associated with the node.
    #[inline]
    pub fn tokens(&self) -> &[Murmur3Token] {
        &self.tokens
    }

    /// Returns the dc the node is in.
    #[inline]
    pub fn datacenter(&self) -> &str {
        &self.datacenter
    }

    /// Returns the rack the node is in.
    #[inline]
    pub fn rack(&self) -> &str {
        &self.rack
    }

    /// Returns connection to given node.
    pub async fn persistent_connection(&self) -> Result<Arc<T>> {
        {
            let connection = self.connection.read().await;
            if let Some(connection) = connection.deref() {
                if !connection.is_broken() {
                    return Ok(connection.clone());
                }

                // note: make sure this is protected by connection guard
                self.state.store(NodeState::Down, Ordering::Relaxed);
            }
        }

        let mut connection = self.connection.write().await;
        if let Some(connection) = connection.deref() {
            if !connection.is_broken() {
                // somebody established connection in the meantime
                return Ok(connection.clone());
            }
        }

        let new_connection = Arc::new(self.new_connection(None, None).await?);
        *connection = Some(new_connection.clone());

        // note: make sure this is protected by connection guard
        self.state.store(NodeState::Up, Ordering::Relaxed);

        Ok(new_connection)
    }

    /// Creates a new connection to the node with optional event and error handlers.
    pub async fn new_connection(
        &self,
        event_handler: Option<Sender<Frame>>,
        error_handler: Option<Sender<Error>>,
    ) -> Result<T> {
        debug!("Establishing new connection to node...");
        self.connection_manager
            .connection(event_handler, error_handler, self.broadcast_rpc_address)
            .await
    }

    /// Returns node distance in relation to the driver, if available.
    #[inline]
    pub fn distance(&self) -> Option<NodeDistance> {
        self.distance
    }

    /// Checks if the node is local in relation to the driver.
    #[inline]
    pub fn is_local(&self) -> bool {
        self.distance == Some(NodeDistance::Local)
    }

    /// Checks if the node is remote in relation to the driver.
    #[inline]
    pub fn is_remote(&self) -> bool {
        self.distance == Some(NodeDistance::Remote)
    }

    /// Should this node be ignored from establishing connections.
    #[inline]
    pub fn is_ignored(&self) -> bool {
        self.distance.is_none() || self.state.load(Ordering::Relaxed) != NodeState::Up
    }

    #[inline]
    pub(crate) fn clone_with_node_info(&self, node_info: NodeInfo) -> Self {
        Node {
            connection_manager: self.connection_manager.clone(),
            connection: Default::default(),
            broadcast_rpc_address: node_info.broadcast_rpc_address,
            broadcast_address: node_info.broadcast_address,
            // since address could change, we can't be sure of distance or state
            distance: None,
            state: Atomic::new(NodeState::Unknown),
            host_id: Some(node_info.host_id),
            tokens: node_info.tokens,
            rack: node_info.rack,
            datacenter: node_info.datacenter,
        }
    }

    #[inline]
    pub(crate) async fn clone_as_contact_point(&self, node_info: NodeInfo) -> Self {
        // control points might have valid state already, so no need to reset
        Node {
            connection_manager: self.connection_manager.clone(),
            connection: RwLock::new(self.connection.read().await.clone()),
            broadcast_rpc_address: self.broadcast_rpc_address,
            broadcast_address: node_info.broadcast_address,
            distance: self.distance,
            state: Atomic::new(self.state.load(Ordering::Relaxed)),
            host_id: Some(node_info.host_id),
            tokens: node_info.tokens,
            rack: node_info.rack,
            datacenter: node_info.datacenter,
        }
    }

    #[inline]
    pub(crate) fn clone_with_node_info_and_state(
        &self,
        node_info: NodeInfo,
        state: NodeState,
    ) -> Self {
        Node {
            connection_manager: self.connection_manager.clone(),
            connection: Default::default(),
            broadcast_rpc_address: node_info.broadcast_rpc_address,
            broadcast_address: node_info.broadcast_address,
            // since address could change, we can't be sure of distance
            distance: None,
            state: Atomic::new(state),
            host_id: Some(node_info.host_id),
            tokens: node_info.tokens,
            rack: node_info.rack,
            datacenter: node_info.datacenter,
        }
    }

    #[inline]
    pub(crate) fn clone_with_node_state(&self, state: NodeState) -> Self {
        Node {
            connection_manager: self.connection_manager.clone(),
            connection: Default::default(),
            broadcast_rpc_address: self.broadcast_rpc_address,
            broadcast_address: self.broadcast_address,
            distance: self.distance,
            state: Atomic::new(state),
            host_id: self.host_id,
            tokens: self.tokens.clone(),
            rack: self.rack.clone(),
            datacenter: self.datacenter.clone(),
        }
    }
}
