use atomic::Atomic;
use cassandra_protocol::error::{Error, Result};
use cassandra_protocol::frame::Envelope;
use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::OnceCell;
use tracing::*;
use uuid::Uuid;

use crate::cluster::connection_pool::{ConnectionPool, ConnectionPoolFactory};
use crate::cluster::topology::{NodeDistance, NodeState};
use crate::cluster::Murmur3Token;
use crate::cluster::{ConnectionManager, NodeInfo};
use crate::load_balancing::node_distance_evaluator::NodeDistanceEvaluator;
use crate::transport::CdrsTransport;

/// Connectivity and state of a node, shared by every [`Node`] value representing the same
/// connection endpoint. Connection pool tasks refer to it rather than to a particular [`Node`], so
/// their state changes are visible on every clone, including the one in current metadata.
pub(crate) struct NodeShared<T: CdrsTransport + 'static, CM: ConnectionManager<T> + 'static> {
    connection_pool_factory: Arc<ConnectionPoolFactory<T, CM>>,
    connection_pool: OnceCell<Arc<ConnectionPool<T, CM>>>,
    broadcast_rpc_address: SocketAddr,
    // determines the pool size
    distance: Option<NodeDistance>,
    state: Atomic<NodeState>,
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> NodeShared<T, CM> {
    fn new(
        connection_pool_factory: Arc<ConnectionPoolFactory<T, CM>>,
        broadcast_rpc_address: SocketAddr,
        distance: Option<NodeDistance>,
        state: NodeState,
    ) -> Self {
        NodeShared {
            connection_pool_factory,
            connection_pool: Default::default(),
            broadcast_rpc_address,
            distance,
            state: Atomic::new(state),
        }
    }

    #[inline]
    pub(crate) fn broadcast_rpc_address(&self) -> SocketAddr {
        self.broadcast_rpc_address
    }

    #[inline]
    pub(crate) fn state(&self) -> NodeState {
        self.state.load(Ordering::Relaxed)
    }

    pub(crate) fn force_down(&self) {
        self.state.store(NodeState::ForcedDown, Ordering::Relaxed);
    }

    pub(crate) fn mark_down(&self) {
        self.try_set_state(NodeState::Down);
    }

    pub(crate) fn mark_up(&self) {
        self.try_set_state(NodeState::Up);
    }

    /// Sets the state, unless the node is forced down. Pool tasks stop for good once the node is
    /// forced down, so leaving that state requires a fresh `NodeShared`.
    pub(crate) fn try_set_state(&self, state: NodeState) -> bool {
        self.state
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                if current == NodeState::ForcedDown && state != NodeState::ForcedDown {
                    None
                } else {
                    Some(state)
                }
            })
            .is_ok()
    }
}

/// Metadata about a Cassandra node in the cluster, along with a connection.
pub struct Node<T: CdrsTransport + 'static, CM: ConnectionManager<T> + 'static> {
    shared: Arc<NodeShared<T, CM>>,
    broadcast_address: Option<SocketAddr>,
    host_id: Option<Uuid>,
    tokens: Vec<Murmur3Token>,
    rack: String,
    datacenter: String,
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> Debug for Node<T, CM> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("broadcast_rpc_address", &self.shared.broadcast_rpc_address)
            .field("broadcast_address", &self.broadcast_address)
            .field("distance", &self.shared.distance)
            .field("state", &self.shared.state)
            .field("host_id", &self.host_id)
            .field("tokens", &self.tokens)
            .field("rack", &self.rack)
            .field("datacenter", &self.datacenter)
            .finish()
    }
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> Node<T, CM> {
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        connection_pool_factory: Arc<ConnectionPoolFactory<T, CM>>,
        broadcast_rpc_address: SocketAddr,
        broadcast_address: Option<SocketAddr>,
        host_id: Option<Uuid>,
        distance: Option<NodeDistance>,
        tokens: Vec<Murmur3Token>,
        rack: String,
        datacenter: String,
    ) -> Self {
        Self::new_with_state(
            connection_pool_factory,
            broadcast_rpc_address,
            broadcast_address,
            host_id,
            distance,
            NodeState::Unknown,
            tokens,
            rack,
            datacenter,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_state(
        connection_pool_factory: Arc<ConnectionPoolFactory<T, CM>>,
        broadcast_rpc_address: SocketAddr,
        broadcast_address: Option<SocketAddr>,
        host_id: Option<Uuid>,
        distance: Option<NodeDistance>,
        state: NodeState,
        tokens: Vec<Murmur3Token>,
        rack: String,
        datacenter: String,
    ) -> Self {
        Self {
            shared: Arc::new(NodeShared::new(
                connection_pool_factory,
                broadcast_rpc_address,
                distance,
                state,
            )),
            broadcast_address,
            host_id,
            tokens,
            rack,
            datacenter,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_distance(
        connection_pool_factory: Arc<ConnectionPoolFactory<T, CM>>,
        broadcast_rpc_address: SocketAddr,
        broadcast_address: Option<SocketAddr>,
        host_id: Option<Uuid>,
        distance: NodeDistance,
    ) -> Self {
        Self::new(
            connection_pool_factory,
            broadcast_rpc_address,
            broadcast_address,
            host_id,
            Some(distance),
            Default::default(),
            Default::default(),
            Default::default(),
        )
    }

    #[cfg(test)]
    pub(crate) fn shared(&self) -> &Arc<NodeShared<T, CM>> {
        &self.shared
    }

    #[cfg(test)]
    pub(crate) fn shares_connectivity_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }

    #[cfg(test)]
    pub(crate) fn connection_pool(&self) -> Option<Arc<ConnectionPool<T, CM>>> {
        self.shared.connection_pool.get().cloned()
    }

    #[inline]
    pub fn state(&self) -> NodeState {
        self.shared.state()
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
        self.shared.broadcast_rpc_address
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

    /// Returns a connection to given node.
    #[inline]
    pub async fn persistent_connection(self: &Arc<Self>) -> Result<Arc<T>> {
        let shared = &self.shared;
        let pool = shared
            .connection_pool
            .get_or_try_init(|| {
                debug!(?self.host_id, "Creating connection pool");

                shared.connection_pool_factory.create(
                    shared.distance.unwrap_or(NodeDistance::Remote),
                    shared.broadcast_rpc_address,
                    Arc::downgrade(shared),
                )
            })
            .await;

        let pool = match pool {
            Ok(pool) => pool,
            Err(Error::InvalidProtocol(addr)) => {
                // we can't connect to this node even if it's up
                self.force_down();
                return Err(Error::InvalidProtocol(addr));
            }
            Err(error) => return Err(error),
        };

        pool.connection().await
    }

    /// Checks if any connection is still available.
    pub async fn is_any_connection_up(&self) -> bool {
        if let Some(pool) = self.shared.connection_pool.get() {
            pool.is_any_connection_up().await
        } else {
            false
        }
    }

    /// Creates a new connection to the node with optional event and error handlers.
    pub async fn new_connection(
        &self,
        event_handler: Option<Sender<Envelope>>,
        error_handler: Option<Sender<Error>>,
    ) -> Result<T> {
        debug!("Establishing new connection to node...");
        self.shared
            .connection_pool_factory
            .new_connection(
                event_handler,
                error_handler,
                self.shared.broadcast_rpc_address,
            )
            .await
    }

    /// Returns node distance in relation to the driver, if available.
    #[inline]
    pub fn distance(&self) -> Option<NodeDistance> {
        self.shared.distance
    }

    /// Checks if the node is local in relation to the driver.
    #[inline]
    pub fn is_local(&self) -> bool {
        self.shared.distance == Some(NodeDistance::Local)
    }

    /// Checks if the node is remote in relation to the driver.
    #[inline]
    pub fn is_remote(&self) -> bool {
        self.shared.distance == Some(NodeDistance::Remote)
    }

    /// Should this node be ignored from establishing connections.
    #[inline]
    pub fn is_ignored(&self) -> bool {
        self.shared.distance.is_none() || self.state() != NodeState::Up
    }

    pub(crate) fn force_down(&self) {
        self.shared.force_down();
    }

    /// Changes the state in place, visible on every node sharing connectivity with this one.
    /// Returns `false` if the node is forced down, which only a fresh node can leave.
    #[inline]
    pub(crate) fn try_set_state(&self, state: NodeState) -> bool {
        self.shared.try_set_state(state)
    }

    #[inline]
    pub(crate) fn clone_with_node_info(
        &self,
        node_info: NodeInfo,
        node_distance_evaluator: &dyn NodeDistanceEvaluator,
    ) -> Self {
        let shared = if self.broadcast_rpc_address() == node_info.broadcast_rpc_address {
            // A down node is excluded from load balancing, so unless its pool is reconnecting,
            // only an UP status event brings it back, and that event may have been sent while the
            // control connection was down. Being listed in the peers table, the node is treated as
            // up instead. Unknown and forced down nodes are left to topology events and load
            // balancers. Refreshes run inside rcu closures, which can run more than once, hence the
            // idempotent exchange.
            if self
                .shared
                .state
                .compare_exchange(
                    NodeState::Down,
                    NodeState::Up,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                debug!(
                    ?node_info.broadcast_rpc_address,
                    "Marked down node as up on refresh.",
                );
            }

            self.shared.clone()
        } else {
            // A node listed in the peers table under a new address is treated as up, like a newly
            // discovered node. Only an UP status event promotes an unknown node, and that event may
            // have been sent while the control connection was down, which would leave the node
            // ignored indefinitely. If the node is not reachable, it is handled like any other up
            // node whose connections fail.
            Arc::new(NodeShared::new(
                self.shared.connection_pool_factory.clone(),
                node_info.broadcast_rpc_address,
                node_distance_evaluator.compute_distance(&node_info),
                NodeState::Up,
            ))
        };

        Self {
            shared,
            broadcast_address: node_info.broadcast_address,
            host_id: Some(node_info.host_id),
            tokens: node_info.tokens,
            rack: node_info.rack,
            datacenter: node_info.datacenter,
        }
    }

    #[inline]
    pub(crate) fn clone_as_contact_point(
        &self,
        node_info: NodeInfo,
        node_distance_evaluator: &dyn NodeDistanceEvaluator,
    ) -> Self {
        // control points might have valid state already, so no need to reset
        Self {
            shared: Arc::new(NodeShared {
                connection_pool_factory: self.shared.connection_pool_factory.clone(),
                connection_pool: self.shared.connection_pool.clone(),
                broadcast_rpc_address: self.broadcast_rpc_address(),
                distance: node_distance_evaluator.compute_distance(&node_info),
                state: Atomic::new(self.state()),
            }),
            broadcast_address: node_info.broadcast_address,
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
        node_distance_evaluator: &dyn NodeDistanceEvaluator,
        state: NodeState,
    ) -> Self {
        Self {
            shared: Arc::new(NodeShared::new(
                self.shared.connection_pool_factory.clone(),
                node_info.broadcast_rpc_address,
                node_distance_evaluator.compute_distance(&node_info),
                state,
            )),
            broadcast_address: node_info.broadcast_address,
            host_id: Some(node_info.host_id),
            tokens: node_info.tokens,
            rack: node_info.rack,
            datacenter: node_info.datacenter,
        }
    }

    /// Shares connectivity with this node and changes the shared state, unless the node is forced
    /// down, in which case the clone gets fresh connectivity.
    #[inline]
    pub(crate) fn clone_with_node_state(&self, state: NodeState) -> Self {
        let shared = if self.try_set_state(state) {
            self.shared.clone()
        } else {
            Arc::new(NodeShared::new(
                self.shared.connection_pool_factory.clone(),
                self.broadcast_rpc_address(),
                self.distance(),
                state,
            ))
        };

        Self {
            shared,
            broadcast_address: self.broadcast_address,
            host_id: self.host_id,
            tokens: self.tokens.clone(),
            rack: self.rack.clone(),
            datacenter: self.datacenter.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use cassandra_protocol::frame::Version;
    use futures::future::ready;
    use futures::FutureExt;

    use crate::cluster::test_support::*;
    use crate::cluster::topology::NodeState;

    #[test]
    fn pool_tasks_do_not_revive_forced_down_node() {
        let connection_pool_factory = create_connection_pool_factory(
            TestConnectionManager::new(|_, _| ready(Ok(void_response(Version::V4))).boxed()),
            Version::V4,
        );
        let node = create_node(&connection_pool_factory, node_address(1));

        node.force_down();
        node.shared().mark_up();
        node.shared().mark_down();

        assert_eq!(node.state(), NodeState::ForcedDown);
    }
}
