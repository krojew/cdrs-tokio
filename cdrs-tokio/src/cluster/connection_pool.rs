use atomic::Atomic;
use cassandra_protocol::frame::{Envelope, Version};
use cassandra_protocol::query::utils::quote;
use derive_more::Display;
use futures::future::join_all;
use itertools::Itertools;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::ops::DerefMut;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::watch::Receiver;
use tokio::sync::{mpsc, RwLock};
use tokio::time::sleep;
use tracing::*;

use crate::cluster::topology::{Node, NodeDistance, NodeState};
use crate::cluster::ConnectionManager;
use crate::error::{Error, Result as CdrsResult};
use crate::retry::{ReconnectionPolicy, ReconnectionSchedule};
use crate::transport::CdrsTransport;

#[derive(Copy, Clone, PartialEq, Eq, Display)]
enum ReconnectionState {
    NotRunning,
    InProgress,
    Disabled,
}

async fn new_connection<T: CdrsTransport, CM: ConnectionManager<T>>(
    connection_manager: &CM,
    broadcast_rpc_address: SocketAddr,
    timeout: Option<Duration>,
    error_handler: mpsc::Sender<Error>,
) -> CdrsResult<T> {
    if let Some(timeout) = timeout {
        tokio::time::timeout(
            timeout,
            connection_manager.connection(None, Some(error_handler), broadcast_rpc_address),
        )
        .await
        .map_err(|_| {
            Error::Timeout(format!(
                "Timeout waiting for connection to: {broadcast_rpc_address}"
            ))
        })
        .and_then(|result| result)
    } else {
        connection_manager
            .connection(None, Some(error_handler), broadcast_rpc_address)
            .await
    }
}

/// Configuration for node connection pools. By default, the pool size depends on the number of
/// cpu for local nodes and a fixed value for remote, and there is no timeout. If the distance to a
/// given node is unknown, it is treated as remote.
#[derive(Clone, Copy)]
pub struct ConnectionPoolConfig {
    local_size: usize,
    remote_size: usize,
    connect_timeout: Option<Duration>,
}

impl Default for ConnectionPoolConfig {
    fn default() -> Self {
        ConnectionPoolConfig {
            local_size: 1,
            remote_size: 1,
            connect_timeout: None,
        }
    }
}

impl ConnectionPoolConfig {
    /// Creates a new configuration for a pool of given size, with optional connect timeout.
    pub fn new(local_size: usize, remote_size: usize, connect_timeout: Option<Duration>) -> Self {
        assert!(local_size > 0 && remote_size > 0);
        ConnectionPoolConfig {
            local_size,
            remote_size,
            connect_timeout,
        }
    }
}

pub(crate) struct ConnectionPoolFactory<
    T: CdrsTransport + 'static,
    CM: ConnectionManager<T> + 'static,
> {
    config: ConnectionPoolConfig,
    version: Version,
    connection_manager: Arc<CM>,
    keyspace_receiver: Receiver<Option<String>>,
    reconnection_policy: Arc<dyn ReconnectionPolicy + Send + Sync>,
    _transport: PhantomData<T>,
}

impl<T: CdrsTransport + 'static, CM: ConnectionManager<T> + 'static> ConnectionPoolFactory<T, CM> {
    pub(crate) fn new(
        config: ConnectionPoolConfig,
        version: Version,
        connection_manager: CM,
        keyspace_receiver: Receiver<Option<String>>,
        reconnection_policy: Arc<dyn ReconnectionPolicy + Send + Sync>,
    ) -> Self {
        ConnectionPoolFactory {
            config,
            version,
            connection_manager: Arc::new(connection_manager),
            keyspace_receiver,
            reconnection_policy,
            _transport: Default::default(),
        }
    }

    #[inline]
    pub(crate) fn connection_manager(&self) -> &CM {
        self.connection_manager.as_ref()
    }

    pub(crate) async fn create(
        &self,
        node_distance: NodeDistance,
        broadcast_rpc_address: SocketAddr,
        node: Weak<Node<T, CM>>,
    ) -> CdrsResult<Arc<ConnectionPool<T, CM>>> {
        let (error_sender, error_receiver) =
            mpsc::channel(if node_distance == NodeDistance::Local {
                self.config.local_size
            } else {
                self.config.remote_size
            });

        let pool = Arc::new(
            ConnectionPool::new(
                &self.connection_manager,
                broadcast_rpc_address,
                node_distance,
                self.config,
                error_sender,
            )
            .await?,
        );

        Self::monitor_connections(
            error_receiver,
            Arc::downgrade(&pool),
            node,
            self.reconnection_policy.clone(),
        );

        // watch for keyspace changes
        let mut keyspace_receiver = self.keyspace_receiver.clone();
        let pool_clone = pool.clone();
        let version = self.version;

        tokio::spawn(async move {
            while let Ok(()) = keyspace_receiver.changed().await {
                let keyspace = keyspace_receiver.borrow().clone();
                if let Some(keyspace) = keyspace {
                    let use_envelope = Arc::new(Envelope::new_req_query(
                        format!("USE {}", quote(&keyspace)),
                        Default::default(),
                        None,
                        false,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        Default::default(),
                        version,
                    ));

                    let pool = pool_clone.pool.read().await;
                    join_all(pool.iter()
                        .filter(|connection| !connection.is_broken())
                        .map(|connection| {
                            let use_envelope = use_envelope.clone();
                            async move {
                                if let Err(error) = connection.write_envelope(use_envelope.as_ref(), false).await {
                                    error!(%error, ?broadcast_rpc_address, "Error settings keyspace for connection!");
                                }
                            }
                        })).await;
                }
            }
        });

        Ok(pool)
    }

    fn monitor_connections(
        mut receiver: mpsc::Receiver<Error>,
        pool: Weak<ConnectionPool<T, CM>>,
        node: Weak<Node<T, CM>>,
        reconnection_policy: Arc<dyn ReconnectionPolicy + Send + Sync>,
    ) {
        tokio::spawn(async move {
            let reconnection_state = Arc::new(Atomic::new(ReconnectionState::NotRunning));
            while receiver.recv().await.is_some() {
                // when one connection goes down, all of them will most likely go down, so we need
                // to protect against many reconnection attempts
                let state = reconnection_state.load(Ordering::Relaxed);
                if state != ReconnectionState::NotRunning {
                    if state == ReconnectionState::Disabled {
                        break;
                    }

                    continue;
                }

                reconnection_state.store(ReconnectionState::InProgress, Ordering::Relaxed);

                if let Some(node) = node.upgrade() {
                    let broadcast_rpc_address = node.broadcast_address();

                    if node.state() == NodeState::ForcedDown {
                        debug!(
                            ?broadcast_rpc_address,
                            "Not starting reconnection for a forced down node."
                        );
                        break;
                    }

                    warn!(
                        ?broadcast_rpc_address,
                        "Connection down. Starting reconnection."
                    );

                    node.mark_down();

                    let reconnection_schedule = reconnection_policy.new_node_schedule();
                    let reconnecting = reconnection_state.clone();
                    let pool = pool.clone();
                    let node = Arc::downgrade(&node);

                    tokio::spawn(async move {
                        let new_state =
                            Self::run_reconnection_loop(reconnection_schedule, pool).await;

                        reconnecting.store(new_state, Ordering::Relaxed);
                        debug!(?broadcast_rpc_address, %new_state, "Reconnection loop stopped.");

                        if new_state == ReconnectionState::Disabled {
                            if let Some(node) = node.upgrade() {
                                warn!(
                                    ?broadcast_rpc_address,
                                    "Forcing node down, since no connection can be established."
                                );
                                node.force_down();
                            }
                        } else if new_state == ReconnectionState::NotRunning {
                            if let Some(node) = node.upgrade() {
                                debug!(?broadcast_rpc_address, "All connections reestablished.");
                                node.mark_up();
                            }
                        }
                    });
                } else {
                    warn!("Node not found when trying to reconnect!");
                    break;
                };
            }

            debug!("Pool monitoring stopped.");
        });
    }

    async fn run_reconnection_loop(
        mut reconnection_schedule: Box<dyn ReconnectionSchedule + Send + Sync>,
        pool: Weak<ConnectionPool<T, CM>>,
    ) -> ReconnectionState {
        while let Some(delay) = reconnection_schedule.next_delay() {
            sleep(delay).await;

            let pool = match pool.upgrade() {
                None => return ReconnectionState::Disabled, // the pool might be gone
                Some(pool) => pool,
            };

            match pool.reconnect_broken().await {
                Ok(all_reconnected) if all_reconnected => return ReconnectionState::NotRunning,
                Err(Error::InvalidProtocol(_)) => return ReconnectionState::Disabled,
                _ => {}
            }
        }

        // the policy doesn't want to reconnect to this node
        ReconnectionState::Disabled
    }
}

pub(crate) struct ConnectionPool<T: CdrsTransport, CM: ConnectionManager<T>> {
    connection_manager: Weak<CM>,
    broadcast_rpc_address: SocketAddr,
    config: ConnectionPoolConfig,
    pool: RwLock<Vec<Arc<T>>>,
    desired_size: usize,
    current_index: AtomicUsize,
    error_sender: mpsc::Sender<Error>,
}

impl<T: CdrsTransport + 'static, CM: ConnectionManager<T>> ConnectionPool<T, CM> {
    async fn new(
        connection_manager: &Arc<CM>,
        broadcast_rpc_address: SocketAddr,
        node_distance: NodeDistance,
        config: ConnectionPoolConfig,
        error_sender: mpsc::Sender<Error>,
    ) -> CdrsResult<Self> {
        let desired_size = if node_distance == NodeDistance::Local {
            config.local_size
        } else {
            config.remote_size
        };

        // initialize the pool
        let pool: Vec<_> = join_all((0..desired_size).map(|_| {
            new_connection(
                connection_manager.as_ref(),
                broadcast_rpc_address,
                config.connect_timeout,
                error_sender.clone(),
            )
        }))
        .await
        .into_iter()
        .filter_map(|connection| match connection {
            Ok(connection) => Some(Ok(connection)),
            // propagate unrecoverable error
            Err(Error::InvalidProtocol(addr)) => Some(Err(Error::InvalidProtocol(addr))),
            // skip invalid connections which can be established later
            Err(_) => None,
        })
        .map_ok(Arc::new)
        .try_collect()?;

        if pool.len() != desired_size {
            // some connections have failed, but can be brought back up, so trigger reconnection
            let _ = error_sender
                .send(Error::General(
                    "Not all pool connections could be established!".to_string(),
                ))
                .await;
        }

        Ok(ConnectionPool {
            connection_manager: Arc::downgrade(connection_manager),
            broadcast_rpc_address,
            config,
            pool: RwLock::new(pool),
            desired_size,
            current_index: AtomicUsize::new(0),
            error_sender,
        })
    }

    pub(crate) async fn connection(&self) -> CdrsResult<Arc<T>> {
        let pool = self.pool.read().await;
        let pool_len = pool.len();
        let mut index = self.current_index.fetch_add(1, Ordering::Relaxed) % pool_len;
        let first_index = index;

        loop {
            let connection = &pool[index];
            if !connection.is_broken() {
                return Ok(connection.clone());
            }

            index = (index + 1) % pool_len;

            if index == first_index {
                // we've checked the whole pool and everything's down
                warn!(broadcast_rpc_address = %self.broadcast_rpc_address, "All connections down to node.");
                return Err(Error::General(format!(
                    "No active connections to: {}",
                    self.broadcast_rpc_address
                )));
            }
        }
    }

    async fn reconnect_broken(&self) -> CdrsResult<bool> {
        if let Some(connection_manager) = self.connection_manager.upgrade() {
            let mut pool = self.pool.write().await;

            // 1. try to reconnect broken
            for connection in pool.deref_mut() {
                if connection.is_broken() {
                    *connection = Arc::new(
                        new_connection(
                            connection_manager.as_ref(),
                            self.broadcast_rpc_address,
                            self.config.connect_timeout,
                            self.error_sender.clone(),
                        )
                        .await?,
                    );
                }
            }

            // 2. try to fill missing
            for _ in pool.len()..self.desired_size {
                pool.push(Arc::new(
                    new_connection(
                        connection_manager.as_ref(),
                        self.broadcast_rpc_address,
                        self.config.connect_timeout,
                        self.error_sender.clone(),
                    )
                    .await?,
                ));
            }

            // at this point either all connections are up or some might have died in the meantime,
            // which will trigger a new reconnection
            Ok(true)
        } else {
            // connection manager is gone - we're probably dropping the session
            Ok(false)
        }
    }
}
