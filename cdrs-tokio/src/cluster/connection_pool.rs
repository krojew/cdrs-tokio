use arc_swap::{ArcSwap, AsRaw};
use cassandra_protocol::frame::{Frame, Version};
use cassandra_protocol::query::utils::quote;
use futures::future::{join_all, try_join_all};
use num_cpus::get_physical;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch::Receiver;
use tracing::*;

use crate::cluster::topology::NodeDistance;
use crate::cluster::ConnectionManager;
use crate::error::{Error, Result as CdrsResult};
use crate::transport::CdrsTransport;

async fn new_connection<T: CdrsTransport, CM: ConnectionManager<T>>(
    connection_manager: &CM,
    broadcast_rpc_address: SocketAddr,
    timeout: Option<Duration>,
) -> CdrsResult<T> {
    if let Some(timeout) = timeout {
        tokio::time::timeout(
            timeout,
            connection_manager.connection(None, None, broadcast_rpc_address),
        )
        .await
        .map_err(|_| {
            Error::General(format!(
                "Timeout waiting for connection to: {}",
                broadcast_rpc_address
            ))
        })
        .and_then(|result| result)
    } else {
        connection_manager
            .connection(None, None, broadcast_rpc_address)
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
            local_size: get_physical() * 2,
            remote_size: 2,
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

/// Factory for node connection pools.
pub struct ConnectionPoolFactory<T: CdrsTransport + 'static, CM: ConnectionManager<T> + 'static> {
    config: ConnectionPoolConfig,
    version: Version,
    connection_manager: Arc<CM>,
    keyspace_receiver: Receiver<Option<String>>,
    _transport: PhantomData<T>,
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> ConnectionPoolFactory<T, CM> {
    pub fn new(
        config: ConnectionPoolConfig,
        version: Version,
        connection_manager: CM,
        keyspace_receiver: Receiver<Option<String>>,
    ) -> Self {
        ConnectionPoolFactory {
            config,
            version,
            connection_manager: Arc::new(connection_manager),
            keyspace_receiver,
            _transport: Default::default(),
        }
    }

    #[inline]
    pub fn connection_manager(&self) -> &CM {
        self.connection_manager.as_ref()
    }

    pub async fn create(
        &self,
        node_distance: NodeDistance,
        broadcast_rpc_address: SocketAddr,
    ) -> CdrsResult<Arc<ConnectionPool<T, CM>>> {
        let pool = Arc::new(
            ConnectionPool::new(
                self.connection_manager.clone(),
                broadcast_rpc_address,
                node_distance,
                self.config,
            )
            .await?,
        );

        // watch for keyspace changes
        let mut keyspace_receiver = self.keyspace_receiver.clone();
        let pool_clone = pool.clone();
        let version = self.version;

        tokio::spawn(async move {
            while let Ok(()) = keyspace_receiver.changed().await {
                let keyspace = keyspace_receiver.borrow().clone();
                if let Some(keyspace) = keyspace {
                    let use_frame = Arc::new(Frame::new_req_query(
                        format!("USE {}", quote(&keyspace)),
                        Default::default(),
                        None,
                        false,
                        None,
                        None,
                        None,
                        None,
                        Default::default(),
                        false,
                        version,
                    ));

                    join_all(pool_clone.pool.iter()
                        .map(|connection| connection.load().clone())
                        .filter(|connection| !connection.is_broken())
                        .map(|connection| {
                            let use_frame = use_frame.clone();
                            async move {
                                if let Err(error) = connection.write_frame(use_frame.as_ref()).await {
                                    error!(%error, %broadcast_rpc_address, "Error settings keyspace for connection!");
                                }
                            }
                        })).await;
                }
            }
        });

        Ok(pool)
    }
}

/// Node connection pool.
pub struct ConnectionPool<T: CdrsTransport, CM: ConnectionManager<T>> {
    connection_manager: Arc<CM>,
    broadcast_rpc_address: SocketAddr,
    config: ConnectionPoolConfig,
    pool: Vec<ArcSwap<T>>,
    current_index: AtomicUsize,
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> ConnectionPool<T, CM> {
    async fn new(
        connection_manager: Arc<CM>,
        broadcast_rpc_address: SocketAddr,
        node_distance: NodeDistance,
        config: ConnectionPoolConfig,
    ) -> CdrsResult<Self> {
        let size = if node_distance == NodeDistance::Local {
            config.local_size
        } else {
            config.remote_size
        };

        // initialize the pool
        let pool = try_join_all((0..size).into_iter().map(|_| {
            new_connection(
                connection_manager.as_ref(),
                broadcast_rpc_address,
                config.connect_timeout,
            )
        }))
        .await?
        .into_iter()
        .map(ArcSwap::from_pointee)
        .collect();

        Ok(ConnectionPool {
            connection_manager,
            broadcast_rpc_address,
            config,
            pool,
            current_index: AtomicUsize::new(0),
        })
    }

    #[inline]
    pub async fn connection(&self) -> CdrsResult<Arc<T>> {
        let index = self.current_index.fetch_add(1, Ordering::Relaxed) % self.pool.len();
        let slot = &self.pool[index];

        let connection = slot.load().clone();
        if !connection.is_broken() {
            return Ok(connection);
        }

        debug!("Establishing new connection...");

        let new_connection = Arc::new(
            new_connection(
                self.connection_manager.as_ref(),
                self.broadcast_rpc_address,
                self.config.connect_timeout,
            )
            .await?,
        );

        // another thread might have already updated this slot, so try once and use current one
        let previous = slot.compare_and_swap(&connection, new_connection.clone());

        Ok(if previous.as_raw() == (&connection).as_raw() {
            new_connection
        } else {
            previous.clone()
        })
    }
}
