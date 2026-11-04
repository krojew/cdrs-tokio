use cassandra_protocol::frame::{Envelope, Version};
use cassandra_protocol::query::utils::quote;
use derive_more::Display;
use futures::future::join_all;
use itertools::Itertools;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::sync::watch::Receiver;
use tokio::sync::{mpsc, watch, RwLock};
use tokio::time::{interval_at, sleep, timeout, Instant, Interval, MissedTickBehavior};
use tracing::*;

use crate::cluster::topology::{NodeDistance, NodeShared, NodeState};
use crate::cluster::ConnectionManager;
use crate::error::{Error, Result as CdrsResult};
use crate::retry::{ReconnectionPolicy, ReconnectionSchedule};
use crate::transport::CdrsTransport;

pub(crate) const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
pub(crate) const DEFAULT_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Copy, Clone, PartialEq, Eq, Display)]
enum ReconnectionState {
    NotRunning,
    InProgress,
    Disabled,
}

struct ReconnectionTracker {
    state: ReconnectionState,
    // set when a connection breaks while a reconnection is in progress
    repeat: bool,
}

async fn new_connection<T: CdrsTransport, CM: ConnectionManager<T>>(
    connection_manager: &CM,
    broadcast_rpc_address: SocketAddr,
    timeout: Option<Duration>,
    event_handler: Option<mpsc::Sender<Envelope>>,
    error_handler: Option<mpsc::Sender<Error>>,
) -> CdrsResult<T> {
    let connection =
        connection_manager.connection(event_handler, error_handler, broadcast_rpc_address);

    if let Some(timeout) = timeout {
        tokio::time::timeout(timeout, connection)
            .await
            .map_err(|_| {
                Error::Timeout(format!(
                    "Timeout waiting for connection to: {broadcast_rpc_address}"
                ))
            })
            .and_then(|result| result)
    } else {
        connection.await
    }
}

/// Configuration for node connection pools. By default, the pool size depends on the number of
/// cpu for local nodes and a fixed value for remote, and there is no timeout. If the distance to a
/// given node is unknown, it is treated as remote. See [ConnectionPoolConfigBuilder].
#[derive(Clone, Copy, Debug)]
pub struct ConnectionPoolConfig {
    local_size: usize,
    remote_size: usize,
    connect_timeout: Option<Duration>,
    heartbeat_interval: Duration,
    heartbeat_timeout: Duration,
}

impl ConnectionPoolConfig {
    #[inline]
    pub(crate) fn heartbeat_interval(&self) -> Duration {
        self.heartbeat_interval
    }

    #[inline]
    pub(crate) fn heartbeat_timeout(&self) -> Duration {
        self.heartbeat_timeout
    }
}

impl Default for ConnectionPoolConfig {
    fn default() -> Self {
        ConnectionPoolConfig {
            local_size: 1,
            remote_size: 1,
            connect_timeout: None,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            heartbeat_timeout: DEFAULT_HEARTBEAT_TIMEOUT,
        }
    }
}

/// A builder for [ConnectionPoolConfig].
#[derive(Default, Clone, Debug)]
pub struct ConnectionPoolConfigBuilder {
    config: ConnectionPoolConfig,
}

impl ConnectionPoolConfigBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    /// Sets local node pool size.
    ///
    /// # Panics
    ///
    /// Panics if `local_size` is 0.
    #[must_use]
    pub fn with_local_size(mut self, local_size: usize) -> Self {
        assert!(
            local_size > 0,
            "Local connection pool size must be at least 1!"
        );

        self.config.local_size = local_size;
        self
    }

    /// Sets remote node pool size.
    ///
    /// # Panics
    ///
    /// Panics if `remote_size` is 0.
    #[must_use]
    pub fn with_remote_size(mut self, remote_size: usize) -> Self {
        assert!(
            remote_size > 0,
            "Remote connection pool size must be at least 1!"
        );

        self.config.remote_size = remote_size;
        self
    }

    /// Sets new connection timeout.
    #[must_use]
    pub fn with_connect_timeout(mut self, connect_timeout: Option<Duration>) -> Self {
        self.config.connect_timeout = connect_timeout;
        self
    }

    /// Sets new heartbeat interval.
    ///
    /// # Panics
    ///
    /// Panics if `heartbeat_interval` is zero.
    #[must_use]
    pub fn with_heartbeat_interval(mut self, heartbeat_interval: Duration) -> Self {
        assert!(
            !heartbeat_interval.is_zero(),
            "Heartbeat interval must be greater than zero!"
        );

        self.config.heartbeat_interval = heartbeat_interval;
        self
    }

    /// Sets how long to wait for a heartbeat response before considering the connection broken.
    ///
    /// # Panics
    ///
    /// Panics if `heartbeat_timeout` is zero.
    #[must_use]
    pub fn with_heartbeat_timeout(mut self, heartbeat_timeout: Duration) -> Self {
        assert!(
            !heartbeat_timeout.is_zero(),
            "Heartbeat timeout must be greater than zero!"
        );

        self.config.heartbeat_timeout = heartbeat_timeout;
        self
    }

    /// Build the resulting config.
    #[must_use]
    pub fn build(self) -> ConnectionPoolConfig {
        self.config
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
    pub(crate) fn config(&self) -> &ConnectionPoolConfig {
        &self.config
    }

    /// Establishes a connection outside any pool, applying the configured connect timeout.
    pub(crate) async fn new_connection(
        &self,
        event_handler: Option<mpsc::Sender<Envelope>>,
        error_handler: Option<mpsc::Sender<Error>>,
        broadcast_rpc_address: SocketAddr,
    ) -> CdrsResult<T> {
        new_connection(
            self.connection_manager.as_ref(),
            broadcast_rpc_address,
            self.config.connect_timeout,
            event_handler,
            error_handler,
        )
        .await
    }

    pub(crate) async fn create(
        &self,
        node_distance: NodeDistance,
        broadcast_rpc_address: SocketAddr,
        node: Weak<NodeShared<T, CM>>,
    ) -> CdrsResult<Arc<ConnectionPool<T, CM>>> {
        let (error_sender, error_receiver) =
            mpsc::channel(if node_distance == NodeDistance::Local {
                self.config.local_size
            } else {
                self.config.remote_size
            });

        // Clone the keyspace receiver BEFORE opening any connections. The
        // receiver remembers the watch's current value at clone time as
        // "seen". If we cloned it after pool initialisation, an update that
        // landed in between would be visible at clone time and changed()
        // would never fire for it - leaving the freshly-opened connections
        // stuck on the previous keyspace.
        let mut keyspace_receiver = self.keyspace_receiver.clone();

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

        let weak_pool = Arc::downgrade(&pool);

        Self::monitor_connections(
            error_receiver,
            weak_pool.clone(),
            node.clone(),
            self.reconnection_policy.clone(),
        );

        Self::start_heartbeat(
            weak_pool.clone(),
            node,
            self.config.heartbeat_interval,
            self.version,
            pool.shutdown.subscribe(),
            pool.wake.subscribe(),
        );

        let weak_pool_for_keyspace = weak_pool.clone();
        let version = self.version;
        let mut shutdown = pool.shutdown.subscribe();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    changed = keyspace_receiver.changed() => {
                        if changed.is_err() {
                            break;
                        }
                    }
                    _ = shutdown.changed() => {
                        debug!("Pool dropped, exiting keyspace watcher task.");
                        break;
                    }
                }

                let keyspace = keyspace_receiver.borrow().clone();

                // Try to upgrade the weak reference to a strong Arc
                let pool = match weak_pool_for_keyspace.upgrade() {
                    Some(pool) => pool,
                    None => {
                        debug!("Pool dropped, exiting keyspace watcher task.");
                        break;
                    }
                };

                if let Some(keyspace) = keyspace {
                    let use_envelope = Arc::new(Envelope::new_req_query(
                        format!("USE {}", quote(&keyspace)),
                        Default::default(),
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        Default::default(),
                        version,
                    ));

                    // responses are awaited without holding the lock
                    let connections: Vec<_> = pool
                        .pool
                        .read()
                        .await
                        .iter()
                        .filter(|connection| !connection.is_broken())
                        .map(|connection| connection.transport.clone())
                        .collect();

                    join_all(connections.iter()
                        .map(|connection| {
                            let use_envelope = use_envelope.clone();
                            async move {
                                if let Err(error) = connection.write_internal_envelope(use_envelope.as_ref(), false).await {
                                    error!(%error, ?broadcast_rpc_address, "Error settings keyspace for connection!");
                                }
                            }
                        })).await;
                }
            }
        });

        Ok(pool)
    }

    fn start_heartbeat(
        pool: Weak<ConnectionPool<T, CM>>,
        node: Weak<NodeShared<T, CM>>,
        heartbeat_interval: Duration,
        version: Version,
        mut shutdown: watch::Receiver<()>,
        mut wake: watch::Receiver<()>,
    ) {
        let mut interval = create_heartbeat_interval(Instant::now(), heartbeat_interval);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = interval.tick() => {}
                    // closes together with shutdown
                    Ok(()) = wake.changed() => {}
                    _ = shutdown.changed() => {
                        debug!("Stopping heartbeat due to pool being gone.");
                        break;
                    }
                }

                if let Some(node) = node.upgrade() {
                    let broadcast_rpc_address = node.broadcast_rpc_address();
                    let state = node.state();
                    if state == NodeState::ForcedDown {
                        debug!(
                            ?broadcast_rpc_address,
                            "Stopping heartbeat due to node being forced down."
                        );
                        break;
                    }

                    if state == NodeState::Up {
                        if let Some(pool) = pool.upgrade() {
                            pool.heartbeat(&Envelope::new_req_options(version)).await;
                        } else {
                            debug!(
                                ?broadcast_rpc_address,
                                "Stopping heartbeat due to pool being gone."
                            );
                            break;
                        }
                    }
                } else {
                    break;
                }
            }

            debug!("Stopped heartbeat.");
        });
    }

    fn monitor_connections(
        mut receiver: mpsc::Receiver<Error>,
        pool: Weak<ConnectionPool<T, CM>>,
        node: Weak<NodeShared<T, CM>>,
        reconnection_policy: Arc<dyn ReconnectionPolicy + Send + Sync>,
    ) {
        tokio::spawn(async move {
            let reconnection_state = Arc::new(Mutex::new(ReconnectionTracker {
                state: ReconnectionState::NotRunning,
                repeat: false,
            }));
            while receiver.recv().await.is_some() {
                if let Some(node) = node.upgrade() {
                    let broadcast_rpc_address = node.broadcast_rpc_address();

                    if node.state() == NodeState::ForcedDown {
                        debug!(
                            ?broadcast_rpc_address,
                            "Not starting reconnection for a forced down node."
                        );
                        break;
                    }

                    // Subscribed before the node can be marked down, so a reconnection sees a wake
                    // which follows right after.
                    let mut wake = if let Some(pool) = pool.upgrade() {
                        let wake = pool.wake.subscribe();

                        // check if the node is down (no active connections)
                        if Self::are_all_connections_down(pool.deref()).await {
                            debug!(
                                ?broadcast_rpc_address,
                                "All connections broken - marking node as down."
                            );
                            node.mark_down();
                        }

                        wake
                    } else {
                        // the pool is gone - we're shutting down
                        break;
                    };

                    // when one connection goes down, all of them will most likely go down, so we need
                    // to protect against many reconnection attempts
                    {
                        let mut tracker = reconnection_state.lock().unwrap();
                        match tracker.state {
                            ReconnectionState::Disabled => break,
                            ReconnectionState::InProgress => {
                                // the connection might have broken after the running
                                // reconnection checked it
                                tracker.repeat = true;
                                continue;
                            }
                            ReconnectionState::NotRunning => {
                                tracker.state = ReconnectionState::InProgress;
                            }
                        }
                    }

                    warn!(
                        ?broadcast_rpc_address,
                        "Connection down. Starting reconnection."
                    );

                    let reconnection_policy = reconnection_policy.clone();
                    let reconnecting = reconnection_state.clone();
                    let pool = pool.clone();
                    let node = Arc::downgrade(&node);

                    tokio::spawn(async move {
                        let new_state = loop {
                            let new_state = Self::run_reconnection_loop(
                                reconnection_policy.new_node_schedule(),
                                pool.clone(),
                                &mut wake,
                            )
                            .await;

                            let repeat = {
                                let mut tracker = reconnecting.lock().unwrap();
                                if new_state == ReconnectionState::NotRunning && tracker.repeat {
                                    tracker.repeat = false;
                                    true
                                } else {
                                    tracker.state = new_state;
                                    tracker.repeat = false;
                                    false
                                }
                            };

                            if !repeat {
                                break new_state;
                            }

                            debug!(
                                ?broadcast_rpc_address,
                                "Connections broke during reconnection. Repeating reconnection."
                            );
                        };

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
                            } else {
                                debug!(
                                    ?broadcast_rpc_address,
                                    "Node is discarded during reconnection."
                                );
                            }
                        } else if let Some(pool) = pool.upgrade() {
                            if pool.is_any_connection_up().await {
                                if let Some(node) = node.upgrade() {
                                    debug!(
                                        ?broadcast_rpc_address,
                                        "Marking node as up - some connections are established."
                                    );
                                    node.mark_up();
                                }
                            }
                        } else if let Some(node) = node.upgrade() {
                            debug!(
                                ?broadcast_rpc_address,
                                "Pool gone while in reconnection loop."
                            );
                            node.force_down();
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

    async fn are_all_connections_down(pool: &ConnectionPool<T, CM>) -> bool {
        let connections = pool.pool.read().await;
        for connection in connections.deref() {
            if !connection.is_broken() {
                return false;
            }
        }

        true
    }

    async fn run_reconnection_loop(
        mut reconnection_schedule: Box<dyn ReconnectionSchedule + Send + Sync>,
        pool: Weak<ConnectionPool<T, CM>>,
        wake: &mut watch::Receiver<()>,
    ) -> ReconnectionState {
        while let Some(delay) = reconnection_schedule.next_delay() {
            tokio::select! {
                _ = sleep(delay) => {}
                woken = wake.changed() => {
                    if woken.is_err() {
                        // the pool is gone
                        return ReconnectionState::Disabled;
                    }

                    debug!("Reconnecting right away after being woken.");
                }
            }

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

struct PooledConnection<T> {
    transport: Arc<T>,
    // set when the connection stops answering heartbeats, which the transport cannot detect
    unresponsive: AtomicBool,
}

impl<T: CdrsTransport> PooledConnection<T> {
    fn new(transport: T) -> Self {
        PooledConnection {
            transport: Arc::new(transport),
            unresponsive: AtomicBool::new(false),
        }
    }

    #[inline]
    fn is_broken(&self) -> bool {
        self.unresponsive.load(Ordering::Relaxed) || self.transport.is_broken()
    }
}

pub(crate) struct ConnectionPool<T: CdrsTransport, CM: ConnectionManager<T>> {
    connection_manager: Weak<CM>,
    broadcast_rpc_address: SocketAddr,
    config: ConnectionPoolConfig,
    pool: RwLock<Vec<Arc<PooledConnection<T>>>>,
    desired_size: usize,
    current_index: AtomicUsize,
    error_sender: mpsc::Sender<Error>,
    // Never sends. The heartbeat and keyspace watcher exit as soon as it is dropped with the pool.
    // The monitor exits once the error channel closes, after every transport holding its sender
    // is dropped too.
    shutdown: watch::Sender<()>,
    // signaled by wake(); the reconnection task exits as soon as it is dropped with the pool
    wake: watch::Sender<()>,
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
                None,
                Some(error_sender.clone()),
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
        .map_ok(|connection| Arc::new(PooledConnection::new(connection)))
        .try_collect()?;

        if pool.len() != desired_size {
            // some connections have failed, but can be brought back up, so trigger reconnection
            match error_sender.try_send(Error::General(
                "Not all pool connections could be established!".to_string(),
            )) {
                Ok(_) => debug!("Error handler notified!"),
                Err(e) => warn!("Error handler failed to notify: {e}"),
            }
        }

        Ok(ConnectionPool {
            connection_manager: Arc::downgrade(connection_manager),
            broadcast_rpc_address,
            config,
            pool: RwLock::new(pool),
            desired_size,
            current_index: AtomicUsize::new(0),
            error_sender,
            shutdown: watch::channel(()).0,
            wake: watch::channel(()).0,
        })
    }

    /// Makes a reconnection in progress attempt right away instead of waiting out its backoff,
    /// and checks connections with a heartbeat, since heartbeats skip nodes which are not up.
    /// Wakes coalesce, so repeating it is harmless.
    pub(crate) fn wake(&self) {
        self.wake.send_replace(());
    }

    pub(crate) async fn connection(&self) -> CdrsResult<Arc<T>> {
        fn create_no_connections_error(broadcast_rpc_address: SocketAddr) -> Error {
            warn!(%broadcast_rpc_address, "All connections down to node.");
            Error::General(format!(
                "No active connections to: {}",
                broadcast_rpc_address
            ))
        }

        let pool = self.pool.read().await;
        let pool_len = pool.len();
        if pool_len == 0 {
            return Err(create_no_connections_error(self.broadcast_rpc_address));
        }

        let mut index = self.current_index.fetch_add(1, Ordering::Relaxed) % pool_len;
        let first_index = index;

        loop {
            let connection = &pool[index];
            if !connection.is_broken() {
                return Ok(connection.transport.clone());
            }

            index = (index + 1) % pool_len;

            if index == first_index {
                // we've checked the whole pool and everything's down
                return Err(create_no_connections_error(self.broadcast_rpc_address));
            }
        }
    }

    async fn heartbeat(&self, envelope: &Envelope) {
        // Responses are awaited without holding the lock, since a writer waiting for it would
        // block all readers in the meantime.
        let connections: Vec<_> = self
            .pool
            .read()
            .await
            .iter()
            .filter(|connection| !connection.is_broken())
            .cloned()
            .collect();

        join_all(connections.iter().map(|connection| async move {
            match timeout(
                self.config.heartbeat_timeout,
                connection.transport.write_envelope(envelope, false),
            )
            .await
            {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => {
                    warn!(broadcast_rpc_address = ?self.broadcast_rpc_address, %error, "Error waiting for heartbeat response - the connection will probably go down.");
                }
                Err(_) => {
                    warn!(broadcast_rpc_address = ?self.broadcast_rpc_address, "Timeout waiting for heartbeat response - marking the connection as broken.");

                    connection.unresponsive.store(true, Ordering::Relaxed);
                    // requests in flight would otherwise wait until the OS drops the connection
                    connection.transport.mark_broken();

                    match self.error_sender.try_send(Error::Timeout(format!(
                        "Timeout waiting for heartbeat response from: {}",
                        self.broadcast_rpc_address
                    ))) {
                        Ok(_) => debug!("Error handler notified!"),
                        Err(e) => warn!("Error handler failed to notify: {e}"),
                    }
                }
            }
        }))
        .await;
    }

    pub(crate) async fn is_any_connection_up(&self) -> bool {
        let connections = self.pool.read().await;
        for connection in connections.deref() {
            if !connection.is_broken() {
                return true;
            }
        }

        false
    }

    async fn reconnect_broken(&self) -> CdrsResult<bool> {
        let connection_manager = match self.connection_manager.upgrade() {
            Some(connection_manager) => connection_manager,
            // connection manager is gone - we're probably dropping the session
            None => return Ok(false),
        };

        let missing = {
            let pool = self.pool.read().await;
            pool.iter()
                .filter(|connection| connection.is_broken())
                .count()
                + self.desired_size.saturating_sub(pool.len())
        };

        // connections are established without holding the lock, so the pool stays usable
        for _ in 0..missing {
            let connection = Arc::new(PooledConnection::new(
                new_connection(
                    connection_manager.as_ref(),
                    self.broadcast_rpc_address,
                    self.config.connect_timeout,
                    None,
                    Some(self.error_sender.clone()),
                )
                .await?,
            ));

            let mut pool = self.pool.write().await;
            if let Some(broken) = pool.iter_mut().find(|connection| connection.is_broken()) {
                *broken = connection;
            } else if pool.len() < self.desired_size {
                pool.push(connection);
            }
        }

        // at this point either all connections are up, or some might have died in the meantime,
        // which will trigger a new reconnection
        Ok(true)
    }
}

/// Builds the [`Interval`] used by the heartbeat loop. The first tick fires
/// `period` after `now`, then every `period`. We explicitly set the missed
/// tick behavior to `Skip` so that if a heartbeat round takes longer than
/// the configured period (e.g. a slow node, a stalled connection), the
/// runtime does not pile up a burst of catch-up ticks the moment we return
/// to `tick().await`. Without this, tokio's default behavior would keep
/// firing immediately until it had "caught up", which on a healthy cluster
/// just means a thundering herd of OPTIONS messages.
pub(crate) fn create_heartbeat_interval(now: Instant, period: Duration) -> Interval {
    let mut interval = interval_at(now + period, period);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    interval
}

#[cfg(test)]
mod heartbeat_interval_tests {
    use super::*;

    #[tokio::test]
    async fn create_heartbeat_interval_skips_missed_ticks() {
        let interval = create_heartbeat_interval(Instant::now(), Duration::from_secs(30));
        assert_eq!(interval.missed_tick_behavior(), MissedTickBehavior::Skip);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::connection_manager::MockConnectionManager;
    use crate::cluster::topology::Node;
    use crate::cluster::{KeyspaceHolder, NodeInfo};
    use crate::load_balancing::node_distance_evaluator::MockNodeDistanceEvaluator;
    use crate::retry::{ConstantReconnectionPolicy, MockReconnectionPolicy};
    use crate::transport::{MockCdrsTransport, TransportTcp};
    use cassandra_protocol::compression::Compression;
    use cassandra_protocol::frame::frame_decoder::LegacyFrameDecoder;
    use cassandra_protocol::frame::frame_encoder::LegacyFrameEncoder;
    use futures::future::pending;
    use futures::FutureExt;
    use std::net::{IpAddr, Ipv4Addr};
    use tokio::io::{duplex, AsyncReadExt};
    use tokio::sync::{watch, Notify};
    use tokio::time::timeout;
    use uuid::Uuid;

    type TestConnectionManager = MockConnectionManager<MockCdrsTransport>;

    // upper bound for awaiting outcomes which are expected to happen right away
    const TEST_GUARD_TIMEOUT: Duration = Duration::from_secs(5);

    fn test_address() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9042)
    }

    fn create_factory<T: CdrsTransport + 'static>(
        config: ConnectionPoolConfig,
        connection_manager: MockConnectionManager<T>,
    ) -> Arc<ConnectionPoolFactory<T, MockConnectionManager<T>>> {
        let (_, keyspace_receiver) = watch::channel(None);
        Arc::new(ConnectionPoolFactory::new(
            config,
            Version::V4,
            connection_manager,
            keyspace_receiver,
            Arc::new(MockReconnectionPolicy::new()),
        ))
    }

    fn create_node<T: CdrsTransport + 'static>(
        factory: Arc<ConnectionPoolFactory<T, MockConnectionManager<T>>>,
    ) -> Arc<Node<T, MockConnectionManager<T>>> {
        Arc::new(Node::new_with_state(
            factory,
            test_address(),
            None,
            None,
            Some(NodeDistance::Local),
            NodeState::Up,
            Default::default(),
            "".into(),
            "".into(),
        ))
    }

    #[test]
    #[should_panic(expected = "Local connection pool size must be at least 1")]
    fn config_rejects_empty_local_pool() {
        let _ = ConnectionPoolConfigBuilder::new().with_local_size(0);
    }

    #[test]
    #[should_panic(expected = "Remote connection pool size must be at least 1")]
    fn config_rejects_empty_remote_pool() {
        let _ = ConnectionPoolConfigBuilder::new().with_remote_size(0);
    }

    #[test]
    #[should_panic(expected = "Heartbeat timeout must be greater than zero")]
    fn config_rejects_zero_heartbeat_timeout() {
        let _ = ConnectionPoolConfigBuilder::new().with_heartbeat_timeout(Duration::ZERO);
    }

    #[test]
    #[should_panic(expected = "Heartbeat interval must be greater than zero")]
    fn config_rejects_zero_heartbeat_interval() {
        let _ = ConnectionPoolConfigBuilder::new().with_heartbeat_interval(Duration::ZERO);
    }

    fn transport_with_broken_flag(broken: Arc<AtomicBool>) -> MockCdrsTransport {
        let mut transport = MockCdrsTransport::new();
        transport
            .expect_is_broken()
            .returning(move || broken.load(Ordering::Relaxed));
        transport
    }

    async fn create_pool<T: CdrsTransport + 'static>(
        connection_manager: MockConnectionManager<T>,
        config: ConnectionPoolConfig,
    ) -> (
        Arc<ConnectionPool<T, MockConnectionManager<T>>>,
        Arc<MockConnectionManager<T>>,
        mpsc::Receiver<Error>,
    ) {
        let connection_manager = Arc::new(connection_manager);
        let (error_sender, error_receiver) = mpsc::channel(config.local_size);

        let pool = ConnectionPool::new(
            &connection_manager,
            test_address(),
            NodeDistance::Local,
            config,
            error_sender,
        )
        .await
        .unwrap();

        (Arc::new(pool), connection_manager, error_receiver)
    }

    #[tokio::test]
    async fn pool_stays_usable_while_reconnecting() {
        let broken = Arc::new(AtomicBool::new(false));
        let (entered_sender, mut entered_receiver) = mpsc::channel(1);
        let release = Arc::new(Notify::new());
        let calls = AtomicUsize::new(0);

        let mut connection_manager = TestConnectionManager::new();
        connection_manager.expect_connection().returning({
            let broken = broken.clone();
            let release = release.clone();
            move |_, _, _| match calls.fetch_add(1, Ordering::Relaxed) {
                0 => {
                    let transport = transport_with_broken_flag(broken.clone());
                    Box::pin(async move { Ok(transport) })
                }
                1 => {
                    let transport = transport_with_broken_flag(Default::default());
                    Box::pin(async move { Ok(transport) })
                }
                _ => {
                    let _ = entered_sender.try_send(());
                    let release = release.clone();
                    let transport = transport_with_broken_flag(Default::default());
                    Box::pin(async move {
                        release.notified().await;
                        Ok(transport)
                    })
                }
            }
        });

        let (pool, _connection_manager, _error_receiver) = create_pool(
            connection_manager,
            ConnectionPoolConfigBuilder::new()
                .with_local_size(2)
                .build(),
        )
        .await;
        broken.store(true, Ordering::Relaxed);

        let reconnection = tokio::spawn({
            let pool = pool.clone();
            async move { pool.reconnect_broken().await }
        });

        timeout(TEST_GUARD_TIMEOUT, entered_receiver.recv())
            .await
            .expect("reconnection did not start");

        let connection = pool
            .connection()
            .now_or_never()
            .expect("pool is locked while reconnecting");
        assert!(!connection.unwrap().is_broken());

        release.notify_one();
        assert!(reconnection.await.unwrap().unwrap());
        assert!(pool
            .pool
            .read()
            .await
            .iter()
            .all(|connection| !connection.is_broken()));
    }

    // A connection which accepts heartbeats, but never answers them.
    fn unresponsive_connection_manager(
        heartbeat_sender: mpsc::Sender<()>,
    ) -> TestConnectionManager {
        let mut connection_manager = TestConnectionManager::new();
        connection_manager
            .expect_connection()
            .returning(move |_, _, _| {
                let heartbeat_sender = heartbeat_sender.clone();
                let mut transport = transport_with_broken_flag(Default::default());
                transport.expect_write_envelope().returning(move |_, _| {
                    let _ = heartbeat_sender.try_send(());
                    Box::pin(pending())
                });
                transport.expect_mark_broken().return_const(());

                Box::pin(async move { Ok(transport) })
            });

        connection_manager
    }

    #[tokio::test]
    async fn heartbeat_does_not_hold_pool_lock_while_waiting_for_response() {
        let (heartbeat_sender, mut heartbeat_receiver) = mpsc::channel(1);
        let (pool, _connection_manager, _error_receiver) = create_pool(
            unresponsive_connection_manager(heartbeat_sender),
            Default::default(),
        )
        .await;

        let heartbeat = tokio::spawn({
            let pool = pool.clone();
            async move {
                pool.heartbeat(&Envelope::new_req_options(Version::V4))
                    .await
            }
        });

        timeout(TEST_GUARD_TIMEOUT, heartbeat_receiver.recv())
            .await
            .expect("heartbeat was not sent");

        assert!(
            pool.pool.try_write().is_ok(),
            "pool is locked while waiting for heartbeat response"
        );

        heartbeat.abort();
    }

    #[tokio::test]
    async fn unanswered_heartbeat_marks_connection_broken() {
        let (heartbeat_sender, _heartbeat_receiver) = mpsc::channel(1);
        let (pool, _connection_manager, mut error_receiver) = create_pool(
            unresponsive_connection_manager(heartbeat_sender),
            ConnectionPoolConfigBuilder::new()
                .with_heartbeat_timeout(Duration::from_millis(10))
                .build(),
        )
        .await;

        timeout(
            TEST_GUARD_TIMEOUT,
            pool.heartbeat(&Envelope::new_req_options(Version::V4)),
        )
        .await
        .expect("heartbeat response was awaited without a timeout");

        assert!(pool.connection().await.is_err());
        assert!(matches!(error_receiver.try_recv(), Ok(Error::Timeout(_))));
    }

    #[tokio::test]
    async fn heartbeat_timeout_fails_requests_in_flight_and_reconnects() {
        let (server_sender, mut server_receiver) = mpsc::channel(2);

        // connections over in-memory streams, which the server keeps open, but never answers
        let mut connection_manager = MockConnectionManager::<TransportTcp>::new();
        connection_manager
            .expect_connection()
            .returning(move |_, error_handler, addr| {
                let (client_stream, server_stream) = duplex(1 << 16);
                let _ = server_sender.try_send(server_stream);

                let (keyspace_sender, _) = watch::channel(None);
                let transport = TransportTcp::with_stream(
                    client_stream,
                    addr,
                    Arc::new(KeyspaceHolder::new(keyspace_sender)),
                    None,
                    error_handler,
                    Compression::None,
                    Box::<LegacyFrameEncoder>::default(),
                    Box::<LegacyFrameDecoder>::default(),
                    64,
                );

                Box::pin(async move { transport.map_err(Error::from) })
            });

        let (pool, _connection_manager, error_receiver) = create_pool(
            connection_manager,
            ConnectionPoolConfigBuilder::new()
                .with_heartbeat_timeout(Duration::from_millis(10))
                .build(),
        )
        .await;
        let mut server = server_receiver.recv().await.unwrap();

        let node = create_node(create_factory(
            Default::default(),
            MockConnectionManager::<TransportTcp>::new(),
        ));
        ConnectionPoolFactory::<TransportTcp, MockConnectionManager<TransportTcp>>::monitor_connections(
            error_receiver,
            Arc::downgrade(&pool),
            Arc::downgrade(node.shared()),
            Arc::new(ConstantReconnectionPolicy::new(Duration::from_millis(1))),
        );

        let transport = pool.connection().await.unwrap();
        let request = tokio::spawn(async move {
            transport
                .write_envelope(&Envelope::new_req_options(Version::V4), false)
                .await
        });

        // the request is in flight once it reaches the server
        let mut buffer = vec![];
        assert_ne!(server.read_buf(&mut buffer).await.unwrap(), 0);

        timeout(
            TEST_GUARD_TIMEOUT,
            pool.heartbeat(&Envelope::new_req_options(Version::V4)),
        )
        .await
        .expect("heartbeat response was awaited without a timeout");

        let response = timeout(TEST_GUARD_TIMEOUT, request)
            .await
            .expect("request in flight was not failed")
            .unwrap();
        assert!(response.is_err());

        let _reconnected_server = timeout(TEST_GUARD_TIMEOUT, server_receiver.recv())
            .await
            .expect("broken connection was not reconnected");

        // the monitor marks the node down before reconnecting and up once done
        timeout(TEST_GUARD_TIMEOUT, async {
            while node.state() != NodeState::Up {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("reconnection did not finish");

        assert!(!pool.connection().await.unwrap().is_broken());
    }

    #[tokio::test]
    async fn keyspace_watcher_propagates_keyspace_with_internal_requests() {
        let (keyspace_sender, keyspace_receiver) = watch::channel(None);
        let (propagated_sender, mut propagated_receiver) = mpsc::channel(1);

        let mut connection_manager = TestConnectionManager::new();
        connection_manager
            .expect_connection()
            .returning(move |_, _, _| {
                let propagated_sender = propagated_sender.clone();
                let mut transport = MockCdrsTransport::new();
                transport.expect_is_broken().return_const(false);
                transport
                    .expect_write_internal_envelope()
                    .returning(move |_, _| {
                        let _ = propagated_sender.try_send(());
                        Box::pin(async { Ok(Envelope::new_req_options(Version::V4)) })
                    });

                Box::pin(async move { Ok(transport) })
            });

        let factory = ConnectionPoolFactory::new(
            Default::default(),
            Version::V4,
            connection_manager,
            keyspace_receiver,
            Arc::new(MockReconnectionPolicy::new()),
        );
        let _pool = factory
            .create(NodeDistance::Local, test_address(), Weak::new())
            .await
            .unwrap();

        keyspace_sender.send(Some("ks".into())).unwrap();

        timeout(TEST_GUARD_TIMEOUT, propagated_receiver.recv())
            .await
            .expect("keyspace was not propagated with an internal request");
    }

    #[tokio::test]
    async fn keyspace_watcher_does_not_hold_pool_lock_while_waiting_for_response() {
        let (keyspace_sender, keyspace_receiver) = watch::channel(None);
        let (propagated_sender, mut propagated_receiver) = mpsc::channel(1);

        let mut connection_manager = TestConnectionManager::new();
        connection_manager
            .expect_connection()
            .returning(move |_, _, _| {
                let propagated_sender = propagated_sender.clone();
                let mut transport = transport_with_broken_flag(Default::default());
                transport
                    .expect_write_internal_envelope()
                    .returning(move |_, _| {
                        let _ = propagated_sender.try_send(());
                        Box::pin(pending())
                    });

                Box::pin(async move { Ok(transport) })
            });

        let factory = ConnectionPoolFactory::new(
            Default::default(),
            Version::V4,
            connection_manager,
            keyspace_receiver,
            Arc::new(MockReconnectionPolicy::new()),
        );
        let pool = factory
            .create(NodeDistance::Local, test_address(), Weak::new())
            .await
            .unwrap();

        keyspace_sender.send(Some("ks".into())).unwrap();

        timeout(TEST_GUARD_TIMEOUT, propagated_receiver.recv())
            .await
            .expect("keyspace was not propagated");

        assert!(
            pool.pool.try_write().is_ok(),
            "pool is locked while waiting for keyspace propagation"
        );
    }

    #[tokio::test]
    async fn connection_broken_during_reconnection_gets_reconnected() {
        let broken: [Arc<AtomicBool>; 2] = Default::default();
        let (reconnecting_sender, mut reconnecting_receiver) = mpsc::channel(4);
        let release = Arc::new(Notify::new());
        let calls = AtomicUsize::new(0);

        let mut connection_manager = TestConnectionManager::new();
        connection_manager.expect_connection().returning({
            let broken = broken.clone();
            let release = release.clone();
            move |_, _, _| {
                let call = calls.fetch_add(1, Ordering::Relaxed);
                if call < broken.len() {
                    let transport = transport_with_broken_flag(broken[call].clone());
                    return Box::pin(async move { Ok(transport) });
                }

                let _ = reconnecting_sender.try_send(());
                let release = release.clone();
                let transport = transport_with_broken_flag(Default::default());
                Box::pin(async move {
                    release.notified().await;
                    Ok(transport)
                })
            }
        });

        let (pool, _connection_manager, error_receiver) = create_pool(
            connection_manager,
            ConnectionPoolConfigBuilder::new()
                .with_local_size(2)
                .build(),
        )
        .await;
        let node = create_node(create_factory(
            Default::default(),
            TestConnectionManager::new(),
        ));

        ConnectionPoolFactory::<MockCdrsTransport, TestConnectionManager>::monitor_connections(
            error_receiver,
            Arc::downgrade(&pool),
            Arc::downgrade(node.shared()),
            Arc::new(ConstantReconnectionPolicy::new(Duration::from_millis(1))),
        );

        broken[0].store(true, Ordering::Relaxed);
        pool.error_sender
            .try_send(Error::General("first connection broken".into()))
            .unwrap();

        timeout(TEST_GUARD_TIMEOUT, reconnecting_receiver.recv())
            .await
            .expect("reconnection did not start");

        broken[1].store(true, Ordering::Relaxed);
        pool.error_sender
            .try_send(Error::General("second connection broken".into()))
            .unwrap();

        // the monitor marks the node down once it sees the second error
        timeout(TEST_GUARD_TIMEOUT, async {
            while node.state() != NodeState::Down {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("second error was not processed");

        release.notify_one();

        timeout(TEST_GUARD_TIMEOUT, reconnecting_receiver.recv())
            .await
            .expect("connection broken during reconnection was not reconnected");

        release.notify_one();
    }

    #[tokio::test]
    async fn monitor_state_change_is_visible_on_refreshed_node() {
        let broken = Arc::new(AtomicBool::new(false));

        let mut connection_manager = TestConnectionManager::new();
        connection_manager.expect_connection().returning({
            let broken = broken.clone();
            move |_, _, _| {
                let transport = transport_with_broken_flag(broken.clone());
                Box::pin(async move { Ok(transport) })
            }
        });

        let (_, keyspace_receiver) = watch::channel(None);
        let factory = Arc::new(ConnectionPoolFactory::new(
            Default::default(),
            Version::V4,
            connection_manager,
            keyspace_receiver,
            // keeps the node down for the duration of the test
            Arc::new(ConstantReconnectionPolicy::new(Duration::from_secs(3600))),
        ));

        let node = create_node(factory);
        drop(node.persistent_connection().await.unwrap());
        let pool = node.connection_pool().unwrap();

        let refreshed = node.clone_with_node_info(
            NodeInfo::new(
                Uuid::new_v4(),
                node.broadcast_rpc_address(),
                None,
                "".into(),
                Default::default(),
                "".into(),
            ),
            &MockNodeDistanceEvaluator::new(),
        );
        drop(node);

        broken.store(true, Ordering::Relaxed);
        pool.error_sender
            .try_send(Error::General("connection broken".into()))
            .unwrap();

        timeout(TEST_GUARD_TIMEOUT, async {
            while refreshed.state() != NodeState::Down {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("monitor did not mark the refreshed node down");
    }

    fn healthy_connection_manager() -> TestConnectionManager {
        let mut connection_manager = TestConnectionManager::new();
        connection_manager.expect_connection().returning(|_, _, _| {
            let transport = transport_with_broken_flag(Default::default());
            Box::pin(async move { Ok(transport) })
        });

        connection_manager
    }

    #[tokio::test]
    async fn keyspace_watcher_exits_when_pool_is_dropped() {
        let (keyspace_sender, keyspace_receiver) = watch::channel(None);
        let factory = ConnectionPoolFactory::new(
            Default::default(),
            Version::V4,
            healthy_connection_manager(),
            keyspace_receiver,
            Arc::new(MockReconnectionPolicy::new()),
        );
        let receiver_count = keyspace_sender.receiver_count();

        let pool = factory
            .create(NodeDistance::Local, test_address(), Weak::new())
            .await
            .unwrap();
        assert_eq!(keyspace_sender.receiver_count(), receiver_count + 1);

        drop(pool);

        timeout(TEST_GUARD_TIMEOUT, async {
            while keyspace_sender.receiver_count() != receiver_count {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("keyspace watcher did not exit");
    }

    #[tokio::test]
    async fn pool_tasks_release_node_when_pool_is_dropped() {
        let factory = create_factory(Default::default(), healthy_connection_manager());
        let node = create_node(factory.clone());

        let pool = factory
            .create(
                NodeDistance::Local,
                test_address(),
                Arc::downgrade(node.shared()),
            )
            .await
            .unwrap();
        assert_ne!(Arc::weak_count(node.shared()), 0);

        drop(pool);

        // the heartbeat would otherwise hold the node until its next tick
        timeout(TEST_GUARD_TIMEOUT, async {
            while Arc::weak_count(node.shared()) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("pool tasks did not exit");
    }

    #[tokio::test]
    async fn promoted_node_checks_connections_right_away() {
        let (heartbeat_sender, mut heartbeat_receiver) = mpsc::channel(1);
        let node = create_node(create_factory(
            Default::default(),
            unresponsive_connection_manager(heartbeat_sender),
        ));
        drop(node.persistent_connection().await.unwrap());

        // connections may go stale unnoticed while the node is down, since heartbeats skip it
        node.shared().mark_down();
        let _refreshed = node.clone_with_node_info(
            NodeInfo::new(
                Uuid::new_v4(),
                node.broadcast_rpc_address(),
                None,
                "".into(),
                Default::default(),
                "".into(),
            ),
            &MockNodeDistanceEvaluator::new(),
        );

        timeout(TEST_GUARD_TIMEOUT, heartbeat_receiver.recv())
            .await
            .expect("connections were not checked");
    }

    #[tokio::test]
    async fn reconnection_task_exits_when_pool_is_dropped() {
        let broken = Arc::new(AtomicBool::new(false));

        let mut connection_manager = TestConnectionManager::new();
        connection_manager.expect_connection().returning({
            let broken = broken.clone();
            move |_, _, _| {
                let transport = transport_with_broken_flag(broken.clone());
                Box::pin(async move { Ok(transport) })
            }
        });

        let (_, keyspace_receiver) = watch::channel(None);
        let factory = Arc::new(ConnectionPoolFactory::new(
            Default::default(),
            Version::V4,
            connection_manager,
            keyspace_receiver,
            Arc::new(ConstantReconnectionPolicy::new(Duration::from_secs(3600))),
        ));
        let node = create_node(factory.clone());

        let pool = factory
            .create(
                NodeDistance::Local,
                test_address(),
                Arc::downgrade(node.shared()),
            )
            .await
            .unwrap();

        broken.store(true, Ordering::Relaxed);
        pool.error_sender
            .try_send(Error::General("connection broken".into()))
            .unwrap();

        timeout(TEST_GUARD_TIMEOUT, async {
            while node.state() != NodeState::Down {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("reconnection did not start");

        drop(pool);

        // the reconnection would otherwise hold the node until its next attempt
        timeout(TEST_GUARD_TIMEOUT, async {
            while Arc::weak_count(node.shared()) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("reconnection task did not exit");
    }

    #[tokio::test]
    async fn node_new_connection_applies_connect_timeout() {
        let mut connection_manager = TestConnectionManager::new();
        connection_manager
            .expect_connection()
            .returning(|_, _, _| Box::pin(pending()));

        let config = ConnectionPoolConfigBuilder::new()
            .with_connect_timeout(Some(Duration::from_millis(10)))
            .build();
        let node = create_node(create_factory(config, connection_manager));

        let result = timeout(TEST_GUARD_TIMEOUT, node.new_connection(None, None))
            .await
            .expect("connect timeout was not applied");

        assert!(matches!(result, Err(Error::Timeout(_))));
    }
}
