use async_trait::async_trait;
use std::iter::Iterator;
use std::marker::PhantomData;
#[cfg(feature = "rust-tls")]
use std::net;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::mpsc::channel as std_channel;
use std::sync::Arc;
use tokio::sync::mpsc::channel;
use tokio::sync::Mutex;

use crate::authenticators::SaslAuthenticatorProvider;
use crate::cluster::connection_manager::ConnectionManager;
#[cfg(feature = "rust-tls")]
use crate::cluster::rustls_connection_manager::RustlsConnectionManager;
use crate::cluster::tcp_connection_manager::TcpConnectionManager;
#[cfg(feature = "rust-tls")]
use crate::cluster::ClusterRustlsConfig;
#[cfg(feature = "rust-tls")]
use crate::cluster::NodeRustlsConfigBuilder;
#[cfg(feature = "unstable-dynamic-cluster")]
use crate::cluster::NodeTcpConfig;
use crate::cluster::{
    CdrsSession, ClusterTcpConfig, GenericClusterConfig, GetConnection, GetRetryPolicy,
    KeyspaceHolder,
};
use crate::cluster::{NodeTcpConfigBuilder, SessionPager};
use crate::compression::Compression;
use crate::error;
use crate::events::{new_listener, EventStream, EventStreamNonBlocking, Listener};
use crate::frame::events::{ServerEvent, SimpleServerEvent, StatusChange, StatusChangeType};
use crate::frame::Frame;
use crate::load_balancing::LoadBalancingStrategy;
use crate::query::{BatchExecutor, ExecExecutor, PrepareExecutor, QueryExecutor};
use crate::retry::{NeverReconnectionPolicy, ReconnectionPolicy, RetryPolicy};
#[cfg(feature = "rust-tls")]
use crate::transport::TransportRustls;
use crate::transport::{CdrsTransport, TransportTcp};

static NEVER_RECONNECTION_POLICY: NeverReconnectionPolicy = NeverReconnectionPolicy;

/// CDRS session that holds a pool of connections to nodes.
pub struct Session<
    T: CdrsTransport + Send + Sync + 'static,
    CM: ConnectionManager<T>,
    LB: LoadBalancingStrategy<CM> + Send + Sync,
> {
    load_balancing: Mutex<LB>,
    event_stream: Option<Mutex<EventStreamNonBlocking>>,
    compression: Compression,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,
    reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
    _transport: PhantomData<T>,
    _connection_manager: PhantomData<CM>,
}

impl<
        'a,
        T: CdrsTransport + Send + Sync + 'static,
        CM: ConnectionManager<T>,
        LB: LoadBalancingStrategy<CM> + Send + Sync,
    > Session<T, CM, LB>
{
    /// Basing on current session returns new `SessionPager` that can be used
    /// for performing paged queries.
    pub fn paged(&'a self, page_size: i32) -> SessionPager<'a, Session<T, CM, LB>, T>
    where
        Session<T, CM, LB>: CdrsSession<T>,
    {
        SessionPager::new(self, page_size)
    }
}

#[async_trait]
impl<
        T: CdrsTransport + Send + Sync + 'static,
        CM: ConnectionManager<T> + Send + Sync,
        LB: LoadBalancingStrategy<CM> + Send + Sync,
    > GetConnection<T> for Session<T, CM, LB>
{
    async fn load_balanced_connection(&self) -> Option<error::Result<Arc<T>>> {
        if cfg!(feature = "unstable-dynamic-cluster") {
            if let Some(event_stream_mx) = &self.event_stream {
                if let Ok(event_stream) = &mut event_stream_mx.try_lock() {
                    loop {
                        let next_event = event_stream.next();

                        match next_event {
                            None => break,
                            Some(ServerEvent::StatusChange(StatusChange {
                                addr,
                                change_type: StatusChangeType::Down,
                            })) => {
                                self.load_balancing
                                    .lock()
                                    .await
                                    .remove_node(|pool| pool.addr() == addr.addr);
                            }
                            Some(_) => continue,
                        }
                    }
                }
            }
        }

        // when using a load balancer with > 1 node, don't use reconnection policy for a given node,
        // but jump to the next one

        let connection_manager = {
            let load_balancing = self.load_balancing.lock().await;
            if load_balancing.size() < 2 {
                load_balancing.next()
            } else {
                None
            }
        };

        if let Some(connection_manager) = connection_manager {
            let connection = connection_manager
                .connection(self.reconnection_policy.deref())
                .await;

            return match connection {
                Ok(connection) => Some(Ok(connection)),
                Err(error) => Some(Err(error)),
            };
        }

        loop {
            let connection_manager = self.load_balancing.lock().await.next()?;
            let connection = connection_manager
                .connection(&NEVER_RECONNECTION_POLICY)
                .await;
            if let Ok(connection) = connection {
                return Some(Ok(connection));
            }
        }
    }

    async fn node_connection(&self, node: &SocketAddr) -> Option<error::Result<Arc<T>>> {
        let connection_manager = self
            .load_balancing
            .lock()
            .await
            .find(|cm| cm.addr() == *node)?;

        Some(
            connection_manager
                .connection(self.reconnection_policy.deref())
                .await,
        )
    }
}

#[async_trait]
impl<
        'a,
        T: CdrsTransport + 'static,
        CM: ConnectionManager<T> + Send + Sync,
        LB: LoadBalancingStrategy<CM> + Send + Sync,
    > QueryExecutor<T> for Session<T, CM, LB>
{
}

#[async_trait]
impl<
        'a,
        T: CdrsTransport + 'static,
        CM: ConnectionManager<T> + Send + Sync,
        LB: LoadBalancingStrategy<CM> + Send + Sync,
    > PrepareExecutor<T> for Session<T, CM, LB>
{
}

#[async_trait]
impl<
        'a,
        T: CdrsTransport + 'static,
        CM: ConnectionManager<T> + Send + Sync,
        LB: LoadBalancingStrategy<CM> + Send + Sync,
    > ExecExecutor<T> for Session<T, CM, LB>
{
}

#[async_trait]
impl<
        'a,
        T: CdrsTransport + 'static,
        CM: ConnectionManager<T> + Send + Sync,
        LB: LoadBalancingStrategy<CM> + Send + Sync,
    > BatchExecutor<T> for Session<T, CM, LB>
{
}

impl<
        T: CdrsTransport + 'static,
        CM: ConnectionManager<T>,
        LB: LoadBalancingStrategy<CM> + Send + Sync,
    > GetRetryPolicy for Session<T, CM, LB>
{
    fn retry_policy(&self) -> &dyn RetryPolicy {
        self.retry_policy.as_ref()
    }
}

impl<
        T: CdrsTransport + 'static,
        CM: ConnectionManager<T> + Send + Sync,
        LB: LoadBalancingStrategy<CM> + Send + Sync,
    > CdrsSession<T> for Session<T, CM, LB>
{
}

#[cfg(feature = "rust-tls")]
async fn connect_tls_static<
    T: CdrsTransport + 'static,
    CM: ConnectionManager<T>,
    LB: LoadBalancingStrategy<CM> + Send + Sync,
>(
    node_configs: &ClusterRustlsConfig,
    mut load_balancing: LB,
    compression: Compression,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,
    reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
) -> error::Result<Session<T, CM, LB>>
where
    LB: LoadBalancingStrategy<RustlsConnectionManager>,
{
    let keyspace_holder = Arc::new(KeyspaceHolder::default());
    let mut nodes = Vec::with_capacity(node_configs.0.len());

    for node_config in &node_configs.0 {
        let connection_manager = RustlsConnectionManager::new(
            node_config.clone(),
            keyspace_holder.clone(),
            compression,
            None,
        );
        nodes.push(Arc::new(connection_manager));
    }

    load_balancing.init(nodes);

    Ok(Session {
        load_balancing: Mutex::new(load_balancing),
        event_stream: None,
        compression,
        retry_policy,
        reconnection_policy,
        _transport: Default::default(),
        _connection_manager: Default::default(),
    })
}

#[cfg(all(feature = "rust-tls", feature = "unstable-dynamic-cluster"))]
async fn connect_tls_dynamic<
    T: CdrsTransport + 'static,
    CM: ConnectionManager<T>,
    LB: LoadBalancingStrategy<CM> + Send + Sync,
>(
    node_configs: &ClusterRustlsConfig,
    mut load_balancing: LB,
    compression: Compression,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,
    reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
    event_src: NodeTcpConfig,
) -> error::Result<Session<T, CM, LB>>
where
    LB: LoadBalancingStrategy<RustlsConnectionManager>,
{
    let keyspace_holder = Arc::new(KeyspaceHolder::default());
    let mut nodes = Vec::with_capacity(node_configs.0.len());

    for node_config in &node_configs.0 {
        let connection_manager = RustlsConnectionManager::new(
            node_config.clone(),
            keyspace_holder.clone(),
            compression,
            None,
        );
        nodes.push(Arc::new(connection_manager));
    }

    load_balancing.init(nodes);

    let mut session = Session {
        load_balancing: Mutex::new(load_balancing),
        event_stream: None,
        compression,
        retry_policy,
        reconnection_policy,
        _transport: Default::default(),
        _connection_manager: Default::default(),
    };

    let (listener, event_stream) = session
        .listen_non_blocking(
            event_src.addr,
            event_src.authenticator,
            vec![SimpleServerEvent::StatusChange],
        )
        .await?;

    tokio::spawn(listener.start());

    session.event_stream = Some(Mutex::new(event_stream));

    Ok(session)
}

/// Workaround for https://github.com/rust-lang/rust/issues/63033
#[repr(transparent)]
pub struct RetryPolicyWrapper(pub Box<dyn RetryPolicy + Send + Sync>);

#[repr(transparent)]
pub struct ReconnectionPolicyWrapper(pub Box<dyn ReconnectionPolicy + Send + Sync>);

/// This function uses a user-supplied connection configuration to initialize all the
/// connections in the session. It can be used to supply your own transport and load
/// balancing mechanisms in order to support unusual node discovery mechanisms
/// or configuration needs.
///
/// The config object supplied differs from the ClusterTcpConfig and ClusterRustlsConfig
/// objects in that it is not expected to include an address. Instead the same configuration
/// will be applied to all connections across the cluster.
pub async fn connect_generic_static<T, C, A, CM, LB>(
    config: &C,
    initial_nodes: &[A],
    mut load_balancing: LB,
    compression: Compression,
    retry_policy: RetryPolicyWrapper,
    reconnection_policy: ReconnectionPolicyWrapper,
) -> error::Result<Session<T, CM, LB>>
where
    A: Clone,
    T: CdrsTransport + 'static,
    CM: ConnectionManager<T>,
    C: GenericClusterConfig<T, CM, Address = A>,
    LB: LoadBalancingStrategy<CM> + Sized + Send + Sync,
{
    let mut nodes = Vec::with_capacity(initial_nodes.len());

    for node in initial_nodes {
        let connection_manager = config.create_manager(node.clone()).await?;
        nodes.push(Arc::new(connection_manager));
    }

    load_balancing.init(nodes);

    Ok(Session {
        load_balancing: Mutex::new(load_balancing),
        event_stream: None,
        compression,
        retry_policy: retry_policy.0,
        reconnection_policy: reconnection_policy.0,
        _transport: Default::default(),
        _connection_manager: Default::default(),
    })
}

#[cfg(feature = "unstable-dynamic-cluster")]
pub async fn connect_generic_dynamic<T, C, A, CM, LB>(
    config: &C,
    initial_nodes: &[A],
    mut load_balancing: LB,
    compression: Compression,
    retry_policy: RetryPolicyWrapper,
    reconnection_policy: ReconnectionPolicyWrapper,
    event_src: NodeTcpConfig,
) -> error::Result<Session<T, CM, LB>>
where
    A: Clone,
    T: CdrsTransport + 'static,
    CM: ConnectionManager<T>,
    C: GenericClusterConfig<T, CM, Address = A>,
    LB: LoadBalancingStrategy<CM> + Sized + Send + Sync,
{
    let mut nodes = Vec::with_capacity(initial_nodes.len());

    for node in initial_nodes {
        let connection_manager = config.create_manager(node.clone()).await?;
        nodes.push(Arc::new(connection_manager));
    }

    load_balancing.init(nodes);

    let mut session = Session {
        load_balancing: Mutex::new(load_balancing),
        event_stream: None,
        compression,
        retry_policy: retry_policy.0,
        reconnection_policy: reconnection_policy.0,
        _transport: Default::default(),
        _connection_manager: Default::default(),
    };

    let (listener, event_stream) = session
        .listen_non_blocking(
            event_src.addr,
            event_src.authenticator,
            vec![SimpleServerEvent::StatusChange],
        )
        .await?;

    tokio::spawn(listener.start());

    session.event_stream = Some(Mutex::new(event_stream));

    Ok(session)
}

async fn connect_static<LB>(
    node_configs: &ClusterTcpConfig,
    mut load_balancing: LB,
    compression: Compression,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,
    reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
) -> error::Result<Session<TransportTcp, TcpConnectionManager, LB>>
where
    LB: LoadBalancingStrategy<TcpConnectionManager> + Send + Sync,
{
    let keyspace_holder = Arc::new(KeyspaceHolder::default());
    let mut nodes = Vec::with_capacity(node_configs.0.len());

    for node_config in &node_configs.0 {
        let connection_manager = TcpConnectionManager::new(
            node_config.clone(),
            keyspace_holder.clone(),
            compression,
            None,
        );
        nodes.push(Arc::new(connection_manager));
    }

    load_balancing.init(nodes);

    Ok(Session {
        load_balancing: Mutex::new(load_balancing),
        event_stream: None,
        compression,
        retry_policy,
        reconnection_policy,
        _transport: Default::default(),
        _connection_manager: Default::default(),
    })
}

#[cfg(feature = "unstable-dynamic-cluster")]
async fn connect_dynamic<LB>(
    node_configs: &ClusterTcpConfig,
    mut load_balancing: LB,
    compression: Compression,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,
    reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
    event_src: NodeTcpConfig,
) -> error::Result<Session<TransportTcp, TcpConnectionManager, LB>>
where
    LB: LoadBalancingStrategy<TcpConnectionManager> + Send + Sync,
{
    let keyspace_holder = Arc::new(KeyspaceHolder::default());
    let mut nodes = Vec::with_capacity(node_configs.0.len());

    for node_config in &node_configs.0 {
        let node_connection = TcpConnectionManager::new(
            node_config.clone(),
            keyspace_holder.clone(),
            compression,
            None,
        );
        nodes.push(Arc::new(node_connection));
    }

    load_balancing.init(nodes);

    let mut session = Session {
        load_balancing: Mutex::new(load_balancing),
        event_stream: None,
        compression,
        retry_policy,
        reconnection_policy,
        _transport: Default::default(),
        _connection_manager: Default::default(),
    };

    let (listener, event_stream) = session
        .listen_non_blocking(
            event_src.addr,
            event_src.authenticator,
            vec![SimpleServerEvent::StatusChange],
        )
        .await?;

    tokio::spawn(listener.start());

    session.event_stream = Some(Mutex::new(event_stream));

    Ok(session)
}

/// Creates new session that will perform queries without any compression. `Compression` type
/// can be changed at any time.
/// As a parameter it takes:
/// * cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
pub async fn new<LB>(
    node_configs: &ClusterTcpConfig,
    load_balancing: LB,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,
    reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
) -> error::Result<Session<TransportTcp, TcpConnectionManager, LB>>
where
    LB: LoadBalancingStrategy<TcpConnectionManager> + Send + Sync,
{
    connect_static(
        node_configs,
        load_balancing,
        Compression::None,
        retry_policy,
        reconnection_policy,
    )
    .await
}

/// Creates new session that will perform queries without any compression. `Compression` type
/// can be changed at any time. Once received topology change event, it will adjust an inner load
/// balancer.
/// As a parameter it takes:
/// * cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
/// * node address where to listen events
#[cfg(feature = "unstable-dynamic-cluster")]
pub async fn new_dynamic<LB>(
    node_configs: &ClusterTcpConfig,
    load_balancing: LB,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,
    reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
    event_src: NodeTcpConfig,
) -> error::Result<Session<TransportTcp, TcpConnectionManager, LB>>
where
    LB: LoadBalancingStrategy<TcpConnectionManager> + Send + Sync,
{
    connect_dynamic(
        node_configs,
        load_balancing,
        Compression::None,
        retry_policy,
        reconnection_policy,
        event_src,
    )
    .await
}

/// Creates new session that will perform queries with Snappy compression. `Compression` type
/// can be changed at any time.
/// As a parameter it takes:
/// * cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
pub async fn new_snappy<LB>(
    node_configs: &ClusterTcpConfig,
    load_balancing: LB,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,
    reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
) -> error::Result<Session<TransportTcp, TcpConnectionManager, LB>>
where
    LB: LoadBalancingStrategy<TcpConnectionManager> + Send + Sync,
{
    connect_static(
        node_configs,
        load_balancing,
        Compression::Snappy,
        retry_policy,
        reconnection_policy,
    )
    .await
}

/// Creates new session that will perform queries with Snappy compression. `Compression` type
/// can be changed at any time. Once received topology change event, it will adjust an inner load
/// balancer.
/// As a parameter it takes:
/// * cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
/// * node address where to listen events
#[cfg(feature = "unstable-dynamic-cluster")]
pub async fn new_snappy_dynamic<LB>(
    node_configs: &ClusterTcpConfig,
    load_balancing: LB,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,
    reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
    event_src: NodeTcpConfig,
) -> error::Result<Session<TransportTcp, TcpConnectionManager, LB>>
where
    LB: LoadBalancingStrategy<TcpConnectionManager> + Send + Sync,
{
    connect_dynamic(
        node_configs,
        load_balancing,
        Compression::Snappy,
        retry_policy,
        reconnection_policy,
        event_src,
    )
    .await
}

/// Creates new session that will perform queries with LZ4 compression. `Compression` type
/// can be changed at any time.
/// As a parameter it takes:
/// * cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
pub async fn new_lz4<LB>(
    node_configs: &ClusterTcpConfig,
    load_balancing: LB,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,
    reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
) -> error::Result<Session<TransportTcp, TcpConnectionManager, LB>>
where
    LB: LoadBalancingStrategy<TcpConnectionManager> + Send + Sync,
{
    connect_static(
        node_configs,
        load_balancing,
        Compression::Lz4,
        retry_policy,
        reconnection_policy,
    )
    .await
}

/// Creates new session that will perform queries with LZ4 compression. `Compression` type
/// can be changed at any time. Once received topology change event, it will adjust an inner load
/// balancer.
/// As a parameter it takes:
/// * cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
/// * node address where to listen events
#[cfg(feature = "unstable-dynamic-cluster")]
pub async fn new_lz4_dynamic<LB>(
    node_configs: &ClusterTcpConfig,
    load_balancing: LB,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,
    reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
    event_src: NodeTcpConfig,
) -> error::Result<Session<TransportTcp, TcpConnectionManager, LB>>
where
    LB: LoadBalancingStrategy<TcpConnectionManager> + Send + Sync,
{
    connect_dynamic(
        node_configs,
        load_balancing,
        Compression::Lz4,
        retry_policy,
        reconnection_policy,
        event_src,
    )
    .await
}

/// Creates new TLS session that will perform queries without any compression. `Compression` type
/// can be changed at any time.
/// As a parameter it takes:
/// * cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
#[cfg(feature = "rust-tls")]
pub async fn new_tls<LB>(
    node_configs: &ClusterRustlsConfig,
    load_balancing: LB,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,
    reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
) -> error::Result<Session<TransportRustls, RustlsConnectionManager, LB>>
where
    LB: LoadBalancingStrategy<RustlsConnectionManager> + Send + Sync,
{
    connect_tls_static(
        node_configs,
        load_balancing,
        Compression::None,
        retry_policy,
        reconnection_policy,
    )
    .await
}

/// Creates new TLS session that will perform queries without any compression. `Compression` type
/// can be changed at any time. Once received topology change event, it will adjust an inner load
/// balancer.
/// As a parameter it takes:
/// * cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
/// * node address where to listen events
#[cfg(all(feature = "rust-tls", feature = "unstable-dynamic-cluster"))]
pub async fn new_tls_dynamic<LB>(
    node_configs: &ClusterRustlsConfig,
    load_balancing: LB,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,
    reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
    event_src: NodeTcpConfig,
) -> error::Result<Session<TransportRustls, RustlsConnectionManager, LB>>
where
    LB: LoadBalancingStrategy<RustlsConnectionManager> + Send + Sync,
{
    connect_tls_dynamic(
        node_configs,
        load_balancing,
        Compression::None,
        retry_policy,
        reconnection_policy,
        event_src,
    )
    .await
}

/// Creates new TLS session that will perform queries with Snappy compression. `Compression` type
/// can be changed at any time.
/// As a parameter it takes:
/// * cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
#[cfg(feature = "rust-tls")]
pub async fn new_snappy_tls<LB>(
    node_configs: &ClusterRustlsConfig,
    load_balancing: LB,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,
    reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
) -> error::Result<Session<TransportRustls, RustlsConnectionManager, LB>>
where
    LB: LoadBalancingStrategy<RustlsConnectionManager> + Send + Sync,
{
    connect_tls_static(
        node_configs,
        load_balancing,
        Compression::Snappy,
        retry_policy,
        reconnection_policy,
    )
    .await
}

/// Creates new TLS session that will perform queries with Snappy compression. `Compression` type
/// can be changed at any time. Once received topology change event, it will adjust an inner load
/// balancer.
/// As a parameter it takes:
/// * cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
/// * node address where to listen events
#[cfg(all(feature = "rust-tls", feature = "unstable-dynamic-cluster"))]
pub async fn new_snappy_tls_dynamic<LB>(
    node_configs: &ClusterRustlsConfig,
    load_balancing: LB,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,
    reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
    event_src: NodeTcpConfig,
) -> error::Result<Session<TransportRustls, RustlsConnectionManager, LB>>
where
    LB: LoadBalancingStrategy<RustlsConnectionManager> + Send + Sync,
{
    connect_tls_dynamic(
        node_configs,
        load_balancing,
        Compression::Snappy,
        retry_policy,
        reconnection_policy,
        event_src,
    )
    .await
}

/// Creates new TLS session that will perform queries with LZ4 compression. `Compression` type
/// can be changed at any time.
/// As a parameter it takes:
/// * cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
#[cfg(feature = "rust-tls")]
pub async fn new_lz4_tls<LB>(
    node_configs: &ClusterRustlsConfig,
    load_balancing: LB,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,
    reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
) -> error::Result<Session<TransportRustls, RustlsConnectionManager, LB>>
where
    LB: LoadBalancingStrategy<RustlsConnectionManager> + Send + Sync,
{
    connect_tls_static(
        node_configs,
        load_balancing,
        Compression::Lz4,
        retry_policy,
        reconnection_policy,
    )
    .await
}

/// Creates new TLS session that will perform queries with LZ4 compression. `Compression` type
/// can be changed at any time. Once received topology change event, it will adjust an inner load
/// balancer.
/// As a parameter it takes:
/// * cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
/// * node address where to listen events
#[cfg(all(feature = "rust-tls", feature = "unstable-dynamic-cluster"))]
pub async fn new_lz4_tls_dynamic<LB>(
    node_configs: &ClusterRustlsConfig,
    load_balancing: LB,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,
    reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
    event_src: NodeTcpConfig,
) -> error::Result<Session<TransportRustls, RustlsConnectionManager, LB>>
where
    LB: LoadBalancingStrategy<RustlsConnectionManager> + Send + Sync,
{
    connect_tls_dynamic(
        node_configs,
        load_balancing,
        Compression::Lz4,
        retry_policy,
        reconnection_policy,
        event_src,
    )
    .await
}

impl<
        T: CdrsTransport + 'static,
        CM: ConnectionManager<T>,
        LB: LoadBalancingStrategy<CM> + Send + Sync,
    > Session<T, CM, LB>
{
    /// Returns new event listener.
    pub async fn listen(
        &self,
        node: SocketAddr,
        authenticator: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
        events: Vec<SimpleServerEvent>,
    ) -> error::Result<(Listener, EventStream)> {
        let keyspace_holder = Arc::new(KeyspaceHolder::default());
        let config = NodeTcpConfigBuilder::new(node, authenticator).build();
        let (event_sender, event_receiver) = channel(256);
        let connection_manager = TcpConnectionManager::new(
            config,
            keyspace_holder,
            self.compression,
            Some(event_sender),
        );
        let transport = connection_manager
            .connection(&NeverReconnectionPolicy)
            .await?;

        let query_frame = Frame::new_req_register(events);
        transport.write_frame(query_frame).await?;

        let (sender, receiver) = std_channel();
        Ok((
            new_listener(sender, event_receiver),
            EventStream::new(receiver),
        ))
    }

    #[cfg(feature = "rust-tls")]
    pub async fn listen_tls(
        &self,
        node: net::SocketAddr,
        authenticator: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
        events: Vec<SimpleServerEvent>,
        dns_name: webpki::DNSName,
        config: Arc<rustls::ClientConfig>,
    ) -> error::Result<(Listener, EventStream)> {
        let keyspace_holder = Arc::new(KeyspaceHolder::default());
        let config = NodeRustlsConfigBuilder::new(node, dns_name, authenticator, config).build();
        let (event_sender, event_receiver) = channel(256);
        let connection_manager = RustlsConnectionManager::new(
            config,
            keyspace_holder,
            self.compression,
            Some(event_sender),
        );
        let transport = connection_manager
            .connection(&NeverReconnectionPolicy)
            .await?;

        let query_frame = Frame::new_req_register(events);
        transport.write_frame(query_frame).await?;

        let (sender, receiver) = std_channel();
        Ok((
            new_listener(sender, event_receiver),
            EventStream::new(receiver),
        ))
    }

    pub async fn listen_non_blocking(
        &self,
        node: SocketAddr,
        authenticator: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
        events: Vec<SimpleServerEvent>,
    ) -> error::Result<(Listener, EventStreamNonBlocking)> {
        self.listen(node, authenticator, events).await.map(|l| {
            let (listener, stream) = l;
            (listener, stream.into())
        })
    }

    #[cfg(feature = "rust-tls")]
    pub async fn listen_tls_blocking(
        &self,
        node: net::SocketAddr,
        authenticator: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
        events: Vec<SimpleServerEvent>,
        dns_name: webpki::DNSName,
        config: Arc<rustls::ClientConfig>,
    ) -> error::Result<(Listener, EventStreamNonBlocking)> {
        self.listen_tls(node, authenticator, events, dns_name, config)
            .await
            .map(|l| {
                let (listener, stream) = l;
                (listener, stream.into())
            })
    }
}
