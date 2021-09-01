use async_trait::async_trait;
use std::marker::PhantomData;
#[cfg(feature = "rust-tls")]
use std::net;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::mpsc::channel as std_channel;
use std::sync::Arc;
use tokio::sync::mpsc::channel;

use crate::authenticators::SaslAuthenticatorProvider;
use crate::cluster::connection_manager::ConnectionManager;
#[cfg(feature = "rust-tls")]
use crate::cluster::rustls_connection_manager::RustlsConnectionManager;
use crate::cluster::tcp_connection_manager::TcpConnectionManager;
#[cfg(feature = "rust-tls")]
use crate::cluster::ClusterRustlsConfig;
#[cfg(feature = "rust-tls")]
use crate::cluster::NodeRustlsConfigBuilder;
use crate::cluster::{
    CdrsSession, ClusterTcpConfig, GenericClusterConfig, GetConnection, GetRetryPolicy,
    KeyspaceHolder,
};
use crate::cluster::{NodeTcpConfigBuilder, SessionPager};
use crate::compression::Compression;
use crate::error;
use crate::events::{new_listener, EventStream, EventStreamNonBlocking, Listener};
use crate::frame::events::SimpleServerEvent;
use crate::frame::Frame;
use crate::load_balancing::LoadBalancingStrategy;
use crate::query::{BatchExecutor, ExecExecutor, PrepareExecutor, QueryExecutor};
use crate::retry::{
    DefaultRetryPolicy, ExponentialReconnectionPolicy, NeverReconnectionPolicy, ReconnectionPolicy,
    RetryPolicy,
};
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
    load_balancing: LB,
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

    fn new(
        load_balancing: LB,
        compression: Compression,
        retry_policy: Box<dyn RetryPolicy + Send + Sync>,
        reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
    ) -> Self {
        Session {
            load_balancing,
            compression,
            retry_policy,
            reconnection_policy,
            _transport: Default::default(),
            _connection_manager: Default::default(),
        }
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
        // when using a load balancer with > 1 node, don't use reconnection policy for a given node,
        // but jump to the next one

        let connection_manager = {
            if self.load_balancing.size() < 2 {
                self.load_balancing.next()
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
            let connection_manager = self.load_balancing.next()?;
            let connection = connection_manager
                .connection(&NEVER_RECONNECTION_POLICY)
                .await;
            if let Ok(connection) = connection {
                return Some(Ok(connection));
            }
        }
    }

    async fn node_connection(&self, node: &SocketAddr) -> Option<error::Result<Arc<T>>> {
        let connection_manager = self.load_balancing.find(|cm| cm.addr() == *node)?;

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
        load_balancing,
        compression,
        retry_policy: retry_policy.0,
        reconnection_policy: reconnection_policy.0,
        _transport: Default::default(),
        _connection_manager: Default::default(),
    })
}

/// Creates new session that will perform queries without any compression. `Compression` type
/// can be changed at any time.
/// As a parameter it takes:
/// * cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
#[deprecated(note = "Use SessionBuilder instead.")]
pub async fn new<LB>(
    node_configs: &ClusterTcpConfig,
    load_balancing: LB,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,
    reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
) -> error::Result<Session<TransportTcp, TcpConnectionManager, LB>>
where
    LB: LoadBalancingStrategy<TcpConnectionManager> + Send + Sync,
{
    Ok(TcpSessionBuilder::new(load_balancing, node_configs.clone())
        .with_retry_policy(retry_policy)
        .with_reconnection_policy(reconnection_policy)
        .build())
}

/// Creates new session that will perform queries with Snappy compression. `Compression` type
/// can be changed at any time.
/// As a parameter it takes:
/// * cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
#[deprecated(note = "Use SessionBuilder instead.")]
pub async fn new_snappy<LB>(
    node_configs: &ClusterTcpConfig,
    load_balancing: LB,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,
    reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
) -> error::Result<Session<TransportTcp, TcpConnectionManager, LB>>
where
    LB: LoadBalancingStrategy<TcpConnectionManager> + Send + Sync,
{
    Ok(TcpSessionBuilder::new(load_balancing, node_configs.clone())
        .with_compression(Compression::Snappy)
        .with_retry_policy(retry_policy)
        .with_reconnection_policy(reconnection_policy)
        .build())
}

/// Creates new session that will perform queries with LZ4 compression. `Compression` type
/// can be changed at any time.
/// As a parameter it takes:
/// * cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
#[deprecated(note = "Use SessionBuilder instead.")]
pub async fn new_lz4<LB>(
    node_configs: &ClusterTcpConfig,
    load_balancing: LB,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,
    reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
) -> error::Result<Session<TransportTcp, TcpConnectionManager, LB>>
where
    LB: LoadBalancingStrategy<TcpConnectionManager> + Send + Sync,
{
    Ok(TcpSessionBuilder::new(load_balancing, node_configs.clone())
        .with_compression(Compression::Lz4)
        .with_retry_policy(retry_policy)
        .with_reconnection_policy(reconnection_policy)
        .build())
}

/// Creates new TLS session that will perform queries without any compression. `Compression` type
/// can be changed at any time.
/// As a parameter it takes:
/// * cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
#[cfg(feature = "rust-tls")]
#[deprecated(note = "Use SessionBuilder instead.")]
pub async fn new_tls<LB>(
    node_configs: &ClusterRustlsConfig,
    load_balancing: LB,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,
    reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
) -> error::Result<Session<TransportRustls, RustlsConnectionManager, LB>>
where
    LB: LoadBalancingStrategy<RustlsConnectionManager> + Send + Sync,
{
    Ok(
        RustlsSessionBuilder::new(load_balancing, node_configs.clone())
            .with_retry_policy(retry_policy)
            .with_reconnection_policy(reconnection_policy)
            .build(),
    )
}

/// Creates new TLS session that will perform queries with Snappy compression. `Compression` type
/// can be changed at any time.
/// As a parameter it takes:
/// * cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
#[cfg(feature = "rust-tls")]
#[deprecated(note = "Use SessionBuilder instead.")]
pub async fn new_snappy_tls<LB>(
    node_configs: &ClusterRustlsConfig,
    load_balancing: LB,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,
    reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
) -> error::Result<Session<TransportRustls, RustlsConnectionManager, LB>>
where
    LB: LoadBalancingStrategy<RustlsConnectionManager> + Send + Sync,
{
    Ok(
        RustlsSessionBuilder::new(load_balancing, node_configs.clone())
            .with_compression(Compression::Snappy)
            .with_retry_policy(retry_policy)
            .with_reconnection_policy(reconnection_policy)
            .build(),
    )
}

/// Creates new TLS session that will perform queries with LZ4 compression. `Compression` type
/// can be changed at any time.
/// As a parameter it takes:
/// * cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
#[cfg(feature = "rust-tls")]
#[deprecated(note = "Use SessionBuilder instead.")]
pub async fn new_lz4_tls<LB>(
    node_configs: &ClusterRustlsConfig,
    load_balancing: LB,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,
    reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
) -> error::Result<Session<TransportRustls, RustlsConnectionManager, LB>>
where
    LB: LoadBalancingStrategy<RustlsConnectionManager> + Send + Sync,
{
    Ok(
        RustlsSessionBuilder::new(load_balancing, node_configs.clone())
            .with_compression(Compression::Lz4)
            .with_retry_policy(retry_policy)
            .with_reconnection_policy(reconnection_policy)
            .build(),
    )
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
        let config = NodeTcpConfigBuilder::new(node)
            .with_authenticator_provider(authenticator)
            .build();
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
        transport.write_frame(&query_frame).await?;

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
        let config = NodeRustlsConfigBuilder::new(node, dns_name, config)
            .with_authenticator_provider(authenticator)
            .build();
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
        transport.write_frame(&query_frame).await?;

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

struct SessionConfig<CM, LB: LoadBalancingStrategy<CM> + Send + Sync> {
    compression: Compression,
    load_balancing: LB,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,
    reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
    _connection_manager: PhantomData<CM>,
}

impl<CM, LB: LoadBalancingStrategy<CM> + Send + Sync> SessionConfig<CM, LB> {
    fn new(
        compression: Compression,
        load_balancing: LB,
        retry_policy: Box<dyn RetryPolicy + Send + Sync>,
        reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
    ) -> Self {
        SessionConfig {
            compression,
            load_balancing,
            retry_policy,
            reconnection_policy,
            _connection_manager: Default::default(),
        }
    }
}

/// Builder for easy `Session` creation. Requires static `LoadBalancingStrategy`, but otherwise, other
/// configuration parameters can be dynamically set. Use concrete implementers to create specific
/// sessions.
pub trait SessionBuilder<
    T: CdrsTransport + Send + Sync + 'static,
    CM: ConnectionManager<T>,
    LB: LoadBalancingStrategy<CM> + Send + Sync,
>
{
    /// Sets new compression.
    fn with_compression(self, compression: Compression) -> Self;

    /// Set new retry policy.
    fn with_retry_policy(self, retry_policy: Box<dyn RetryPolicy + Send + Sync>) -> Self;

    /// Set new reconnection policy.
    fn with_reconnection_policy(
        self,
        reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
    ) -> Self;

    /// Builds the resulting session.
    fn build(self) -> Session<T, CM, LB>;
}

/// Builder for non-TLS sessions.
pub struct TcpSessionBuilder<LB: LoadBalancingStrategy<TcpConnectionManager> + Send + Sync> {
    config: SessionConfig<TcpConnectionManager, LB>,
    node_configs: ClusterTcpConfig,
}

impl<LB: LoadBalancingStrategy<TcpConnectionManager> + Send + Sync> TcpSessionBuilder<LB> {
    /// Creates a new builder with default session configuration.
    pub fn new(load_balancing: LB, node_configs: ClusterTcpConfig) -> Self {
        TcpSessionBuilder {
            config: SessionConfig::new(
                Compression::None,
                load_balancing,
                Box::new(DefaultRetryPolicy::default()),
                Box::new(ExponentialReconnectionPolicy::default()),
            ),
            node_configs,
        }
    }
}

impl<LB: LoadBalancingStrategy<TcpConnectionManager> + Send + Sync>
    SessionBuilder<TransportTcp, TcpConnectionManager, LB> for TcpSessionBuilder<LB>
{
    fn with_compression(mut self, compression: Compression) -> Self {
        self.config.compression = compression;
        self
    }

    fn with_retry_policy(mut self, retry_policy: Box<dyn RetryPolicy + Send + Sync>) -> Self {
        self.config.retry_policy = retry_policy;
        self
    }

    fn with_reconnection_policy(
        mut self,
        reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
    ) -> Self {
        self.config.reconnection_policy = reconnection_policy;
        self
    }

    fn build(mut self) -> Session<TransportTcp, TcpConnectionManager, LB> {
        let keyspace_holder = Arc::new(KeyspaceHolder::default());
        let mut nodes = Vec::with_capacity(self.node_configs.0.len());

        for node_config in self.node_configs.0 {
            let connection_manager = TcpConnectionManager::new(
                node_config,
                keyspace_holder.clone(),
                self.config.compression,
                None,
            );
            nodes.push(Arc::new(connection_manager));
        }

        self.config.load_balancing.init(nodes);

        Session::new(
            self.config.load_balancing,
            self.config.compression,
            self.config.retry_policy,
            self.config.reconnection_policy,
        )
    }
}

#[cfg(feature = "rust-tls")]
/// Builder for TLS sessions.
pub struct RustlsSessionBuilder<LB: LoadBalancingStrategy<RustlsConnectionManager> + Send + Sync> {
    config: SessionConfig<RustlsConnectionManager, LB>,
    node_configs: ClusterRustlsConfig,
}

#[cfg(feature = "rust-tls")]
impl<LB: LoadBalancingStrategy<RustlsConnectionManager> + Send + Sync> RustlsSessionBuilder<LB> {
    /// Creates a new builder with default session configuration.
    pub fn new(load_balancing: LB, node_configs: ClusterRustlsConfig) -> Self {
        RustlsSessionBuilder {
            config: SessionConfig::new(
                Compression::None,
                load_balancing,
                Box::new(DefaultRetryPolicy::default()),
                Box::new(ExponentialReconnectionPolicy::default()),
            ),
            node_configs,
        }
    }
}

#[cfg(feature = "rust-tls")]
impl<LB: LoadBalancingStrategy<RustlsConnectionManager> + Send + Sync>
    SessionBuilder<TransportRustls, RustlsConnectionManager, LB> for RustlsSessionBuilder<LB>
{
    fn with_compression(mut self, compression: Compression) -> Self {
        self.config.compression = compression;
        self
    }

    fn with_retry_policy(mut self, retry_policy: Box<dyn RetryPolicy + Send + Sync>) -> Self {
        self.config.retry_policy = retry_policy;
        self
    }

    fn with_reconnection_policy(
        mut self,
        reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
    ) -> Self {
        self.config.reconnection_policy = reconnection_policy;
        self
    }

    fn build(mut self) -> Session<TransportRustls, RustlsConnectionManager, LB> {
        let keyspace_holder = Arc::new(KeyspaceHolder::default());
        let mut nodes = Vec::with_capacity(self.node_configs.0.len());

        for node_config in self.node_configs.0 {
            let connection_manager = RustlsConnectionManager::new(
                node_config,
                keyspace_holder.clone(),
                self.config.compression,
                None,
            );
            nodes.push(Arc::new(connection_manager));
        }

        self.config.load_balancing.init(nodes);

        Session::new(
            self.config.load_balancing,
            self.config.compression,
            self.config.retry_policy,
            self.config.reconnection_policy,
        )
    }
}
