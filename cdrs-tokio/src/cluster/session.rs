use async_trait::async_trait;
use std::iter::Iterator;
#[cfg(feature = "rust-tls")]
use std::net;
use std::ops::Deref;
use std::sync::mpsc::channel as std_channel;
use std::sync::Arc;
use tokio::sync::mpsc::channel;
use tokio::sync::Mutex;

use crate::authenticators::SaslAuthenticatorProvider;
#[cfg(feature = "unstable-dynamic-cluster")]
use crate::cluster::NodeTcpConfig;
use crate::cluster::SessionPager;
#[cfg(feature = "rust-tls")]
use crate::cluster::{new_rustls_pool, ClusterRustlsConfig, RustlsConnectionPool};
use crate::cluster::{
    new_tcp_pool, startup, CdrsSession, ClusterTcpConfig, ConnectionPool, GenericClusterConfig,
    GetConnection, GetRetryPolicy, KeyspaceHolder, TcpConnectionPool,
};
use crate::compression::Compression;
use crate::error;
use crate::events::{new_listener, EventStream, EventStreamNonBlocking, Listener};
use crate::frame::events::{ServerEvent, SimpleServerEvent, StatusChange, StatusChangeType};
use crate::frame::Frame;
use crate::load_balancing::LoadBalancingStrategy;
use crate::query::{BatchExecutor, ExecExecutor, PrepareExecutor, QueryExecutor};
use crate::retry::RetryPolicy;
#[cfg(feature = "rust-tls")]
use crate::transport::TransportRustls;
use crate::transport::{CdrsTransport, TransportTcp};

/// CDRS session that holds one pool of authorized connections per node.
/// `compression` field contains data compressor that will be used
/// for decompressing data received from Cassandra server.
pub struct Session<LB> {
    load_balancing: Mutex<LB>,
    event_stream: Option<Mutex<EventStreamNonBlocking>>,
    compression: Compression,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,
}

impl<'a, LB> Session<LB> {
    /// Basing on current session returns new `SessionPager` that can be used
    /// for performing paged queries.
    pub fn paged<T: CdrsTransport + Unpin + 'static>(
        &'a self,
        page_size: i32,
    ) -> SessionPager<'a, Session<LB>, T>
    where
        Session<LB>: CdrsSession<T>,
    {
        SessionPager::new(self, page_size)
    }
}

#[async_trait]
impl<
        T: CdrsTransport + Send + Sync + 'static,
        LB: LoadBalancingStrategy<ConnectionPool<T>> + Send + Sync,
    > GetConnection<T> for Session<LB>
{
    async fn connection(&self) -> Option<Arc<ConnectionPool<T>>> {
        if cfg!(feature = "unstable-dynamic-cluster") {
            if let Some(ref event_stream_mx) = self.event_stream {
                if let Ok(ref mut event_stream) = event_stream_mx.try_lock() {
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

        self.load_balancing.lock().await.next()
    }
}

#[async_trait]
impl<
        'a,
        T: CdrsTransport + Unpin + 'static,
        LB: LoadBalancingStrategy<ConnectionPool<T>> + Send + Sync,
    > QueryExecutor<T> for Session<LB>
{
}

#[async_trait]
impl<
        'a,
        T: CdrsTransport + Unpin + 'static,
        LB: LoadBalancingStrategy<ConnectionPool<T>> + Send + Sync,
    > PrepareExecutor<T> for Session<LB>
{
}

#[async_trait]
impl<
        'a,
        T: CdrsTransport + Unpin + 'static,
        LB: LoadBalancingStrategy<ConnectionPool<T>> + Send + Sync,
    > ExecExecutor<T> for Session<LB>
{
}

#[async_trait]
impl<
        'a,
        T: CdrsTransport + Unpin + 'static,
        LB: LoadBalancingStrategy<ConnectionPool<T>> + Send + Sync,
    > BatchExecutor<T> for Session<LB>
{
}

impl<LB> GetRetryPolicy for Session<LB> {
    fn retry_policy(&self) -> &dyn RetryPolicy {
        self.retry_policy.as_ref()
    }
}

impl<
        T: CdrsTransport + Unpin + 'static,
        LB: LoadBalancingStrategy<ConnectionPool<T>> + Send + Sync,
    > CdrsSession<T> for Session<LB>
{
}

#[cfg(feature = "rust-tls")]
async fn connect_tls_static<LB>(
    node_configs: &ClusterRustlsConfig,
    mut load_balancing: LB,
    compression: Compression,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,
) -> error::Result<Session<LB>>
where
    LB: LoadBalancingStrategy<RustlsConnectionPool>,
{
    let mut nodes: Vec<Arc<RustlsConnectionPool>> = Vec::with_capacity(node_configs.0.len());

    for node_config in &node_configs.0 {
        let node_connection_pool = new_rustls_pool(node_config.clone(), compression).await?;
        nodes.push(Arc::new(node_connection_pool));
    }

    load_balancing.init(nodes);

    Ok(Session {
        load_balancing: Mutex::new(load_balancing),
        event_stream: None,
        compression,
        retry_policy,
    })
}

#[cfg(all(feature = "rust-tls", feature = "unstable-dynamic-cluster"))]
async fn connect_tls_dynamic<LB>(
    node_configs: &ClusterRustlsConfig,
    mut load_balancing: LB,
    compression: Compression,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,
    event_src: NodeTcpConfig,
) -> error::Result<Session<LB>>
where
    LB: LoadBalancingStrategy<RustlsConnectionPool>,
{
    let mut nodes: Vec<Arc<RustlsConnectionPool>> = Vec::with_capacity(node_configs.0.len());

    for node_config in &node_configs.0 {
        let node_connection_pool = new_rustls_pool(node_config.clone(), compression).await?;
        nodes.push(Arc::new(node_connection_pool));
    }

    load_balancing.init(nodes);

    let mut session = Session {
        load_balancing: Mutex::new(load_balancing),
        event_stream: None,
        compression,
        retry_policy,
    };

    let (listener, event_stream) = session
        .listen_non_blocking(
            &event_src.addr,
            event_src.authenticator.deref(),
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

/// This function uses a user-supplied connection configuration to initialize all the
/// connections in the session. It can be used to supply your own transport and load
/// balancing mechanisms in order to support unusual node discovery mechanisms
/// or configuration needs.
///
/// The config object supplied differs from the ClusterTcpConfig and ClusterRustlsConfig
/// objects in that it is not expected to include an address. Instead the same configuration
/// will be applied to all connections across the cluster.
pub async fn connect_generic_static<T, M, C, A, LB>(
    config: &C,
    initial_nodes: &[A],
    mut load_balancing: LB,
    compression: Compression,
    retry_policy: RetryPolicyWrapper,
) -> error::Result<Session<LB>>
where
    M: bb8::ManageConnection<Connection = T>,
    A: Clone,
    T: CdrsTransport<Manager = M>,
    C: GenericClusterConfig<Transport = T, Address = A>,
    LB: LoadBalancingStrategy<ConnectionPool<T>> + Sized,
{
    let mut nodes: Vec<Arc<ConnectionPool<T>>> = Vec::with_capacity(initial_nodes.len());

    for node in initial_nodes {
        let pool = config.connect(node.clone()).await?;
        nodes.push(Arc::new(pool));
    }

    load_balancing.init(nodes);

    Ok(Session {
        load_balancing: Mutex::new(load_balancing),
        event_stream: None,
        compression,
        retry_policy: retry_policy.0,
    })
}

#[cfg(feature = "unstable-dynamic-cluster")]
pub async fn connect_generic_dynamic<T, M, C, A, LB>(
    config: &C,
    initial_nodes: &[A],
    mut load_balancing: LB,
    compression: Compression,
    retry_policy: RetryPolicyWrapper,
    event_src: NodeTcpConfig,
) -> error::Result<Session<LB>>
where
    M: bb8::ManageConnection<Connection = T>,
    A: Clone,
    T: CdrsTransport<Manager = M>,
    C: GenericClusterConfig<Transport = T, Address = A>,
    LB: LoadBalancingStrategy<ConnectionPool<T>> + Sized,
{
    let mut nodes: Vec<Arc<ConnectionPool<T>>> = Vec::with_capacity(initial_nodes.len());

    for node in initial_nodes {
        let pool = config.connect(node.clone()).await?;
        nodes.push(Arc::new(pool));
    }

    load_balancing.init(nodes);

    let mut session = Session {
        load_balancing: Mutex::new(load_balancing),
        event_stream: None,
        compression,
        retry_policy: retry_policy.0,
    };

    let (listener, event_stream) = session
        .listen_non_blocking(
            &event_src.addr,
            event_src.authenticator.deref(),
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
) -> error::Result<Session<LB>>
where
    LB: LoadBalancingStrategy<TcpConnectionPool>,
{
    let mut nodes: Vec<Arc<TcpConnectionPool>> = Vec::with_capacity(node_configs.0.len());

    for node_config in &node_configs.0 {
        let node_connection_pool = new_tcp_pool(node_config.clone(), compression).await?;
        nodes.push(Arc::new(node_connection_pool));
    }

    load_balancing.init(nodes);

    Ok(Session {
        load_balancing: Mutex::new(load_balancing),
        event_stream: None,
        compression,
        retry_policy,
    })
}

#[cfg(feature = "unstable-dynamic-cluster")]
async fn connect_dynamic<LB>(
    node_configs: &ClusterTcpConfig,
    mut load_balancing: LB,
    compression: Compression,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,
    event_src: NodeTcpConfig,
) -> error::Result<Session<LB>>
where
    LB: LoadBalancingStrategy<TcpConnectionPool>,
{
    let mut nodes: Vec<Arc<TcpConnectionPool>> = Vec::with_capacity(node_configs.0.len());

    for node_config in &node_configs.0 {
        let node_connection_pool = new_tcp_pool(node_config.clone(), compression).await?;
        nodes.push(Arc::new(node_connection_pool));
    }

    load_balancing.init(nodes);

    let mut session = Session {
        load_balancing: Mutex::new(load_balancing),
        event_stream: None,
        compression,
        retry_policy,
    };

    let (listener, event_stream) = session
        .listen_non_blocking(
            &event_src.addr,
            event_src.authenticator.deref(),
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
) -> error::Result<Session<LB>>
where
    LB: LoadBalancingStrategy<TcpConnectionPool>,
{
    connect_static(
        node_configs,
        load_balancing,
        Compression::None,
        retry_policy,
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
    event_src: NodeTcpConfig,
) -> error::Result<Session<LB>>
where
    LB: LoadBalancingStrategy<TcpConnectionPool>,
{
    connect_dynamic(
        node_configs,
        load_balancing,
        Compression::None,
        retry_policy,
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
) -> error::Result<Session<LB>>
where
    LB: LoadBalancingStrategy<TcpConnectionPool>,
{
    connect_static(
        node_configs,
        load_balancing,
        Compression::Snappy,
        retry_policy,
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
    event_src: NodeTcpConfig,
) -> error::Result<Session<LB>>
where
    LB: LoadBalancingStrategy<TcpConnectionPool>,
{
    connect_dynamic(
        node_configs,
        load_balancing,
        Compression::Snappy,
        retry_policy,
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
) -> error::Result<Session<LB>>
where
    LB: LoadBalancingStrategy<TcpConnectionPool>,
{
    connect_static(node_configs, load_balancing, Compression::Lz4, retry_policy).await
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
    event_src: NodeTcpConfig,
) -> error::Result<Session<LB>>
where
    LB: LoadBalancingStrategy<TcpConnectionPool>,
{
    connect_dynamic(
        node_configs,
        load_balancing,
        Compression::Lz4,
        retry_policy,
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
) -> error::Result<Session<LB>>
where
    LB: LoadBalancingStrategy<RustlsConnectionPool>,
{
    connect_tls_static(
        node_configs,
        load_balancing,
        Compression::None,
        retry_policy,
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
    event_src: NodeTcpConfig,
) -> error::Result<Session<LB>>
where
    LB: LoadBalancingStrategy<RustlsConnectionPool>,
{
    connect_tls_dynamic(
        node_configs,
        load_balancing,
        Compression::None,
        retry_policy,
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
) -> error::Result<Session<LB>>
where
    LB: LoadBalancingStrategy<RustlsConnectionPool>,
{
    connect_tls_static(
        node_configs,
        load_balancing,
        Compression::Snappy,
        retry_policy,
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
    event_src: NodeTcpConfig,
) -> error::Result<Session<LB>>
where
    LB: LoadBalancingStrategy<RustlsConnectionPool>,
{
    connect_tls_dynamic(
        node_configs,
        load_balancing,
        Compression::Snappy,
        retry_policy,
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
) -> error::Result<Session<LB>>
where
    LB: LoadBalancingStrategy<RustlsConnectionPool>,
{
    connect_tls_static(node_configs, load_balancing, Compression::Lz4, retry_policy).await
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
    event_src: NodeTcpConfig,
) -> error::Result<Session<LB>>
where
    LB: LoadBalancingStrategy<RustlsConnectionPool>,
{
    connect_tls_dynamic(
        node_configs,
        load_balancing,
        Compression::Lz4,
        retry_policy,
        event_src,
    )
    .await
}

impl<L> Session<L> {
    /// Returns new event listener.
    pub async fn listen<A: SaslAuthenticatorProvider + Send + Sync + ?Sized + 'static>(
        &self,
        node: &str,
        authenticator: &A,
        events: Vec<SimpleServerEvent>,
    ) -> error::Result<(Listener<TransportTcp>, EventStream)> {
        let keyspace_holder = Arc::new(KeyspaceHolder::default());
        let (frame_sender, frame_receiver) = channel(256);
        let transport = TransportTcp::new(
            node,
            keyspace_holder.clone(),
            Some(frame_sender),
            self.compression,
        )
        .await?;

        startup(
            &transport,
            authenticator,
            keyspace_holder.deref(),
            self.compression,
        )
        .await?;

        let query_frame = Frame::new_req_register(events);
        transport.write_frame(query_frame).await?;

        let (sender, receiver) = std_channel();
        Ok((
            new_listener(transport, sender, frame_receiver),
            EventStream::new(receiver),
        ))
    }

    #[cfg(feature = "rust-tls")]
    /// Returns new event listener.
    pub async fn listen_tls<A: SaslAuthenticatorProvider + Send + Sync + ?Sized + 'static>(
        &self,
        node: net::SocketAddr,
        authenticator: &A,
        events: Vec<SimpleServerEvent>,
        dns_name: webpki::DNSName,
        config: Arc<rustls::ClientConfig>,
    ) -> error::Result<(Listener<TransportRustls>, EventStream)> {
        let keyspace_holder = Arc::new(KeyspaceHolder::default());
        let (frame_sender, frame_receiver) = channel(256);
        let transport = TransportRustls::new(
            node,
            dns_name,
            config,
            keyspace_holder.clone(),
            Some(frame_sender),
            self.compression,
        )
        .await?;

        startup(
            &transport,
            authenticator,
            keyspace_holder.deref(),
            self.compression,
        )
        .await?;

        let query_frame = Frame::new_req_register(events);
        transport.write_frame(query_frame).await?;

        let (sender, receiver) = std_channel();
        Ok((
            new_listener(transport, sender, frame_receiver),
            EventStream::new(receiver),
        ))
    }

    pub async fn listen_non_blocking<
        A: SaslAuthenticatorProvider + Send + Sync + ?Sized + 'static,
    >(
        &self,
        node: &str,
        authenticator: &A,
        events: Vec<SimpleServerEvent>,
    ) -> error::Result<(Listener<TransportTcp>, EventStreamNonBlocking)> {
        self.listen(node, authenticator, events).await.map(|l| {
            let (listener, stream) = l;
            (listener, stream.into())
        })
    }
}
