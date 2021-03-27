use async_trait::async_trait;
use fxhash::FxHashMap;
use std::iter::Iterator;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;
use tokio::{io::AsyncWriteExt, sync::Mutex};

#[cfg(feature = "unstable-dynamic-cluster")]
use crate::cluster::NodeTcpConfig;
#[cfg(feature = "rust-tls")]
use crate::cluster::{new_rustls_pool, ClusterRustlsConfig, RustlsConnectionPool};
use crate::cluster::{
    new_tcp_pool, startup, CDRSSession, ClusterTcpConfig, ConnectionConfig, ConnectionPool,
    GetCompressor, GetConnection, KeyspaceHolder, ResponseCache, TcpConnectionPool,
};
use crate::error;
use crate::load_balancing::LoadBalancingStrategy;
use crate::transport::{CDRSTransport, TransportTcp};

use crate::authenticators::Authenticator;
use crate::cluster::SessionPager;
use crate::compression::Compression;
use crate::events::{new_listener, EventStream, EventStreamNonBlocking, Listener};
use crate::frame::events::{ServerEvent, SimpleServerEvent, StatusChange, StatusChangeType};
use crate::frame::parser::parse_frame;
use crate::frame::{AsBytes, Frame, StreamId};
use crate::query::{BatchExecutor, ExecExecutor, PrepareExecutor, QueryExecutor};

/// CDRS session that holds one pool of authorized connecitons per node.
/// `compression` field contains data compressor that will be used
/// for decompressing data received from Cassandra server.
#[derive(Debug)]
pub struct Session<LB> {
    load_balancing: Mutex<LB>,
    event_stream: Option<Mutex<EventStreamNonBlocking>>,
    responses: Mutex<FxHashMap<StreamId, Frame>>,
    #[allow(dead_code)]
    pub compression: Compression,
}

impl<LB> GetCompressor for Session<LB> {
    /// Returns compression that current session has.
    fn get_compressor(&self) -> Compression {
        self.compression
    }
}

impl<'a, LB> Session<LB> {
    /// Basing on current session returns new `SessionPager` that can be used
    /// for performing paged queries.
    pub fn paged<T: CDRSTransport + Unpin + 'static>(
        &'a mut self,
        page_size: i32,
    ) -> SessionPager<'a, Session<LB>, T>
    where
        Session<LB>: CDRSSession<T>,
    {
        SessionPager::new(self, page_size)
    }
}

#[async_trait]
impl<
        T: CDRSTransport + Send + Sync + 'static,
        LB: LoadBalancingStrategy<ConnectionPool<T>> + Send + Sync,
    > GetConnection<T> for Session<LB>
{
    async fn get_connection(&self) -> Option<Arc<ConnectionPool<T>>> {
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
                                    .remove_node(|pool| pool.get_addr() == addr.addr);
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
        T: CDRSTransport + Unpin + 'static,
        LB: LoadBalancingStrategy<ConnectionPool<T>> + Send + Sync,
    > QueryExecutor<T> for Session<LB>
{
}

#[async_trait]
impl<
        'a,
        T: CDRSTransport + Unpin + 'static,
        LB: LoadBalancingStrategy<ConnectionPool<T>> + Send + Sync,
    > PrepareExecutor<T> for Session<LB>
{
}

#[async_trait]
impl<
        'a,
        T: CDRSTransport + Unpin + 'static,
        LB: LoadBalancingStrategy<ConnectionPool<T>> + Send + Sync,
    > ExecExecutor<T> for Session<LB>
{
}

#[async_trait]
impl<
        'a,
        T: CDRSTransport + Unpin + 'static,
        LB: LoadBalancingStrategy<ConnectionPool<T>> + Send + Sync,
    > BatchExecutor<T> for Session<LB>
{
}

impl<
        T: CDRSTransport + Unpin + 'static,
        LB: LoadBalancingStrategy<ConnectionPool<T>> + Send + Sync,
    > CDRSSession<T> for Session<LB>
{
}

#[async_trait]
impl<LB> ResponseCache for Session<LB>
where
    LB: Send,
{
    async fn match_or_cache_response(&self, stream_id: i16, frame: Frame) -> Option<Frame> {
        if frame.stream == stream_id {
            return Some(frame);
        }

        let mut responses = self.responses.lock().await;

        responses.insert(frame.stream, frame);
        responses.remove(&stream_id)
    }
}

#[cfg(feature = "rust-tls")]
async fn connect_tls_static<LB>(
    node_configs: &ClusterRustlsConfig,
    mut load_balancing: LB,
    compression: Compression,
) -> error::Result<Session<LB>>
where
    LB: LoadBalancingStrategy<RustlsConnectionPool>,
{
    let mut nodes: Vec<Arc<RustlsConnectionPool>> = Vec::with_capacity(node_configs.0.len());

    for node_config in &node_configs.0 {
        let node_connection_pool = new_rustls_pool(node_config.clone()).await?;
        nodes.push(Arc::new(node_connection_pool));
    }

    load_balancing.init(nodes);

    Ok(Session {
        load_balancing: Mutex::new(load_balancing),
        event_stream: None,
        responses: Default::default(),
        compression,
    })
}

#[cfg(all(feature = "rust-tls", feature = "unstable-dynamic-cluster"))]
async fn connect_tls_dynamic<LB>(
    node_configs: &ClusterRustlsConfig,
    mut load_balancing: LB,
    compression: Compression,
    event_src: NodeTcpConfig,
) -> error::Result<Session<LB>>
where
    LB: LoadBalancingStrategy<RustlsConnectionPool>,
{
    let mut nodes: Vec<Arc<RustlsConnectionPool>> = Vec::with_capacity(node_configs.0.len());

    for node_config in &node_configs.0 {
        let node_connection_pool = new_rustls_pool(node_config.clone()).await?;
        nodes.push(Arc::new(node_connection_pool));
    }

    load_balancing.init(nodes);

    let mut session = Session {
        load_balancing: Mutex::new(load_balancing),
        event_stream: None,
        responses: Default::default(),
        compression,
    };

    let (listener, event_stream) = session
        .listen_non_blocking(
            &event_src.addr,
            event_src.authenticator.deref(),
            vec![SimpleServerEvent::StatusChange],
        )
        .await?;

    tokio::spawn(listener.start(Compression::None));

    session.event_stream = Some(Mutex::new(event_stream));

    Ok(session)
}

/// This function uses a user-supplied connection configuration to initialize all the
/// connections in the session. It can be used to supply your own transport and load
/// balancing mechanisms in order to support unusual node discovery mechanisms
/// or configuration needs.
///
/// The config object supplied differs from the ClusterTcpConfig and ClusterRustlsConfig
/// objects in that it is not expected to include an address. Instead the same configuration
/// will be applied to all connections across the cluster.
pub async fn connect<T, M, C, LB>(
    config: &C,
    initial_nodes: &[SocketAddr],
    mut load_balancing: LB,
    compression: Compression,
) -> Result<Session<LB>, C::Error>
where
    M: bb8::ManageConnection<Connection = T>,
    T: CDRSTransport<Manager = M>,
    C: ConnectionConfig<Transport = T, Manager = M>,
    LB: LoadBalancingStrategy<ConnectionPool<T>> + Sized,
{
    let mut nodes: Vec<Arc<ConnectionPool<T>>> = Vec::with_capacity(initial_nodes.len());

    for node in initial_nodes {
        let pool = config.connect(*node).await?;
        nodes.push(Arc::new(ConnectionPool::new(pool, *node)));
    }

    load_balancing.init(nodes);

    Ok(Session {
        load_balancing: Mutex::new(load_balancing),
        event_stream: None,
        responses: Default::default(),
        compression,
    })
}

async fn connect_static<LB>(
    node_configs: &ClusterTcpConfig,
    mut load_balancing: LB,
    compression: Compression,
) -> error::Result<Session<LB>>
where
    LB: LoadBalancingStrategy<TcpConnectionPool>,
{
    let mut nodes: Vec<Arc<TcpConnectionPool>> = Vec::with_capacity(node_configs.0.len());

    for node_config in &node_configs.0 {
        let node_connection_pool = new_tcp_pool(node_config.clone()).await?;
        nodes.push(Arc::new(node_connection_pool));
    }

    load_balancing.init(nodes);

    Ok(Session {
        load_balancing: Mutex::new(load_balancing),
        event_stream: None,
        responses: Default::default(),
        compression,
    })
}

#[cfg(feature = "unstable-dynamic-cluster")]
async fn connect_dynamic<LB>(
    node_configs: &ClusterTcpConfig,
    mut load_balancing: LB,
    compression: Compression,
    event_src: NodeTcpConfig,
) -> error::Result<Session<LB>>
where
    LB: LoadBalancingStrategy<TcpConnectionPool>,
{
    let mut nodes: Vec<Arc<TcpConnectionPool>> = Vec::with_capacity(node_configs.0.len());

    for node_config in &node_configs.0 {
        let node_connection_pool = new_tcp_pool(node_config.clone()).await?;
        nodes.push(Arc::new(node_connection_pool));
    }

    load_balancing.init(nodes);

    let mut session = Session {
        load_balancing: Mutex::new(load_balancing),
        event_stream: None,
        responses: Default::default(),
        compression,
    };

    let (listener, event_stream) = session
        .listen_non_blocking(
            &event_src.addr,
            event_src.authenticator.deref(),
            vec![SimpleServerEvent::StatusChange],
        )
        .await?;

    tokio::spawn(listener.start(Compression::None));

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
) -> error::Result<Session<LB>>
where
    LB: LoadBalancingStrategy<TcpConnectionPool>,
{
    connect_static(node_configs, load_balancing, Compression::None).await
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
    event_src: NodeTcpConfig,
) -> error::Result<Session<LB>>
where
    LB: LoadBalancingStrategy<TcpConnectionPool>,
{
    connect_dynamic(node_configs, load_balancing, Compression::None, event_src).await
}

/// Creates new session that will perform queries with Snappy compression. `Compression` type
/// can be changed at any time.
/// As a parameter it takes:
/// * cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
pub async fn new_snappy<LB>(
    node_configs: &ClusterTcpConfig,
    load_balancing: LB,
) -> error::Result<Session<LB>>
where
    LB: LoadBalancingStrategy<TcpConnectionPool>,
{
    connect_static(node_configs, load_balancing, Compression::Snappy).await
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
    event_src: NodeTcpConfig,
) -> error::Result<Session<LB>>
where
    LB: LoadBalancingStrategy<TcpConnectionPool>,
{
    connect_dynamic(node_configs, load_balancing, Compression::Snappy, event_src).await
}

/// Creates new session that will perform queries with LZ4 compression. `Compression` type
/// can be changed at any time.
/// As a parameter it takes:
/// * cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
pub async fn new_lz4<LB>(
    node_configs: &ClusterTcpConfig,
    load_balancing: LB,
) -> error::Result<Session<LB>>
where
    LB: LoadBalancingStrategy<TcpConnectionPool>,
{
    connect_static(node_configs, load_balancing, Compression::Lz4).await
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
    event_src: NodeTcpConfig,
) -> error::Result<Session<LB>>
where
    LB: LoadBalancingStrategy<TcpConnectionPool>,
{
    connect_dynamic(node_configs, load_balancing, Compression::Lz4, event_src).await
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
) -> error::Result<Session<LB>>
where
    LB: LoadBalancingStrategy<RustlsConnectionPool>,
{
    connect_tls_static(node_configs, load_balancing, Compression::None).await
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
    event_src: NodeTcpConfig,
) -> error::Result<Session<LB>>
where
    LB: LoadBalancingStrategy<RustlsConnectionPool>,
{
    connect_tls_dynamic(node_configs, load_balancing, Compression::None, event_src).await
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
) -> error::Result<Session<LB>>
where
    LB: LoadBalancingStrategy<RustlsConnectionPool>,
{
    connect_tls_static(node_configs, load_balancing, Compression::Snappy).await
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
    event_src: NodeTcpConfig,
) -> error::Result<Session<LB>>
where
    LB: LoadBalancingStrategy<RustlsConnectionPool>,
{
    connect_tls_dynamic(node_configs, load_balancing, Compression::Snappy, event_src).await
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
) -> error::Result<Session<LB>>
where
    LB: LoadBalancingStrategy<RustlsConnectionPool>,
{
    connect_tls_static(node_configs, load_balancing, Compression::Lz4).await
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
    event_src: NodeTcpConfig,
) -> error::Result<Session<LB>>
where
    LB: LoadBalancingStrategy<RustlsConnectionPool>,
{
    connect_tls_dynamic(node_configs, load_balancing, Compression::Lz4, event_src).await
}

impl<L> Session<L> {
    /// Returns new event listener.
    pub async fn listen<A: Authenticator + Send + Sync + ?Sized + 'static>(
        &self,
        node: &str,
        authenticator: &A,
        events: Vec<SimpleServerEvent>,
    ) -> error::Result<(Listener<Mutex<TransportTcp>>, EventStream)> {
        let compression = self.get_compressor();
        let keyspace_holder = Arc::new(KeyspaceHolder::default());
        let transport = TransportTcp::new(node, keyspace_holder.clone())
            .await
            .map(Mutex::new)?;

        startup(&transport, authenticator, keyspace_holder.deref()).await?;

        let query_frame = Frame::new_req_register(events).as_bytes();
        transport.lock().await.write(query_frame.as_slice()).await?;
        parse_frame(&transport, compression).await?;

        Ok(new_listener(transport))
    }

    pub async fn listen_non_blocking<A: Authenticator + Send + Sync + ?Sized + 'static>(
        &self,
        node: &str,
        authenticator: &A,
        events: Vec<SimpleServerEvent>,
    ) -> error::Result<(Listener<Mutex<TransportTcp>>, EventStreamNonBlocking)> {
        self.listen(node, authenticator, events).await.map(|l| {
            let (listener, stream) = l;
            (listener, stream.into())
        })
    }
}
