use futures::future::{abortable, AbortHandle};
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast::{channel, Receiver, Sender};

use crate::cluster::connection_manager::ConnectionManager;
use crate::cluster::control_connection::ControlConnection;
#[cfg(feature = "rust-tls")]
use crate::cluster::rustls_connection_manager::RustlsConnectionManager;
use crate::cluster::tcp_connection_manager::TcpConnectionManager;
#[cfg(feature = "rust-tls")]
use crate::cluster::ClusterRustlsConfig;
use crate::cluster::SessionPager;
use crate::cluster::{ClusterMetadata, ClusterMetadataManager};
use crate::cluster::{
    ClusterTcpConfig, GenericClusterConfig, GetRetryPolicy, KeyspaceHolder, Node,
};
use crate::compression::Compression;
use crate::error;
use crate::events::ServerEvent;
use crate::frame::frame_result::BodyResResultPrepared;
use crate::frame::Frame;
use crate::load_balancing::{LoadBalancingStrategy, QueryPlan, Request};
use crate::query::utils::{prepare_flags, send_frame};
use crate::query::{
    PreparedQuery, Query, QueryBatch, QueryParams, QueryParamsBuilder, QueryValues,
};
use crate::retry::{
    DefaultRetryPolicy, ExponentialReconnectionPolicy, ReconnectionPolicy, RetryPolicy,
};
#[cfg(feature = "rust-tls")]
use crate::transport::TransportRustls;
use crate::transport::{CdrsTransport, TransportTcp};

pub const DEFAULT_TRANSPORT_BUFFER_SIZE: usize = 1024;
const EVENT_CHANNEL_CAPACITY: usize = 32;

/// CDRS session that holds a pool of connections to nodes.
pub struct Session<
    T: CdrsTransport + Send + Sync + 'static,
    CM: ConnectionManager<T>,
    LB: LoadBalancingStrategy<T, CM> + Send + Sync,
> {
    load_balancing: Arc<LB>,
    keyspace_holder: Arc<KeyspaceHolder>,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,
    control_connection_handle: AbortHandle,
    event_sender: Sender<ServerEvent>,
    cluster_metadata_manager: Arc<ClusterMetadataManager<T, CM>>,
    _transport: PhantomData<T>,
    _connection_manager: PhantomData<CM>,
}

impl<
        T: CdrsTransport + Send + Sync + 'static,
        CM: ConnectionManager<T>,
        LB: LoadBalancingStrategy<T, CM> + Send + Sync,
    > Drop for Session<T, CM, LB>
{
    fn drop(&mut self) {
        self.control_connection_handle.abort();
    }
}

impl<
        T: CdrsTransport + Send + Sync + 'static,
        CM: ConnectionManager<T> + Send + Sync + 'static,
        LB: LoadBalancingStrategy<T, CM> + Send + Sync + 'static,
    > Session<T, CM, LB>
{
    /// Basing on current session returns new `SessionPager` that can be used
    /// for performing paged queries.
    pub fn paged(&self, page_size: i32) -> SessionPager<T, CM, LB> {
        SessionPager::new(self, page_size)
    }

    /// Executes given prepared query with query parameters and optional tracing, and warnings.
    pub async fn exec_with_params_tw(
        &self,
        prepared: &PreparedQuery,
        query_parameters: QueryParams,
        with_tracing: bool,
        with_warnings: bool,
    ) -> error::Result<Frame> {
        let flags = prepare_flags(with_tracing, with_warnings);
        let options_frame = Frame::new_req_execute(
            prepared
                .id
                .read()
                .expect("Cannot read prepared query id!")
                .deref(),
            &query_parameters,
            flags,
        );

        let keyspace = prepared
            .keyspace
            .as_deref()
            .or_else(|| query_parameters.keyspace.as_deref());

        let mut result = send_frame(
            self,
            options_frame,
            query_parameters.is_idempotent,
            keyspace,
        )
        .await;

        if let Err(error::Error::Server(error)) = &result {
            // if query is unprepared
            if error.error_code == 0x2500 {
                if let Ok(new) = self.prepare_raw(&prepared.query).await {
                    *prepared
                        .id
                        .write()
                        .expect("Cannot write prepared query id!") = new.id.clone();
                    let flags = prepare_flags(with_tracing, with_warnings);
                    let options_frame = Frame::new_req_execute(&new.id, &query_parameters, flags);
                    result = send_frame(
                        self,
                        options_frame,
                        query_parameters.is_idempotent,
                        keyspace,
                    )
                    .await;
                }
            }
        }
        result
    }

    /// Executes given prepared query with query parameters.
    pub async fn exec_with_params(
        &self,
        prepared: &PreparedQuery,
        query_parameters: QueryParams,
    ) -> error::Result<Frame> {
        self.exec_with_params_tw(prepared, query_parameters, false, false)
            .await
    }

    /// Executes given prepared query with query values and optional tracing, and warnings.
    pub async fn exec_with_values_tw<V: Into<QueryValues> + Sync + Send>(
        &self,
        prepared: &PreparedQuery,
        values: V,
        with_tracing: bool,
        with_warnings: bool,
    ) -> error::Result<Frame> {
        let query_params_builder = QueryParamsBuilder::new();
        let query_params = query_params_builder.values(values.into()).finalize();
        self.exec_with_params_tw(prepared, query_params, with_tracing, with_warnings)
            .await
    }

    /// Executes given prepared query with query values.
    pub async fn exec_with_values<V: Into<QueryValues> + Sync + Send>(
        &self,
        prepared: &PreparedQuery,
        values: V,
    ) -> error::Result<Frame> {
        self.exec_with_values_tw(prepared, values, false, false)
            .await
    }

    /// Executes given prepared query with optional tracing and warnings.
    pub async fn exec_tw(
        &self,
        prepared: &PreparedQuery,
        with_tracing: bool,
        with_warnings: bool,
    ) -> error::Result<Frame> {
        let query_params = QueryParamsBuilder::new().finalize();
        self.exec_with_params_tw(prepared, query_params, with_tracing, with_warnings)
            .await
    }

    /// Executes given prepared query.
    pub async fn exec(&self, prepared: &PreparedQuery) -> error::Result<Frame>
    where
        Self: Sync,
    {
        self.exec_tw(prepared, false, false).await
    }

    /// Prepares a query for execution. Along with query itself, the
    /// method takes `with_tracing` and `with_warnings` flags to get
    /// tracing information and warnings. Returns the raw prepared
    /// query result.
    pub async fn prepare_raw_tw<Q: ToString>(
        &self,
        query: Q,
        with_tracing: bool,
        with_warnings: bool,
    ) -> error::Result<BodyResResultPrepared> {
        let flags = prepare_flags(with_tracing, with_warnings);

        let query_frame = Frame::new_req_prepare(query.to_string(), flags);

        send_frame(self, query_frame, false, None)
            .await
            .and_then(|response| response.body())
            .and_then(|body| {
                body.into_prepared()
                    .ok_or_else(|| "CDRS BUG: cannot convert frame into prepared".into())
            })
    }

    /// Prepares query without additional tracing information and warnings.
    /// Returns the raw prepared query result.
    pub async fn prepare_raw<Q: ToString>(&self, query: Q) -> error::Result<BodyResResultPrepared> {
        self.prepare_raw_tw(query, false, false).await
    }

    /// Prepares a query for execution. Along with query itself,
    /// the method takes `with_tracing` and `with_warnings` flags
    /// to get tracing information and warnings. Returns the prepared
    /// query.
    pub async fn prepare_tw<Q: ToString>(
        &self,
        query: Q,
        with_tracing: bool,
        with_warnings: bool,
    ) -> error::Result<PreparedQuery> {
        let s = query.to_string();
        self.prepare_raw_tw(query, with_tracing, with_warnings)
            .await
            .map(|result| PreparedQuery {
                id: RwLock::new(result.id),
                query: s,
                keyspace: result
                    .metadata
                    .global_table_spec
                    .map(|(keyspace, _)| keyspace.as_plain()),
            })
    }

    /// It prepares query without additional tracing information and warnings.
    /// Returns the prepared query.
    pub async fn prepare<Q: ToString>(&self, query: Q) -> error::Result<PreparedQuery>
    where
        Self: Sync,
    {
        self.prepare_tw(query, false, false).await
    }

    /// Executes batch query with optional tracing and warnings.
    pub async fn batch_with_params_tw(
        &self,
        mut batch: QueryBatch,
        with_tracing: bool,
        with_warnings: bool,
    ) -> error::Result<Frame> {
        let flags = prepare_flags(with_tracing, with_warnings);
        let is_idempotent = batch.is_idempotent;
        let keyspace = batch.keyspace.take();

        let query_frame = Frame::new_req_batch(batch, flags);

        send_frame(self, query_frame, is_idempotent, keyspace.as_deref()).await
    }

    /// Executes batch query.
    pub async fn batch_with_params(&self, batch: QueryBatch) -> error::Result<Frame> {
        self.batch_with_params_tw(batch, false, false).await
    }

    /// Executes a query with parameters and ability to trace it and see warnings.
    pub async fn query_with_params_tw<Q: ToString>(
        &self,
        query: Q,
        mut query_params: QueryParams,
        with_tracing: bool,
        with_warnings: bool,
    ) -> error::Result<Frame> {
        let is_idempotent = query_params.is_idempotent;
        let keyspace = query_params.keyspace.take();

        let query = Query {
            query: query.to_string(),
            params: query_params,
        };

        let flags = prepare_flags(with_tracing, with_warnings);
        let query_frame = Frame::new_query(query, flags);

        send_frame(self, query_frame, is_idempotent, keyspace.as_deref()).await
    }

    /// Executes a query.
    pub async fn query<Q: ToString>(&self, query: Q) -> error::Result<Frame> {
        self.query_tw(query, false, false).await
    }

    /// Executes a query with ability to trace it and see warnings.
    pub async fn query_tw<Q: ToString>(
        &self,
        query: Q,
        with_tracing: bool,
        with_warnings: bool,
    ) -> error::Result<Frame> {
        let query_params = QueryParamsBuilder::new().finalize();
        self.query_with_params_tw(query, query_params, with_tracing, with_warnings)
            .await
    }

    /// Executes a query with bounded values (either with or without names).
    pub async fn query_with_values<Q: ToString, V: Into<QueryValues> + Send>(
        &self,
        query: Q,
        values: V,
    ) -> error::Result<Frame> {
        self.query_with_values_tw(query, values, false, false).await
    }

    /// Executes a query with bounded values (either with or without names)
    /// and ability to see warnings, trace a request and default parameters.
    pub async fn query_with_values_tw<Q: ToString, V: Into<QueryValues> + Send>(
        &self,
        query: Q,
        values: V,
        with_tracing: bool,
        with_warnings: bool,
    ) -> error::Result<Frame> {
        let query_params_builder = QueryParamsBuilder::new();
        let query_params = query_params_builder.values(values.into()).finalize();
        self.query_with_params_tw(query, query_params, with_tracing, with_warnings)
            .await
    }

    /// Executes a query with query params without warnings and tracing.
    pub async fn query_with_params<Q: ToString>(
        &self,
        query: Q,
        query_params: QueryParams,
    ) -> error::Result<Frame> {
        self.query_with_params_tw(query, query_params, false, false)
            .await
    }

    /// Returns currently set global keyspace.
    #[inline]
    pub fn current_keyspace(&self) -> Option<Arc<String>> {
        self.keyspace_holder.current_keyspace()
    }

    /// Returns current cluster metadata.
    #[inline]
    pub fn cluster_metadata(&self) -> Arc<ClusterMetadata<T, CM>> {
        self.cluster_metadata_manager.metadata()
    }

    /// Returns query plan for given request. If no request is given, return a generic plan for
    /// establishing connection(s) to node(s).
    #[inline]
    pub fn query_plan(&self, request: Option<Request>) -> QueryPlan<T, CM> {
        self.load_balancing
            .query_plan(request, self.cluster_metadata().as_ref())
    }

    /// Creates a new server event receiver. You can use multiple receivers at the same time.
    #[inline]
    pub fn create_event_receiver(&self) -> Receiver<ServerEvent> {
        self.event_sender.subscribe()
    }

    fn new(
        load_balancing: LB,
        keyspace_holder: Arc<KeyspaceHolder>,
        retry_policy: Box<dyn RetryPolicy + Send + Sync>,
        reconnection_policy: Arc<dyn ReconnectionPolicy + Send + Sync>,
        contact_points: Vec<Arc<Node<T, CM>>>,
    ) -> Self {
        let load_balancing = Arc::new(load_balancing);
        let (event_sender, event_receiver) = channel(EVENT_CHANNEL_CAPACITY);

        let cluster_metadata_manager =
            Arc::new(ClusterMetadataManager::new(event_receiver, contact_points));

        let control_connection = ControlConnection::new(
            load_balancing.clone(),
            reconnection_policy.clone(),
            cluster_metadata_manager.clone(),
            event_sender.clone(),
        );

        let (control_connection_future, control_connection_handle) =
            abortable(control_connection.run());

        tokio::spawn(control_connection_future);

        Session {
            load_balancing,
            keyspace_holder,
            retry_policy,
            control_connection_handle,
            event_sender,
            cluster_metadata_manager,
            _transport: Default::default(),
            _connection_manager: Default::default(),
        }
    }
}

impl<
        T: CdrsTransport + 'static,
        CM: ConnectionManager<T>,
        LB: LoadBalancingStrategy<T, CM> + Send + Sync,
    > GetRetryPolicy for Session<T, CM, LB>
{
    fn retry_policy(&self) -> &dyn RetryPolicy {
        self.retry_policy.as_ref()
    }
}

/// Workaround for <https://github.com/rust-lang/rust/issues/63033>
#[repr(transparent)]
pub struct RetryPolicyWrapper(pub Box<dyn RetryPolicy + Send + Sync>);

#[repr(transparent)]
pub struct ReconnectionPolicyWrapper(pub Arc<dyn ReconnectionPolicy + Send + Sync>);

/// This function uses a user-supplied connection configuration to initialize all the
/// connections in the session. It can be used to supply your own transport and load
/// balancing mechanisms in order to support unusual node discovery mechanisms
/// or configuration needs.
///
/// The config object supplied differs from the [`ClusterTcpConfig`] and [`ClusterRustlsConfig`]
/// objects in that it is not expected to include an address. Instead the same configuration
/// will be applied to all connections across the cluster.
pub async fn connect_generic_static<T, C, A, CM, LB>(
    config: &C,
    initial_nodes: &[A],
    load_balancing: LB,
    retry_policy: RetryPolicyWrapper,
    reconnection_policy: ReconnectionPolicyWrapper,
) -> error::Result<Session<T, CM, LB>>
where
    A: Clone,
    T: CdrsTransport + Send + Sync + 'static,
    CM: ConnectionManager<T> + Send + Sync + 'static,
    C: GenericClusterConfig<T, CM, Address = A>,
    LB: LoadBalancingStrategy<T, CM> + Sized + Send + Sync + 'static,
{
    let mut nodes = Vec::with_capacity(initial_nodes.len());

    for node in initial_nodes {
        let connection_manager = config.create_manager(node.clone()).await?;
        nodes.push(Arc::new(Node::new(connection_manager)));
    }

    Ok(Session::new(
        load_balancing,
        Default::default(),
        retry_policy.0,
        reconnection_policy.0,
        nodes,
    ))
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
    reconnection_policy: Arc<dyn ReconnectionPolicy + Send + Sync>,
) -> error::Result<Session<TransportTcp, TcpConnectionManager, LB>>
where
    LB: LoadBalancingStrategy<TransportTcp, TcpConnectionManager> + Send + Sync + 'static,
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
    reconnection_policy: Arc<dyn ReconnectionPolicy + Send + Sync>,
) -> error::Result<Session<TransportTcp, TcpConnectionManager, LB>>
where
    LB: LoadBalancingStrategy<TransportTcp, TcpConnectionManager> + Send + Sync + 'static,
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
    reconnection_policy: Arc<dyn ReconnectionPolicy + Send + Sync>,
) -> error::Result<Session<TransportTcp, TcpConnectionManager, LB>>
where
    LB: LoadBalancingStrategy<TransportTcp, TcpConnectionManager> + Send + Sync + 'static,
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
    reconnection_policy: Arc<dyn ReconnectionPolicy + Send + Sync>,
) -> error::Result<Session<TransportRustls, RustlsConnectionManager, LB>>
where
    LB: LoadBalancingStrategy<TransportRustls, RustlsConnectionManager> + Send + Sync + 'static,
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
    reconnection_policy: Arc<dyn ReconnectionPolicy + Send + Sync>,
) -> error::Result<Session<TransportRustls, RustlsConnectionManager, LB>>
where
    LB: LoadBalancingStrategy<TransportRustls, RustlsConnectionManager> + Send + Sync + 'static,
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
    reconnection_policy: Arc<dyn ReconnectionPolicy + Send + Sync>,
) -> error::Result<Session<TransportRustls, RustlsConnectionManager, LB>>
where
    LB: LoadBalancingStrategy<TransportRustls, RustlsConnectionManager> + Send + Sync + 'static,
{
    Ok(
        RustlsSessionBuilder::new(load_balancing, node_configs.clone())
            .with_compression(Compression::Lz4)
            .with_retry_policy(retry_policy)
            .with_reconnection_policy(reconnection_policy)
            .build(),
    )
}

struct SessionConfig<
    T: CdrsTransport,
    CM: ConnectionManager<T>,
    LB: LoadBalancingStrategy<T, CM> + Send + Sync,
> {
    compression: Compression,
    transport_buffer_size: usize,
    tcp_nodelay: bool,
    load_balancing: LB,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,
    reconnection_policy: Arc<dyn ReconnectionPolicy + Send + Sync>,
    _connection_manager: PhantomData<CM>,
    _transport: PhantomData<T>,
}

impl<
        T: CdrsTransport,
        CM: ConnectionManager<T>,
        LB: LoadBalancingStrategy<T, CM> + Send + Sync,
    > SessionConfig<T, CM, LB>
{
    fn new(
        compression: Compression,
        transport_buffer_size: usize,
        tcp_nodelay: bool,
        load_balancing: LB,
        retry_policy: Box<dyn RetryPolicy + Send + Sync>,
        reconnection_policy: Arc<dyn ReconnectionPolicy + Send + Sync>,
    ) -> Self {
        SessionConfig {
            compression,
            transport_buffer_size,
            tcp_nodelay,
            load_balancing,
            retry_policy,
            reconnection_policy,
            _connection_manager: Default::default(),
            _transport: Default::default(),
        }
    }
}

/// Builder for easy `Session` creation. Requires static `LoadBalancingStrategy`, but otherwise, other
/// configuration parameters can be dynamically set. Use concrete implementers to create specific
/// sessions.
pub trait SessionBuilder<
    T: CdrsTransport + Send + Sync + 'static,
    CM: ConnectionManager<T>,
    LB: LoadBalancingStrategy<T, CM> + Send + Sync + 'static,
>
{
    /// Sets new compression.
    fn with_compression(self, compression: Compression) -> Self;

    /// Set new retry policy.
    fn with_retry_policy(self, retry_policy: Box<dyn RetryPolicy + Send + Sync>) -> Self;

    /// Set new reconnection policy.
    fn with_reconnection_policy(
        self,
        reconnection_policy: Arc<dyn ReconnectionPolicy + Send + Sync>,
    ) -> Self;

    /// Sets new transport buffer size. High values are recommended with large amounts of in flight
    /// queries.
    fn with_transport_buffer_size(self, transport_buffer_size: usize) -> Self;

    /// Sets NODELAY for given session connections.
    fn with_tcp_nodelay(self, tcp_nodelay: bool) -> Self;

    /// Builds the resulting session.
    fn build(self) -> Session<T, CM, LB>;
}

/// Builder for non-TLS sessions.
pub struct TcpSessionBuilder<
    LB: LoadBalancingStrategy<TransportTcp, TcpConnectionManager> + Send + Sync,
> {
    config: SessionConfig<TransportTcp, TcpConnectionManager, LB>,
    node_configs: ClusterTcpConfig,
}

impl<LB: LoadBalancingStrategy<TransportTcp, TcpConnectionManager> + Send + Sync>
    TcpSessionBuilder<LB>
{
    /// Creates a new builder with default session configuration.
    pub fn new(load_balancing: LB, node_configs: ClusterTcpConfig) -> Self {
        TcpSessionBuilder {
            config: SessionConfig::new(
                Compression::None,
                DEFAULT_TRANSPORT_BUFFER_SIZE,
                true,
                load_balancing,
                Box::new(DefaultRetryPolicy::default()),
                Arc::new(ExponentialReconnectionPolicy::default()),
            ),
            node_configs,
        }
    }
}

impl<LB: LoadBalancingStrategy<TransportTcp, TcpConnectionManager> + Send + Sync + 'static>
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
        reconnection_policy: Arc<dyn ReconnectionPolicy + Send + Sync>,
    ) -> Self {
        self.config.reconnection_policy = reconnection_policy;
        self
    }

    fn with_transport_buffer_size(mut self, transport_buffer_size: usize) -> Self {
        self.config.transport_buffer_size = transport_buffer_size;
        self
    }

    fn with_tcp_nodelay(mut self, tcp_nodelay: bool) -> Self {
        self.config.tcp_nodelay = tcp_nodelay;
        self
    }

    fn build(self) -> Session<TransportTcp, TcpConnectionManager, LB> {
        let keyspace_holder = Arc::new(KeyspaceHolder::default());
        let mut nodes = Vec::with_capacity(self.node_configs.0.len());

        for node_config in self.node_configs.0 {
            let connection_manager = TcpConnectionManager::new(
                node_config,
                keyspace_holder.clone(),
                self.config.reconnection_policy.clone(),
                self.config.compression,
                self.config.transport_buffer_size,
                self.config.tcp_nodelay,
            );
            nodes.push(Arc::new(Node::new(connection_manager)));
        }

        Session::new(
            self.config.load_balancing,
            keyspace_holder,
            self.config.retry_policy,
            self.config.reconnection_policy,
            nodes,
        )
    }
}

#[cfg(feature = "rust-tls")]
/// Builder for TLS sessions.
pub struct RustlsSessionBuilder<
    LB: LoadBalancingStrategy<TransportRustls, RustlsConnectionManager> + Send + Sync,
> {
    config: SessionConfig<TransportRustls, RustlsConnectionManager, LB>,
    node_configs: ClusterRustlsConfig,
}

#[cfg(feature = "rust-tls")]
impl<LB: LoadBalancingStrategy<TransportRustls, RustlsConnectionManager> + Send + Sync>
    RustlsSessionBuilder<LB>
{
    /// Creates a new builder with default session configuration.
    pub fn new(load_balancing: LB, node_configs: ClusterRustlsConfig) -> Self {
        RustlsSessionBuilder {
            config: SessionConfig::new(
                Compression::None,
                DEFAULT_TRANSPORT_BUFFER_SIZE,
                true,
                load_balancing,
                Box::new(DefaultRetryPolicy::default()),
                Arc::new(ExponentialReconnectionPolicy::default()),
            ),
            node_configs,
        }
    }
}

#[cfg(feature = "rust-tls")]
impl<
        LB: LoadBalancingStrategy<TransportRustls, RustlsConnectionManager> + Send + Sync + 'static,
    > SessionBuilder<TransportRustls, RustlsConnectionManager, LB> for RustlsSessionBuilder<LB>
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
        reconnection_policy: Arc<dyn ReconnectionPolicy + Send + Sync>,
    ) -> Self {
        self.config.reconnection_policy = reconnection_policy;
        self
    }

    fn with_transport_buffer_size(mut self, transport_buffer_size: usize) -> Self {
        self.config.transport_buffer_size = transport_buffer_size;
        self
    }

    fn with_tcp_nodelay(mut self, tcp_nodelay: bool) -> Self {
        self.config.tcp_nodelay = tcp_nodelay;
        self
    }

    fn build(self) -> Session<TransportRustls, RustlsConnectionManager, LB> {
        let keyspace_holder = Arc::new(KeyspaceHolder::default());
        let mut nodes = Vec::with_capacity(self.node_configs.0.len());

        for node_config in self.node_configs.0 {
            let connection_manager = RustlsConnectionManager::new(
                node_config,
                keyspace_holder.clone(),
                self.config.reconnection_policy.clone(),
                self.config.compression,
                self.config.transport_buffer_size,
                self.config.tcp_nodelay,
            );
            nodes.push(Arc::new(Node::new(connection_manager)));
        }

        Session::new(
            self.config.load_balancing,
            keyspace_holder,
            self.config.retry_policy,
            self.config.reconnection_policy,
            nodes,
        )
    }
}
