use cassandra_protocol::compression::Compression;
use cassandra_protocol::consistency::Consistency;
use cassandra_protocol::error;
use cassandra_protocol::events::ServerEvent;
use cassandra_protocol::frame::frame_result::{BodyResResultPrepared, TableSpec};
use cassandra_protocol::frame::{Frame, Serialize, Version};
use cassandra_protocol::query::utils::prepare_flags;
use cassandra_protocol::query::{PreparedQuery, Query, QueryBatch, QueryValues};
use cassandra_protocol::token::Murmur3Token;
use cassandra_protocol::types::value::Value;
use cassandra_protocol::types::{CIntShort, SHORT_LEN};
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use itertools::Itertools;
use lazy_static::lazy_static;
use std::io::{Cursor, Write};
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast::{channel, Receiver, Sender};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio::{pin, select};
use tracing::*;

use crate::cluster::connection_manager::ConnectionManager;
use crate::cluster::connection_pool::{ConnectionPoolConfig, ConnectionPoolFactory};
use crate::cluster::control_connection::ControlConnection;
#[cfg(feature = "rust-tls")]
use crate::cluster::rustls_connection_manager::RustlsConnectionManager;
use crate::cluster::send_frame::send_frame;
use crate::cluster::tcp_connection_manager::TcpConnectionManager;
use crate::cluster::topology::{Node, NodeDistance, NodeState};
#[cfg(feature = "rust-tls")]
use crate::cluster::NodeRustlsConfig;
use crate::cluster::{ClusterMetadata, ClusterMetadataManager, SessionContext};
use crate::cluster::{GenericClusterConfig, KeyspaceHolder};
use crate::cluster::{NodeTcpConfig, SessionPager};
use crate::load_balancing::node_distance_evaluator::AllLocalNodeDistanceEvaluator;
use crate::load_balancing::node_distance_evaluator::NodeDistanceEvaluator;
use crate::load_balancing::{
    InitializingWrapperLoadBalancingStrategy, LoadBalancingStrategy, QueryPlan, Request,
};
use crate::retry::{
    DefaultRetryPolicy, ExponentialReconnectionPolicy, ReconnectionPolicy, RetryPolicy,
};
use crate::speculative_execution::{Context, SpeculativeExecutionPolicy};
use crate::statement::{StatementParams, StatementParamsBuilder};
#[cfg(feature = "rust-tls")]
use crate::transport::TransportRustls;
use crate::transport::{CdrsTransport, TransportTcp};

pub const DEFAULT_TRANSPORT_BUFFER_SIZE: usize = 1024;
const DEFAULT_EVENT_CHANNEL_CAPACITY: usize = 128;

lazy_static! {
    static ref DEFAULT_STATEMET_PARAMETERS: StatementParams = Default::default();
}

fn create_keyspace_holder() -> (Arc<KeyspaceHolder>, watch::Receiver<Option<String>>) {
    let (keyspace_sender, keyspace_receiver) = watch::channel(None);
    (
        Arc::new(KeyspaceHolder::new(keyspace_sender)),
        keyspace_receiver,
    )
}

// https://github.com/apache/cassandra/blob/3a950b45c321e051a9744721408760c568c05617/src/java/org/apache/cassandra/db/marshal/CompositeType.java#L39
fn serialize_routing_value(cursor: &mut Cursor<&mut Vec<u8>>, value: &Value) {
    let temp_size: CIntShort = 0;
    temp_size.serialize(cursor);

    let before_value_pos = cursor.position();
    value.serialize(cursor);

    let after_value_pos = cursor.position();
    cursor.set_position(before_value_pos - SHORT_LEN as u64);

    let value_size: CIntShort = (after_value_pos - before_value_pos) as CIntShort;
    value_size.serialize(cursor);

    cursor.set_position(after_value_pos);
    let _ = cursor.write(&[0]);
}

fn serialize_routing_key_with_indexes(values: &[Value], pk_indexes: &[i16]) -> Option<Vec<u8>> {
    match pk_indexes.len() {
        0 => None,
        1 => values
            .get(pk_indexes[0] as usize)
            .map(|value| value.serialize_to_vec()),
        _ => {
            let mut buf = vec![];
            if pk_indexes
                .iter()
                .map(|index| values.get(*index as usize))
                .fold_options(Cursor::new(&mut buf), |mut cursor, value| {
                    serialize_routing_value(&mut cursor, value);
                    cursor
                })
                .is_some()
            {
                Some(buf)
            } else {
                None
            }
        }
    }
}

fn serialize_routing_key(values: &[Value]) -> Vec<u8> {
    match values.len() {
        0 => vec![],
        1 => values[0].serialize_to_vec(),
        _ => {
            let mut buf = vec![];
            let mut cursor = Cursor::new(&mut buf);

            for value in values {
                serialize_routing_value(&mut cursor, value);
            }

            buf
        }
    }
}

/// CDRS session that holds a pool of connections to nodes and provides an interface for
/// interacting with the cluster.
pub struct Session<
    T: CdrsTransport + 'static,
    CM: ConnectionManager<T> + 'static,
    LB: LoadBalancingStrategy<T, CM> + Send + Sync,
> {
    load_balancing: Arc<InitializingWrapperLoadBalancingStrategy<T, CM, LB>>,
    keyspace_holder: Arc<KeyspaceHolder>,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,
    speculative_execution_policy: Option<Box<dyn SpeculativeExecutionPolicy + Send + Sync>>,
    control_connection_handle: JoinHandle<()>,
    event_sender: Sender<ServerEvent>,
    cluster_metadata_manager: Arc<ClusterMetadataManager<T, CM>>,
    _transport: PhantomData<T>,
    _connection_manager: PhantomData<CM>,
    version: Version,
}

impl<
        T: CdrsTransport + 'static,
        CM: ConnectionManager<T>,
        LB: LoadBalancingStrategy<T, CM> + Send + Sync,
    > Drop for Session<T, CM, LB>
{
    fn drop(&mut self) {
        self.control_connection_handle.abort();
    }
}

impl<
        T: CdrsTransport + 'static,
        CM: ConnectionManager<T> + Send + Sync + 'static,
        LB: LoadBalancingStrategy<T, CM> + Send + Sync + 'static,
    > Session<T, CM, LB>
{
    /// Returns new `SessionPager` that can be used for performing paged queries.
    pub fn paged(&self, page_size: i32) -> SessionPager<T, CM, LB> {
        SessionPager::new(self, page_size)
    }

    /// Executes given prepared query with query parameters.
    pub async fn exec_with_params(
        &self,
        prepared: &PreparedQuery,
        parameters: &StatementParams,
    ) -> error::Result<Frame> {
        let consistency = parameters.query_params.consistency;
        let flags = prepare_flags(parameters.tracing, parameters.warnings);
        let options_frame =
            Frame::new_req_execute(&prepared.id, &parameters.query_params, flags, self.version);

        let keyspace = prepared
            .keyspace
            .as_deref()
            .or_else(|| parameters.keyspace.as_deref());

        let routing_key = parameters
            .query_params
            .values
            .as_ref()
            .and_then(|values| match values {
                QueryValues::SimpleValues(values) => {
                    serialize_routing_key_with_indexes(values, &prepared.pk_indexes).or_else(|| {
                        parameters
                            .routing_key
                            .as_ref()
                            .map(|values| serialize_routing_key(values))
                    })
                }
                QueryValues::NamedValues(_) => None,
            });

        let mut result = self
            .send_frame(
                options_frame,
                parameters.is_idempotent,
                keyspace,
                parameters.token,
                routing_key.as_deref(),
                Some(consistency),
                parameters.speculative_execution_policy.as_ref(),
                parameters.retry_policy.as_ref(),
            )
            .await;

        if let Err(error::Error::Server(error)) = &result {
            // if query is unprepared
            if error.error_code == 0x2500 {
                debug!("Re-preparing statement.");
                if let Ok(new) = self.prepare_raw(&prepared.query).await {
                    // re-prepare the statement and check the resulting id - it should remain the
                    // same as the old one, except when schema changed in the meantime, in which
                    // case, the client should have the knowledge how to handle it
                    // see: https://issues.apache.org/jira/browse/CASSANDRA-10786

                    if prepared.id != new.id {
                        return Err("Re-preparing an unprepared statement resulted in a different id - probably schema changed on the server.".into());
                    }

                    let flags = prepare_flags(parameters.tracing, parameters.warnings);
                    let options_frame = Frame::new_req_execute(
                        &new.id,
                        &parameters.query_params,
                        flags,
                        self.version,
                    );
                    result = self
                        .send_frame(
                            options_frame,
                            parameters.is_idempotent,
                            keyspace,
                            parameters.token,
                            routing_key.as_deref(),
                            Some(consistency),
                            parameters.speculative_execution_policy.as_ref(),
                            parameters.retry_policy.as_ref(),
                        )
                        .await;
                }
            }
        }
        result
    }

    /// Executes given prepared query with query values.
    pub async fn exec_with_values<V: Into<QueryValues>>(
        &self,
        prepared: &PreparedQuery,
        values: V,
    ) -> error::Result<Frame> {
        self.exec_with_params(
            prepared,
            &StatementParamsBuilder::new()
                .with_values(values.into())
                .build(),
        )
        .await
    }

    /// Executes given prepared query.
    #[inline]
    pub async fn exec(&self, prepared: &PreparedQuery) -> error::Result<Frame> {
        self.exec_with_params(prepared, &DEFAULT_STATEMET_PARAMETERS)
            .await
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

        let query_frame = Frame::new_req_prepare(query.to_string(), flags, self.version);

        self.send_frame(query_frame, false, None, None, None, None, None, None)
            .await
            .and_then(|response| response.response_body())
            .and_then(|body| {
                body.into_prepared()
                    .ok_or_else(|| "CDRS BUG: cannot convert frame into prepared".into())
            })
    }

    /// Prepares query without additional tracing information and warnings.
    /// Returns the raw prepared query result.
    #[inline]
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
                id: result.id,
                query: s,
                keyspace: result
                    .metadata
                    .global_table_spec
                    .map(|TableSpec { ks_name, .. }| ks_name),
                pk_indexes: result.metadata.pk_indexes,
            })
    }

    /// It prepares query without additional tracing information and warnings.
    /// Returns the prepared query.
    #[inline]
    pub async fn prepare<Q: ToString>(&self, query: Q) -> error::Result<PreparedQuery> {
        self.prepare_tw(query, false, false).await
    }

    /// Executes batch query.
    #[inline]
    pub async fn batch(&self, batch: QueryBatch) -> error::Result<Frame> {
        self.batch_with_params(batch, &DEFAULT_STATEMET_PARAMETERS)
            .await
    }

    /// Executes batch query with parameters.
    pub async fn batch_with_params(
        &self,
        batch: QueryBatch,
        parameters: &StatementParams,
    ) -> error::Result<Frame> {
        let flags = prepare_flags(parameters.tracing, parameters.warnings);
        let consistency = batch.consistency;

        let query_frame = Frame::new_req_batch(batch, flags, self.version);

        self.send_frame(
            query_frame,
            parameters.is_idempotent,
            parameters.keyspace.as_deref(),
            None,
            None,
            Some(consistency),
            parameters.speculative_execution_policy.as_ref(),
            parameters.retry_policy.as_ref(),
        )
        .await
    }

    /// Executes a query.
    #[inline]
    pub async fn query<Q: ToString>(&self, query: Q) -> error::Result<Frame> {
        self.query_with_params(query, DEFAULT_STATEMET_PARAMETERS.clone())
            .await
    }

    /// Executes a query with bounded values (either with or without names).
    #[inline]
    pub async fn query_with_values<Q: ToString, V: Into<QueryValues>>(
        &self,
        query: Q,
        values: V,
    ) -> error::Result<Frame> {
        self.query_with_params(
            query,
            StatementParamsBuilder::new()
                .with_values(values.into())
                .build(),
        )
        .await
    }

    /// Executes a query with query parameters.
    pub async fn query_with_params<Q: ToString>(
        &self,
        query: Q,
        parameters: StatementParams,
    ) -> error::Result<Frame> {
        let is_idempotent = parameters.is_idempotent;
        let consistency = parameters.query_params.consistency;
        let keyspace = parameters.keyspace;
        let token = parameters.token;
        let routing_key = parameters
            .routing_key
            .as_ref()
            .map(|values| serialize_routing_key(values));

        let query = Query {
            query: query.to_string(),
            params: parameters.query_params,
        };

        let flags = prepare_flags(parameters.tracing, parameters.warnings);
        let query_frame = Frame::new_query(query, flags, self.version);

        self.send_frame(
            query_frame,
            is_idempotent,
            keyspace.as_deref(),
            token,
            routing_key.as_deref(),
            Some(consistency),
            parameters.speculative_execution_policy.as_ref(),
            parameters.retry_policy.as_ref(),
        )
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

    /// Returns current retry policy.
    #[inline]
    pub fn retry_policy(&self) -> &dyn RetryPolicy {
        self.retry_policy.as_ref()
    }

    #[allow(clippy::too_many_arguments)]
    async fn send_frame(
        &self,
        frame: Frame,
        is_idempotent: bool,
        keyspace: Option<&str>,
        token: Option<Murmur3Token>,
        routing_key: Option<&[u8]>,
        consistency: Option<Consistency>,
        speculative_execution_policy: Option<&Arc<dyn SpeculativeExecutionPolicy + Send + Sync>>,
        retry_policy: Option<&Arc<dyn RetryPolicy + Send + Sync>>,
    ) -> error::Result<Frame> {
        let current_keyspace = self.current_keyspace();
        let request = Request::new(
            keyspace.or_else(|| current_keyspace.as_ref().map(|keyspace| &***keyspace)),
            token,
            routing_key,
            consistency,
        );

        let query_plan = self.query_plan(Some(request));

        struct SharedQueryPlan<
            T: CdrsTransport + 'static,
            CM: ConnectionManager<T> + 'static,
            I: Iterator<Item = Arc<Node<T, CM>>>,
        > {
            current_node: Mutex<I>,
        }

        impl<
                T: CdrsTransport + 'static,
                CM: ConnectionManager<T> + 'static,
                I: Iterator<Item = Arc<Node<T, CM>>>,
            > SharedQueryPlan<T, CM, I>
        {
            fn new(current_node: I) -> Self {
                SharedQueryPlan {
                    current_node: Mutex::new(current_node),
                }
            }
        }

        impl<
                T: CdrsTransport + 'static,
                CM: ConnectionManager<T> + 'static,
                I: Iterator<Item = Arc<Node<T, CM>>>,
            > Iterator for &SharedQueryPlan<T, CM, I>
        {
            type Item = Arc<Node<T, CM>>;

            fn next(&mut self) -> Option<Self::Item> {
                self.current_node.lock().unwrap().next()
            }
        }

        let speculative_execution_policy = speculative_execution_policy
            .map(|speculative_execution_policy| speculative_execution_policy.as_ref())
            .or_else(|| self.speculative_execution_policy.as_deref());

        let retry_policy = retry_policy
            .map(|retry_policy| retry_policy.as_ref())
            .unwrap_or_else(|| self.retry_policy.as_ref());

        match speculative_execution_policy {
            Some(speculative_execution_policy) if is_idempotent => {
                let shared_query_plan = SharedQueryPlan::new(query_plan.into_iter());

                let mut context = Context::new(1);
                let mut async_tasks = FuturesUnordered::new();
                async_tasks.push(send_frame(
                    &shared_query_plan,
                    &frame,
                    is_idempotent,
                    retry_policy.new_session(),
                ));

                let sleep_fut = sleep(
                    speculative_execution_policy
                        .execution_interval(&context)
                        .unwrap_or_default(),
                )
                .fuse();

                pin!(sleep_fut);

                let mut last_error = None;

                loop {
                    select! {
                        _ = &mut sleep_fut => {
                            if let Some(interval) =
                                speculative_execution_policy.execution_interval(&context)
                            {
                                context.running_executions += 1;
                                async_tasks.push(send_frame(
                                    &shared_query_plan,
                                    &frame,
                                    is_idempotent,
                                    retry_policy.new_session(),
                                ));

                                sleep_fut.set(sleep(interval).fuse());
                            }
                        }
                        result = async_tasks.select_next_some() => {
                            match result {
                                Some(result) => {
                                    match result {
                                        Err(error::Error::Io(_)) | Err(error::Error::Timeout(_)) => {
                                            last_error = Some(result);
                                        },
                                        _ => return result,
                                    }
                                }
                                None => {
                                    if async_tasks.is_empty() {
                                        // at this point, we exhausted all available nodes and
                                        // there's no request in flight, which can potentially
                                        // reach a node
                                        return last_error.unwrap_or_else(|| Err("No nodes available in query plan!".into()));
                                    }
                                }
                            }
                        }
                    }
                }
            }
            _ => send_frame(
                query_plan.into_iter(),
                &frame,
                is_idempotent,
                retry_policy.new_session(),
            )
            .await
            .unwrap_or_else(|| Err("No nodes available in query plan!".into())),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn new(
        load_balancing: LB,
        keyspace_holder: Arc<KeyspaceHolder>,
        keyspace_receiver: watch::Receiver<Option<String>>,
        retry_policy: Box<dyn RetryPolicy + Send + Sync>,
        reconnection_policy: Arc<dyn ReconnectionPolicy + Send + Sync>,
        node_distance_evaluator: Box<dyn NodeDistanceEvaluator + Send + Sync>,
        speculative_execution_policy: Option<Box<dyn SpeculativeExecutionPolicy + Send + Sync>>,
        contact_points: Vec<SocketAddr>,
        connection_manager: CM,
        event_channel_capacity: usize,
        version: Version,
        connection_pool_config: ConnectionPoolConfig,
    ) -> Self {
        let connection_pool_factory = Arc::new(ConnectionPoolFactory::new(
            connection_pool_config,
            version,
            connection_manager,
            keyspace_receiver,
        ));

        let contact_points = contact_points
            .into_iter()
            .map(|contact_point| {
                Arc::new(Node::new_with_state(
                    connection_pool_factory.clone(),
                    contact_point,
                    None,
                    None,
                    // assume contact points are local until refresh
                    Some(NodeDistance::Local),
                    NodeState::Up,
                    Default::default(),
                    // as with distance, rack/dc is unknown until refresh
                    "".into(),
                    "".into(),
                ))
            })
            .collect_vec();

        let load_balancing = Arc::new(InitializingWrapperLoadBalancingStrategy::new(
            load_balancing,
            contact_points.clone(),
        ));

        let (event_sender, event_receiver) = channel(event_channel_capacity);

        let session_context = Arc::new(SessionContext::default());

        let cluster_metadata_manager = Arc::new(ClusterMetadataManager::new(
            contact_points.clone(),
            connection_pool_factory,
            session_context.clone(),
            node_distance_evaluator,
            version,
        ));

        cluster_metadata_manager
            .clone()
            .listen_to_events(event_receiver);

        let control_connection = ControlConnection::new(
            load_balancing.clone(),
            contact_points,
            reconnection_policy.clone(),
            cluster_metadata_manager.clone(),
            event_sender.clone(),
            session_context,
            version,
        );

        let control_connection_handle = tokio::spawn(control_connection.run());

        Session {
            load_balancing,
            keyspace_holder,
            retry_policy,
            speculative_execution_policy,
            control_connection_handle,
            event_sender,
            cluster_metadata_manager,
            _transport: Default::default(),
            _connection_manager: Default::default(),
            version,
        }
    }
}

/// Workaround for <https://github.com/rust-lang/rust/issues/63033>
#[repr(transparent)]
pub struct RetryPolicyWrapper(pub Box<dyn RetryPolicy + Send + Sync>);

/// Workaround for <https://github.com/rust-lang/rust/issues/63033>
#[repr(transparent)]
pub struct ReconnectionPolicyWrapper(pub Arc<dyn ReconnectionPolicy + Send + Sync>);

/// Workaround for <https://github.com/rust-lang/rust/issues/63033>
#[repr(transparent)]
pub struct NodeDistanceEvaluatorWrapper(pub Box<dyn NodeDistanceEvaluator + Send + Sync>);

/// Workaround for <https://github.com/rust-lang/rust/issues/63033>
#[repr(transparent)]
pub struct SpeculativeExecutionPolicyWrapper(pub Box<dyn SpeculativeExecutionPolicy + Send + Sync>);

/// This function uses a user-supplied connection configuration to initialize all the
/// connections in the session. It can be used to supply your own transport and load
/// balancing mechanisms in order to support unusual node discovery mechanisms
/// or configuration needs.
///
/// The config object supplied differs from the [`NodeTcpConfig`] and [`NodeRustlsConfig`]
/// objects in that it is not expected to include an address. Instead the same configuration
/// will be applied to all connections across the cluster.
pub async fn connect_generic<T, C, A, CM, LB>(
    config: &C,
    initial_nodes: A,
    load_balancing: LB,
    retry_policy: RetryPolicyWrapper,
    reconnection_policy: ReconnectionPolicyWrapper,
    node_distance_evaluator: NodeDistanceEvaluatorWrapper,
    speculative_execution_policy: Option<SpeculativeExecutionPolicyWrapper>,
) -> error::Result<Session<T, CM, LB>>
where
    A: IntoIterator<Item = SocketAddr>,
    T: CdrsTransport + 'static,
    CM: ConnectionManager<T> + Send + Sync + 'static,
    C: GenericClusterConfig<T, CM>,
    LB: LoadBalancingStrategy<T, CM> + Sized + Send + Sync + 'static,
{
    let (keyspace_holder, keyspace_receiver) = create_keyspace_holder();
    let connection_manager = config.create_manager(keyspace_holder.clone()).await?;
    Ok(Session::new(
        load_balancing,
        keyspace_holder,
        keyspace_receiver,
        retry_policy.0,
        reconnection_policy.0,
        node_distance_evaluator.0,
        speculative_execution_policy.map(|policy| policy.0),
        initial_nodes.into_iter().collect(),
        connection_manager,
        config.event_channel_capacity(),
        config.version(),
        config.connection_pool_config(),
    ))
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
    node_distance_evaluator: Box<dyn NodeDistanceEvaluator + Send + Sync>,
    speculative_execution_policy: Option<Box<dyn SpeculativeExecutionPolicy + Send + Sync>>,
    event_channel_capacity: usize,
    connection_pool_config: ConnectionPoolConfig,
    keyspace: Option<String>,
    _connection_manager: PhantomData<CM>,
    _transport: PhantomData<T>,
}

impl<
        T: CdrsTransport,
        CM: ConnectionManager<T>,
        LB: LoadBalancingStrategy<T, CM> + Send + Sync + 'static,
    > SessionConfig<T, CM, LB>
{
    fn new(load_balancing: LB) -> Self {
        SessionConfig {
            compression: Compression::None,
            transport_buffer_size: DEFAULT_TRANSPORT_BUFFER_SIZE,
            tcp_nodelay: true,
            load_balancing,
            retry_policy: Box::new(DefaultRetryPolicy::default()),
            reconnection_policy: Arc::new(ExponentialReconnectionPolicy::default()),
            node_distance_evaluator: Box::new(AllLocalNodeDistanceEvaluator::default()),
            speculative_execution_policy: None,
            event_channel_capacity: DEFAULT_EVENT_CHANNEL_CAPACITY,
            connection_pool_config: Default::default(),
            keyspace: None,
            _connection_manager: Default::default(),
            _transport: Default::default(),
        }
    }

    fn into_session(
        self,
        keyspace_holder: Arc<KeyspaceHolder>,
        keyspace_receiver: watch::Receiver<Option<String>>,
        contact_points: Vec<SocketAddr>,
        connection_manager: CM,
        version: Version,
    ) -> Session<T, CM, LB> {
        if let Some(keyspace) = self.keyspace {
            keyspace_holder.update_current_keyspace_without_notification(keyspace);
        }

        Session::new(
            self.load_balancing,
            keyspace_holder,
            keyspace_receiver,
            self.retry_policy,
            self.reconnection_policy,
            self.node_distance_evaluator,
            self.speculative_execution_policy,
            contact_points,
            connection_manager,
            self.event_channel_capacity,
            version,
            self.connection_pool_config,
        )
    }
}

/// Builder for easy `Session` creation. Requires static `LoadBalancingStrategy`, but otherwise, other
/// configuration parameters can be dynamically set. Use concrete implementers to create specific
/// sessions.
pub trait SessionBuilder<
    T: CdrsTransport + 'static,
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

    /// Sets new node distance evaluator. Computing node distance is fundamental to proper
    /// topology-aware load balancing - see [`NodeDistanceEvaluator`].
    fn with_node_distance_evaluator(
        self,
        node_distance_evaluator: Box<dyn NodeDistanceEvaluator + Send + Sync>,
    ) -> Self;

    /// Sets new speculative execution policy.
    fn with_speculative_execution_policy(
        self,
        speculative_execution_policy: Box<dyn SpeculativeExecutionPolicy + Send + Sync>,
    ) -> Self;

    /// Sets new transport buffer size. High values are recommended with large amounts of in flight
    /// queries.
    fn with_transport_buffer_size(self, transport_buffer_size: usize) -> Self;

    /// Sets NODELAY for given session connections.
    fn with_tcp_nodelay(self, tcp_nodelay: bool) -> Self;

    /// Sets event channel capacity. If the driver receives more server events than the capacity,
    /// some events might get dropped. This can result in the driver operating in a sub-optimal way.
    fn with_event_channel_capacity(self, event_channel_capacity: usize) -> Self;

    /// Sets node connection pool configuration for given session.
    fn with_connection_pool_config(self, connection_pool_config: ConnectionPoolConfig) -> Self;

    /// Sets the keyspace to use. If not using a keyspace explicitly in queries, one should be set
    /// either by calling this function, or by a `USE` statement. Due to the asynchronous nature of
    /// the driver and the usage of connection pools, the effect of switching current keyspace via
    /// `USE` might not propagate immediately to all active connections, resulting in queries
    /// using a wrong keyspace. If one is known upfront, it's safer to set it while building
    /// the [`Session`].
    fn with_keyspace(self, keyspace: String) -> Self;

    /// Builds the resulting session.
    fn build(self) -> Session<T, CM, LB>;
}

/// Builder for non-TLS sessions.
pub struct TcpSessionBuilder<
    LB: LoadBalancingStrategy<TransportTcp, TcpConnectionManager> + Send + Sync,
> {
    config: SessionConfig<TransportTcp, TcpConnectionManager, LB>,
    node_config: NodeTcpConfig,
}

impl<LB: LoadBalancingStrategy<TransportTcp, TcpConnectionManager> + Send + Sync + 'static>
    TcpSessionBuilder<LB>
{
    //noinspection DuplicatedCode
    /// Creates a new builder with default session configuration.
    pub fn new(load_balancing: LB, node_config: NodeTcpConfig) -> Self {
        TcpSessionBuilder {
            config: SessionConfig::new(load_balancing),
            node_config,
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

    fn with_node_distance_evaluator(
        mut self,
        node_distance_evaluator: Box<dyn NodeDistanceEvaluator + Send + Sync>,
    ) -> Self {
        self.config.node_distance_evaluator = node_distance_evaluator;
        self
    }

    fn with_speculative_execution_policy(
        mut self,
        speculative_execution_policy: Box<dyn SpeculativeExecutionPolicy + Send + Sync>,
    ) -> Self {
        self.config.speculative_execution_policy = Some(speculative_execution_policy);
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

    fn with_event_channel_capacity(mut self, event_channel_capacity: usize) -> Self {
        self.config.event_channel_capacity = event_channel_capacity;
        self
    }

    fn with_connection_pool_config(mut self, connection_pool_config: ConnectionPoolConfig) -> Self {
        self.config.connection_pool_config = connection_pool_config;
        self
    }

    fn with_keyspace(mut self, keyspace: String) -> Self {
        self.config.keyspace = Some(keyspace);
        self
    }

    fn build(self) -> Session<TransportTcp, TcpConnectionManager, LB> {
        let (keyspace_holder, keyspace_receiver) = create_keyspace_holder();
        let connection_manager = TcpConnectionManager::new(
            self.node_config.authenticator_provider,
            keyspace_holder.clone(),
            self.config.reconnection_policy.clone(),
            self.config.compression,
            self.config.transport_buffer_size,
            self.config.tcp_nodelay,
            self.node_config.version,
        );

        self.config.into_session(
            keyspace_holder,
            keyspace_receiver,
            self.node_config.contact_points,
            connection_manager,
            self.node_config.version,
        )
    }
}

#[cfg(feature = "rust-tls")]
/// Builder for TLS sessions.
pub struct RustlsSessionBuilder<
    LB: LoadBalancingStrategy<TransportRustls, RustlsConnectionManager> + Send + Sync + 'static,
> {
    config: SessionConfig<TransportRustls, RustlsConnectionManager, LB>,
    node_config: NodeRustlsConfig,
}

#[cfg(feature = "rust-tls")]
impl<LB: LoadBalancingStrategy<TransportRustls, RustlsConnectionManager> + Send + Sync>
    RustlsSessionBuilder<LB>
{
    //noinspection DuplicatedCode
    /// Creates a new builder with default session configuration.
    pub fn new(load_balancing: LB, node_config: NodeRustlsConfig) -> Self {
        RustlsSessionBuilder {
            config: SessionConfig::new(load_balancing),
            node_config,
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

    fn with_node_distance_evaluator(
        mut self,
        node_distance_evaluator: Box<dyn NodeDistanceEvaluator + Send + Sync>,
    ) -> Self {
        self.config.node_distance_evaluator = node_distance_evaluator;
        self
    }

    fn with_speculative_execution_policy(
        mut self,
        speculative_execution_policy: Box<dyn SpeculativeExecutionPolicy + Send + Sync>,
    ) -> Self {
        self.config.speculative_execution_policy = Some(speculative_execution_policy);
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

    fn with_event_channel_capacity(mut self, event_channel_capacity: usize) -> Self {
        self.config.event_channel_capacity = event_channel_capacity;
        self
    }

    fn with_connection_pool_config(mut self, connection_pool_config: ConnectionPoolConfig) -> Self {
        self.config.connection_pool_config = connection_pool_config;
        self
    }

    fn with_keyspace(mut self, keyspace: String) -> Self {
        self.config.keyspace = Some(keyspace);
        self
    }

    fn build(self) -> Session<TransportRustls, RustlsConnectionManager, LB> {
        let (keyspace_holder, keyspace_receiver) = create_keyspace_holder();
        let connection_manager = RustlsConnectionManager::new(
            self.node_config.dns_name,
            self.node_config.authenticator_provider,
            self.node_config.config,
            keyspace_holder.clone(),
            self.config.reconnection_policy.clone(),
            self.config.compression,
            self.config.transport_buffer_size,
            self.config.tcp_nodelay,
            self.node_config.version,
        );

        self.config.into_session(
            keyspace_holder,
            keyspace_receiver,
            self.node_config.contact_points,
            connection_manager,
            self.node_config.version,
        )
    }
}
