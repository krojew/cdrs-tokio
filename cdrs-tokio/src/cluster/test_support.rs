//! In-memory doubles for exercising request execution without a live cluster.

use cassandra_protocol::error::{Error, Result};
use cassandra_protocol::frame::message_error::{ErrorBody, ErrorType};
use cassandra_protocol::frame::message_request::RequestBody;
use cassandra_protocol::frame::message_result::{
    BodyResResultPrepared, BodyResResultRows, ColSpec, ColType, ColTypeOption, ColTypeOptionValue,
    PreparedMetadata, ResResultBody, RowsMetadata, RowsMetadataFlags, TableSpec,
};
use cassandra_protocol::frame::{Direction, Envelope, Flags, Opcode, Serialize, Version};
use cassandra_protocol::types::{CBytes, CBytesShort};
use futures::future::ready;
use futures::FutureExt;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc::{Sender, WeakSender};
use tokio::sync::watch;
use tokio::time::timeout;
use uuid::Uuid;

use crate::cluster::connection_pool::ConnectionPoolFactory;
use crate::cluster::topology::{Node, NodeDistance, NodeState};
use crate::cluster::ConnectionManager;
use crate::future::BoxFuture;
use crate::retry::{ConstantReconnectionPolicy, NeverReconnectionPolicy, ReconnectionPolicy};
use crate::transport::CdrsTransport;

// upper bound for awaiting outcomes which are expected to happen right away
pub(crate) const TEST_GUARD_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) type RequestHandler =
    Arc<dyn Fn(SocketAddr, &Envelope) -> BoxFuture<'static, Result<Envelope>> + Send + Sync>;

/// Transport which answers every request with a shared handler.
pub(crate) struct TestTransport {
    addr: SocketAddr,
    handler: RequestHandler,
    broken: Arc<AtomicBool>,
}

impl TestTransport {
    pub(crate) fn new(addr: SocketAddr, handler: RequestHandler) -> Self {
        TestTransport {
            addr,
            handler,
            broken: Default::default(),
        }
    }
}

impl CdrsTransport for TestTransport {
    fn write_envelope<'a>(
        &'a self,
        envelope: &'a Envelope,
        _handshake: bool,
    ) -> BoxFuture<'a, Result<Envelope>> {
        (self.handler)(self.addr, envelope)
    }

    fn is_broken(&self) -> bool {
        self.broken.load(Ordering::Relaxed)
    }

    fn address(&self) -> SocketAddr {
        self.addr
    }
}

type ConnectHandler = Arc<dyn Fn(SocketAddr) -> Result<()> + Send + Sync>;

// broken flag and error handler of a connection
type TrackedConnection = (Arc<AtomicBool>, Option<WeakSender<Error>>);

/// Records connections made by a [`TestConnectionManager`], so tests can count and break them.
#[derive(Clone, Default)]
pub(crate) struct ConnectionTracker {
    connections: Arc<Mutex<Vec<TrackedConnection>>>,
}

impl ConnectionTracker {
    pub(crate) fn count(&self) -> usize {
        self.connections.lock().unwrap().len()
    }

    /// Breaks every connection made so far and reports it to its error handler, as a real
    /// transport does.
    pub(crate) fn break_all(&self) {
        for (broken, error_handler) in self.connections.lock().unwrap().iter() {
            broken.store(true, Ordering::Relaxed);

            if let Some(error_handler) = error_handler.as_ref().and_then(WeakSender::upgrade) {
                let _ = error_handler.try_send(Error::General("connection broken".into()));
            }
        }
    }
}

/// Connection manager creating [`TestTransport`]s. Connecting to a node can be made to fail.
pub(crate) struct TestConnectionManager {
    handler: RequestHandler,
    connect_handler: ConnectHandler,
    tracker: ConnectionTracker,
}

impl TestConnectionManager {
    pub(crate) fn new(
        handler: impl Fn(SocketAddr, &Envelope) -> BoxFuture<'static, Result<Envelope>>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        TestConnectionManager {
            handler: Arc::new(handler),
            connect_handler: Arc::new(|_| Ok(())),
            tracker: Default::default(),
        }
    }

    #[must_use]
    pub(crate) fn with_connection_tracker(mut self, tracker: ConnectionTracker) -> Self {
        self.tracker = tracker;
        self
    }

    /// Sets a handler called on every connection attempt; returning an error fails the attempt.
    #[must_use]
    pub(crate) fn with_connect_handler(
        mut self,
        connect_handler: impl Fn(SocketAddr) -> Result<()> + Send + Sync + 'static,
    ) -> Self {
        self.connect_handler = Arc::new(connect_handler);
        self
    }

    /// Makes connection attempts to given nodes fail.
    #[must_use]
    pub(crate) fn with_unreachable_nodes(self, unreachable: Vec<SocketAddr>) -> Self {
        self.with_connect_handler(move |addr| {
            if unreachable.contains(&addr) {
                Err(connection_refused())
            } else {
                Ok(())
            }
        })
    }
}

impl ConnectionManager<TestTransport> for TestConnectionManager {
    fn connection(
        &self,
        _event_handler: Option<Sender<Envelope>>,
        error_handler: Option<Sender<Error>>,
        addr: SocketAddr,
    ) -> BoxFuture<'_, Result<TestTransport>> {
        let result = (self.connect_handler)(addr).map(|_| {
            let transport = TestTransport::new(addr, self.handler.clone());
            self.tracker.connections.lock().unwrap().push((
                transport.broken.clone(),
                error_handler.as_ref().map(Sender::downgrade),
            ));

            transport
        });

        ready(result).boxed()
    }
}

pub(crate) type TestNode = Node<TestTransport, TestConnectionManager>;

pub(crate) fn node_address(index: u8) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, index], 9042))
}

pub(crate) fn connection_refused() -> Error {
    Error::Io(io::Error::new(
        io::ErrorKind::ConnectionRefused,
        "connection refused",
    ))
}

pub(crate) fn create_connection_pool_factory(
    connection_manager: TestConnectionManager,
    version: Version,
) -> Arc<ConnectionPoolFactory<TestTransport, TestConnectionManager>> {
    create_connection_pool_factory_with_reconnection_policy(
        connection_manager,
        version,
        Arc::new(NeverReconnectionPolicy),
    )
}

pub(crate) fn create_connection_pool_factory_with_reconnection_policy(
    connection_manager: TestConnectionManager,
    version: Version,
    reconnection_policy: Arc<dyn ReconnectionPolicy + Send + Sync>,
) -> Arc<ConnectionPoolFactory<TestTransport, TestConnectionManager>> {
    let (_, keyspace_receiver) = watch::channel(None);
    Arc::new(ConnectionPoolFactory::new(
        Default::default(),
        version,
        connection_manager,
        keyspace_receiver,
        reconnection_policy,
    ))
}

/// Creates a factory whose pools wait an hour between reconnection attempts, so a node whose
/// connections break stays down for the duration of a test, unless something wakes its pool.
pub(crate) fn create_slowly_reconnecting_connection_pool_factory(
    tracker: ConnectionTracker,
) -> Arc<ConnectionPoolFactory<TestTransport, TestConnectionManager>> {
    create_connection_pool_factory_with_reconnection_policy(
        TestConnectionManager::new(|_, _| ready(Ok(void_response(Version::V4))).boxed())
            .with_connection_tracker(tracker),
        Version::V4,
        Arc::new(ConstantReconnectionPolicy::new(Duration::from_secs(3600))),
    )
}

/// Breaks all connections and waits until the pool of the node marks it down, which starts a
/// reconnection.
pub(crate) async fn break_connections(node: &TestNode, tracker: &ConnectionTracker) {
    tracker.break_all();

    timeout(TEST_GUARD_TIMEOUT, async {
        while node.state() != NodeState::Down {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("node was not marked down");
}

/// Waits until the pool of the node hands out a connection, which is expected to happen right
/// away.
pub(crate) async fn wait_for_connection(node: &Arc<TestNode>) {
    timeout(TEST_GUARD_TIMEOUT, async {
        while node.persistent_connection().await.is_err() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("node was not reconnected right away");
}

pub(crate) fn create_node(
    connection_pool_factory: &Arc<ConnectionPoolFactory<TestTransport, TestConnectionManager>>,
    addr: SocketAddr,
) -> Arc<TestNode> {
    Arc::new(Node::new_with_state(
        connection_pool_factory.clone(),
        addr,
        None,
        None,
        Some(NodeDistance::Local),
        NodeState::Up,
        Default::default(),
        "".into(),
        "".into(),
    ))
}

pub(crate) fn server_error(addr: SocketAddr, ty: ErrorType) -> Error {
    Error::Server {
        body: ErrorBody::new("test error", ty),
        addr,
    }
}

pub(crate) fn result_response(body: ResResultBody, version: Version) -> Envelope {
    Envelope::new(
        version,
        Direction::Response,
        Flags::empty(),
        Opcode::Result,
        0,
        body.serialize_to_vec(version),
        None,
        vec![],
    )
}

pub(crate) fn void_response(version: Version) -> Envelope {
    result_response(ResResultBody::Void, version)
}

pub(crate) fn prepared_response(id: &CBytesShort, version: Version) -> Envelope {
    result_response(
        ResResultBody::Prepared(BodyResResultPrepared {
            id: id.clone(),
            result_metadata_id: if version >= Version::V5 {
                Some(CBytesShort::new(vec![0]))
            } else {
                None
            },
            metadata: PreparedMetadata {
                pk_indexes: vec![],
                global_table_spec: None,
                col_specs: vec![],
            },
            result_metadata: RowsMetadata {
                flags: RowsMetadataFlags::NO_METADATA,
                columns_count: 0,
                paging_state: None,
                new_metadata_id: None,
                global_table_spec: None,
                col_specs: vec![],
            },
        }),
        version,
    )
}

pub(crate) fn rows_body(
    columns: Vec<(&str, ColTypeOption)>,
    rows: Vec<Vec<CBytes>>,
    version: Version,
) -> BodyResResultRows {
    BodyResResultRows {
        metadata: RowsMetadata {
            flags: RowsMetadataFlags::GLOBAL_TABLE_SPACE,
            columns_count: columns.len() as i32,
            paging_state: None,
            new_metadata_id: None,
            global_table_spec: Some(TableSpec {
                ks_name: "test_ks".into(),
                table_name: "test_table".into(),
            }),
            col_specs: columns
                .into_iter()
                .map(|(name, col_type)| ColSpec {
                    table_spec: None,
                    name: name.into(),
                    col_type,
                })
                .collect(),
        },
        rows_count: rows.len() as i32,
        rows_content: rows,
        protocol_version: version,
    }
}

pub(crate) fn simple_type(id: ColType) -> ColTypeOption {
    ColTypeOption { id, value: None }
}

pub(crate) fn request_query(envelope: &Envelope) -> Option<String> {
    match envelope.request_body() {
        Ok(RequestBody::Query(query)) => Some(query.query),
        _ => None,
    }
}

fn inet_value(ip: IpAddr) -> CBytes {
    CBytes::new(match ip {
        IpAddr::V4(ip) => ip.octets().to_vec(),
        IpAddr::V6(ip) => ip.octets().to_vec(),
    })
}

fn text_list_value(items: &[String]) -> CBytes {
    let mut bytes = (items.len() as i32).to_be_bytes().to_vec();
    for item in items {
        bytes.extend_from_slice(&(item.len() as i32).to_be_bytes());
        bytes.extend_from_slice(item.as_bytes());
    }

    CBytes::new(bytes)
}

fn topology_columns(peers: bool) -> Vec<(&'static str, ColTypeOption)> {
    let mut columns = vec![
        ("host_id", simple_type(ColType::Uuid)),
        (
            if peers { "peer" } else { "broadcast_address" },
            simple_type(ColType::Inet),
        ),
        ("data_center", simple_type(ColType::Varchar)),
        ("rack", simple_type(ColType::Varchar)),
        (
            "tokens",
            ColTypeOption {
                id: ColType::List,
                value: Some(ColTypeOptionValue::CList(Box::new(simple_type(
                    ColType::Varchar,
                )))),
            },
        ),
    ];

    if peers {
        columns.push(("rpc_address", simple_type(ColType::Inet)));
    }

    columns
}

fn topology_row(index: usize, addr: SocketAddr, peers: bool) -> Vec<CBytes> {
    let mut row = vec![
        CBytes::new(Uuid::from_u128(index as u128 + 1).as_bytes().to_vec()),
        if peers {
            inet_value(addr.ip())
        } else {
            CBytes::new_null()
        },
        CBytes::new(b"dc1".to_vec()),
        CBytes::new(b"rack1".to_vec()),
        text_list_value(&[(index as i64 * 1000).to_string()]),
    ];

    if peers {
        row.push(inet_value(addr.ip()));
    }

    row
}

/// Answers control connection queries as a cluster made of nodes at given addresses would, with
/// the first address being the control node. All nodes need to share the same port.
pub(crate) fn topology_response(envelope: &Envelope, addresses: &[SocketAddr]) -> Result<Envelope> {
    let version = envelope.version;
    let query = request_query(envelope).unwrap_or_default();

    let body = if query.starts_with("SELECT * FROM system.local") {
        ResResultBody::Rows(rows_body(
            topology_columns(false),
            vec![topology_row(0, addresses[0], false)],
            version,
        ))
    } else if query.starts_with("SELECT * FROM system.peers") {
        ResResultBody::Rows(rows_body(
            topology_columns(true),
            addresses
                .iter()
                .enumerate()
                .skip(1)
                .map(|(index, addr)| topology_row(index, *addr, true))
                .collect(),
            version,
        ))
    } else {
        ResResultBody::Void
    };

    Ok(result_response(body, version))
}
